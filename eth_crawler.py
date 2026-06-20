#!/usr/bin/env python3
"""
ETH Transaction Graph Crawler -- Fund-Flow Tree Edition
=========================================================
Recursively follows ETH outward from a seed address using the Etherscan API,
producing a CSV of edges suitable for later import into a Maltego graph.

KEY IDEA: PER-NODE TIME WINDOWS, NOT ONE GLOBAL CLOCK
-------------------------------------------------------
Earlier versions anchored everything to either "now" or one fixed date. That's
wrong for fund-flow tracing: a transaction can be 800+ days old and still be
exactly the one you care about, because what matters is what happened *after
that specific address received the funds* -- not how old the transaction is
relative to today.

So each address in the tree carries its own `received_at` timestamp:
  - The seed's received_at = either --anchor-date (if you know roughly when
    the funds of interest landed there) or the median timestamp of the
    seed's own INCOMING transactions (auto-detected).
  - Every child address's received_at = the timestamp of the specific
    transaction that introduced it into the tree (i.e. when IT received
    funds from its parent in the traced path).

The window (--window-days, default 270 ~ 9 months) is then evaluated
relative to *that node's own* received_at, not a global anchor. This means:
  - Old hub accounts (exchanges, routers) don't blow up the crawl, because we
    only look at the slice of their activity right after they received the
    specific funds we're tracing -- not their entire multi-year history.
  - The "clock" naturally propagates forward through the tree at whatever
    pace the real money actually moved.

HUB / CONTRACT TERMINAL DETECTION (NEW)
----------------------------------------
Before a newly-discovered address is queued for expansion, it's checked
against two signals:

  1. CONTRACT CHECK -- one eth_getcode call (cached, so each unique address
     is only ever checked once across the whole crawl). Non-empty bytecode
     means it's a contract (token, router, protocol, etc.) -> flagged as a
     HUB_TERMINAL with hub_reason="contract".

  2. HIGH-FANOUT EOA HEURISTIC -- for addresses that ARE plain wallets (not
     contracts), one txlist call is used to count distinct senders and
     distinct recipients across its full history. If either exceeds
     --hub-fanout-threshold (default 50), it's flagged as a HUB_TERMINAL
     with hub_reason="high_fanout_eoa". This catches exchange hot wallets
     and similar addresses that aren't contracts but are still not useful
     to expand from.

A flagged hub/terminal node is NOT removed from the graph -- the edge that
discovered it (i.e. where the traced money actually went) is always kept.
It's just not expanded further. What happens with its OWN transaction
history is controlled by --terminal-mode:

  --terminal-mode leaf          (default) Record only the single inbound
                                 edge that discovered the node. No txlist
                                 call is made on it at all. Cheapest option,
                                 and the right one if you only care about
                                 "where did the money go," not "who else
                                 paid into this address."

  --terminal-mode inbound-only   Do one txlist call, but only log edges
                                 where the hub is the RECIPIENT (to ==
                                 address) -- i.e. who else sent funds into
                                 it. The hub is still never expanded outward
                                 and never queued as a parent.

Either way, hub/terminal nodes never get queued as parents, so they never
balloon the crawl the way following every counterparty of a busy exchange
wallet or router contract would.

DIRECTION
---------
--direction forward  (default): follow where each node SENT money after
    receiving it. Window = [received_at, received_at + window_days].
    This is "follow the money downstream."
--direction backward: follow where each node's money CAME FROM before it
    arrived. Window = [received_at - window_days, received_at].
    This is "trace funds upstream toward their source."
--direction both: do both expansions from every node.

USAGE
-----
pip install requests

python3 eth_crawler.py \
    --api-key YOUR_KEY \
    --seed 0xfB476620BA0503ee0aDF837D39C9a51d586eF655 \
    --depth 3 \
    --max-addresses 150 \
    --window-days 270 \
    --direction forward \
    --terminal-mode leaf \
    --hub-fanout-threshold 50 \
    --out eth_graph.csv

Optional: --anchor-date 2024-02-26  (ISO date; EXACT override for seed's receipt time)
Optional: --start-date 2024-01-01   (ISO date; floor -- uses the most recent seed tx
                                      on/after this date as the receipt time, instead
                                      of blindly defaulting to now() when nothing else
                                      can be auto-detected)
Optional: --no-hub-detection         disable contract/fanout detection entirely and
                                      fall back to the old behavior (expand everything
                                      up to --depth / --max-addresses)

API VERSION
-----------
Etherscan deprecated the V1 endpoint -- this script uses V2 (https://api.etherscan.io/v2/api),
which requires a chainid parameter (1 = Ethereum mainnet). Already wired in below.

OUTPUT
------
Two CSVs are written:
  --out                main edge list (same shape as before, plus a
                        `target_is_hub` / `target_hub_reason` column pair
                        and an `edge_relation` column distinguishing
                        normal in-window discovery edges from
                        inbound-only edges logged on a terminal hub).
  --nodes-out           one row per visited/discovered address with its
                        role (SEED / PATH / HUB_TERMINAL), contract flag,
                        hub reason, and fan-out stats -- mirrors the
                        eth_nodes.csv shape used downstream in Maltego.

NOTES
-----
- Etherscan free tier = 5 req/sec, 100k req/day. Default rate-sleep (0.25s) is safe.
- Use --exclude to manually block known addresses regardless of detection.
- The bot-timing heuristic at the end is a triage signal, not a verdict.
"""

import argparse
import csv
import statistics
import sys
import time
from datetime import datetime, timezone
from collections import deque, defaultdict

import requests

API_BASE = "https://api.etherscan.io/v2/api"  # V1 is deprecated, V2 requires chainid
CHAIN_ID = 1  # Ethereum mainnet
DAY_SECONDS = 86400


def get_txlist(address, api_key, page_size=1000, max_pages=10, sleep=0.25):
    """Fetch normal (external) transactions for an address, paginated."""
    all_txs = []
    page = 1
    while page <= max_pages:
        params = {
            "chainid": CHAIN_ID,
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": 0,
            "endblock": 99999999,
            "page": page,
            "offset": page_size,
            "sort": "asc",
            "apikey": api_key,
        }
        try:
            resp = requests.get(API_BASE, params=params, timeout=15)
            data = resp.json()
        except Exception as e:
            print(f"  [!] request failed for {address}: {e}", file=sys.stderr)
            break

        if data.get("status") != "1":
            msg = data.get("message", "")
            if msg != "No transactions found":
                print(f"  [!] API message for {address}: {msg} -> {data.get('result')}", file=sys.stderr)
            break

        result = data.get("result", [])
        if not isinstance(result, list):
            break

        all_txs.extend(result)
        if len(result) < page_size:
            break
        page += 1
        time.sleep(sleep)

    return all_txs


def get_code(address, api_key, sleep=0.25):
    """Return contract bytecode (hex string) for an address, or '0x' if it's a plain EOA."""
    params = {
        "chainid": CHAIN_ID,
        "module": "proxy",
        "action": "eth_getCode",
        "address": address,
        "tag": "latest",
        "apikey": api_key,
    }
    try:
        resp = requests.get(API_BASE, params=params, timeout=15)
        data = resp.json()
        time.sleep(sleep)
        return data.get("result", "0x") or "0x"
    except Exception as e:
        print(f"  [!] eth_getCode failed for {address}: {e}", file=sys.stderr)
        time.sleep(sleep)
        return "0x"


def parse_anchor_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def fmt(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else "n/a"


def determine_seed_received_at(seed, seed_txs, anchor_date_override, start_date_override):
    # 1. Exact override: use this date as received_at, no lookup.
    if anchor_date_override:
        return parse_anchor_date(anchor_date_override), f"user-specified exact date {anchor_date_override}"

    # 2. Start-date floor: don't consider transactions before this date;
    #    among those on/after it, use the MOST RECENT one's timestamp as
    #    the actual received_at (so the window still anchors to a real
    #    transaction, not an arbitrary calendar date).
    if start_date_override:
        floor_ts = parse_anchor_date(start_date_override)

        incoming_after = [int(t.get("timeStamp", 0)) for t in seed_txs
                           if (t.get("to") or "").lower() == seed
                           and t.get("timeStamp") and int(t["timeStamp"]) >= floor_ts]
        if incoming_after:
            return max(incoming_after), (f"most recent INCOMING tx on/after --start-date {start_date_override}")

        any_after = [int(t.get("timeStamp", 0)) for t in seed_txs
                     if t.get("timeStamp") and int(t["timeStamp"]) >= floor_ts]
        if any_after:
            return max(any_after), (f"no incoming tx on/after --start-date {start_date_override}; "
                                     f"used most recent tx of any kind on/after that date instead")

        print(f"  [!] no transactions found on/after --start-date {start_date_override}; "
              f"falling back to auto-detection from full history", file=sys.stderr)

    # 3. Auto-detect from full history: median of incoming, else median of all.
    incoming_ts = [int(t.get("timeStamp", 0)) for t in seed_txs
                   if (t.get("to") or "").lower() == seed and t.get("timeStamp")]
    if incoming_ts:
        return int(statistics.median(incoming_ts)), "median of seed's own INCOMING tx timestamps"

    all_ts = [int(t.get("timeStamp", 0)) for t in seed_txs if t.get("timeStamp")]
    if all_ts:
        return int(statistics.median(all_ts)), "seed had no incoming txs -- used median of all its txs instead"

    return int(time.time()), "seed had no transactions at all -- defaulted to now()"


def classify_hub(address, api_key, rate_sleep, hub_fanout_threshold, hub_detection_enabled,
                  code_cache, txs_for_fanout=None):
    """
    Decide whether `address` should be treated as a HUB_TERMINAL.

    Returns (is_hub, reason, is_contract, distinct_senders, distinct_recipients, fanout_txs)
    where fanout_txs is the txlist that was fetched to compute fan-out (so callers
    using --terminal-mode inbound-only can reuse it instead of fetching twice), or
    None if no txlist call was made for classification.
    """
    if not hub_detection_enabled:
        return False, "", None, None, None, None

    # 1. Contract check (cached across the whole crawl).
    if address in code_cache:
        code = code_cache[address]
    else:
        code = get_code(address, api_key, sleep=rate_sleep)
        code_cache[address] = code

    is_contract = code not in ("0x", "0x0", "", None)
    if is_contract:
        return True, "contract", True, None, None, None

    # 2. High-fanout EOA heuristic -- needs a txlist call if not already provided.
    fanout_txs = txs_for_fanout
    if fanout_txs is None:
        fanout_txs = get_txlist(address, api_key, sleep=rate_sleep)
        time.sleep(rate_sleep)

    senders = {(t.get("from") or "").lower() for t in fanout_txs if (t.get("to") or "").lower() == address}
    recipients = {(t.get("to") or "").lower() for t in fanout_txs if (t.get("from") or "").lower() == address}
    senders.discard("")
    recipients.discard("")

    if len(senders) > hub_fanout_threshold or len(recipients) > hub_fanout_threshold:
        return True, "high_fanout_eoa", False, len(senders), len(recipients), fanout_txs

    return False, "", False, len(senders), len(recipients), fanout_txs


def crawl(seed, api_key, max_depth, max_addresses, rate_sleep, exclude,
          window_days, direction, anchor_date_override, start_date_override,
          hub_detection_enabled, hub_fanout_threshold, terminal_mode, verbose=True):
    seed = seed.lower()
    exclude = exclude or set()

    print(f"[seed] fetching transactions for {seed} ...")
    seed_txs = get_txlist(seed, api_key, sleep=rate_sleep)
    time.sleep(rate_sleep)

    seed_received_at, source = determine_seed_received_at(
        seed, seed_txs, anchor_date_override, start_date_override)
    print(f"[anchor] seed received_at = {fmt(seed_received_at)}  ({source})\n")

    visited = set()
    hub_terminals = {}  # address -> {"reason":..., "is_contract":..., "distinct_senders":..., "distinct_recipients":...}
    code_cache = {}
    rows = []
    node_meta = {}  # address -> dict of node-level info for the nodes CSV

    def record_node(address, role, depth, received_at=None, is_contract=None,
                     hub_reason="", distinct_senders=None, distinct_recipients=None):
        if address not in node_meta:
            node_meta[address] = {
                "address": address,
                "role": role,
                "min_depth_seen": depth,
                "received_at": received_at,
                "is_contract": is_contract,
                "hub_reason": hub_reason,
                "distinct_senders": distinct_senders,
                "distinct_recipients": distinct_recipients,
            }
        else:
            existing = node_meta[address]
            existing["min_depth_seen"] = min(existing["min_depth_seen"], depth)
            # role only ever gets "more terminal", never demoted
            if role == "HUB_TERMINAL":
                existing["role"] = "HUB_TERMINAL"
            if hub_reason:
                existing["hub_reason"] = hub_reason
            if is_contract is not None:
                existing["is_contract"] = is_contract
            if distinct_senders is not None:
                existing["distinct_senders"] = distinct_senders
            if distinct_recipients is not None:
                existing["distinct_recipients"] = distinct_recipients

    record_node(seed, "SEED", 0, received_at=seed_received_at)

    # queue entries: (address, depth, received_at, cached_txs_or_None)
    queue = deque([(seed, 0, seed_received_at, seed_txs)])

    while queue and len(visited) < max_addresses:
        address, depth, received_at, cached_txs = queue.popleft()
        if address in visited or address in exclude:
            continue
        visited.add(address)

        if direction == "forward":
            window_low, window_high = received_at, received_at + window_days * DAY_SECONDS
        elif direction == "backward":
            window_low, window_high = received_at - window_days * DAY_SECONDS, received_at
        else:  # both
            window_low, window_high = (received_at - window_days * DAY_SECONDS,
                                        received_at + window_days * DAY_SECONDS)

        if verbose:
            print(f"[depth {depth}] {address}  received_at={fmt(received_at)}  "
                  f"window=[{fmt(window_low)} .. {fmt(window_high)}]  "
                  f"({len(visited)}/{max_addresses} visited)")

        if cached_txs is not None:
            txs = cached_txs
        else:
            txs = get_txlist(address, api_key, sleep=rate_sleep)
            time.sleep(rate_sleep)

        for tx in txs:
            frm = (tx.get("from") or "").lower()
            to = (tx.get("to") or "").lower()
            try:
                value_eth = int(tx.get("value", "0")) / 1e18
            except (TypeError, ValueError):
                value_eth = None
            ts = int(tx.get("timeStamp", "0") or 0)
            tx_in_window = bool(ts) and (window_low <= ts <= window_high)

            rows.append({
                "tx_hash": tx.get("hash"),
                "block": tx.get("blockNumber"),
                "timestamp": ts,
                "datetime_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)) if ts else "",
                "from": frm,
                "to": to,
                "value_eth": value_eth,
                "gas_used": tx.get("gasUsed"),
                "is_error": tx.get("isError"),
                "method_id": tx.get("methodId"),
                "function_name": tx.get("functionName", ""),
                "node_address": address,
                "node_received_at": received_at,
                "node_window_low": window_low,
                "node_window_high": window_high,
                "discovered_from_node_depth": depth,
                "in_window": tx_in_window,
                "edge_relation": "expansion",
                "target_is_hub": False,
                "target_hub_reason": "",
            })

            if not tx_in_window or depth >= max_depth:
                continue

            # FORWARD: this node sent money out -> the recipient's clock
            # starts ticking at this tx's timestamp.
            if direction in ("forward", "both") and frm == address:
                new_addr = to
                if new_addr and new_addr not in visited and new_addr not in exclude:
                    is_hub, reason, is_contract, d_send, d_recv, fanout_txs = classify_hub(
                        new_addr, api_key, rate_sleep, hub_fanout_threshold,
                        hub_detection_enabled, code_cache)

                    if is_hub:
                        hub_terminals[new_addr] = reason
                        rows[-1]["target_is_hub"] = True
                        rows[-1]["target_hub_reason"] = reason
                        record_node(new_addr, "HUB_TERMINAL", depth + 1, received_at=ts,
                                    is_contract=is_contract, hub_reason=reason,
                                    distinct_senders=d_send, distinct_recipients=d_recv)

                        if terminal_mode == "inbound_only":
                            # One extra pass: log inbound edges to this hub, but never
                            # queue it as a parent / never expand outward from it.
                            hub_txs = fanout_txs if fanout_txs is not None else get_txlist(
                                new_addr, api_key, sleep=rate_sleep)
                            if fanout_txs is None:
                                time.sleep(rate_sleep)
                            for htx in hub_txs:
                                h_frm = (htx.get("from") or "").lower()
                                h_to = (htx.get("to") or "").lower()
                                if h_to != new_addr:
                                    continue  # only inbound edges into the hub
                                if htx.get("hash") == tx.get("hash"):
                                    continue  # skip the discovering edge itself, already logged above
                                h_ts = int(htx.get("timeStamp", "0") or 0)
                                try:
                                    h_val = int(htx.get("value", "0")) / 1e18
                                except (TypeError, ValueError):
                                    h_val = None
                                rows.append({
                                    "tx_hash": htx.get("hash"),
                                    "block": htx.get("blockNumber"),
                                    "timestamp": h_ts,
                                    "datetime_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(h_ts)) if h_ts else "",
                                    "from": h_frm,
                                    "to": h_to,
                                    "value_eth": h_val,
                                    "gas_used": htx.get("gasUsed"),
                                    "is_error": htx.get("isError"),
                                    "method_id": htx.get("methodId"),
                                    "function_name": htx.get("functionName", ""),
                                    "node_address": new_addr,
                                    "node_received_at": None,
                                    "node_window_low": None,
                                    "node_window_high": None,
                                    "discovered_from_node_depth": depth + 1,
                                    "in_window": None,
                                    "edge_relation": "hub_inbound_only",
                                    "target_is_hub": True,
                                    "target_hub_reason": reason,
                                })
                        # Never queued -- hub/terminal nodes are never expanded.
                        continue

                    record_node(new_addr, "PATH", depth + 1, received_at=ts,
                                is_contract=is_contract, distinct_senders=d_send,
                                distinct_recipients=d_recv)
                    queue.append((new_addr, depth + 1, ts, None))

            # BACKWARD: this node received money -> the sender's relevant
            # window is what THEY did before sending it to us.
            if direction in ("backward", "both") and to == address:
                new_addr = frm
                if new_addr and new_addr not in visited and new_addr not in exclude:
                    is_hub, reason, is_contract, d_send, d_recv, fanout_txs = classify_hub(
                        new_addr, api_key, rate_sleep, hub_fanout_threshold,
                        hub_detection_enabled, code_cache)

                    if is_hub:
                        hub_terminals[new_addr] = reason
                        rows[-1]["target_is_hub"] = True
                        rows[-1]["target_hub_reason"] = reason
                        record_node(new_addr, "HUB_TERMINAL", depth + 1, received_at=ts,
                                    is_contract=is_contract, hub_reason=reason,
                                    distinct_senders=d_send, distinct_recipients=d_recv)

                        if terminal_mode == "inbound_only":
                            hub_txs = fanout_txs if fanout_txs is not None else get_txlist(
                                new_addr, api_key, sleep=rate_sleep)
                            if fanout_txs is None:
                                time.sleep(rate_sleep)
                            for htx in hub_txs:
                                h_frm = (htx.get("from") or "").lower()
                                h_to = (htx.get("to") or "").lower()
                                if h_to != new_addr:
                                    continue
                                if htx.get("hash") == tx.get("hash"):
                                    continue  # skip the discovering edge itself, already logged above
                                h_ts = int(htx.get("timeStamp", "0") or 0)
                                try:
                                    h_val = int(htx.get("value", "0")) / 1e18
                                except (TypeError, ValueError):
                                    h_val = None
                                rows.append({
                                    "tx_hash": htx.get("hash"),
                                    "block": htx.get("blockNumber"),
                                    "timestamp": h_ts,
                                    "datetime_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(h_ts)) if h_ts else "",
                                    "from": h_frm,
                                    "to": h_to,
                                    "value_eth": h_val,
                                    "gas_used": htx.get("gasUsed"),
                                    "is_error": htx.get("isError"),
                                    "method_id": htx.get("methodId"),
                                    "function_name": htx.get("functionName", ""),
                                    "node_address": new_addr,
                                    "node_received_at": None,
                                    "node_window_low": None,
                                    "node_window_high": None,
                                    "discovered_from_node_depth": depth + 1,
                                    "in_window": None,
                                    "edge_relation": "hub_inbound_only",
                                    "target_is_hub": True,
                                    "target_hub_reason": reason,
                                })
                        continue

                    record_node(new_addr, "PATH", depth + 1, received_at=ts,
                                is_contract=is_contract, distinct_senders=d_send,
                                distinct_recipients=d_recv)
                    queue.append((new_addr, depth + 1, ts, None))

    return rows, visited, node_meta


def flag_bot_like_patterns(rows, min_tx_count=6, cv_threshold=0.15):
    """
    Triage heuristic: addresses whose outgoing-tx send intervals are
    unusually regular (low coefficient of variation). Signal for manual
    review, not a verdict. Only considers in-window transactions.
    """
    by_address = defaultdict(list)
    for r in rows:
        if r["timestamp"] and r["in_window"]:
            by_address[r["from"]].append(r["timestamp"])

    flags = {}
    for addr, timestamps in by_address.items():
        timestamps = sorted(set(timestamps))
        if len(timestamps) < min_tx_count:
            continue
        deltas = [t2 - t1 for t1, t2 in zip(timestamps, timestamps[1:])]
        if not deltas:
            continue
        mean_delta = sum(deltas) / len(deltas)
        if mean_delta == 0:
            continue
        variance = sum((d - mean_delta) ** 2 for d in deltas) / len(deltas)
        cv = (variance ** 0.5) / mean_delta

        if cv < cv_threshold:
            flags[addr] = {
                "tx_count": len(timestamps),
                "mean_interval_sec": round(mean_delta, 2),
                "coefficient_of_variation": round(cv, 3),
            }
    return flags


def main():
    p = argparse.ArgumentParser(description="Crawl an ETH fund-flow tree via the Etherscan API, "
                                              "with per-node time windows anchored to receipt time "
                                              "and hub/contract terminal detection")
    p.add_argument("--api-key", required=True)
    p.add_argument("--seed", required=True, help="Seed ETH address")
    p.add_argument("--depth", type=int, default=3, help="Max hops outward from seed")
    p.add_argument("--max-addresses", type=int, default=150, help="Cap on total distinct addresses visited (expanded nodes only -- hub terminals don't count against this)")
    p.add_argument("--rate-sleep", type=float, default=0.25, help="Seconds between API calls")
    p.add_argument("--out", default="eth_graph.csv", help="Output edges CSV path")
    p.add_argument("--nodes-out", default="eth_nodes_out.csv", help="Output nodes CSV path")
    p.add_argument("--exclude", nargs="*", default=[], help="Addresses to exclude from expansion")
    p.add_argument("--window-days", type=int, default=270, help="Window size in days (default ~9 months)")
    p.add_argument("--direction", choices=["forward", "backward", "both"], default="forward",
                    help="forward = follow where money went after receipt; "
                         "backward = trace where it came from; both = expand both ways")
    p.add_argument("--anchor-date", default=None,
                    help="ISO date (YYYY-MM-DD): EXACT override for the seed's received_at. "
                         "Use this if you know precisely when the relevant funds landed.")
    p.add_argument("--start-date", default=None,
                    help="ISO date (YYYY-MM-DD): don't consider transactions before this date. "
                         "Among the seed's transactions on/after this date, the MOST RECENT one's "
                         "timestamp is used as received_at (so the anchor is still a real tx, not "
                         "an arbitrary calendar date). Ignored if --anchor-date is also given.")
    p.add_argument("--no-hub-detection", action="store_true",
                    help="Disable contract/fanout hub detection entirely (old behavior: "
                         "expand every address up to --depth / --max-addresses)")
    p.add_argument("--hub-fanout-threshold", type=int, default=50,
                    help="Plain wallets (non-contracts) with more distinct senders OR recipients "
                         "than this, across their full history, are flagged HUB_TERMINAL (default 50)")
    p.add_argument("--terminal-mode", choices=["leaf", "inbound_only"], default="leaf",
                    help="leaf (default) = record only the single discovering edge into a hub, "
                         "no txlist call on it at all. inbound_only = also fetch and log every "
                         "OTHER inbound edge into the hub (who else paid it), still never expand "
                         "outward from it or queue it as a parent.")
    args = p.parse_args()

    exclude = {a.lower() for a in args.exclude}

    rows, visited, node_meta = crawl(
        seed=args.seed,
        api_key=args.api_key,
        max_depth=args.depth,
        max_addresses=args.max_addresses,
        rate_sleep=args.rate_sleep,
        exclude=exclude,
        window_days=args.window_days,
        direction=args.direction,
        anchor_date_override=args.anchor_date,
        start_date_override=args.start_date,
        hub_detection_enabled=not args.no_hub_detection,
        hub_fanout_threshold=args.hub_fanout_threshold,
        terminal_mode=args.terminal_mode,
    )

    if not rows:
        print("No transactions found anywhere in the crawl. Nothing to write.")
        return

    fieldnames = list(rows[0].keys())
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    node_fieldnames = ["address", "role", "min_depth_seen", "received_at", "is_contract",
                       "hub_reason", "distinct_senders", "distinct_recipients"]
    with open(args.nodes_out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=node_fieldnames)
        writer.writeheader()
        for addr, meta in node_meta.items():
            writer.writerow({k: meta.get(k, "") for k in node_fieldnames})

    in_window_count = sum(1 for r in rows if r["in_window"])
    hub_count = sum(1 for m in node_meta.values() if m["role"] == "HUB_TERMINAL")
    path_count = sum(1 for m in node_meta.values() if m["role"] == "PATH")
    print(f"\nWrote {len(rows)} edge rows ({in_window_count} in-window) "
          f"across {len(node_meta)} discovered addresses -> {args.out}")
    print(f"  expanded (PATH/SEED): {path_count + 1}   hub/terminal (not expanded): {hub_count}")
    print(f"Node summary -> {args.nodes_out}")

    flags = flag_bot_like_patterns(rows)
    if flags:
        print("\n--- Addresses with unusually regular send timing, in-window only (manual review suggested) ---")
        for addr, info in flags.items():
            print(f"{addr}: {info}")
    else:
        print("\nNo strongly regular timing patterns detected in this crawl.")


if __name__ == "__main__":
    main()