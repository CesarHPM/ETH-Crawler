# ETH Transaction Graph Crawler

A Python-based Ethereum fund-flow crawler that recursively traces ETH transactions from a seed address and exports the resulting graph as CSV files suitable for visualization in tools such as Maltego.

## Features

* Recursive transaction graph expansion from a seed Ethereum address
* Forward, backward, or bidirectional fund tracing
* Per-node time windows based on when each address received funds
* Contract detection using `eth_getCode`
* Automatic hub/exchange wallet detection using fan-out analysis
* Configurable crawl depth and address limits
* CSV export for graph analysis and visualization
* Optional terminal-node inbound analysis
* Basic timing-pattern heuristic for identifying highly regular transaction behavior

## Why Per-Node Time Windows?

Most transaction crawlers use a single global time window. This can produce poor results when tracing historical fund movements through active wallets.

This crawler assigns every discovered address its own `received_at` timestamp:

* The seed address receives an initial anchor timestamp.
* Every child address inherits the timestamp of the transaction that introduced it into the graph.
* Transaction windows are evaluated relative to that address's receipt time.

This approach follows the actual movement of funds rather than anchoring analysis to a fixed date.

## Installation

```bash
pip install requests
```

## Example Usage

```bash
python3 eth_crawler.py \
    --api-key YOUR_ETHERSCAN_KEY \
    --seed 0xfB476620BA0503ee0aDF837D39C9a51d586eF655 \
    --depth 3 \
    --max-addresses 150 \
    --window-days 270 \
    --direction forward \
    --out eth_graph.csv
```

## Direction Modes

### Forward

Follow where funds moved after an address received them.

```bash
--direction forward
```

### Backward

Trace where funds originated before reaching an address.

```bash
--direction backward
```

### Both

Perform both forward and backward expansion.

```bash
--direction both
```

## Hub Detection

To prevent large exchange wallets and contracts from exploding the graph size, newly discovered addresses are automatically classified.

### Contract Detection

Addresses with deployed bytecode are marked as terminal contract nodes.

### High-Fanout Wallet Detection

Externally owned accounts (EOAs) with large numbers of unique counterparties are marked as hub terminals.

Threshold can be adjusted with:

```bash
--hub-fanout-threshold 50
```

## Output Files

### Edge CSV

Contains transaction-level graph relationships.

```text
eth_graph.csv
```

### Node CSV

Contains node metadata including:

* Address
* Role
* Discovery depth
* Contract status
* Hub classification
* Fan-out statistics

```text
eth_nodes_out.csv
```

## Notes

* Uses the Etherscan V2 API.
* Supports Ethereum Mainnet (Chain ID 1).
* Optimized for investigative fund-flow analysis rather than full blockchain indexing.
* Native ETH transfers only. ERC-20 token transfers are not currently traced.

## Disclaimer

This tool is intended for blockchain investigation, research, and educational purposes. Results should be manually reviewed and are not proof of ownership, control, or malicious activity.
