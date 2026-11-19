# Hand In 5

**Description:**
You are required to implement a **distributed auction system** using replication: a distributed component which handles auctions, and provides operations for bidding and querying the state of an auction. The component must faithfully implement the semantics of the auction, and must at least be resilient to one (1) crash failure.

## About
A distributed auction system written in Go with gRPC. Three nodes run as separate processes and replicate the auction state using primary-backup replication. The system tolerates the crash of one node. Node 1 starts as primary, and if the primary crashes, the live node with the lowest ID takes over.

### Features:

- Three nodes (ports 5001-5003) running as separate processes
- Primary-backup replication: the primary sends a snapshot of the auction state to every live backup before replying to a bid
- Heartbeat-based failure detector that marks crashed nodes as dead and picks the lowest live ID as primary
- At-most-once bids: each request ID is stored, so a repeated request returns the stored outcome instead of being applied again
- Bids must be positive and higher than the current highest bid
- The auction ends after 1200 seconds, and the highest bidder wins
- Command-line client for placing bids and querying the result

### API

| Method | Input | Output | Description |
|--------|-------|--------|-------------|
| `Bid` | `bidder_id`, `amount`, `request_id` | `status`: `success`, `fail` or `exception` | Places a bid. Fails if the amount is too low or the auction is over. Exception on invalid input. |
| `Result` | none | `auction_over`, `winner`, `highest_bid` | Returns the highest bid while the auction is running, and the winner when it is over. |

Internal RPCs between nodes:

| Method | Description |
|--------|-------------|
| `ReplicateBid` | The primary sends a snapshot of the auction state to a backup. |
| `Heartbeat` | Used by the failure detector to check that a node is alive. |


## Getting Started

### Installation

From source:

```bash
git clone https://github.com/Anne174-boop/handInsDistributedSystems.git
cd HandIn5
```

### To run the program

Open one terminal per node, all in the project root of HandIn5. Start all nodes before running the client:

```bash
go run . -id 1
go run . -id 2
go run . -id 3
```


### Using the client

In a new terminal, from the project root, start the client (connects to node 1 on localhost:5001)::

```bash
go run -client
```


### The auction

The auction ends 120 seconds after the start of each node. After that, bids return `fail` and `result` returns the winner. The deadline is part of the replicated state, so a new primary knows when the auction ends.

## Assumptions

* The network has reliable, ordered message transport, and messages to non-failed nodes arrive within a known time limit.
* At most one node crashes (fail-stop) and it does not recover.
* All nodes are started before the first request.
* Clients send one request at a time and keep using the same node. The client in this repo connects to node 1.
* Clock skew between nodes is small compared to the auction length, since each node computes its own deadline from its start time.

## Project Structure

```
HandIn5/
├── proto/
│   ├── Auction_grpc.pb.go
│   ├── Auction.pb.go
│   └── Auction.proto
├── main.go
├── node.go
├── auction.go
├── failure.go
├── replication.go
├── client.go
├── go.mod
├── go.sum
└── README.md
```
