# Hand In 5

**Description:**
You are required to implement a **distributed auction system** using replication: a distributed component which handles auctions, and provides operations for bidding and querying the state of an auction. The component must faithfully implement the semantics of the auction, and must at least be resilient to one (1) crash failure.

## About


### Features:



### API


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
go run ./node 1
go run ./node 2
go run ./node 3
```

Each node prints "SOMETHING" when it is ready.

### Using the client

In a new terminal, from the project root:

```bash
go run ./client
```


### The auction

The auction ends [TODO: 100] time units after [TODO: the start of the primary]. After that, bids return `fail` and `result` returns the winner. The deadline is part of the replicated state, so a new primary knows when the auction ends.

## Assumptions

* The network has reliable, ordered message transport, and messages to non-failed nodes arrive within a known time limit.
* At most one node crashes (fail-stop) and it does not recover.
* All nodes are started before the first request.
* [TODO: any assumption about the number of clients, or about the clocks]

## Project Structure

```
HandIn5/
├── proto/
│   └── Auction_grpc.pb.go
│   └── Auction.pb.go
│   └── proto.proto
├── node/
│   └── node.go
│   └── auction.go
│   └── auction_test.go
├── client/
│   └── main.go
└── go.mod
└── go.sum
└── README.md
```
