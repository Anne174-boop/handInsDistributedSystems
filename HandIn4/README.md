# Hand In 4

Description: You are required to implement distributed mutual exclusion among nodes in a distributed system. Your implementation must use the Ricart-Argawala algorithm discussed during the lectures.

## About

This project implements distributed mutual exclusion between peer nodes using the Ricart-Agrawala algorithm. Every node is both a gRPC server and a client to all the other nodes, and there is no central server. Requesting, replying, entering and leaving the Critical Section is logged by each node. The system can be started with at least three (3) nodes, and any node can request access to the Critical Section at any time. The Critical Section is emulated by a log entry and a short sleep.

**Features:**

- Access to the Critical Section
- Replying or deferring requests from other nodes
- Entering and leaving the Critical Section

## Getting Started

### Installation

From source:

```bash
git clone https://github.com/Anne174-boop/handInsDistributedSystems.git
cd HandIn4
```

### To run the nodes
Open one terminal per node, all in the project root of HandIn4. Start all three nodes before running other commands:
```bash
go run ./node 1
go run ./node 2
go run ./node 3
```
Each node prints "Server is running on port ..." when it is ready.

### Requesting the Critical Section
Press **Enter** in a node's terminal to make that node request the Critical Section.
To see a conflict, press Enter in two terminals at almost the same time.
Logs are written to the terminal and to `node1.log`, `node2.log` and `node3.log`.

### Assumptions
Nodes do not crash, and all nodes are started before the first request.

## Testing

### Automated tests
From the project root:

```bash
go test ./node
```

The test in `node/algorithm_test.go` covers `hasPriority`, which decides which of two requests wins a conflict: the lowest Lamport timestamp wins, and on equal timestamps the lowest node ID wins. Both orders are tested for each rule, so two nodes always reach the same decision about who goes first.

The rest of the protocol (replying, deferring, entering and leaving the Critical Section) is demonstrated with the manual test below and the logs in the report.

### Manual test (three nodes)
This scenario shows how a node gains access to the Critical Section, and how a conflict is resolved.

1. Start all three nodes, each in its own terminal (see *Running the nodes*), and wait until each prints "Server is running on port ...".
2. Press **Enter** in node 1 and node 2 at almost the same time. Both request the Critical Section, and the request with the lowest (Lamport timestamp, node ID) wins.
3. While the winner is inside the Critical Section (it lasts 10 seconds), press **Enter** in node 3.
4. Press **Enter** a few more times in each node, so every node enters more than once.

## Project Structure
```
HandIn4/       
├── grpc/  
│   └── proto_grpc.pb.go
│   └── proto.pb.go 
│   └── proto.proto     
├── node/
│   └── main.go
│   └── algorithm_test.go  
│   └── algorithm.go
└── nodes.json
└── go.mod
└── README.md
```