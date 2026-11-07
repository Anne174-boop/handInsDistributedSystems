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

```bash
go run ./node 1
go run ./node 2
go run ./node 3
```
## Project Structure

HandIn4/       
├── grpc/  
│   └── proto_grpc.pb.go/
│   └── proto.pb.go/   
│   └── proto.proto/     
├── node/
│   └── main.go/
│   └── algorithm_test.go/   
│   └── algorithm.go/
└── README.md
└── nodes.json
