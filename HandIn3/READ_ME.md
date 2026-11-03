# Hand In 3

In this assignment you will design and implement Chit Chat, a distributed chat service where participants can join, exchange messages, and leave the conversation at any time. Chit Chat is a lively playground for exploring the essence of distributed systems: communication, coordination, and the ordering of events in a world without a single shared clock.

## About
To run a server, and host at least two clients. For either joining, leaving or post a message will be logged across server and client. The system can be started with at least three (3) nodes (two client and a server) and it must be able to handle "join" of at least one client and "leave" of at least one client.

**Features:**

- Joining
- Messaging
- Leaving

## Getting Started

### Installation

From source:

```bash
git clone https://github.com/Anne174-boop/handInsDistributedSystems.git
cd Handin3
```

### To run the server
```bash
go run ./server 2>&1 | tee logs/server.log
```

### To run client
In a new terminal
```bash
cd Handin3
go run ./client <name> 2>&1 | tee logs/<client_name>.log
```
#### Adding messages
```bash
<Your message>
```

#### Leaving the chat
```bash
/leave
```

## Project Structure

```
HandIn3/
├── client/
│   └── main.go/       
├── grpc/  
│   └── chitchat_grpc.pb.go/
│   └── chitchat.pb.go/   
│   └── chitchat.proto/     
├── server/
│   └── main.go/
│   └── validator_test.go/   
│   └── validator.go/
├── logs/
│   └── contains logs from the terminal to document requirements

└── README.md
```