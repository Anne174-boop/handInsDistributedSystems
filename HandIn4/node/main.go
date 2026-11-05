package main

import (
	"fmt"
	"os"
	"log"
	"net"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	ID string
	Port string
}	

type NodeConfig struct {
	ID string `json:"id"`
	Address string `json:"address"`
}

func main() {
	nodeID := os.Args[1]
	node := Node{
		ID: nodeID,
		Port: "500" + nodeID,
	}

	data, err := os.ReadFile("../nodes.json")
	if err != nil {
		log.Fatalf("could not read nodes file: %v", err)
	}

	var nodes []NodeConfig
	err = json.Unmarshal(data, &nodes)
	if err != nil {
		log.Fatalf("could not unmarshal nodes file: %v", err)
	}
	var peers []NodeConfig
	for i := 0; i < len(nodes); i++ {
		if nodes[i].ID != node.ID {
			peers = append(peers, nodes[i])
		}
	}

	lis, err := net.Listen("tcp", ":"+node.Port)
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	
	fmt.Println("Server is running on port", node.Port)
	
	for i := 0; i < len(peers); i++ {
		fmt.Println("Connecting to node", peers[i].ID, "at", peers[i].Address)
		conn, err := grpc.NewClient(peers[i].Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect to node %s: %v", peers[i].ID, err)
		}

		fmt.Println("Connected to node", peers[i].ID)

		defer conn.Close()
	}

	select {}
}