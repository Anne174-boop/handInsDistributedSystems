package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"bufio"
	"strconv"

	pb "handin4/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeConfig struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

func main() {
	// Programmet forventer ét argument: knudens ID.
	// os.Args indeholder også programnavnet, så længden skal være 2.
	if len(os.Args) != 2 {
		log.Fatal("brug: go run ./node <node-id>")
	}

	// Henter knudens ID fra kommandolinjen, fx "1".
	nodeID := os.Args[1]

	// Omdanner ID'et fra tekst til et positivt heltal eller nul.
	// 10 betyder titalssystemet, og 32 angiver den tilladte bitstørrelse.
	numericID, err := strconv.ParseUint(nodeID, 10, 32)
	if err != nil {
		// Stopper programmet, hvis ID'et ikke er et gyldigt tal.
		log.Fatalf("ugyldigt node-id: %v", err)
	}

	// Opretter knuden.
	// & betyder, at node er en pointer til den oprettede struktur.
	node := &Node{
		// ID som tekst, bruges bl.a. til sammenligning med konfigurationen.
		ID: nodeID,

		// Danner portnummeret: ID "1" giver port "5001".
		Port: "500" + nodeID,

		// ID som tal, bruges i beskeder og til at afgøre prioritet.
		numericID: uint32(numericID),

		// Knuden starter uden at anmode om den kritiske sektion.
		state: Released,

		// Opretter et tomt map til gRPC-klienterne til de andre knuder.
		// Nøglen er den anden knudes ID; værdien er dens klient.
		clients: make(map[uint32]pb.HandIn4Client),
	}

	node.cond = sync.NewCond(&node.mu)

	data, err := os.ReadFile("nodes.json")
	if err != nil {
		log.Fatalf("could not read nodes file: %v", err)

	}

	var nodes []NodeConfig
	err = json.Unmarshal(data, &nodes)
	if err != nil {
		log.Fatalf("could not unmarshal nodes file: %v", err)
	}

	lis, err := net.Listen("tcp", ":"+node.Port)
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}

	var peers []NodeConfig

	// Fylder peers med alle andre knuder fra nodes.json.
	for _, configuredNode := range nodes {
		if configuredNode.ID != node.ID {
			peers = append(peers, configuredNode)
		}
	}

	// Din eksisterende klientløkke fortsætter her.
	for i := 0; i < len(peers); i++ {
		// Omdanner den anden knudes ID fra tekst til tal.
		peerID, err := strconv.ParseUint(peers[i].ID, 10, 32)
		if err != nil {
			log.Fatalf("ugyldigt peer-id: %v", err)
		}

		// Opretter forbindelsen til den anden knude.
		conn, err := grpc.NewClient(
			peers[i].Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("failed to create client: %v", err)
		}

		// Gemmer klienten, så algoritmen kan sende beskeder til knuden.
		node.clients[uint32(peerID)] = pb.NewHandIn4Client(conn)

		fmt.Println("Client created for node", peers[i].ID)

		// Lukker forbindelsen, når main afsluttes.
		defer conn.Close()
	}

	grpcServer := grpc.NewServer()

	// Registrerer knudens RPC-metoder på serveren.
	pb.RegisterHandIn4Server(grpcServer, node)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	fmt.Println("Server is running on port", node.Port)

	go func() {
		fmt.Println("Press Enter to request the critical section")
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			if err := node.SendRequest(); err != nil {
				log.Printf("node=%d SEND_REQUEST_ERROR: %v", node.numericID, err)
			}
		}
	}()

	select {}
}
