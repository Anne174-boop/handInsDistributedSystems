package main

import (
	pb "HandIn5/proto"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// starter én node (flag: --id, --port, --peers)
// Client: retry med samme request-id

func main() {
	id := flag.Int("id", 0, "Node-id: 1, 2 eller 3")
	deadline := time.Now().Add(120 * time.Second).Unix()
	asClient := flag.Bool("client", false, "Kør som klient i stedet for node")
	newDeadline := flag.Bool("newdeadline", false, "Print en deadline 120 sek. frem og afslut")
	flag.Parse()

	if *newDeadline {
		fmt.Println(time.Now().Add(120 * time.Second).Unix())
		return
	}

	if *asClient {
		client()
		return
	}

	if *id < 1 || *id > 3 || deadline <= 0 {
		log.Fatal("angiv -id 1, 2 eller 3 og en fælles -deadline")
	}

	selfID := uint32(*id)
	addresses := map[uint32]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
		3: "127.0.0.1:5003",
	}

	// Brug den samme deadline i alle tre processer.
	a := NewAuction(0)
	a.SetDeadline(time.Unix(deadline, 0).UnixNano())

	//skal der måske her være en server der lytter på gRPC? (til at modtage bud fra andre noder)

	//kald til result med formattering af print:
	over, winner, bid := a.Result()
	if over {
		fmt.Printf("Auction over. Winner: %d with bid %d\n", winner, bid)
	} else {
		fmt.Printf("Auction ongoing. Highest bid: %d by %d\n", bid, winner)
	}

	// Opret en gRPC-klient til hver af de andre noder.
	peers := make(Peers)
	for peerID, address := range addresses {
		if peerID == selfID {
			continue
		}

		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("opret gRPC-klient til node %d: %v", peerID, err)
		}
		defer conn.Close()

		peers[peerID] = pb.NewAuctionServiceClient(conn)
	}

	// Node 1 starter som primary.
	role := Replica
	if selfID == 1 {
		role = Primary
	}

	node1 := NewNode(*id, role, a)
	node1.peers = peers
	detector := NEWfailureDetector(selfID, peers, 200*time.Millisecond, time.Second)
	node1.failureDetector = detector

	fmt.Print("Node created: ", node1.id)

	lis, err := net.Listen("tcp", addresses[selfID])
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterAuctionServiceServer(
		grpcServer,
		node1,
	)
	go detector.run(context.Background())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
	select {}
}

func NewAuction(duration time.Duration) *Auction {
	return &Auction{
		deadline: time.Now().Add(duration),
		bidders:  make(map[int]int),
		seen:     make(map[int]Outcome),
	}
}
