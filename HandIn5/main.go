package main

import (
	pb "HandIn5/proto"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
)

// starter én node (flag: --id, --port, --peers)
// Client: retry med samme request-id

func main() {
	id := flag.Int("id", 0, "Node-id: 1, 2 eller 3")
	deadline := flag.Int64("deadline", 0, "Fælles deadline som Unix-tid i sekunder")
	flag.Parse()

	if *id < 1 || *id > 3 || *deadline <= 0 {
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
	a.SetDeadline(time.Unix(*deadline, 0).UnixNano())

	//skal der måske her være en server der lytter på gRPC? (til at modtage bud fra andre noder)

	//kald til result med formattering af print:
	over, winner, bid := a.Result()
	if over {
		fmt.Printf("Auction over. Winner: %d with bid %d\n", winner, bid)
	} else {
		fmt.Printf("Auction ongoing. Highest bid: %d by %d\n", bid, winner)
	}

	node1 := NewNode(1, Primary, a)

	fmt.Print("Node created: ", node1.id)

	lis, err := net.Listen("tcp", ":5001")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterAuctionServiceServer(
		grpcServer,
		node1,
	)

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
