package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	pb "HandIn5/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	nodes []string
}

func NewClient(nodes []string) *Client {
	return &Client{
		nodes: nodes,
	}
}

func Bid(clients []pb.AuctionServiceClient, bidderID string, amount int32, reqID int32) {
	for round := 0; round < 10; round++ {
		for _, c := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := c.Bid(ctx, &pb.BidRequest{
				BidderId:  bidderID,
				Amount:    amount,
				RequestId: reqID,
			})
			cancel()

			if err == nil {
				fmt.Println("Status:", resp.Status)
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Bid failed: no node available")
}

func Result(clients []pb.AuctionServiceClient) {
	for round := 0; round < 10; round++ {
		for _, c := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := c.Result(ctx, &pb.ResultRequest{})
			cancel()

			if err == nil {
				fmt.Println("Winner:", resp.Winner)
				fmt.Println("Highest bid:", resp.HighestBid)
				fmt.Println("Auction over:", resp.AuctionOver)
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Result failed: no node available")
}

func client() {
	addresses := []string{"localhost:5001", "localhost:5002", "localhost:5003"}

	var clients []pb.AuctionServiceClient
	for _, addr := range addresses {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		clients = append(clients, pb.NewAuctionServiceClient(conn))
	}

	reqID := rand.Int31n(900_000)

	for {
		fmt.Println("\n1. Place bid")
		fmt.Println("2. Get result")

		var choice int
		fmt.Print("Choose option: ")
		fmt.Scanln(&choice)

		switch choice {

		case 1:
			var bidderID string
			var amount int32

			fmt.Print("Bidder ID: ")
			fmt.Scanln(&bidderID)

			fmt.Print("Amount: ")
			fmt.Scanln(&amount)

			reqID++
			Bid(clients, bidderID, amount, reqID)

		case 2:
			Result(clients)

		default:
			fmt.Println("Invalid choice")
		}
	}
}