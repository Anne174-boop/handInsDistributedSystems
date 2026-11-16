package main

import (
	"context"
	"fmt"
	"log"

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

func Bid(client pb.AuctionServiceClient, bidderID string, amount int32) {
	resp, err := client.Bid(
		context.Background(),
		&pb.BidRequest{
			BidderId: bidderID,
			Amount:   amount,
		},
	)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Status:", resp.Status)
}

func Result(client pb.AuctionServiceClient) {
	resp, err := client.Result(
		context.Background(),
		&pb.ResultRequest{},
	)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Winner:", resp.Winner)
	fmt.Println("Highest bid:", resp.HighestBid)
	fmt.Println("Auction over:", resp.AuctionOver)
}

func client() {

	conn, err := grpc.NewClient(
		"localhost:5001",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewAuctionServiceClient(conn)

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

			Bid(client, bidderID, amount)

		case 2:
			Result(client)

		default:
			fmt.Println("Invalid choice")
		}
	}
}
