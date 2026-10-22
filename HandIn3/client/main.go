package main

import (
	pb "HandIn3/grpc" //beskedtyper og gRPC-funktioner genereret fra .proto
	"log"             // skriver logbeskeder
	"context"
	"os" 			//allows us to put arguments
	"google.golang.org/grpc" // bruges til at oprette en gRPC-server
	"google.golang.org/grpc/credentials/insecure"
)


func main() {
	//log.Println(os.Args)
	//read ID from input, save as clientID
	clientID := os.Args[1]

	//connect to localhost
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //
		log.Fatalf("failed to connect: %v", err)
	}
	
	client := pb.NewChitChatClient(conn)

	//call join
	stream, err := client.Join(context.Background(), &pb.JoinRequest{ClientId: clientID, LogicalTime: 0})
	if err != nil { //
		log.Fatalf("failed to join: %v", err)
	}

	//read lines
	for { 
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("component=client event=stream_ended error=%v", err)
			return
		}
	
		log.Printf("component=client event=received content=%q logical_time=%d", msg.Content, msg.LogicalTime)
	}

	// call leave
	// close connection once main has run
	defer conn.Close()
}