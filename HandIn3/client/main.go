package main

import (
	pb "HandIn3/grpc" //beskedtyper og gRPC-funktioner genereret fra .proto
	"bufio"           // Læser brugerens input fra terminalen.
	"context"         // Oplysninger om RPC-kaldet.
	"log"             // skriver logbeskeder
	"os"              //allows us to put arguments
	"sync"            // Beskytter klientens Lamport-ur.

	"google.golang.org/grpc" // bruges til at oprette en gRPC-server
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	//Kontrollér, at brugeren har angivet et klient-ID.
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run ./client <client-id>")
	}

	//log.Println(os.Args)
	//read ID from input, save as clientID
	clientID := os.Args[1]

	//connect to localhost
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //
		log.Fatalf("failed to connect: %v", err)
	}

	client := pb.NewChitChatClient(conn)

	// Luk forbindelsen, når main afsluttes.
	// Fjern den gamle defer conn.Close() nederst i filen.
	defer conn.Close()

	// Klientens eget Lamport-ur.
	var clock uint64
	var clockMu sync.Mutex

	// Øg uret før afsendelse, og returnér den nye tid.
	nextTime := func() uint64 {
		clockMu.Lock()
		defer clockMu.Unlock()

		clock++
		return clock
	}

	// Opdatér uret ved modtagelse.
	receiveTime := func(received uint64) {
		clockMu.Lock()
		defer clockMu.Unlock()

		if received > clock {
			clock = received
		}
		clock++
	}

	//call join
	stream, err := client.Join(context.Background(), &pb.JoinRequest{ClientId: clientID, LogicalTime: nextTime()})
	if err != nil { //
		log.Fatalf("failed to join: %v", err)
	}

	// Signalerer, når vi har modtaget første broadcast.
	// Så ved vi, at serveren har registreret klienten.
	ready := make(chan struct{})
	var readyOnce sync.Once

	// Læs og send beskeder i en separat goroutine.
	// Den eksisterende løkke kan derfor fortsat modtage broadcasts.
	go func() {
		// Vent på, at tilmeldingen er registreret.
		<-ready

		scanner := bufio.NewScanner(os.Stdin)

		// Læs én linje, hver gang brugeren trykker Enter.
		for scanner.Scan() {
			message := scanner.Text()

			// Send teksten og klientens nye Lamport-tid til serveren.
			reply, err := client.Publish(
				context.Background(),
				&pb.PublishRequest{
					ClientId:    clientID,
					Content:     message,
					LogicalTime: nextTime(),
				},
			)

			if err != nil {
				// Fx hvis serveren afviser en for lang besked.
				log.Printf(
					"component=Client event=publish_failed client_id=%q error=%q",
					clientID, err,
				)
				continue
			}

			// Kvitteringen er også en modtaget besked.
			receiveTime(reply.GetLogicalTime())
		}

		// Kontrollér, om læsningen fra terminalen fejlede.
		if err := scanner.Err(); err != nil {
			log.Printf(
				"component=Client event=input_failed client_id=%q error=%q",
				clientID, err,
			)
		}
	}()

	//read lines
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("component=client event=stream_ended error=%v", err)
			return
		}

		// Opdatér vores eget ur med broadcastets tid.
		receiveTime(msg.GetLogicalTime())

		// Frigiv input-goroutinen efter første broadcast.
		// sync.Once sørger for, at channelen kun lukkes én gang.
		readyOnce.Do(func() {
			close(ready)
		})

		// Vis og log modtagerens ID, beskedindhold og broadcast-tid.
		log.Printf(
			"component=Client event=received client_id=%q content=%q logical_time=%d",
			clientID,
			msg.Content,
			msg.LogicalTime,
		)
	}

}
