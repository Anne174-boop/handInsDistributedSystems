package main

import (
	pb "HandIn3/grpc" //beskedtyper og gRPC-funktioner genereret fra .proto
	"log"             // bruges til at håndtere kontekst i gRPC-kald
	"net"
	"sync" // bruges til at synkronisere adgang til deltagerlisten

	"google.golang.org/grpc" // bruges til at oprette en gRPC-server
	// bruges til at definere gRPC-fejlkoder
	// bruges til at oprette gRPC-fejlmeddelelser
)

// En deltager med sin egen channel til udgående beskeder.
// En deltager i chatten.
type participant struct {
	// Køen indeholder broadcasts, som endnu ikke er sendt.
	// Hvert element er en pointer til en Broadcast-besked.
	queue []*pb.Broadcast

	// Denne channel signalerer, at der er beskeder i køen.
	// Selve beskederne ligger i queue, ikke i denne channel.
	ready chan struct{}
}

// Serverens fælles tilstand.
type chatServer struct {
	// Giver standardimplementeringer til RPC-metoder,
	// som vi endnu ikke selv har implementeret.
	pb.UnimplementedChitChatServer

	// En mutex fungerer som en lås.
	// Den beskytter deltagerlisten, beskedkøerne og Lamport-uret,
	// når flere RPC-handlere kører samtidigt.
	mu sync.Mutex

	// Kobler et klient-ID til deltagerens kø og signalchannel.
	participants map[string]*participant

	// Serverens Lamport-ur. Starter automatisk på 0.
	logicalTime uint64
}

// her starter programmet

func main() {
	//åbn porten så klienten kan blive tilsluttet
	//listener er den åbne netværkslytter (Idk)
	//err er den eventuelle fejl der kan opstå.
	listener, err := net.Listen("tcp", ":50051") // åbner port 50051 så klienten kan kontaktes.

	//hvis der er en fejl uskrives den og programmet sopper
	//f.eks. hvis porten allede er i brug
	if err != nil { //
		log.Fatalf("failed to listen: %v", err)
	}

	//her lukkes lytteren, når main afsluttes normalt.
	//defer betyder "gør dette, når funktionen afsluttes"
	defer listener.Close()

	//opretter en gRPC-server
	grpcServer := grpc.NewServer()

	// Opret vores chatserver med en tom deltagerliste.
	// Lamport-uret starter automatisk på 0.
	chat := &chatServer{
		participants: make(map[string]*participant),
	}

	// Registrér chatserveren hos gRPC.
	pb.RegisterChitChatServer(grpcServer, chat)

	log.Println("component=server event=start port=50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
