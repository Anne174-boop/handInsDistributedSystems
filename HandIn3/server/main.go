package main

import (
	pb "HandIn3/grpc" //beskedtyper og gRPC-funktioner genereret fra .proto
	"log"             // skriver logbeskeder
	"net"             //åbner en netværksport

	"google.golang.org/grpc" // bruges til at oprette en gRPC-server
)

// pb betyder Protocol Buffers, som giver adgang til de beskedtyper
// og pRPC funktioner som genereres fra -proto-filen.

// vores chatserver, senere skal vi tilføje en deltagerlist
// unimplementedchitchatserver giver standdard-metoder,
// som siger "not implemented " indtil vi selv skriver join publish og leave.
type chatServer struct {
	pb.UnimplementedChitChatServer
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

	//registrerer vores chatserver på gRPC-serveren
	pb.RegisterChitChatServer(grpcServer, &chatServer{})

	log.Println("component=server event=start port=50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
