package main

import (
	pb "HandIn3/grpc" //beskedtyper og gRPC-funktioner genereret fra .proto
	"context"         // Oplysninger om RPC-kaldet.
	"log"             // bruges til at håndtere kontekst i gRPC-kald
	"net"
	"sync" // bruges til at synkronisere adgang til deltagerlisten

	"google.golang.org/grpc"        // bruges til at oprette en gRPC-server
	"google.golang.org/grpc/codes"  //grpc-fejlkoder
	"google.golang.org/grpc/status" // opretter grpcfejl fra klient til server
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

// Opdatér serverens Lamport-ur ved modtagelse.
// Serverens mutex skal være låst, når funktionen kaldes.
func (s *chatServer) receiveTime(received uint64) {
	// Brug den modtagne tid, hvis den er større end vores egen.
	if received > s.logicalTime {
		s.logicalTime = received
	}

	// Modtagelsen er en ny hændelse.
	s.logicalTime++
}

// Læg et broadcast i alle aktive deltageres køer.
// Serverens mutex skal være låst, når funktionen kaldes.
func (s *chatServer) broadcast(message *pb.Broadcast) {
	// Gennemgå alle aktive deltagere.
	for _, p := range s.participants {
		// Tilføj beskeden bagerst i deltagerens kø.
		p.queue = append(p.queue, message)

		// Giv deltagerens afsender besked om, at køen har indhold.
		select {
		case p.ready <- struct{}{}:
			// Signalet blev sendt.

		default:
			// Der ligger allerede et signal.
			// Beskeden bliver i køen, så vi kan fortsætte.
		}
	}
}

// Publish kaldes, når en klient sender en chatbesked.
func (s *chatServer) Publish(
	ctx context.Context,
	request *pb.PublishRequest,
) (*pb.Reply, error) {
	// Kontrollér UTF-8 og grænsen på 128 kodepunkter.
	if !validateMessage(request.GetContent()) {
		return nil, status.Error(
			codes.InvalidArgument,
			"message must be valid UTF-8 and at most 128 characters",
		)
	}

	// Beskyt deltagerlisten, køerne og uret mod samtidig adgang.
	s.mu.Lock()

	// Lås automatisk op, når metoden afsluttes.
	defer s.mu.Unlock()

	// Kun tilmeldte deltagere må publicere.
	if _, exists := s.participants[request.GetClientId()]; !exists {
		return nil, status.Error(
			codes.FailedPrecondition,
			"client must join first",
		)
	}

	// Opdatér serverens ur ved modtagelse.
	s.receiveTime(request.GetLogicalTime())

	// Broadcastet er en ny afsendelseshændelse.
	s.logicalTime++

	// Opret beskeden med serverens broadcast-tid.
	message := &pb.Broadcast{
		ClientId:    request.GetClientId(),
		Content:     request.GetContent(),
		LogicalTime: s.logicalTime,
	}

	// Læg beskeden i alle aktive deltageres køer.
	s.broadcast(message)

	// Log kølægningen. Beskeden er endnu ikke nødvendigvis leveret.
	log.Printf(
		"component=Server event=broadcast_queued client_id=%q logical_time=%d content=%q",
		message.ClientId, message.LogicalTime, message.Content,
	)

	// Kvitteringen er en separat afsendelseshændelse.
	s.logicalTime++

	// Send kvitteringen tilbage uden fejl.
	return &pb.Reply{
		LogicalTime: s.logicalTime,
	}, nil
}

// her starter programmet

func main() {
	//åbn porten så klienten kan blive tilsluttet
	//listener er den åbne netværkslytter (Idk)
	//err er den eventuelle fejl der kan opstå.
	listener, err := net.Listen("tcp", ":50051") // åbner port 50051 så klienten kan kontaktes.

	//hvis der er en fejl udskrives den og programmet sopper
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

func (s *chatServer) Join(req *pb.JoinRequest, stream grpc.ServerStreamingServer[pb.Broadcast]) error {
	log.Printf("component=server event=join client_id=%s", req.ClientId)

	<-stream.Context().Done()

	log.Printf("component=server event=stream_closed client_id=%s", req.ClientId)
	return nil
}
	
func (s *chatServer) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.Reply, error) {
	log.Printf("component=server event=leave client_id=%s", req.ClientId)
	return &pb.Reply{}, nil
}
