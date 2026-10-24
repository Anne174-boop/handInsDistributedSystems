package main

import (
	pb "HandIn3/grpc" //beskedtyper og gRPC-funktioner genereret fra .proto
	"context"         // Oplysninger om RPC-kaldet.
	"fmt"
	"log" // bruges til at håndtere kontekst i gRPC-kald
	"net"
	"sync" // bruges til at synkronisere adgang til deltagerlisten
	"os"

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
	log.SetOutput(os.Stdout)
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

// register new clients joining
func (s *chatServer) Join(req *pb.JoinRequest, stream grpc.ServerStreamingServer[pb.Broadcast]) error {
	clientID := req.GetClientId()

	s.mu.Lock() // Beskyt deltagerlisten, køerne og uret mod samtidig adgang.

	// Undgå at registrere samme klient to gange.
	if _, exists := s.participants[clientID]; exists {
		s.mu.Unlock()
		return status.Error(
			codes.AlreadyExists,
			"client has already joined",
		)
	}

	// register new participant before broadcasting
	p := &participant{
		queue: make([]*pb.Broadcast, 0),
		ready: make(chan struct{}, 1),
	}
	s.participants[clientID] = p

	// update logical_time to the clients time
	s.receiveTime(req.GetLogicalTime())

	//create join message with new logical_time
	s.logicalTime++
	message := &pb.Broadcast{
		ClientId: req.GetClientId(),
		Content: fmt.Sprintf("Participant %s joined Chit Chat at logical time %d",
			req.GetClientId(),
			s.logicalTime,
		),
		LogicalTime: s.logicalTime,
	}

	//now send the join-message to all active participants
	s.broadcast(message)

	s.mu.Unlock()

	// Kør oprydningen, når Join afsluttes,
	// fx ved afbrudt forbindelse eller fejl under afsendelse.
	defer func() {
		s.mu.Lock()

		s.mu.Lock()

		// Registrér afbrydelsen som en lokal hændelse.
		s.logicalTime++

		// Fjern deltageren, og broadcast leave-beskeden.
		s.removeParticipant(clientID, p)

		s.mu.Unlock()

		log.Printf(
			"component=Server event=client_disconnected client_id=%q",
			clientID,
		)
	}()

	log.Printf("component=server event=join client_id=%s", req.ClientId)

	// Send queued broadcasts to this participant.
	for {
		select {
		case <-stream.Context().Done():
			// Client disconnected.
			return nil

		case <-p.ready:
			// There may be multiple messages in the queue.
			for {
				s.mu.Lock()

				if len(p.queue) == 0 {
					s.mu.Unlock()
					break
				}

				// Take the oldest message from the queue.
				msg := p.queue[0]
				p.queue = p.queue[1:]

				s.mu.Unlock()

				// Send the message to the client.
				if err := stream.Send(msg); err != nil {
					return err
				}
			}
		}
	}

	log.Printf("component=server event=stream_closed client_id=%s", req.ClientId)
	return nil
}

// Fjern en deltager, og annoncér det til de resterende.
//
// Serverens mutex skal være låst, når metoden kaldes.
// expected er den konkrete deltagerforbindelse, vi vil fjerne.
func (s *chatServer) removeParticipant(
	clientID string,
	expected *participant,
) {
	// Kontrollér, at det stadig er den samme forbindelse.
	// Det forhindrer dobbelt leave og beskytter en eventuel
	// ny forbindelse, der bruger samme klient-ID.
	current, exists := s.participants[clientID]
	if !exists || current != expected {
		return
	}

	// Fjern deltageren FØR broadcastet.
	// Dermed modtager kun de resterende deltagere beskeden.
	delete(s.participants, clientID)

	// Leave-broadcastet er en ny afsendelseshændelse.
	s.logicalTime++

	message := &pb.Broadcast{
		ClientId: clientID,
		Content: fmt.Sprintf(
			"Participant %s left Chit Chat at logical time %d",
			clientID,
			s.logicalTime,
		),
		LogicalTime: s.logicalTime,
	}

	// Læg leave-beskeden i de resterende deltageres køer.
	s.broadcast(message)

	log.Printf(
		"component=Server event=leave_broadcast_queued client_id=%q logical_time=%d content=%q",
		clientID,
		message.LogicalTime,
		message.Content,
	)
}

// Leave kaldes, når en deltager melder sig ud.
func (s *chatServer) Leave(
	ctx context.Context,
	req *pb.LeaveRequest,
) (*pb.Reply, error) {
	// Beskyt deltagerlisten, køerne og Lamport-uret.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Opdatér uret ved modtagelse af Leave-kaldet.
	s.receiveTime(req.GetLogicalTime())

	// Find den deltager, der ønsker at forlade chatten.
	p, exists := s.participants[req.GetClientId()]
	if !exists {
		return nil, status.Error(
			codes.NotFound,
			"client is not an active participant",
		)
	}

	// Fjern deltageren og send leave-beskeden til de resterende.
	s.removeParticipant(req.GetClientId(), p)

	// Kvitteringen er en ny afsendelseshændelse.
	s.logicalTime++

	return &pb.Reply{
		LogicalTime: s.logicalTime,
	}, nil
}
