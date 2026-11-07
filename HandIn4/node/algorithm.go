package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "handin4/grpc"
)

type State int

const (
	Released State = iota // Anmoder ikke om adgang.
	Wanted                // Venter på adgang.
	Held                  // Er i den kritiske sektion.
)

type Request struct {
	NodeID    uint32
	Timestamp uint64
}

// Erstatter den eksisterende Node-struktur i main.go.
type Node struct {
	pb.UnimplementedHandIn4Server

	ID   string
	Port string

	//add field to track replies
	replies uint32
	cond 	*sync.Cond //wakes sendRequest when last reply is received

	mu               sync.Mutex
	numericID        uint32
	clock            uint64
	state            State
	requestTimestamp uint64
	deferred         []Request

	// Oprettes før serveren starter og ændres derefter ikke.
	clients map[uint32]pb.HandIn4Client
}

// Kaldes kun, mens mutexen er låst.
func (n *Node) receiveTimestamp(timestamp uint64) {
	if timestamp > n.clock {
		n.clock = timestamp
	}
	n.clock++
}

// Laveste tidsstempel vinder.
// Ved samme tidsstempel vinder laveste knude-ID.
func hasPriority(a, b Request) bool {
	if a.Timestamp == b.Timestamp {
		return a.NodeID < b.NodeID
	}
	return a.Timestamp < b.Timestamp
}

// Opretter en lokal anmodning.
func (n *Node) BeginRequest() (Request, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Released {
		return Request{}, fmt.Errorf("knuden har allerede en aktiv anmodning")
	}

	n.clock++
	n.state = Wanted
	n.requestTimestamp = n.
	
	//reset replies before it goes out
	n.replies = 0

	request := Request{
		NodeID:    n.numericID,
		Timestamp: n.requestTimestamp,
	}

	log.Printf(
		"node=%d REQUEST timestamp=%d",
		n.numericID,
		request.Timestamp,
	)


	return request, nil
}

// gRPC kalder denne metode, når en anden knude anmoder om adgang.
func (n *Node) RequestAccess(
	ctx context.Context,
	in *pb.AccessRequest,
) (*pb.Acknowledgement, error) {
	request := Request{
		NodeID:    in.GetNodeId(),
		Timestamp: in.GetTimestamp(),
	}

	n.mu.Lock()

	n.receiveTimestamp(request.Timestamp)

	ownRequest := Request{
		NodeID:    n.numericID,
		Timestamp: n.requestTimestamp,
	}

	// Udskyd, hvis vi selv er inde, eller hvis vores egen
	// anmodning har prioritet.
	shouldDefer := n.state == Held ||
		(n.state == Wanted && hasPriority(ownRequest, request))

	if shouldDefer {
		n.deferred = append(n.deferred, request)
	}

	n.mu.Unlock()

	log.Printf(
		"node=%d RECEIVE_REQUEST from=%d timestamp=%d deferred=%t",
		n.numericID,
		request.NodeID,
		request.Timestamp,
		shouldDefer,
	)

	if !shouldDefer {
		// Tilladelsen sendes som et separat RPC-kald.
		// Netværkskald udføres uden at holde mutexen.
		if err := n.sendPermission(request); err != nil {
			return nil, err
		}
	}

	n.mu.Lock()
	n.clock++
	ack := &pb.Acknowledgement{
		NodeId:    n.numericID,
		Timestamp: n.clock,
	}
	n.mu.Unlock()

	// Kvitteringen er ikke en tilladelse.
	return ack, nil
}

func (n *Node) Reply(
	ctx context.Context,
	in *pb.AccessReply,
	) (*pb.Acknowledgement, error) {
	
	// læs beskeden og gem i variabler
	senderID := in.GetNodeId()
	senderClock := in.GetTimestamp()
	forRequest := in.GetRequestTimestamp()

	//opdatere shared state
	n.mu.Lock()
	n.receiveTimestamp(senderClock)

	isCurrent := nstate == Wanted && forRequest == n.RequestTimestamp
	
	if isCurrent {
		n.replies++
		if n.replies >= len(n.clients) {
			n.cond.Signal() //wkaes one goRoutine
		}
	}
	
	n.mu.Unlock()

	log.Printf(
		"node=%d RECEIVE_REPLY from=%d request_timestamp=%d counted=%d",
		n.numericID,
		senderID,
		forRequest,
		isCurrent,
	)

	n.mu.Lock()
	n.clock++
	ack := &pb.Acknowledgement{
		NodeId:    n.numericID,
		Timestamp: n.clock,
	}
	n.mu.Unlock()

	return ack, nil
}

// Bruges både til øjeblikkelige tilladelser og senere
// af person 3, når udskudte anmodninger skal besvares.
func (n *Node) sendPermission(request Request) error {
	client, exists := n.clients[request.NodeID]
	if !exists {
		return fmt.Errorf("ukendt knude: %d", request.NodeID)
	}

	n.mu.Lock()
	n.clock++
	reply := &pb.AccessReply{
		NodeId:           n.numericID,
		Timestamp:        n.clock,
		RequesterId:      request.NodeID,
		RequestTimestamp: request.Timestamp,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf(
		"node=%d SEND_REPLY to=%d request_timestamp=%d",
		n.numericID,
		request.NodeID,
		request.Timestamp,
	)

	ack, err := client.Reply(ctx, reply)
	if err != nil {
		return fmt.Errorf("tilladelse til knude %d: %w", request.NodeID, err)
	}

	n.mu.Lock()
	n.receiveTimestamp(ack.GetTimestamp())
	n.mu.Unlock()

	return nil
}

// Opretter en anmodning og sender den til alle andre knuder.
func (n *Node) SendRequest() error {
	request, err := n.BeginRequest()
	if err != nil {
		return err
	}

	// Gemmer den første fejl, men forsøger stadig at sende til alle.
	var firstError error

	for peerID, client := range n.clients {
		// Hvert kald får sin egen tidsgrænse.
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		log.Printf(
			"node=%d SEND_REQUEST to=%d request_timestamp=%d",
			n.numericID,
			peerID,
			request.Timestamp,
		)

		// Alle peers får samme anmodnings-ID og tidsstempel.
		ack, err := client.RequestAccess(ctx, &pb.AccessRequest{
			NodeId:    request.NodeID,
			Timestamp: request.Timestamp,
		})

		// Frigiver timeren efter det enkelte kald.
		cancel()

		if err != nil {
			log.Printf(
				"node=%d REQUEST_FAILED to=%d error=%v",
				n.numericID,
				peerID,
				err,
			)

			if firstError == nil {
				firstError = fmt.Errorf(
					"anmodning til knude %d: %w",
					peerID,
					err,
				)
			}

			continue
		}

		// Modtagelse af en kvittering opdaterer Lamport-uret.
		n.mu.Lock()
		n.receiveTimestamp(ack.GetTimestamp())
		n.mu.Unlock()

		log.Printf(
			"node=%d REQUEST_ACK from=%d",
			n.numericID,
			peerID,
		)
	}

	// Funktionen venter ikke på tilladelser og giver ikke adgang.
	// Det håndterer person 3.
	return firstError
}
