package main

import (
	pb "HandIn5/proto"
	"context"
	"fmt"
	"sync"

	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Role string

const (
	Primary Role = "primary"
	Replica Role = "replica"
)

// skal håndtere indgående g-RPC-kald
type node struct {

	// Giver standardimplementeringer som returnerer Unimplemented for RPC'er, som endnu ikke har skrevet.
	pb.UnimplementedAuctionServiceServer

	mu               sync.Mutex //beskytter aktionen og de der versionsfelter
	id               int
	role             Role
	auction          *Auction
	failureDetector  *FailureDetector
	version          uint64 //den version for snapshot der er installeret
	installedPrimary uint32 // den som sendte den installerede snapshot?
}

func NewNode(id int, role Role, auction *Auction) *node {
	return &node{
		id:      id,
		role:    role,
		auction: auction,
	}
}

func (n *node) Result(
	ctx context.Context,
	req *pb.ResultRequest,
) (*pb.ResultResponse, error) {
	n.mu.Lock()

	over, winner, bid := n.auction.Result()

	n.mu.Unlock()

	return &pb.ResultResponse{
		AuctionOver: over,
		Winner:      fmt.Sprint(winner),
		HighestBid:  int32(bid),
	}, nil
}

func (n *node) Bid(
	ctx context.Context,
	req *pb.BidRequest,
) (*pb.BidResponse, error) {
	n.mu.Lock()

	outcome := n.auction.Bid(
		int(req.Amount),
		1,
		1,
	)

	n.mu.Unlock()

	var status string

	switch outcome {
	case Success:
		status = "success"

	case Fail:
		status = "fail"

	case Exception:
		status = "exception"
	}

	return &pb.BidResponse{
		Status: status,
	}, nil
}

// skal bekræfte at noden er i live.
func (n *node) Heartbeat(_ context.Context, _ *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{}, nil // Returnér en pointer til et tomt heartbeat-svar og nil for at vise, at kaldet lykkedes.
}

// ReplicateBid installerer primarys snapshot på denne backup.
func (n *node) ReplicateBid(
	_ context.Context,
	req *pb.ReplicationRequest,
) (*pb.ReplicationResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.failureDetector == nil {
		return nil, status.Error(codes.FailedPrecondition, "fejldetektoren er ikke sat")
	}

	primaryID := req.GetPrimaryId()

	// En backup kan opdage lederskiftet lidt senere end den nye primary.
	if primaryID == n.failureDetector.selfID ||
		primaryID != n.failureDetector.PrimaryID() {
		return nil, status.Error(codes.Unavailable, "afsender er endnu ikke kendt som primary")
	}

	version := req.GetVersion()
	if version == 0 {
		return nil, status.Error(codes.InvalidArgument, "version skal være positiv")
	}

	// Sammenlign versioner fra samme primary.
	if primaryID == n.installedPrimary {
		switch {
		case version < n.version:
			return nil, status.Error(codes.FailedPrecondition, "snapshot er for gammelt")
		case version == n.version:
			// Gentag kvitteringen for et snapshot, der allerede er installeret.
			return &pb.ReplicationResponse{Success: true}, nil
		}
	}

	// Afkod og validér snapshot'et, før auktionstilstanden ændres.
	var snapshot SnapshotAuction
	if err := json.Unmarshal(req.GetSnapshot(), &snapshot); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "afkod snapshot: %v", err)
	}
	if snapshot.Bidders == nil || snapshot.Seen == nil || snapshot.Deadline.IsZero() {
		return nil, status.Error(codes.InvalidArgument, "snapshot mangler auktionstilstand")
	}

	// Installér tilstanden, før backupen kvitterer.
	Restore(n.auction, snapshot)
	n.version = version
	n.installedPrimary = primaryID

	return &pb.ReplicationResponse{Success: true}, nil
}
