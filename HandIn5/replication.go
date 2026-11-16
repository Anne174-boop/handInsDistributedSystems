package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "HandIn5/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Peers bruger node-id som nøgle og nodens gRPC-klient som værdi.
type Peers map[uint32]pb.AuctionServiceClient

// ReplicateToBackups venter på, at hver levende backup har installeret snapshot'et.
func (p Peers) ReplicateToBackups(
	ctx context.Context, // Styrer hele replikeringen.
	detector *FailureDetector, // Holder styr på nedbrudte noder.
	version uint64, // Nummeret på denne opdatering.
	snapshot SnapshotAuction, // Auktionens tilstand efter opdateringen.
	rpcTimeout time.Duration, // Tidsgrænse for ét RPC-forsøg.
	retryDelay time.Duration, // Pause mellem genforsøg.
) error {
	// Kod hele auktionens tilstand som JSON-bytes.
	data, err := json.Marshal(snapshot)
	if err != nil {
		// %w bevarer den oprindelige fejl.
		return fmt.Errorf("kod snapshot: %w", err)
	}

	// Opret anmodningen én gang, så alle forsøg sender samme tilstand.
	request := &pb.ReplicationRequest{
		PrimaryId: detector.selfID, // Backupen kan kontrollere afsenderens id.
		Version:   version,         // Backupen kan genkende en gentaget opdatering.
		Snapshot:  data,            // Tilstanden, som backupen skal installere.
	}

	// Den ydre løkke vælger, hvilken backup der skal modtage snapshot'et.
	for peerID, client := range p {
		if peerID == detector.selfID {
			continue // Spring den lokale node over.
		}

		// Den indre løkke gentager forsøget for samme backup.
		for !detector.IsDead(peerID) {
			if err := ctx.Err(); err != nil {
				return err // Hele replikeringen er afbrudt.
			}

			// Giv dette forsøg sin egen tidsgrænse.
			attempt, cancel := context.WithTimeout(ctx, rpcTimeout)

			// WaitForReady lader kaldet vente på forbindelsen inden for tidsgrænsen.
			reply, err := client.ReplicateBid(
				attempt, request, grpc.WaitForReady(true),
			)
			cancel() // Frigiv forsøgets timer med det samme.

			if err == nil {
				// Kontrollér både RPC-svaret og backupens kvittering.
				if !reply.GetSuccess() {
					return fmt.Errorf(
						"backup %d afviste version %d", peerID, version,
					)
				}
				break // Afslut den indre løkke og gå videre til næste backup.
			}

			// Afslut ved fejl i anmodningen eller dens version.
			switch status.Code(err) {
			case codes.InvalidArgument, codes.FailedPrecondition:
				return fmt.Errorf("backup %d: %w", peerID, err)
			}

			// Vent lidt, så en midlertidig fejl ikke giver gentagne kald uden pause.
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop() // Afbryd pausen, hvis replikeringen annulleres.
				return ctx.Err()
			case <-timer.C:
				// Pausen er slut. Tjek igen, om backupen stadig lever.
			}
		}
	}

	// Alle backups har kvitteret eller er erklæret døde.
	// ctx.Err() giver nil, hvis konteksten stadig er aktiv.
	return ctx.Err()
}
