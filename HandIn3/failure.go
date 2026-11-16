package main

import (
	"context"
	"sync"
	"time"

	pb "HandIn5/proto"
)

// FailureDetector: heartbeat/timeout, vælg ny primary

// FailureDetector overvåger de andre noder med gRPC-heartbeats.
type FailureDetector struct {
	selfID         uint32        // ID for den node, der kører fejldetektoren.
	peers          Peers         // gRPC-forbindelser til de andre noder, slået op efter node-id.
	probeInterval  time.Duration // Tid mellem heartbeat-forsøg.
	heartbeatLimit time.Duration // Maksimal ventetid på ét svar.

	mu   sync.RWMutex    // Beskytter dead mod samtidige læsninger og skrivninger.
	dead map[uint32]bool // true, hvis noden med dette id er markeret som død.

}

// NEWfailureDetector opretter en ny fejldetektor for den givne node.
// selfID: ID for den node, der kører fejldetektoren.
// peers: gRPC-forbindelser til de andre noder, slået op efter node-id.
// probeInterval: tid mellem heartbeat-forsøg.
// heartbeatLimit: maksimal ventetid på ét svar.
// * FailureDetector: pointer til den oprettede fejldetektor.
func NEWfailureDetector(selfID uint32, peers Peers, probeInterval, heartbeatLimit time.Duration) *FailureDetector {
	return &FailureDetector{ // Returnerer en pointer til den nye fejldetektor.
		selfID:         selfID,
		peers:          peers,
		probeInterval:  probeInterval,         //interval mellem heartbeat-forsøg
		heartbeatLimit: heartbeatLimit,        //timeout for et heartbeat (forsøg?)
		dead:           make(map[uint32]bool), //ingen noder er døde fra starten, så derfor et tomt map.
	}
}

// Start efter den fælles opstartsbarriere.
func (fd *FailureDetector) run() {

	var props sync.WaitGroup       // WaitGroup til at vente på alle heartbeat-forsøg.
	for peerID := range fd.peers { // Itererer over alle peer-id'er i peers.
		if peerID == fd.selfID { // Springer over sig selv.
			continue
		}
		props.Add(1)             // Tilføjer en tæller til WaitGroup for hver peer.
		go func(peerID uint32) { // Starter en goroutine for hver peer.
			defer props.Done() // Decrementer tælleren, når goroutinen afsluttes.
		}(peerID)
	}
	props.Wait() //venter på, at alle heardbeat-forsøgene er færdige, så vi kan fortsætte med at tjekke om nogen er døde.
}

// watch overvåger en enkelt peer med det givne peerID.
// sender heardbeats til peerID og venter på svar.
func (f *FailureDetector) watch(ctx context.Context, peerID uint32) {
	//tickets angiver hvornår næste heardbeat kan sendes.
	ticker := time.NewTicker(f.probeInterval)
	defer ticker.Stop() // Stopper ticker, når funktionen afsluttes.

	for {
		select {
		case <-ctx.Done(): // Hvis konteksten annulleres, afsluttes overvågningen.
			return
		case <-ticker.C: // Når tickeren udløber, sendes et heartbeat.
			f.sendHeartbeat(peerID) // Sender et heartbeat til peerID.
			if f.IsDead(peerID) {   // Tjekker om peerID er markeret som død.
				return // Hvis peerID er død, springes heartbeat-forsøget over.
			}

		}
	}
}
func (f *FailureDetector) sendHeartbeat(peerID uint32) {
	// Opretter en kontekst med timeout for heartbeat-forsøget.
	//giver mulighed for at annullere requesten, hvis den tager for lang tid.
	//da hvert grpc har en timelimit, så vi ikke venter for evigt på et svar.
	ctx, cancel := context.WithTimeout(context.Background(), f.heartbeatLimit)
	defer cancel() // Annullerer konteksten, når funktionen afsluttes.

	// Henter gRPC-klienten for peerID.
	client := f.peers[peerID]

	// Sender et heartbeat-request til peerID.
	_, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{SenderId: f.selfID})
	if err != nil { // Hvis der opstår en fejl, markeres peerID som død.
		f.markDead(peerID)
	}
}

func (f *FailureDetector) IsDead(peerID uint32) bool {
	f.mu.RLock()          // Låser for læsning af dead-mappen.
	defer f.mu.RUnlock()  // Frigiver låsen, når funktionen afsluttes.
	return f.dead[peerID] // Returnerer true, hvis peerID er markeret som død.
}

// gør at vi kan få id'et ud
func (f *FailureDetector) PrimaryID() uint32 {
	f.mu.RLock() // her bliver

	primary := f.selfID

	for peersID := range f.peers {
		if peersID < primary && !f.dead[peerID] {
			primary = peersID
		}
	}
	return primary
}
