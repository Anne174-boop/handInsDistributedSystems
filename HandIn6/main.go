// raft.go
// ✨🌸 A tiny, cute Raft implementation in Go 🌸✨   (needs Go 1.21+)
//
// Raft in one sentence: a group of nodes elects ONE leader, the leader
// accepts commands, copies them to the followers, and once a MAJORITY has
// a copy the command is "committed" and every node applies it, in the same order.
//
// How the code is organised:
//   1. Types        - states, log entries, RPC messages, the Node struct
//   2. Setup        - creating + starting a node
//   3. Main loop    - each node's heartbeat / election-timeout loop
//   4. Elections    - becoming a candidate, asking for votes (RequestVote)
//   5. Replication  - leader copying its log to followers (AppendEntries)
//   6. Demo         - main(): 5 nodes, elect a leader, send commands, kill leader <3

package main

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// =====================================================================
// 1. TYPES 🧸
// =====================================================================

// Every node is always in exactly one of these three states.
type State int

const (
	Follower  State = iota // 🌱 just listens to the leader
	Candidate              // 🙋 wants to become leader, is asking for votes
	Leader                 // 👑 runs the show
)

// One entry in the log. The Term tells us WHICH leader created it.
type LogEntry struct {
	Term    int
	Command string // in a real system this would be "set x=5" etc.
}

// --- RPC messages ---
// (net/rpc needs exported (Capitalised) types and fields so it can send them)

// A candidate sends this to ask "will you vote for me?" 🗳️
type RequestVoteArgs struct {
	Term         int // the candidate's term
	CandidateID  int
	LastLogIndex int // how long the candidate's log is...
	LastLogTerm  int // ...and the term of its last entry (so we only elect up-to-date nodes)
}

type RequestVoteReply struct {
	Term        int  // the voter's term (so a stale candidate can notice it's behind)
	VoteGranted bool // yes / no
}

// The leader sends this to copy log entries. With NO entries it is a heartbeat 💓
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int        // index of the entry right BEFORE the new ones
	PrevLogTerm  int        // term of that entry (follower checks it matches its own log)
	Entries      []LogEntry // new entries (empty for heartbeats)
	LeaderCommit int        // how far the leader has committed
}

type AppendEntriesReply struct {
	Term    int
	Success bool // false means "my log doesn't match at PrevLogIndex"
}

// Node is one server in our little Raft cluster.
type Node struct {
	mu       sync.Mutex // many goroutines touch the node, so everything is protected by this lock
	id       int
	addr     string   // e.g. "localhost:9000"
	peers    []string // addresses of the OTHER nodes
	alive    bool     // set to false to simulate a crash 💥
	listener net.Listener

	// --- state from the Raft paper ---
	state       State
	currentTerm int        // the latest term this node has seen (terms = "reigns" of leaders)
	votedFor    int        // who we voted for in currentTerm (-1 = nobody yet)
	log         []LogEntry // index 0 is a dummy entry, so real entries start at index 1

	commitIndex int      // highest log index known to be committed (safe!)
	lastApplied int      // highest log index we have applied
	applied     []string // the commands we applied, in order (our "state machine")

	// leader-only bookkeeping, one entry per follower
	nextIndex  map[string]int // the next log index to send to that follower
	matchIndex map[string]int // the highest log index we KNOW that follower has

	electionDeadline time.Time // if this passes without a heartbeat -> start an election
}

// =====================================================================
// 2. SETUP 🧁
// =====================================================================

func newNode(id int, addr string, peers []string) *Node {
	n := &Node{
		id:         id,
		addr:       addr,
		peers:      peers,
		alive:      true,
		state:      Follower, // everybody starts as a follower
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}}, // dummy entry at index 0
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
	}
	n.resetElectionTimer()
	return n
}

// start opens a TCP port for incoming RPCs and launches the main loop.
func (n *Node) start() {
	server := rpc.NewServer()
	server.RegisterName("Raft", n) // exposes n.RequestVote and n.AppendEntries to other nodes

	listener, err := net.Listen("tcp", n.addr)
	if err != nil {
		panic(err)
	}
	n.listener = listener

	go server.Accept(listener) // handle incoming RPCs in the background
	go n.run()                 // the node's own heartbeat / election loop
}

// kill simulates a crash: close the port and stop doing anything. 💥
func (n *Node) kill() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.alive = false
	n.listener.Close()
	fmt.Printf("💥 node %d crashed!\n", n.id)
}

// call sends one RPC to another node. It returns false if the node can't be reached.
// Every call has a timeout so a dead node can't block us forever.
func call(addr, method string, args, reply any) bool {
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		return false
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	done := client.Go(method, args, reply, make(chan *rpc.Call, 1)).Done
	select {
	case c := <-done:
		return c.Error == nil
	case <-time.After(200 * time.Millisecond):
		return false
	}
}

// Random timeouts make it unlikely that two nodes become candidates at the same time.
func (n *Node) resetElectionTimer() {
	n.electionDeadline = time.Now().Add(time.Duration(400+rand.Intn(400)) * time.Millisecond)
}

// Small helpers for the log (remember: index 0 is the dummy entry).
func (n *Node) lastLogIndex() int { return len(n.log) - 1 }
func (n *Node) lastLogTerm() int  { return n.log[n.lastLogIndex()].Term }

// stepDown: we saw a newer term, so we go back to being a follower 🌱
func (n *Node) stepDown(term int) {
	if n.state == Leader {
		fmt.Printf("🌱 node %d is no longer leader (saw term %d)\n", n.id, term)
	}
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1 // new term -> we haven't voted yet
	}
	n.state = Follower
}

// =====================================================================
// 3. MAIN LOOP ⏰
// =====================================================================

// run ticks every 50ms:
//   - leaders send heartbeats (so followers know the leader is alive)
//   - everybody else checks if the election timer ran out
func (n *Node) run() {
	for {
		time.Sleep(50 * time.Millisecond)

		n.mu.Lock()
		if !n.alive {
			n.mu.Unlock()
			return // crashed nodes stop
		}
		if n.state == Leader {
			n.replicateToAll() // 💓 (also sends any new log entries)
		} else if time.Now().After(n.electionDeadline) {
			n.startElection() // nobody talked to us in a while -> we try to become leader
		}
		n.mu.Unlock()
	}
}

// =====================================================================
// 4. ELECTIONS 🗳️
// =====================================================================

// startElection (the lock is already held when this is called)
func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++        // a new term begins
	n.votedFor = n.id      // we vote for ourselves
	n.resetElectionTimer() // if this election fails, we try again later
	fmt.Printf("🙋 node %d starts an election for term %d\n", n.id, n.currentTerm)

	term := n.currentTerm
	votes := 1 // our own vote
	total := len(n.peers) + 1
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}

	// Ask every peer for its vote, all at the same time (one goroutine per peer).
	for _, peer := range n.peers {
		go func(peer string) {
			var reply RequestVoteReply
			if !call(peer, "Raft.RequestVote", args, &reply) {
				return // peer unreachable, ignore
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm { // someone is ahead of us -> give up
				n.stepDown(reply.Term)
				return
			}
			// Ignore replies that arrive too late (we're not running for this term anymore).
			if n.state != Candidate || n.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > total/2 { // MAJORITY! 🎉
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	fmt.Printf("👑 node %d is the LEADER of term %d!\n", n.id, n.currentTerm)
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1 // optimistic: assume followers are up to date
		n.matchIndex[peer] = 0                   // ...but we don't KNOW that yet
	}
	n.replicateToAll() // announce ourselves right away with a heartbeat
}

// RequestVote is called BY a candidate ON us (via RPC). Should we vote for it?
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return errors.New("node is down")
	}

	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term < n.currentTerm {
		return nil // candidate is from the past, vote no
	}

	// Safety rule: only vote for a candidate whose log is at least as up-to-date as ours.
	// This guarantees a new leader always has all committed entries.
	upToDate := args.LastLogTerm > n.lastLogTerm() ||
		(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())

	// One vote per term: we only vote if we haven't voted, or already voted for this candidate.
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimer() // we heard from a viable candidate, so wait a bit
	}
	return nil
}

// =====================================================================
// 5. LOG REPLICATION 💌
// =====================================================================

// propose is how a client gives the leader a new command.
// Returns false if this node isn't the leader (the client should try another node).
func (n *Node) propose(cmd string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive || n.state != Leader {
		return false
	}
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: cmd})
	n.replicateToAll()
	return true
}

// replicateToAll (lock held) sends AppendEntries to every follower.
func (n *Node) replicateToAll() {
	for _, peer := range n.peers {
		next := n.nextIndex[peer]
		prev := next - 1
		args := AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prev,
			PrevLogTerm:  n.log[prev].Term,
			Entries:      append([]LogEntry{}, n.log[next:]...), // a COPY of what the follower is missing
			LeaderCommit: n.commitIndex,
		}
		go n.replicateTo(peer, args)
	}
}

// replicateTo sends one AppendEntries and handles the answer.
func (n *Node) replicateTo(peer string, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !call(peer, "Raft.AppendEntries", &args, &reply) {
		return // follower unreachable, we'll retry on the next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm { // a newer term exists -> we're not the leader anymore
		n.stepDown(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != args.Term {
		return // stale reply
	}

	if reply.Success {
		// The follower now has everything up to PrevLogIndex + len(Entries).
		match := args.PrevLogIndex + len(args.Entries)
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
			n.nextIndex[peer] = match + 1
			n.advanceCommitIndex()
		}
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]-- // logs didn't match -> step back one entry and retry next heartbeat
	}
}

// advanceCommitIndex (lock held): an entry is COMMITTED once a majority has it.
func (n *Node) advanceCommitIndex() {
	total := len(n.peers) + 1
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		// Raft rule: a leader only commits entries from its OWN term by counting copies.
		// (Older entries become committed along the way.)
		if n.log[idx].Term != n.currentTerm {
			break
		}
		copies := 1 // the leader has it
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				copies++
			}
		}
		if copies > total/2 {
			n.commitIndex = idx
			n.applyCommitted()
			break
		}
	}
}

// AppendEntries is called BY the leader ON us (via RPC): heartbeat + new log entries.
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return errors.New("node is down")
	}

	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	reply.Term = n.currentTerm
	reply.Success = false

	if args.Term < n.currentTerm {
		return nil // message from an old leader, ignore it
	}

	// This is a real leader for our term: we're a follower and we reset our timer 💓
	n.state = Follower
	n.resetElectionTimer()

	// Consistency check: do we have the entry right before the new ones, with the same term?
	if args.PrevLogIndex > n.lastLogIndex() || n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil // nope -> leader will back up and send older entries
	}

	// Add the new entries. If one of ours conflicts with the leader's, delete it
	// and everything after it. (The leader is always right!)
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= n.lastLogIndex() {
			if n.log[idx].Term == entry.Term {
				continue // we already have this exact entry
			}
			n.log = n.log[:idx] // conflict! cut our log here
		}
		n.log = append(n.log, entry)
	}
	reply.Success = true

	// Move our commit index forward to match the leader's (but never past what we have).
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		n.applyCommitted()
	}
	return nil
}

// applyCommitted (lock held): run every newly committed command, in order.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		cmd := n.log[n.lastApplied].Command
		n.applied = append(n.applied, cmd)
		fmt.Printf("   💌 node %d applied #%d: %q\n", n.id, n.lastApplied, cmd)
	}
}

// =====================================================================
// 6. DEMO 🎀
// =====================================================================

func (n *Node) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.alive && n.state == Leader
}

func (n *Node) appliedCommands() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string{}, n.applied...)
}

// sendToLeader looks for the leader and gives it a command (like a client would).
func sendToLeader(nodes []*Node, cmd string) {
	for _, node := range nodes {
		if node.propose(cmd) {
			fmt.Printf("📮 client sent %q to leader node %d\n", cmd, node.id)
			return
		}
	}
	fmt.Println("😿 no leader found, try again later")
}

func main() {
	fmt.Println("🌸 starting a 5-node Raft cluster 🌸")

	// Give each node an address, and tell it about all the OTHER addresses.
	addrs := []string{"localhost:9000", "localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004"}
	nodes := make([]*Node, len(addrs))
	for i, addr := range addrs {
		var peers []string
		for j, other := range addrs {
			if j != i {
				peers = append(peers, other)
			}
		}
		nodes[i] = newNode(i, addr, peers)
		nodes[i].start()
	}

	time.Sleep(2 * time.Second) // wait for a leader to be elected

	sendToLeader(nodes, "make tea 🍵")
	sendToLeader(nodes, "pet the cat 🐱")
	sendToLeader(nodes, "write the report 📝")
	time.Sleep(1 * time.Second)

	// Crash the current leader and watch the others elect a new one!
	for _, node := range nodes {
		if node.isLeader() {
			node.kill()
			break
		}
	}
	time.Sleep(2 * time.Second)

	sendToLeader(nodes, "celebrate 🎉")
	time.Sleep(1 * time.Second)

	// Every surviving node should have applied the SAME commands in the SAME order.
	fmt.Println("\n🎀 final state machines:")
	for _, node := range nodes {
		fmt.Printf("node %d: %v\n", node.id, node.appliedCommands())
	}
}