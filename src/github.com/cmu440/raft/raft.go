//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = true

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
const (
	follower  = 0
	candidate = 1
	leader    = 2
)

const heartbeatInterval = 120 * time.Millisecond

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	currentTerm int // Current election term.
	votedFor    int // Peer voted for in this term; -1 means nobody.
	state       int // follower, candidate, or leader.

	// Your data here (3A, 3B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	electionDeadline time.Time

	stopped bool
	log     []LogEntry

	heartbeatDeadline time.Time
}

func (rf *Raft) resetElectionDeadlineLocked() {
	// Choose a fresh random delay.
	// Set electionDeadline to now plus that delay.

	timeout := time.Duration(450+rand.Intn(301)) * time.Millisecond

	rf.electionDeadline = time.Now().Add(timeout)
}

// Caller must hold rf.mux.
func (rf *Raft) startElectionLocked() {
	// 1. Become a candidate.
	rf.state = candidate
	// 2. Increase the current term by one.
	rf.currentTerm += 1
	// 3. Vote for this peer itself.
	rf.votedFor = rf.me
	// 4. Reset the election deadline.
	rf.resetElectionDeadlineLocked()

	term := rf.currentTerm
	votes := 1
	majority := len(rf.peers)/2 + 1

	lastIndex := len(rf.log) - 1

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  rf.log[lastIndex].Term,
	}

	if votes >= majority {
		rf.becomeLeaderLocked()
		return
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mux.Lock()
			defer rf.mux.Unlock()
			if rf.stopped {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.state = follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				return
			}

			if rf.state != candidate || rf.currentTerm != term {
				return
			}

			if reply.Term == term && reply.VoteGranted {
				votes += 1
			}
			if majority <= votes {
				rf.becomeLeaderLocked()
			}
			return

			// Process this reply in the order described below.
		}(peer)
	}
}

// Caller must hold rf.mux.
func (rf *Raft) becomeLeaderLocked() {
	rf.state = leader
	rf.broadcastHeartbeatLocked()
}

// Caller must hold rf.mux. RPC goroutines process replies under the same lock.
func (rf *Raft) broadcastHeartbeatLocked() {
	if rf.stopped || rf.state != leader {
		return
	}
	term := rf.currentTerm
	lastIndex := len(rf.log) - 1
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  rf.log[lastIndex].Term,
		Entries:      nil,
		LeaderCommit: 0, // Real commit tracking comes with log replication.
	}
	rf.heartbeatDeadline = time.Now().Add(heartbeatInterval)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			var reply AppendEntriesReply
			if !rf.sendAppendEntries(server, &args, &reply) {
				return
			}
			rf.mux.Lock()
			defer rf.mux.Unlock()
			if rf.stopped {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.votedFor = -1
				return
			}
			if rf.state != leader || rf.currentTerm != term {
				return
			}
			// Replication progress will be handled in a later step.
		}(peer)
	}
}

func (rf *Raft) electionLoop() {
	for {
		time.Sleep(10 * time.Millisecond)

		rf.mux.Lock()
		if rf.stopped {
			rf.mux.Unlock()
			return
		}

		if rf.state == leader {
			if time.Now().After(rf.heartbeatDeadline) {
				rf.broadcastHeartbeatLocked()
			}
		} else if time.Now().After(rf.electionDeadline) {
			rf.startElectionLocked()
		}

		rf.mux.Unlock()
	}
}

// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock()
	var me int
	var term int
	var isleader bool
	// Your code here (3A)
	me = rf.me
	term = rf.currentTerm
	isleader = (rf.state == leader)
	rf.mux.Unlock()
	return me, term, isleader
}

// RequestVoteArgs
// ===============
//
// # Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B)
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A)
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.stopped {
		return
	}

	if rf.currentTerm > args.Term {
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = follower
	reply.Term = rf.currentTerm
	rf.resetElectionDeadlineLocked()

	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log) {
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	if len(args.Entries) != 0 {
		// TODO: Implement nonempty log replication later.
		return
	}

	reply.Success = true

}

// RequestVote
// ===========
//
// Example RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B)
	rf.mux.Lock()
	defer rf.mux.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1

	}

	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	var upToDate bool

	if lastTerm < args.LastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == lastTerm && lastIndex <= args.LastLogIndex {
		upToDate = true
	}

	reply.Term = rf.currentTerm
	canVote := (rf.votedFor == -1) || (rf.votedFor == args.CandidateID)

	if canVote && upToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.resetElectionDeadlineLocked()
	}

}

// sendRequestVote
// ===============
//
// # Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// # Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// # Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// # If this server is not the leader, return false
//
// # Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// # The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B)

	return index, term, isLeader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	// Your code here, if desired
	rf.mux.Lock()
	rf.stopped = true
	rf.mux.Unlock()
}

// NewPeer
// ====
//
// # The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (3A, 3B)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = follower
	rf.log = []LogEntry{{Term: 0}}

	rf.resetElectionDeadlineLocked()

	go rf.electionLoop()

	return rf
}
