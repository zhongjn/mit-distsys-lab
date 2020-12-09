package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// ApplyMsg struct.
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type persistLogEntry struct {
	Term    int
	Command interface{}
}

type role int

const (
	roleFollower role = iota
	roleCandidate
	roleLeader
)

const (
	heartbeatPeriod    time.Duration = 125 * time.Millisecond
	minElectionTimeout time.Duration = 300 * time.Millisecond
	maxElectionTimeout time.Duration = 500 * time.Millisecond
)

//
// Raft struct implementing a single Raft peer.
//
type Raft struct {
	mu        Mutex               // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	killed bool

	// persistent state
	currentTerm int
	votedFor    int // -1 if none
	log         []persistLogEntry

	// volatile state
	role                  role
	electionTime          time.Time
	electionTimeValid     bool
	electionTimerEnable   sync.Cond
	lastLeaderMessageTime time.Time
	commitIndex           int
	lastApplied           int

	// volatile state (leader only)
	nextIndex  []int
	matchIndex []int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == roleLeader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// TODO: persist snapshot
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// RequestVoteArgs is RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply is RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// if term is higher, update currentTerm
// rf.mu must be holding
func (rf *Raft) updateTerm(term int) {
	rf.mu.AssertHeld()
	if term > rf.currentTerm {
		DPrintf("#%d: updating term from %d to %d", rf.me, rf.currentTerm, term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.role = roleFollower
	}
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// current leader exists?
	tLastLeader := time.Now().Sub(rf.lastLeaderMessageTime)
	if tLastLeader < minElectionTimeout {
		// no need to persist anything
		// since currentTerm is not affected
		goto notGrant
	}

	rf.updateTerm(args.Term)

	if args.Term < rf.currentTerm {
		rf.persist()
		goto notGrant
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		rf.persist()

		// grant vote
		*reply = RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: true,
		}
		return
	}

notGrant:
	*reply = RequestVoteReply{
		Term:        rf.currentTerm,
		VoteGranted: false,
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntriesArgs RPC structure
//
type AppendEntriesArgs struct {
	Term          int
	LeaderID      int
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []interface{}
	LearderCommit int
}

//
// AppendEntriesReply RPC structure
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)

	if args.Term < rf.currentTerm {
		rf.persist()
		*reply = AppendEntriesReply{
			Success: false,
			Term:    rf.currentTerm,
		}
		return
	}

	// NOTE:
	// term must be equal from here

	if rf.role == roleLeader {
		panic("two leader in the same term!")
	}

	if rf.role == roleCandidate {
		rf.role = roleFollower
		DPrintf("#%d: converting to follower", rf.me)
	}

	DPrintf("#%d: reset election timer, leader=%d", rf.me, args.LeaderID)
	rf.resetElectionTimer()
	rf.lastLeaderMessageTime = time.Now()

	// TODO: actual log appending

	rf.persist()
	*reply = AppendEntriesReply{
		Success: true,
		Term:    rf.currentTerm,
	}
}

func (rf *Raft) doAllAppendEntries() {
	rf.mu.AssertHeld()
	prevTerm := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				// TODO: figure out which log entris should we send
				args := AppendEntriesArgs{
					Term:          rf.currentTerm,
					LeaderID:      rf.me,
					PrevLogIndex:  -1,
					PrevLogTerm:   0,
					Entries:       nil,
					LearderCommit: rf.commitIndex,
				}
				var reply AppendEntriesReply
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(i, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !ok {
					DPrintf("#%d: AppendEntries RPC to peer #%d failed", rf.me, i)
					return
				}

				rf.updateTerm(reply.Term)

				if rf.currentTerm != prevTerm {
					return
				}

				// NOTE:
				// since the term equals, we are still the leader

				if reply.Success {
					// TODO: increment next index & match index
				} else {
					// TODO: decrement next index
				}
			}(i)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) onElectionTimeout() {
	rf.mu.AssertHeld()

	voteCount := 1 // vote for self

	rf.role = roleCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	DPrintf("#%d: starting election, term=%d", rf.me, rf.currentTerm)

	prevTerm := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()

				lastLogIndex := len(rf.log) - 1
				lastLogTerm := 0
				if lastLogIndex >= 0 {
					lastLogTerm = rf.log[lastLogIndex].Term
				}

				args := RequestVoteArgs{
					CandidateID:  rf.me,
					LastLogTerm:  lastLogTerm,
					LastLogIndex: lastLogIndex,
					Term:         rf.currentTerm,
				}
				var reply RequestVoteReply
				rf.mu.Unlock()
				ok := rf.sendRequestVote(i, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !ok {
					DPrintf("#%d: RequestVote RPC to peer #%d failed", rf.me, i)
					return
				}

				rf.updateTerm(reply.Term)

				// might already be leader or follower
				if rf.role != roleCandidate {
					return
				}

				// same term as we request vote?
				if rf.currentTerm != prevTerm {
					return
				}

				// is vote granted?
				if !reply.VoteGranted {
					return
				}

				voteCount++
				if voteCount > len(rf.peers)/2 {
					DPrintf("$%d: won election, term=%d", rf.me, rf.currentTerm)
					rf.role = roleLeader
					rf.electionTimeValid = false // cancel election timer
					rf.startHeartbeatWorker()
				}
			}(i)
		}
	}
}

func (rf *Raft) startHeartbeatWorker() {
	rf.mu.AssertHeld()
	startTerm := rf.currentTerm
	go func() {
		for {
			rf.mu.Lock()
			if rf.killed || rf.currentTerm != startTerm {
				rf.mu.Unlock()
				return
			}

			DPrintf("#%d: sending heartbeat", rf.me)
			rf.doAllAppendEntries()

			rf.mu.Unlock()
			time.Sleep(heartbeatPeriod)
		}
	}()
}

func (rf *Raft) startElectionTimerWorker() {
	go func() {
		for {
			rf.mu.Lock()

			for !rf.electionTimeValid && !rf.killed {
				rf.electionTimerEnable.Wait()
			}

			if rf.killed {
				rf.mu.Unlock()
				return
			}

			sleep := rf.electionTime.Sub(time.Now())

			if sleep > 0 {
				rf.mu.Unlock()
				time.Sleep(sleep)
			} else {
				rf.electionTimeValid = false
				rf.onElectionTimeout()
				rf.mu.Unlock()
			}
		}
	}()
}

// reset the election timer (randomized)
// rf.mu must be held
func (rf *Raft) resetElectionTimer() {
	rf.mu.AssertHeld()
	low, high := int64(minElectionTimeout), int64(maxElectionTimeout)
	t := rand.Int63()%(high-low) + low
	rf.electionTime = time.Now().Add(time.Duration(t))
	rf.electionTimeValid = true

	// notify election timer thread
	rf.electionTimerEnable.Broadcast()
}

//
// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// Kill the raft instance.
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.electionTimerEnable.Broadcast()
	rf.killed = true
	rf.mu.Unlock()
}

//
// Make a new Raft instance.
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = roleFollower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimeValid = false
	rf.electionTimerEnable = sync.Cond{L: &rf.mu}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.startElectionTimerWorker()

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	return rf
}
