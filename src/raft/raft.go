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
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
	"util"
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
	CommandTerm  int
}

type logEntry struct {
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
	minElectionTimeout time.Duration = 500 * time.Millisecond
	maxElectionTimeout time.Duration = 1000 * time.Millisecond
)

//
// Raft struct implementing a single Raft peer.
//
type Raft struct {
	mu        util.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg
	killed  bool

	// persistent state
	currentTerm int
	votedFor    int        // -1 if none
	log         []logEntry // physical log
	logOffset   int        // offset between virtual log & physical (truncated) log

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

	// persist snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {

		panic("read persist error")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
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

func (rf *Raft) logIndexP2V(physical int) int {
	rf.mu.AssertHeld()
	return physical + rf.logOffset
}

func (rf *Raft) logIndexV2P(virtual int) int {
	rf.mu.AssertHeld()

	physical := virtual - rf.logOffset
	if physical < 0 || physical >= len(rf.log) {
		log.Panicf("physical log index %d out of bound [0,%d)", physical, len(rf.log))
	}

	return physical
}

func (rf *Raft) logVirtualLength() int {
	rf.mu.AssertHeld()
	return len(rf.log) + rf.logOffset
}

// if term is higher, update currentTerm
// rf.mu must be holding
func (rf *Raft) updateTerm(term int) {
	rf.mu.AssertHeld()
	if term > rf.currentTerm {
		DPrintf("#%d: updating term from %d to %d", rf.me, rf.currentTerm, term)
		rf.currentTerm = term
		rf.votedFor = -1
		// if leader step down, restart election timer
		if rf.role == roleLeader {
			rf.resetElectionTimer()
		}
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

	persist := true

	// current leader exists?
	tLastLeader := time.Now().Sub(rf.lastLeaderMessageTime)
	if tLastLeader < minElectionTimeout {
		// no need to persist anything
		// since currentTerm is not affected
		persist = false
		goto notGrant
	}

	rf.updateTerm(args.Term)

	if args.Term < rf.currentTerm {
		goto notGrant
	}

	if !(rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		goto notGrant
	}

	// election restriction
	{
		lastLogIndex := rf.logVirtualLength() - 1
		lastLogTerm := rf.log[rf.logIndexV2P(lastLogIndex)].Term
		DPrintf("#%d: self term[%d]=%d, candidate #%d with term[%d]=%d",
			rf.me, lastLogIndex, lastLogTerm, args.CandidateID, args.LastLogIndex, args.LastLogTerm)

		// is candidate at least up-to-date as self?
		candidateUpToDate :=
			args.LastLogTerm > lastLogTerm ||
				(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		if !candidateUpToDate {
			goto notGrant
		}
	}

	rf.votedFor = args.CandidateID
	rf.persist()

	// grant vote
	DPrintf("#%d: vote granted to #%d", rf.me, args.CandidateID)
	*reply = RequestVoteReply{
		Term:        rf.currentTerm,
		VoteGranted: true,
	}
	return

notGrant:
	if persist {
		rf.persist()
	}

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
	Entries       []logEntry
	LearderCommit int
}

//
// AppendEntriesReply RPC structure
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)

	success := false
	conflictIndex := -1
	conflictTerm := -1

	if args.Term < rf.currentTerm {
		goto notSuccess
	}

	// NOTE:
	// term must be equal from here

	util.Assert(rf.role != roleLeader, "two leader in the same term!")

	if rf.role == roleCandidate {
		rf.role = roleFollower
		DPrintf("#%d: converting to follower", rf.me)
	}

	rf.resetElectionTimer()
	rf.lastLeaderMessageTime = time.Now()

	if args.PrevLogIndex >= rf.logVirtualLength() {
		conflictIndex = rf.logVirtualLength()
		goto notSuccess
	}

	{
		prevLogActualTerm := rf.log[rf.logIndexV2P(args.PrevLogIndex)].Term

		if args.PrevLogTerm != prevLogActualTerm {
			conflictTerm = prevLogActualTerm
			tBegin, _, ok := rf.findRangeOfTerm(prevLogActualTerm)
			util.Assert(ok, "term should be found")

			conflictIndex = tBegin
			goto notSuccess
		}

		// index (in raft log) of first log entry in AppendEntries request
		insertIndex := args.PrevLogIndex + 1

		// for existing entries (overlapping part of log & request)
		// i is the index in AppendEntries request
		conflictCheckLen := util.Min(rf.logVirtualLength()-insertIndex, len(args.Entries))
		for i := 0; i < conflictCheckLen; i++ {
			raftIndex := i + insertIndex
			existingTerm := rf.log[rf.logIndexV2P(raftIndex)].Term
			curTerm := args.Entries[i].Term

			// term conflict?
			if curTerm != existingTerm {
				// truncate log
				rf.log = rf.log[0:rf.logIndexV2P(raftIndex)]
				break
			}
		}

		// for entries that need actual append
		// i is the index in AppendEntries request
		for i := rf.logVirtualLength() - insertIndex; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
	}

	if args.LearderCommit > rf.commitIndex {
		rf.commitIndex = util.Min(args.LearderCommit, rf.logVirtualLength()-1)
		rf.notifyCommandApplied()
	}

	DPrintf("#%d: AppendEntries done, leader=%d, prevIndex=%d, len=%d, commitIndex=%d",
		rf.me, args.LeaderID, args.PrevLogIndex, len(args.Entries), rf.commitIndex)

	// append entry succeeded
	success = true

notSuccess:
	rf.persist()
	*reply = AppendEntriesReply{
		Success:       success,
		Term:          rf.currentTerm,
		ConflictIndex: conflictIndex,
		ConflictTerm:  conflictTerm,
	}
}

// NOTE: begin is inclusive, end is exclusive
func (rf *Raft) findRangeOfTerm(term int) (begin, end int, ok bool) {
	rf.mu.AssertHeld()

	// TODO: faster (e.g. binary search)
	for i, e := range rf.log {
		if e.Term == term {
			begin = i

			var j int
			for j = begin; j < rf.logVirtualLength(); j++ {
				if rf.log[rf.logIndexV2P(j)].Term != e.Term {
					break
				}
			}
			end = j

			ok = true
			return
		}
	}

	return -1, -1, false
}

func (rf *Raft) assertLeader() {
	rf.mu.AssertHeld()
	util.Assert(rf.role == roleLeader, "not leader")
}

// notify command is applied
func (rf *Raft) notifyCommandApplied() {
	DPrintf("#%d: command applied, commitIndex=%d", rf.me, rf.commitIndex)

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				CommandTerm:  rf.log[rf.logIndexV2P(i)].Term,
				Command:      rf.log[rf.logIndexV2P(i)].Command,
			}
		}
		rf.lastApplied = rf.commitIndex
	}()
}

// advance commit index based on follower's matchIndex
func (rf *Raft) leaderAdvanceCommitIndex() {
	rf.mu.AssertHeld()
	rf.assertLeader()

	followerIndexArr := make([]int, len(rf.peers))
	copy(followerIndexArr, rf.matchIndex)
	// fix the matchIndex hole for leader itself
	followerIndexArr[rf.me] = rf.logVirtualLength() - 1
	DPrintf("#%d: all commit index %v", rf.me, followerIndexArr)

	// sort in increasing order to check majority
	sort.Ints(followerIndexArr)
	majorityIndex := followerIndexArr[len(rf.peers)/2]

	if majorityIndex > rf.commitIndex &&
		rf.log[rf.logIndexV2P(majorityIndex)].Term == rf.currentTerm {

		rf.commitIndex = majorityIndex
		rf.notifyCommandApplied()
	}
}

func (rf *Raft) leaderBroadcastAppendEntries() {
	rf.mu.AssertHeld()
	rf.assertLeader()
	prevTerm := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				startIndex := rf.nextIndex[i]     // inclusive
				endIndex := rf.logVirtualLength() // exclusive

				// update nextIndex eagerly
				// because packet loss & reorder is relatively rare
				rf.nextIndex[i] = endIndex

				args := AppendEntriesArgs{
					Term:          rf.currentTerm,
					LeaderID:      rf.me,
					PrevLogIndex:  startIndex - 1,
					PrevLogTerm:   rf.log[rf.logIndexV2P(startIndex-1)].Term,
					Entries:       rf.log[rf.logIndexV2P(startIndex):rf.logIndexV2P(endIndex)],
					LearderCommit: rf.commitIndex,
				}
				var reply AppendEntriesReply
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(i, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.killed {
					return
				}

				if !ok {
					// DPrintf("#%d: AppendEntries RPC to peer #%d failed", rf.me, i)
					return
				}

				rf.updateTerm(reply.Term)

				if rf.currentTerm != prevTerm {
					return
				}

				// NOTE:
				// since the term equals, we are still the leader

				if reply.Success {
					// check match index monotonicity
					// discard old packet
					if endIndex-1 > rf.matchIndex[i] {
						// increment next index & match index
						rf.matchIndex[i] = endIndex - 1
						rf.nextIndex[i] = endIndex
						rf.leaderAdvanceCommitIndex()
					}
				} else {
					// decrement next index
					_, tEnd, ok := rf.findRangeOfTerm(reply.ConflictTerm)
					if !ok {
						rf.nextIndex[i] = util.Max(reply.ConflictIndex, rf.matchIndex[i]+1)
					} else {
						rf.nextIndex[i] = util.Max(tEnd, rf.matchIndex[i]+1)
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) candidateWonElection() {
	rf.mu.AssertHeld()
	util.Assert(rf.role == roleCandidate, "not candidate")

	DPrintf("$%d: won election, term=%d", rf.me, rf.currentTerm)
	rf.role = roleLeader
	rf.electionTimeValid = false // cancel election timer
	rf.startHeartbeatWorker()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.logVirtualLength()
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) onElectionTimeout() {
	rf.mu.AssertHeld()

	voteCount := 1 // vote for self

	rf.role = roleCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.persist()

	DPrintf("#%d: starting election, term=%d", rf.me, rf.currentTerm)

	prevTerm := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()

				lastLogIndex := rf.logVirtualLength() - 1
				lastLogTerm := rf.log[rf.logIndexV2P(lastLogIndex)].Term

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

				if rf.killed {
					return
				}

				if !ok {
					// DPrintf("#%d: RequestVote RPC to peer #%d failed", rf.me, i)
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

				DPrintf("#%d: received vote from #%d", rf.me, i)

				voteCount++
				if voteCount > len(rf.peers)/2 {
					rf.candidateWonElection()
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
			rf.leaderBroadcastAppendEntries()

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

// GetStateSize method gets the current persisted state (log) size.
func (rf *Raft) GetStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// UpdateSnapshot method updates the snapshot in raft.
// term & index refer to the last included log entry in the snapshot.
func (rf *Raft) UpdateSnapshot(term int, index int, snapshot interface{}) {
	// TODO: update snapshot
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != roleLeader {
		isLeader = false
		goto exit
	}

	index = rf.logVirtualLength()
	term = rf.currentTerm

	rf.log = append(rf.log, logEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.persist()
	rf.leaderBroadcastAppendEntries()

exit:
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
	DPrintf("#%d: killing", rf.me)

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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.role = roleFollower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimeValid = false
	rf.electionTimerEnable = sync.Cond{L: &rf.mu}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logOffset = 0
	rf.log = make([]logEntry, 1)
	rf.log[0] = logEntry{
		Command: nil,
		Term:    0,
	}

	// NOTE:
	// nextIndex[], matchIndex[] fields is
	// initialized in candidateWonElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// NOTE:
	// Raft struct is initialized
	// Need mutex to protect access from now on

	rf.startElectionTimerWorker()

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	return rf
}
