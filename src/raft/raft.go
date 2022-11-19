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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"math/rand"

	//	"6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// election timeout and sleep range in ms
const ELECTION_TIMEOUT = 1000
const MIN_SLEEP_INTERVAL = 1000
const MAX_SLEEP_INTERVAL = 1500
const HEARTBEAT_INTERVAL = 110 //send heartbeat RPCs no more than ten times per second.

type State int
const (
	Follower State = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command	     interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct{
	// command for state machine
	// term when entry was received by leader (first index is 1)
	Command  interface{}
	TermReceivedByLeader int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32	              // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state                  State
	lastHeartbeatTimestamp time.Time

	// persistent state
	currentTerm int
	votedFor    int
	log	        []LogEntry
	// volatile state
	commitIndex int
	lastApplied int
	// leaders
	nextIndex   []int
	matchIndex  []int

	appendEntriesStepDownChannel chan bool

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.state == Leader
	// Your code here (2A).
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term	     int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// returns index and term of last log entry
// or (-1, -1) if log is empty
// assumes caller acquired lock on rf
func (rf *Raft) GetLastLogEntryInfo() (int, int) {
	var lastLogIdx, lastLogTerm int
	if (len(rf.log) > 0) {
		lastLogIdx = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIdx].TermReceivedByLeader
	} else {
		lastLogIdx = -1
		lastLogTerm = -1
	}
	return lastLogIdx, lastLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("server %d received request to grant vote to %d\n", rf.me, args.CandidateId)
	reply.VoteGranted = false // default

	// -------------------------------------------------------------------------------- Rules for all servers
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term > currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	//  ------------------------------------------------------------------------------- RequestVote Receiver implementation (figure 2)

	// (1) don't grant if term < currentTerm (§5.1)
	if args.Term < currentTerm{
		DPrintf("rf %d is NOT granting vote to %d (bc of current term)\n", rf.me, args.CandidateId)
		return
	}

	// (2) If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4, last paragraph of 5.4.1)
	haventVotedSomeoneElse := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if !haventVotedSomeoneElse{
		DPrintf("rf %d is NOT granting vote to %d (voted for %d)\n", rf.me, args.CandidateId, rf.votedFor)
	}

	myLastLogIdx, myLastLogTerm := rf.GetLastLogEntryInfo()

	candidateLastLogIdx := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm

	candidateLogNotBehind := (candidateLastLogTerm > myLastLogTerm) ||
		((candidateLastLogTerm == myLastLogTerm) && candidateLastLogIdx >= myLastLogIdx)

	if !candidateLogNotBehind{
		DPrintf("rf %d is NOT granting vote to %d (log is behind)\n", rf.me, args.CandidateId)
	}

	if (candidateLogNotBehind && haventVotedSomeoneElse) {
		DPrintf("rf %d is granting vote to %d\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

//
// Append Entries args and reply structures
//
type AppendEntriesArgs  struct {
	Term		 int
	LeaderId	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct{
	Term	 int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// -------------------------------------------------------------------------------- Rules for all servers
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term > currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// If the leader’s term (included in its RPC) is at least as large as the candidate’s current term
	// then the candidate recognizes the leader as legitimate and returns to follower state
	if rf.state == Candidate && args.Term >= currentTerm{
		DPrintf("candidate %d (term %d) got heartbeat from leader %d (term %d): step down\n", rf.me, currentTerm, args.LeaderId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.appendEntriesStepDownChannel <- true
	}

	//  ------------------------------------------------------------------------------- AppendEntries Receiver implementation (figure 2)
	//  1. Reply false if term < currentTerm (5.1)
	if args.Term < currentTerm{
		reply.Success = false
		return
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	//    whose term matches prevLogTerm (5.3)
	reply.Success = true
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	//    delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	rf.lastHeartbeatTimestamp = time.Now()
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
	// DPrintf("rf %d is making RPC request vote call to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		amLeader := rf.state == Leader
		if !amLeader && time.Since(rf.lastHeartbeatTimestamp).Milliseconds() > ELECTION_TIMEOUT{
			// convert to candidate and start election
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.lastHeartbeatTimestamp = time.Now()
			DPrintf("rf %d converted to candidate, new term is %d \n", rf.me, rf.currentTerm)
			// args for RequestVote RPC
			lastLogIdx, lastLogTerm := rf.GetLastLogEntryInfo()
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = lastLogIdx
			args.LastLogTerm = lastLogTerm
			// request from all other servers: release lock while sending and waiting for RPC
			numServers := len(rf.peers)
			voteChannel := make(chan bool)
			giveUpChannel := make(chan bool)
			rf.mu.Unlock()
			for server := 0; server < numServers; server++ {
				if (server == rf.me) {
					continue
				}
				reply := RequestVoteReply{}
				go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
					rf.sendRequestVote(server, &args, &reply)
					voteChannel <- reply.VoteGranted
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						DPrintf("candidate %d (term %d) send RequestVoteRPC to server %d (term %d): step down\n", rf.me, rf.currentTerm, server, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						giveUpChannel <- true

					}
					rf.mu.Unlock()
				}(server, args, reply)
			}
			// listen on voteChannel for votes and giveUpChannel for timeout or new leaders
			resultsReceived := 0
			votesWon := 1 // self-vote
			go func() {
				time.Sleep(ELECTION_TIMEOUT * time.Millisecond)
				giveUpChannel <- true
			}()
			giveUp := false
			for (resultsReceived < len(rf.peers) - 1 && votesWon <= len(rf.peers)/2 && !giveUp){
				select {
				case newResult := <-voteChannel:
					resultsReceived += 1
					if newResult {
						votesWon += 1
					}
				case <-giveUpChannel:
					giveUp = true
				case <- rf.appendEntriesStepDownChannel:
					giveUp = true
				}
			}
			// see if i won or lost election
			rf.mu.Lock()
			// DPrintf("candidate %d has %d votes\n", rf.me, votesWon)
			if votesWon > len(rf.peers) / 2 {
				DPrintf("candidate %d won, its term is %d\n", rf.me, rf.currentTerm)
				rf.state = Leader
				// release lock, send everyone heartbeats right after winning, reclaim lock
				rf.mu.Unlock()
				rf.sendHeartbeats()
				rf.mu.Lock()
			} else{
				DPrintf("candidate %d lost\n", rf.me)
				rf.state = Follower
				rf.votedFor = -1
			}
		}
		rf.mu.Unlock()
		randomSleep := rand.Intn(MAX_SLEEP_INTERVAL - MIN_SLEEP_INTERVAL) + MIN_SLEEP_INTERVAL
		time.Sleep(time.Duration(randomSleep) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeats() {
	// send heartbeats to all other servers
	// called from: (1) long-running sendHeartbeats goroutine
	//              (2) after a new leader wins election
	// NOTE: don't hold rf lock before calling this helper fn
	numServers := len(rf.peers)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := 0; server < numServers; server++ {
		if (server == rf.me) {
			continue
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		reply := AppendEntriesReply{}
		go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
			// DPrintf("rf %d is sending heartbeat to %d\n", rf.me, server)
			rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
			}
			rf.mu.Unlock()
		}(server, args, reply)
	}
}

//
// long running goroutine, for leader to send heartbeats
//
func (rf *Raft) heartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		amLeader := rf.state == Leader
		if amLeader {
			rf.sendHeartbeats()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
	}
}



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
	rf.state = Follower

	rf.votedFor = -1
	rf.lastHeartbeatTimestamp = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.appendEntriesStepDownChannel = make(chan bool)


	// start ticker goroutine to start elections
	go rf.ticker()
	// start heartbeat goroutine
	go rf.heartbeats()


	return rf
}
