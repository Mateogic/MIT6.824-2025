package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Constants for server states
const (
	Follower = iota
	Candidate
	Leader
)

// Timing constants
const (
	HeartbeatInterval       = 120 * time.Millisecond
	ElectionTimeoutBaseMs   = 400 // Base for election timeout in milliseconds
	ElectionTimeoutRandomMs = 400 // Random addition to election timeout
)

// LogEntry contains command for state machine, and term when entry was received by leader.
// Log entries are 1-indexed from the perspective of the paper, but 0-indexed in our rf.log slice.
// rf.log[0] is a sentinel entry.
type LogEntry struct {
	Term    int
	Command interface{}
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones (paper index)
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex (paper index)
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// For fast log conflict resolution (Lab 3C optimization)
	ConflictTerm  int // term of the conflicting entry
	ConflictIndex int // paper index of first entry with that term, or follower's log length if too short
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries; log[0] is a sentinel entry. Entries are 1-indexed conceptually.

	// Volatile state on all servers
	state       int // Follower, Candidate, or Leader
	commitIndex int // index of highest log entry known to be committed (initialized to 0, paper index)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, paper index)

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (paper index)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (paper index)

	// Channel for sending committed messages to the service/tester
	applyCh chan raftapi.ApplyMsg

	// Election timer related
	electionResetEvent time.Time // Time of the last event that reset the election timer (heartbeat, vote granted, or start of election)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// getLastLogIndex returns the index of the last entry in the log (paper index).
// Log is 0-indexed internally (rf.log), with log[0] as a sentinel.
// Paper's 1-based indexing means last log index is len(rf.log) - 1 if log[0] is sentinel at paper index 0.
func (rf *Raft) getLastLogIndex() int {
	// rf.mu must be held by caller or access must be otherwise safe
	// Since rf.log always contains at least the sentinel, len(rf.log) >= 1.
	// The paper index of the last log entry is len(rf.log) - 1.
	return len(rf.log) - 1
}

// getLastLogTerm returns the term of the last entry in the log.
func (rf *Raft) getLastLogTerm() int {
	// rf.mu must be held by caller or access must be otherwise safe
	lastIdx := rf.getLastLogIndex() // This is a paper index
	// rf.log is 0-indexed, and paper index `i` corresponds to `rf.log[i]`
	// Since lastIdx is len(rf.log) - 1, it's a valid index for rf.log.
	return rf.log[lastIdx].Term
}

// getLogEntryTerm returns the term of the log entry at the given paper index.
// Converts paper index to internal slice index.
// Returns term of sentinel if index is 0.
func (rf *Raft) getLogEntryTerm(paperIndex int) int {
	// rf.mu must be held by caller or access must be otherwise safe
	// paperIndex directly corresponds to rf.log[paperIndex]
	if paperIndex < 0 || paperIndex >= len(rf.log) {
		// This should not happen if logic is correct, especially with sentinel.
		// For safety, if paperIndex is 0 (sentinel) and log exists, return rf.log[0].Term.
		// Otherwise, this indicates an issue. For now, rely on caller to provide valid index.
		// A panic might be appropriate here if index is truly out of bounds for a non-empty log.
		// If paperIndex is 0 (sentinel index) and log is not empty.
		if paperIndex == 0 && len(rf.log) > 0 {
			return rf.log[0].Term
		}
		// Consider what to return for an invalid index. For now, assume valid indices.
		// This path implies an error in logic or state if reached for paperIndex > 0.
		return -1 // Or panic, indicating an invalid state or request
	}
	return rf.log[paperIndex].Term
}

// getLogStartIndex returns the first valid index in the log (paper index of sentinel).
func (rf *Raft) getLogStartIndex() int {
	return 0 // Sentinel is at paper index 0, which is rf.log[0]
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry (paper index)
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// convertToFollower transitions the Raft peer to a follower state.
// rf.mu must be held by the caller.
func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1 // Reset votedFor when term changes or converting to follower
	// rf.persist() // Call persist in Lab 3C
	// DPrintf("[%d] converted to follower in term %d", rf.me, rf.currentTerm)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower.
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		// rf.persist() // Call persist in Lab 3C
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false // Default

	// Grant vote if votedFor is null or candidateId, and candidate's log is at least as up-to-date. (§5.2, §5.4)
	myLastLogTerm := rf.getLastLogTerm()
	myLastLogIndex := rf.getLastLogIndex()

	candidateLogIsUpToDate := false
	if args.LastLogTerm > myLastLogTerm {
		candidateLogIsUpToDate = true
	} else if args.LastLogTerm == myLastLogTerm {
		if args.LastLogIndex >= myLastLogIndex {
			candidateLogIsUpToDate = true
		}
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateLogIsUpToDate {
		rf.votedFor = args.CandidateId
		// rf.persist() // Call persist in Lab 3C
		reply.VoteGranted = true
		rf.electionResetEvent = time.Now() // Reset election timer upon granting vote
		// DPrintf("[%d] Voted for %d in term %d", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		// DPrintf("[%d] Did not vote for %d. MyVotedFor: %d, candidateLogIsUpToDate: %v. MyLastLog(I%d,T%d), CandLastLog(I%d,T%d)",
		//	rf.me, args.CandidateId, rf.votedFor, candidateLogIsUpToDate, myLastLogIndex, myLastLogTerm, args.LastLogIndex, args.LastLogTerm)
	}
}

// AppendEntries RPC arguments structure is defined earlier.
// AppendEntries RPC reply structure is defined earlier.

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		// DPrintf("[%d] AE from %d: Rejected, args.Term %d < rf.currentTerm %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower.
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		// rf.persist() // Call persist in Lab 3C
	}

	// Reset election timer: crucial for followers to not start elections if leader is alive.
	rf.electionResetEvent = time.Now()
	// DPrintf("[%d] AE from %d: Received, reset election timer. MyState: %d, MyTerm: %d, ArgsTerm: %d", rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)

	// If candidate receives AppendEntries from a new leader, it transitions to follower.
	if rf.state == Candidate && args.Term >= rf.currentTerm { // args.Term will be >= rf.currentTerm if we passed the first check or updated term
		rf.convertToFollower(args.Term) // Ensure state is Follower
		// rf.persist() // Call persist in Lab 3C
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// args.PrevLogIndex is a paper index.
	if args.PrevLogIndex < rf.getLogStartIndex() || args.PrevLogIndex > rf.getLastLogIndex() || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// DPrintf("[%d] AE from %d: Log mismatch. PrevLogIndex %d (my last: %d, my sentinel: %d), PrevLogTerm %d. My term at PrevLogIndex: %d",
		//	rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex(), rf.getLogStartIndex(), args.PrevLogTerm, rf.getLogEntryTerm(args.PrevLogIndex))

		reply.Term = rf.currentTerm
		reply.Success = false

		// Optimization for log backtracking (Lab 3C)
		if args.PrevLogIndex > rf.getLastLogIndex() { // Follower's log is too short
			reply.ConflictIndex = rf.getLastLogIndex() + 1
			reply.ConflictTerm = -1 // Using -1 to indicate no specific term, just too short (0 is a valid term for sentinel)
		} else { // Conflict in existing log
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			conflictSearchIndex := args.PrevLogIndex
			// Find the first index in this term (paper index)
			for conflictSearchIndex > rf.getLogStartIndex() && rf.log[conflictSearchIndex-1].Term == reply.ConflictTerm {
				conflictSearchIndex--
			}
			reply.ConflictIndex = conflictSearchIndex
		}
		return
	}

	// For Lab 3A (heartbeats), args.Entries is empty.
	// Log processing (rules 3, 4, 5 from Figure 2) will be for Lab 3B.

	reply.Term = rf.currentTerm
	reply.Success = true
	// DPrintf("[%d] AE from %d: Success for PrevLogIndex %d, PrevLogTerm %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker is the main goroutine that drives Raft's behavior, handling election timeouts and heartbeats.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state

		switch state {
		case Follower, Candidate:
			// Check election timeout
			timeoutDuration := time.Duration(ElectionTimeoutBaseMs+rand.Intn(ElectionTimeoutRandomMs)) * time.Millisecond
			if time.Since(rf.electionResetEvent) > timeoutDuration {
				// DPrintf("[%d] %v: election timeout (since reset: %v > %v), becoming candidate for term %d", rf.me, state, time.Since(rf.electionResetEvent), timeoutDuration, rf.currentTerm+1)
				rf.becomeCandidateLocked() // This function handles its own locking nuances for RPCs
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond) // Poll frequently for timeout

		case Leader:
			// Leader sends heartbeats. sendHeartbeats acquires lock internally.
			rf.mu.Unlock() // Release lock before calling sendHeartbeats
			rf.sendHeartbeats()
			time.Sleep(HeartbeatInterval) // Sleep for the heartbeat interval
		}
	}
}

// becomeCandidateLocked transitions to candidate state and starts an election.
// rf.mu must be held by the caller upon entry.
// This function may unlock and re-lock rf.mu internally when broadcasting RPCs.
func (rf *Raft) becomeCandidateLocked() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me // Vote for self
	rf.electionResetEvent = time.Now() // Reset election timer for this new election round
	// rf.persist() // Call persist in Lab 3C
	// DPrintf("[%d] became candidate for term %d. LastLog(I%d, T%d)", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	currentTermWhenStarted := rf.currentTerm // Capture current term for goroutines

	rf.mu.Unlock() // Unlock before broadcasting, as sendRequestVote might block and RPC handlers lock
	rf.broadcastRequestVote(currentTermWhenStarted, args)
	rf.mu.Lock()   // Re-acquire lock as the main ticker loop expects it to be held after this call if it was held before
}

// broadcastRequestVote sends RequestVote RPCs to all other peers.
// Does not hold rf.mu itself, but callbacks (RPC handlers) will acquire it.
func (rf *Raft) broadcastRequestVote(electionTerm int, args RequestVoteArgs) {
	// DPrintf("[%d] broadcasting RequestVote for term %d", rf.me, electionTerm)
	var votesReceivedAtomic int32 = 1 // Start with 1 for self-vote
	numPeers := len(rf.peers)

	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}

		go func(server int, rpcArgs RequestVoteArgs) { // Pass args by value
			reply := RequestVoteReply{}
			// DPrintf("[%d] sending RequestVote to %d for term %d", rpcArgs.CandidateId, server, rpcArgs.Term)
			ok := rf.sendRequestVote(server, &rpcArgs, &reply) // sendRequestVote is from skeleton

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok {
				// DPrintf("[%d] RequestVote to %d failed or timed out for term %d", rpcArgs.CandidateId, server, rpcArgs.Term)
				return
			}

			// Check if still candidate and in the same election term
			if rf.state != Candidate || rf.currentTerm != electionTerm {
				// DPrintf("[%d] RV reply from %d: No longer candidate or term changed (state %d, myTerm %d, electionTerm %d -> replyTerm %d)",
				// rpcArgs.CandidateId, server, rf.state, rf.currentTerm, electionTerm, reply.Term)
				return
			}

			if reply.Term > rf.currentTerm {
				// DPrintf("[%d] RV reply from %d: Their term %d > my term %d. Converting to follower.",
				//	rpcArgs.CandidateId, server, reply.Term, rf.currentTerm)
				rf.convertToFollower(reply.Term)
				// rf.persist() // Call persist in Lab 3C
				return
			}

			if reply.VoteGranted {
				// DPrintf("[%d] RV reply from %d: Vote granted for term %d", rpcArgs.CandidateId, server, rpcArgs.Term)
				newCount := atomic.AddInt32(&votesReceivedAtomic, 1)
				// DPrintf("[%d] Votes received: %d for term %d (need > %d)", rpcArgs.CandidateId, newCount, rf.currentTerm, numPeers/2)
				if newCount > int32(numPeers/2) {
					// Won election
					if rf.state == Candidate && rf.currentTerm == electionTerm { // Double check state and term
						// DPrintf("[%d] Won election for term %d with %d votes. Becoming leader.", rpcArgs.CandidateId, rf.currentTerm, newCount)
						rf.becomeLeaderLocked()
					}
				}
			}
		}(serverId, args)
	}
}

// becomeLeaderLocked transitions to leader state.
// rf.mu must be held by the caller.
func (rf *Raft) becomeLeaderLocked() {
	if rf.state != Candidate { // Should only transition from Candidate
		// DPrintf("[%d] Attempted to become leader from state %d (not Candidate) in term %d", rf.me, rf.state, rf.currentTerm)
		return
	}
	// DPrintf("[%d] transitioning to Leader in term %d", rf.me, rf.currentTerm)
	rf.state = Leader

	// Initialize nextIndex and matchIndex for all followers (Figure 2)
	lastLogIdx := rf.getLastLogIndex() // This is a paper index
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIdx + 1 // Paper index
		rf.matchIndex[i] = 0             // Paper index, initialized to 0
	}

	// Send initial heartbeats immediately. The main ticker loop for Leader will also send heartbeats periodically.
	// To ensure quick assertion of leadership, we can call sendHeartbeats here.
	// sendHeartbeats will acquire its own lock, so release current lock before calling if it's not designed to be re-entrant or expect lock.
	// However, sendHeartbeats is designed to be called and it will manage its lock.
	// For safety, and because sendHeartbeats will take the lock, it's better if this function doesn't hold it across that call if sendHeartbeats is substantial.
	// Given sendHeartbeats spawns goroutines, it's fine to call it while holding the lock here, as the lock is primarily for state transition.
	// The DPrintf for becoming leader is already printed. We can let the ticker loop send the first heartbeat.
	// Or, to be explicit and immediate:
	rf.mu.Unlock() // sendHeartbeats will re-acquire
	rf.sendHeartbeats() // Send initial heartbeats
	rf.mu.Lock()   // Re-acquire lock as expected by caller (e.g. broadcastRequestVote's goroutine)
}

// sendHeartbeats sends AppendEntries RPCs (heartbeats) to all peers.
// This function acquires and releases rf.mu internally.
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex // For Lab 3B

	// DPrintf("[%d] Leader in term %d sending heartbeats/AE. CommitIdx: %d", rf.me, currentTerm, leaderCommit)

	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}

		// prevLogIndex is a paper index. rf.nextIndex stores paper indices.
		prevLogIndex := rf.nextIndex[serverId] - 1
		// Ensure prevLogIndex is valid. If nextIndex[i] is 1 (logStartIndex + 1), then prevLogIndex is 0 (logStartIndex).
		if prevLogIndex < rf.getLogStartIndex() { // Sentinel index is getLogStartIndex() (0)
			prevLogIndex = rf.getLogStartIndex() // Correctly point to sentinel if needed
		}
		prevLogTerm := rf.log[prevLogIndex].Term // rf.log is 0-indexed, prevLogIndex is paper index

		// For heartbeats, Entries is empty. For Lab 3B, this will contain actual log entries.
		entriesToSend := []LogEntry{}

		// Lab 3B: If there are entries to send for this follower:
		// lastLogIndex := rf.getLastLogIndex()
		// if rf.nextIndex[serverId] <= lastLogIndex {
		// 	 entriesToSend = rf.log[rf.nextIndex[serverId] : lastLogIndex+1] // Slicing rf.log with paper indices
		// }

		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: leaderCommit,
		}

		// Spawn a goroutine to send RPC and handle reply, to avoid blocking the leader's main loop.
		go func(server int, rpcArgs AppendEntriesArgs) { // Pass args by value
			reply := AppendEntriesReply{}
			// DPrintf("[%d] sending AE to %d for T%d. PrevLog(I%d,T%d). Entries: %d. LeaderCommit: %d",
			//	rpcArgs.LeaderId, server, rpcArgs.Term, rpcArgs.PrevLogIndex, rpcArgs.PrevLogTerm, len(rpcArgs.Entries), rpcArgs.LeaderCommit)
			ok := rf.peers[server].Call("Raft.AppendEntries", &rpcArgs, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok {
				// DPrintf("[%d] AE to %d failed or timed out for T%d", rpcArgs.LeaderId, server, rpcArgs.Term)
				return
			}

			// Check if still leader and in the same term; otherwise, result is stale.
			if rf.state != Leader || rf.currentTerm != rpcArgs.Term {
				// DPrintf("[%d] AE reply from %d: No longer leader or term changed (state %d, myTerm %d, args.Term %d -> replyTerm %d)",
				//	rpcArgs.LeaderId, server, rf.state, rf.currentTerm, rpcArgs.Term, reply.Term)
				return
			}

			if reply.Term > rf.currentTerm {
				// DPrintf("[%d] AE reply from %d: Their term %d > my term %d. Converting to follower.",
				//	rpcArgs.LeaderId, server, reply.Term, rf.currentTerm)
				rf.convertToFollower(reply.Term)
				// rf.persist() // Call persist in Lab 3C
				return
			}

			if reply.Success {
				// Heartbeat (or log entries if any) was successful.
				// Update nextIndex and matchIndex for this follower.
				// DPrintf("[%d] AE to %d successful for T%d. PrevLogIndex %d, len(Entries) %d",
				//	rpcArgs.LeaderId, server, rpcArgs.Term, rpcArgs.PrevLogIndex, len(rpcArgs.Entries))
				rf.matchIndex[server] = rpcArgs.PrevLogIndex + len(rpcArgs.Entries) // Paper index
				rf.nextIndex[server] = rf.matchIndex[server] + 1                   // Paper index

				// Lab 3B: Update commitIndex if a majority of followers have replicated up to a certain point in the current term.
				// rf.updateCommitIndexLocked()
			} else {
				// Follower rejected. Decrement nextIndex and retry. (Lab 3B/3C)
				// DPrintf("[%d] AE to %d FAILED for T%d. Reply Term: %d. Conflict(T%d, I%d)",
				//	rpcArgs.LeaderId, server, rpcArgs.Term, reply.Term, reply.ConflictTerm, reply.ConflictIndex)

				// Use conflict info to backtrack nextIndex more efficiently (Lab 3C)
				if reply.ConflictTerm == -1 { // Follower's log is too short, ConflictIndex is follower's log length + 1
					rf.nextIndex[server] = reply.ConflictIndex // Paper index
				} else {
					// Search leader's log for the last entry with ConflictTerm (paper index)
					leaderLastIndexWithConflictTerm := -1
					for i := rf.getLastLogIndex(); i >= rf.getLogStartIndex(); i-- { // Iterate paper indices
						if rf.log[i].Term == reply.ConflictTerm {
							leaderLastIndexWithConflictTerm = i
							break
						}
					}

					if leaderLastIndexWithConflictTerm != -1 {
						// Leader has ConflictTerm. Set nextIndex to leader's entry after that term.
						rf.nextIndex[server] = leaderLastIndexWithConflictTerm + 1 // Paper index
					} else {
						// Leader does not have ConflictTerm. Set nextIndex to follower's conflicting entry.
						rf.nextIndex[server] = reply.ConflictIndex // Paper index
					}
				}
				// Ensure nextIndex does not go below sentinel_index + 1 (paper index 1)
				if rf.nextIndex[server] <= rf.getLogStartIndex() {
					rf.nextIndex[server] = rf.getLogStartIndex() + 1
				}
				// DPrintf("[%d] Adjusted nextIndex for server %d to %d", rpcArgs.LeaderId, server, rf.nextIndex[server])
			}
		}(serverId, args)
	}
	rf.mu.Unlock() // Unlock after iterating through peers and spawning goroutines
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh // Store applyCh

	// Your initialization code here (3A, 3B, 3C).
	// --- BEGIN INITIALIZATION ---
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // Initialize with one sentinel entry at paper index 0
	rf.log[0] = LogEntry{Term: 0, Command: nil} // Sentinel entry

	rf.commitIndex = 0 // Paper index
	rf.lastApplied = 0 // Paper index

	rf.state = Follower
	rf.electionResetEvent = time.Now() // Initialize election timer reset event

	// Initialize volatile state on leaders (reinitialized after election, but good to init here)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// Initial values for nextIndex and matchIndex will be set when a server becomes leader.
	// For now, zero values are fine. Leader will set nextIndex to its last log index + 1.
	// --- END INITIALIZATION ---

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
