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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type State int
type Entry struct {
	Command interface{}
	Term    int
}

const (
	Follower  State = 0
	Candidate       = 1
	Leader          = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      *int
	state         State
	heartbeatDue  time.Time
	heartbeatTime time.Time
	log           []Entry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	applyCh       *chan ApplyMsg
	applyCond     *sync.Cond
}

func getEletionTimeout() time.Duration {
	return time.Duration(350+rand.Intn(500)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d[term %d] received RequestVote from server %d[term %d]", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	rf.heartbeatDue = time.Now().Add(getEletionTimeout())
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		DPrintf("\033[1;31;40mServer %d[term %d] downgrad to follower\033[0m", rf.me, rf.currentTerm)
		rf.becomesFollower()
	} else if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Server %d[term %d] received RequestVote from server %d[term %d] done, success = %v", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
		return
	}
	reply.Term = rf.currentTerm
	if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && len(rf.log)-1 == args.LastLogIndex && rf.log[len(rf.log)-1].Term == args.LastLogTerm {
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	DPrintf("Server %d[term %d] received RequestVote from server %d[term %d] done, success = %v", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
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

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("Server %d[term %d, state %d] received AppendEntries from server %d[term %d]", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term)
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.becomesFollower()
	} else if rf.currentTerm > args.Term || !(len(rf.log) > args.PrevLogIndex && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.state == Candidate {
		rf.becomesFollower()
	}

	index := args.PrevLogIndex + 1

	for i := 0; i < len(args.Entries); i++ {
		if rf.log[index].Term != args.Entries[i].Term {
			rf.log = rf.log[:index]
		}
		if index >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i])
		}
		index++
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = index
		}
	}

	rf.heartbeatDue = time.Now().Add(getEletionTimeout())
	rf.currentTerm = args.Term
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server, term, prevLogIndex, prevLogTerm int) {
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	reply := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(server, &args, reply)
	done := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if done {
		return
	}
	if ok && args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.becomesFollower()
			done = true
			return
		} else if !reply.Success {
			DPrintf("Leader %d[term %d] enconter log inconsistency at index %d follower %d[term %d] heartbeat, decreasing to %d", rf.me, args.Term, prevLogIndex, server, reply.Term, rf.nextIndex[server]-1)
			rf.nextIndex[server]--
		}
	}
}

func (rf *Raft) sendNoneEmptyAppendEntries() {
	term := rf.currentTerm
	lastLogIndex := len(rf.log)
	log := make([]Entry, len(rf.log))
	for i := 0; i < len(log); i++ {
		log[i] = rf.log[i]
	}
	commitId := rf.commitIndex
	done := false
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if lastLogIndex > rf.nextIndex[i] {
			//DPrintf("Server %d's next index = %d at leader %d", i, rf.nextIndex[i], rf.me)
			go func(server, term, prevLogIndex int) {
				args := AppendEntriesArgs{
					LeaderId:     rf.me,
					Term:         term,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  log[prevLogIndex].Term,
					Entries:      log[prevLogIndex+1:],
					LeaderCommit: commitId,
				}
				reply := new(AppendEntriesReply)

				ok := rf.sendAppendEntries(server, &args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if done {
					return
				}
				if ok || args.Term == rf.currentTerm {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.becomesFollower()
						done = true
						return
					} else if !reply.Success {
						DPrintf("Leader %d enconter log inconsistency at index %d follower %d, decreasing to %d", rf.me, prevLogIndex, server, rf.nextIndex[server]-1)
						rf.nextIndex[server]--
					}
				}
			}(i, term, rf.nextIndex[i]-1)
		}
	}
	rf.mu.Lock()
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
	rf.mu.Lock()
	_, isLeader = rf.GetState()
	rf.mu.Unlock()
	if isLeader {
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for !rf.killed() {
				rf.applyCond.Wait()
			}
		}()
	}

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
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			if rf.state == Leader {
				command := rf.log[rf.lastApplied].Command
				commandIndex := rf.lastApplied
				rf.mu.Unlock()
				go func() {
					*rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      command,
						CommandIndex: commandIndex,
					}
				}()
				rf.mu.Lock()
			}
		}
		if rf.state == Follower {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			if time.Now().After(rf.heartbeatDue) {
				DPrintf("\033[1;31;40mserver %d heartbeat timeout, start election.\033[0m", rf.me)
				rf.startsElection()
			}
		} else if rf.state == Candidate {
			if time.Now().After(rf.heartbeatDue) {
				DPrintf("\033[1;31;40mserver %d[term %d] election timeout, restart election.\033[0m", rf.me, rf.currentTerm)
				rf.startsElection()
			}
		} else if rf.state == Leader {
			if rf.heartbeatTime.Before(time.Now()) {
				term := rf.currentTerm
				rf.heartbeatTime = time.Now().Add(100 * time.Millisecond)
				prevLogIndex := len(rf.log) - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				//DPrintf("server %d[term %d] sending Heartbeats. ", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.sendHeartbeat(i, term, prevLogIndex, prevLogTerm)
				}
				rf.mu.Lock()
			}
			if rf.state == Leader {
				rf.sendNoneEmptyAppendEntries()
			}
		} else {
			panic("Unknown State")
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startsElection() {
	rf.currentTerm++
	term := rf.currentTerm
	rf.state = Candidate
	done := false
	rf.heartbeatDue = time.Now().Add(getEletionTimeout())
	rf.votedFor = &rf.me
	voted := 1
	total := 1
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	DPrintf("server %d[term %d, state %d]: starts Election", rf.me, rf.currentTerm, rf.state)
	rf.mu.Unlock()
	defer rf.mu.Lock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server, t, lastLogIndex, lastLogTerm int) {
			args := RequestVoteArgs{
				Term:         t,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			DPrintf("Server %d[term %d] send RequestVote to server %d", rf.me, t, server)
			reply := new(RequestVoteReply)
			ok := rf.sendRequestVote(server, &args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if done {
				return
			}
			if ok && args.Term == rf.currentTerm {
				if reply.Term > rf.currentTerm || rf.state != Candidate {
					rf.becomesFollower()
					DPrintf("\033[1;31;40mServer %d[term %d] downgrad to follower\033[0m", rf.me, rf.currentTerm)
					rf.currentTerm = reply.Term
					done = true
					return
				}
				if reply.VoteGranted {
					voted++
				}
			}
			total++
			if 2*voted >= len(rf.peers) || total == len(rf.peers) {
				done = true
				DPrintf("Server %d[term %d] election Done. Got %d votes, total %d votes.", rf.me, rf.currentTerm, voted, total)
				if 2*voted >= len(rf.peers) {
					DPrintf("\033[1;32;40mServer %d[term %d] becomes leader\033[0m", rf.me, rf.currentTerm)
					rf.state = Leader
					term = rf.currentTerm
					rf.heartbeatTime = time.Now().Add(50 * time.Millisecond)
					prevLogIndex := len(rf.log) - 1
					prevLogTerm := rf.log[prevLogIndex].Term
					for i := 0; i < len(rf.nextIndex); i++ {
						rf.nextIndex[i] = len(rf.log)
						DPrintf("Server %d's next index initialized to %d", i, rf.nextIndex[i])
					}
					rf.mu.Unlock()
					defer rf.mu.Lock()
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						go rf.sendHeartbeat(i, term, prevLogIndex, prevLogTerm)
					}
				}
			}
		}(i, term, lastLogIndex, lastLogTerm)
	}
}

func (rf *Raft) becomesFollower() {
	rf.state = Follower
	rf.votedFor = nil
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

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatDue = time.Now().Add(getEletionTimeout())
	DPrintf("Initial heartbeatDue %s", rf.heartbeatDue.String())
	rf.state = Follower
	rand.Seed(int64(rf.me))
	rf.log = append(rf.log, Entry{})
	DPrintf("Init rf.log[0]: Term %v, Command %v", rf.log[0].Term, rf.log[0].Command)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = &applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
