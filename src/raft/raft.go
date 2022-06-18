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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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
	currentTerm           int
	votedFor              *int
	state                 State
	heartbeatDue          time.Time
	heartbeatTime         time.Time
	log                   []Entry
	commitIndex           int
	lastApplied           int
	nextIndex             []int
	matchIndex            []int
	applyCh               *chan ApplyMsg
	startHandle           *sync.Cond
	sendAppendEntriesCond []*sync.Cond
	applyCond             *sync.Cond
}

func getEletionTimeout() time.Duration {
	return time.Duration(250+rand.Intn(150)) * time.Millisecond
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	var votedFor int
	if rf.votedFor != nil {
		votedFor = *rf.votedFor
		e.Encode(votedFor)
	} else {
		votedFor := -1
		e.Encode(votedFor)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("\033[36mServer[%d] persist: currentTerm=%d, votedFor=%d, rf.log=%v\033[0m", rf.me, rf.currentTerm, votedFor, rf.log)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []Entry
	var currentTerm int
	var votedFor int
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		DPrintf("Server[%d] readPersist: decode error", rf.me)
		rf.currentTerm = 0
		rf.votedFor = nil
		rf.log = make([]Entry, 0)
	} else {
		rf.currentTerm = currentTerm
		rf.log = make([]Entry, len(log))
		for i := 0; i < len(rf.log); i++ {
			rf.log[i] = log[i]
		}
		if votedFor != -1 {
			rf.votedFor = new(int)
			d.Decode(rf.votedFor)
		} else {
			rf.votedFor = nil
		}
		DPrintf("\033[33mServer[%d] readPersist: currentTerm=%d, votedFor=%d, rf.log=%v\033[0m", rf.me, rf.currentTerm, votedFor, rf.log)
	}
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
	persistStateChange := false
	DPrintf("Server %d[term %d] received RequestVote from server %d[term %d]", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		DPrintf("\033[1;31;40mServer %d[term %d] downgrad to follower\033[0m", rf.me, rf.currentTerm)
		rf.becomesFollower()
		persistStateChange = true
	}
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Server %d[term %d] received RequestVote from server %d[term %d] done, success = %v", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
		if persistStateChange {
			rf.persist()
		}
		return
	}
	reply.Term = rf.currentTerm
	lastLog := rf.log[len(rf.log)-1]
	lastLogIndex := len(rf.log) - 1
	if lastLog.Term > args.LastLogTerm {
		reply.VoteGranted = false
	} else if lastLog.Term == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
	} else if rf.votedFor == nil || rf.votedFor == &args.CandidateId {
		rf.heartbeatDue = time.Now().Add(getEletionTimeout())
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
		persistStateChange = true
	} else {
		reply.VoteGranted = false
	}
	DPrintf("Server %d[term %d] received RequestVote from server %d[term %d] done, success = %v", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
	if persistStateChange {
		rf.persist()
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
	persistStateChange := false
	if len(args.Entries) > 0 {
		DPrintf("Server %d[term %d, state %d] received AppendEntries from server %d[term %d] command %v", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, args.Entries)
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.becomesFollower()
		persistStateChange = true
	}
	if rf.currentTerm > args.Term || !(len(rf.log) > args.PrevLogIndex && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
		if rf.currentTerm == args.Term {
			rf.heartbeatDue = time.Now().Add(getEletionTimeout())
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		if persistStateChange {
			rf.persist()
		}
		return
	}

	if rf.state == Candidate {
		rf.becomesFollower()
		persistStateChange = true
	}

	index := args.PrevLogIndex + 1

	for i := 0; i < len(args.Entries); i++ {
		if index < len(rf.log) && rf.log[index].Term != args.Entries[i].Term {
			rf.log = rf.log[:index]
			persistStateChange = true
		}
		if index >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i])
			persistStateChange = true
		}
		index++
	}

	if args.LeaderCommit > rf.commitIndex {
		var newCommitIndex int
		index = len(rf.log) - 1
		if args.LeaderCommit > index {
			newCommitIndex = index
		} else {
			newCommitIndex = args.LeaderCommit
		}
		rf.commitIndex = newCommitIndex
		DPrintf("Follower %d commitIndex = %d", rf.me, rf.commitIndex)
	}

	rf.heartbeatDue = time.Now().Add(getEletionTimeout())
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.applyCond.Broadcast()
	if persistStateChange {
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server, term, prevLogIndex, prevLogTerm, leaderCommit int) {
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: leaderCommit,
	}
	reply := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(server, &args, reply)
	done := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if done || args.Term != rf.currentTerm {
		done = true
		return
	}
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.becomesFollower()
			done = true
			rf.persist()
			return
		} else if !reply.Success && prevLogIndex == rf.nextIndex[server]-1 {
			DPrintf("Leader %d[term %d] enconter log inconsistency at index %d follower %d[term %d] heartbeat, decreasing to %d", rf.me, args.Term, prevLogIndex, server, reply.Term, rf.nextIndex[server]-1)
			for rf.log[rf.nextIndex[server]-1].Term == args.PrevLogTerm {
				rf.nextIndex[server]--
			}
			go func() {
				DPrintf("Sending")
				rf.sendAppendEntriesCond[server].Broadcast()
				DPrintf("Sent")
			}()
		}
	}
}

func (rf *Raft) appendEntriesHandler() {
	peerNum := len(rf.peers)
	for i := 0; i < peerNum; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.sendAppendEntriesCond[server].L.Lock()
			for !rf.killed() {
				rf.mu.Lock()
				term := rf.currentTerm
				lastLogIndex := len(rf.log)
				log := make([]Entry, len(rf.log))
				for i := 0; i < len(log); i++ {
					log[i] = rf.log[i]
				}
				commitId := rf.commitIndex
				done := false
				state := rf.state
				nextIndex := make([]int, len(rf.nextIndex))
				for i := 0; i < len(nextIndex); i++ {
					nextIndex[i] = rf.nextIndex[i]
				}
				rf.mu.Unlock()

				if state != Leader || lastLogIndex <= nextIndex[server] {
					DPrintf("Leader %d[term %d] for %d waiting.", rf.me, term, server)
					rf.sendAppendEntriesCond[server].Wait()
					DPrintf("Leader %d[term %d] sendAppendEntriesCond[%d]", rf.me, term, server)
					continue
				}

				DPrintf("Server %d's next index = %d at leader %d", server, nextIndex[server], rf.me)
				func(server, term, prevLogIndex int) {
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
					if done || args.Term != rf.currentTerm {
						done = true
						return
					}
					if ok {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.becomesFollower()
							done = true
							rf.persist()
							return
						} else if reply.Success {
							newLogIndex := prevLogIndex + len(args.Entries)
							DPrintf("Follower %d[term %d] append prevLogIndex %d[total len %d] success to leader %d[term %d]", server, reply.Term, prevLogIndex, newLogIndex, rf.me, rf.currentTerm)
							rf.nextIndex[server] = newLogIndex + 1
							rf.matchIndex[server] = newLogIndex
							if newLogIndex > rf.commitIndex && rf.log[newLogIndex].Term == rf.currentTerm {
								followerCommitOk := 1
								for i := 0; i < len(rf.matchIndex) && followerCommitOk*2 <= len(rf.matchIndex); i++ {
									if i == rf.me {
										continue
									}
									if rf.matchIndex[i] >= newLogIndex {
										followerCommitOk++
									}
								}
								DPrintf("Follower %d[term %d] commit index %d, followerCommitOk %d", server, rf.currentTerm, newLogIndex, followerCommitOk)
								if followerCommitOk*2 >= len(rf.matchIndex) {
									DPrintf("Leader %d commit index %d", rf.me, newLogIndex)
									log := make([]Entry, len(rf.log))
									for i := 0; i < len(log); i++ {
										log[i] = rf.log[i]
									}
									rf.commitIndex = newLogIndex
									rf.applyCond.Broadcast()
								}
							}
						} else {
							for rf.nextIndex[server] > 1 && rf.log[rf.nextIndex[server]-1].Term == args.PrevLogTerm {
								rf.nextIndex[server]--
							}
							DPrintf("Leader %d enconter log inconsistency at index %d follower %d, decreasing to %d", rf.me, prevLogIndex, server, rf.nextIndex[server]-1)
						}
					}
				}(server, term, nextIndex[server]-1)
			}
			rf.sendAppendEntriesCond[server].L.Unlock()
		}(i)
	}
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
	_, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, Entry{
			Command: command,
			Term:    rf.currentTerm,
		})
		DPrintf("\033[1;32;40mLeader %d[term %d] append log %d[%d] command %v\033[0m", rf.me, term, index, len(rf.log), command)
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			go func(server int) {
				rf.sendAppendEntriesCond[server].Broadcast()
			}(i)
		}
		rf.persist()
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

func (rf *Raft) applyHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		//DPrintf("Server %d[term %d, state %d] checking lastApplied %d and commitIndex %d", rf.me, rf.currentTerm, rf.state, rf.lastApplied, rf.commitIndex)
		if rf.commitIndex > rf.lastApplied {
			applyBegin := rf.lastApplied + 1
			applyEntry := rf.log[applyBegin]
			DPrintf("Server %d[term %d, state %d] applied log entry %d, entry %v", rf.me, rf.currentTerm, rf.state, applyBegin, rf.log[applyBegin])
			rf.mu.Unlock()
			*rf.applyCh <- ApplyMsg{CommandValid: true, Command: applyEntry.Command, CommandIndex: applyBegin}
			rf.mu.Lock()
			rf.lastApplied++

		} else {
			rf.applyCond.Wait()
		}
	}
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
	for i := 0; i < len(rf.peers); i++ {
		go func(server int) {
			rf.sendAppendEntriesCond[server].Broadcast()
		}(i)

	}
	rf.applyCond.Broadcast()
	DPrintf("Server %d killed", rf.me)
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
				leaderCommitIndex := rf.commitIndex
				//DPrintf("server %d[term %d] sending Heartbeats. ", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.sendHeartbeat(i, term, prevLogIndex, prevLogTerm, leaderCommitIndex)
				}
				rf.mu.Lock()
			}
		} else {
			panic("Unknown State")
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	rf.persist()

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
			if done || rf.currentTerm != t {
				done = true
				return
			}
			if ok {
				if reply.Term > rf.currentTerm || rf.state != Candidate {
					rf.becomesFollower()
					DPrintf("\033[1;31;40mServer %d[term %d] downgrad to follower\033[0m", rf.me, rf.currentTerm)
					rf.currentTerm = reply.Term
					done = true
					rf.persist()
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
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						go func(server int) {
							DPrintf("Sending leader heartbeat")
							rf.sendAppendEntriesCond[server].Broadcast()
							DPrintf("Sent leader heartbeat")
						}(i)
					}
					rf.heartbeatTime = time.Now().Add(100 * time.Millisecond)
					prevLogIndex := len(rf.log) - 1
					prevLogTerm := rf.log[prevLogIndex].Term
					leaderCommit := rf.commitIndex
					for i := 0; i < len(rf.nextIndex); i++ {
						if rf.me == i {
							continue
						}
						rf.nextIndex[i] = len(rf.log)
						DPrintf("Server %d's next index initialized to %d", i, rf.nextIndex[i])
					}
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						go rf.sendHeartbeat(i, rf.currentTerm, prevLogIndex, prevLogTerm, leaderCommit)
					}
					rf.persist()
				}
			}
		}(i, term, lastLogIndex, lastLogTerm)
	}
}

func (rf *Raft) becomesFollower() {
	rf.state = Follower
	rf.votedFor = nil
	rf.heartbeatDue = time.Now().Add(getEletionTimeout())
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
	rf.state = Follower
	rand.Seed(int64(rf.me + int(time.Now().UnixNano())))
	rf.log = append(rf.log, Entry{})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = &applyCh
	rf.startHandle = sync.NewCond(&rf.mu)
	for i := 0; i < len(rf.peers); i++ {
		rf.sendAppendEntriesCond = append(rf.sendAppendEntriesCond, sync.NewCond(&sync.Mutex{}))
		DPrintf("Server %d's sendAppendEntriesCond[%d] initialized.", rf.me, i)
	}
	rf.appendEntriesHandler()
	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.applyHandler()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
