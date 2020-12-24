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
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
)
import "labrpc"
import "time"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int

	state      State
	timerStart time.Time

	logs        []LogEntry
	commitIndex int
	commitEvent chan ApplyMsg
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int

	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int

	Success bool
}

func (rf *Raft) getLastEntryIndexAndTerm() (int, int) {
	//if len(rf.logs) > 1 {
	last := len(rf.logs) - 1
	return last, rf.logs[last].Term
	//}
	//return -1,-1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	_ = d.Decode(&rf.currentTerm)
	_ = d.Decode(&rf.votedFor)
	_ = d.Decode(&rf.logs)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	lastIndex, lastTerm := rf.getLastEntryIndexAndTerm()

	reply.VoteGranted = false
	//if rf.currentTerm > 1 {
	//	fmt.Println("vote: ", rf.currentTerm,rf.me, lastIndex, lastTerm, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
	//}
	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastTerm || (args.LastLogIndex >= lastIndex && args.LastLogTerm == lastTerm)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.timerStart = time.Now()
		go rf.persist()
	}

	//if rf.currentTerm > 1 {
	//	fmt.Println("vote: ", rf.currentTerm,rf.me, lastIndex, lastTerm, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
	//}

	reply.Term = rf.currentTerm

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}

	rf.logs = append(rf.logs, entry)

	go rf.persist()

	return len(rf.logs) - 1, rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.state = Follower
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = append(rf.logs, LogEntry{
		Term: 0, // because test index start at index 1
	})

	rf.commitEvent = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go func() {
		rf.mu.Lock()
		rf.timerStart = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()

	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) runElectionTimer() {
	timeoutDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()

	//fmt.Println(rf.me, rf.currentTerm, rf.state)

	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		<-ticker.C

		rf.mu.Lock()

		//if rf.currentTerm>=2 {
		//	fmt.Println(rf.currentTerm, rf.me, rf.state)
		//}

		if rf.state == Leader || rf.currentTerm != termStarted {
			rf.mu.Unlock()
			return
		}

		if time.Since(rf.timerStart) >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm += 1
	tempCurrentTerm := rf.currentTerm
	rf.timerStart = time.Now()
	rf.votedFor = rf.me

	go rf.persist()

	var voteCount int32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {
			rf.mu.Lock()
			lastIndex, lastTerm := rf.getLastEntryIndexAndTerm()
			rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         tempCurrentTerm,
				CandidateID:  rf.me,
				LastLogTerm:  lastTerm,
				LastLogIndex: lastIndex,
			}
			var reply RequestVoteReply
			if ok := rf.sendRequestVote(id, args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Candidate {
					return
				}

				if reply.Term > tempCurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				} else if reply.Term == tempCurrentTerm && reply.VoteGranted {
					votes := int(atomic.AddInt32(&voteCount, 1))
					if votes*2 > len(rf.peers) {
						rf.becomeLeader()
						return
					}
				}
			}
		}(i)

	}

	go rf.runElectionTimer()
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.timerStart = time.Now()

	if term != rf.currentTerm {
		go rf.persist()
	}

	go rf.runElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.votedFor = -1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}

	//fmt.Println(rf.me, rf.currentTerm)

	go rf.persist()

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			rf.sendHeartBeatSig()

			<-ticker.C

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		if rf.state != Follower { // may be candidate or leader recover from crash
			rf.becomeFollower(args.Term)
		}
		rf.timerStart = time.Now() //clear timer

		if args.PrevLogIndex == 0 ||
			(args.PrevLogIndex < len(rf.logs) && args.PrevLogTerm == rf.logs[args.PrevLogIndex].Term) {
			reply.Success = true

			index := args.PrevLogIndex + 1
			appendIndex := 0

			for {
				if index >= len(rf.logs) || appendIndex >= len(args.Entries) {
					break
				}
				if rf.logs[index].Term != args.Entries[appendIndex].Term {
					break // same index and term can confirm the same entry/command
				}
				index++
				appendIndex++
			}

			if appendIndex < len(args.Entries) {
				rf.logs = append(rf.logs[:index], args.Entries[appendIndex:]...)
				go rf.persist()
			}

			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < len(rf.logs)-1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.logs) - 1
				}

				go rf.sendUpdateCommitIndexEvent()
			}
		}
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendUpdateCommitIndexEvent() {
	rf.mu.Lock()

	lastApplied := rf.lastApplied
	var entries []LogEntry
	if lastApplied < rf.commitIndex {
		entries = rf.logs[lastApplied+1 : rf.commitIndex+1]
		rf.lastApplied = rf.commitIndex
	}
	rf.mu.Unlock()

	for i, entry := range entries {
		rf.mu.Lock()
		//fmt.Println(rf.me, rf.state, rf.currentTerm, lastApplied+1+i, (entry.Command).(int))
		rf.mu.Unlock()
		rf.commitEvent <- ApplyMsg{
			Index:   lastApplied + 1 + i,
			Command: entry.Command,
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeatSig() {
	rf.mu.Lock()
	tempCurrentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}

		rf.mu.Lock()

		nextIndex := rf.nextIndex[i]
		prevIndex := nextIndex - 1
		prevTerm := 0
		entries := rf.logs[nextIndex:]
		if prevIndex >= 0 {
			prevTerm = rf.logs[prevIndex].Term
		}

		args := AppendEntriesArgs{
			Term:     tempCurrentTerm,
			LeaderId: rf.me,

			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()
		go func(id int) {
			var reply AppendEntriesReply

			if ok := rf.sendAppendEntries(id, args, &reply); ok {

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if tempCurrentTerm < reply.Term {
					//rf.mu.Lock()
					//fmt.Println(rf.me, rf.currentTerm, reply.Term, rf.state)
					rf.becomeFollower(reply.Term)
					//fmt.Println(rf.me, rf.currentTerm, reply.Term, rf.state)
					//rf.mu.Unlock()
					return
				}

				if tempCurrentTerm == reply.Term && rf.state == Leader {
					if reply.Success {

						rf.nextIndex[id] = nextIndex + len(entries)
						rf.matchIndex[id] = rf.nextIndex[id] - 1

						saveCommitIndex := rf.commitIndex

						// update rf.commitIndex by check how many nodes containing new index
						for index := rf.commitIndex + 1; index < len(rf.logs); index++ {
							if rf.logs[index].Term == rf.currentTerm {
								count := 1
								for _, matchIndex := range rf.matchIndex {
									if matchIndex >= index { //bacause no update on leader's matchindex
										count++
									}
								}
								//fmt.Println(rf.me, count, rf.logs[index].Command)
								if 2*count > len(rf.peers) {
									rf.commitIndex = index

								}
							}
						}

						if saveCommitIndex != rf.commitIndex {
							go rf.sendUpdateCommitIndexEvent()
						}
					} else {
						rf.nextIndex[id] = nextIndex - 1
					}
				}
			}

		}(i)
	}

}
