package raft

import (
	"bytes"
	"distributed-kv/labrpc"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	Candidate = iota
	Follower
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	Command      interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state     int
	voteCount int

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// channels
	chanApply            chan ApplyMsg
	chanGrantVote        chan bool
	chanWinElection      chan bool
	chanReceiveHeartbeat chan bool
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) persist() {
	data := rf.getRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("decode error")
	}
}


func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil {
		panic("encode error")
	}
	return w.Bytes()
}

// get previous encoded raft state size
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// take a snapshot for this raft peer and kv, and persist them
func (rf *Raft) TakeSnapshot(kvSnapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIncludedIndex, lastLogIndex := rf.log[0].Index, rf.getLastLogIndex()
	if index <= lastIncludedIndex || index > lastLogIndex {
		// invalid index
		return
	}
	rf.trimLog(index, rf.log[index-lastIncludedIndex].Term)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.log[0].Index)
	_ = e.Encode(rf.log[0].Term)
	snapshot := append(w.Bytes(), kvSnapshot...)

	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

// read from previous saved snapshot
func (rf *Raft) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("decode error")
	}

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.trimLog(lastIncludedIndex, lastIncludedTerm)

	// remind kv server to use snapshot
	msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
	rf.chanApply <- msg
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isAtLeastUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	}
}

// check if candidate's log is at least as up-to-date as the voter
func (rf *Raft) isAtLeastUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if ok {
		if rf.state != Candidate || rf.currentTerm != args.Term {
			// stale RPC
			return ok
		}
		if rf.currentTerm < reply.Term {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				// win
				rf.state = Leader
				rf.persist()
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.chanWinElection <- true
			}
		}
	}

	return ok
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.state == Candidate {
			go rf.sendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.chanReceiveHeartbeat <- true

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	baseIndex := rf.log[0].Index

	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		// fast rollback
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if rf.log[i-baseIndex].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		// success
		rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if rf.commitIndex < args.LeaderCommit {
			// update commitIndex and apply logs
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.log[i-baseIndex].Command
		rf.chanApply <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Leader || args.Term != rf.currentTerm {
		// stale RPC
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.getLastLogIndex())
	}

	baseIndex := rf.log[0].Index
	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		// find if there exists an N to update commitIndex
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLog()
			break
		}
	}

	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.chanReceiveHeartbeat <- true

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)

		// remind kv server to use snapshot
		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.chanApply <- msg
	}
}

// trim old entries according to lastIncludedIndex
func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int) {
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Leader || args.Term != rf.currentTerm {
		// invalid request
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
}

// broadcast heartbeat, if logs required are trimmed, then send snapshot
func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	snapshot := rf.persister.ReadSnapshot()

	for server := range rf.peers {
		if server != rf.me && rf.state == Leader {
			if rf.nextIndex[server] > baseIndex {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[server] - 1
				if args.PrevLogIndex >= baseIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				}
				if rf.nextIndex[server] <= rf.getLastLogIndex() {
					args.Entries = rf.log[rf.nextIndex[server]-baseIndex:]
				}
				args.LeaderCommit = rf.commitIndex

				go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
			} else {
				args := &InstallSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].Index
				args.LastIncludedTerm = rf.log[0].Term
				args.Data = snapshot

				go rf.sendInstallSnapshot(server, args, &InstallSnapshotReply{})
			}
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, index := -1, -1
	isLeader := rf.state == Leader

	if isLeader {
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()
	}
	return index, term, isLeader
}


func (rf *Raft) Kill() {
	// do nothing
}

func (rf *Raft) Run() {
	for {
		switch rf.state {
		case Follower:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanReceiveHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.state = Candidate
				rf.persist()
			}
		case Leader:
			go rf.broadcastHeartbeat()
			time.Sleep(time.Millisecond * 60)
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-rf.chanReceiveHeartbeat:
				rf.state = Follower
			case <-rf.chanWinElection:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
			}
		}
	}
}


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.voteCount = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanApply = applyCh
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanWinElection = make(chan bool, 100)
	rf.chanReceiveHeartbeat = make(chan bool, 100)

	// recover from crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.persist()

	go rf.Run()

	return rf
}
