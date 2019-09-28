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
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
// ApplyMsg写入到ApplyCh就相当于应用于状态机了
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	HeartbeatInterval    = time.Duration(100) * time.Millisecond
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
)

type NodeState uint8

const (
	Follower  = NodeState(1)
	Candidate = NodeState(2)
	Leader    = NodeState(3)
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	voteFor        int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	state          NodeState

	//2B
	applyCh  chan ApplyMsg
	commitId int
	entries  []LogEntries

	nextIndex []int //对于每个followr节点，下次应该检查哪个index

}

type LogEntries struct {
	Command interface{}
	Term    int
}

type AppendEntriesReq struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.entries)
	e.Encode(rf.commitId)

	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeRaftState())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, voteFor, commitId int
	var entries []LogEntries

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&entries) != nil ||
		d.Decode(&commitId) != nil {
		fmt.Println("decode error when readPersisit")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.voteFor = voteFor
		rf.entries = append(rf.entries, entries...)
		rf.commitId = commitId
		DPrintf("recover rf.term %v,rf.commiId %v,rf.entries:%v", rf.currentTerm, rf.commitId, rf.entries)
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	reply.CurrentTerm = rf.currentTerm

	if args.CandidateTerm < rf.currentTerm {
		return
	}

	//election restriction(5.4.1)
	//Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries
	//in the logs.
	//If the logs have last entries with different terms,then the log with the later term is more up-to-date.
	//If the logs end with same term ,then whichever log is longer is more up-to-date.
	lastindex := len(rf.entries) - 1
	if args.LastLogTerm < rf.getLastTerm() ||
		(args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex < lastindex) {
		rf.currentTerm = args.CandidateTerm
		rf.convertState(Follower)
		return
	}

	if args.CandidateTerm >= rf.currentTerm {
		rf.voteFor = args.CandidateId
		DPrintf("set peer %v currentTerm to %v", rf.me, rf.currentTerm)
		rf.currentTerm = args.CandidateTerm
		//reply.CurrentTerm = args.CandidateTerm //这个地方的term应该是什么呢
		reply.VoteGranted = true
		rf.convertState(Follower)
	}
	DPrintf("peer %v revecive requestvote from %v", rf.me, args.CandidateId)
	return
	// Your code here (2A, 2B).
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//在Appendentries RPC中发送new operation 给其他server

// 1.需要处理election restriction(section 5.4.1 in paper)
// 2.un-needed election: 第一，成为leader后需要马上发送heartbearts,and so on ....
// 3.收到大多数follower接受log后，Leader就把command应用于state machine，并把结果返回给client
//   注意这里的commit要包括之前所有未提交的entry
// 4.如何某个Follow没有接受log,那么Leader会再次给它发送，即使结果已经返回给client了
//   Leader为每个个Follow都保存了一个nextIndex 去检查是否一致

// Questions
// 1. 收到command就应该立即放入到entries吗？还是等收到大多数接受之后
// Ans: The leader appends the command to its log as a new entry,
//      then is- sues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1

	rf.mu.Lock()
	isLeader := rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, rf.currentTerm, isLeader
	}
	if isLeader {
		entry := LogEntries{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.entries = append(rf.entries, entry)
		index = len(rf.entries) - 1
		rf.persist()
		rf.mu.Unlock()
		rf.broadcastHeartbeat()
		return index, rf.currentTerm, isLeader
	}

	// Your code here (2B).
	rf.mu.Unlock()
	return index, rf.currentTerm, isLeader
}

func (rf *Raft) FollowcommitEntries(commit int) {
	defer rf.persist()
	if rf.commitId+1 > commit {
		return
	}
	InfoPrintf("peer %v commit from %v to %v", rf.me, rf.commitId, commit)
	for i := rf.commitId + 1; i <= commit; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.entries[i].Command,
			CommandIndex: i,
		}
	}
	rf.commitId = commit
}

//从当前到上一次都得提交
func (rf *Raft) commitEntries() {
	defer rf.persist()

	if rf.commitId >= len(rf.entries)-1 {
		return
	}
	InfoPrintf("peer %v commit from %v to %v", rf.me, rf.commitId, len(rf.entries)-1)

	for i := rf.commitId + 1; i < len(rf.entries); i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.entries[i].Command,
			CommandIndex: i,
		}
	}
	rf.commitId = len(rf.entries) - 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesReq, reply *AppendEntriesReply) {
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = false
	InfoPrintf("peer %v recevie leader %v,peer's term:%v,leader's term:%v,leader's prevlogindex:%v", rf.me, args.LeaderId, rf.currentTerm, args.LeaderTerm, args.PrevLogIndex)
	if rf.currentTerm > args.LeaderTerm {
		return
	}
	//DPrintf("peer %v,before entries:%v", rf.me, rf.entries)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//注意：在任何时候发现比自己term更大的信息，都应该重新设置自己的term
	if rf.currentTerm < args.LeaderTerm {
		rf.currentTerm = args.LeaderTerm
		rf.convertState(Follower)
		//这里不能返回
	}

	//收到心跳，重置自己的electionTimer
	if rf.state != Leader {
		rf.electionTimer.Reset(randTimeDuration())
	}

	if len(rf.entries) == 0 {
		rf.entries = append(rf.entries, args.Entries...)
		reply.Success = true
		//DPrintf("afert insert peer %v entries %v", rf.me, rf.entries)
		rf.commitEntries()
		return
	}
	if args.PrevLogIndex < len(rf.entries) && args.PrevLogIndex >= 0 &&
		args.PrevLogTerm != rf.entries[args.PrevLogIndex].Term {
		DPrintf("peer %v term not match", rf.me)
		return
	}

	index := args.PrevLogIndex

	var i int
	for i = 0; i < len(args.Entries); i++ {
		index += 1
		if index >= len(rf.entries) {
			break
		}
		if args.Entries[i].Term != rf.entries[index].Term {
			rf.entries = rf.entries[:index]

			//最开始这里没有考虑到i之前的是匹配的，不应该把整个参数都append过去
			rf.entries = append(rf.entries, args.Entries[i:]...)
			break
		}
	}
	DPrintf("peer %v conflict index %v", rf.me, index)
	if index == len(rf.entries) && i >= 0 {
		rf.entries = append(rf.entries, args.Entries[i:]...)
	}

	var commit int
	if args.LeaderCommit > rf.commitId {
		commit = min(args.LeaderCommit, len(rf.entries)-1)
	}

	//DPrintf("peer %v , afeter entries:%v args.entries:%v", rf.me, rf.entries, args.Entries)
	rf.FollowcommitEntries(commit)
	reply.Success = true
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func (rf *Raft) getPrevIndex(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevTerm(i int) int {
	index := rf.getPrevIndex(i)
	if index >= 0 {
		return rf.entries[index].Term
	}
	return 0
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesReq, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getLastTerm() int {
	index := rf.getLastIndex()
	if index > -1 {
		return rf.entries[index].Term
	}
	return 0
}

func (rf *Raft) getLastIndex() int {
	if len(rf.entries) > 0 {
		return len(rf.entries) - 1
	}
	return -1
}

func (rf *Raft) startElection() {
	defer rf.persist()
	rf.electionTimer.Reset(randTimeDuration())

	var myvotes int32 = 0
	rf.currentTerm += 1
	InfoPrintf("peer %v start election at term %v", rf.me, rf.currentTerm)
	args := &RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  rf.getLastIndex(),
		LastLogTerm:   rf.getLastTerm(),
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			rf.voteFor = i
			atomic.AddInt32(&myvotes, 1)
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{
				VoteGranted: false,
			}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				DPrintf("peer %v,receive votereply from %v, reply term:%v,reply voteGranted:%v", rf.me, server, reply.CurrentTerm, reply.VoteGranted)

				if reply.CurrentTerm > rf.currentTerm {
					rf.currentTerm = reply.CurrentTerm
					rf.convertState(Follower)
				} else if reply.VoteGranted == true && rf.state == Candidate {
					atomic.AddInt32(&myvotes, 1)
					if atomic.LoadInt32(&myvotes) > int32(len(rf.peers)/2) {
						rf.convertState(Leader)
					}
				}
				rf.mu.Unlock()
			} else {
				DPrintf("sendRequestVote failed to peer %v", server)
			}
		}(i)
	}

}

//TODO : 提交只能从当前Term来提交
func (rf *Raft) broadcastHeartbeat() {
	var agreeCount int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()
				if len(rf.entries) <= 0 || rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				entries := make([]LogEntries, 0)

				entries = append(entries, rf.entries[rf.nextIndex[server]:len(rf.entries)]...)
				args := AppendEntriesReq{
					LeaderTerm:   rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevIndex(server),
					PrevLogTerm:  rf.getPrevTerm(server),
					Entries:      entries,
					LeaderCommit: rf.commitId,
				}
				rf.mu.Unlock()
				var reply AppendEntriesReply

				if ok := rf.sendAppendEntries(server, &args, &reply); !ok && rf.state == Leader {
					//InfoPrintf("sendAppendEntries  to peer %v faild. ", server)
					continue
				}
				InfoPrintf("leader %v 's term is %v, reveiveReply from peer %v,success %v,term %v ", rf.me, rf.currentTerm, server, reply.Success, reply.Term)

				rf.mu.Lock()
				if rf.state != Leader || rf.currentTerm != args.LeaderTerm {
					InfoPrintf("Leader's state or other info has changed, so return")
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if reply.Success {
					atomic.AddInt32(&agreeCount, 1)
					rf.mu.Lock()
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries)
					if agreeCount > int32(len(rf.peers)/2) && rf.state == Leader {
						//DPrintf("Leader %v entries:%v", rf.me, rf.entries)
						rf.commitEntries()
					}
					rf.mu.Unlock()

					break
				} else {
					if reply.Term > rf.currentTerm {
						InfoPrintf("reply term %v > leader term %v,convert leader %v to follower .", reply.Term, rf.currentTerm, rf.me)
						rf.convertState(Follower)
					} else {
						//TODO: more efficient
						if rf.nextIndex[server] >= 1 {
							rf.nextIndex[server] -= 1
						}
					}
				}
			}

		}(i)
	}

}

func (rf *Raft) convertState(s NodeState) {
	if s == rf.state {
		return
	}
	rf.state = s
	DPrintf("peer %v convert state to %v at term %v ", rf.me, s, rf.currentTerm)
	switch s {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration())
		rf.voteFor = -1
	case Candidate:
		rf.electionTimer.Reset(randTimeDuration())
		rf.startElection()

	case Leader:
		rf.electionTimer.Stop()
		rf.broadcastHeartbeat()
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	}
	return
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
	DPrintf("Make raft server......")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.nextIndex = make([]int, len(peers))
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitId = -1
	rf.state = Follower
	rf.electionTimer = time.NewTimer(randTimeDuration())
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.applyCh = applyCh
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				rf.electionTimer.Reset(randTimeDuration())
				if rf.state == Follower {
					rf.convertState(Candidate)
				} else {
					rf.startElection()
				}
				rf.mu.Unlock()

			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.broadcastHeartbeat()
					rf.heartbeatTimer.Reset(HeartbeatInterval)
				}
				rf.mu.Unlock()
			}
		}
	}()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randTimeDuration() time.Duration {
	num := rand.Int63n(ElectionTimeoutUpper.Nanoseconds()-ElectionTimeoutLower.Nanoseconds()) + ElectionTimeoutLower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}
