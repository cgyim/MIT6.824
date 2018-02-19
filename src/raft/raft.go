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

import "sync"
import "labrpc"
import "time"
import (
	//"fmt"
	"math/rand"
)

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
type ServerStatus int

type AppendEntries struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntries
	LeaderCommitIndex int
}

const (
	Leader = iota
	Follower
	Candidate
)

//
// A Go object implementing a single Raft peer.
//

type LogEntries struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	winElectionChan chan struct{}       //notify win election for candidate
	VoteGrantChan   chan struct{}       // notify get vote for a server

	AppendEntryRpcChan chan struct{} //heartbeat

	//LeaderInvalid          chan struct{}
	//CandidateInvalid       chan struct{}
	currentTerm int
	//todo
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	candidateId int //作为candidate iD,默认为peer index
	Log         []LogEntries
	CommitIndex int   //即将提交的log的index
	LastApplied int   //已经提交到state machine的log的index
	NextIndex   []int //leader选项，存放其他server下一个加的index
	MatchIndex  []int //leader选项，存放目前已知所有server最高的index

	status        ServerStatus
	votedFor      int // vote for which server, init as -1, change to -1 when get newer term rpc rq.
	GetVoteNumber int // how many votes that a candidate gets in an election.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term, s := rf.currentTerm, rf.status
	rf.mu.Unlock()
	if s == Leader {
		isleader = true
	} else {
		isleader = false
	}
	//rf.mu.Unlock()
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	Term         int
	VotedGranted bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term {
		reply.VotedGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//if rf.HasVoteForAnother == true && rf.currentTerm == args.Term {
	//	//fmt.Println(rf.me ,"reject vote rq from node: ", args.CandidateId)
	//	reply.VotedGranted = false
	//	reply.Term = rf.currentTerm
	//	rf.mu.Unlock()
	//	return
	//}
	if rf.currentTerm < args.Term {
		rf.status = Follower
		rf.currentTerm = args.Term //should convert to follower
		rf.votedFor = -1

	}
	reply.VotedGranted = false
	reply.Term = rf.currentTerm


	//if voted for is null or voted for is just the candidate,
	// and candidate log at least update as rf's , granted.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogIndex >= len(rf.Log)-1 && args.LastLogTerm >= rf.Log[len(rf.Log)-1].Term) {
		//fmt.Println(rf.me, " vote for ", args.CandidateId)
		reply.VotedGranted = true
		rf.votedFor = args.CandidateId
		rf.VoteGrantChan <- struct{}{} //notify that should status -> follower,even now it's follower.
		return
	}
	//todo log append
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Candidate || args.Term != rf.currentTerm {
		//because candidate may find newer term and will immediately convert to follower.
		// and because when this requests return , candidate may not in the term when send request.
		return ok
	}
	if reply.Term > rf.currentTerm {
		//invalid candidate
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		return ok
	}
	if reply.VotedGranted {
		rf.GetVoteNumber++
		if rf.GetVoteNumber > len(rf.peers)/2 {
			rf.status = Leader
			rf.winElectionChan <- struct{}{}
		}
	}

	return ok
}

func (rf *Raft) SendAllVoteRequests() {

	rf.mu.Lock()
    defer rf.mu.Unlock()



	for i := range rf.peers {
		if i != rf.me && rf.status == Candidate { //有可能race condition
			rq := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
				LastLogIndex: rf.LastApplied, LastLogTerm: rf.Log[rf.LastApplied].Term}
			go rf.sendRequestVote(i, &rq, &RequestVoteReply{})
		}
	}

}

func (rf *Raft) AppendEntriesRpc(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.status = Follower
		rf.currentTerm = args.Term

	}
	//empty for heartbeat
	reply.Term = rf.currentTerm
	rf.AppendEntryRpcChan <- struct{}{}
	reply.Success = true
    return
	//todo 这里已经考虑到appendRpc log append,非heartbeat的情况,appendlog 需要修改
	//if args.Entries[args.PrevLogIndex].Term != args.PrevLogTerm || len(args.Entries) < args.PrevLogIndex {
	//	reply.Success = false
	//	reply.Term = rf.currentTerm
	//	return
	//}
	//todo if not heartbeat but append log entries.

}

func (rf *Raft) sendAppendEntryRpc(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRpc", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.status != Leader || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		return ok
	}
	return ok
	//todo log conflict
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

func (rf *Raft) SendAllAppendRpc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			appendEntry := AppendEntries{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				//	PrevLogIndex: 0,
				//	PrevLogTerm : 0,
				Entries: []LogEntries{},
				//    LeaderCommitIndex:0
			}
			go rf.sendAppendEntryRpc(i, &appendEntry, &AppendEntriesReply{})
		}
	}

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1 //does not vote for anyone else
	rf.status = Follower
	rf.LastApplied = 0
	rf.GetVoteNumber = 0
	rf.currentTerm = 0
	rf.CommitIndex = 0
	rf.Log = append(rf.Log, LogEntries{Term: 0})
	rf.winElectionChan = make(chan struct{}) //notify win election for candidate
	rf.VoteGrantChan = make(chan struct{})   // notify get vote for a server
	rf.AppendEntryRpcChan = make(chan struct{})

	go func() {
		for {
			switch rf.status {
			case Follower:
				select {
				case <-rf.AppendEntryRpcChan: //follower appendEntry msg should be empty as heartbeat

				case <-rf.VoteGrantChan: //receive candidate rpc vote request

				case <-time.After(time.Duration(300+rand.Intn(100)) * time.Millisecond): //election timeout set 500-800 ms
					//fmt.Println("node ", rf.me, " term :", rf.currentTerm, "heartbeat timeout and no= vote rq,convert to candidate")
					rf.mu.Lock()
					rf.status = Candidate
					rf.mu.Unlock()
					continue



				}
			case Candidate:
				//once := new(sync.Once)
				//mu := new(sync.Mutex)
                rf.mu.Lock()
				rf.votedFor = rf.me
				rf.currentTerm++
				rf.GetVoteNumber = 1
				rf.mu.Unlock()
				rf.SendAllVoteRequests()
				//count the number of votes received,or wait until get appendEntryRpc,or timeout

				select {
				case <-rf.winElectionChan:
					rf.mu.Lock()
					rf.status = Leader
					rf.mu.Unlock()
				case <-time.After(time.Duration(300+rand.Intn(100)) * time.Millisecond): //timeout candidate => candidate
					//fmt.Println("node: ", rf.me, " term :", rf.currentTerm, "as a candidate election timeout not enough vote,reelcet..")
                      continue
				case <-rf.AppendEntryRpcChan:
					//rf.mu.Lock()
					rf.status = Follower
					//rf.mu.Unlock()
					//fmt.Println("node: ", rf.me, " term :", rf.currentTerm, "receive leader heartbeat..conv to follower..")
				}

			case Leader:

				//todo send append log rpc message to all servers,here only implement empty log entries heartbeat for 2A
                rf.SendAllAppendRpc()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
