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

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implement a sle Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	need_election atomic.Bool
	leaderId      int

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.leaderId == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and includ index. this means the
// service no longer needs the log through (and includ)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	Term    int
	Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("    n%v recv AppendEntries %v", rf.me, args)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term >= rf.currentTerm {
		// heartbeat update
		rf.need_election.Store(false)
		rf.leaderId = args.LeaderId
		rf.currentTerm = args.Term

		// append log
		// prev log found
		if args.PrevLogIndex < len(rf.log) {
			if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				// disagreed, delete unmatched logs
				log.Printf("    n%v disagree with prev log i%v, delete logs", rf.me, args.PrevLogIndex)
				rf.log = rf.log[:args.PrevLogIndex]
			} else {
				// agreed, append logs really really carefully
				// possible situation:
				//	1. same as local but shorter
				//  2. overwrite local logs (maybe diff in length)
				sync_log_len := args.PrevLogIndex + len(args.Entries) + 1
				// resize log
				if sync_log_len > len(rf.log) {
					rf.log = append(rf.log, make([]LogEntry, sync_log_len-len(rf.log))...)
				}
				// just copy
				for i, v := range args.Entries {
					rf.log[args.PrevLogIndex+1+i] = v
				}
				if len(args.Entries) != 0 {
					log.Printf("    n%v agree with prev log i%v, append logs", rf.me, args.PrevLogIndex)
				}
				reply.Success = true

				// only allow commit when the log it's same with leader
				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit <= len(rf.log)-1 {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = len(rf.log) - 1
					}
					log.Printf("    n%v update commit index to i%v", rf.me, rf.commitIndex)
				}
			}
		} else {
			log.Printf("    n%v do NOT found prev log i%v", rf.me, args.PrevLogIndex)
		}
	}
	log.Printf("    n%v rply AppendEntries %v", rf.me, reply)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("    n%v recv RequestVote %v", rf.me, args)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// already voted in this term
	if args.Term < rf.currentTerm {
		log.Printf("    n%v do NOT vote for %v: term expired", rf.me, args.CandidateId)
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		log.Printf("    n%v do NOT vote for %v: already voted", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leaderId = -1
	}

	// check if the candidate's log is at least up-to-date as me
	last_log := rf.log[len(rf.log)-1]
	if args.LastLogTerm > last_log.Term || (args.LastLogTerm == last_log.Term && args.LastLogIndex >= len(rf.log)-1) {
		rf.need_election.Store(false)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		log.Printf("    n%v vote for %v", rf.me, args.CandidateId)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// rf.mu.Lock()
	// log.Printf("t%v: l%v send AppendEntries to n%v", rf.currentTerm, rf.me, server)
	// rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// rf.mu.Lock()
	// log.Printf("t%v: n%v send RequestVote to n%v", rf.currentTerm, rf.me, server)
	// rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service us Raft (e.g. a k/v server) wants to start
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
	// this entire function has to be locked, in case of current starts
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.leaderId != rf.me {
		return 0, 0, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	log.Printf("t%v: l%v start i%v: %v", rf.currentTerm, rf.me, len(rf.log)-1, rf.log[len(rf.log)-1])
	rf.sendLogs()
	return len(rf.log) - 1, rf.currentTerm, rf.leaderId == rf.me
}

func (rf *Raft) sendLogs() {
	// fixing term and leaderid, otherwise it could sending wrong logs with updated term after found newer term in reply
	fixedTerm := rf.currentTerm
	fixedLeaderId := rf.me
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				for {
					rf.mu.Lock()
					args := &AppendEntriesArgs{}
					args.Term = fixedTerm
					args.LeaderId = fixedLeaderId
					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					args.Entries = rf.log[rf.nextIndex[i]:]
					args.LeaderCommit = rf.commitIndex
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, args, reply)

					// retry later
					if !ok {
						break
					}

					rf.mu.Lock()
					if reply.Success {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.mu.Unlock()
						break
					} else {
						// failed due to unmatched logs, retry
						// two possible ways for AppendEntries to reply false:
						//	1. leader's term is expired
						//	2. log unmatched
						if rf.currentTerm < reply.Term {
							log.Printf("t%v: l%v found newer term in AE reply t%v", rf.currentTerm, rf.me, reply.Term)
							rf.currentTerm = reply.Term
							rf.leaderId = -1
							rf.mu.Unlock()
							break
						} else {
							// retry if it's not modified, otherwise an other sendLog is doing samething but faster
							// also make sure i am the leader
							if rf.nextIndex[i]-1 == args.PrevLogIndex && rf.leaderId == rf.me {
								rf.nextIndex[i] -= 1
								log.Printf("t%v: l%v found unmatched log for n%v, rollback to %v and retry", rf.currentTerm, rf.me, i, rf.nextIndex[i])
							} else {
								rf.mu.Unlock()
								break
							}
						}
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-runn goroutines use memory and may chew
// up CPU time, perhaps caus later tests to fail and generat
// confus debug output. any goroutine with a long-runn loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId == rf.me
}

func (rf *Raft) updater() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.leaderId == rf.me {
			for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
				peer_num := 1
				for i, v := range rf.matchIndex {
					if i != rf.me {
						if v >= n {
							peer_num += 1
						}
					}
				}
				if peer_num > len(rf.peers)/2 && rf.log[n].Term == rf.currentTerm {
					rf.commitIndex = n
					log.Printf("t%v: l%v inc commit index to i%v", rf.currentTerm, rf.me, rf.commitIndex)
					break
				}
			}
		}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.log[rf.lastApplied].Command
			msg.CommandIndex = rf.lastApplied
			log.Printf("t%v: n%v commit i%v: %v", rf.currentTerm, rf.me, msg.CommandIndex, msg.Command)
			rf.applyCh <- msg
		}
		log.Printf("t%v: n%v STATUS: log len: %v, last apply: i%v, leader: %v --------------", rf.currentTerm, rf.me, len(rf.log), rf.lastApplied, rf.leaderId)
		rf.mu.Unlock()
		time.Sleep(time.Duration(200 * 1e6))
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// timeout for AE request = heartbeat + random  ->  start election
		rf.need_election.Store(true)
		// time for RV response = heartbeat	->  next heartbeat or next election
		time.Sleep(time.Duration(200 * 1e6))
		if rf.isLeader() {
			rf.sendLogs()
		} else {
			time.Sleep(time.Duration((100 + rand.Intn(200)) * 1e6))
			if rf.need_election.Load() {
				go rf.startElection()
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	log.Printf("t%v: n%v start election", rf.currentTerm, rf.me)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	last_log := &rf.log[len(rf.log)-1]
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = last_log.Term
	rf.mu.Unlock()
	group := atomic.Int32{}
	vote_num := atomic.Int32{}
	vote_num.Add(1)
	for i := range rf.peers {
		if i != rf.me {
			group.Add(1)
			go func(i int) {
				defer group.Add(-1)
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				if ok && reply.VoteGranted {
					vote_num.Add(1)
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && reply.Term > rf.currentTerm {
					log.Printf("t%v: n%v found newer term t%v in election", rf.currentTerm, rf.me, reply.Term)
				}
			}(i)
		}
	}
	for {
		rf.mu.Lock()
		// anthor election already started
		if rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if int(vote_num.Load()) > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.leaderId = rf.me
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.matchIndex = make([]int, len(rf.peers))
			log.Printf("t%v: l%v become leader with %v votes", rf.currentTerm, rf.me, vote_num.Load())
			rf.mu.Unlock()
			rf.sendLogs()
			break
		} else if group.Load() == 0 {
			rf.mu.Lock()
			log.Printf("t%v: n%v stop election: %v votes", rf.currentTerm, rf.me, vote_num.Load())
			rf.mu.Unlock()
			break
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (includ this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-runn work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.leaderId = -1
	rf.applyCh = applyCh

	// Your initialization code here (S2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	log.SetFlags(log.Lmicroseconds)
	// log.SetOutput(ioutil.Discard)
	log.Printf("t%v: n%v start", rf.currentTerm, rf.me)
	go rf.ticker()
	go rf.updater()

	return rf
}
