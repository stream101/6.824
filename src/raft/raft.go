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
import "math/rand"

// import "bytes"
// import "encoding/gob"

const FOLLOWER = 0
const CANDIDATE = 1
const LEADER = 2

type LogEntry struct {
    Term int
    Command interface{}
}


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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
    currentTerm int     //latest term server sees
    votedFor int        //candidateId that receive vote in current term, or null
    log []LogEntry      //log entry and term
    timer *time.Timer   // election timer
    electTimeout time.Duration //in milliseconds
    votesCollected  int //record votes collected from other servers in electing leader
    //state   int  //0-follower, 1-candidate, 2-leader
    leader    int   //leader id
    quit chan bool   //use to kill raft worker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    term = rf.currentTerm
    isleader = (rf.me == rf.leader)
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
    Term int
    CandidateId int   //candidate requesting vote
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Granted bool   //granted the vote
    Term    int //peer term to update requester
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    DPrintf("%d (Term %d) request vote from %d (Term %d, votedFor %d)",
        args.CandidateId, args.Term, rf.me, rf.currentTerm, rf.votedFor)
	// Your code here (2A, 2B).
    if args.Term < rf.currentTerm {
        reply.Granted = false
    } else if args.Term >= rf.currentTerm {
        if args.Term > rf.currentTerm {
            //update current Term and clean votedFor if current term
            rf.currentTerm = args.Term
            rf.votedFor = -1
            //enter follower state
            rf.leader = -1
            DPrintf("%d -> follower", rf.me)
        }
        if rf.votedFor < 0 { // havent vote for current term
            rf.votedFor = args.CandidateId
            reply.Granted = true
            DPrintf("%d grant %d", rf.me, args.CandidateId)
        } else {
            reply.Granted = false
        }
    }

    //reset elect timeout
    rf.timer.Reset(rf.electTimeout)
    reply.Term = rf.currentTerm

       /*
        //each server at most vote 1, in FIFO manner
        if len(rf.log) == 0 {
            //if self log is emtpy, requester log must be newer than me
            reply.Granted = true
        } else if (args.LastLogTerm > rf.currentTerm) {
            reply.Granted = true
        } else if (args.LastLogTerm == rf.currentTerm) &&
            (args.LastLogIndex >= (len(rf.log) - 1)) {
            //if in same term, requester's log longer than myself
            reply.Granted = true
        }else {
            reply.Granted = false
        }
    } else {
        reply.Granted = false
    }*/
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

type AppendEntryArgs struct{
    Term int  //leader's term
    LeaderId  int //leader id
}

type AppendEntryReply struct{
    Term int  //currentTerm for leader to update itself
    Success bool  //true if follower contained entry mathching prevLogIndex and prevLogTerm
}


//AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
    if (args.Term < rf.currentTerm) {
        reply.Success = false
    } else {
        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
        }
        reply.Success = true
        //reset elect timer
        rf.timer.Reset(rf.electTimeout)
        // record leader
        rf.leader = args.LeaderId
        DPrintf("%d receive appendEntries from %d", rf.me, args.LeaderId)
    }
    reply.Term = rf.currentTerm
}

func (rf * Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func genRandElectTimeout() time.Duration {
    rand.Seed(time.Now().UnixNano())
    timeout := 1300 + rand.Intn(300)  //1.3 -1.6 sec
    return time.Duration(timeout) * time.Millisecond
}

func sendVoteRequest(rf *Raft, server int, voteArgs *RequestVoteArgs) {
    //rf send vote request to server
    voteReply := &RequestVoteReply{}
    rf.sendRequestVote(server, voteArgs, voteReply)
    rf.mu.Lock()
    if (voteReply.Granted) {
        rf.votesCollected++
        //DPrintf("%d recv %d votes", server, rf.votesCollected)
        if (rf.votesCollected >= len(rf.peers)/2) {
            // become leader
            rf.leader = rf.me
            DPrintf("%d -> leader", rf.me)
        }
    }
    if voteReply.Term > rf.currentTerm {
        rf.currentTerm = voteReply.Term
    }
    rf.mu.Unlock()
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
    rf.quit <- true
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
    rf.currentTerm = -1
    rf.votedFor = -1        //candidateId that receive vote in current term, or null
    rf.log = []LogEntry{}      //log entry and term
    rf.electTimeout = genRandElectTimeout()
    rf.timer = time.NewTimer(rf.electTimeout)
    rf.leader = -1
    rf.quit = make(chan bool)
    DPrintf("%d timeout val %v", rf.me, rf.electTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go raftWorker(rf, applyCh)

	return rf
}

func leaderWorker(rf *Raft, applyCh chan ApplyMsg)  {
    //DPrintf("%d term %d leader addr %p",rf.me, rf.currentTerm)
    appendArgs := &AppendEntryArgs{}
    appendArgs.Term = rf.currentTerm
    appendArgs.LeaderId = rf.me
    for server, _ := range rf.peers {
        if server != rf.me {
            //no need send heartbeat to self
            reply := &AppendEntryReply{}
            rf.sendAppendEntries(server, appendArgs, reply)
            if reply.Success == false && reply.Term > rf.currentTerm {
                // update current term
                rf.currentTerm = reply.Term
                // transit to follower
                rf.leader = -1
                DPrintf("%d -> follower", rf.me)
            }
        }
    }
    //send heartbeat every 600ms
    time.Sleep(600 * time.Millisecond)

}

func followerWorker(rf *Raft, applyCh chan ApplyMsg) {
     select {
        case <-rf.timer.C:
            //election timeout
            DPrintf("Server %d timeout!!", rf.me)
            //begin election ...
            voteArgs := &RequestVoteArgs{}
            //inc currentTerm
            rf.currentTerm += 1
            voteArgs.Term = rf.currentTerm
            //vote for self
            rf.votedFor = rf.me
            voteArgs.CandidateId = rf.me
            //reset votes collected
            rf.votesCollected = 0
            //reset elect timer
            rf.timer.Reset(rf.electTimeout)
            voteArgs.LastLogIndex = len(rf.log) -1
            if voteArgs.LastLogIndex >= 0 {
                voteArgs.LastLogTerm = rf.log[len(rf.log) -1].Term
            } else {
                voteArgs.LastLogTerm = -1
            }
            for server,_ := range rf.peers {
                //send vote to all other servers
                if server != rf.me {
                    go sendVoteRequest(rf, server, voteArgs)
                }
            }
        default:
            time.Sleep(1 * time.Millisecond)
    }
}

func raftWorker(rf *Raft, applyCh chan ApplyMsg) {
    for {
        select {
        case <- rf.quit:
            DPrintf("raft %d quit", rf.me)
            return
        default:
            _, isleader := rf.GetState()
            if (isleader) { //leader
                leaderWorker(rf, applyCh)
            } else { //follower 
                followerWorker(rf, applyCh)
            }
        }
    }
}
