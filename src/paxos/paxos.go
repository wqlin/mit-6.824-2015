package paxos

//
// implementation reference:
// https://ramcloud.stanford.edu/~ongaro/userstudy/paxossummary.pdf
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"net"
)
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import (
	crand "crypto/rand"
	"encoding/gob"
	"math/big"
	"math/rand"
	"time"
)

func init() {
	gob.Register(Proposal{})
	gob.Register(LogEntry{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

const Voter = int32(1)
const NonVoter = int32(0)
const ProposeRetryInterval = 1500 * time.Millisecond
const RPCRetryInterval = 10 * time.Millisecond
const RPCRetryTimes = 5

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Forgotten Fate = iota + 1 // decided but forgotten.
	Pending                   // not yet decided.
	Decided
)

func (f Fate) String() string {
	switch f {
	case Decided:
		return "Decided"
	case Pending:
		return "Pending"
	case Forgotten:
		return "Forgotten"
	}
	return "Unknown"
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func newRandDuration(maxDuration time.Duration) time.Duration {
	return time.Duration(nrand())%maxDuration + 1500*time.Millisecond
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	majority   int
	me         int   // index into peers[]
	role       int32 // 1 as voter and 0 as non-voter. Paxos replica can only vote as voter

	logIndex          int   // index of next log entry, initialize as 0
	validProposeIndex int32 // valid propose index
	offset            int   // index less than offset has been forgotten
	entries           []LogEntry
	doneValues        map[int]int // server -> done values
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) Lock() {
	px.mu.Lock()
}

func (px *Paxos) Unlock() {
	px.mu.Unlock()
}

func (px *Paxos) offsetIndex(index int) int {
	return index - px.offset
}

// generate empty entry for index idx
func genEntryFor(idx int) LogEntry {
	return LogEntry{
		Index:          idx,
		Fate:           Pending,
		MinProposalNum: 0,
		Proposal: Proposal{
			ProposalNum: 0,
			Value:       nil, // default value is nil
		}}
}

// fill log entry gap to satisfy out-of-order agreement
func (px *Paxos) fillGap(up int) {
	for i := px.logIndex; i <= up; i++ {
		px.entries = append(px.entries, genEntryFor(i))
		px.logIndex += 1
	}
}

func (px *Paxos) entry(idx int) LogEntry {
	if idx >= px.logIndex {
		px.fillGap(idx)
	}
	offsetIndex := px.offsetIndex(idx)
	// DPrintf("[genEntry] %d get %d, offset %d offsetIndex %d length %d", px.me, idx, px.offset, offsetIndex, len(px.entries))
	return px.entries[offsetIndex]
}

func (px *Paxos) setEntry(idx int, entry LogEntry) {
	offsetIndex := px.offsetIndex(idx)
	px.entries[offsetIndex] = entry
}

func (px *Paxos) SetRole(role int32) {
	atomic.StoreInt32(&px.role, role)
}

func (px *Paxos) Role() int32 {
	return atomic.LoadInt32(&px.role)
}

// log entries up to `up` is discarded
func (px *Paxos) truncateLog(up int) {
	start := px.offsetIndex(up)
	if start >= 0 && start < len(px.entries) {
		px.entries = append([]LogEntry{}, px.entries[(start + 1):]...)
		px.offset = up + 1
	}
}

func (px *Paxos) State() (offset int, entries []LogEntry, doneValues map[int]int) {
	return px.offset, px.entries, px.doneValues
}

func (px *Paxos) SetState(validProposeIndex int32, offset int, entries []LogEntry, doneValues map[int]int) {
	px.mu.Lock()
	px.validProposeIndex = validProposeIndex
	px.offset = offset
	px.entries = entries
	px.logIndex = offset + len(entries)
	px.doneValues = doneValues
	px.mu.Unlock()
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
// if Start() is called with a sequence number less
// than Min, the Start() call should be ignored
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	if seq < px.offset {
		px.mu.Unlock()
		return
	}
	entry := px.entry(seq)
	if entry.Fate != Decided {
		go px.doPropose(seq, v)
	}
	px.mu.Unlock()
}

// the application on this machine is done with
// all instances <= seq.
func (px *Paxos) Done(seq int) {
	px.setDoneFor(px.me, seq)
	px.broadcastDone()
}

func (px *Paxos) getDoneFor(server int) int {
	px.mu.Lock()
	done := px.doneValues[server]
	px.mu.Unlock()
	return done
}

func (px *Paxos) setDoneFor(server int, done int) {
	px.mu.Lock()
	if done > px.doneValues[server] {
		px.doneValues[server] = done
	}
	up := px.getMin() - 1
	if up > px.offset {
		px.truncateLog(up)
	}
	px.mu.Unlock()
}

func (px *Paxos) Max() int {
	px.mu.Lock()
	max := px.logIndex - 1
	px.mu.Unlock()
	return max
}

func (px *Paxos) Min() int {
	px.mu.Lock()
	min := px.getMin()
	px.mu.Unlock()
	return min
}

func (px *Paxos) getMin() int {
	min := (1 << 31) - 1
	for _, value := range px.doneValues {
		if value < min {
			min = value
		}
	}
	return min + 1
}

func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	if seq < px.offset {
		px.mu.Unlock()
		return Forgotten, nil
	} else if seq >= px.logIndex {
		px.mu.Unlock()
		return Pending, nil
	}
	entry := px.entry(seq)
	fate, value := entry.Fate, entry.Proposal.Value
	px.mu.Unlock()
	return fate, value
}

func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// drive paxos instance to reach agreement on entry
func (px *Paxos) doPropose(idx int, v interface{}) {
	var proposeRetryInterval time.Duration
loop:
	if px.isdead() || px.Role() != Voter || int(atomic.LoadInt32(&px.validProposeIndex)) > idx {
		return
	}
	proposeRetryInterval = newRandDuration(ProposeRetryInterval)
	proposalNum, proposalValue := time.Now().UnixNano()+int64(px.me), v
	// DPrintf("[%d doPropose idx %d Prepare Phase] proposalNum %d proposalValue %+v", px.me, idx, proposalNum, proposalValue)
	prepareNotifyCh := make(chan PrepareReply, len(px.peers))
	prepareArgs := &PrepareArgs{Server: px.me, Index: idx, ProposalNumber: proposalNum}
	for i := range px.peers {
		go px.sendPrepareMsg(prepareArgs, i, prepareNotifyCh)
	}
	highestProposalNum := int64(0) // highest proposal number received from other server
	prepareCnt, prepareReplyCnt := 0, 0
	for prepareReplyCnt < len(px.peers) {
		reply := <-prepareNotifyCh
		DPrintf("[Fuck Reply] server %d proposalNum %d reply %+v", px.me, proposalNum, reply)
		prepareReplyCnt ++
		if reply.Err == ErrForgotten {
			px.Done(idx)
			return
		} else if reply.Err == OK {
			replyEntry := reply.Entry
			DPrintf("[Fuck] server %d proposalNum %d replyEntry %+v", px.me, proposalNum, replyEntry)
			if replyEntry.MinProposalNum > proposalNum {
				// other server has promise to accept higher proposal
				// abort this proposal and retry
				time.Sleep(proposeRetryInterval)
				goto loop
			}
			if replyEntry.Proposal.ProposalNum > highestProposalNum {
				highestProposalNum = replyEntry.Proposal.ProposalNum
				proposalValue = replyEntry.Proposal.Value
			}
			prepareCnt += 1
		}
	}
	if prepareCnt < px.majority { // didn't receive response from a majority of server
		DPrintf("[%d doPropose idx %d proposalNum %d Prepare Phase Retry] didn't receive enough prepare count %d\n", px.me, idx, proposalNum, prepareCnt)
		time.Sleep(proposeRetryInterval)
		goto loop
	}
	DPrintf("[%d doPropose idx %d Accept Phase] prepareCnt %d proposalNum %d proposalValue %+v ", px.me, idx, prepareCnt, proposalNum, proposalValue)
	acceptNotifyCh := make(chan AcceptReply, len(px.peers))
	acceptArgs := &AcceptArgs{Server: px.me, Index: idx, Proposal: Proposal{proposalNum, proposalValue}}
	for i := range px.peers {
		go px.sendAcceptMsg(acceptArgs, i, acceptNotifyCh)
	}
	acceptCnt, acceptReplyCnt := 0, 0
	for acceptReplyCnt < len(px.peers) {
		reply := <-acceptNotifyCh
		acceptReplyCnt ++
		if reply.Err == ErrForgotten {
			px.Done(idx)
			return
		} else if reply.Err == OK {
			acceptorProposalNum := reply.Entry.MinProposalNum
			if acceptorProposalNum > proposalNum {
				DPrintf("[%d doPropose idx %d Accept Phase] come across higher acceptorProposalNum %d proposalNum %d", px.me, idx, acceptorProposalNum, proposalNum)
				time.Sleep(proposeRetryInterval)
				goto loop
			} else {
				acceptCnt += 1
			}
		}
	}
	// DPrintf("[%d doPropose idx %d Decided Phase] acceptCnt %d proposalNum %d proposalValue %+v", px.me, idx, acceptCnt, proposalNum, proposalValue)
	if acceptCnt < px.majority {
		time.Sleep(proposeRetryInterval)
		goto loop
	}
	decidedArgs := &DecidedArgs{Server: px.me, Index: idx, DecidedProposal: Proposal{proposalNum, proposalValue}}
	for i := range px.peers {
		if i != px.me {
			go func(server int) {
				for px.isdead() == false {
					reply := DecidedReply{}
					if call(px.peers[server], "Paxos.Decided", &decidedArgs, &reply) {
						// DPrintf("[%d doPropose idx %d Decided Done] server %d reply %d", px.me, idx, server, reply.Done)
						return
					}
					time.Sleep(RPCRetryInterval)
				}
			}(i)
		} else {
			go px.handleDecidedMsg(decidedArgs)
		}
	}
}

func (px *Paxos) sendPrepareMsg(args *PrepareArgs, server int, ch chan PrepareReply) {
	for i := 0; i < RPCRetryTimes; i++ {
		reply := PrepareReply{}
		if call(px.peers[server], "Paxos.Prepare", args, &reply) {
			ch <- reply
			return
		}
	}
	ch <- PrepareReply{Server: server, Err: ErrRPCFail}
}

func (px *Paxos) sendAcceptMsg(args *AcceptArgs, server int, ch chan AcceptReply) {
	for i := 0; i < RPCRetryTimes; i++ {
		reply := AcceptReply{}
		if call(px.peers[server], "Paxos.Accept", args, &reply) {
			ch <- reply
			return
		}
		time.Sleep(RPCRetryInterval)
	}
	ch <- AcceptReply{Server: server, Err: ErrRPCFail}
}

func (px *Paxos) sendDoneMsg(args *DoneBroadcastArgs, server int, ch chan DoneBroadcastReply) {
	for i := 0; i < RPCRetryTimes; i++ {
		reply := DoneBroadcastReply{}
		if call(px.peers[server], "Paxos.DoneBroadcast", args, &reply) {
			ch <- reply
			return
		}
		time.Sleep(RPCRetryInterval)
	}
	ch <- DoneBroadcastReply{Server: server, Err: ErrRPCFail}
}

func (px *Paxos) broadcastDone() {
	args := &DoneBroadcastArgs{Server: px.me, Done: px.getDoneFor(px.me)}
	ch := make(chan DoneBroadcastReply, len(px.peers))
	for i := range px.peers {
		if i != px.me {
			go px.sendDoneMsg(args, i, ch)
		}
	}
	cnt := 1
	for cnt < len(px.peers) {
		select {
		case reply := <-ch:
			if reply.Err == OK {
				px.setDoneFor(reply.Server, reply.Done)
			}
			cnt++
		}
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.majority = len(px.peers)/2 + 1
	px.me = me
	px.role = Voter // on start, act as voter
	px.validProposeIndex = 0
	px.logIndex = 0
	px.offset = 0
	px.entries = make([]LogEntry, 0, 10)
	px.doneValues = make(map[int]int)
	for i := range px.peers {
		px.doneValues[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
