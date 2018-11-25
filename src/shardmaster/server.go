package shardmaster

import (
	"net"
)
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import (
	"math/rand"
	"time"
)

const StartTimeoutInterval = 3 * time.Second // start operation timeout
const PollInterval = 10 * time.Millisecond

func init() {
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	gob.Register(Config{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type notifyArgs struct {
	ClientId   int64
	RequestSeq int
	Err        Err
	Value      interface{}
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	requestSeq int32
	pollSeq    int32
	configs    []Config // indexed by config num
	notifyChs  map[int]chan notifyArgs

	applyCh    chan Op
}

type Op struct {
	PaxosSeq int
	Args     interface{}
}

func (sm *ShardMaster) nextRequestSeq() int32 {
	seq := atomic.LoadInt32(&sm.requestSeq)
	sm.setRequestSeq(seq + 1)
	return seq
}

func (sm *ShardMaster) setRequestSeq(i int32) {
	atomic.StoreInt32(&sm.requestSeq, i)
}

// call after acquire lock
func (sm *ShardMaster) poll() {
	seq := atomic.LoadInt32(&sm.pollSeq)
	for !sm.isdead() {
		status, arg := sm.px.Status(int(seq))
		if status == paxos.Pending {
			sm.setRequestSeq(seq)
			time.Sleep(PollInterval)
			continue
		} else if status == paxos.Decided {
			sm.applyCh <- arg.(Op)
		}
		seq ++
		atomic.StoreInt32(&sm.pollSeq, seq)
	}
}

func (sm *ShardMaster) start(clientId int64, requestSeq int, args interface{}) (interface{}, Err) {
	sm.mu.Lock()
	if queryArg, ok := args.(QueryArgs); ok && queryArg.Num > 0 && queryArg.Num < len(sm.configs) {
		config := sm.getConfig(queryArg.Num)
		sm.mu.Unlock()
		return config, OK
	}
	seq := int(sm.nextRequestSeq())
	sm.px.Start(seq, Op{PaxosSeq: seq, Args: args})
	notifyCh := make(chan notifyArgs, 1)
	DPrintf("[Server %d Start %d] ClientId %d args %#v", sm.me, seq, clientId, args)
	sm.notifyChs[seq] = notifyCh
	sm.mu.Unlock()

	select {
	case notifyResult := <-notifyCh:
		if notifyResult.ClientId == clientId && notifyResult.RequestSeq == requestSeq {
			return notifyResult.Value, notifyResult.Err
		} else {
			return struct{}{}, ErrRetry
		}
	case <-time.After(StartTimeoutInterval):
		return struct{}{}, ErrRetry
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	_, reply.Err = sm.start(args.ClientId, args.RequestSeq, args.copy())
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	_, reply.Err = sm.start(args.ClientId, args.RequestSeq, args.copy())
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	_, reply.Err = sm.start(args.ClientId, args.RequestSeq, args.copy())
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	value, err := sm.start(args.ClientId, args.RequestSeq, args.copy())
	reply.Err = err
	if err == OK {
		reply.Config = value.(Config)
	}
	return nil
}

func (sm *ShardMaster) getConfig(i int) Config {
	var srcConfig Config
	if i < 0 || i >= len(sm.configs) {
		srcConfig = sm.configs[len(sm.configs)-1]
	} else {
		srcConfig = sm.configs[i]
	}
	return srcConfig.Copy()
}

func (sm *ShardMaster) appendNewConfig(newConfig Config) {
	newConfig.Num = len(sm.configs)
	sm.configs = append(sm.configs, newConfig.Copy())
}

func (sm *ShardMaster) apply(op Op) {
	i := op.PaxosSeq
	sm.mu.Lock()
	notifyArg := notifyArgs{-1, 0, OK, ""}
	if arg, ok := op.Args.(JoinArgs); ok { // need re-balance
		notifyArg.ClientId, notifyArg.RequestSeq = arg.ClientId, arg.RequestSeq
		DPrintf("[Join] Server %d apply join index %d args %#v", sm.me, i, arg)
		newConfig := sm.getConfig(-1)
		newConfig.Groups[arg.GID] = append([]string{}, arg.Servers...) // add new group
		if len(newConfig.Groups) == 1 { // first groups arrive
			for i := range newConfig.Shards {
				newConfig.Shards[i] = arg.GID
			}
		} else {
			mapping := queryShardsByGroup(&newConfig)
		loop:
			for {
				gid, maxShards := findMax(mapping)
				if maxShards == 0 || len(mapping[arg.GID]) >= NShards/len(newConfig.Groups) { // have enough servers
					break loop
				} else {
					shards := mapping[gid]
					newConfig.Shards[shards[0]] = arg.GID // assign first shard to new group
					mapping[arg.GID] = append(mapping[arg.GID], shards[0])
					mapping[gid] = append(shards[1:]) // remove shard from old group
				}
			}
		}
		DPrintf("[Join Done] Server %d apply join index %d args %#v", sm.me, i, arg)
		sm.appendNewConfig(newConfig)
	} else if arg, ok := op.Args.(LeaveArgs); ok { // need re-balance
		notifyArg.ClientId, notifyArg.RequestSeq = arg.ClientId, arg.RequestSeq
		newConfig := sm.getConfig(-1)
		delete(newConfig.Groups, arg.GID) // remove
		reBalanceServers := make([]int, 0, 0)
		for shard, gid := range newConfig.Shards {
			if gid == arg.GID {
				reBalanceServers = append(reBalanceServers, shard)
			}
		}
		mapping := queryShardsByGroup(&newConfig)
		DPrintf("[Server %d apply leave index %d args %#v re-balance %v]", sm.me, i, arg, reBalanceServers)
		for _, shard := range reBalanceServers {
			gid, _ := findMin(mapping)
			newConfig.Shards[shard] = gid
			mapping[gid] = append(mapping[gid], shard)
		}
		sm.appendNewConfig(newConfig)
	} else if arg, ok := op.Args.(MoveArgs); ok {
		notifyArg.ClientId, notifyArg.RequestSeq = arg.ClientId, arg.RequestSeq
		DPrintf("[Server %d apply move index %d args %#v]", sm.me, i, arg)
		newConfig := sm.getConfig(-1)
		newConfig.Shards[arg.Shard] = arg.GID
		sm.appendNewConfig(newConfig)
	} else if arg, ok := op.Args.(QueryArgs); ok {
		// DPrintf("[Server %d apply query index %d args %#v]", sm.me, i, arg)
		notifyArg.ClientId, notifyArg.RequestSeq = arg.ClientId, arg.RequestSeq
		notifyArg.Value = sm.getConfig(arg.Num)
	}
	sm.notifyIfPresent(i, notifyArg)
	go sm.px.Done(op.PaxosSeq)
	sm.mu.Unlock()
}

func (sm *ShardMaster) notifyIfPresent(seq int, arg notifyArgs) {
	if ch, ok := sm.notifyChs[seq]; ok {
		ch <- arg
		delete(sm.notifyChs, seq)
	}
}

func (sm *ShardMaster) run() {
	go sm.poll()
	for !sm.isdead() {
		op := <-sm.applyCh
		sm.apply(op)
	}
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.requestSeq = 0
	sm.pollSeq = 0
	sm.notifyChs = make(map[int]chan notifyArgs)
	sm.applyCh = make(chan Op, 100)

	rpcs := rpc.NewServer()
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	go sm.run()
	return sm
}
