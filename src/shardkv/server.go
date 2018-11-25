package shardkv

import (
	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const StartTimeoutInterval = 5 * time.Second // start operation timeout
const PollInterval = 10 * time.Millisecond
const TickInterval = 250 * time.Millisecond
const SubmitInterval = 200 * time.Millisecond

func init() {
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register([]ShardMigrationArgs{})
	gob.Register(Op{})
	gob.Register(shardmaster.Config{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type notifyArgs struct {
	ClientId   int64
	RequestSeq int
	Value      string
	Err        Err
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	gid        int64              // my replica group ID
	config     shardmaster.Config // current config

	ownShards       IntSet                             // shards that currently owned by server at current configuration
	receivingShards map[int]map[int]ShardMigrationArgs // config number -> shard and migration data, receiving from other group
	waitingShards   IntSet                             // shards -> config number, waiting to migrate from other group

	requestSeq int32 // index of next entry to start
	pollSeq    int32 // index of next entry to poll
	data       map[string]string
	cache      map[int64]int
	notifyChs  map[int]chan notifyArgs
	applyCh    chan Op
}

func (kv *ShardKV) configNum() int {
	kv.mu.Lock()
	num := kv.config.Num
	kv.mu.Unlock()
	return num
}

func (kv *ShardKV) waitingShardNum() int {
	kv.mu.Lock()
	num := len(kv.waitingShards)
	kv.mu.Unlock()
	return num
}

func (kv *ShardKV) getPollSeq() int32 {
	return atomic.LoadInt32(&kv.pollSeq)
}

func (kv *ShardKV) nextRequestSeq() int32 {
	seq := atomic.LoadInt32(&kv.requestSeq)
	kv.setRequestSeq(seq + 1)
	return seq
}

func (kv *ShardKV) setRequestSeq(i int32) {
	atomic.StoreInt32(&kv.requestSeq, i)
}

func (kv *ShardKV) poll() {
	seq := atomic.LoadInt32(&kv.pollSeq)
	for !kv.isdead() {
		status, arg := kv.px.Status(int(seq))
		if status == paxos.Pending {
			kv.setRequestSeq(seq)
			time.Sleep(PollInterval)
			continue
		} else if status == paxos.Decided {
			kv.applyCh <- arg.(Op)
		}
		seq ++
		atomic.StoreInt32(&kv.pollSeq, seq)
	}
}

func (kv *ShardKV) start(configNum int, clientId int64, requestSeq int, args interface{}) (string, Err) {
	if kv.isdead() || kv.px.Role() != paxos.Voter || kv.configNum() != configNum {
		return "", ErrRetry
	}
	seq := int(kv.nextRequestSeq())
	kv.mu.Lock()
	kv.px.Start(seq, Op{PaxosSeq: seq, Args: args})
	notifyCh := make(chan notifyArgs, 1)
	DPrintf("[Start] gid %d server %d seq %d clientId %d args %+v", kv.gid, kv.me, seq, clientId, args)
	kv.notifyChs[seq] = notifyCh
	kv.mu.Unlock()
	select {
	case notifyResult := <-notifyCh:
		DPrintf("[Start Return] gid %d server %d seq %d clientId %d reply %+v", kv.gid, kv.me, seq, clientId, notifyResult)
		if notifyResult.ClientId == clientId && notifyResult.RequestSeq == requestSeq {
			return notifyResult.Value, notifyResult.Err
		} else {
			return "", ErrRetry
		}
	case <-time.After(StartTimeoutInterval):
		return "", ErrRetry
	}
	return "", OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	reply.Value, reply.Err = kv.start(args.ConfigNum, args.ClientId, args.RequestSeq, args.copy())
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	_, reply.Err = kv.start(args.ConfigNum, args.ClientId, args.RequestSeq, args.copy())
	return nil
}

func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) error {
	kv.mu.Lock()
	if _, ok := kv.receivingShards[args.ConfigNum]; !ok {
		kv.receivingShards[args.ConfigNum] = make(map[int]ShardMigrationArgs)
	}
	// DPrintf("[ShardMigration args.ConfigNum %d args.Shard %d] gid %d server %d current config %d", args.ConfigNum, args.Shard, kv.gid, kv.me, kv.config.Num)
	kv.receivingShards[args.ConfigNum][args.Shard] = args.copy()
	kv.mu.Unlock()
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	for !kv.isdead() {
		if kv.waitingShardNum() == 0 {
			currentConfigNum := kv.configNum()
			nextConfigNum := currentConfigNum + 1
			newConfig := kv.sm.Query(nextConfigNum) // handle configuration one at a time
			if newConfig.Num == nextConfigNum {
				kv.start(currentConfigNum, -2, 1, newConfig)
			}
		}
		time.Sleep(TickInterval)
	}
}

func (kv *ShardKV) submit() {
	for !kv.isdead() {
		kv.mu.Lock()
		configNum := kv.config.Num
		if v, ok := kv.receivingShards[configNum]; ok {
			var args []ShardMigrationArgs
			for shard := range kv.waitingShards {
				if arg, ok := v[shard]; ok {
					args = append(args, arg.copy())
				}
			}
			kv.mu.Unlock()
			kv.start(configNum, -1, 1, args)
			kv.mu.Lock()
		}
		kv.mu.Unlock()
		time.Sleep(SubmitInterval)
	}
}

func (kv *ShardKV) apply(op Op) {
	kv.mu.Lock()
	i := op.PaxosSeq
	notifyArg := notifyArgs{Value: "", Err: OK}
	if arg, ok := op.Args.(PutAppendArgs); ok {
		shard := key2shard(arg.Key)
		if arg.ConfigNum != kv.config.Num {
			notifyArg.Err = ErrWrongGroup
		} else if _, ok := kv.ownShards[shard]; !ok {
			notifyArg.Err = ErrWrongGroup
		} else {
			if kv.cache[arg.ClientId] < arg.RequestSeq {
				if arg.Op == "Put" {
					kv.data[arg.Key] = arg.Value
					// DPrintf("[Server Put] gid %d server %d config %d client %d seq %d index %d shard %d key %s value %s result %s", kv.gid, kv.me, kv.config.Num, arg.ClientId, arg.RequestSeq, i, shard, arg.Key, arg.Value, kv.data[arg.Key])
				} else {
					kv.data[arg.Key] += arg.Value
					// DPrintf("[Server Append] gid %d server %d config %d client %d seq %d index %d shard %d key %s value %s result %s", kv.gid, kv.me, kv.config.Num, arg.ClientId, arg.RequestSeq, i, shard, arg.Key, arg.Value, kv.data[arg.Key])
				}
				kv.cache[arg.ClientId] = arg.RequestSeq
			}
		}
		notifyArg.ClientId, notifyArg.RequestSeq, notifyArg.Value = arg.ClientId, arg.RequestSeq, ""
	} else if arg, ok := op.Args.(GetArgs); ok {
		shard := key2shard(arg.Key)
		if arg.ConfigNum != kv.config.Num {
			notifyArg.Err = ErrWrongGroup
		} else if _, ok := kv.ownShards[shard]; !ok {
			notifyArg.Err = ErrWrongGroup
		} else {
			notifyArg.Value, notifyArg.Err = kv.data[arg.Key], OK
		}
		// DPrintf("[Server Get] gid %d server %d config %d client %d seq %d index %d shard %d key %s value %s", kv.gid, kv.me, kv.config.Num, arg.ClientId, arg.RequestSeq, i, shard, arg.Key, kv.data[arg.Key])
		notifyArg.ClientId, notifyArg.RequestSeq = arg.ClientId, arg.RequestSeq
	} else if newConfig, ok := op.Args.(shardmaster.Config); ok {
		kv.applyNewConf(newConfig.Copy())
		notifyArg.ClientId, notifyArg.RequestSeq = -2, 1
	} else if args, ok := op.Args.([]ShardMigrationArgs); ok {
		for _, arg := range args {
			if arg.ConfigNum == kv.config.Num {
				delete(kv.waitingShards, arg.Shard)
				if _, ok := kv.ownShards[arg.Shard]; !ok { // add new shard only if not already added
					kv.ownShards[arg.Shard] = struct{}{}
					for k, v := range arg.Data {
						kv.data[k] = v
					}
					for k, v := range arg.Cache {
						if v > kv.cache[k] {
							kv.cache[k] = v
						}
					}
					DPrintf("[Apply NewShard] gid %d server %d config %d ownShards %v apply arg %#v", kv.gid, kv.me, kv.config.Num, kv.ownShards, arg)
				}
			}
		}
		notifyArg.ClientId, notifyArg.RequestSeq = -1, 1
	}
	go kv.px.Done(op.PaxosSeq)
	kv.notifyIfPresent(i, notifyArg)
	kv.mu.Unlock()
}

// apply new configuration
func (kv *ShardKV) applyNewConf(newConfig shardmaster.Config) {
	if newConfig.Num <= kv.config.Num {
		return
	}
	oldConfig, oldShards := kv.config, kv.ownShards
	kv.ownShards, kv.config = make(IntSet), newConfig.Copy()
	for shard, newGID := range newConfig.Shards {
		if newGID == kv.gid {
			if _, ok := oldShards[shard]; ok || oldConfig.Num == 0 {
				kv.ownShards[shard] = struct{}{}
				delete(oldShards, shard)
			} else {
				kv.waitingShards[shard] = struct{}{}
			}
		}
	}
	for shard := range oldShards {
		args := ShardMigrationArgs{kv.gid, kv.me, newConfig.Num, shard, make(map[string]string), make(map[int64]int)}
		for k, v := range kv.data {
			if key2shard(k) == shard {
				args.Data[k] = v
				delete(kv.data, k)
			}
		}
		for k, v := range kv.cache {
			args.Cache[k] = v
		}
		go kv.pushShard(shard, newConfig, args)
	}
}

func (kv *ShardKV) pushShard(shard int, config shardmaster.Config, args ShardMigrationArgs) {
	//	DPrintf("[Push Shard %d] gid %d server %d config %d", shard, kv.gid, kv.me, config.Num)
	//defer DPrintf("[Push Success Shard %d] gid %d server %d config %d", shard, kv.gid, kv.me, config.Num)
	gid := config.Shards[shard]
	servers := config.Groups[gid]
	pushCount, majority := 0, len(servers)/2+1 // push to a majority of server
	pushServerSet := make(map[string]struct{})
	for {
		if pushCount >= majority {
			return
		}
		for _, server := range servers {
			if _, ok := pushServerSet[server]; ok {
				continue
			}
			var reply ShardMigrationReply
			if call(server, "ShardKV.ShardMigration", &args, &reply) {
				pushServerSet[server] = struct{}{}
				pushCount += 1
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) notifyIfPresent(seq int, arg notifyArgs) {
	if ch, ok := kv.notifyChs[seq]; ok {
		ch <- arg
		delete(kv.notifyChs, seq)
	}
}

func (kv *ShardKV) run() {
	go kv.poll()
	go kv.submit()
	go kv.tick()
	for !kv.isdead() {
		op := <-kv.applyCh
		kv.apply(op)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string, servers []string, me int) *ShardKV {
	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.config = shardmaster.Config{}

	kv.ownShards = make(IntSet)
	kv.receivingShards = make(map[int]map[int]ShardMigrationArgs)
	kv.waitingShards = make(IntSet)

	kv.requestSeq = 0
	kv.pollSeq = 0
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]int)
	kv.notifyChs = make(map[int]chan notifyArgs)
	kv.applyCh = make(chan Op, 100) // prevent sending into channel blocked

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go kv.run()
	return kv
}
