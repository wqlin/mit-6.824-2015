package diskv

import (
	"bytes"
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

const StartTimeoutInterval = 3 * time.Second // start operation timeout
const PollInterval = 10 * time.Millisecond
const TickInterval = 250 * time.Millisecond
const SubmitInterval = 200 * time.Millisecond

func init() {
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register([]ShardMigrationArgs{})
	gob.Register(Op{})
	gob.Register(shardmaster.Config{})
	gob.Register(paxos.LogEntry{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type notifyArgs struct {
	ClientId   int64
	RequestSeq int
	Value      string
	Err        Err
}

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	servers    []string
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	ps         *Persister         // wrap file and directory operation
	gid        int64              // my replica group ID
	config     shardmaster.Config // stateFile config

	ownShards       IntSet                             // shards that currently owned by server at stateFile configuration
	receivingShards map[int]map[int]ShardMigrationArgs // config number -> shard and migration data, receiving from other group
	waitingShards   IntSet                             // shards -> config number, waiting to migrate from other group

	requestSeq int32 // index of next entry to start
	pollSeq    int32 // index of next entry to poll
	applySeq   int   // current applying entry
	data       map[string]string
	cache      map[int64]int
	notifyChs  map[int]chan notifyArgs
	applyCh    chan Op
}

func (kv *DisKV) configNum() int {
	kv.mu.Lock()
	num := kv.config.Num
	kv.mu.Unlock()
	return num
}

func (kv *DisKV) waitingShardNum() int {
	kv.mu.Lock()
	num := len(kv.waitingShards)
	kv.mu.Unlock()
	return num
}

func (kv *DisKV) getPollSeq() int32 {
	return atomic.LoadInt32(&kv.pollSeq)
}

func (kv *DisKV) nextRequestSeq() int32 {
	seq := atomic.LoadInt32(&kv.requestSeq)
	kv.setRequestSeq(seq + 1)
	return seq
}

func (kv *DisKV) setRequestSeq(i int32) {
	atomic.StoreInt32(&kv.requestSeq, i)
}

func (kv *DisKV) poll() {
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

// server state doesn't not include key-value pairs of server
func (kv *DisKV) State() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.px.Lock()
	offset, entries, doneValues := kv.px.State()
	e.Encode(offset)
	e.Encode(entries)
	e.Encode(doneValues)
	e.Encode(kv.requestSeq)
	e.Encode(kv.applySeq)
	e.Encode(kv.config)
	e.Encode(kv.ownShards)
	e.Encode(kv.receivingShards)
	e.Encode(kv.waitingShards)
	e.Encode(kv.cache)
	e.Encode(kv.data)
	kv.px.Unlock()
	data := w.Bytes()
	DPrintf("[State] gid %d server %d offset %d length of entries %d pollSeq %d requestSeq %d applySeq %d",
		kv.gid, kv.me, offset, len(entries), kv.pollSeq, kv.requestSeq, kv.applySeq)
	return data
}

func (kv *DisKV) ReadState(data []byte) error {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	offset, entries, doneValues := 0, []paxos.LogEntry{}, make(map[int]int)
	requestSeq, applySeq := int32(0), 0
	if d.Decode(&offset) != nil ||
		d.Decode(&entries) != nil ||
		d.Decode(&doneValues) != nil ||
		d.Decode(&requestSeq) != nil ||
		d.Decode(&applySeq) != nil ||
		d.Decode(&kv.config) != nil ||
		d.Decode(&kv.ownShards) != nil ||
		d.Decode(&kv.receivingShards) != nil ||
		d.Decode(&kv.waitingShards) != nil ||
		d.Decode(&kv.cache) != nil ||
		d.Decode(&kv.data) != nil {
		return ErrReadState
		// log.Fatal("Error in reading content")
	}
	kv.px.SetState(requestSeq-1, offset, entries, doneValues)
	kv.requestSeq, kv.pollSeq, kv.applySeq = requestSeq, int32(applySeq), applySeq
	return nil
}

func (kv *DisKV) persist() error {
	if kv.isdead() {
		return nil
	}
	DPrintf("[Persist] gid %d server %d ownShards %v pollSeq %d",
		kv.gid, kv.me, kv.ownShards, kv.getPollSeq())
	data := kv.State()
	if err := kv.ps.filePut(data); err != nil {
		return err
	}
	DPrintf("[Persist Success] gid %d server %d persist data %v ",
		kv.gid, kv.me, kv.data)
	return nil
}

// restore paxos and kvserver state
func (kv *DisKV) restartFromDisk() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[Restore] gid %d server %d", kv.gid, kv.me)
	data, err := kv.ps.fileGet()
	if err != nil {
		DPrintf("[Restore Read Fail] gid %d server %d", kv.gid, kv.me)
		return err
	}
	return kv.ReadState(data)
}

func (kv *DisKV) start(configNum int, clientId int64, requestSeq int, args interface{}) (string, Err) {
	if kv.isdead() || kv.px.Role() != paxos.Voter || kv.configNum() != configNum {
		return "", ErrRetry
	}
	seq := int(kv.nextRequestSeq())
	kv.mu.Lock()
	kv.px.Start(seq, Op{PaxosSeq: seq, Args: args})
	notifyCh := make(chan notifyArgs, 1)
	DPrintf("[Start] gid %d server %d seq %d clientId %d args %#v", kv.gid, kv.me, seq, clientId, args)
	kv.notifyChs[seq] = notifyCh
	kv.mu.Unlock()
	select {
	case notifyResult := <-notifyCh:
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

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	reply.Value, reply.Err = kv.start(args.ConfigNum, args.ClientId, args.RequestSeq, args.copy())
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	_, reply.Err = kv.start(args.ConfigNum, args.ClientId, args.RequestSeq, args.copy())
	return nil
}

func (kv *DisKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) error {
	kv.mu.Lock()
	if _, ok := kv.receivingShards[args.ConfigNum]; !ok {
		kv.receivingShards[args.ConfigNum] = make(map[int]ShardMigrationArgs)
	}
	kv.receivingShards[args.ConfigNum][args.Shard] = args.copy()
	kv.mu.Unlock()
	return nil
}

// when replica loses its disk content, it use this RPC to ask snapshot from other replicas
func (kv *DisKV) SnapshotTransfer(args *SnapshotTransferArgs, reply *SnapshotTransferReply) error {
	kv.mu.Lock()
	reply.Server, reply.Err = kv.me, ErrBrokenSnapshot
	if kv.px.Role() != paxos.Voter {
		kv.mu.Unlock()
		return nil
	}
	reply.State = kv.State()
	reply.PollSeq, reply.Err = kv.getPollSeq(), OK
	DPrintf("[SnapshotTransfer] gid %d server %d receive snapshot request from server %d", kv.gid, kv.me, args.Server)
	kv.mu.Unlock()
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
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

func (kv *DisKV) submit() {
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

func (kv *DisKV) apply(op Op) {
	kv.mu.Lock()
	i := op.PaxosSeq
	kv.applySeq = i
	notifyArg := notifyArgs{Value: "", Err: OK}
	DPrintf("[Apply] gid %d server %d pollSeq %d args %#v", kv.gid, kv.me, atomic.LoadInt32(&kv.pollSeq), op)
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
					DPrintf("[Apply Put] gid %d server %d config %d client %d seq %d index %d shard %d key %s value %s result %s", kv.gid, kv.me, kv.config.Num, arg.ClientId, arg.RequestSeq, i, shard, arg.Key, arg.Value, kv.data[arg.Key])
				} else {
					kv.data[arg.Key] += arg.Value
					DPrintf("[Apply Append] gid %d server %d config %d client %d seq %d index %d shard %d key %s value %s result %s", kv.gid, kv.me, kv.config.Num, arg.ClientId, arg.RequestSeq, i, shard, arg.Key, arg.Value, kv.data[arg.Key])
				}
				kv.cache[arg.ClientId] = arg.RequestSeq
			}
		}
		notifyArg.ClientId, notifyArg.RequestSeq = arg.ClientId, arg.RequestSeq
	} else if arg, ok := op.Args.(GetArgs); ok {
		shard := key2shard(arg.Key)
		if arg.ConfigNum != kv.config.Num {
			notifyArg.Err = ErrWrongGroup
		} else if _, ok := kv.ownShards[shard]; !ok {
			notifyArg.Err = ErrWrongGroup
		} else {
			notifyArg.Value, notifyArg.Err = kv.data[arg.Key], OK
		}
		DPrintf("[Apply Get] gid %d server %d config %d client %d seq %d index %d shard %d key %s value %s", kv.gid, kv.me, kv.config.Num, arg.ClientId, arg.RequestSeq, i, shard, arg.Key, kv.data[arg.Key])
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
	kv.px.Done(op.PaxosSeq)
	kv.persist()
	kv.notifyIfPresent(i, notifyArg)
	kv.mu.Unlock()
}

// apply new configuration
func (kv *DisKV) applyNewConf(newConfig shardmaster.Config) {
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
	DPrintf("[Apply New Config %d] gid %d server %d waitingShards %v oldShards %v newConfig %v", newConfig.Num, kv.gid, kv.me, kv.waitingShards, oldShards, newConfig)
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

func (kv *DisKV) pushShard(shard int, config shardmaster.Config, args ShardMigrationArgs) {
	gid := config.Shards[shard]
	servers := config.Groups[gid]
	pushCount, majority := 0, len(servers)/2+1 // push to a majority of server
	pushServerSet := make(map[string]struct{})
	for pushCount < majority {
		for _, server := range servers {
			if _, ok := pushServerSet[server]; ok {
				continue
			}
			var reply ShardMigrationReply
			if call(server, "DisKV.ShardMigration", &args, &reply) {
				pushServerSet[server] = struct{}{}
				pushCount += 1
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *DisKV) notifyIfPresent(seq int, arg notifyArgs) {
	if ch, ok := kv.notifyChs[seq]; ok {
		ch <- arg
		delete(kv.notifyChs, seq)
	}
}

func (kv *DisKV) requestSnapshot(args SnapshotTransferArgs, server int, replyCh chan SnapshotTransferReply) {
	var reply SnapshotTransferReply
	if !call(kv.servers[server], "DisKV.SnapshotTransfer", &args, &reply) {
		reply.Err, reply.Server = ErrRetry, server
	}
	replyCh <- reply
}

func (kv *DisKV) restart() {
	DPrintf("[Restart] gid %d server %d restart setting ", kv.gid, kv.me)
	kv.px.SetRole(paxos.NonVoter) // when restart, paxos can only act non-voter
	if kv.restartFromDisk() != nil { // have to request snapshot from other server
		DPrintf("[Restart ReadDisk failed] gid %d server %d restart read from disk error", kv.gid, kv.me)
		args := SnapshotTransferArgs{kv.me}
		// SnapshotReply from other server that has most complete state
		var bestReply SnapshotTransferReply
		majority, validReplyCnt, replyCh := len(kv.servers)/2+1, 0, make(chan SnapshotTransferReply)
		for i := range kv.servers {
			if i != kv.me {
				go kv.requestSnapshot(args, i, replyCh)
			}
		}
		for validReplyCnt < majority {
			reply := <-replyCh
			if reply.Err == OK {
				if reply.PollSeq > bestReply.PollSeq {
					bestReply = reply
				}
				validReplyCnt += 1
			} else {
				go kv.requestSnapshot(args, reply.Server, replyCh)
			}
		}
		kv.mu.Lock()
		kv.ReadState(bestReply.State)
		DPrintf("[Restart ReadSnapshot success] gid %d server %d pollSeq %d requestSeq %d applySeq %d data %v",
			kv.gid, kv.me, kv.getPollSeq(), kv.requestSeq, kv.applySeq, kv.data)
		kv.persist()
		kv.mu.Unlock()
	} else {
		kv.mu.Lock()
		DPrintf("[Restore Disk Success] gid %d server %d pollSeq %d requestSeq %d applySeq %d data %v",
			kv.gid, kv.me, kv.getPollSeq(), kv.requestSeq, kv.applySeq, kv.data)
		kv.mu.Unlock()
	}
	kv.px.SetRole(paxos.Voter)
}

func (kv *DisKV) run(restart bool) {
	if restart {
		kv.restart()
	}
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
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
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
// stateFile is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string, servers []string, me int, dir string, restart bool) *DisKV {
	kv := new(DisKV)
	kv.me = me
	kv.servers = servers
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.ps = StartPersister(dir)
	kv.config = shardmaster.Config{}

	kv.ownShards = make(IntSet)
	kv.receivingShards = make(map[int]map[int]ShardMigrationArgs)
	kv.waitingShards = make(IntSet)

	kv.requestSeq = 0
	kv.pollSeq = 0
	kv.applySeq = 0
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]int)
	kv.notifyChs = make(map[int]chan notifyArgs)
	kv.applyCh = make(chan Op, 100)

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go kv.run(restart)
	return kv
}
