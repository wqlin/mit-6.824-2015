package kvpaxos

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
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(Op{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type notifyArgs struct {
	ClientId   int64
	RequestSeq int
	Value      string
	Err        Err
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	requestSeq int32 // index of next entry to start
	pollSeq    int32 // index of next entry to poll
	data       map[string]string
	cache      map[int64]int
	notifyChs  map[int]chan notifyArgs
	applyCh    chan Op
}

func (kv *KVPaxos) nextRequestSeq() int32 {
	seq := atomic.LoadInt32(&kv.requestSeq)
	kv.setRequestSeq(seq + 1)
	return seq
}

func (kv *KVPaxos) setRequestSeq(i int32) {
	atomic.StoreInt32(&kv.requestSeq, i)
}

func (kv *KVPaxos) poll() {
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

func (kv *KVPaxos) start(clientId int64, requestSeq int, args interface{}) (string, Err) {
	seq := int(kv.nextRequestSeq())
	kv.mu.Lock()
	kv.px.Start(seq, Op{PaxosSeq: seq, Args: args})
	notifyCh := make(chan notifyArgs, 1)
	DPrintf("[Server %d Start %d] ClientId %d", kv.me, seq, clientId)
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

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	reply.Value, reply.Err = kv.start(args.ClientId, args.RequestSeq, args.copy())
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	_, reply.Err = kv.start(args.ClientId, args.RequestSeq, args.copy())
	return nil
}

func (kv *KVPaxos) apply(op Op) {
	i := op.PaxosSeq
	kv.mu.Lock()
	notifyArg := notifyArgs{-1, -1, "", ErrRetry}
	if putAppendArg, ok := op.Args.(PutAppendArgs); ok {
		if kv.cache[putAppendArg.ClientId] < putAppendArg.RequestSeq {
			if putAppendArg.Op == "Put" {
				kv.data[putAppendArg.Key] = putAppendArg.Value
			} else {
				kv.data[putAppendArg.Key] += putAppendArg.Value
			}
			kv.cache[putAppendArg.ClientId] = putAppendArg.RequestSeq
			DPrintf("[Server %d Key %s execute PutAppend Client %d Seq %d] index %d value %s result %s", kv.me, putAppendArg.Key, putAppendArg.ClientId, putAppendArg.RequestSeq, i, putAppendArg.Value, kv.data[putAppendArg.Key])
		}
		notifyArg.ClientId, notifyArg.RequestSeq, notifyArg.Value, notifyArg.Err = putAppendArg.ClientId, putAppendArg.RequestSeq, "", OK
	} else if getArg, ok := op.Args.(GetArgs); ok {
		DPrintf("[Server %d Key %s execute Get Client %d Seq %d] index %d value %s", kv.me, getArg.Key, getArg.ClientId, getArg.RequestSeq, i, kv.data[getArg.Key])
		notifyArg.ClientId, notifyArg.RequestSeq, notifyArg.Value, notifyArg.Err = getArg.ClientId, getArg.RequestSeq, kv.data[getArg.Key], OK
	}
	kv.notifyIfPresent(i, notifyArg)
	kv.mu.Unlock()
	kv.px.Done(op.PaxosSeq)
}

func (kv *KVPaxos) notifyIfPresent(seq int, arg notifyArgs) {
	if ch, ok := kv.notifyChs[seq]; ok {
		ch <- arg
		delete(kv.notifyChs, seq)
	}
}

func (kv *KVPaxos) run() {
	go kv.poll()
	for !kv.isdead() {
		op := <-kv.applyCh
		kv.apply(op)
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	kv := new(KVPaxos)
	kv.me = me
	kv.requestSeq = 0
	kv.pollSeq = 0
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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go kv.run()
	return kv
}
