package diskv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"
import "crypto/rand"
import "math/big"
const QueryMaxSleepDuration = 100 * time.Millisecond

type Clerk struct {
	mu     sync.Mutex // one RPC at a time
	sm     *shardmaster.Clerk
	config shardmaster.Config
	id     int64
	seq    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	ck.id = nrand()
	ck.seq = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

//
// fetch the stateFile value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq ++
	args := GetArgs{-1, ck.id, ck.seq, key}
	// DPrintf("[Get] client %d seq %d key %s shard %d", ck.id, ck.seq, key, key2shard(key))
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if ok { // try each server in the shard's replication group.
			args.ConfigNum = ck.config.Num
			for i, srv := range servers {
				var reply GetReply
				ok := call(srv, "DisKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					 DPrintf("[Get Succeed] client %d seq %d gid %d server %d key %s value %s", ck.id, ck.seq, gid, i, key, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				time.Sleep(QueryMaxSleepDuration)
			}
		}
		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

// send a Put or Append request.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq ++
	args := PutAppendArgs{-1, ck.id, ck.seq, key, value, op}
	//if op == "Put" {
	//	DPrintf("[Put] client %d seq %d key %s value %s shard %d", ck.id, ck.seq, key, value, key2shard(key))
	//} else {
	//	DPrintf("[Append] client %d seq %d key %s value %s shard %d", ck.id, ck.seq, key, value, key2shard(key))
	//}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if ok { // try each server in the shard's replication group.
			args.ConfigNum = ck.config.Num
			for i, srv := range servers {
				var reply PutAppendReply
				ok := call(srv, "DisKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					if op == "Put" {
						DPrintf("[Put Succeed] client %d seq %d gid %d server %d key %s value %s", ck.id, ck.seq, gid, i, key, value)
					} else {
						DPrintf("[Append Succeed] client %d seq %d gid %d server %d key %s value %s", ck.id, ck.seq, gid, i, key, value)
					}
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				time.Sleep(QueryMaxSleepDuration)
			}
		}
		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
