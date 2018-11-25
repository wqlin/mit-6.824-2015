package shardmaster

//
// Shardmaster clerk.
// Please don't change this file.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"sync"
	"time"
)

const RPCRetryInterval = 100 * time.Millisecond

type Clerk struct {
	servers  []string // shardmaster replicas
	mu       sync.Mutex
	clientId int64
	seq      int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.seq = 0
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

func (ck *Clerk) getNextSeq() int {
	ck.mu.Lock()
	ck.seq ++
	seq := ck.seq
	ck.mu.Unlock()
	return seq
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{ck.clientId, ck.getNextSeq(), num}
	//DPrintf("[Query] client %d seq %d num %d", ck.clientId, args.RequestSeq, num)
	for {
		for _, srv := range ck.servers {
			reply := QueryReply{}
			ok := call(srv, "ShardMaster.Query", &args, &reply)
			if ok && reply.Err == OK {
				//DPrintf("[Query Succeed] client %d seq %d num %d config %v", ck.clientId, args.RequestSeq, num, reply.Config)
				return reply.Config
			}
		}
		time.Sleep(RPCRetryInterval)
	}
}

func (ck *Clerk) Join(gid int64, servers []string) {
	args := JoinArgs{ck.clientId, ck.getNextSeq(), gid, servers}
	DPrintf("[Join] client %d seq %d gid %d servers %v", ck.clientId, args.RequestSeq, gid, servers)
	for {
		for _, srv := range ck.servers {
			reply := JoinReply{}
			ok := call(srv, "ShardMaster.Join", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("[Join Succeed] client %d seq %d gid %d servers %v", ck.clientId, args.RequestSeq, gid, servers)
				return
			}
		}
		time.Sleep(RPCRetryInterval)
	}
}

func (ck *Clerk) Leave(gid int64) {
	args := LeaveArgs{ck.clientId, ck.getNextSeq(), gid}
	DPrintf("[Leave] client %d seq %d gid %d", ck.clientId, args.RequestSeq, gid)
	for {
		for _, srv := range ck.servers {
			reply := LeaveReply{}
			ok := call(srv, "ShardMaster.Leave", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("[Leave Succeed] client %d seq %d gid %d", ck.clientId, args.RequestSeq, gid)
				return
			}
		}
		time.Sleep(RPCRetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int64) {
	args := MoveArgs{ck.clientId, ck.getNextSeq(), shard, gid}
	DPrintf("[Move] client %d seq %d gid %d shard %d", ck.clientId, args.RequestSeq, gid, shard)
	for {
		for _, srv := range ck.servers {
			reply := MoveReply{}
			ok := call(srv, "ShardMaster.Move", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("[Move Succeed] client %d seq %d gid %d shard %d", ck.clientId, args.RequestSeq, gid, shard)
				return
			}
		}
		time.Sleep(RPCRetryInterval)
	}
}
