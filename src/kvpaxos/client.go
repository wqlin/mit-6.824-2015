package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"
import (
	"time"
	"log"
)

type Clerk struct {
	servers    []string
	id         int64 // client identifier
	seq        int
	lastServer int
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
	ck.id = nrand()
	ck.seq = 0
	ck.lastServer = 0
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

	log.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.seq++
	arg := GetArgs{ck.id, ck.seq, key}
	DPrintf("[Get] client %d seq %d key %s", ck.id, ck.seq, key)
	for {
		reply := GetReply{}
		if call(ck.servers[ck.lastServer], "KVPaxos.Get", &arg, &reply) && reply.Err == OK {
			DPrintf("[Get Succeed] client %d seq %d key %s value %s", ck.id, ck.seq, key, reply.Value)
			return reply.Value
		}
		ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq++
	arg := PutAppendArgs{ck.id, ck.seq, key, value, op}
	if op == "Put" {
		DPrintf("[Put] client %d seq %d key %s value %s", ck.id, ck.seq, key, value)
	} else {
		DPrintf("[Append] client %d seq %d key %s value", ck.id, ck.seq, key, value)
	}
	for {
		reply := PutAppendReply{}
		if call(ck.servers[ck.lastServer], "KVPaxos.PutAppend", &arg, &reply) && reply.Err == OK {
			if op == "Put" {
				DPrintf("[Put Succeed] client %d seq %d key %s value %s", ck.id, ck.seq, key, value)
			} else {
				DPrintf("[Append Succeed] client %d seq %d key %s value %s", ck.id, ck.seq,  key, value)
			}
			return
		}
		ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
