package shardkv

import (
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrRetry      = "ErrRetry"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string
type IntSet map[int]struct{}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type PutAppendArgs struct {
	ConfigNum  int
	ClientId   int64
	RequestSeq int
	Key        string
	Value      string
	Op         string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ConfigNum  int // config number
	ClientId   int64
	RequestSeq int
	Key        string
}

type GetReply struct {
	Err   Err
	Value string
}

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{arg.ConfigNum, arg.ClientId, arg.RequestSeq, arg.Key}
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{arg.ConfigNum, arg.ClientId, arg.RequestSeq, arg.Key, arg.Value, arg.Op}
}

type Op struct {
	PaxosSeq int // sequence number
	Args     interface{}
}

// rpc for pushing shards
type ShardMigrationArgs struct {
	Gid       int64
	Server    int
	ConfigNum int
	Shard     int
	Data      map[string]string
	Cache     map[int64]int
}

func (arg *ShardMigrationArgs) copy() ShardMigrationArgs {
	result := ShardMigrationArgs{arg.Gid, arg.Server, arg.ConfigNum, arg.Shard, make(map[string]string), make(map[int64]int)}
	for k, v := range arg.Data {
		result.Data[k] = v
	}
	for k, v := range arg.Cache {
		result.Cache[k] = v
	}
	return result
}

type ShardMigrationReply struct {
	Err Err
}
