package kvpaxos

import (
	"log"
)

const (
	OK       = "OK"
	ErrRetry = "ErrRetry"
)

type Err string

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type PutAppendArgs struct {
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
	ClientId   int64
	RequestSeq int
	Key        string
}

type GetReply struct {
	Err   Err
	Value string
}

type Op struct {
	PaxosSeq int // sequence number
	Args     interface{}
}

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{arg.ClientId, arg.RequestSeq, arg.Key}
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{arg.ClientId, arg.RequestSeq, arg.Key, arg.Value, arg.Op}
}
