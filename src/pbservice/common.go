package pbservice

import "fmt"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongServer      = "ErrWrongServer"
	ErrWrongView        = "ErrWrongView"
	ErrTransferUnfinish = "ErrTransferUnfinish"
	ErrReplicationFail  = "ErrReplicationFail"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	RequestId       string
	ExpireRequestId string
	Key             string
	Value           string
	Op              string
	Viewnum         uint
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	Viewnum uint
}

type GetReply struct {
	Err   Err
	Value string
}

// make a copy of GetArgs
func copyGet(src *GetArgs) GetArgs {
	return GetArgs{
		Key:     src.Key,
		Viewnum: src.Viewnum}
}

// make a copy of PutAppendArgs
func copyPutAppend(src *PutAppendArgs) PutAppendArgs {
	return PutAppendArgs{
		RequestId:       src.RequestId,
		ExpireRequestId: src.ExpireRequestId,
		Key:             src.Key,
		Value:           src.Value,
		Op:              src.Op,
		Viewnum:         src.Viewnum}
}

type ReplicationArgs struct {
	Viewnum     uint
	Entries     []LogEntry
	CommitIndex int
}

type ReplicationReply struct {
	Err Err
}

// transfer entire database from primary to backup, use pull-based model
type TransferArgs struct {
	Viewnum uint
}

type TransferReply struct {
	Data        map[string]string
	Cache       map[string]struct{}
	Entries     []LogEntry
	ApplyIndex  int
	CommitIndex int
	Err         Err
}

func Max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
