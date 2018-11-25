package shardmaster

import (
	"log"
	"sort"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(gid, servers) -- replica group gid is joining, give it some shards.
// Leave(gid) -- replica group gid is retiring, hand off all its shards.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// Please don't change this file.
//

const NShards = 10

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

type Config struct {
	Num    int                // config number
	Shards [NShards]int64     // shard -> gid
	Groups map[int64][]string // gid -> servers[]
}

func (config Config) Copy() Config {
	newConfig := Config{}
	newConfig.Num = config.Num
	for shard, gid := range config.Shards {
		newConfig.Shards[shard] = gid
	}
	newConfig.Groups = make(map[int64][]string)
	for k, v := range config.Groups {
		newConfig.Groups[k] = append([]string{}, v...)
	}
	return newConfig
}

type JoinArgs struct {
	ClientId   int64
	RequestSeq int
	GID        int64    // unique replica group ID
	Servers    []string // group server ports
}

func (arg *JoinArgs) copy() JoinArgs {
	return JoinArgs{arg.ClientId, arg.RequestSeq, arg.GID, append([]string{}, arg.Servers...)}
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ClientId   int64
	RequestSeq int
	GID        int64
}

func (arg *LeaveArgs) copy() LeaveArgs {
	return LeaveArgs{arg.ClientId, arg.RequestSeq, arg.GID}
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ClientId   int64
	RequestSeq int
	Shard      int
	GID        int64
}

func (arg *MoveArgs) copy() MoveArgs {
	return MoveArgs{arg.ClientId, arg.RequestSeq, arg.Shard, arg.GID}
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	ClientId   int64
	RequestSeq int
	Num        int // desired config number
}

func (arg *QueryArgs) copy() QueryArgs {
	return QueryArgs{arg.ClientId, arg.RequestSeq, arg.Num}
}

type QueryReply struct {
	Err    Err
	Config Config
}

// return gid -> array of shards that belong to that group
func queryShardsByGroup(config *Config) map[int64][]int {
	mapping := make(map[int64][]int)
	for gid := range config.Groups {
		mapping[gid] = []int{}
	}
	for shard, gid := range config.Shards {
		if _, ok := mapping[gid]; ok {
			mapping[gid] = append(mapping[gid], shard)
		}
	}
	return mapping
}

func sortKeys(mapping map[int64][]int) []int64 {
	if len(mapping) == 0 {
		return []int64{}
	}
	keys := make([]int64, 0)
	for k := range mapping {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

// return (gid that has maximum number of shards, number of shards that gid have)
func findMax(mapping map[int64][]int) (int64, int) {
	gid, maxShards := int64(0), 0
	keys := sortKeys(mapping)
	for _, k := range keys {
		v := mapping[k]
		if len(v) > maxShards {
			gid = k
			maxShards = len(v)
		}
	}
	return gid, maxShards
}

// return (gid that has minimum number of shards, number of shards that gid have)
func findMin(mapping map[int64][]int) (int64, int) {
	gid, minShards := int64(0), 1<<31-1
	keys := sortKeys(mapping)
	for _, k := range keys {
		v := mapping[k]
		if len(v) < minShards {
			gid = k
			minShards = len(v)
		}
	}
	return gid, minShards
}
