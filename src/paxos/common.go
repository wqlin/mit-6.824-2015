package paxos

import (
	"log"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Err string

const (
	OK           = "OK"
	ErrRPCFail   = "ErrRPCFail"
	ErrForgotten = "ErrForgotten"
	ErrNotVoter  = "ERrrNoteVoter" // cannot vote because it's not a voter
)

type Proposal struct {
	ProposalNum int64
	Value       interface{}
}

type LogEntry struct {
	Index          int
	Fate           Fate
	MinProposalNum int64
	Proposal       Proposal
}

func (l LogEntry) copy() LogEntry {
	return LogEntry{l.Index,
		l.Fate,
		l.MinProposalNum,
		Proposal{l.Proposal.ProposalNum,
			l.Proposal.Value},
	}
}

// since multiple request can be concurrent so each request/response must carry an index for identification
type PrepareArgs struct {
	Server         int
	Index          int
	ProposalNumber int64
}

type PrepareReply struct {
	Err    Err
	Server int
	Entry  LogEntry
}

type AcceptArgs struct {
	Server   int
	Index    int
	Proposal Proposal
}

type AcceptReply struct {
	Err    Err
	Server int
	Entry  LogEntry
}

// send from paxos server when a majority of servers agree on the instance
type DecidedArgs struct {
	Server          int
	Index           int
	DecidedProposal Proposal
}

type DecidedReply struct {
	Err    Err
	Server int
}

type DoneBroadcastArgs struct {
	Server int
	Done   int
}

type DoneBroadcastReply struct {
	Err    Err
	Server int
	Done   int
}
