package paxos

// this file contains paxos rpc handler
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	r := px.handlePrepareMsg(args)
	reply.Entry, reply.Err, reply.Server = r.Entry, r.Err, px.me
	// DPrintf("[%d Prepare at %d from %d] args Proposal %+v reply %+v", px.me, args.Index, args.Server, args.ProposalNumber, reply)
	return nil
}

func (px *Paxos) handlePrepareMsg(args *PrepareArgs) PrepareReply {
	if px.Role() != Voter {
		return PrepareReply{Err: ErrNotVoter}
	}
	px.mu.Lock()
	if args.Index < px.offset {
		px.mu.Unlock()
		return PrepareReply{Err: ErrForgotten}
	}
	idx, proposalNum := args.Index, args.ProposalNumber
	entry := px.entry(idx)
	DPrintf("[%d Prepare at %d from %d] entry Fate %s proposalNum %d", px.me, idx, args.Server, entry.Fate.String(), proposalNum)
	if proposalNum >= entry.MinProposalNum {
		entry.MinProposalNum = proposalNum
		px.setEntry(idx, entry)
	}
	reply := PrepareReply{OK, px.me, entry.copy()}
	px.mu.Unlock()
	return reply
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	r := px.handleAcceptMsg(args)
	reply.Entry, reply.Err, reply.Server = r.Entry, r.Err, px.me
	// DPrintf("[%d Accept at %d from %d] proposal %+v reply %+v", px.me, args.Index, args.Server, args.Proposal, reply)
	return nil
}

func (px *Paxos) handleAcceptMsg(args *AcceptArgs) AcceptReply {
	if px.Role() != Voter {
		return AcceptReply{Err: ErrNotVoter}
	}
	px.mu.Lock()
	if args.Index < px.offset {
		px.mu.Unlock()
		return AcceptReply{Err: ErrForgotten}
	}
	idx, proposal := args.Index, args.Proposal
	entry := px.entry(idx)
	oldProposal := entry.Proposal
	if proposal.ProposalNum >= entry.MinProposalNum {
		entry.MinProposalNum, entry.Proposal = proposal.ProposalNum, Proposal{ProposalNum: proposal.ProposalNum, Value: proposal.Value}
		px.setEntry(idx, entry)
	}
	DPrintf("[%d Accept at %d from %d] oldProposal %v newProposal %v", px.me, idx, args.Server, oldProposal, entry.Proposal)
	reply := AcceptReply{OK, px.me, entry.copy()}
	px.mu.Unlock()
	return reply
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.handleDecidedMsg(args)
	reply.Err, reply.Server = OK, px.me
	return nil
}

func (px *Paxos) handleDecidedMsg(args *DecidedArgs) {
	px.mu.Lock()
	if args.Index < px.offset {
		px.mu.Unlock()
		return
	}
	idx, proposal := args.Index, args.DecidedProposal
	entry := px.entry(idx)
	if entry.Fate == Decided {
		px.mu.Unlock()
		return
	}
	oldProposal := entry.Proposal
	if proposal.ProposalNum >= entry.MinProposalNum {
		entry.MinProposalNum = proposal.ProposalNum
		entry.Fate, entry.Proposal = Decided, Proposal{ProposalNum: proposal.ProposalNum, Value: proposal.Value}
		px.setEntry(idx, entry)
		DPrintf("[%d Decided at %d from %d] oldProposal %+v newProposal %+v", px.me, idx, args.Server, oldProposal, entry.Proposal)
	} else {
		DPrintf("[%d Not Decided at %d from %d] oldProposal %+v newProposal %+v", px.me, idx, args.Server, oldProposal, entry.Proposal)
	}
	px.mu.Unlock()
}

func (px *Paxos) DoneBroadcast(args *DoneBroadcastArgs, reply *DoneBroadcastReply) error {
	px.setDoneFor(args.Server, args.Done)
	reply.Err, reply.Server, reply.Done = OK, px.me, px.getDoneFor(px.me)
	return nil
}
