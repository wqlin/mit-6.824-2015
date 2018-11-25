package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "encoding/gob"

func init() {
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(LogEntry{})
}

type LogEntry struct {
	Index int
	Args  interface{}
}

func copyEntry(entry LogEntry) LogEntry {
	if getArg, ok := entry.Args.(GetArgs); ok {
		return LogEntry{Index: entry.Index, Args: copyGet(&getArg)}
	} else if putAppendArg, ok := entry.Args.(PutAppendArgs); ok {
		return LogEntry{Index: entry.Index, Args: copyPutAppend(&putAppendArg)}
	}
	return LogEntry{}
}

type notifyArgs struct {
	Value string
	Err   Err
}

// implementation hit:
// 1. use pull-based model instead of push-based model
// when primary transfer complete key/value database to backup
// 2. when primary sends key/value database to backup, it
// should also sends the state to filter duplicates
// 3. instead of directly forwarding request to backup,
// primary should replicate to backup in log-based entry
type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	current      viewservice.View    // current view
	data          map[string]string   // storing key-value pair
	cache         map[string]struct{} // request id as kye cache put/append requests that server has processeds
	entries       []LogEntry          // log-based entries, then primary and backup can apply in order
	applyIndex    int                 // entries that apply to database
	commitIndex   int                 // entries that have been commit
	transferDone  bool                // whether backup in current view finish transfer
	notifyChs     map[int]chan notifyArgs
	notifyApplyCh chan struct{}
}

func (pb *PBServer) start(Viewnum uint, args interface{}) (string, Err) {
	pb.mu.Lock()
	currentViewnum, primary := pb.current.Viewnum, pb.current.Primary
	pb.mu.Unlock()
	if Viewnum != currentViewnum {
		return "", ErrWrongView
	} else if primary != pb.me {
		return "", ErrWrongServer
	}
	pb.mu.Lock()
	index := len(pb.entries)
	entry := LogEntry{Index: index, Args: args}
	pb.entries = append(pb.entries, entry)
	notifyCh := make(chan notifyArgs, 1)
	pb.notifyChs[index] = notifyCh
	pb.mu.Unlock()
	select {
	case result := <-notifyCh:
		return result.Value, result.Err
	case <-time.After(time.Second * 3):
		return "", ErrWrongServer
	}
	return "", OK
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	reply.Value, reply.Err = pb.start(args.Viewnum, copyGet(args))
	return nil
}

func (pb *PBServer) doPutAppend(args *PutAppendArgs) {
	delete(pb.cache, args.ExpireRequestId)
	if _, ok := pb.cache[args.RequestId]; !ok {
		if args.Op == "Put" {
			pb.data[args.Key] = args.Value
		} else {
			pb.data[args.Key] += args.Value
		}
		pb.cache[args.RequestId] = struct{}{}
	}
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	_, reply.Err = pb.start(args.Viewnum, copyPutAppend(args))
	return nil
}

// backup replication handler
func (pb *PBServer) Replicate(args *ReplicationArgs, reply *ReplicationReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if args.Viewnum != pb.current.Viewnum {
		reply.Err = ErrWrongView
	} else if !pb.transferDone {
		reply.Err = ErrTransferUnfinish
	} else {
		reply.Err = OK
		if len(args.Entries) != 0 {
			for _, entry := range args.Entries {
				if entry.Index >= len(pb.entries) {
					pb.entries = append(pb.entries, copyEntry(entry))
				} else {
					pb.entries[entry.Index] = copyEntry(entry)
				}
			}
		}
		pb.commitIndex = Max(pb.commitIndex, args.CommitIndex)
		if pb.applyIndex < pb.commitIndex {
			// DPrintf("[Backup apply] length %d apply %d commit %d\n", len(pb.entries), pb.applyIndex, pb.commitIndex)
			go pb.notifyApply()
		}
	}
	return nil
}

// database transfer handler
func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if args.Viewnum != pb.current.Viewnum {
		reply.Err = ErrWrongView
	} else {
		reply.Err = OK
		reply.Data, reply.Cache = make(map[string]string), make(map[string]struct{})
		for k, v := range pb.data {
			reply.Data[k] = v
		}
		for k := range pb.cache {
			reply.Cache[k] = struct{}{}
		}
		reply.Entries = make([]LogEntry, len(pb.entries))
		copy(reply.Entries, pb.entries)
		reply.ApplyIndex, reply.CommitIndex = pb.applyIndex, pb.commitIndex
		// DPrintf("[Primary Transfer] length %d, apply %d commit %d\n", len(pb.entries), pb.applyIndex, pb.commitIndex)
	}
	return nil
}

func (pb *PBServer) doTransfer() {
	pb.mu.Lock()
	Viewnum, primary := pb.current.Viewnum, pb.current.Primary
	pb.mu.Unlock()
	args := TransferArgs{Viewnum}
	for {
		reply := TransferReply{}
		if call(primary, "PBServer.Transfer", &args, &reply) {
			if reply.Err == OK {
				pb.mu.Lock()
				if pb.transferDone == false { // prevent duplicate messages
					// DPrintf("[Backup Transfer]Backup %s transfer data from primary %s at %d\n", pb.current.Backup, pb.current.Primary, pb.current.Viewnum)
					// DPrintf("Data: %v\n", reply.Data)
					/// DPrintf("[Backup Transfer] length %d, apply %d commit %d\n", len(reply.Entries), reply.ApplyIndex, reply.CommitIndex)
					pb.data = make(map[string]string)
					for k, v := range reply.Data {
						pb.data[k] = v
					}
					for k := range reply.Cache {
						pb.cache[k] = struct{}{}
					}
					pb.entries = make([]LogEntry, len(reply.Entries))
					copy(pb.entries, reply.Entries)
					pb.applyIndex, pb.commitIndex = reply.ApplyIndex, reply.CommitIndex
					pb.transferDone = true
				}
				pb.mu.Unlock()
				return
			}
		}
	}
}

func (pb *PBServer) notifyApply() {
	pb.notifyApplyCh <- struct{}{}
}

func (pb *PBServer) apply() {
	for !pb.isdead(){
		<-pb.notifyApplyCh
		pb.mu.Lock()
		if pb.applyIndex < pb.commitIndex {
			//if pb.current.Primary == pb.me {
			//	DPrintf("[Apply] Primary %s apply %d commit %d length %d, entries %#v\n", pb.me, pb.applyIndex, pb.commitIndex, len(pb.entries), pb.entries[pb.applyIndex+1:pb.commitIndex+1])
			//} else {
			//	DPrintf("[Apply] Backup %s apply %d commit %d length %d, entries %#v\n", pb.me, pb.applyIndex, pb.commitIndex, len(pb.entries), pb.entries[pb.applyIndex+1:pb.commitIndex+1])
			//}
			for i := pb.applyIndex + 1; i <= pb.commitIndex && i < len(pb.entries); i++ {
				entry := pb.entries[i]
				if getArg, ok := entry.Args.(GetArgs); ok {
					if notifyCh, find := pb.notifyChs[entry.Index]; find {
						notifyCh <- notifyArgs{Value: pb.data[getArg.Key], Err: OK}
						delete(pb.notifyChs, entry.Index)
					}
				} else if putAppendArg, ok := entry.Args.(PutAppendArgs); ok {
					pb.doPutAppend(&putAppendArg)
					if notifyCh, find := pb.notifyChs[entry.Index]; find {
						notifyCh <- notifyArgs{Value: "", Err: OK}
						delete(pb.notifyChs, entry.Index)
					}
				}
			}
		}
		pb.applyIndex = Min(pb.commitIndex, len(pb.entries)-1)
		pb.mu.Unlock()
	}
}

// replicate to backup in background
func (pb *PBServer) backgroundReplicate() {
	for !pb.isdead() {
		pb.mu.Lock()
		if pb.current.Primary == pb.me {
			if pb.current.Backup != "" {
				args := ReplicationArgs{Viewnum: pb.current.Viewnum, Entries: nil, CommitIndex: pb.commitIndex}
				if pb.commitIndex < len(pb.entries)-1 {
					// replicate from commitIndex
					args.Entries = append([]LogEntry{}, pb.entries[pb.commitIndex:]...)
				}
				backup := pb.current.Backup
				pb.mu.Unlock()
				reply := ReplicationReply{}
				if call(backup, "PBServer.Replicate", &args, &reply) {
					// DPrintf("[Replication]from %s to %s, args %v, reply %v\n", pb.current.Backup, pb.current.Primary, args, reply)
					pb.mu.Lock()
					if reply.Err == OK && len(args.Entries) != 0 {
						pb.commitIndex += len(args.Entries) - 1
					}
					if pb.applyIndex < pb.commitIndex {
						go pb.notifyApply()
					}
					pb.mu.Unlock()
				}
			} else {
				pb.commitIndex = len(pb.entries) - 1
				if pb.applyIndex < pb.commitIndex {
					go pb.notifyApply()
				}
				pb.mu.Unlock()
			}
		} else {
			pb.mu.Unlock()
		}
		time.Sleep(viewservice.PingInterval / 2)
	}
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	Viewnum := pb.current.Viewnum
	pb.mu.Unlock()
	v, error := pb.vs.Ping(Viewnum)
	if error == nil {
		pb.mu.Lock()
		if v.Viewnum != pb.current.Viewnum {
			pb.current = v
			if pb.current.Backup == pb.me {
				pb.transferDone = false
				go pb.doTransfer()
			}
		}
		pb.mu.Unlock()
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func (pb *PBServer) role() string {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.current.Primary == pb.me {
		return "Primary"
	} else if pb.current.Backup == pb.me {
		return "Backup"
	} else {
		return "Nothing"
	}
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.current, _ = pb.vs.Get()
	pb.data = make(map[string]string)
	pb.cache = make(map[string]struct{})
	pb.entries = []LogEntry{{0, ""}}
	pb.applyIndex = 0
	pb.commitIndex = 0
	pb.transferDone = false
	pb.notifyChs = make(map[int]chan notifyArgs)
	pb.notifyApplyCh = make(chan struct{})

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	go pb.apply()
	go pb.backgroundReplicate()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
