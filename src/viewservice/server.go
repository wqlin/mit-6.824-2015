package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu           sync.Mutex
	l            net.Listener
	dead         int32 // for testing
	rpccount     int32 // for testing
	me           string
	acknowledged bool                 // keep track of whether the primary for the current view has acknowledged it
	primary      string               // current primary
	backup       string               // current backup
	idles        map[string]struct{}  // idle servers
	viewNum      uint                 // view number, increase monotonically
	servers      map[string]time.Time // keep track of the most recent time at which the viewservice has heard a Ping from each server
}

// find idle server. If no idle server available, return empty string
func (vs *ViewServer) findIdleServer() string {
	idle := ""
	if len(vs.idles) != 0 {
		for server, _ := range vs.idles {
			idle = server
			delete(vs.idles, server)
			break
		}
	}
	return idle
}

// promote backup to primary, return whether promotion success
func (vs *ViewServer) promoteBackup() bool {
	vs.primary = vs.backup
	vs.backup = vs.findIdleServer()
	return vs.primary != ""
}

// promote idle server to backup, return whether promotion success
func (vs *ViewServer) promoteIdle() bool {
	vs.backup = vs.findIdleServer()
	return vs.backup != ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if args.Viewnum == 0 { // server might crashed and restarted
		if vs.primary == "" && vs.viewNum == 0 { // viewservice first start should accept any server as first primary
			vs.primary = args.Me
			vs.viewNum += 1
			vs.acknowledged = false
		} else if vs.acknowledged && vs.backup == "" { // idle server can becomes backup
			vs.backup = args.Me
			vs.viewNum += 1
			vs.acknowledged = false
		} else { //  server restart and newly join are both possible
			if args.Me == vs.primary { // check if primary restart?
				if vs.promoteBackup() {
					vs.acknowledged = false // new primacy does not ack current view
				}
			} else if args.Me == vs.backup { // check if backup restart?
				vs.promoteIdle()
			}
			vs.idles[args.Me] = struct{}{} // put this server into idle servers
		}
	} else if args.Viewnum == vs.viewNum && args.Me == vs.primary { // primary ack this view
		vs.acknowledged = true
		if vs.backup == "" && vs.promoteIdle() {
			vs.viewNum += 1
			vs.acknowledged = false
		}
	} else if args.Me != vs.primary && args.Me != vs.backup {
		vs.idles[args.Me] = struct{}{}
	}
	vs.servers[args.Me] = time.Now()
	reply.View = View{Viewnum: vs.viewNum, Primary: vs.primary, Backup: vs.backup}
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = View{Viewnum: vs.viewNum, Primary: vs.primary, Backup: vs.backup}
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	now := time.Now()
	// check if there is any server dead
	for server, t := range vs.servers {
		if now.Sub(t) >= DeadPings*PingInterval {
			if server == vs.primary {
				if vs.acknowledged {
					vs.primary = ""
				}
			} else if server == vs.backup {
				if vs.acknowledged {
					vs.backup = ""
				}
			} else {
				delete(vs.idles, server)
			}
		}
	}
	if vs.acknowledged {
		if vs.primary == "" && vs.backup == "" { // both primary and backup are dead, can move to next view
			vs.primary = vs.findIdleServer()
			vs.backup = vs.findIdleServer()
			if vs.primary != "" {
				vs.viewNum += 1
				vs.acknowledged = false
			}
		} else if vs.primary == "" && vs.promoteBackup() { // promotion succeed, move to next view
			vs.acknowledged = false
			vs.viewNum += 1

		} else if vs.backup == "" && vs.promoteIdle() {
			vs.acknowledged = false
			vs.viewNum += 1
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.viewNum = 0
	vs.acknowledged = false
	vs.primary = ""
	vs.backup = ""
	vs.idles = make(map[string]struct{})
	vs.servers = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l
	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
