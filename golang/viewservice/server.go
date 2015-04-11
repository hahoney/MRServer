package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	/* unnecessary to record view history */
	curView View
	newView View // this is like a buffer to be updated when a tick()
	// arrives, plus the condition that 1) the current View is acked
	// by primary 2) something has changed to newView since the last
	// update
	recvTime  map[string]time.Time
	ackedPrim bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	acked := true

	vs.mu.Lock()

	if args.Me == vs.curView.Primary && args.Viewnum == vs.curView.Viewnum {
		vs.ackedPrim = true
	}

	if vs.viewComp(vs.curView, vs.newView) && vs.curView.Viewnum > 0 {
		acked = false
	} else {
		if vs.newView.Primary != vs.curView.Primary {
			if !vs.ackedPrim {
				acked = false
			}
		}
	}

	// modify the buffer View

	if vs.newView.Primary == "" || vs.newView.Backup == "" {
		if args.Viewnum != 0 || vs.newView.Backup != args.Me {
			vs.updateView(&vs.newView, args.Viewnum, args.Me)
		}
	} else {
		if args.Me == vs.newView.Primary && args.Viewnum == 0 {
			if args.Me != vs.newView.Backup {
				vs.newView.Primary = vs.newView.Backup
				vs.newView.Backup = ""
			}
		}
	}

	if acked {
		vs.newView.Viewnum += 1
		vs.ackedPrim = false
		vs.curView = vs.newView
	}

	vs.recvTime[args.Me] = time.Now()
	reply.View = vs.curView
	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.curView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	// Can not handle two server dead wait for Ping to fill in
	vs.mu.Lock()

	if vs.newView.Primary != "" {
		priTimePassed := time.Now().Sub(vs.recvTime[vs.newView.Primary])
		if priTimePassed > PingInterval*DeadPings {
			vs.newView.Primary = ""
		}
	}
	if vs.newView.Backup != "" {
		backTimePassed := time.Now().Sub(vs.recvTime[vs.newView.Backup])
		if backTimePassed > PingInterval*DeadPings {
			vs.newView.Backup = ""
		}
	}
	vs.mu.Unlock()
	return
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.recvTime = make(map[string]time.Time)
	vs.ackedPrim = false
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
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}

/* my func */

func (vs *ViewServer) updateView(outView *View, viewNum uint, me string) {
	if outView.Backup == "" {
		if outView.Primary == "" || outView.Primary == me {
			outView.Primary = me
		} else {
			outView.Backup = me
		}
	} else if outView.Primary == "" {
		outView.Primary = outView.Backup
		if outView.Backup != me {
			outView.Backup = me
		} else {
			outView.Backup = ""
		}
	}
}

func (vs *ViewServer) viewComp(u View, v View) bool {
	if u.Primary == v.Primary && u.Backup == v.Backup {
		return true
	}
	return false
}
