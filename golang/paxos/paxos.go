package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

const (
	OK           = "OK"
	REJECT       = "REJECT"
	EMPTY_NUMBER = -1
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]
	// Your data here.
	instances   map[int]*PaxosInst
	doneSeqs    []int // store the min done seq number
	nPrep		int   // temp var to for new Id, instance could be unavailable
}

type PaxosInst struct {
	seq		int
	nPrep	int  // highest prep id seen
	nAgree	int
	vAgree	interface{}
	decided	bool
}

type PaxosArgs struct {
	Seq		int
	NPrep	int
	VPrep	interface{}
	// for passing Done only
	Done	int
	Me		int
}

type PaxosReply struct {
	Seq        	int
	NPrep		int
	VPrep	 	interface{}
	Accept     	string
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

func makeInst(seq int, nAgree int, v interface{}) (*PaxosInst){
	return &PaxosInst{seq: seq, nPrep: EMPTY_NUMBER, nAgree: nAgree, vAgree: v, decided: false}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//

// seq is the number for agreement instance
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		px.doPropose(seq, v)
	}()
}

func (px *Paxos) DumpValue() {
	for _, inst := range px.instances {
		fmt.Print("Seq ", inst.seq, ": ", inst.vAgree, " ")
	}
}


func (px *Paxos) doPropose(seq int, v interface{}) {
	for {
		agree, ok := px.sendPrepareToAll(seq, v)
		if ok {
			ok := px.sendAcceptToAll(agree)
			if ok {
				px.sendDecidedToAll(agree)
				break
			}
		}
	}
}

func (px *Paxos) sendPrepareToAll(seq int, v interface{}) (*PaxosInst, bool) {
	prepCount := 0
	agree := makeInst(seq, EMPTY_NUMBER, v)
	agree.nPrep = px.doPrepare()
	
	for _, peer := range px.peers {
		reply := px.sendPrepare(peer, agree)
		if reply.Accept == OK {
			prepCount++
			if agree.nAgree < reply.NPrep {
				agree.nAgree = reply.NPrep
				agree.vAgree = reply.VPrep
			}
		}
	}
	return agree, px.isMajority(prepCount)
}

func (px *Paxos) sendPrepare(peer string, agree *PaxosInst) *PaxosReply {
	args := &PaxosArgs{Seq: agree.seq, NPrep: agree.nPrep}
	reply := &PaxosReply{Accept:REJECT}
	if peer == px.peers[px.me] {
		px.ProcPrepare(args, reply)
	} else {
		call(peer, "Paxos.ProcPrepare", args, reply)
	}
	return reply
}

// Potential bugs should avoid: if the px.instance is empty,
// ProcPrepare must accept the value with OK but without satisfying 
// args.NPrep > px.nHiPrepSeen
func (px *Paxos) ProcPrepare(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	if _, ok := px.instances[seq]; !ok {
		px.instances[seq] = makeInst(seq, EMPTY_NUMBER, nil)
		reply.Accept = OK
	} else {
		if args.NPrep > px.instances[seq].nPrep {
			reply.Accept = OK
		}
	}
	if reply.Accept == OK {
		px.instances[seq].nPrep = args.NPrep
		reply.Seq = args.Seq
		reply.NPrep = px.instances[seq].nAgree
		reply.VPrep = px.instances[seq].vAgree
	}
	return nil
}

func (px *Paxos) doPrepare() int {
	if px.nPrep == EMPTY_NUMBER {
		px.nPrep = px.me
	} else {
		px.nPrep += len(px.peers)
	}
	return px.nPrep
}

func (px *Paxos) sendAcceptToAll(agree *PaxosInst) bool {
	accCount := 0
	for _, peer := range px.peers {
		reply := px.sendAccept(peer, agree)
		if reply.Accept == OK {
			accCount += 1
		}
	}
	//fmt.Print(px.isMajority(accCount))
	return px.isMajority(accCount)
}

func (px *Paxos) sendAccept(peer string, agree *PaxosInst) (*PaxosReply) {
	args := &PaxosArgs{Seq: agree.seq, NPrep: agree.nPrep, VPrep: agree.vAgree}
	reply := &PaxosReply{}
	if peer == px.peers[px.me] {
    	px.ProcAccept(args, reply)
	} else {
		call(peer, "Paxos.ProcAccept", args, reply)
	}
	return reply
}

func (px *Paxos) ProcAccept(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	reply.Accept = REJECT
	if 	_, exist := px.instances[args.Seq]; !exist {
		px.instances[seq] = makeInst(seq, EMPTY_NUMBER, nil)
	}
	if args.NPrep >= px.instances[seq].nPrep {
		px.instances[seq].nPrep = args.NPrep
		px.instances[seq].nAgree = args.NPrep
		px.instances[seq].vAgree = args.VPrep
		reply.Accept = OK
	}
	return nil
}

func (px *Paxos) sendDecidedToAll(agree *PaxosInst) {
	for _, peer := range px.peers {
		px.sendDecided(peer, agree)
	}
}

// the min done inst information about this server on other servers 
// should be updated when decided. It helps Min() function
// 
func (px *Paxos) sendDecided(peer string, agree *PaxosInst) {
	done := px.doneSeqs[px.me]
	args := &PaxosArgs{Seq: agree.seq, NPrep: agree.nPrep, VPrep: agree.vAgree, Done: done, Me: px.me}
	reply := &PaxosReply{Accept: OK} // Accept all
	if peer == px.peers[px.me] {
		px.ProcDecided(args, reply)
	} else {
		call(peer, "Paxos.ProcDecided", args, reply)
	}
}

func (px *Paxos) ProcDecided(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	if _, exist := px.instances[seq]; !exist {
		px.instances[seq] = makeInst(seq, EMPTY_NUMBER, nil)
	}
	px.instances[seq].vAgree = args.VPrep
	px.instances[seq].decided = true
	px.doneSeqs[args.Me] = args.Done
	return nil
}

func (px *Paxos) isMajority(count int) bool {
	return count > len(px.peers)/2
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	//px.mu.Lock()
	//defer px.mu.Unlock()	
	if px.doneSeqs[px.me] < seq {
		px.doneSeqs[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max := 0
	for m, _ := range px.instances {
		if m > max {
			max = m
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.doneSeqs[px.me]
	for _, i := range px.doneSeqs {
		if min > i {
			min = i
		}
	}
	
	for k, _ := range px.instances {
		if k < min && px.instances[k].decided {
			delete(px.instances, k)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.	
	min := px.Min()
	if seq < min {
		return false, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	inst, ok := px.instances[seq]
	if !ok {
		return false, nil
	}
	return inst.decided, inst.vAgree
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	//px.nHiPrepSeen = EMPTY_NUMBER
	px.instances = make(map[int]*PaxosInst)
	px.doneSeqs = make([]int, len(px.peers))
	
	for i := 0; i < len(px.peers); i++ {
		px.doneSeqs[i] = EMPTY_NUMBER
	}
	
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
