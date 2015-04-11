package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"
import "errors"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	view    	viewservice.View
	store   	map[string]string
	tag        	map[string]string
	prev		map[string]string
	mu      	sync.Mutex
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	if pb.tag[args.Key] == args.Tag {
		reply.PreviousValue = pb.prev[args.Key]
		reply.Err = OK
		pb.mu.Unlock()
		return nil
	}
	
	if args.DoHash {
		value := strconv.Itoa(int(hash(pb.prev[args.Key] + args.Value)))
		if value != pb.store[args.Key] {
			pb.tag[args.Key] = args.Tag
			pb.prev[args.Key] = pb.store[args.Key]
			pb.store[args.Key] = strconv.Itoa(int(hash(pb.store[args.Key] + args.Value)))
		}
		reply.PreviousValue = pb.prev[args.Key]
	} else {
		if args.Value != pb.store[args.Key] {
			pb.tag[args.Key] = args.Tag
			pb.prev[args.Key] = pb.store[args.Key]
			pb.store[args.Key] = args.Value
		}
	}
	
	if pb.me == pb.view.Primary && pb.view.Backup != "" {
		for call(pb.view.Backup, "PBServer.DoForward", 
		&ForArgs{args.Key, pb.store[args.Key], args.Tag, pb.prev[args.Key]}, &ForReply{}) == false {
		}
	}
	
	reply.Err = OK
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) DoForward(args *ForArgs, reply *ForReply) error {
	if pb.me == pb.view.Primary {
		reply.Err = ErrWrongServer
		return errors.New("Wrong server\n")
	}
	pb.store[args.Key] = args.Value
	pb.tag[args.Key] = args.Tag
	pb.prev[args.Key] = args.PreviousValue
	reply.Err = OK
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	if pb.me == pb.view.Primary {
		reply.Value, _ = pb.store[args.Key]
		reply.Err = OK
	} else if pb.me == pb.view.Backup && pb.view.Primary != "" {
		for call(pb.view.Primary, "PBServer.Get", args, reply) == false {
			time.Sleep(viewservice.PingInterval)
		}
	}
	pb.mu.Unlock()
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	curBackup := pb.view.Backup
	pb.view, _ = pb.vs.Ping(pb.view.Viewnum)
	if pb.me == pb.view.Primary && pb.view.Backup != "" && curBackup != pb.view.Backup {
		// dump the store
		for key, value := range pb.store {
			args := &ForArgs{Key: key, Value: value, Tag: pb.tag[key], PreviousValue: pb.prev[key] }
			reply := &ForReply{}
			for call(pb.view.Backup, "PBServer.DoForward", args, reply) == false {
				time.Sleep(viewservice.PingInterval)
			}
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.store = make(map[string]string)
	pb.tag = make(map[string]string)
	pb.view, _ = pb.vs.Ping(0)
	pb.prev = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
