package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

import "time"

const Debug=1

const (
	EMPTY_NUMBER = -1
	GET_ID = 0
	PUT_ID = 1
	TRASH_ID = 2
	)



func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
	Key			string
	Value		string
	Dohash		bool
	OperType	int
	OpId		int64
	Client		string
	PrevValue	string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
	seen	map[string] int64 // Highest unique request ID from client
	prevValues map[string] string // previous value for each client
	curValues  map[string] string // kv store
	curSeq	int // Highest seq number processed
	// all seq less than curSeq are reachable
}



func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := kv.makeOp(args.Key, "", false, GET_ID, args.OpId, args.Client)
	reply.Value = kv.reachPaxosAgreement(op)
	
	//fmt.Println("Server ", kv.me, " receive GET from Client ", args.Client, " with key ", args.Key)
	//fmt.Println("Server ", kv.me, " response GET to Client ", args.Client, " with (key: ", args.Key, " value: ", reply.Value, ")")

  	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	op := kv.makeOp(args.Key, args.Value, args.DoHash, PUT_ID, args.OpId, args.Client)
	reply.PreviousValue = kv.reachPaxosAgreement(op)
	
	//fmt.Println("Server ", kv.me, " receive PUT from Client ", args.Client, " with (key: ", args.Key, " value: ", args.Value, ")")
	
	if args.DoHash {
		//fmt.Println("Server ", kv.me, " reply PUT to Client ", args.Client, " with previous key: ", reply.PreviousValue)
	}
		
  	return nil
}

func (kv *KVPaxos) makeOp(key string, value string, doHash bool, operTyped int, opId int64, client string) Op {
	return Op{Key: key, Value: value, Dohash: doHash, OperType: operTyped, OpId: opId, Client: client}
}

func (kv *KVPaxos) reachPaxosAgreement(op Op) string {
	
	opId, exists := kv.seen[op.Client]
	// Here is the trick ">=". "==" means the servers received a resend
	// from the clients. How could ">" happen ? It means the resend Op (which is
	// sent earlier) is late in paxos agreement. For example, Op1 is sent at moment A
	// and is resend due to network failure. Op2 is sent right after Op1 (moment B) but is processed
	// without retry. There is a small chance that Op2 reaches paxo agreement earlier than Op1 does.
	// The eventual agreed data store will have both Op1 and Op2. But the paxos sequence will certainly
	// differ. The solution is we simply return the previous value saved on local server without updating the map.
	// ">" solves the many partition test and some errors in unreliable test.
	if exists && opId >= op.OpId {
		return kv.prevValues[op.Client]
	}
		
	for {
		seq := kv.curSeq + 1		
		decided, value := kv.px.Status(seq)
		var result Op
		if decided {
			result = value.(Op)
		} else {
			kv.px.Start(seq, op)
			result = kv.wait(seq)
		}
		kv.updateMap(result)
		if result.OpId == op.OpId {
			break
		}
	}
	return kv.prevValues[op.Client]
}

func (kv *KVPaxos) updateMap(op Op) {
		
	previous, exist := kv.curValues[op.Key]
	if !exist {
		previous = ""
	}
	kv.prevValues[op.Client] = previous
	kv.seen[op.Client] = op.OpId
	
	if op.OperType == PUT_ID {
		if op.Dohash {
			kv.curValues[op.Key] = NextValue(previous, op.Value)
		} else {
			kv.curValues[op.Key] = op.Value
		}
	}
	kv.curSeq++
	kv.px.Done(kv.curSeq)
}


func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
  	for {
  		decided, val := kv.px.Status(seq)
  		if decided {
			return val.(Op)
    	}
    	time.Sleep(to)
    	if to < 10 * time.Second {
      	to *= 2
    	}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.curSeq = EMPTY_NUMBER
  kv.seen = make(map[string]int64)
  kv.prevValues = make(map[string]string) 
  kv.curValues = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

