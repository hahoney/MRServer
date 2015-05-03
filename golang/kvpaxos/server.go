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
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
	seqMap	map[string] int // map key to seq id
	prevValues map[string] string
	curValues  map[string] string
	forgetList map[int] bool // seq to forget
	curSeq	int // Highest seq number, remember that seq in px all continuous
	// all seq less than curSeq are reachable
}

/* Ok, I finally got it. Paxos learning procedure:
if a server is left behind, it sends new proposed value
to seq. If the majority has another value, they will respond
and make new decision so that the sender will agree on that value
the initially proposed value will be ignored.
To update, start with any op, if it is different from what 
we get, keep it and move to the next seq
Where to stop? The seq at which the sent op and recv op are exactly the same
*/

/* How the hashPut works does it hash the previous hashed value with the current
unhashed one or both are unhashed? What if the previous hash is empty? Guess Guess guess!!!!
*/

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  // How do we know whether other minority nodes are learning ?
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := kv.makeOp(args.Key, "", false, GET_ID, args.OpId)
	seq := kv.curSeq + 1
	
	for {
		decided, value := kv.px.Status(seq)
		
		if !decided {
			kv.px.Start(seq, op)
			kv.wait(seq)
		} else {
			check := value.(Op)
			if check.OpId == op.OpId {
				break
			} else {
				if check.OperType == PUT_ID {
					kv.updateMap(check, seq)
					kv.updatePrevMap(check)
				}
				seq++
			}
		}
	}
	seq, exist := kv.seqMap[args.Key]
	if exist {
		_, value := kv.px.Status(seq)
		kv.updatePrevMap(value.(Op))
	}
	reply.Value = kv.curValues[args.Key]
	
  	return nil
}

func (kv *KVPaxos) updatePrevMap(value Op) {
	key := value.Key
	if _, exist := kv.curValues[key]; !exist {
		kv.curValues[key] = ""
	}
	if _, exist := kv.prevValues[key]; !exist {
		kv.prevValues[key] = ""
	}
	
	if kv.curValues[key] != value.Value {
		kv.prevValues[key] = kv.curValues[key]
		if value.Dohash {
			kv.curValues[key] = CalcHash(kv.prevValues[key], value.Value)
		} else {
			kv.curValues[key] = value.Value
		}
	}
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := kv.makeOp(args.Key, args.Value, args.DoHash, PUT_ID, args.OpId)
	doHash := args.DoHash
 	seq := kv.curSeq + 1
	
	for {
		decided, value := kv.px.Status(seq)
		if !decided {
			kv.px.Start(seq, op)
			kv.wait(seq)
		} else {
			check := value.(Op)
			if check.OpId == op.OpId {
				kv.updateMap(check, seq)
				kv.updatePrevMap(check)
				break
			} else {
				if check.OperType == PUT_ID {
					kv.updateMap(check, seq)
					kv.updatePrevMap(check)
				}
			}
			seq++
		}		
	}
	if doHash {
		reply.PreviousValue = kv.prevValues[args.Key]
	}
  	return nil
}

func (kv *KVPaxos) makeOp(key string, value string, doHash bool, operTyped int, opId int64) Op {
	return Op{Key: key, Value: value, Dohash: doHash, OperType: operTyped, OpId: opId}
}


func (kv *KVPaxos) updateMap(op Op, seq int) {
	// different seq may have same op
	previous, exist := kv.curValues[op.Key]
	if !exist {
		previous = ""
	}
	kv.prevValues[op.Key] = previous
	kv.seqMap[op.Key] = seq
	
	if op.OperType == PUT_ID {
		if op.Dohash {
			kv.curValues[op.Key] = CalcHash(previous, op.Value)
		} else {
			kv.curValues[op.Key] = op.Value
		}
	}
	kv.curSeq++
	kv.px.Done(kv.curSeq)
}



/*
func (kv *KVPaxos) updateMap() {
	max := kv.px.Max()
	for seq := kv.curSeq + 1; seq <= max; seq++ {
		decided, value := kv.px.Status(seq)
		// forget all GET, do not update
		if value != nil {
			operType := value.(Op).OperType
			if operType == GET_ID {
				kv.forgetList[seq] = true
				continue
			}
		}
		// update seqMap and forget list when overwrite
		if decided {
			key := value.(Op).Key
			if _, exist := kv.seqMap[key]; exist {
				oldSeq := kv.seqMap[key]
				kv.forgetList[oldSeq] = true
			}		
			kv.seqMap[key] = seq
		} else {
			if value != nil {
				key := value.(Op).Key
				kv.seqMap[key] = seq
			}
		}
	}
	// update min
	minSeq := kv.px.Min()
	for {
		if ok, _ := kv.forgetList[minSeq]; ok {
			delete(kv.forgetList, minSeq)
			minSeq++
		} else {
			break
		}
	} 
	kv.px.Done(minSeq - 1) 
	kv.curSeq = max
}
*/

func (kv *KVPaxos) dumpSeqMap() {
	fmt.Print("Node ", kv.me)
	for key, seq := range kv.seqMap {
		fmt.Print(" ( Seq ", seq, " Key ", key, " ) ")
	}
}


func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
  	for {
  		decided, value := kv.px.Status(seq)
  		if decided == true {
    		return value.(Op)
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
  kv.seqMap = make(map[string]int)
  kv.forgetList = make(map[int]bool)
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

