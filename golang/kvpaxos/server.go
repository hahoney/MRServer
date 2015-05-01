/* Keep Notes while working on the code
   1 seqMap could expire, update the seqMap before Put and Get. 
   Failed first time: seq has duplicate keys

*/

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
	forgetList map[int] bool // seq to forget
	curSeq	int // Highest seq number, remember that seq in px all continuous
	// all seq less than curSeq are reachable
	prevOp	Op
	operId	int
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


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  // How do we know whether other minority nodes are learning ?
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.updateMap()
	
	op := kv.makeOp(args.Key, "", false, GET_ID, args.OpId)
	seq := kv.curSeq + 1
	ok := false
	for !ok {
		decided, value := kv.px.Status(seq)
		
		if !decided {
			kv.px.Start(seq, op)
			kv.wait(seq)
		} else {
			check := value.(Op).OpId
			if check == op.OpId {
				ok = true
			} else {
				op = value.(Op)
				key := op.Key
				kv.seqMap[key] = seq
				seq++
			}
		}
	}

	key := args.Key
	seq, exist := kv.seqMap[key]

	if exist {
		kv.wait(seq)
		decided, value := kv.px.Status(seq)
		if decided {
			reply.Value = value.(Op).Value
		}
	}
	fmt.Println("Node ", kv.me)
	kv.px.DumpValue()
	kv.dumpSeqMap()
	fmt.Println()
	
  	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.updateMap() // Might miss some seq between two Put Ops
	op := kv.makeOp(args.Key, args.Value, args.DoHash, PUT_ID, args.OpId)
	doHash := args.DoHash
 	seq := kv.curSeq + 1
	
	// Update the seq to keep consistency
	for {
		decided, value := kv.px.Status(seq)
		
		if !decided {
			kv.px.Start(seq, op)
			kv.wait(seq)
		} else {
			check := value.(Op).OpId
			if check == op.OpId {
				break
			} else {
				op = value.(Op)
				key := op.Key
				kv.seqMap[key] = seq
				seq++
			}
		}		
	}
	
	fmt.Println("Node ", kv.me,  " put seq ", seq, " Key ", args.Key, " value ", args.Value)

	if doHash {
		reply.PreviousValue = kv.prevOp.Value
		kv.prevOp = op
	}		
  	return nil
}

func (kv *KVPaxos) makeOp(key string, value string, doHash bool, operTyped int, opId int64) Op {
	return Op{Key: key, Value: value, Dohash: doHash, OperType: operTyped, OpId: opId}
}

/* Can I take the undecided value. What is the meaning
of decided. Is it processing? or empty forever?
A value undecided and empty is unaccepted
A value undecided but not empty is accepted
A value decided is decided
we want those decided and to be decided values
*/
func (kv *KVPaxos) updateMap() {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
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


func (kv *KVPaxos) dumpSeqMap() {
	for key, seq := range kv.seqMap {
		fmt.Print("Seq ", seq, " Key ", key, "# ")
	}
}


/* Update seqMap and th curSeq pt to max. max is the highest
   agreed seq by all majority nodes. Or if undecided value exists
   return the lowest seq below which all seq have been agreed 
   update a seq 1 2 3 4 5 6 7 8 
   from 3 to 8 is not seen by a node yet. Update map
   the curSeq is at 2
   begin scan 3 if its key is same as one in the map, overwrite and save
   the overwritten seq to the garbage list. if new, insert to the map
   after the scan curSeq is at 8
   1 could some seq be empty ? How to do with it
   2 how to determine the garbage list
   all new incoming seq must be larger than max
*/ 
/*
func (kv *KVPaxos) UpdateMap(isPut bool) {
	max := kv.px.Max()
	forgetList := make(map[int]bool)
	result := max
	for seq := kv.curSeq + 1; seq <= max; seq++{
		decided, value := kv.px.Status(seq)
		if decided {
			_, ok := kv.seqMap[value.(Op).Key]
			if ok {  // is already in map then overwrite should be recorded
				forgetList[seq] = true
			}
			kv.seqMap[value.(Op).Key] = seq
		} else { // seq is not decided
			if value == nil  && isPut { // not accepted
				//result = seq - 1
				//break
			} else { // accepted by this node
				kv.seqMap[value.(Op).Key] = seq
			}
		}
	}
	minSeq := kv.px.Min()
	for {
		if _, ok := forgetList[minSeq]; ok {
			minSeq++
		} else {
			break
		}
	}
	kv.px.Done(minSeq)
	kv.curSeq = result
}
*/

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
  kv.prevOp = Op{}
  kv.operId = EMPTY_NUMBER
  kv.forgetList = make(map[int]bool)


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

