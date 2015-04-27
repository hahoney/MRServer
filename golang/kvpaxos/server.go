/*  Examples! examples! Cannot understand the outputs in test files.
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
	Key		string
	Value	string
	Dohash	bool
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
	curSeq	int // highest unbroken seq to fill or update a new key
	// all seq less than curSeq are reachable
	prevOp	Op
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  // How do we know whether other minority nodes are learning ?
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.UpdateMap(true)
	 kv.px.Status(kv.curSeq + 1)
	//if value != nil { 
		fmt.Println("hhahahahha ", kv.me)
	//}
	
	seq, exist := kv.seqMap[args.Key]
	if exist {
		kv.wait(seq) 
		ok, value := kv.px.Status(seq)
		//fmt.Println("node ", kv.me, " seq ", seq, " key ", args.Key, " value ", value.(Op).Value)
		if ok { // decided
			//fmt.Println("seq is ", seq, " minseq is ", kv.px.Min(), " max is ", kv.px.Max())
			reply.Value = value.(Op).Value
					fmt.Println(kv.me, " wuwuuwuwuwuwuw ", value.(Op).Value)
		}
	}
  	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.UpdateMap(true)
	key := args.Key
	value := args.Value
	doHash := args.DoHash	
	op := Op{Key:key, Value:value, Dohash: doHash}
	
	seq := kv.curSeq + 1
	kv.px.Start(seq, op)
	kv.wait(seq)
	kv.seqMap[key] = seq
	kv.curSeq++
	
	if doHash {
		reply.PreviousValue = kv.prevOp.Value
		kv.prevOp = op
	}		
  	return nil
}

/* Update seqMap and th curSeq pt to max. max is the highest
   agreed seq by all majority nodes. Or if undecided value exists
   return the lowest seq below which all seq have been agreed */ 
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
				result = seq - 1
				break
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


func (kv *KVPaxos) wait(seq int) {
	to := 10 * time.Millisecond
  	for {
  		decided, _ := kv.px.Status(seq)
  		if decided == true {
    		return 
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
  kv.curSeq = -1
  kv.seqMap = make(map[string]int)
  kv.prevOp = Op{}


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

