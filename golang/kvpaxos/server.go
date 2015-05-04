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
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
	seen	map[string] int64 // Unique request ID from client
	prevValues map[string] string // client name as key and value as value
	curValues  map[string] string
	curSeq	int // Highest seq number, remember that seq in px all continuous
	// all seq less than curSeq are reachable
}



func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := kv.makeOp(args.Key, "", false, GET_ID, args.OpId, args.Client)
	reply.Value = kv.reachPaxosAgreement(op)

  	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	op := kv.makeOp(args.Key, args.Value, args.DoHash, PUT_ID, args.OpId, args.Client)
	reply.PreviousValue = kv.reachPaxosAgreement(op)
		
  	return nil
}

func (kv *KVPaxos) makeOp(key string, value string, doHash bool, operTyped int, opId int64, client string) Op {
	return Op{Key: key, Value: value, Dohash: doHash, OperType: operTyped, OpId: opId, Client: client}
}


func (kv *KVPaxos) reachPaxosAgreement(op Op) string {
	var ok = false
	for !ok {
		uuid, exists := kv.seen[op.Client]
		if exists && uuid == op.OpId {
			return kv.prevValues[op.Client]
		}	
		seq := kv.curSeq + 1		
		decided, value := kv.px.Status(seq)
		var res Op
		if decided {
			res = value.(Op)
		} else {
			kv.px.Start(seq, op)
			res = kv.wait(seq)
		}
		ok = res.OpId == op.OpId
		kv.updateMap(res)
	}
	return kv.prevValues[op.Client]
}

// 一个Op进来怎么处理? 1 首先要看看是否以前已经处理过, 如果处理过就不更新返回
// 2 然后看看是不是Put型的Op,如果是的话就要更新数据结构
// 3 看看是不是dohash, 如果是的话计算hash 否则直接保存
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

