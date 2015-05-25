package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug=0

const (
	GET_TYPE = 1
	PUT_TYPE = 2
	RECONFIGURE_TYPE = 3
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  // Your definitions here.
  Key string
  Value string
  DoHash bool
  TimeStamp int64
  OpType int
  Client string
  NewConfig shardmaster.Config
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  kvStore map[string]string // key -> value
  prevValue map[string]string   // client -> value
  seen map[string]int64  // highest seen number
  curOp int // current operation number
  curConfig shardmaster.Config // current config

}


// Auxiliary functions
// reach agreement within group
func (kv *ShardKV) reachPaxosAgreement(op Op) (Err, string) {
	var result Op
	
	if op.OpType == RECONFIGURE_TYPE {
		if kv.curConfig.Num >= op.NewConfig.Num {
			return OK, ""
		}
	}
	if op.OpType == PUT_TYPE || op.OpType == GET_TYPE {
		shard := key2shard(op.Key)
		if kv.gid != kv.curConfig.Shards[shard] {
			fmt.Println("Wrong group")
//			return ErrWrongGroup, ""
		}
		// same as kvpaxos
		timeStamp, exist := kv.seen[op.Client]
		if exist && op.TimeStamp <= timeStamp {
			return OK, kv.prevValue[op.Client]
		}
	}
	
	for {
		seq := kv.curOp + 1
		decided, log := kv.px.Status(seq)
		if decided {
			result = log.(Op)
		} else {
			kv.px.Start(seq, op)
			result = kv.wait(seq)
		}
		kv.applyOperation(result)
		if result.TimeStamp == op.TimeStamp {
			break
		}
	}
	return OK, kv.prevValue[op.Client]
}


func (kv *ShardKV) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}

func (kv *ShardKV) applyOperation(op Op) {
	if op.OpType == PUT_TYPE {
		kv.applyPut(op)
	}
	if op.OpType == GET_TYPE {
		kv.applyGet(op)
	}
	if op.OpType == RECONFIGURE_TYPE {
		kv.applyReconfigure(op)
	}
	kv.curOp++
	kv.px.Done(kv.curOp)
}

func (kv *ShardKV) applyPut(op Op) {
	prev, _ := kv.kvStore[op.Key]
	kv.prevValue[op.Client] = prev
	kv.seen[op.Client] = op.TimeStamp
	if op.DoHash {
		kv.kvStore[op.Key] = HashValue(prev, op.Value)
	} else {
		kv.kvStore[op.Key] = op.Value
	}
}

// if not exist send ""
func (kv *ShardKV) applyGet(op Op) {
	prev, _ := kv.kvStore[op.Key]
	kv.prevValue[op.Client] = prev
	kv.seen[op.Client] = op.TimeStamp
	//fmt.Println("The kv store is ", kv.kvStore)
}

func (kv *ShardKV) applyReconfigure(op Op) {
	newConfig := op.NewConfig
	kv.migrateKVShards(newConfig)
	//fmt.Println(kv.me, " newConfig is ", newConfig.Num)
	kv.curConfig = newConfig
}


func (kv *ShardKV) reconfigure(config shardmaster.Config) {
	curConfig := kv.curConfig
	for i := curConfig.Num + 1; i <= config.Num; i++ {
		newConfig := kv.sm.Query(i)
		op := Op{OpType: RECONFIGURE_TYPE, NewConfig: newConfig}
		kv.reachPaxosAgreement(op)
		//fmt.Println("!!!", newConfig)
	}
}

// migrate from more group to less group
func (kv *ShardKV) migrateKVShards(newConfig shardmaster.Config) {
	oldConfig := kv.curConfig
	// migrate from group in curConfig to config since config will
	// be new config. migrate group server only once.
	for index, gid := range newConfig.Shards {
		if gid != oldConfig.Shards[index] && gid == kv.gid {
			var ok bool
			args := &MigrateArgs{ShardIndex: index}
			reply := &MigrateReply{}
			for _, server := range oldConfig.Groups[gid] {
				ok = call(server, "ShardKV.MigrateShard", args, reply) // copy kvserver from current group to future group
				if ok {
					break
				}
			}
		} 
	}
	kv.curConfig = newConfig
}

// find all kv match the shard number and copy to caller's kvstore
func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, value := range kv.kvStore {
		shard := key2shard(key)
		if shard == args.ShardIndex {
			reply.kvShard[key] = value
		}
	}
	for client, value := range kv.prevValue {
		reply.prevValue[client] = value
	}
	return nil
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  key := args.Key
  op := Op{Key: key, TimeStamp: args.TimeStamp, OpType: GET_TYPE, Client: args.Client}
  reply.Err, reply.Value = kv.reachPaxosAgreement(op)
//fmt.Println("Get key ", op.Key, " Value ", reply.Value,  " kvstore ", kv.kvStore)
  return nil
}


func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  key, value, doHash := args.Key, args.Value, args.DoHash
  op := Op{Key: key, Value: value, TimeStamp: args.TimeStamp, OpType: PUT_TYPE, Client: args.Client, DoHash: doHash}
  var errorMsg Err
  errorMsg, prevValue := kv.reachPaxosAgreement(op)
  if doHash {
	reply.PreviousValue = prevValue
  }
  reply.Err = errorMsg
//fmt.Println(kv.me)
  return nil
}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := kv.sm.Query(-1)
	if config.Num != kv.curConfig.Num {
		fmt.Println(kv.me, " Config changed!!!! ", config.Num, " ", kv.curConfig.Num, " Config ", kv.curConfig)
		kv.reconfigure(config)		
	}
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.kvStore = make(map[string]string)
  kv.prevValue = make(map[string]string)
  kv.seen = make(map[string]int64)

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
