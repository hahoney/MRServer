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
	MIGRATE_SHARD_TYPE = 3
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
  curOp int // current operation number
  curConfig shardmaster.Config // current config

}


// Auxiliary functions
// reach agreement within group
func (kv *ShardKV) reachPaxosAgreement(op Op) string {
	var prevValue string
	if op.OpType == PUT_TYPE {
		value := op.Value
		prevValue = kv.prevValue[op.Client]
		if op.DoHash {
			value = HashValue(prevValue, value)
		}
		kv.kvStore[op.Key] = value
		kv.prevValue[op.Client] = value
		return prevValue
	}
	return kv.kvStore[op.Key]
}

func (kv *ShardKV) applyOperation(op Op) {
	if 
}


func (kv *ShardKV) reconfigure(config shardmaster.Config) {
	curConfig := kv.curConfig
	for i := curConfig.Num + 1; i <= config.Num; i++ {
		newConfig := kv.sm.Query(i)
		kv.migrateKVShards(newConfig)	
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
				ok = call(server, "MigrateShard", args, reply) // copy kvserver from current group to future group
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
  reply.Value = kv.reachPaxosAgreement(op)
  reply.Err = OK
  return nil
}


func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  key, value, doHash := args.Key, args.Value, args.DoHash
  op := Op{Key: key, Value: value, TimeStamp: args.TimeStamp, OpType: PUT_TYPE, Client: args.Client, DoHash: doHash}
  prevValue := kv.reachPaxosAgreement(op)
  if doHash {
	reply.PreviousValue = prevValue
  }
  reply.Err = OK
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
