package shardmaster

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

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num use paxos to reach agreement

}

const (
	OPERATION_JOIN = 1
	OPERATION_LEAVE = 2
	OPERATION_MOVE = 3
	OPERATION_QUERY = 4
	
)


type Op struct {
  // Your data here.
	GID int64
	Servers []string
	OpType int
	Shard int // from 1 to 10
	Num	int // Num is for query only
}

// reach paxos agreement on configs
func (sm *ShardMaster) reachPaxosAgreement(op Op) {
	min := sm.px.Min()
	max := sm.px.Max()
	for i := min; i <= max; i++ {	
		decided, value := sm.px.Status(i)
		var result Op
		if decided {
			result = value.(Op)
		} else {
			sm.px.Start(i, op)
			result = sm.wait(i)
		}
		sm.updateMap(result, i)
	}
}


func (sm *ShardMaster) updateMap(op Op, configNum int) {
	// TODO: implement different operations
	var config Config
	var ok bool
	if op.OpType == OPERATION_JOIN {
		config, ok = sm.makeJoin(op, configNum)
	}
	if op.OpType == OPERATION_LEAVE {
		config, ok = sm.makeLeave(op, configNum)
	}
	if op.OpType == OPERATION_MOVE {
		config, ok = sm.makeMove(op, configNum)
	}
	if op.OpType == OPERATION_QUERY {
		config, ok = sm.makeQuery(op, configNum)
	}
	if !ok {
		fmt.Println("Error")
		return
	}
	sm.configs[configNum] = config
	sm.px.Done(configNum)
}

func (sm *ShardMaster) initNewConfig(configNum int) (Config, int) {
	var lastConfig Config
	lengthGroups := 0
	
	if configNum == 0 {
		lastConfig = Config{}
	} else {
		lastConfig = sm.configs[configNum - 1]
	}
	
	newConfig := Config{Num: configNum}
	newConfig.Shards = [NShards]int64{}
	newConfig.Groups = make(map[int64][]string)
	
	for i, shard := range lastConfig.Shards {
		if shard != 0 {
			lengthGroups++
			newConfig.Shards[i] = shard
		}
	}
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
		
	return newConfig, lengthGroups
}


// The shard are distributed as evenly as possible and the shard moves needed
// are as few as possible.
func (sm *ShardMaster) balanceShards(config Config, lengthGroups int) Config {
	totalShards := sm.calcTotalShards(config, lengthGroups)
	average := totalShards / lengthGroups
	remainder := totalShards % lengthGroups
	serverDistributor := make([]string, 0)
	newConfig := config

	for i := 0; i < lengthGroups; i++ {
		gid := newConfig.Shards[i]
		servers := newConfig.Groups[gid]
		if len(servers) > average {
			serverDistributor = append(serverDistributor, servers[average:]...)
			newConfig.Groups[gid] = servers[:average]
		}
	}
	
	moved := 0 // the index of largest moved elements in dist exclusive
	for i := 0; i < lengthGroups; i++ {
		gid := newConfig.Shards[i]
		servers := newConfig.Groups[gid]
		if len(servers) <= average {
			if remainder > 0 {
				newConfig.Groups[gid], moved = sm.addToServers(serverDistributor, moved, servers, average + 1)
				remainder--
			} else {
				newConfig.Groups[gid], moved = sm.addToServers(serverDistributor, moved, servers, average)
			}
		}
	}
	return newConfig
}


func (sm *ShardMaster) addToServers(dist []string, moved int, servers []string, target int) ([]string, int) {
	length := len(servers)
	newMoved := moved + target - length
	servers = append(servers, dist[moved : newMoved]...) // target - length servers are moved
	return servers, newMoved
}


func (sm *ShardMaster) calcTotalShards(config Config, length int) int {
	totalShards := 0
	for i := 0; i < length; i++ {
		totalShards += len(config.Groups[config.Shards[i]])
	}
	return totalShards
}


func (sm *ShardMaster) validateInput(lengthGroups int, gid int, opType int) bool {
	if lengthGroups >= NShards && opType == OPERATION_JOIN {
		fmt.Println("Cannot join, reach max Nshards")
		return false
	}
	
	if lengthGroups > NShards + 1 && opType == OPERATION_LEAVE {
		fmt.Println("Cannot join, reach max Nshards")
		return false
	}
	
	if _, exist := joinConfig.Groups[gid]; exist {
		fmt.Println("Group already exist")
		return false
	}
	return true
}





func (sm *ShardMaster) makeJoin(op Op, configNum int) (Config, bool) {
	joinConfig, lengthGroups := sm.initNewConfig(configNum)
	ok := sm.validateInput(lengthGroups, op.GID, op.OpType)
	if !ok { 
		return joinConfig, ok
	}
	joinConfig.Shards[lengthGroups] = op.GID
	joinConfig.Groups[op.GID] = op.Servers
	lengthGroups++
	joinConfig = sm.balanceShards(joinConfig, lengthGroups)
	return joinConfig, ok
}



func (sm *ShardMaster) makeLeave(op Op, configNum int) (Config, bool) {
	leaveConfig, lengthGroups := sm.initNewConfig(configNum)
	ok := sm.validateInput(lengthGroups, op.GID, op.OpType)
	if !ok {
		return joinConfig, ok
	}
	for nShards, gid := range leaveConfig.Shards {
		if gid == op.GID {
			leaveConfig.Shards[nShards] = 0
			delete(leaveConfig.Groups, gid)
			break
		}
	}
	// do nothing since the shards is already balanced
	return config, true
}

func (sm *ShardMaster) makeMove(op Op, configNum int) (Config, bool) {
	config := Config{}
	return config, true
}

func (sm *ShardMaster) makeQuery(op Op, configNum int) (Config, bool) {
	config := Config{}
	return config, true
}




func (sm *ShardMaster) wait(config int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := sm.px.Status(config)
		if decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{OpType: OPERATION_JOIN, GID: args.GID, Servers: args.Servers}
  sm.reachPaxosAgreement(op)
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{OpType: OPERATION_LEAVE, GID: args.GID}
  sm.reachPaxosAgreement(op)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{OpType: OPERATION_MOVE, GID: args.GID, Shard: args.Shard}
  sm.reachPaxosAgreement(op)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{OpType: OPERATION_QUERY, Num: args.Num}
  sm.reachPaxosAgreement(op)
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
