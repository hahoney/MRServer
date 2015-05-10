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
// Config number must be consecutive
  curOp	int

}

const (
	OPERATION_JOIN = 1
	OPERATION_LEAVE = 2
	OPERATION_MOVE = 3
	OPERATION_QUERY = 4
	EMPTY_NUMBER = -1
	
)

const debug = 0

type Op struct {
  // Your data here.
	GID int64
	Servers []string
	OpType int
	Shard int
	Num	int // Num is for query only
	
}

func (sm *ShardMaster) dumpConfigs() {
	for _, config := range sm.configs {
		fmt.Println(config)
	}
}

func (sm *ShardMaster) printOpBrief(op Op) {
	if debug == 1 {
		fmt.Println(" Op GID ", op.GID, " Op Type ", op.OpType)
	}
}


// reach paxos agreement on configs
func (sm *ShardMaster) reachPaxosAgreement(op Op) Config {
	min := sm.curOp + 1
	max := sm.px.Max()
	var config Config
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
	if op.OpType != OPERATION_QUERY {
		sm.px.Start(max + 1, op)
		sm.wait(max + 1)
	}
	config = sm.updateMap(op, max + 1)
	return config
}


func (sm *ShardMaster) updateMap(op Op, configNum int) Config {
	//fmt.Println("Operation update ", configNum)
	var config Config
	var ok bool
	if op.OpType == OPERATION_JOIN {
		sm.printOpBrief(op)
		config, ok = sm.makeJoin(op, configNum)
	}
	if op.OpType == OPERATION_LEAVE {
		sm.printOpBrief(op)
		config, ok = sm.makeLeave(op, configNum)
	}
	if op.OpType == OPERATION_MOVE {
		sm.printOpBrief(op)
		config, ok = sm.makeMove(op, configNum)
	}
	if op.OpType == OPERATION_QUERY {
		sm.printOpBrief(op)
		config, ok = sm.doQuery(op.Num)
		return config
	}
	if !ok {
	//	fmt.Println("Error")
		return config
	}
	// Query does not progress the configs
	if op.OpType != OPERATION_QUERY {
		sm.configs = append(sm.configs, config)
		sm.px.Done(configNum)
		sm.curOp = configNum
	}
	return config
}


func (sm *ShardMaster) initNewConfig(configNum int) Config {
	var lastConfig Config
	configLength := len(sm.configs)
	lastConfig = sm.configs[configLength - 1]
	newConfig := Config{Num: configNum}
	newConfig.Shards = [NShards]int64{}
	newConfig.Groups = make(map[int64][]string)
	
	for i, shard := range lastConfig.Shards {
		newConfig.Shards[i] = shard
	}
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	return newConfig
}


// The servers in a group do not change. Only the grouping is changed
func (sm *ShardMaster) balanceJoin(config Config, gid int64) Config {
	totalGroups := len(config.Groups)
	average := NShards / totalGroups
	remainder := NShards % totalGroups

	// count shards for each group (GID)
	shardDist, freeShards := sm.countShards(config)
	// cut shards from above average groups
	// balancer := make([]int, 0) // allocator is []int index -> shard number
	for gid, shardGroup := range shardDist {
		var target int
		if remainder > 0 {
			target = average + 1
			remainder--
		} else {
			target = average
		}
		if len(shardGroup) > target {
			freeShards = append(freeShards, shardGroup[target:]...)
			shardDist[gid] = shardGroup[:target] // shardGroup is []int of shard Number
			// shardDist is a map  gid -> a []int list of shard number\
			// freeShard is a list []int of index -> shardNumber, index has no meaning 
		}
	}
	// move shards from balancer to new group
	// fmt.Println("balancer length ", len(freeShards), " average ", average,  " totalGroups ", totalGroups)
	for _, shardNum := range freeShards {
		config.Shards[shardNum] = gid
	}
	return config
}

func (sm *ShardMaster) balanceLeave(config Config, configNum int) Config {
	totalGroups := len(config.Groups)
	average := NShards / totalGroups
	remainder := NShards % totalGroups
	//fmt.Println("Enter Leave")
	shardDist, freeShards := sm.countShards(config)
	moved := 0
	for gid, shardGroup := range shardDist {
		var target int
		if remainder > 0 {
			target = average + 1
			remainder--
		} else {
			target = average
		} 
		//fmt.Println("Leave ", shardDist, " ", target, " ", freeShards, " configNum ", configNum)
		//sm.dumpConfigs()
		if len(shardGroup) < target {
			shardsToMove := target - len(shardGroup)
			if shardsToMove > len(freeShards) - moved {
				shardsToMove = len(freeShards) - moved
			}
			for i := moved; i  < moved + shardsToMove; i++ {
				fmt.Println(i, " ", len(freeShards), " ", gid, " ", target, " ", shardsToMove, " ", moved)
				config.Shards[freeShards[i]] = gid
			}
			moved += shardsToMove
		}  
	} 
	//fmt.Println("Exit Leave ", len(config.Groups))
	return config
}

func (sm *ShardMaster) countShards(config Config) (map[int64][]int, []int) {
	shardDist := make(map[int64][]int) // GID -> shard numbers
	freeShards := make([]int, 0)
	for index, gid := range config.Shards {
		var shard []int
		if gid == EMPTY_NUMBER {
			freeShards = append(freeShards, index)
			continue
		}
		_, exist := shardDist[gid]
		if !exist {
			shard = make([]int, 0)
		} else {
			shard = shardDist[gid] // shard is []int
		}
		shard = append(shard, index)
		shardDist[gid] = shard
	}
	return shardDist, freeShards
}


func (sm *ShardMaster) validateInput(config Config, gid int64, opType int) bool {
	lengthGroups := len(config.Groups)
	if lengthGroups >= NShards && opType == OPERATION_JOIN {
		fmt.Println("Waster groups")
		return false
	}
	
	if lengthGroups == 0 && opType == OPERATION_LEAVE {
		fmt.Println("Cannot leave, reach 0")
		return false
	}
	
	if _, exist := config.Groups[gid]; exist && opType == OPERATION_JOIN {
		fmt.Println("Group already exist, gid = ", gid)
		return false
	}
	if _, exist := config.Groups[gid]; !exist && opType == OPERATION_LEAVE {
		fmt.Println("Leave Id does not exist ", gid)
		return false
	}
	return true
}


func (sm *ShardMaster) makeJoin(op Op, configNum int) (Config, bool) {
	joinConfig := sm.initNewConfig(configNum)
	ok := sm.validateInput(joinConfig, op.GID, op.OpType)
	if !ok { 
		return joinConfig, false
	}
	joinConfig.Groups[op.GID] = op.Servers
	joinConfig = sm.balanceJoin(joinConfig, op.GID)
	return joinConfig, ok
}


func (sm *ShardMaster) makeLeave(op Op, configNum int) (Config, bool) {
	leaveConfig := sm.initNewConfig(configNum)
	fmt.Println("Config before leave ", leaveConfig, " and leave GID is ", op.GID)
	ok := sm.validateInput(leaveConfig, op.GID, op.OpType)
	if !ok {
		return leaveConfig, false
	}
	for shardNumber, gid := range leaveConfig.Shards {
		if gid == op.GID {
			leaveConfig.Shards[shardNumber] = EMPTY_NUMBER
		}
	}
	delete(leaveConfig.Groups, op.GID)
	leaveConfig = sm.balanceLeave(leaveConfig, configNum)
	fmt.Println("Config after leave ", leaveConfig)
	return leaveConfig, true
}


func (sm *ShardMaster) makeMove(op Op, configNum int) (Config, bool) {
	moveConfig := sm.initNewConfig(configNum)
	ok := sm.validateInput(moveConfig, op.GID, op.OpType)
	if !ok {
		return moveConfig, false
	}
	moveConfig.Shards[op.Shard] = op.GID
	return moveConfig, true
}


func (sm *ShardMaster) doQuery(configNum int) (Config, bool) {
	maxConfig := sm.getMaxConfigIndex()
	if configNum > maxConfig || configNum == -1 {
		return sm.configs[maxConfig], true
	}
	var result Config
	var ok bool
	if configNum <= maxConfig && configNum >= 0 {
		result = sm.configs[configNum]
		ok = true
	} else {
		ok = false
	}
	return result, ok
}


func (sm *ShardMaster) getMaxConfigIndex() int {
	max := -1
	maxIndex := -1
	for i, config := range sm.configs {
		if config.Num > max {
			max = config.Num
			maxIndex = i
		}
	}
	return maxIndex
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
  reply.Config = sm.reachPaxosAgreement(op)
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

  for i := 0; i < NShards; i++ {
	sm.configs[0].Shards[i] = EMPTY_NUMBER 
  }
  //sm.curOp = EMPTY_NUMBER
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
