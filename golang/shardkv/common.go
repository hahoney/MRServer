package shardkv
import "hash/fnv"
import "time"
import "crypto/rand"
import "math/big"
import "strconv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  TimeStamp int64
  Client string

}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  TimeStamp int64
  Client string
}

type GetReply struct {
  Err Err
  Value string
}


func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func GeneratePaxosNumber() int64 {
	begin := time.Date(2015, time.May, 5, 1, 0, 0, 0, time.UTC)
	duration := time.Now().Sub(begin)
	return duration.Nanoseconds()
}

func nrand() string {
	max := big.NewInt(int64(int64(1) << 62))
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	str := strconv.FormatInt(x, 10)
	return str
}

func HashValue(hprev string, val string) string {
  h := hash(hprev + val)
  return strconv.Itoa(int(h))
}

