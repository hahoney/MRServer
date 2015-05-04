package kvpaxos

import "hash/fnv"
import "strconv"
import "crypto/rand"
import "math/big"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  OpId		int64
  Client	string
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  OpId	int64
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

func nrand() int64 {
	max := big.NewInt(int64(int64(1) << 62))
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func CalcHash(hprev string, val string) string {
	//fmt.Println("In calcHash hprev is ", hprev, " val is ", val )
	h := hash(hprev + val)
	return strconv.Itoa(int(h))
	//return hprev
}
