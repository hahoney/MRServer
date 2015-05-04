package kvpaxos

import "net/rpc"
import "fmt"
import "strconv"
import "time"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
  me string
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.me = strconv.FormatInt(nrand(), 10)
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
	server := 0 //rand.Intn(len(ck.servers))
	args := &GetArgs{Key: key, OpId: ck.GeneratePaxosNumber(), Client: ck.me}
	reply := &GetReply{}
	for {
	    ok := call(ck.servers[server], "KVPaxos.Get", args, reply)
		if ok {
			break
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(time.Second * 1)
		//time.Sleep(100 * time.Millisecond)
	}
	return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
// Your put is completed only when the instance is decided on the machine
	server := 0 //rand.Intn(len(ck.servers))
	args := &PutArgs{Key: key, Value: value, DoHash: dohash, OpId: ck.GeneratePaxosNumber(), Client: ck.me}
	reply := &PutReply{}
	for {
		ok := call(ck.servers[server], "KVPaxos.Put", args, reply)
		if ok {
			break
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(time.Second * 1)
		//time.Sleep(100 * time.Millisecond)
	}
	if dohash {
		return reply.PreviousValue
	}
  	return ""
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}


func (ck *Clerk) GeneratePaxosNumber() int64 {
	begin := time.Date(2014, time.May, 5, 1, 0, 0, 0, time.UTC)
	duration := time.Now().Sub(begin)
	return duration.Nanoseconds()
	//return strconv.FormatInt(duration.Nanoseconds(), 10) + ck.me
}
