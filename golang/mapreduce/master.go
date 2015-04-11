package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  mapChan := make(chan int, mr.nMap)
  for i := 0; i < mr.nMap; i++ {
      go func(i int) {
          for {
              worker := <-mr.registerChannel
              ok := mr.runMap(i, worker)
              if ok { 
                  mapChan<-1  // mapChan<- before registerChan<- for synchronization
                  mr.registerChannel <- worker
                  break
              } 
          }
      }(i)
  }
  for i := 0; i < mr.nMap; i++ {
      <-mapChan
  }

  reduceChan := make(chan int, mr.nReduce)
  for i := 0; i < mr.nReduce; i++ {
      go func(i int) {
         for {
             worker := <-mr.registerChannel
             ok := mr.runReduce(i, worker)
             if ok {
                 reduceChan<-1
                 mr.registerChannel <- worker
                 break
             }
          }
      }(i)
  }

  for i := 0; i < mr.nReduce; i++ {
      <-reduceChan
  }

  return mr.KillWorkers()
}

func (mr *MapReduce) runMap(i int, worker string) bool {
  args := DoJobArgs{
      File: mr.file,
      Operation: Map,
      JobNumber: i,
      NumOtherPhase: mr.nReduce,
  }
  reply := DoJobReply{}

  return call(worker, "Worker.DoJob", args, &reply)
}

func (mr *MapReduce) runReduce(i int, worker string) bool {
  args := DoJobArgs{
      File: mr.file,
      Operation: Reduce,
      JobNumber: i,
      NumOtherPhase: mr.nMap,
  }
  reply := DoJobReply{}

  return call(worker, "Worker.DoJob", args, &reply)
}


