package pbservice

import "viewservice"
import "fmt"
import "io"
import "net"
import "testing"
import "time"
import "log"
import "runtime"
import "math/rand"
import "os"
import "strconv"

func check(ck *Clerk, key string, value string) {
  v := ck.Get(key)
  if v != value {
    log.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
  }
}

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "pb-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}


func TestRepeatedCrashUnreliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  tag := "rcu"
  vshost := port(tag+"v", 1)
  vs := viewservice.StartServer(vshost)
  time.Sleep(time.Second)
  vck := viewservice.MakeClerk("", vshost)
  
  fmt.Printf("Test: Repeated failures/restarts; unreliable ...\n")

  const nservers = 3
  var sa [nservers]*PBServer
  for i := 0; i < nservers; i++ {
    sa[i] = StartServer(vshost, port(tag, i+1))
    sa[i].unreliable = true
  }

  for i := 0; i < viewservice.DeadPings; i++ {
    v, _ := vck.Get()
    if v.Primary != "" && v.Backup != "" {
      break
    }
    time.Sleep(viewservice.PingInterval)
  }

  // wait a bit for primary to initialize backup
  time.Sleep(viewservice.DeadPings * viewservice.PingInterval)

  done := false

  go func() {
    // kill and restart servers
    rr := rand.New(rand.NewSource(int64(os.Getpid())))
    for done == false {
      i := rr.Int() % nservers
      // fmt.Printf("%v killing %v\n", ts(), 5001+i)
      sa[i].kill()

      // wait long enough for new view to form, backup to be initialized
      time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)

      sa[i] = StartServer(vshost, port(tag, i+1))

      // wait long enough for new view to form, backup to be initialized
      time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)
    }
  } ()

  const nth = 2
  var cha [nth]chan bool
  for xi := 0; xi < nth; xi++ {
    cha[xi] = make(chan bool)
    go func(i int) {
      ok := false
      defer func() { cha[i] <- ok } ()
      ck := MakeClerk(vshost, "")
      data := map[string]string{}
      // rr := rand.New(rand.NewSource(int64(os.Getpid()+i)))
      k := strconv.Itoa(i)
      data[k] = ""
      n := 0
      for done == false {
        v := strconv.Itoa(n)
        pv := ck.PutHash(k, v)
        if pv != data[k] {
          t.Fatalf("ck.Puthash(%s) returned %v but expected %v at iter %d\n", k, pv, data[k], n)
        }
        h := hash(data[k] + v)
        data[k] = strconv.Itoa(int(h))
        v = ck.Get(k)
        if v != data[k] {
          t.Fatalf("ck.Get(%s) returned %v but expected %v at iter %d\n", k, v, data[k], n)
        }
        // if no sleep here, then server tick() threads do not get 
        // enough time to Ping the viewserver.
        time.Sleep(10 * time.Millisecond)
        n++
      }
      ok = true

    }(xi)
  }

  time.Sleep(20 * time.Second)
  done = true

  fmt.Printf("  ... Put/Gets done ... \n")

  for i := 0; i < nth; i++ {
    ok := <- cha[i]
    if ok == false {
      t.Fatal("child failed")
    }
  }

  ck := MakeClerk(vshost, "")
  ck.Put("aaa", "bbb")
  if v := ck.Get("aaa"); v != "bbb" {
    t.Fatalf("final Put/Get failed")
  }

  fmt.Printf("  ... Passed\n")

  for i := 0; i < nservers; i++ {
    sa[i].kill()
  }
  time.Sleep(time.Second)
  vs.Kill()
  time.Sleep(time.Second)
}

func proxy(t *testing.T, port string, delay *int) {
  portx := port + "x"
  os.Remove(portx)
  if os.Rename(port, portx) != nil {
    t.Fatalf("proxy rename failed")
  }
  l, err := net.Listen("unix", port)
  if err != nil {
    t.Fatalf("proxy listen failed: %v", err)
  }
  go func() {
    defer l.Close()
    defer os.Remove(portx)
    defer os.Remove(port)
    for {
      c1, err := l.Accept()
      if err != nil {
        t.Fatalf("proxy accept failed: %v\n", err)
      }
      time.Sleep(time.Duration(*delay) * time.Second)
      c2, err := net.Dial("unix", portx)
      if err != nil {
        t.Fatalf("proxy dial failed: %v\n", err)
      }
      
      go func() {
        for {
          buf := make([]byte, 1000)
          n, _ := c2.Read(buf)
          if n == 0 {
            break
          }
          n1, _ := c1.Write(buf[0:n])
          if n1 != n {
            break
          }
        }
      }()
      for {
        buf := make([]byte, 1000)
        n, err := c1.Read(buf)
        if err != nil && err != io.EOF {
          t.Fatalf("proxy c1.Read: %v\n", err)
        }
        if n == 0 {
          break
        }
        n1, err1 := c2.Write(buf[0:n])
        if err1 != nil || n1 != n {
          t.Fatalf("proxy c2.Write: %v\n", err1)
        }
      }
      
      c1.Close()
      c2.Close()
    }
  }()
}

func TestPartition1(t *testing.T) {
  runtime.GOMAXPROCS(4)

  tag := "part1"
  vshost := port(tag+"v", 1)
  vs := viewservice.StartServer(vshost)
  time.Sleep(time.Second)
  vck := viewservice.MakeClerk("", vshost)

  ck1 := MakeClerk(vshost, "")

  fmt.Printf("Test: Old primary does not serve Gets ...\n")

  vshosta := vshost + "a"
  os.Link(vshost, vshosta)

  s1 := StartServer(vshosta, port(tag, 1))
  delay := 0
  proxy(t, port(tag, 1), &delay)

  deadtime := viewservice.PingInterval * viewservice.DeadPings
  time.Sleep(deadtime * 2)
  if vck.Primary() != s1.me {
    t.Fatal("primary never formed initial view")
  }

  s2 := StartServer(vshost, port(tag, 2))
  time.Sleep(deadtime * 2)
  v1, _ := vck.Get()
  if v1.Primary != s1.me || v1.Backup != s2.me {
    t.Fatal("backup did not join view")
  }
  
  ck1.Put("a", "1")
  check(ck1, "a", "1")

  os.Remove(vshosta)

  // start a client Get(), but use proxy to delay it long
  // enough that it won't reach s1 until after s1 is no
  // longer the primary.
  delay = 4
  stale_get := false
  go func() {
    x := ck1.Get("a")
    if x == "1" {
      stale_get = true
    }
  }()

  // now s1 cannot talk to viewserver, so view will change,
  // and s1 won't immediately realize.

  for iter := 0; iter < viewservice.DeadPings * 3; iter++ {
    if vck.Primary() == s2.me {
      break
    }
    time.Sleep(viewservice.PingInterval)
  }
  if vck.Primary() != s2.me {
    t.Fatalf("primary never changed")
  }

  // wait long enough that s2 is guaranteed to have Pinged
  // the viewservice, and thus that s2 must know about
  // the new view.
  time.Sleep(2 * viewservice.PingInterval)

  // change the value (on s2) so it's no longer "1".
  ck2 := MakeClerk(vshost, "")
  ck2.Put("a", "111")
  check(ck2, "a", "111")

  // wait for the background Get to s1 to be delivered.
  time.Sleep(5 * time.Second)
  if stale_get {
    t.Fatalf("Get to old primary succeeded and produced stale value")
  }

  check(ck2, "a", "111")

  fmt.Printf("  ... Passed\n")

  s1.kill()
  s2.kill()
  vs.Kill()
}

func TestPartition2(t *testing.T) {
  runtime.GOMAXPROCS(4)

  tag := "part2"
  vshost := port(tag+"v", 1)
  vs := viewservice.StartServer(vshost)
  time.Sleep(time.Second)
  vck := viewservice.MakeClerk("", vshost)

  ck1 := MakeClerk(vshost, "")

  vshosta := vshost + "a"
  os.Link(vshost, vshosta)

  s1 := StartServer(vshosta, port(tag, 1))
  delay := 0
  proxy(t, port(tag, 1), &delay)

  fmt.Printf("Test: Partitioned old primary does not complete Gets ...\n")

  deadtime := viewservice.PingInterval * viewservice.DeadPings
  time.Sleep(deadtime * 2)
  if vck.Primary() != s1.me {
    t.Fatal("primary never formed initial view")
  }

  s2 := StartServer(vshost, port(tag, 2))
  time.Sleep(deadtime * 2)
  v1, _ := vck.Get()
  if v1.Primary != s1.me || v1.Backup != s2.me {
    t.Fatal("backup did not join view")
  }
  
  ck1.Put("a", "1")
  check(ck1, "a", "1")

  os.Remove(vshosta)

  // start a client Get(), but use proxy to delay it long
  // enough that it won't reach s1 until after s1 is no
  // longer the primary.
  delay = 5
  stale_get := false
  go func() {
    x := ck1.Get("a")
    if x == "1" {
      stale_get = true
    }
  }()

  // now s1 cannot talk to viewserver, so view will change.

  for iter := 0; iter < viewservice.DeadPings * 3; iter++ {
    if vck.Primary() == s2.me {
      break
    }
    time.Sleep(viewservice.PingInterval)
  }
  if vck.Primary() != s2.me {
    t.Fatalf("primary never changed")
  }

  s3 := StartServer(vshost, port(tag, 3))
  for iter := 0; iter < viewservice.DeadPings * 3; iter++ {
    v, _ := vck.Get()
    if v.Backup == s3.me && v.Primary == s2.me {
      break
    }
    time.Sleep(viewservice.PingInterval)
  }
  v2, _ := vck.Get()
  if v2.Primary != s2.me || v2.Backup != s3.me {
    t.Fatalf("new backup never joined")
  }
  time.Sleep(2 * time.Second)

  ck2 := MakeClerk(vshost, "")
  ck2.Put("a", "2")
  check(ck2, "a", "2")

  s2.kill()

  // wait for delayed get to s1 to complete.
  time.Sleep(6 * time.Second)

  if stale_get == true {
    t.Fatalf("partitioned primary replied to a Get with a stale value")
  }

  check(ck2, "a", "2")

  fmt.Printf("  ... Passed\n")

  s1.kill()
  s2.kill()
  s3.kill()
  vs.Kill()
}
