package test_raftstore

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
)

func TestReadWrite(t *testing.T) {
	nservers := 3
	cfg := config.NewTestConfig()
	cfg.RaftLogGcCountLimit = uint64(100)
	cfg.RegionMaxSize = 300
	cfg.RegionSplitSize = 200
	cluster := NewTestCluster(nservers, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	electionTimeout := cfg.RaftBaseTickInterval * time.Duration(cfg.RaftElectionTimeoutTicks)
	// Wait for leader election
	time.Sleep(2 * electionTimeout)

	nclients := 16
	chTasks := make(chan int, 1000)
	clnts := make([]chan bool, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan bool, 1)
		go func(cli int) {
			defer func() {
				clnts[cli] <- true
			}()
			for {
				j, more := <-chTasks
				if more {
					key := fmt.Sprintf("%02d%08d", cli, j)
					value := "x " + strconv.Itoa(j) + " y"
					cluster.MustPut([]byte(key), []byte(value))
					if (rand.Int() % 1000) < 500 {
						value := cluster.Get([]byte(key))
						if value == nil {
							t.Fatal("value is emtpy")
						}
					}
				} else {
					return
				}
			}
		}(i)
	}

	start := time.Now()
	for i := 0; i < 10000; i++ {
		chTasks <- i
	}
	close(chTasks)
	for cli := 0; cli < nclients; cli++ {
		ok := <-clnts[cli]
		if ok == false {
			t.Fatalf("failure")
		}
	}
	elasped := time.Since(start)
	t.Logf("QPS: %v", 10000/elasped.Seconds())
}
