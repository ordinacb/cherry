package cherryDiscovery

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	cnats "github.com/cherry-game/cherry/net/nats"
	cproto "github.com/cherry-game/cherry/net/proto"
	"github.com/nats-io/nats.go"
)

// A fresh worker must fetch its table even if the master still knows the old incarnation.
func TestClientInitialRegistrationWithKnownNodeID(t *testing.T) {
	for _, fault := range []string{"none", "timeout", "malformed", "empty"} {
		t.Run(fault, func(t *testing.T) { testInitialRegistration(t, fault) })
	}
}

func testInitialRegistration(t *testing.T, fault string) {
	url := os.Getenv("BILLIARD_TEST_NATS")
	if url == "" {
		t.Skip("set isolated BILLIARD_TEST_NATS")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	prefix := "cherry.cold." + cnats.NewStringReqID()
	master := newTestMaster("master", "master")
	master.thisMember = &cproto.Member{NodeID: "master"}
	master.publishConnect.Conn = nc
	master.subscribeConnect = cnats.NewConnect("master-subscribe")
	master.subscribeConnect.Conn = nc
	master.registerSubject, master.heartbeatSubject = prefix+".register", prefix+".heartbeat"
	master.AddMember(master.thisMember)
	master.AddMember(&cproto.Member{NodeID: "worker", Settings: map[string]string{"generation": "old"}})
	master.AddMember(&cproto.Member{NodeID: "peer"})
	master.heartbeatSubscribe()
	var attempts atomic.Int32
	_, err = nc.Subscribe(master.registerSubject, func(msg *nats.Msg) {
		n := attempts.Add(1)
		if n == 1 && fault == "timeout" {
			return
		}
		if n == 1 && fault == "malformed" {
			_ = master.publishConnect.RequestReply(msg.Header.Get(cnats.REQ_ID), msg.Reply, []byte{0xff})
			return
		}
		if n == 1 && fault == "empty" {
			_ = master.publishConnect.RequestReply(msg.Header.Get(cnats.REQ_ID), msg.Reply, nil)
			return
		}
		member, err := master.bytes2Member(msg.Data)
		if err != nil {
			return
		}
		master.AddMember(member)
		master.replyMemberList(msg)
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := newTestMaster("worker", "master")
	worker.App().(*mockApp).isRunning = true
	worker.thisMember = &cproto.Member{NodeID: "worker", HeartbeatTimeout: 3000, Settings: map[string]string{"generation": "new"}}
	worker.AddMember(worker.thisMember)
	worker.registerSubject, worker.heartbeatSubject = master.registerSubject, master.heartbeatSubject
	worker.publishConnect = cnats.NewConnect("worker-publish", cnats.WithRequestTimeout(80*time.Millisecond))
	worker.publishConnect.Conn = nc
	if err := worker.publishConnect.SubscribeReply(prefix + ".reply"); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker.ctx = ctx
	done := make(chan struct{})
	go func() { defer close(done); worker.clientTicker() }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, found := worker.GetMember("peer"); found {
			registered, _ := master.GetMember("worker")
			if registered.GetSettings()["generation"] != "new" {
				t.Fatal("old incarnation was not replaced")
			}
			if fault != "none" && attempts.Load() < 2 {
				t.Fatal("registration failure was not retried")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("new worker never registered and fetched peer; fault=%s attempts=%d", fault, attempts.Load())
}
