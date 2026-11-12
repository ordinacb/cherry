package cherryActor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ccode "github.com/cherry-game/cherry/code"
	cfacade "github.com/cherry-game/cherry/facade"
	cproto "github.com/cherry-game/cherry/net/proto"
	"google.golang.org/protobuf/proto"
)

func TestLocalMailboxBoundsConcurrentProducers(t *testing.T) {
	system := NewSystem()
	actor, err := newActor("blocked", "", &Base{}, system)
	if err != nil {
		t.Fatal(err)
	}
	system.actorMap.Store("blocked", actor)
	actor.setState(WorkerState)
	defer actor.localMail.onStop(nil)

	const limit = 256
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range limit {
				msg := cfacade.GetMessage()
				msg.Target = ".blocked"
				if system.PostLocal(msg) {
					accepted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := actor.localMail.Count(); got != limit || accepted.Load() != limit {
		t.Fatalf("mailbox accepted %d and retained %d, want %d", accepted.Load(), got, limit)
	}
}

func TestExpiredLocalMessageSkipsHandler(t *testing.T) {
	handler := &countingLocalHandler{}
	system := NewSystem()
	actor, err := newActor("blocked", "", handler, system)
	if err != nil {
		t.Fatal(err)
	}
	defer actor.localMail.onStop(nil)
	msg := cfacade.GetMessage()
	msg.Target = ".blocked"
	actor.PostLocal(msg)
	if actor.localMail.Count() != 1 {
		t.Fatal("message should be accepted before its deadline")
	}
	// This test drives the queue manually, so no Actor is reading the message.
	msg.BuildTime = time.Now().Add(-time.Minute).UnixMilli()
	actor.processLocal()
	if handler.called {
		t.Fatal("expired message reached OnLocalReceived")
	}
}

func TestQueueDeadlineSurvivesClusterPacketAndClone(t *testing.T) {
	msg := cfacade.GetMessage()
	defer msg.Recycle()
	msg.Session = &cproto.Session{Mid: 12}
	deadline := time.Now().Add(time.Second).UnixMilli()
	msg.Session.SetQueueDeadlineMillis(deadline)
	msg.RPCDeadlineMs = deadline + 1
	wire, err := msg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	received := cfacade.GetMessage()
	defer received.Recycle()
	if err := received.Unmarshal(wire); err != nil {
		t.Fatal(err)
	}
	forwarded := received.Clone()
	defer forwarded.Recycle()
	actor := &Actor{system: NewSystem()}
	if actor.queueExpired(forwarded, true, deadline-1) || !actor.queueExpired(forwarded, true, deadline) ||
		forwarded.RPCDeadlineMs != deadline+1 {
		t.Fatal("the original queue deadline was not preserved")
	}
}

func TestExpiredRemoteRPCPreservesPriority(t *testing.T) {
	system := NewSystem()
	system.SetRemoteMailboxPriority(func(name string) bool { return name == "priority" })
	actor := &Actor{system: system}
	message := cfacade.GetMessage()
	defer message.Recycle()
	message.RPCDeadlineMs = 1000
	if actor.queueExpired(message, false, 999) || !actor.queueExpired(message, false, 1000) {
		t.Fatal("ordinary RPC did not honor the caller deadline")
	}
	message.FuncName = "priority"
	if actor.queueExpired(message, false, 2000) {
		t.Fatal("priority RPC must remain eligible when the caller times out")
	}
}

func TestRemoteMailboxFullRepliesOnOriginalSubject(t *testing.T) {
	cluster := &mailboxReplyCluster{}
	system := NewSystem()
	system.app = &mailboxReplyApp{cluster: cluster}
	system.SetMailboxLimits(1, 1, 0)
	actor, err := newActor("blocked", "", &Base{}, system)
	if err != nil {
		t.Fatal(err)
	}
	defer actor.remoteMail.onStop(system.app)

	first := cfacade.GetMessage()
	if reason := actor.postMessage(first, false); reason != "" {
		t.Fatal(reason)
	}
	overflow := cfacade.GetMessage()
	overflow.ReqID, overflow.Reply = "req-42", "remote.replies"
	if reason := actor.postMessage(overflow, false); reason != DropMailboxFull {
		t.Fatalf("unexpected drop reason %q", reason)
	}
	if cluster.reqID != "req-42" || cluster.subject != "remote.replies" || cluster.code != ccode.ActorMailboxFull {
		t.Fatalf("wrong overflow reply req=%s subject=%s code=%d", cluster.reqID, cluster.subject, cluster.code)
	}
}

type mailboxReplyApp struct {
	cfacade.IApplication
	cluster cfacade.ICluster
}

func (*mailboxReplyApp) NodeID() string              { return "mailbox-test" }
func (a *mailboxReplyApp) Cluster() cfacade.ICluster { return a.cluster }

type mailboxReplyCluster struct {
	cfacade.ICluster
	reqID, subject string
	code           int32
}

func (c *mailboxReplyCluster) RequestReply(reqID, subject string, data []byte) error {
	var reply cproto.Response
	if err := proto.Unmarshal(data, &reply); err != nil {
		return err
	}
	c.reqID, c.subject, c.code = reqID, subject, reply.Code
	return nil
}

type countingLocalHandler struct {
	Base
	called bool
}

func (h *countingLocalHandler) OnLocalReceived(*cfacade.Message) (bool, bool) {
	h.called = true
	return false, false
}
