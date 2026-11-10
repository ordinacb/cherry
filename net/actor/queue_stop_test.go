package cherryActor

import (
	"runtime"
	"sync"
	"testing"

	cfacade "github.com/cherry-game/cherry/facade"
)

func TestQueueDestroyConcurrentPush(t *testing.T) {
	q := newQueue()
	start := make(chan struct{})
	panics := make(chan any, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if err := recover(); err != nil {
					panics <- err
				}
			}()
			<-start
			for j := 0; j < 1000; j++ {
				q.Push(j + 1)
				runtime.Gosched()
			}
		}()
	}
	close(start)
	runtime.Gosched()
	q.Destroy()
	wg.Wait()
	close(panics)
	for err := range panics {
		t.Errorf("posting while stopping panicked: %v", err)
	}
	if t.Failed() {
		return
	}
	q.Destroy()
	q.Push(1)
	if q.Count() != 0 || !q.Empty() || q.Pop() != nil {
		t.Fatal("destroyed queue accepted a new item")
	}
}

func TestStoppedMailboxDropsAndRecyclesAcceptedAndLateMessages(t *testing.T) {
	previous := dropHandler
	defer SetDropHandler(previous)
	var dropped []string
	SetDropHandler(func(_ cfacade.IApplication, m *cfacade.Message, reason string) {
		if reason != DropActorNotReady {
			t.Errorf("unexpected drop reason: %s", reason)
		}
		dropped = append(dropped, m.FuncName)
	})
	box := newMailbox("stopping")
	pending := cfacade.GetMessage()
	pending.FuncName = "accepted"
	pending.AddRef()
	if !box.Push(pending) {
		t.Fatal("live mailbox rejected message")
	}
	box.onStop(nil)
	if pending.BuildTime != 0 {
		t.Fatal("accepted message not recycled on stop")
	}
	actor := &Actor{system: &System{app: &callWaitTestApp{}}, remoteMail: &box}
	late := cfacade.GetMessage()
	late.FuncName = "late"
	actor.PostRemote(late)
	if late.BuildTime != 0 {
		t.Fatal("late message not recycled")
	}
	if len(dropped) != 2 || dropped[0] != "accepted" || dropped[1] != "late" {
		t.Fatalf("missing delivery failure callbacks: %v", dropped)
	}
}
