package pomelo

import (
	"strconv"
	"sync"
	"testing"

	cfacade "github.com/cherry-game/cherry/facade"
	cproto "github.com/cherry-game/cherry/net/proto"
)

func newTestAgent(sid string) *Agent {
	return &Agent{
		sid:  cfacade.SID(sid),
		data: &cproto.SessionData{},
	}
}

func TestBindSIDAndGetAgentWithSID(t *testing.T) {
	agent := newTestAgent("sid-1")
	BindSID(agent)

	got, found := GetAgentWithSID("sid-1")
	if !found {
		t.Fatal("GetAgentWithSID not found")
	}
	if got.SID() != "sid-1" {
		t.Fatalf("expected sid-1, got %s", got.SID())
	}
}

func TestBindAndGetAgentWithUID(t *testing.T) {
	agent := newTestAgent("sid-2")
	BindSID(agent)

	_, err := Bind("sid-2", 100)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}

	got, found := GetAgentWithUID(100)
	if !found {
		t.Fatal("GetAgentWithUID not found")
	}
	if got.UID() != 100 {
		t.Fatalf("expected uid 100, got %d", got.UID())
	}
}

func TestBindDuplicateLogin(t *testing.T) {
	old := newTestAgent("old-sid")
	BindSID(old)
	Bind("old-sid", 200)

	neu := newTestAgent("new-sid")
	BindSID(neu)

	oldAgent, err := Bind("new-sid", 200)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	if oldAgent == nil {
		t.Fatal("expected old agent for duplicate login")
	}
	if oldAgent.SID() != "old-sid" {
		t.Fatalf("expected old-sid, got %s", oldAgent.SID())
	}

	got, _ := GetAgentWithUID(200)
	if got.SID() != "new-sid" {
		t.Fatalf("expected new-sid to own uid, got %s", got.SID())
	}
}

func TestUnbind(t *testing.T) {
	agent := newTestAgent("sid-3")
	BindSID(agent)
	Bind("sid-3", 300)

	Unbind("sid-3")

	_, found := GetAgentWithSID("sid-3")
	if found {
		t.Fatal("agent should be removed after Unbind")
	}
	_, found = GetAgentWithUID(300)
	if found {
		t.Fatal("uid mapping should be removed after Unbind")
	}
}

func TestUnbindKeepsNewSIDMapping(t *testing.T) {
	a1 := newTestAgent("s1")
	BindSID(a1)
	Bind("s1", 400)

	a2 := newTestAgent("s2")
	BindSID(a2)
	Bind("s2", 400)

	Unbind("s1")

	got, found := GetAgentWithUID(400)
	if !found {
		t.Fatal("uid should still be bound to s2")
	}
	if got.SID() != "s2" {
		t.Fatalf("expected s2, got %s", got.SID())
	}
}

func TestConcurrentUnbindKeepsNewSIDMapping(t *testing.T) {
	const uid = 401
	old := newTestAgent("concurrent-old")
	next := newTestAgent("concurrent-new")
	BindSID(next)
	t.Cleanup(func() { Unbind(next.SID()) })

	closing := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		for range closing {
			Unbind(old.SID())
			closed <- struct{}{}
		}
	}()
	defer close(closing)

	for i := range 1024 {
		BindSID(old)
		if _, err := Bind(old.SID(), uid); err != nil {
			t.Fatal(err)
		}
		closing <- struct{}{}
		_, err := Bind(next.SID(), uid)
		<-closed
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := GetAgentWithUID(uid); !ok || got != next {
			t.Fatalf("round %d: old disconnect removed the new UID binding", i)
		}
	}
}

func TestGetAgent(t *testing.T) {
	agent := newTestAgent("sid-5")
	BindSID(agent)
	Bind("sid-5", 500)

	got, found := GetAgent("sid-5", 0)
	if !found || got.SID() != "sid-5" {
		t.Fatal("GetAgent by sid failed")
	}

	got, found = GetAgent("", 500)
	if !found || got.UID() != 500 {
		t.Fatal("GetAgent by uid failed")
	}

	_, found = GetAgent("", 0)
	if found {
		t.Fatal("GetAgent with empty args should not find")
	}
}

func TestCountAndForeach(t *testing.T) {
	before := Count()

	for i := 0; i < 3; i++ {
		sid := cfacade.SID("sid-foreach-" + string(rune('a'+i)))
		t.Cleanup(func() { Unbind(sid) })
		BindSID(newTestAgent(string(sid)))
	}

	if got := Count(); got != before+3 {
		t.Fatalf("Count = %d, want %d", got, before+3)
	}

	seen := 0
	ForeachAgent(func(a *Agent) {
		seen++
	})
	if seen != before+3 {
		t.Fatalf("ForeachAgent saw %d, want %d", seen, before+3)
	}
}

func TestCountSIDReplacement(t *testing.T) {
	before := Count()
	const sid = "count-replacement"
	t.Cleanup(func() { Unbind(sid) })
	first, next := newTestAgent(sid), newTestAgent(sid)
	BindSID(first)
	BindSID(first)
	BindSID(next)
	if agent, _ := GetAgentWithSID(sid); agent != next {
		t.Fatal("expected replacement agent for SID")
	}
	if got := Count(); got != before+1 {
		t.Fatalf("Count after replacement = %d, want %d", got, before+1)
	}
	Unbind(sid)
	Unbind(sid)
	if got := Count(); got != before {
		t.Fatalf("Count after repeated unbind = %d, want %d", got, before)
	}
}

func TestCountConcurrentSIDChurn(t *testing.T) {
	before := Count()
	const sid = "count-concurrent"
	t.Cleanup(func() { Unbind(sid) })
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			agent := newTestAgent(sid)
			<-start
			for range 64 {
				BindSID(agent)
				Unbind(sid)
				if got := Count(); got < before || got > before+1 {
					t.Errorf("Count with one churning SID = %d, want %d..%d", got, before, before+1)
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	if got := Count(); got != before {
		t.Fatalf("Count after churn = %d, want %d", got, before)
	}
}

func BenchmarkCount(b *testing.B) {
	for _, size := range []int{1, 1000, 10000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			for i := range size {
				agent := newTestAgent("count-bench-" + strconv.Itoa(i))
				BindSID(agent)
				b.Cleanup(func() { popAgent(agent.SID()) })
			}
			b.ReportAllocs()
			for b.Loop() {
				Count()
			}
		})
	}
}
