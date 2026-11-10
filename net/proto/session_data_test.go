package cherryProto

import (
	"fmt"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestSessionDataSnapshotConcurrentMarshal(t *testing.T) {
	data := &SessionData{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			data.Set("tableId", fmt.Sprint(i))
		}
	}()
	for i := 0; i < 10000; i++ {
		session := &Session{Data: data.Clone(), Mid: uint32(i)}
		encoded, err := proto.Marshal(session)
		if err != nil {
			t.Fatal(err)
		}
		decoded := &Session{}
		if err := proto.Unmarshal(encoded, decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.GetMID() != uint32(i) {
			t.Fatalf("mid changed: %d", decoded.GetMID())
		}
	}
	wg.Wait()
}

func TestSessionDataApplyIsAtomicAndDeletes(t *testing.T) {
	data := &SessionData{}
	data.Set("remove", "old")
	data.Apply(map[string]string{"table": "a", "node": "a", "remove": ""})
	if data.Has("remove") {
		t.Fatal("empty value must delete")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			value := fmt.Sprint(i)
			data.Apply(map[string]string{"table": value, "node": value})
		}
	}()
	for i := 0; i < 10000; i++ {
		snapshot := data.Clone()
		if snapshot["table"] != snapshot["node"] {
			t.Fatal("torn session snapshot")
		}
	}
	wg.Wait()
}

func TestSessionDataGetOrSetKeepsExistingBinding(t *testing.T) {
	data := &SessionData{}
	if got := data.GetOrSet("account", "first"); got != "first" {
		t.Fatal(got)
	}
	data.Set("account", "bound")
	if got := data.GetOrSet("account", "stale"); got != "bound" {
		t.Fatal(got)
	}
	if got := data.GetOrSet("", "ignored"); got != "" {
		t.Fatal(got)
	}
}
