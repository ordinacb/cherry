package cherryCluster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	cfacade "github.com/cherry-game/cherry/facade"
	cnats "github.com/cherry-game/cherry/net/nats"
	cproto "github.com/cherry-game/cherry/net/proto"
	cprofile "github.com/cherry-game/cherry/profile"
	"github.com/nats-io/nats.go"
)

type isolationTestApp struct {
	cfacade.IApplication
	discovery cfacade.IDiscovery
}

func (p isolationTestApp) NodeID() string                { return "gate" }
func (p isolationTestApp) NodeType() string              { return "gate" }
func (p isolationTestApp) IsFrontend() bool              { return true }
func (p isolationTestApp) Discovery() cfacade.IDiscovery { return p.discovery }

type isolationTestDiscovery struct {
	cfacade.IDiscovery
}

func (isolationTestDiscovery) GetMember(id string) (cfacade.IMember, bool) {
	return &cproto.Member{NodeID: id, NodeType: id}, true
}

func TestFrontendLocalPublisherIndependentOfGeneralConnection(t *testing.T) {
	address := os.Getenv("BILLIARD_TEST_NATS")
	if address == "" {
		t.Skip("set isolated BILLIARD_TEST_NATS")
	}
	prefix := "isolation" + cnats.NewStringReqID()
	profile := map[string]any{
		"cluster": map[string]any{"nats": map[string]any{
			"address":                       address,
			"prefix":                        prefix,
			"frontend_local_publish_target": "billiard.table",
		}},
		"node": map[string]any{"gate": []any{map[string]any{"node_id": "gate", "enable": true}}},
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cprofile.Init(filepath.ToSlash(path), "gate"); err != nil {
		t.Fatal(err)
	}

	p := New()
	p.Set(isolationTestApp{discovery: isolationTestDiscovery{}})
	p.loadNatsConfig()
	defer p.OnStop()

	receiver, err := nats.Connect(address)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sub, err := receiver.SubscribeSync(p.GetLocalSubject(prefix, "billiard", "billiard"))
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}

	p.publishConnect.Close()
	for _, name := range []string{"operation", "hitBallOver"} {
		msg := cfacade.GetMessage()
		msg.Source, msg.Target, msg.FuncName = "gate.user", "billiard.table.t1", name
		if err := p.PublishLocal("billiard", msg); err != nil {
			t.Fatalf("%s must reach the table when the general publisher is unavailable: %v", name, err)
		}
	}
	for _, name := range []string{"operation", "hitBallOver"} {
		wire, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var received cfacade.Message
		if err := received.Unmarshal(wire.Data); err != nil {
			t.Fatal(err)
		}
		if received.Target != "billiard.table.t1" || received.FuncName != name {
			t.Fatalf("changed destination or order: target=%q method=%q, want %q", received.Target, received.FuncName, name)
		}
	}

	account := cfacade.GetMessage()
	account.Source, account.Target, account.FuncName = "gate.user", "account.player.1", "getBag"
	if err := p.PublishLocal("account", account); err == nil {
		t.Fatal("ordinary Local requests must still use the general publisher")
	}
	room := cfacade.GetMessage()
	room.Source, room.Target, room.FuncName = "gate.user", "billiard.room.rid:3001", "matching"
	if err := p.PublishLocal("billiard", room); err == nil {
		t.Fatal("room matching must still use the general publisher")
	}
	remote := cfacade.GetMessage()
	remote.Source, remote.Target, remote.FuncName = "gate.user", "billiard.table.t1", "internal"
	if err := p.PublishRemote("billiard", remote); err == nil {
		t.Fatal("Remote requests must still use the general publisher")
	}
}
