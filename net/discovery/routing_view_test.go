package cherryDiscovery

import (
	"slices"
	"testing"
)

func TestRoutingTypeIsAllocationFree(t *testing.T) {
	d := routingTestDiscovery()
	got, err := d.GetType("account1")
	if err != nil || got != "account" {
		t.Fatalf("GetType: got %q, %v", got, err)
	}
	allocations := testing.AllocsPerRun(100, func() { _, _ = d.GetType("account1") })
	if allocations != 0 {
		t.Errorf("GetType allocates %.0f times to read the node type", allocations)
	}
}

type routingIDReader interface {
	NodeIDsByType(string) []string
}

func TestRoutingIDsFollowMembershipAndRemainDetached(t *testing.T) {
	d := routingTestDiscovery()
	routing, ok := any(d).(routingIDReader)
	if !ok {
		t.Fatal("discovery must expose node IDs without cloning whole members")
	}
	ids := routing.NodeIDsByType("account")
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"account1", "account2", "account3"}) {
		t.Fatalf("initial account IDs: %v", ids)
	}
	ids[0] = "changed"
	d.RemoveMember("account2")
	d.UpdateMember(newTestMember("account3", "gate", "127.0.0.1"))
	if got := routing.NodeIDsByType("account"); !slices.Equal(got, []string{"account1"}) {
		t.Fatalf("updated account IDs: %v", got)
	}
	if kind, err := d.GetType("account3"); err != nil || kind != "gate" {
		t.Fatalf("updated type: %q, %v", kind, err)
	}
	if _, err := d.GetType("account2"); err == nil {
		t.Fatal("removed member still has a type")
	}
}

func BenchmarkRoutingNodeIDs(b *testing.B) {
	d := routingTestDiscovery()
	routing, fast := any(d).(routingIDReader)
	b.ReportAllocs()
	for b.Loop() {
		if fast {
			_ = routing.NodeIDsByType("account")
			continue
		}
		members := d.ListByType("account")
		ids := make([]string, 0, len(members))
		for _, member := range members {
			ids = append(ids, member.GetNodeID())
		}
	}
}

func BenchmarkRoutingNodeType(b *testing.B) {
	d := routingTestDiscovery()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = d.GetType("account1")
	}
}

func routingTestDiscovery() *ComponentDefault {
	d := &ComponentDefault{}
	for _, id := range []string{"account1", "account2", "account3"} {
		member := newTestMember(id, "account", "127.0.0.1")
		member.Settings["metrics_addr"] = ":9000"
		d.AddMember(member)
	}
	d.AddMember(newTestMember("billiard1", "billiard", "127.0.0.1"))
	d.AddMember(newTestMember("gate1", "gate", "127.0.0.1"))
	return d
}
