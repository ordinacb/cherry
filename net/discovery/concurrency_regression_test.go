package cherryDiscovery

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cfacade "github.com/cherry-game/cherry/facade"
	cnats "github.com/cherry-game/cherry/net/nats"
	cproto "github.com/cherry-game/cherry/net/proto"
	"github.com/nats-io/nats.go"
)

// 注册与分发可以由启动线程和 NATS 回调同时触发；三类监听器均须支持。
func TestDiscoveryConcurrentListenerRegistration(t *testing.T) {
	d := &ComponentDefault{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			d.OnAddMember(func(cfacade.IMember) {})
			d.OnUpdateMember(func(cfacade.IMember) {})
			d.OnRemoveMember(func(cfacade.IMember) {})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			d.AddMember(newTestMember("worker", "game", "one"))
			d.UpdateMember(newTestMember("worker", "game", "two"))
			d.RemoveMember("worker")
		}
	}()
	wg.Wait()
}

// API、入参和监听器拿到的 map 都是各自快照，不能借用后污染发现表或其它回调。
func TestDiscoveryMemberSnapshotsAreDetached(t *testing.T) {
	d := &ComponentDefault{}
	input := &cproto.Member{NodeID: "worker", NodeType: "game", LastAt: 10, Settings: map[string]string{"zone": "original"}}
	d.OnAddMember(func(m cfacade.IMember) { m.GetSettings()["zone"] = "callback"; d.OnAddMember(func(cfacade.IMember) {}) })
	d.OnAddMember(func(m cfacade.IMember) {
		if m.GetSettings()["zone"] != "original" {
			t.Error("回调共享了可变快照")
		}
	})
	d.AddMember(input)
	input.Settings["zone"] = "caller"
	for _, snapshot := range []cfacade.IMember{d.Map()["worker"], d.ListByType("game")[0]} {
		snapshot.GetSettings()["zone"] = "escaped"
	}
	member, _ := d.GetMember("worker")
	member.GetSettings()["zone"] = "escaped"
	member.(*cproto.Member).LastAt = 99
	stored, _ := d.GetMember("worker")
	if stored.GetSettings()["zone"] != "original" || stored.(*cproto.Member).LastAt != 10 {
		t.Fatal("发现表被外部快照修改", stored)
	}
}

// 心跳与注册回复使用实际 NATS 回调和 protobuf 序列化，复现 LastAt 发布期间的数据竞争。
func TestDiscoveryHeartbeatConcurrentMemberSerialization(t *testing.T) {
	url := os.Getenv("BILLIARD_TEST_NATS")
	if url == "" {
		t.Skip("设置隔离 BILLIARD_TEST_NATS 才执行实际心跳")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	m := newTestMaster("master", "master")
	m.thisMember = &cproto.Member{NodeID: "master"}
	m.heartbeatSubject = "cherry.race-test." + time.Now().Format("150405.000000000")
	m.publishConnect = cnats.NewConnect("test-publish")
	m.publishConnect.Conn = nc
	m.subscribeConnect = cnats.NewConnect("test-subscribe")
	m.subscribeConnect.Conn = nc
	m.AddMember(&cproto.Member{NodeID: "worker", NodeType: "game", LastAt: 1, Settings: map[string]string{}})
	m.heartbeatSubscribe()
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
	payload, _ := m.NodeID2Bytes("worker")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1500; i++ {
			if _, err := m.memberList2Bytes(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 300; i++ {
		if _, err = nc.Request(m.heartbeatSubject, payload, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	current, _ := m.GetMember("worker")
	if current.(*cproto.Member).LastAt <= 1 {
		t.Fatal("心跳只改了副本，没有更新发现表")
	}
	if !removeIsStale(current, 1) {
		t.Fatal("过期删除会删除已续心跳的节点")
	}
}

// 超时扫描与删除广播都必须保留新注册/心跳，且已返回的快照不能随后改变。
func TestDiscoveryConditionalRemovalAndHeartbeatSnapshot(t *testing.T) {
	m := newTestMaster("master", "master")
	removed := 0
	m.OnRemoveMember(func(cfacade.IMember) { removed++; m.OnRemoveMember(func(cfacade.IMember) {}) })
	m.AddMember(&cproto.Member{NodeID: "worker", LastAt: 10})
	before, _ := m.GetMember("worker")
	if !m.refreshHeartbeat("worker", 20) {
		t.Fatal("没有续上已有节点")
	}
	if before.(*cproto.Member).LastAt != 10 {
		t.Fatal("已返回快照被心跳修改")
	}
	if m.removeMemberAt("worker", 10) {
		t.Fatal("旧超时删除了已心跳节点")
	}
	m.AddMember(&cproto.Member{NodeID: "worker", LastAt: 30, Settings: map[string]string{"generation": "new"}})
	if m.removeMemberAt("worker", 20) {
		t.Fatal("旧广播删除了同ID新注册")
	}
	if removed != 0 {
		t.Fatal("忽略旧删除仍触发了离线回调")
	}
	if !m.removeMemberAt("worker", 30) || removed != 1 {
		t.Fatal("同时间戳删除没有执行一次")
	}
	m.AddMember(&cproto.Member{NodeID: "worker", LastAt: 40})
	if !m.removeMemberAt("worker", 0) || removed != 2 {
		t.Fatal("正常下线不再无条件删除")
	}
	if m.refreshHeartbeat("worker", 50) {
		t.Fatal("未知节点被心跳复活")
	}
}

// 本地设置、注册序列化与列表回复必须共享同一快照发布规则。
func TestDiscoverySettingsConcurrentSerialization(t *testing.T) {
	m := newTestMaster("worker", "master")
	m.thisMember = &cproto.Member{NodeID: "worker", NodeType: "game", Settings: map[string]string{}}
	m.AddMember(m.thisMember)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.UpdateSettings(map[string]string{"zone": "new", "generation": "one"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if _, err := m.memberList2Bytes(); err != nil {
				t.Error(err)
			}
			if _, err := m.member2Bytes(m.localMemberSnapshot()); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
	stored, _ := m.GetMember("worker")
	if stored.GetSettings()["zone"] != "new" {
		t.Fatal("本地更新未发布到成员表")
	}
}

// 两个本地发布者不能把 thisMember 与发现表留在不同版本；观察也使用同一把锁。
func TestDiscoveryConcurrentLocalSettingsPublication(t *testing.T) {
	m := newTestMaster("worker", "master")
	m.thisMember = &cproto.Member{NodeID: "worker", Settings: map[string]string{"generation": "initial"}}
	m.AddMember(m.thisMember)
	start := make(chan struct{})
	var done sync.WaitGroup
	var mismatch atomic.Bool
	done.Add(3)
	for _, value := range []string{"writer-a", "writer-b"} {
		go func(value string) {
			defer done.Done()
			<-start
			for i := 0; i < 1500; i++ {
				m.UpdateSetting("generation", value)
			}
		}(value)
	}
	go func() {
		defer done.Done()
		<-start
		for i := 0; i < 10000; i++ {
			m.memberMu.RLock()
			stored, _ := m.memberMap.Load("worker")
			if stored.(*cproto.Member).Settings["generation"] != m.thisMember.Settings["generation"] {
				mismatch.Store(true)
			}
			m.memberMu.RUnlock()
		}
	}()
	close(start)
	done.Wait()
	if mismatch.Load() {
		t.Fatal("本地成员与发现表发布了不同版本的设置")
	}
}
