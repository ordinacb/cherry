package cherryActor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	creflect "github.com/cherry-game/cherry/extend/reflect"
	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type blockedSlowLogSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	bytes.Buffer
}

func TestSlowMessageRecordDefersAggregation(t *testing.T) {
	s := newSlowMessageLog()
	key := slowMessageKey{mailbox: RemoteName, phase: "arrival", target: "gate.user-out-0", method: "response"}
	s.record(key, "first", 101, 100, 1000)
	s.record(key, "worst", 250, 100, 1001)
	if len(s.pending) != 0 {
		t.Fatal("record aggregated slow messages on the Actor's goroutine")
	}
	var got slowMessageStat
	s.write = func(_ slowMessageKey, stat slowMessageStat, _ bool) { got = stat }
	s.flush()
	if got.count != 2 || got.maxMS != 250 || got.source != "worst" {
		t.Fatalf("pending samples were not aggregated: %+v", got)
	}
}

// A stalled log sink retains a bounded number of diagnostic samples and groups.
// Excess samples are counted without blocking Actors.
func TestSlowMessageLogBoundsGroupsWhileWriterBlocked(t *testing.T) {
	s := newSlowMessageLog()
	entered, release, flushed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var total, overflow, inboxOverflow, maxMS int64
	var once sync.Once
	s.write = func(_ slowMessageKey, stat slowMessageStat, over bool) {
		once.Do(func() { close(entered); <-release })
		total += stat.count
		if over {
			overflow += stat.count
		}
		if stat.maxMS > maxMS {
			maxMS = stat.maxMS
		}
	}
	s.writeInboxOverflow = func(mailbox, phase string, count int64) {
		if mailbox != RemoteName || phase != "arrival" {
			t.Errorf("wrong inbox overflow: %s/%s", mailbox, phase)
		}
		inboxOverflow += count
	}
	s.record(slowMessageKey{target: "initial"}, "source", 101, 100, 1000)
	go func() { defer close(flushed); s.flush() }()
	<-entered
	var producers sync.WaitGroup
	const workers = 8
	const perWorker = slowMessageInboxSize / workers * 2
	for worker := range workers {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := range perWorker {
				s.record(slowMessageKey{target: fmt.Sprintf("%d-%d", worker, i)}, "source", 999, 100, 2000)
			}
		}()
	}
	produced := make(chan struct{})
	go func() { producers.Wait(); close(produced) }()
	select {
	case <-produced:
	case <-time.After(time.Second):
		t.Error("producers waited for the blocked diagnostic writer")
	}
	close(release)
	<-produced
	<-flushed
	if len(s.inbox) != slowMessageInboxSize || len(s.pending) != 0 {
		t.Fatalf("retained %d samples and %d groups", len(s.inbox), len(s.pending))
	}
	s.stop()
	if total != slowMessageInboxSize+1 || overflow != slowMessageInboxSize-slowMessageGroups ||
		inboxOverflow != workers*perWorker-slowMessageInboxSize || maxMS != 999 {
		t.Fatalf("counts lost: total=%d overflow=%d inbox_overflow=%d max_ms=%d", total, overflow, inboxOverflow, maxMS)
	}
}

func TestSlowMessageLogKeepsPhasesAndWorstSource(t *testing.T) {
	s := newSlowMessageLog()
	key := slowMessageKey{mailbox: "remote", phase: "arrival", target: "gate.user-out-0", method: "response"}
	s.record(key, "account.player.1", 120, 100, 1000)
	s.record(key, "account.player.2", 260, 100, 1001)
	s.record(key, "account.player.3", 130, 100, 1002)
	execution := key
	execution.phase = "execution"
	s.record(execution, "account.player.4", 150, 100, 1003)
	rows := make(map[slowMessageKey]slowMessageStat)
	s.write = func(k slowMessageKey, v slowMessageStat, overflow bool) {
		if overflow {
			t.Error("unexpected overflow")
		}
		rows[k] = v
	}
	s.stop()
	stat := rows[key]
	if len(rows) != 2 || stat.count != 3 || stat.maxMS != 260 || stat.source != "account.player.2" || stat.firstAt != 1000 || stat.lastAt != 1002 {
		t.Fatalf("bad aggregate: %+v", rows)
	}
	s.record(key, "late", 999, 100, 2000)
	if len(s.inbox) != 0 {
		t.Fatal("closed reporter retained diagnostics")
	}
}

func TestSlowMessageOverflowKeepsMailboxPhaseAndWorstIdentity(t *testing.T) {
	s := newSlowMessageLog()
	for i := range slowMessageGroups {
		s.record(slowMessageKey{target: fmt.Sprint(i)}, "seed", 101, 100, 1000)
	}
	for _, mailbox := range []string{LocalName, RemoteName} {
		for _, phase := range []string{"arrival", "execution"} {
			s.record(slowMessageKey{mailbox, phase, "slow-target", "slow-method"}, "slow-source", 500, 100, 2000)
			s.record(slowMessageKey{mailbox, phase, "other-target", "other-method"}, "other-source", 200, 100, 2001)
		}
	}
	seen := make(map[[2]string]bool)
	s.write = func(k slowMessageKey, v slowMessageStat, overflow bool) {
		if !overflow {
			return
		}
		seen[[2]string{k.mailbox, k.phase}] = true
		if k.target != "slow-target" || k.method != "slow-method" || v.source != "slow-source" || v.count != 2 || v.maxMS != 500 {
			t.Errorf("overflow lost phase/sample: key=%+v stat=%+v", k, v)
		}
	}
	s.stop()
	if len(seen) != 4 {
		t.Fatalf("overflow phases merged: %+v", seen)
	}
}

func (s *blockedSlowLogSink) Write(p []byte) (int, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Buffer.Write(p)
}

// Slow diagnostics must not stall delivery when stdout or its collector stops reading.
func TestSlowMessageLoggingDoesNotBlockRemoteDrain(t *testing.T) {
	sink := &blockedSlowLogSink{entered: make(chan struct{}), release: make(chan struct{})}
	previous := clog.DefaultLogger
	clog.DefaultLogger = &clog.CherryLogger{SugaredLogger: zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.Lock(zapcore.AddSync(sink)), zap.WarnLevel)).Sugar()}
	system := NewSystem()
	system.Start(&callWaitTestApp{})
	system.slowMessages.record(slowMessageKey{mailbox: RemoteName, phase: "arrival", target: "seed", method: "deliver"}, "source", 101, 100, time.Now().UnixMilli())
	select {
	case <-sink.entered:
	case <-time.After(3 * time.Second):
		close(sink.release)
		system.Stop()
		clog.DefaultLogger = previous
		t.Fatal("background slow log writer did not start")
	}
	actor, err := newActor("output", "", &Base{}, system)
	if err != nil {
		t.Fatal(err)
	}
	actor.remoteMail.Register("deliver", func() {})
	const count = 1024
	for i := range count {
		m := cfacade.GetMessage()
		m.Source = "account.player"
		m.Target = actor.PathString()
		m.FuncName = "deliver"
		m.Args = i
		m.BuildTime = time.Now().Add(-time.Second).UnixMilli()
		if reason := actor.postMessage(m, false); reason != "" {
			t.Fatalf("remote delivery rejected: %s", reason)
		}
	}
	seen := make([]int, 0, count)
	system.remoteInvokeFunc = func(_ cfacade.IApplication, _ *creflect.FuncInfo, m *cfacade.Message) {
		seen = append(seen, m.Args.(int))
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range count {
			actor.processRemote()
		}
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Error("slow-message logging blocked the Remote mailbox behind stdout")
	}
	close(sink.release)
	<-done
	actor.onStop()
	system.Stop()
	clog.DefaultLogger = previous
	if len(seen) != count || system.RemoteMailboxDepth() != 0 {
		t.Fatalf("processed %d/%d, depth=%d", len(seen), count, system.RemoteMailboxDepth())
	}
	for i, n := range seen {
		if i != n {
			t.Fatalf("delivery reordered at %d: %d", i, n)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(sink.Bytes()))
	var reported int64
	for {
		var row struct {
			Message string `json:"msg"`
			Count   int64  `json:"count"`
			Target  string `json:"target"`
		}
		if err := decoder.Decode(&row); err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}
		if row.Message == "actor_message_slow" && row.Target == actor.PathString() {
			reported += row.Count
		}
	}
	if reported != count {
		t.Fatalf("shutdown flushed %d/%d slow diagnostics", reported, count)
	}
}
