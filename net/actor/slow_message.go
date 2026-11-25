package cherryActor

import (
	"sync"
	"sync/atomic"
	"time"

	clog "github.com/cherry-game/cherry/logger"
)

const (
	slowMessageGroups    = 256
	slowMessageInboxSize = 4096
)

type slowMessageKey struct {
	mailbox, phase, target, method string
}

type slowMessageStat struct {
	count, maxMS, limitMS, firstAt, lastAt int64
	source                                 string
}

type slowMessageOverflow struct {
	key  slowMessageKey
	stat slowMessageStat
}

type slowMessageSample struct {
	key                slowMessageKey
	source             string
	elapsed, limit, at int64
}

// slowMessageLog accepts bounded diagnostic samples from Actors. Only the
// background worker aggregates them and writes to the log sink.
type slowMessageLog struct {
	mu                 sync.RWMutex // Serializes submission with shutdown only.
	closed             bool
	inbox              chan slowMessageSample
	inboxOverflow      [4]atomic.Int64
	pending            map[slowMessageKey]slowMessageStat
	overflow           [4]slowMessageOverflow
	stopCh, done       chan struct{}
	write              func(slowMessageKey, slowMessageStat, bool)
	writeInboxOverflow func(mailbox, phase string, count int64)
}

func newSlowMessageLog() *slowMessageLog {
	return &slowMessageLog{
		inbox:   make(chan slowMessageSample, slowMessageInboxSize),
		pending: make(map[slowMessageKey]slowMessageStat, slowMessageGroups),
		write:   writeSlowMessage,
		writeInboxOverflow: func(mailbox, phase string, count int64) {
			clog.Warnw("actor_message_slow_inbox_overflow", "mailbox", mailbox, "phase", phase, "count", count)
		},
	}
}

func (s *slowMessageLog) record(key slowMessageKey, source string, elapsed, limit, at int64) {
	if s == nil {
		return
	}
	s.mu.RLock()
	if !s.closed {
		select {
		case s.inbox <- slowMessageSample{key, source, elapsed, limit, at}:
		default:
			s.inboxOverflow[slowMessageLane(key)].Add(1)
		}
	}
	s.mu.RUnlock()
}

func slowMessageLane(key slowMessageKey) int {
	index := 0
	if key.mailbox == LocalName {
		index += 2
	}
	if key.phase == "execution" {
		index++
	}
	return index
}

func (s *slowMessageLog) aggregate(sample slowMessageSample) {
	key := sample.key
	stat, found := s.pending[key]
	if !found && len(s.pending) >= slowMessageGroups {
		bucket := &s.overflow[slowMessageLane(key)]
		if sample.elapsed > bucket.stat.maxMS {
			bucket.key = key
		}
		bucket.stat.add(sample.source, sample.elapsed, sample.limit, sample.at)
		return
	}
	stat.add(sample.source, sample.elapsed, sample.limit, sample.at)
	s.pending[key] = stat
}

func (s *slowMessageStat) add(source string, elapsed, limit, at int64) {
	if s.count == 0 {
		s.firstAt = at
	}
	s.count++
	s.lastAt = at
	if elapsed > s.maxMS {
		s.maxMS, s.limitMS, s.source = elapsed, limit, source
	}
}

func (s *slowMessageLog) start() {
	s.stopCh, s.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.flush()
			case <-s.stopCh:
				s.flush()
				return
			}
		}
	}()
}

func (s *slowMessageLog) stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	alreadyClosed := s.closed
	s.closed = true
	s.mu.Unlock()
	if alreadyClosed {
		return
	}
	if s.done == nil {
		s.flush()
		return
	}
	close(s.stopCh)
	<-s.done
}

func (s *slowMessageLog) flush() {
	for range len(s.inbox) {
		s.aggregate(<-s.inbox)
	}
	for key, stat := range s.pending {
		s.write(key, stat, false)
	}
	clear(s.pending)
	for _, bucket := range s.overflow {
		if bucket.stat.count > 0 {
			s.write(bucket.key, bucket.stat, true)
		}
	}
	clear(s.overflow[:])
	mailboxes := [...]string{RemoteName, LocalName}
	phases := [...]string{"arrival", "execution"}
	for index := range s.inboxOverflow {
		if count := s.inboxOverflow[index].Swap(0); count > 0 {
			s.writeInboxOverflow(mailboxes[index/2], phases[index%2], count)
		}
	}
}

func writeSlowMessage(key slowMessageKey, stat slowMessageStat, overflow bool) {
	fields := []any{"count", stat.count, "ms", stat.maxMS, "limit_ms", stat.limitMS,
		"first_at", time.UnixMilli(stat.firstAt), "last_at", time.UnixMilli(stat.lastAt)}
	fields = append(fields, "mailbox", key.mailbox, "phase", key.phase, "target", key.target, "method", key.method, "source", stat.source)
	if overflow {
		clog.Warnw("actor_message_slow_overflow", fields...)
		return
	}
	clog.Warnw("actor_message_slow", fields...)
}
