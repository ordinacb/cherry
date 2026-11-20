package cherryActor

import (
	"sync"
	"time"

	clog "github.com/cherry-game/cherry/logger"
)

const slowMessageGroups = 256

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

// slowMessageLog aggregates diagnostics independently of Actor delivery. The active
// and flushing maps are bounded; no Actor waits for a logger or an output pipe.
type slowMessageLog struct {
	mu           sync.Mutex
	pending      map[slowMessageKey]slowMessageStat
	overflow     [4]slowMessageOverflow
	closed       bool
	stopCh, done chan struct{}
	write        func(slowMessageKey, slowMessageStat, bool)
}

func newSlowMessageLog() *slowMessageLog {
	return &slowMessageLog{pending: make(map[slowMessageKey]slowMessageStat), write: writeSlowMessage}
}

func (s *slowMessageLog) record(key slowMessageKey, source string, elapsed, limit, at int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	stat, found := s.pending[key]
	if !found && len(s.pending) >= slowMessageGroups {
		index := 0
		if key.mailbox == LocalName {
			index += 2
		}
		if key.phase == "execution" {
			index++
		}
		bucket := &s.overflow[index]
		if elapsed > bucket.stat.maxMS {
			bucket.key = key
		}
		bucket.stat.add(source, elapsed, limit, at)
		return
	}
	stat.add(source, elapsed, limit, at)
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
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return
	}
	batch, overflow := s.pending, s.overflow
	s.pending = make(map[slowMessageKey]slowMessageStat)
	s.overflow = [4]slowMessageOverflow{}
	s.mu.Unlock()
	for key, stat := range batch {
		s.write(key, stat, false)
	}
	for _, bucket := range overflow {
		if bucket.stat.count > 0 {
			s.write(bucket.key, bucket.stat, true)
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
