package cherryProto

import (
	"sync"

	cconst "github.com/cherry-game/cherry/const"
	cstring "github.com/cherry-game/cherry/extend/string"
)

const (
	MIDKey = "mid"
)

// sessionMu guards every access to Session.Data.
//
// A session is shared by two goroutines by design. The agent's read loop
// stamps the message id onto it before dispatching each request, while the
// backend actors that request reaches write their own keys into it — a bound
// uid, a table id, the node holding the player. Neither side knows about the
// other, and the map underneath had no protection, so a request arriving while
// an actor was writing crashed the whole process with "concurrent map writes".
// It needs the two to overlap within the same microsecond, which is why it
// only showed up once a login grew slow enough to still be finishing when the
// client's next request landed.
//
// One lock for all sessions rather than one per session, because Session is a
// generated protobuf type with no room for a mutex field, and because the
// critical sections are single map operations. Contention is a map lookup
// wide; the alternative is a process-wide crash.
var sessionMu sync.RWMutex

func (x *Session) IsBind() bool {
	return x.Uid > 0
}

func (x *Session) ActorPath() string {
	return x.AgentPath + cconst.DOT + x.Sid
}

func (x *Session) Add(key string, value interface{}) {
	x.Set(key, cstring.ToString(value))
}

func (x *Session) Remove(key string) {
	sessionMu.Lock()
	defer sessionMu.Unlock()

	delete(x.Data, key)
}

func (x *Session) Set(key string, value string) {
	if key == "" || value == "" {
		return
	}

	sessionMu.Lock()
	defer sessionMu.Unlock()

	x.set(key, value)
}

// set writes without locking, for callers already holding the lock.
func (x *Session) set(key string, value string) {
	if key == "" || value == "" {
		return
	}

	x.Data[key] = value
}

func (x *Session) SetMID(mid uint32) {
	x.Add(MIDKey, mid)
}

func (x *Session) GetMID() uint32 {
	return uint32(x.GetUint(MIDKey))
}

func (x *Session) ImportAll(data map[string]string) {
	sessionMu.Lock()
	defer sessionMu.Unlock()

	for k, v := range data {
		x.set(k, v)
	}
}

func (x *Session) Contains(key string) bool {
	sessionMu.RLock()
	defer sessionMu.RUnlock()

	_, found := x.Data[key]
	return found
}

func (x *Session) Equal(key, value string) bool {
	sessionMu.RLock()
	defer sessionMu.RUnlock()

	dataValue, found := x.Data[key]
	if !found {
		return false
	}

	return dataValue == value
}

func (x *Session) Restore(data map[string]string) {
	sessionMu.Lock()
	defer sessionMu.Unlock()

	// Clearing and refilling under one lock, so that no reader can observe the
	// session empty part-way through a restore.
	for k := range x.Data {
		delete(x.Data, k)
	}

	for k, v := range data {
		x.set(k, v)
	}
}

// Clear releases all settings related to current sc
func (x *Session) Clear() {
	sessionMu.Lock()
	defer sessionMu.Unlock()

	for k := range x.Data {
		delete(x.Data, k)
	}
}

// get reads under the shared lock.
func (x *Session) get(key string) (string, bool) {
	sessionMu.RLock()
	defer sessionMu.RUnlock()

	v, ok := x.Data[key]
	return v, ok
}

func (x *Session) GetUint(key string) uint {
	v, ok := x.get(key)
	if !ok {
		return 0
	}

	value, ok := cstring.ToUint(v)
	if !ok {
		return 0
	}
	return value
}

func (x *Session) GetInt(key string) int {
	v, ok := x.get(key)
	if !ok {
		return 0
	}

	value, ok := cstring.ToInt(v)
	if !ok {
		return 0
	}
	return value
}

// GetInt32 returns the value associated with the key as a int32.
func (x *Session) GetInt32(key string) int32 {
	v, ok := x.get(key)
	if !ok {
		return 0
	}

	value, ok := cstring.ToInt32(v)
	if !ok {
		return 0
	}
	return value
}

func (x *Session) GetInt64(key string) int64 {
	v, ok := x.get(key)
	if !ok {
		return 0
	}

	value, ok := cstring.ToInt64(v)
	if !ok {
		return 0
	}
	return value
}

// GetString returns the value associated with the key as a string.
func (x *Session) GetString(key string) string {
	v, _ := x.get(key)
	return v
}
