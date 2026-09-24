package cherryNats

import (
	"sync"

	"github.com/nats-io/nats.go"
)

var (
	_natsMsgPool = &sync.Pool{
		New: func() any {
			return &nats.Msg{}
		},
	}
)

func GetNatsMsg() *nats.Msg {
	msg := _natsMsgPool.Get().(*nats.Msg)
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	return msg
}

// ReleaseNatsMsg resets the message and returns it to the pool.
// The header map is dropped, not cleared: a caller may have assigned its own
// map, and clearing it would wipe the caller's data. GetNatsMsg allocates a
// fresh one.
func ReleaseNatsMsg(natsMsg *nats.Msg) {
	natsMsg.Header = nil
	natsMsg.Subject = ""
	natsMsg.Reply = ""
	natsMsg.Data = nil
	_natsMsgPool.Put(natsMsg)
}
