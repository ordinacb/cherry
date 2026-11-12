package cherryActor

import (
	ccode "github.com/cherry-game/cherry/code"
	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
	cproto "github.com/cherry-game/cherry/net/proto"
)

// Reasons passed to a DropHandler.
const (
	DropActorNotFound = "actor_not_found"
	DropActorNotReady = "actor_not_ready"
	DropChildNotFound = "child_not_found"
	DropFuncNotFound  = "func_not_found"
	DropMailboxFull   = "actor_mailbox_full"
	DropQueueExpired  = "actor_queue_expired"
)

// DropHandler is called when a message is discarded before reaching a handler.
// It runs on the delivering goroutine before the message is recycled, so it
// must not keep m. Typical use: answer the client request carried in
// m.Session so it fails fast instead of timing out.
type DropHandler func(app cfacade.IApplication, m *cfacade.Message, reason string)

var dropHandler DropHandler

// SetDropHandler registers the drop callback. Call before startup.
func SetDropHandler(fn DropHandler) { dropHandler = fn }

func notifyDrop(app cfacade.IApplication, m *cfacade.Message, reason string) {
	if m == nil {
		return
	}
	if reason == DropMailboxFull || reason == DropQueueExpired {
		status := mailboxDropCode(reason)
		switch {
		case m.ChanResult != nil:
			select {
			case m.ChanResult <- &cproto.Response{Code: status}:
			default:
			}
			return
		case m.Reply != "":
			replyReponseCode(app, m, status)
			return
		}
	}
	if dropHandler == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			clog.Warnf("[notifyDrop] handler panic. reason = %s, err = %v", reason, r)
		}
	}()
	dropHandler(app, m, reason)
}

func mailboxDropCode(reason string) int32 {
	switch reason {
	case DropMailboxFull:
		return ccode.ActorMailboxFull
	case DropQueueExpired:
		return ccode.ActorQueueTimeout
	default:
		return ccode.ActorInvokeRemoteError
	}
}
