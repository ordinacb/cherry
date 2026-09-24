package cherryActor

import (
	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
)

// Reasons passed to a DropHandler.
const (
	DropActorNotFound = "actor_not_found"
	DropActorNotReady = "actor_not_ready"
	DropChildNotFound = "child_not_found"
	DropFuncNotFound  = "func_not_found"
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
	if dropHandler == nil || m == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			clog.Warnf("[notifyDrop] handler panic. reason = %s, err = %v", reason, r)
		}
	}()
	dropHandler(app, m, reason)
}
