package cherryActor

import (
	"testing"
	"time"

	cfacade "github.com/cherry-game/cherry/facade"
	cproto "github.com/cherry-game/cherry/net/proto"
)

type directLocalTestParent struct {
	Base
	allow bool
}

func (p *directLocalTestParent) DirectLocalChild() bool { return p.allow }

func TestDirectLocalChildDoesNotConsumeParentCapacity(t *testing.T) {
	system := NewSystem()
	system.SetLocalMailboxLimit(1)
	handler := &directLocalTestParent{allow: true}
	parent := localDispatchTestActor(t, system, "table", "", handler)
	child := localDispatchTestActor(t, system, "table", "tid", &Base{})
	parent.child.childActors.Store("tid", child)
	system.actorMap.Store("table", parent)

	if !system.PostLocal(localDispatchTestMessage(parent, nil)) {
		t.Fatal("could not fill the parent mailbox")
	}
	session := &cproto.Session{Mid: 7}
	deadline := time.Now().Add(time.Minute).UnixMilli()
	session.SetQueueDeadlineMillis(deadline)
	if !system.PostLocal(localDispatchTestMessage(child, session)) {
		t.Fatal("the busy parent blocked delivery to an idle child")
	}
	if system.PostLocal(localDispatchTestMessage(child, nil)) {
		t.Fatal("direct dispatch did not enforce the child's capacity")
	}
	message := child.localMail.Pop()
	if message == nil {
		t.Fatal("direct message missing from child")
	}
	defer message.Recycle()
	if message.Session.GetQueueDeadlineMs() != deadline {
		t.Fatal("direct dispatch changed the original deadline")
	}
	if parent.localMail.Count() != 1 {
		t.Fatal("direct dispatch used the parent mailbox")
	}
}

func TestLocalChildDispatchKeepsParentForMissingChildAndOptOut(t *testing.T) {
	system := NewSystem()
	handler := &directLocalTestParent{allow: false}
	parent := localDispatchTestActor(t, system, "table", "", handler)
	child := localDispatchTestActor(t, system, "table", "tid", &Base{})
	parent.child.childActors.Store("tid", child)
	system.actorMap.Store("table", parent)

	if !system.PostLocal(localDispatchTestMessage(child, nil)) {
		t.Fatal("opted-out parent rejected the ordinary local path")
	}
	handler.allow = true
	if !system.PostRemote(localDispatchTestMessage(child, nil)) {
		t.Fatal("remote delivery to the parent failed")
	}
	parent.child.Remove("tid")
	if !system.PostLocal(localDispatchTestMessage(child, nil)) {
		t.Fatal("missing child must use the parent's normal handling")
	}
	if parent.localMail.Count() != 2 || parent.remoteMail.Count() != 1 ||
		child.localMail.Count() != 0 || child.remoteMail.Count() != 0 {
		t.Fatal("a fallback message bypassed the parent")
	}
}

func localDispatchTestActor(t *testing.T, system *System, parent, child string, handler cfacade.IActorHandler) *Actor {
	t.Helper()
	actor, err := newActor(parent, child, handler, system)
	if err != nil {
		t.Fatal(err)
	}
	actor.setState(WorkerState)
	t.Cleanup(func() {
		actor.localMail.onStop(nil)
		actor.remoteMail.onStop(nil)
	})
	return actor
}

func localDispatchTestMessage(actor *Actor, session *cproto.Session) *cfacade.Message {
	message := cfacade.GetMessage()
	message.Target = actor.PathString()
	message.Session = session
	return message
}
