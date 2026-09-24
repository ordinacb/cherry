package cherryActor

import (
	"testing"

	cfacade "github.com/cherry-game/cherry/facade"
)

func TestNotifyDropCallsHandlerAndRecovers(t *testing.T) {
	defer SetDropHandler(nil)

	var gotReason string
	SetDropHandler(func(_ cfacade.IApplication, m *cfacade.Message, reason string) {
		gotReason = reason
		panic("boom")
	})

	notifyDrop(nil, &cfacade.Message{FuncName: "x"}, DropActorNotFound)
	if gotReason != DropActorNotFound {
		t.Fatalf("expected reason %q, got %q", DropActorNotFound, gotReason)
	}

	notifyDrop(nil, nil, DropActorNotFound) // nil message must be ignored
}
