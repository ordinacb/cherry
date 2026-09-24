package cherryNats

import (
	"errors"
	"testing"
)

func TestAsyncErrorFieldsNilSubscriptionDoesNotPanic(t *testing.T) {
	// nats.go flusher calls AsyncErrorCB(nc, nil, err) when a write flush
	// fails. The old ErrorHandler read sub.Subject unconditionally and
	// crashed the gate process.
	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("nil sub panicked: %v", rec)
		}
	}()

	connected, errMsg, subject := asyncErrorFields(nil, nil, errors.New("flush failed"))
	if connected {
		t.Fatal("nil conn should report not connected")
	}
	if errMsg != "flush failed" {
		t.Fatalf("errMsg = %q", errMsg)
	}
	if subject != "" {
		t.Fatalf("nil sub subject = %q", subject)
	}
}

func TestNatsMaxReconnectsZeroMeansForever(t *testing.T) {
	if got := natsMaxReconnects(0); got != -1 {
		t.Fatalf("0 should map to -1 (unlimited), got %d", got)
	}
	if got := natsMaxReconnects(5); got != 5 {
		t.Fatalf("positive value should pass through, got %d", got)
	}
}
