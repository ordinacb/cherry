package cherryConnector

import (
	"net"
	"testing"
	"time"
)

// freeAddr returns a loopback address with a currently unused port.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestNewTCPConnector(t *testing.T) {
	addr := freeAddr(t)
	connected := make(chan struct{}, 1)

	tcp := NewTCP(addr)
	tcp.OnConnect(func(conn net.Conn) {
		_ = conn.Close()
		connected <- struct{}{}
	})

	done := make(chan struct{})
	go func() {
		tcp.Start()
		close(done)
	}()

	var client net.Conn
	var err error
	for i := 0; i < 50; i++ {
		if client, err = net.Dial("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnect was not called")
	}

	tcp.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

func TestTCPConnectorStopBeforeStart(t *testing.T) {
	tcp := NewTCP(freeAddr(t))
	tcp.Stop() // must not panic on a nil listener
}
