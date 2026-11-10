package cherryConnector

import (
	"io"
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

func boundAddr(t *testing.T, connector *Connector) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		connector.lifecycle.RLock()
		listener := connector.listener
		connector.lifecycle.RUnlock()
		if listener != nil {
			return listener.Addr().String()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("listener did not bind")
	return ""
}

func TestNewTCPConnector(t *testing.T) {
	connected := make(chan struct{}, 1)

	tcp := NewTCP("127.0.0.1:0")
	tcp.OnConnect(func(conn net.Conn) {
		defer conn.Close()
		payload := make([]byte, 4)
		if _, err := io.ReadFull(conn, payload); err == nil && string(payload) == "ping" {
			connected <- struct{}{}
		}
	})
	t.Cleanup(tcp.Stop)

	done := make(chan struct{})
	go func() {
		tcp.Start()
		close(done)
	}()
	addr := boundAddr(t, &tcp.Connector)

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
	// 穿过可能延迟建立后端连接的本机转发层，验证真实收包而非仅完成 TCP 握手。
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}

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
