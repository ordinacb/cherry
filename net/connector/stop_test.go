package cherryConnector

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestStopRejectsBlockedConnectionDelivery(t *testing.T) {
	c := NewConnector(0)
	conn, peer := net.Pipe()
	defer peer.Close()
	done := make(chan struct{})
	go func() { c.InChan(conn); close(done) }()
	c.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delivery remained blocked after stop")
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("rejected connection not closed: %v", err)
	}
	c.Stop()
}

func TestStopRacingListenerPublication(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := NewConnector(1)
		var listener net.Listener
		var err error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); listener, err = c.GetListener("", "", "127.0.0.1:0") }()
		go func() { defer wg.Done(); c.Stop() }()
		wg.Wait()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
		if listener != nil {
			_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(20 * time.Millisecond))
			accepted, err := listener.Accept()
			if accepted != nil {
				_ = accepted.Close()
			}
			if !errors.Is(err, net.ErrClosed) {
				_ = listener.Close()
				t.Fatalf("listener survived stop: %v", err)
			}
		}
	}
}

func TestTCPStartAfterStopDoesNotOpenListener(t *testing.T) {
	tcp := NewTCP("127.0.0.1:0")
	tcp.OnConnect(func(conn net.Conn) { _ = conn.Close() })
	tcp.Stop()
	tcp.Start()
	if tcp.listener != nil {
		t.Fatal("start revived stopped listener")
	}
}
