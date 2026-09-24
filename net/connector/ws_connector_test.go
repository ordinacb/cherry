package cherryConnector

import (
	"net"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNewWSConnector(t *testing.T) {
	addr := freeAddr(t)
	received := make(chan []byte, 1)

	ws := NewWS(addr)
	ws.OnConnect(func(conn net.Conn) {
		go func() {
			defer conn.Close()
			buf := make([]byte, 64)
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			received <- buf[:n]
		}()
	})

	done := make(chan struct{})
	go func() {
		ws.Start()
		close(done)
	}()

	var client *websocket.Conn
	var err error
	for i := 0; i < 50; i++ {
		if client, _, err = websocket.DefaultDialer.Dial("ws://"+addr+"/", nil); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	if err := client.WriteMessage(websocket.BinaryMessage, []byte("ping")); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-received:
		if string(got) != "ping" {
			t.Fatalf("expected ping, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive the frame")
	}

	ws.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}
