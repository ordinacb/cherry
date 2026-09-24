package pomeloClient

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// These tests need a live pomelo WebSocket server. Set POMELO_TEST_WS_ADDR
// (e.g. 127.0.0.1:8003) to run them; otherwise they are skipped.
func testAddr(tb testing.TB) string {
	addr := os.Getenv("POMELO_TEST_WS_ADDR")
	if addr == "" {
		tb.Skip("POMELO_TEST_WS_ADDR not set")
	}
	return addr
}

func TestClient(t *testing.T) {
	addr := testAddr(t)

	client := New(
		WithRequestTimeout(1 * time.Second),
	)
	client.TagName = "dog egg"
	if err := client.ConnectToWS(addr, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()
}

func BenchmarkClient(b *testing.B) {
	addr := testAddr(b)

	for i := 0; i < b.N; i++ {
		client := New(
			WithRequestTimeout(1 * time.Second),
		)
		client.TagName = fmt.Sprintf("c-%d", i)
		client.ConnectToWS(addr, "")

		client.Disconnect()
	}
}
