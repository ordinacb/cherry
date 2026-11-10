package cherryQueue

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	num := 3

	q := NewQueue()
	for i := 0; i < num; i++ {
		q.Push(i)
	}

	for i := 0; i < num; i++ {
		fmt.Println(q.Pop())
	}

}

func BenchmarkNewFIFOQueue(b *testing.B) {
	q := NewQueue()
	for i := 0; i < 1000000; i++ {
		q.Push(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

func TestQueuePop(t *testing.T) {
	q := NewQueue()
	const initial, producers, each = 10, 4, 1000
	for i := 0; i < initial; i++ {
		q.Push(i)
	}
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				q.Push(initial + p*each + i)
			}
		}(p)
	}
	seen := make([]bool, initial+producers*each)
	next := make([]int, producers)
	deadline := time.Now().Add(5 * time.Second)
	for received := 0; received < len(seen); {
		v := q.Pop()
		if v == nil {
			if time.Now().After(deadline) {
				t.Fatalf("queue lost items: received %d/%d", received, len(seen))
			}
			runtime.Gosched()
			continue
		}
		i, ok := v.(int)
		if !ok || i < 0 || i >= len(seen) || seen[i] {
			t.Fatalf("unexpected/duplicate item: %v", v)
		}
		seen[i] = true
		if i < initial {
			if i != received {
				t.Fatalf("initial FIFO order: got %d at %d", i, received)
			}
		} else {
			p, offset := (i-initial)/each, (i-initial)%each
			if offset != next[p] {
				t.Fatalf("producer %d FIFO order: got %d want %d", p, offset, next[p])
			}
			next[p]++
		}
		received++
	}
	wg.Wait()
	if !q.Empty() || q.Pop() != nil {
		t.Fatal("queue not empty after all items were received")
	}
}
