package cherryActor

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type (
	queue struct {
		lifecycle  sync.RWMutex
		closed     bool
		head, tail *queueNode
		C          chan int32
		count      int32
		totalDepth *atomic.Int64 // optional node-wide Remote depth
	}

	queueNode struct {
		next *queueNode
		val  interface{}
	}
)

func newQueue() queue {
	stub := &queueNode{}
	return queue{
		head:  stub,
		tail:  stub,
		C:     make(chan int32, 1),
		count: 0,
	}
}

type queuePushResult uint8

const (
	queueAccepted queuePushResult = iota
	queueClosed
	queueFull
)

func (p *queue) Push(v interface{}) bool {
	return p.push(v, 0) == queueAccepted
}

// push reserves capacity before publishing the node. A zero limit preserves
// the existing admission rules for Remote, timers and events.
func (p *queue) push(v interface{}, limit int32) queuePushResult {
	if v == nil {
		return queueClosed
	}
	p.lifecycle.RLock()
	defer p.lifecycle.RUnlock()
	if p.closed {
		return queueClosed
	}
	for {
		count := p.Count()
		if limit > 0 && count >= limit {
			return queueFull
		}
		if atomic.CompareAndSwapInt32(&p.count, count, count+1) {
			break
		}
	}

	if p.totalDepth != nil {
		p.totalDepth.Add(1)
	}
	n := new(queueNode)
	n.val = v
	// current producer acquires head node
	prev := (*queueNode)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&p.head)), unsafe.Pointer(n)))

	// release node to consumer
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&prev.next)), unsafe.Pointer(n))

	p.signal(p.Count())
	return queueAccepted
}

func (p *queue) Pop() interface{} {
	p.lifecycle.RLock()
	defer p.lifecycle.RUnlock()
	if p.closed {
		return nil
	}
	return p.pop()
}

// pop has one consumer; Destroy holds the exclusive lifecycle lock when draining.
func (p *queue) pop() interface{} {
	tail := p.tail
	next := (*queueNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next)))) // acquire
	if next != nil {
		p.tail = next
		v := next.val
		next.val = nil
		if p.totalDepth != nil {
			p.totalDepth.Add(-1)
		}
		p.signal(atomic.AddInt32(&p.count, -1))
		return v
	}

	return nil
}

func (p *queue) Empty() bool {
	p.lifecycle.RLock()
	defer p.lifecycle.RUnlock()
	if p.closed {
		return true
	}
	tail := p.tail
	next := (*queueNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next))))
	return next == nil
}

func (p *queue) Count() int32 {
	return atomic.LoadInt32(&p.count)
}

func (p *queue) signal(count int32) {
	if count > 0 {
		select {
		case p.C <- count:
		default:
		}
	}
}

// Destroy excludes active producers and returns accepted items for disposal.
func (p *queue) Destroy() []interface{} {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var pending []interface{}
	for v := p.pop(); v != nil; v = p.pop() {
		pending = append(pending, v)
	}
	close(p.C)
	p.head = nil
	p.tail = nil
	atomic.StoreInt32(&p.count, 0)
	return pending
}
