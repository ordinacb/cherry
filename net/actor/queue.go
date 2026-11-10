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

func (p *queue) Push(v interface{}) bool {
	if v == nil {
		return false
	}
	p.lifecycle.RLock()
	defer p.lifecycle.RUnlock()
	if p.closed {
		return false
	}

	n := new(queueNode)
	n.val = v
	// current producer acquires head node
	prev := (*queueNode)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&p.head)), unsafe.Pointer(n)))

	// release node to consumer
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&prev.next)), unsafe.Pointer(n))

	p._setCount(1)
	return true
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
		p._setCount(-1)
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

func (p *queue) _setCount(delta int32) {
	count := atomic.AddInt32(&p.count, delta)
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
