package cherryConnector

import (
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
)

type (
	Connector struct {
		lifecycle     sync.RWMutex
		dispatchOnce  sync.Once
		listener      net.Listener
		onConnectFunc cfacade.OnConnectFunc
		connChan      chan net.Conn
		running       int32
		done          chan struct{}
		dispatchDone  chan struct{}
	}
)

func NewConnector(size int) Connector {
	return Connector{
		connChan:     make(chan net.Conn, size),
		running:      1,
		done:         make(chan struct{}),
		dispatchDone: make(chan struct{}),
	}
}

func (p *Connector) OnConnect(fn cfacade.OnConnectFunc) {
	if fn != nil {
		p.onConnectFunc = fn
	}
}

func (p *Connector) InChan(conn net.Conn) {
	if conn == nil {
		return
	}
	p.lifecycle.RLock()
	if !p.Running() {
		p.lifecycle.RUnlock()
		_ = conn.Close()
		return
	}
	select {
	case p.connChan <- conn:
		p.lifecycle.RUnlock()
	case <-p.done:
		p.lifecycle.RUnlock()
		_ = conn.Close()
	}
}

func (p *Connector) Start() {
	if p.onConnectFunc == nil {
		panic("onConnectFunc is nil.")
	}

	p.dispatchOnce.Do(func() {
		go func() {
			defer close(p.dispatchDone)
			for {
				select {
				case <-p.done:
					return
				case conn := <-p.connChan:
					if !p.Running() {
						_ = conn.Close()
						continue
					}
					p.onConnectFunc(conn)
				}
			}
		}()
	})
}

func (p *Connector) Stop() {
	if atomic.SwapInt32(&p.running, 0) == 0 {
		return
	}
	close(p.done)
	p.lifecycle.Lock()
	listener := p.listener
	var pending []net.Conn
	for {
		select {
		case conn := <-p.connChan:
			pending = append(pending, conn)
		default:
			goto drained
		}
	}
drained:
	p.lifecycle.Unlock()
	for _, conn := range pending {
		_ = conn.Close()
	}
	if listener == nil {
		return
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		clog.Errorf("Failed to stop: %s", err)
	}
}

func (p *Connector) Running() bool {
	return atomic.LoadInt32(&p.running) != 0
}

func (p *Connector) GetListener(certFile, keyFile, address string) (net.Listener, error) {
	if !p.Running() {
		return nil, net.ErrClosed
	}
	var err error
	var listener net.Listener
	if certFile == "" || keyFile == "" {
		listener, err = net.Listen("tcp", address)
	} else {
		var crt tls.Certificate
		crt, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err == nil {
			listener, err = tls.Listen("tcp", address, &tls.Config{Certificates: []tls.Certificate{crt}})
		}
	}
	if err != nil {
		return nil, err
	}
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	if !p.Running() {
		_ = listener.Close()
		return nil, net.ErrClosed
	}
	p.listener = listener
	return listener, nil
}
