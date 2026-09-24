package cherryConnector

import (
	"errors"
	"net"
	"time"

	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
)

type (
	TCPConnector struct {
		cfacade.Component
		Connector
		Options
	}
)

func (*TCPConnector) Name() string {
	return "tcp_connector"
}

func (t *TCPConnector) OnAfterInit() {
}

func (t *TCPConnector) OnStop() {
	t.Stop()
}

func NewTCP(address string, opts ...Option) *TCPConnector {
	if address == "" {
		clog.Warn("Create tcp connector fail. Address is null.")
		return nil
	}

	tcp := &TCPConnector{
		Options: Options{
			address:  address,
			certFile: "",
			keyFile:  "",
			chanSize: 256,
		},
	}

	for _, opt := range opts {
		opt(&tcp.Options)
	}

	tcp.Connector = NewConnector(tcp.chanSize)

	return tcp
}

func (t *TCPConnector) Start() {
	listener, err := t.GetListener(t.certFile, t.keyFile, t.address)
	if err != nil {
		clog.Fatalf("failed to listen: %s", err)
	}

	clog.Infof("Tcp connector listening at Address %s", t.address)
	if t.certFile != "" || t.keyFile != "" {
		clog.Infof("certFile = %s, keyFile = %s", t.certFile, t.keyFile)
	}

	t.Connector.Start()

	var backoff time.Duration
	for t.Running() {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || !t.Running() {
				return
			}
			// Back off on transient errors (e.g. EMFILE) instead of spinning.
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else if backoff < time.Second {
				backoff *= 2
			}
			clog.Errorf("Failed to accept TCP connection: %s. retry in %v", err.Error(), backoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0

		t.InChan(conn)
	}
}

func (t *TCPConnector) Stop() {
	t.Connector.Stop()
}
