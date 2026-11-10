package cherrySnowflake

import (
	ccrypto "github.com/cherry-game/cherry/extend/crypto"
	clog "github.com/cherry-game/cherry/logger"
	"sync"
)

var (
	defaultNode   *Node
	defaultNodeMu sync.RWMutex
)

func InitDefaultNode(str string) {
	mu.Lock()
	maximum := nodeMax
	mu.Unlock()
	var (
		crc32Value = int64(ccrypto.CRC32(str))
		nodeValue  = crc32Value % maximum
	)

	SetDefaultNode(nodeValue)
}

func SetDefaultNode(nodeValue int64) {
	defaultNodeMu.Lock()
	if defaultNode != nil {
		defaultNodeMu.Unlock()
		clog.Warn("default snowflake node is created.")
		return
	}

	var err error
	defaultNode, err = NewNode(nodeValue)
	defaultNodeMu.Unlock()
	if err != nil {
		clog.Warn(err)
		clog.Warnf("create default snowflake node fail. nodeValue = %d", nodeValue)
	}

	mu.Lock()
	maximum := nodeMax
	mu.Unlock()
	clog.Infof("[snowflake] nodeValue = %d, nodeMax = %d", nodeValue, maximum)
}

func Next() ID {
	defaultNodeMu.RLock()
	node := defaultNode
	defaultNodeMu.RUnlock()
	return node.Generate()
}

func NextID() int64 {
	return Next().Int64()
}
