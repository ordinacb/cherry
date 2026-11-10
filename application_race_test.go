package cherry

import (
	"sync"
	"sync/atomic"
	"testing"
)

// 启停状态使用与 Startup 相同的原子写，后台服务必须能同时安全读取。
func TestApplicationRunningConcurrentTransitions(t *testing.T) {
	a := &Application{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			atomic.StoreInt32(&a.running, int32(i%2))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			_ = a.Running()
		}
	}()
	wg.Wait()
	atomic.StoreInt32(&a.running, 1)
	if !a.Running() {
		t.Fatal("已运行状态没有发布")
	}
	atomic.StoreInt32(&a.running, 0)
	if a.Running() {
		t.Fatal("停止状态没有发布")
	}
}
