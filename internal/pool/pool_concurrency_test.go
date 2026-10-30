package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codearts2api/internal/auth"
)

// 并发锁必须真的把在途请求数限制在 maxConcurrent 以内，并在结束后归零。
// 线上曾因「释放了并未持有的槽位」把计数减到别的请求头上，导致上限形同虚设。
func TestActiveConcurrentNeverExceedsMax(t *testing.T) {
	const max = 2
	a := auth.New("acct", "n", "d", "tok", "ak", "sk", time.Now().Add(time.Hour).Format(time.RFC3339), "", "")
	p, err := New([]*auth.Auth{a}, Config{MaxConcurrent: max}, "")
	if err != nil {
		t.Fatal(err)
	}

	var inFlight, peak int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !p.AcquireLockWait("acct", 10*time.Second) {
				t.Errorf("等待槽位超时")
				return
			}
			n := atomic.AddInt32(&inFlight, 1)
			for {
				old := atomic.LoadInt32(&peak)
				if n <= old || atomic.CompareAndSwapInt32(&peak, old, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt32(&inFlight, -1)
			p.ReleaseLock("acct")
		}()
	}
	wg.Wait()

	if peak > max {
		t.Fatalf("并发峰值 %d 超过上限 %d", peak, max)
	}
	if peak < 2 {
		t.Fatalf("并发峰值只有 %d，锁把请求完全串行化了", peak)
	}
	if p.Busy("acct") {
		t.Error("全部释放后 active_concurrent 应归零")
	}
}
