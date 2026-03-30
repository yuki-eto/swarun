package atomicmap

import (
	"sync"
	"testing"
	"time"
)

func TestMap_GetOrCompute_Concurrency(t *testing.T) {
	m := New[string, int]()
	var callCount int
	var mu sync.Mutex

	compute := func() int {
		mu.Lock()
		callCount++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond) // 副作用をシミュレート
		return 1
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.GetOrCompute("key", compute)
		}()
	}
	wg.Wait()

	if callCount != 1 {
		t.Errorf("expected compute to be called exactly once, got %d", callCount)
	}
}
