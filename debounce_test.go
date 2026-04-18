package kubernetes

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestWatcher_CoalescePattern(t *testing.T) {
	// This tests the coalesce/debounce logic pattern used in startInformer.
	// We avoid using a real K8s informer here to prevent reflector-related hangs in CI.

	k := &Kubernetes{
		logger: zap.NewNop(),
	}

	var syncCalls int32
	var mu sync.Mutex

	// Mock implementation of the logic in startInformer
	const coalesceDuration = 50 * time.Millisecond
	var coalesceTimer *time.Timer
	updatePending := false

	// We use a WaitGroup to know when the background work is done
	var wg sync.WaitGroup

	triggerUpdate := func() {
		k.cacheMu.Lock()
		defer k.cacheMu.Unlock()

		k.needsRebuild = true

		if updatePending {
			return
		}

		updatePending = true
		wg.Add(1)
		coalesceTimer = time.AfterFunc(coalesceDuration, func() {
			k.cacheMu.Lock()
			defer k.cacheMu.Unlock()

			updatePending = false

			// Simulate syncSnapshot
			atomic.AddInt32(&syncCalls, 1)
			k.needsRebuild = false

			wg.Done()
		})
	}

	// Trigger 10 updates rapidly
	for i := 0; i < 10; i++ {
		triggerUpdate()
		time.Sleep(2 * time.Millisecond)
	}

	// Wait for the background timer to complete
	wg.Wait()

	finalCalls := atomic.LoadInt32(&syncCalls)
	if finalCalls != 1 {
		t.Errorf("Expected exactly 1 sync call after rapid updates, got %d", finalCalls)
	}

	// Cleanup timer
	mu.Lock()
	if coalesceTimer != nil {
		coalesceTimer.Stop()
	}
	mu.Unlock()
}
