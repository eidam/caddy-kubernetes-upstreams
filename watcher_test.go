package kubernetes

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWatcher_CoalescePattern(t *testing.T) {
	k := &Kubernetes{
		logger: zap.NewNop(),
	}

	var syncCalls int32
	var mu sync.Mutex

	const coalesceDuration = 50 * time.Millisecond
	var coalesceTimer *time.Timer
	updatePending := false
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
			atomic.AddInt32(&syncCalls, 1)
			k.needsRebuild = false
			wg.Done()
		})
	}

	for i := 0; i < 10; i++ {
		triggerUpdate()
		time.Sleep(2 * time.Millisecond)
	}

	wg.Wait()

	if atomic.LoadInt32(&syncCalls) != 1 {
		t.Errorf("Expected exactly 1 sync call after rapid updates, got %d", atomic.LoadInt32(&syncCalls))
	}

	mu.Lock()
	if coalesceTimer != nil {
		coalesceTimer.Stop()
	}
	mu.Unlock()
}

func TestLifecycle_GracefulShutdown(t *testing.T) {
	k := &Kubernetes{
		Namespace:    "default",
		Service:      "test-svc",
		PollInterval: caddy.Duration(100 * time.Millisecond),
		client:       fake.NewSimpleClientset(),
		logger:       zap.NewNop(),
		ready:        make(chan struct{}),
	}
	k.provisionMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	go k.run(ctx)

	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	cancel()

	done := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
		t.Logf("Shutdown completed in %v", time.Since(start))
	case <-time.After(2 * time.Second):
		t.Error("Shutdown took too long; goroutines might be hanging")
	}
}

func TestLifecycle_Cleanup(t *testing.T) {
	k := &Kubernetes{}
	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel
	k.ctx = ctx

	if err := k.Cleanup(); err != nil {
		t.Errorf("Cleanup failed: %v", err)
	}

	select {
	case <-k.ctx.Done():
		// Success
	default:
		t.Error("Cleanup did not cancel context")
	}
}

func TestGetUpstreams_ServiceFallback(t *testing.T) {
	k := &Kubernetes{
		MaxStaleness: caddy.Duration(time.Minute),
		logger:       zap.NewNop(),
	}
	k.provisionMetrics()
	fallback := []*reverseproxy.Upstream{{Dial: "fallback:80"}}
	k.fallbackUpstreams.Store(&fallback)

	freshSnap := &kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{{Dial: "normal:80"}},
		LastUpdated: time.Now(),
	}
	k.upstreams.Store(freshSnap)

	ups, err := k.GetUpstreams(nil)
	if err != nil || len(ups) != 1 || ups[0].Dial != "normal:80" {
		t.Errorf("Expected normal upstream, got %v", ups)
	}

	staleSnap := &kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{{Dial: "normal:80"}},
		LastUpdated: time.Now().Add(-2 * time.Minute),
	}
	k.upstreams.Store(staleSnap)

	ups, err = k.GetUpstreams(nil)
	if err != nil || len(ups) != 1 || ups[0].Dial != "fallback:80" {
		t.Errorf("Expected fallback upstream, got %v", ups)
	}

	k.MaxStaleness = 0
	ups, err = k.GetUpstreams(nil)
	if err != nil || len(ups) != 1 || ups[0].Dial != "normal:80" {
		t.Errorf("Expected stale normal upstream when fallback disabled, got %v", ups)
	}
}

func TestGetUpstreams_FallbackOnEmpty(t *testing.T) {
	k := &Kubernetes{
		Namespace:    "default",
		Service:      "test-svc",
		Port:         "80",
		MaxStaleness: caddy.Duration(1 * time.Minute),
		logger:       zap.NewNop(),
	}
	k.provisionMetrics()

	fallback := []*reverseproxy.Upstream{{Dial: "10.0.0.10:80"}}
	k.fallbackUpstreams.Store(&fallback)

	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{},
		LastUpdated: time.Now(),
	})

	upstreams, err := k.GetUpstreams(nil)
	if err != nil {
		t.Fatalf("GetUpstreams failed: %v", err)
	}

	if len(upstreams) == 0 {
		t.Errorf("Expected fallback upstreams when fine-grained list is empty, but got 0")
	} else if upstreams[0].Dial != "10.0.0.10:80" {
		t.Errorf("Expected fallback upstream 10.0.0.10:80, got %s", upstreams[0].Dial)
	}
}

func TestGetUpstreams_NoFallbackIfDisabled(t *testing.T) {
	k := &Kubernetes{
		Namespace:    "default",
		Service:      "test-svc",
		Port:         "80",
		MaxStaleness: 0,
		logger:       zap.NewNop(),
	}
	k.provisionMetrics()

	fallback := []*reverseproxy.Upstream{{Dial: "10.0.0.10:80"}}
	k.fallbackUpstreams.Store(&fallback)

	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{},
		LastUpdated: time.Now(),
	})

	upstreams, err := k.GetUpstreams(nil)
	if err != nil {
		t.Fatalf("GetUpstreams failed: %v", err)
	}

	if len(upstreams) != 0 {
		t.Errorf("Expected 0 upstreams when fallback is disabled, but got %d", len(upstreams))
	}
}
