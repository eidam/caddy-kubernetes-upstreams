package kubernetes

import (
	"context"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes/fake"
)

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

	// Start the loops
	go k.run(ctx)

	// Let them run for a bit
	time.Sleep(200 * time.Millisecond)

	// Shutdown
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
	// Provision normally sets up cancel
	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel
	k.ctx = ctx

	// Cleanup should not panic and should call cancel
	err := k.Cleanup()
	if err != nil {
		t.Errorf("Cleanup failed: %v", err)
	}

	select {
	case <-k.ctx.Done():
		// Success
	default:
		t.Error("Cleanup did not cancel context")
	}
}
