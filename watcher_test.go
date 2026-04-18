package kubernetes

import (
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
)

func TestGetUpstreams_ServiceFallback(t *testing.T) {
	k := &Kubernetes{
		MaxStaleness: caddy.Duration(time.Minute),
		logger:       zap.NewNop(),
	}
	fallback := []*reverseproxy.Upstream{{Dial: "fallback:80"}}
	k.fallbackUpstreams.Store(&fallback)
	k.provisionMetrics()

	// 1. Fresh state: should return normal upstreams
	freshSnap := &kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{{Dial: "normal:80"}},
		LastUpdated: time.Now(),
	}
	k.upstreams.Store(freshSnap)

	ups, err := k.GetUpstreams(nil)
	if err != nil || len(ups) != 1 || ups[0].Dial != "normal:80" {
		t.Errorf("Expected normal upstream, got %v", ups)
	}

	// 2. Stale state: should return fallback upstreams
	staleSnap := &kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{{Dial: "normal:80"}},
		LastUpdated: time.Now().Add(-2 * time.Minute),
	}
	k.upstreams.Store(staleSnap)

	ups, err = k.GetUpstreams(nil)
	if err != nil || len(ups) != 1 || ups[0].Dial != "fallback:80" {
		t.Errorf("Expected fallback upstream, got %v", ups)
	}

	// 3. Fallback disabled: should return stale normal upstreams
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

	// 1. Setup fallback upstreams (ClusterIP)
	fallback := []*reverseproxy.Upstream{{Dial: "10.0.0.10:80"}}
	k.fallbackUpstreams.Store(&fallback)

	// 2. Simulate discovery results with NO endpoints (e.g. port mismatch)
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{},
		LastUpdated: time.Now(),
	})

	// Behavior: returns fallback upstreams because fine-grained list is empty.
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
		MaxStaleness: 0, // Fallback disabled
		logger:       zap.NewNop(),
	}
	k.provisionMetrics()

	// 1. Setup fallback upstreams (ClusterIP)
	fallback := []*reverseproxy.Upstream{{Dial: "10.0.0.10:80"}}
	k.fallbackUpstreams.Store(&fallback)

	// 2. Simulate discovery results with NO endpoints
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{},
		LastUpdated: time.Now(),
	})

	// Behavior: returns 0 because fallback is disabled.
	upstreams, err := k.GetUpstreams(nil)
	if err != nil {
		t.Fatalf("GetUpstreams failed: %v", err)
	}

	if len(upstreams) != 0 {
		t.Errorf("Expected 0 upstreams when fallback is disabled, but got %d", len(upstreams))
	}
}
