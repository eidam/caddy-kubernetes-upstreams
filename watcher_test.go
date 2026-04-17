package kubernetes

import (
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
)

func TestGetUpstreams_ServiceFallback(t *testing.T) {
	k := &Kubernetes{
		MaxStaleness: caddy.Duration(time.Minute),
	}
	fallback := []*reverseproxy.Upstream{{Dial: "fallback:80"}}
	k.fallbackUpstreams.Store(&fallback)

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
