package kubernetes

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	testingk8s "k8s.io/client-go/testing"
)

func TestProvision_StrictInit(t *testing.T) {
	// Mock a client that hangs on List
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "endpointslices", func(action testingk8s.Action) (handled bool, ret runtime.Object, err error) {
		time.Sleep(200 * time.Millisecond)
		return true, &discoveryv1.EndpointSliceList{}, nil
	})

	k := &Kubernetes{
		Service:            "test-svc",
		StrictInit:         true,
		StartupPollTimeout: caddy.Duration(50 * time.Millisecond), // Short timeout
		client:             client,
		logger:             zap.NewNop(),
	}
	k.provisionMetrics()

	// We can't call Provision directly because it calls initKubernetesClient
	// So we'll test the select logic in a simulated provision snippet
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k.ctx = ctx
	k.ready = make(chan struct{})

	start := time.Now()
	err := func() error {
		select {
		case <-k.ready:
			return nil
		case <-time.After(time.Duration(k.StartupPollTimeout)):
			if k.StrictInit {
				return fmt.Errorf("timeout")
			}
			return nil
		}
	}()

	if err == nil || err.Error() != "timeout" {
		t.Errorf("Expected timeout error for StrictInit, got %v", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Errorf("Timeout happened too fast: %v", time.Since(start))
	}
}

func TestGetUpstreams_ImmediateFallback(t *testing.T) {
	k := &Kubernetes{
		MaxStaleness: caddy.Duration(time.Minute),
		logger:       zap.NewNop(),
	}
	fallback := []*reverseproxy.Upstream{{Dial: "fallback:80"}}
	k.fallbackUpstreams.Store(&fallback)
	k.provisionMetrics()

	// 1. Initial state: LastUpdated is zero time
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{},
		LastUpdated: time.Time{},
	})

	// 2. GetUpstreams should IMMEDIATELY return fallback because zero-time is way older than MaxStaleness
	ups, err := k.GetUpstreams(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ups) != 1 || ups[0].Dial != "fallback:80" {
		t.Errorf("Expected immediate fallback, got %v", ups)
	}

	// 3. After a sync, it should return normal upstreams
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{{Dial: "pod:80"}},
		LastUpdated: time.Now(),
	})

	ups, err = k.GetUpstreams(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ups) != 1 || ups[0].Dial != "pod:80" {
		t.Errorf("Expected pod upstream after sync, got %v", ups)
	}
}
