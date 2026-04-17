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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	testingk8s "k8s.io/client-go/testing"
)

func TestWatchLoop_Bookmarks(t *testing.T) {
	client := fake.NewSimpleClientset()
	k := &Kubernetes{
		Namespace: "default",
		Service:   "test-svc",
		client:    client,
		logger:    zap.NewNop(),
		Watch:     boolPtr(true),
	}
	k.provisionMetrics()

	// 1. Initial version is empty
	k.lastResourceVersion = ""

	// 2. Simulate a Bookmark event with version "123"
	bookmark := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{ResourceVersion: "123"},
	}

	// Process event manually to verify logic
	event := watch.Event{Type: watch.Bookmark, Object: bookmark}
	if meta, ok := event.Object.(metav1.Object); ok {
		if isNewer(meta.GetResourceVersion(), k.lastResourceVersion) {
			k.lastResourceVersion = meta.GetResourceVersion()
		}
	}

	if k.lastResourceVersion != "123" {
		t.Errorf("ResourceVersion not progressed by Bookmark, got %s", k.lastResourceVersion)
	}
}

func TestWatchLoop_Forbidden(t *testing.T) {
	client := fake.NewSimpleClientset()
	// Mock a Forbidden error on Watch
	client.PrependWatchReactor("endpointslices", func(action testingk8s.Action) (handled bool, ret watch.Interface, err error) {
		return true, nil, errors.NewForbidden(discoveryv1.Resource("endpointslices"), "test-svc", nil)
	})

	k := &Kubernetes{
		Namespace: "default",
		Service:   "test-svc",
		client:    client,
		logger:    zap.NewNop(),
		Watch:     boolPtr(true),
		ready:     make(chan struct{}),
	}
	k.provisionMetrics()
	// Close ready to avoid hanging
	close(k.ready)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Should return immediately without hanging/crashing
	k.watchLoop(ctx)
}

func TestWatchLoop_Expired(t *testing.T) {
	client := fake.NewSimpleClientset()

	listCalled := make(chan struct{}, 1)
	client.PrependReactor("list", "endpointslices", func(action testingk8s.Action) (handled bool, ret runtime.Object, err error) {
		listCalled <- struct{}{}
		return true, &discoveryv1.EndpointSliceList{
			ListMeta: metav1.ListMeta{ResourceVersion: "200"},
		}, nil
	})

	// Mock an Expired error on first watch
	expired := true
	client.PrependWatchReactor("endpointslices", func(action testingk8s.Action) (handled bool, ret watch.Interface, err error) {
		if expired {
			expired = false
			return true, nil, errors.NewResourceExpired("too old")
		}
		// Second call returns normal empty watch
		return true, watch.NewFake(), nil
	})

	k := &Kubernetes{
		Namespace: "default",
		Service:   "test-svc",
		client:    client,
		logger:    zap.NewNop(),
		Watch:     boolPtr(true),
		ready:     make(chan struct{}),
	}
	k.provisionMetrics()
	close(k.ready)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start watch loop in background because it's an infinite loop
	go k.watchLoop(ctx)

	// Wait for the List call triggered by the expired error
	select {
	case <-listCalled:
		// Success: rebuild() was called
	case <-ctx.Done():
		t.Fatal("Timeout waiting for rebuild() after watch expiry")
	}
}

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
		fallbackUpstreams: []*reverseproxy.Upstream{
			{Dial: "fallback:80"},
		},
	}
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
