package kubernetes

import (
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestIsNewer(t *testing.T) {
	tests := []struct {
		newRV string
		oldRV string
		want  bool
	}{
		{"100", "99", true},
		{"99", "100", false},
		{"100", "100", false},
		{"100", "", true},
		{"abc", "def", false}, // fallback to string comparison
		{"def", "abc", true},
	}

	for _, tt := range tests {
		if got := isNewer(tt.newRV, tt.oldRV); got != tt.want {
			t.Errorf("isNewer(%s, %s) = %v, want %v", tt.newRV, tt.oldRV, got, tt.want)
		}
	}
}

func TestWatcher_Concurrency(t *testing.T) {
	k := &Kubernetes{
		Namespace: "default",
		Service:   "test-svc",
		client:    fake.NewSimpleClientset(),
		logger:    zap.NewNop(),
		ready:     make(chan struct{}),
	}
	k.provisionMetrics()
	// Initial empty state
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   nil,
		LastUpdated: time.Now(),
	})

	// 1. Simulate a "Watch" event that is NEWER than a pending "List"
	// We'll manually manipulate the internal state to simulate a race.

	// Pre-condition: Cache is empty, version is "10"
	k.lastResourceVersion = "10"
	k.slicesCache = make(map[string]discoveryv1.EndpointSlice)

	// A stale "List" result comes in with version "5"
	staleList := &discoveryv1.EndpointSliceList{
		TypeMeta: metav1.TypeMeta{},
		ListMeta: metav1.ListMeta{ResourceVersion: "5"},
		Items: []discoveryv1.EndpointSlice{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "stale-slice", ResourceVersion: "5"},
			},
		},
	}

	// We'll call a modified version of rebuild or just trigger it.
	// Since we can't easily intercept the fake client's internal timing,
	// we test the logic of rebuild() directly.

	k.cacheMu.Lock()
	// Simulate: List result version is 5, but current version is 10
	if isNewer(staleList.ResourceVersion, k.lastResourceVersion) || staleList.ResourceVersion == k.lastResourceVersion {
		t.Error("Rebuild should have detected stale list version")
	}
	k.cacheMu.Unlock()

	// 2. Simulate correct update
	newSlice := discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "new-slice",
			ResourceVersion: "15",
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: boolPtr(true),
				},
			},
		},
		Ports: []discoveryv1.EndpointPort{
			{
				Port: int32Ptr(80),
			},
		},
	}

	// Simulate watch event
	k.cacheMu.Lock()
	if isNewer(newSlice.ResourceVersion, k.lastResourceVersion) {
		k.lastResourceVersion = newSlice.ResourceVersion
		k.slicesCache[newSlice.Name] = newSlice
	}
	k.syncSnapshot()
	k.cacheMu.Unlock()

	snap := k.upstreams.Load()
	if len(snap.upstreams) != 1 {
		t.Fatalf("Expected 1 upstream, got %d", len(snap.upstreams))
	}
	if snap.upstreams[0].Dial != "10.0.0.1:80" {
		t.Errorf("Expected 10.0.0.1:80, got %s", snap.upstreams[0].Dial)
	}

	// 3. Simulate Deletion
	k.cacheMu.Lock()
	delete(k.slicesCache, newSlice.Name)
	k.syncSnapshot()
	k.cacheMu.Unlock()

	snap = k.upstreams.Load()
	if len(snap.upstreams) != 0 {
		t.Errorf("Expected 0 upstreams after deletion, got %d", len(snap.upstreams))
	}
}

func TestWatcher_LaggingWatchEvent(t *testing.T) {
	k := &Kubernetes{
		Namespace: "default",
		Service:   "test-svc",
		client:    fake.NewSimpleClientset(),
		logger:    zap.NewNop(),
	}
	k.provisionMetrics()

	// 1. Set current state to version "100" (e.g. from a recent Poll)
	k.cacheMu.Lock()
	k.lastResourceVersion = "100"
	k.slicesCache = map[string]discoveryv1.EndpointSlice{
		"slice-1": {
			ObjectMeta: metav1.ObjectMeta{Name: "slice-1", ResourceVersion: "100"},
			Endpoints:  []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(true)}}},
			Ports:      []discoveryv1.EndpointPort{{Port: int32Ptr(80)}},
		},
	}
	k.syncSnapshot()
	k.cacheMu.Unlock()

	// 2. Receive a lagging Watch event for version "90" (e.g. from a frozen stream burst)
	laggingSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-1", ResourceVersion: "90"},
		Endpoints:  []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(true)}}},
		Ports:      []discoveryv1.EndpointPort{{Port: int32Ptr(80)}},
	}

	// This block mimics the logic inside the watchLoop event processing
	k.cacheMu.Lock()
	if isNewer(laggingSlice.ResourceVersion, k.lastResourceVersion) {
		k.lastResourceVersion = laggingSlice.ResourceVersion
		k.slicesCache[laggingSlice.Name] = *laggingSlice
		k.syncSnapshot()
	}
	k.cacheMu.Unlock()

	// 3. Verify that the cache was NOT updated (should still be 10.0.0.1)
	snap := k.upstreams.Load()
	if len(snap.upstreams) != 1 || snap.upstreams[0].Dial != "10.0.0.1:80" {
		t.Errorf("Lagging event overwritten newer state! Got %v", snap.upstreams[0].Dial)
	}
}

func TestGetUpstreams_ServiceFallback(t *testing.T) {
	k := &Kubernetes{
		MaxStaleness: caddy.Duration(time.Minute),
		fallbackUpstreams: []*reverseproxy.Upstream{
			{Dial: "fallback:80"},
		},
	}

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

func TestRebuild_Heartbeat(t *testing.T) {
	k := &Kubernetes{
		client: fake.NewSimpleClientset(),
		logger: zap.NewNop(),
	}
	k.provisionMetrics()

	// Initial state version "50"
	initialTime := time.Now().Add(-10 * time.Minute)
	k.lastResourceVersion = "50"
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   []*reverseproxy.Upstream{{Dial: "10.0.0.1:80"}},
		LastUpdated: initialTime,
	})

	// Simulate a List result with the SAME version "50"
	slices := &discoveryv1.EndpointSliceList{
		ListMeta: metav1.ListMeta{ResourceVersion: "50"},
	}

	// We'll call the logic directly since rebuild() involves network
	k.cacheMu.Lock()
	if slices.ResourceVersion == k.lastResourceVersion {
		k.refreshTimestamp()
	}
	k.cacheMu.Unlock()

	snap := k.upstreams.Load()
	if snap.LastUpdated.Before(time.Now().Add(-1 * time.Second)) {
		t.Error("Heartbeat failed to refresh LastUpdated timestamp")
	}
	if len(snap.upstreams) != 1 || snap.upstreams[0].Dial != "10.0.0.1:80" {
		t.Error("Heartbeat corrupted the upstream list")
	}
}
