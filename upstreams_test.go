package kubernetes

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestBuildUpstreams(t *testing.T) {
	k := &Kubernetes{
		Port:            "8080",
		resolvedPodPort: 8080,
		logger:          zap.NewNop(),
	}

	slices := []discoveryv1.EndpointSlice{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "slice1"},
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses: []string{"10.0.0.1"},
					Conditions: discoveryv1.EndpointConditions{
						Ready:   ptr.To(true),
						Serving: ptr.To(true),
					},
				},
				{
					Addresses: []string{"10.0.0.2"},
					Conditions: discoveryv1.EndpointConditions{
						Ready:   ptr.To(false),
						Serving: ptr.To(false),
					},
				},
				{
					Addresses: []string{"10.0.0.3"},
					Conditions: discoveryv1.EndpointConditions{
						Ready:       ptr.To(false),
						Serving:     ptr.To(true),
						Terminating: ptr.To(true),
					},
				},
				{
					Addresses: []string{"10.0.0.4"},
					Conditions: discoveryv1.EndpointConditions{
						Ready: ptr.To(true), // Serving nil, fallback to Ready
					},
				},
			},
			Ports: []discoveryv1.EndpointPort{
				{
					Port: ptr.To(int32(8080)),
				},
			},
		},
	}

	tests := []struct {
		name             string
		allowUnready     bool
		allowTerminating bool
		want             []string
	}{
		{
			name:             "default (ready only, no terminating)",
			allowUnready:     false,
			allowTerminating: false,
			want:             []string{"10.0.0.1:8080", "10.0.0.4:8080"},
		},
		{
			name:             "allow unready",
			allowUnready:     true,
			allowTerminating: false,
			want:             []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.4:8080"},
		},
		{
			name:             "allow terminating (includes 10.0.0.3 which is serving and terminating)",
			allowUnready:     false,
			allowTerminating: true,
			want:             []string{"10.0.0.1:8080", "10.0.0.3:8080", "10.0.0.4:8080"},
		},
		{
			name:             "allow all",
			allowUnready:     true,
			allowTerminating: true,
			want:             []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080", "10.0.0.4:8080"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k.AllowUnready = tt.allowUnready
			k.AllowTerminating = tt.allowTerminating
			upstreams := k.buildUpstreams(slices)

			var got []string
			for _, u := range upstreams {
				got = append(got, u.Dial)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildUpstreams() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildUpstreams_PointerStability(t *testing.T) {
	k := &Kubernetes{
		Port:            "80",
		resolvedPodPort: 80,
		logger:          zap.NewNop(),
	}

	slices := []discoveryv1.EndpointSlice{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "slice-1"},
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses:  []string{"10.0.0.1"},
					Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				},
			},
			Ports: []discoveryv1.EndpointPort{{Port: ptr.To(int32(80))}},
		},
	}

	// 1. Initial build
	ups1 := k.buildUpstreams(slices)
	if len(ups1) != 1 {
		t.Fatalf("Expected 1 upstream, got %d", len(ups1))
	}
	ptr1 := ups1[0]

	// 2. Second build with same data
	ups2 := k.buildUpstreams(slices)
	if len(ups2) != 1 {
		t.Fatalf("Expected 1 upstream, got %d", len(ups2))
	}
	ptr2 := ups2[0]

	// Pointers must be identical to preserve Caddy LB state
	if ptr1 != ptr2 {
		t.Errorf("Pointer changed for same address! %p != %p", ptr1, ptr2)
	}

	// 3. Third build with DIFFERENT data
	slices2 := []discoveryv1.EndpointSlice{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "slice-1"},
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses:  []string{"10.0.0.2"},
					Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				},
			},
			Ports: []discoveryv1.EndpointPort{{Port: ptr.To(int32(80))}},
		},
	}
	ups3 := k.buildUpstreams(slices2)
	ptr3 := ups3[0]

	if ptr1 == ptr3 {
		t.Errorf("Pointer reused for DIFFERENT address! %p == %p", ptr1, ptr3)
	}

	// 4. Verify pruning
	if _, ok := k.upstreamPool["10.0.0.1:80"]; ok {
		t.Error("Old address not pruned from upstreamPool")
	}
}

func TestBuildUpstreams_IPv6(t *testing.T) {
	k := &Kubernetes{
		Port:            "443",
		resolvedPodPort: 443,
		logger:          zap.NewNop(),
	}

	slices := []discoveryv1.EndpointSlice{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "slice-ipv6"},
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses:  []string{"2001:db8::1"},
					Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				},
			},
			Ports: []discoveryv1.EndpointPort{{Port: ptr.To(int32(443))}},
		},
	}

	ups := k.buildUpstreams(slices)
	if len(ups) != 1 {
		t.Fatalf("Expected 1 upstream, got %d", len(ups))
	}
	// Must have brackets for IPv6
	want := "[2001:db8::1]:443"
	if ups[0].Dial != want {
		t.Errorf("IPv6 Dial = %v, want %v", ups[0].Dial, want)
	}
}

func TestResolvePort(t *testing.T) {
	defaultPorts := []discoveryv1.EndpointPort{
		{
			Name: ptr.To("http"),
			Port: ptr.To(int32(80)),
		},
		{
			Name: ptr.To("https"),
			Port: ptr.To(int32(443)),
		},
	}

	tests := []struct {
		name             string
		port             string
		resolvedPortName string
		resolvedPodPort  int32
		ports            []discoveryv1.EndpointPort
		wantPort         int32
		wantFound        bool
	}{
		{
			name:             "by name http",
			port:             "http",
			resolvedPortName: "http",
			ports:            defaultPorts,
			wantPort:         80,
			wantFound:        true,
		},
		{
			name:             "by name https",
			port:             "https",
			resolvedPortName: "https",
			ports:            defaultPorts,
			wantPort:         443,
			wantFound:        true,
		},
		{
			name:            "by number 80",
			port:            "80",
			resolvedPodPort: 80,
			ports:           defaultPorts,
			wantPort:        80,
			wantFound:       true,
		},
		{
			name:            "by number 443",
			port:            "443",
			resolvedPodPort: 443,
			ports:           defaultPorts,
			wantPort:        443,
			wantFound:       true,
		},
		{
			name:      "name not found",
			port:      "foo",
			ports:     defaultPorts,
			wantFound: false,
		},
		{
			name:      "number not found",
			port:      "8080",
			ports:     defaultPorts,
			wantFound: false,
		},
		{
			name:             "targetPort mapping: requested 80, resolved to http, matches slice port 8080",
			port:             "80",
			resolvedPortName: "http",
			ports: []discoveryv1.EndpointPort{
				{
					Name: ptr.To("http"),
					Port: ptr.To(int32(8080)),
				},
			},
			wantPort:  8080,
			wantFound: true,
		},
		{
			name:            "unnamed port match: requested 80, no name in service, matches slice port 80",
			port:            "80",
			resolvedPodPort: 80,
			ports: []discoveryv1.EndpointPort{
				{
					Port: ptr.To(int32(80)),
				},
			},
			wantPort:  80,
			wantFound: true,
		},
		{
			name:             "name mismatch: requested 80, resolved to http, slice has only https",
			port:             "80",
			resolvedPortName: "http",
			ports: []discoveryv1.EndpointPort{
				{
					Name: ptr.To("https"),
					Port: ptr.To(int32(443)),
				},
			},
			wantFound: false,
		},
		{
			name:             "direct name match in config: requested 'http', matches slice port 80",
			port:             "http",
			resolvedPortName: "http",
			ports: []discoveryv1.EndpointPort{
				{
					Name: ptr.To("http"),
					Port: ptr.To(int32(80)),
				},
			},
			wantPort:  80,
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Kubernetes{
				Port:             tt.port,
				resolvedPortName: tt.resolvedPortName,
				resolvedPodPort:  tt.resolvedPodPort,
			}
			gotPort, gotFound := k.resolvePort(tt.ports)
			if gotPort != tt.wantPort || gotFound != tt.wantFound {
				t.Errorf("resolvePort() = (%v, %v), want (%v, %v)", gotPort, gotFound, tt.wantPort, tt.wantFound)
			}
		})
	}
}

func TestComprehensivePortResolution(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		requestedPort string
		service       *corev1.Service
		slicePorts    []discoveryv1.EndpointPort
		wantPort      int32
		wantFound     bool
	}{
		{
			name:          "Scenario 1: Simple Numeric Match (No Names)",
			requestedPort: "80",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Port: 80}},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Port: ptr.To(int32(80))},
			},
			wantPort:  80,
			wantFound: true,
		},
		{
			name:          "Scenario 2: TargetPort Numeric Mapping",
			requestedPort: "80",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Port: ptr.To(int32(8080))},
			},
			wantPort:  8080,
			wantFound: true,
		},
		{
			name:          "Scenario 3: TargetPort Name Mapping (Flux Case)",
			requestedPort: "80",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "webhook-receiver", Namespace: "flux-system"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       80,
							TargetPort: intstr.FromString("http-webhook"),
						},
					},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{
					Name: ptr.To("http"), // Matches Service Port name
					Port: ptr.To(int32(9292)),
				},
			},
			wantPort:  9292,
			wantFound: true,
		},
		{
			name:          "Scenario 4: Named Port Request",
			requestedPort: "https",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "http", Port: 80},
						{Name: "https", Port: 443},
					},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Name: ptr.To("http"), Port: ptr.To(int32(80))},
				{Name: ptr.To("https"), Port: ptr.To(int32(443))},
			},
			wantPort:  443,
			wantFound: true,
		},
		{
			name:          "Scenario 5: Multi-port Auto-selection (Requested Port Empty)",
			requestedPort: "",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "metrics", Port: 9090},
					},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Name: ptr.To("metrics"), Port: ptr.To(int32(9090))},
			},
			wantPort:  9090,
			wantFound: true,
		},
		{
			name:          "Scenario 6: Multi-port Selection by Number",
			requestedPort: "443",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "http", Port: 80},
						{Name: "https", Port: 443},
					},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Name: ptr.To("http"), Port: ptr.To(int32(80))},
				{Name: ptr.To("https"), Port: ptr.To(int32(443))},
			},
			wantPort:  443,
			wantFound: true,
		},
		{
			name:          "Scenario 7: Mismatch - Port in Service but not in Slice",
			requestedPort: "80",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromString("web")}},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Name: ptr.To("wrong"), Port: ptr.To(int32(8080))},
			},
			wantFound: false,
		},
		{
			name:          "Scenario 8: Headless Service Mapping",
			requestedPort: "80",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "headless", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					ClusterIP: "None",
					Ports:     []corev1.ServicePort{{Name: "web", Port: 80}},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Name: ptr.To("web"), Port: ptr.To(int32(8080))},
			},
			wantPort:  8080,
			wantFound: true,
		},
		{
			name:          "Scenario 9: Service Port Number matches Slice Port Number but Names Differ",
			requestedPort: "80",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{{Name: "http", Port: 80}},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Name: ptr.To("legacy"), Port: ptr.To(int32(80))},
			},
			wantPort:  80,
			wantFound: false, // Strict: names must match if ServicePort has a name
		},
		{
			name:          "Scenario 10: Mixed TargetPort Types in Service",
			requestedPort: "8080",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "web", Port: 80, TargetPort: intstr.FromString("http")},
						{Name: "admin", Port: 8080, TargetPort: intstr.FromInt32(9090)},
					},
				},
			},
			slicePorts: []discoveryv1.EndpointPort{
				{Name: ptr.To("admin"), Port: ptr.To(int32(9090))},
			},
			wantPort:  9090,
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(tt.service)
			k := &Kubernetes{
				Namespace: tt.service.Namespace,
				Service:   tt.service.Name,
				Port:      tt.requestedPort,
				client:    client,
				logger:    zap.NewNop(),
			}

			k.updateFallbackUpstreams(ctx)

			gotPort, gotFound := k.resolvePort(tt.slicePorts)
			if gotFound != tt.wantFound {
				t.Errorf("resolvePort() found = %v, want %v", gotFound, tt.wantFound)
			}
			if tt.wantFound && gotPort != tt.wantPort {
				t.Errorf("resolvePort() port = %v, want %v", gotPort, tt.wantPort)
			}
		})
	}
}

func TestUpdateFallbackUpstreams(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		namespace   string
		service     string
		port        string
		existingSvc *corev1.Service
		wantAddr    string
	}{
		{
			name:      "API success with ClusterIP",
			namespace: "default",
			service:   "my-svc",
			port:      "80",
			existingSvc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					ClusterIP: "10.96.0.10",
					Ports:     []corev1.ServicePort{{Port: 80}},
				},
			},
			wantAddr: "10.96.0.10:80",
		},
		{
			name:      "API success with named port",
			namespace: "default",
			service:   "my-svc",
			port:      "http",
			existingSvc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					ClusterIP: "10.96.0.10",
					Ports:     []corev1.ServicePort{{Name: "http", Port: 8080}},
				},
			},
			wantAddr: "10.96.0.10:8080",
		},
		{
			name:      "API failure (missing service) - fallback to DNS",
			namespace: "other-ns",
			service:   "my-svc",
			port:      "443",
			wantAddr:  "my-svc.other-ns.svc:443",
		},
		{
			name:      "Headless service - fallback to DNS",
			namespace: "default",
			service:   "headless",
			port:      "80",
			existingSvc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "headless", Namespace: "default"},
				Spec: corev1.ServiceSpec{
					ClusterIP: "None",
					Ports:     []corev1.ServicePort{{Port: 80}},
				},
			},
			wantAddr: "headless.default.svc:80",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var client *fake.Clientset
			if tt.existingSvc != nil {
				client = fake.NewSimpleClientset(tt.existingSvc)
			} else {
				client = fake.NewSimpleClientset()
			}

			k := &Kubernetes{
				Namespace: tt.namespace,
				Service:   tt.service,
				Port:      tt.port,
				client:    client,
				logger:    zap.NewNop(),
			}
			k.provisionMetrics()
			k.updateFallbackUpstreams(ctx)

			fallbackPtr := k.fallbackUpstreams.Load()
			if fallbackPtr == nil {
				t.Fatalf("Expected fallback upstreams to be populated")
			}
			fallback := *fallbackPtr

			if len(fallback) != 1 {
				t.Fatalf("Expected 1 fallback upstream, got %d", len(fallback))
			}
			if fallback[0].Dial != tt.wantAddr {
				t.Errorf("Fallback Dial = %v, want %v", fallback[0].Dial, tt.wantAddr)
			}
		})
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

func TestRebuildOnMappingChange(t *testing.T) {
	ctx := context.Background()

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "v1", Port: 80}},
		},
	}
	client := fake.NewSimpleClientset(svc)

	rebuildCalled := 0
	k := &Kubernetes{
		Namespace: "default",
		Service:   "svc",
		Port:      "80",
		client:    client,
		logger:    zap.NewNop(),
		triggerUpdate: func(rebuild bool) {
			if rebuild {
				rebuildCalled++
			}
		},
	}

	// First run - populates initial mapping
	k.updateFallbackUpstreams(ctx)
	initialRebuilds := rebuildCalled

	// Update service name in fake client
	svc.Spec.Ports[0].Name = "v2"
	client.CoreV1().Services("default").Update(ctx, svc, metav1.UpdateOptions{})

	// Second run - should detect change and trigger rebuild
	k.updateFallbackUpstreams(ctx)
	if rebuildCalled <= initialRebuilds {
		t.Errorf("Expected rebuild on mapping change, got %d (initial: %d)", rebuildCalled, initialRebuilds)
	}
	afterChangeRebuilds := rebuildCalled

	// Third run - no change, no rebuild
	k.updateFallbackUpstreams(ctx)
	if rebuildCalled != afterChangeRebuilds {
		t.Errorf("Expected no rebuild when mapping is identical, got %d (previous: %d)", rebuildCalled, afterChangeRebuilds)
	}
}

func BenchmarkBuildUpstreams(b *testing.B) {
	k := &Kubernetes{
		Service: "test",
		logger:  zap.NewNop(),
	}

	// Create 100 slices with 10 endpoints each
	var slices []discoveryv1.EndpointSlice
	for i := 0; i < 100; i++ {
		slice := discoveryv1.EndpointSlice{
			Ports: []discoveryv1.EndpointPort{
				{Port: ptr.To(int32(80))},
			},
		}
		for j := 0; j < 10; j++ {
			slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
				Addresses: []string{fmt.Sprintf("10.0.%d.%d", i, j)},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(true),
				},
			})
		}
		slices = append(slices, slice)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = k.buildUpstreams(slices)
	}
}
