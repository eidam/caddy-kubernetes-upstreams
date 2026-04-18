package kubernetes

import (
	"fmt"
	"reflect"
	"testing"

	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestBuildUpstreams(t *testing.T) {
	k := &Kubernetes{
		Port:   "8080",
		logger: zap.NewNop(),
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
		Port:   "80",
		logger: zap.NewNop(),
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
		Port:   "443",
		logger: zap.NewNop(),
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
	ports := []discoveryv1.EndpointPort{
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
		name      string
		port      string
		wantPort  int32
		wantFound bool
	}{
		{
			name:      "by name http",
			port:      "http",
			wantPort:  80,
			wantFound: true,
		},
		{
			name:      "by name https",
			port:      "https",
			wantPort:  443,
			wantFound: true,
		},
		{
			name:      "by number 80",
			port:      "80",
			wantPort:  80,
			wantFound: true,
		},
		{
			name:      "by number 443",
			port:      "443",
			wantPort:  443,
			wantFound: true,
		},
		{
			name:      "name not found",
			port:      "foo",
			wantFound: false,
		},
		{
			name:      "number not found",
			port:      "8080",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Kubernetes{
				Port: tt.port,
			}
			gotPort, gotFound := k.resolvePort(ports)
			if gotPort != tt.wantPort || gotFound != tt.wantFound {
				t.Errorf("resolvePort() = (%v, %v), want (%v, %v)", gotPort, gotFound, tt.wantPort, tt.wantFound)
			}
		})
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
