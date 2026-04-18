package kubernetes

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestUnmarshalCaddyfile(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		wantService      string
		wantNamespace    string
		wantPort         string
		wantMaxStaleness caddy.Duration
		wantErr          bool
	}{
		{
			name:        "simple service",
			input:       `dynamic kubernetes my-service`,
			wantService: "my-service",
		},
		{
			name:        "service and port shortcut",
			input:       `dynamic kubernetes my-service:8080`,
			wantService: "my-service",
			wantPort:    "8080",
		},
		{
			name:          "namespace and service shortcut",
			input:         `dynamic kubernetes my-ns/my-service`,
			wantNamespace: "my-ns",
			wantService:   "my-service",
		},
		{
			name:        "service and named port shortcut",
			input:       `dynamic kubernetes my-service:http`,
			wantService: "my-service",
			wantPort:    "http",
		},
		{
			name:          "namespace, service and port shortcut",
			input:         `dynamic kubernetes my-ns/my-service:8080`,
			wantNamespace: "my-ns",
			wantService:   "my-service",
			wantPort:      "8080",
		},
		{
			name:          "namespace, service and named port shortcut",
			input:         `dynamic kubernetes my-ns/my-service:http`,
			wantNamespace: "my-ns",
			wantService:   "my-service",
			wantPort:      "http",
		},
		{
			name: "full block",
			input: `dynamic kubernetes my-service {
				namespace other-ns
				port 8080
				allow_unready true
				allow_terminating true
				max_staleness 2m
			}`,
			wantService:      "my-service",
			wantNamespace:    "other-ns",
			wantPort:         "8080",
			wantMaxStaleness: caddy.Duration(2 * time.Minute),
		},
		{
			name: "full block with named port",
			input: `dynamic kubernetes my-service {
				port http
			}`,
			wantService: "my-service",
			wantPort:    "http",
		},
		{
			name:    "invalid shortcut (too many slashes)",
			input:   `dynamic kubernetes ns/svc/extra`,
			wantErr: true,
		},
		{
			name:    "invalid shortcut (too many colons)",
			input:   `dynamic kubernetes svc:80:extra`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := new(Kubernetes)
			d := caddyfile.NewTestDispenser(tt.input)
			err := k.UnmarshalCaddyfile(d)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalCaddyfile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if k.Service != tt.wantService {
				t.Errorf("Service = %v, want %v", k.Service, tt.wantService)
			}
			if k.Namespace != tt.wantNamespace {
				t.Errorf("Namespace = %v, want %v", k.Namespace, tt.wantNamespace)
			}
			if tt.wantPort != "" && k.Port != tt.wantPort {
				t.Errorf("Port = %v, want %v", k.Port, tt.wantPort)
			}
			if k.MaxStaleness != tt.wantMaxStaleness {
				t.Errorf("MaxStaleness = %v, want %v", k.MaxStaleness, tt.wantMaxStaleness)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		k       *Kubernetes
		wantErr bool
	}{
		{
			name: "valid",
			k: &Kubernetes{
				Service: "foo",
			},
			wantErr: false,
		},
		{
			name: "missing service",
			k: &Kubernetes{
				Namespace: "default",
			},
			wantErr: true,
		},
		{
			name: "invalid max_staleness (too low)",
			k: &Kubernetes{
				Service:      "foo",
				PollInterval: caddy.Duration(15 * time.Second),
				MaxStaleness: caddy.Duration(10 * time.Second),
			},
			wantErr: true,
		},
		{
			name: "valid max_staleness",
			k: &Kubernetes{
				Service:      "foo",
				PollInterval: caddy.Duration(15 * time.Second),
				MaxStaleness: caddy.Duration(30 * time.Second),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.k.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMetrics_Values(t *testing.T) {
	k := &Kubernetes{
		Namespace: "default",
		Service:   "test-svc",
		client:    fake.NewSimpleClientset(),
		logger:    zap.NewNop(),
	}
	k.provisionMetrics()

	// 1. Verify initial endpoint count is 0
	val := getGaugeValue(k.metricEndpoints)
	if val != 0 {
		t.Errorf("Expected initial endpoints 0, got %v", val)
	}

	// 2. Simulate sync with 2 endpoints
	slices := []discoveryv1.EndpointSlice{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "s1"},
			Endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}},
				{Addresses: []string{"10.0.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}},
			},
			Ports: []discoveryv1.EndpointPort{{Port: ptr.To(int32(80))}},
		},
	}

	upstreams := k.buildUpstreams(slices)
	k.storeSnapshot(upstreams)

	val = getGaugeValue(k.metricEndpoints)
	if val != 2 {
		t.Errorf("Expected endpoints metric to be 2, got %v", val)
	}

	// 3. Verify Error counter
	k.metricErrors.Inc()
	errVal := getCounterValue(k.metricErrors)
	if errVal != 1 {
		t.Errorf("Expected error counter to be 1, got %v", errVal)
	}
}

func TestProvision_Replacer(t *testing.T) {
	// Set an env var for replacement
	t.Setenv("MY_NS", "prod-namespace")

	k := &Kubernetes{
		Namespace: "{env.MY_NS}",
		Service:   "my-service",
		client:    fake.NewSimpleClientset(),
	}

	// Manually trigger the replacement logic found in Provision
	repl := caddy.NewReplacer()
	k.Namespace = repl.ReplaceAll(k.Namespace, "")

	if k.Namespace != "prod-namespace" {
		t.Errorf("Replacer failed: expected prod-namespace, got %s", k.Namespace)
	}
}

func TestProvision_NoK8sConfig(t *testing.T) {
	// Backup and clear env
	oldKubeconfig := os.Getenv("KUBECONFIG")
	oldHome := os.Getenv("HOME")
	os.Setenv("KUBECONFIG", "")
	os.Setenv("HOME", "/tmp/non-existent-home-caddy-test")
	defer func() {
		os.Setenv("KUBECONFIG", oldKubeconfig)
		os.Setenv("HOME", oldHome)
	}()

	k := &Kubernetes{
		Service:            "test-service",
		StartupPollTimeout: caddy.Duration(100 * time.Millisecond),
		MaxStaleness:       caddy.Duration(1 * time.Second),
	}

	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()

	err := k.Provision(ctx)
	if err != nil {
		t.Fatalf("Provision failed without K8s config: %v", err)
	}

	if k.client != nil {
		t.Error("Expected k.client to be nil when no config is found")
	}

	select {
	case <-k.ready:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Error("Timed out waiting for k.ready to be closed")
	}

	ups, err := k.GetUpstreams(nil)
	if err != nil {
		t.Errorf("GetUpstreams failed: %v", err)
	}
	if len(ups) != 1 {
		t.Errorf("Expected 1 fallback upstream, got %d", len(ups))
	}
}

func getGaugeValue(g prometheus.Gauge) float64 {
	m := &dto.Metric{}
	g.Write(m)
	return m.GetGauge().GetValue()
}

func getCounterValue(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	c.Write(m)
	return m.GetCounter().GetValue()
}
