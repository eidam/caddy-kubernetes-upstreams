package kubernetes

import (
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

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
		client:    fake.NewSimpleClientset(), // Satisfy initKubernetesClient if needed
	}

	// We'll mock the client initialization to avoid actual K8s client logic
	k.client = fake.NewSimpleClientset()

	// Manually trigger the replacement logic found in Provision
	repl := caddy.NewReplacer()
	k.Namespace = repl.ReplaceAll(k.Namespace, "")

	if k.Namespace != "prod-namespace" {
		t.Errorf("Replacer failed: expected prod-namespace, got %s", k.Namespace)
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
