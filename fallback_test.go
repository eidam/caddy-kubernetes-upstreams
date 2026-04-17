package kubernetes

import (
	"context"
	"testing"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestUpdateFallbackUpstreams(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		namespace     string
		service       string
		port          string
		existingSvc   *corev1.Service
		wantAddr      string
		expectWarning bool
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
			name:          "API failure (missing service) - fallback to DNS",
			namespace:     "other-ns",
			service:       "my-svc",
			port:          "443",
			expectWarning: true,
			wantAddr:      "my-svc.other-ns.svc.cluster.local:443",
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
			expectWarning: true,
			wantAddr:      "headless.default.svc.cluster.local:80",
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
