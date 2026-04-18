package kubernetes

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
)

func TestProvision_NoK8sConfig(t *testing.T) {
	// We want to verify that Provision doesn't return an error when K8s config is missing.

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

	// Create a dummy Caddy context
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()

	// Provision should succeed by logging a warning and falling back to DNS
	err := k.Provision(ctx)
	if err != nil {
		t.Fatalf("Provision failed without K8s config: %v", err)
	}

	if k.client != nil {
		t.Error("Expected k.client to be nil when no config is found")
	}

	// Verify that it didn't hang and closed the ready channel
	select {
	case <-k.ready:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Error("Timed out waiting for k.ready to be closed")
	}

	// GetUpstreams should return the DNS-based fallback because discovery is disabled
	ups, err := k.GetUpstreams(nil)
	if err != nil {
		t.Errorf("GetUpstreams failed: %v", err)
	}
	if len(ups) != 1 {
		t.Errorf("Expected 1 fallback upstream, got %d", len(ups))
	}
}
