package kubernetes

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	defaultPollInterval       = 15 * time.Second
	defaultMaxStaleness       = 1 * time.Minute
	defaultStartupPollTimeout = 10 * time.Second
	defaultFallbackTimeout    = 5 * time.Second
	defaultWatch              = true
)

// Kubernetes is a Caddy dynamic upstream module that discovers backend endpoints for a Kubernetes Service.
type Kubernetes struct {
	// The Kubernetes namespace. Defaults to 'default' or the current namespace if running in-cluster.
	Namespace string `json:"namespace,omitempty"`

	// The name of the Kubernetes Service.
	Service string `json:"service,omitempty"`

	// Path to the kubeconfig file. Optional.
	Kubeconfig string `json:"kubeconfig,omitempty"`

	// The port number or name to use. If not specified, and the Service has only one port, it will be used automatically.
	Port string `json:"port,omitempty"`

	// Enable near real-time updates via the Kubernetes Watch API. Defaults to true.
	Watch *bool `json:"watch,omitempty"`

	// The interval at which to poll the Kubernetes API as a fallback. Default: 15s.
	PollInterval caddy.Duration `json:"poll_interval,omitempty"`

	// How long to wait for the initial endpoint discovery before Caddy starts. Default: 10s.
	StartupPollTimeout caddy.Duration `json:"startup_poll_timeout,omitempty"`

	// Allow endpoints that have not yet passed their readiness probes. Default: false.
	AllowUnready bool `json:"allow_unready,omitempty"`

	// Allow endpoints that are in the process of terminating. Default: false.
	AllowTerminating bool `json:"allow_terminating,omitempty"`

	// How long the data can be stale before falling back to ClusterIP.
	// Setting this > 0 enables Service Fallback. Default: 1m.
	MaxStaleness caddy.Duration `json:"max_staleness,omitempty"`

	// If true, Caddy will fail to start if the initial endpoint discovery fails or times out.
	// Default: false.
	StrictInit bool `json:"strict_init,omitempty"`

	client            kubernetes.Interface
	upstreams         atomic.Pointer[kubernetesSnapshot]
	fallbackUpstreams atomic.Pointer[[]*reverseproxy.Upstream]
	logger            *zap.Logger

	// Metrics
	metricLabels     prometheus.Labels
	metricEndpoints  prometheus.Gauge
	metricErrors     prometheus.Counter
	metricFallback   prometheus.Gauge
	metricSyncTiming prometheus.Observer

	// Informer for EndpointSlices
	informer cache.SharedIndexInformer
	cacheMu  sync.Mutex

	// Pool of long-lived upstream pointers to ensure stability for load balancer state.
	// Only modified under cacheMu.
	upstreamPool map[string]*reverseproxy.Upstream

	// Track fallback state for logging purposes
	isFallingBack bool

	ready  chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

type kubernetesSnapshot struct {
	upstreams   []*reverseproxy.Upstream
	LastUpdated time.Time
}

// Interface guards
var (
	_ caddy.Module                = (*Kubernetes)(nil)
	_ caddy.Provisioner           = (*Kubernetes)(nil)
	_ caddy.Validator             = (*Kubernetes)(nil)
	_ caddy.CleanerUpper          = (*Kubernetes)(nil)
	_ reverseproxy.UpstreamSource = (*Kubernetes)(nil)
	_ caddyfile.Unmarshaler       = (*Kubernetes)(nil)
)
