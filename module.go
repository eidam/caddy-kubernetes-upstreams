package kubernetes

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultPollInterval       = 15 * time.Second
	defaultMaxStaleness       = 1 * time.Minute
	defaultStartupPollTimeout = 10 * time.Second
	defaultFallbackTimeout    = 5 * time.Second
)

// Kubernetes is a Caddy dynamic upstream module that discovers backend endpoints for a Kubernetes Service.
//
// It is designed to be a lightweight, high-performance alternative to full Ingress controllers.
// Instead of using a global EndpointSliceInformer that listens to all services, this module
// creates a targeted, scoped Informer for each configured service using a LabelSelector.
// This significantly reduces resource overhead in large clusters.
type Kubernetes struct {
	// The Kubernetes namespace. Defaults to 'default' or the current namespace if running in-cluster.
	Namespace string `json:"namespace,omitempty"`

	// The name of the Kubernetes Service.
	Service string `json:"service,omitempty"`

	// Path to the kubeconfig file. Optional.
	Kubeconfig string `json:"kubeconfig,omitempty"`

	// The port number or name to use. If not specified, and the Service has only one port, it will be used automatically.
	Port string `json:"port,omitempty"`

	// The interval at which the Informer resyncs its local cache. Default: 15s.
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
	metricLabels    prometheus.Labels
	metricEndpoints prometheus.Gauge
	metricErrors    prometheus.Counter
	metricFallback  prometheus.Gauge

	// Informer for EndpointSlices
	informer cache.SharedIndexInformer
	cacheMu  sync.Mutex

	// Flag to track if a full rebuild is needed or just a timestamp refresh.
	// Only modified under cacheMu.
	needsRebuild bool

	// Pool of long-lived upstream pointers to ensure stability for load balancer state.
	// Only modified under cacheMu.
	upstreamPool map[string]*reverseproxy.Upstream

	// Track fallback state for logging purposes
	isFallingBack atomic.Bool

	// The resolved name of the port from the Service spec.
	// EndpointSlice ports are matched against this name if it's set.
	resolvedPortName string

	// The resolved numeric pod port (targetPort).
	// Used for matching EndpointSlice ports if resolvedPortName is empty.
	resolvedPodPort int32

	// triggerUpdate is a callback to trigger a snapshot rebuild.
	// Only set during Provision/startInformer.
	triggerUpdate func(rebuild bool)

	ready  chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

type kubernetesSnapshot struct {
	upstreams   []*reverseproxy.Upstream
	LastUpdated time.Time
}

func init() {
	caddy.RegisterModule(new(Kubernetes))
}

// CaddyModule returns the Caddy module information.
func (*Kubernetes) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.upstreams.kubernetes",
		New: func() caddy.Module { return new(Kubernetes) },
	}
}

// Provision sets up the module.
func (k *Kubernetes) Provision(ctx caddy.Context) error {
	k.logger = ctx.Logger(k)
	k.ready = make(chan struct{})
	k.ctx, k.cancel = context.WithCancel(ctx)

	// Initialize empty upstreams snapshot.
	// We set LastUpdated to zero time to ensure that if the first sync is slow,
	// GetUpstreams will immediately trigger the Service Fallback (ClusterIP)
	// instead of returning an empty list during the MaxStaleness window.
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   make([]*reverseproxy.Upstream, 0),
		LastUpdated: time.Time{},
	})

	repl := caddy.NewReplacer()
	k.Namespace = repl.ReplaceAll(k.Namespace, "")
	k.Service = repl.ReplaceAll(k.Service, "")
	k.Port = repl.ReplaceAll(k.Port, "")

	// Default namespace logic
	if k.Namespace == "" {
		// Try to detect if running in-cluster
		if ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			k.Namespace = string(ns)
		} else {
			k.Namespace = "default"
		}
	}

	// Initialize metrics
	k.provisionMetrics()

	if k.PollInterval <= 0 {
		k.PollInterval = caddy.Duration(defaultPollInterval)
	}
	if k.MaxStaleness <= 0 {
		k.MaxStaleness = caddy.Duration(defaultMaxStaleness)
	}
	if k.StartupPollTimeout <= 0 {
		k.StartupPollTimeout = caddy.Duration(defaultStartupPollTimeout)
	}

	err := k.initKubernetesClient()
	if err != nil {
		return err
	}

	if k.MaxStaleness > 0 {
		// Use a short timeout for the fallback initialization to avoid hanging startup
		fallbackCtx, cancel := context.WithTimeout(k.ctx, defaultFallbackTimeout)
		k.updateFallbackUpstreams(fallbackCtx)
		cancel()
	}

	// Start the control loops
	go k.run(k.ctx)

	// Wait for initial sync or timeout
	if k.client != nil {
		select {
		case <-k.ready:
			if c := k.logger.Check(zapcore.DebugLevel, "initial sync complete"); c != nil {
				c.Write(
					zap.String("namespace", k.Namespace),
					zap.String("service", k.Service),
				)
			}
		case <-time.After(time.Duration(k.StartupPollTimeout)):
			if k.StrictInit {
				return fmt.Errorf("initial kubernetes sync timed out after %s (strict_init=true)", time.Duration(k.StartupPollTimeout))
			}
			k.logger.Warn("initial sync timed out; starting anyway",
				zap.String("namespace", k.Namespace),
				zap.String("service", k.Service))
		case <-k.ctx.Done():
			return k.ctx.Err()
		}
	} else {
		k.logger.Info("kubernetes API discovery disabled; skipping initial sync")
		select {
		case <-k.ready:
		default:
			close(k.ready)
		}
	}

	return nil
}

func (k *Kubernetes) provisionMetrics() {
	// Initialize metrics labels
	k.metricLabels = prometheus.Labels{
		"namespace": k.Namespace,
		"service":   k.Service,
		"port":      k.Port,
	}

	// Register/Initialize metrics
	k.metricEndpoints = upstreamsEndpoints.With(k.metricLabels)
	k.metricErrors = upstreamsErrors.With(k.metricLabels)
	k.metricFallback = upstreamsFallbackActive.With(k.metricLabels)

	// Ensure metrics start at 0
	k.metricEndpoints.Set(0)
	k.metricFallback.Set(0)
}

func (k *Kubernetes) initKubernetesClient() error {
	var config *rest.Config
	var err error

	repl := caddy.NewReplacer()
	kubeconfigPath := repl.ReplaceAll(k.Kubeconfig, "")

	if kubeconfigPath != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes config from kubeconfig at %s: %v", kubeconfigPath, err)
		}
	} else {
		// Try in-cluster config first
		config, err = rest.InClusterConfig()
		if err != nil {
			// Fallback to standard kubeconfig location
			kubeconfig := os.Getenv("KUBECONFIG")
			if kubeconfig == "" {
				home, _ := os.UserHomeDir()
				kubeconfig = filepath.Join(home, ".kube", "config")
			}

			// Check if the kubeconfig file exists before trying to build from it.
			// If it doesn't exist and we're not in-cluster, we just skip API discovery.
			if _, statErr := os.Stat(kubeconfig); os.IsNotExist(statErr) {
				k.logger.Warn("no kubernetes configuration found; API discovery will be disabled",
					zap.String("tried", kubeconfig))
				return nil
			}

			config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
			if err != nil {
				return fmt.Errorf("failed to create kubernetes config: %v", err)
			}
		}
	}

	k.client, err = kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %v", err)
	}

	return nil
}

// Validate validates the configuration.
func (k *Kubernetes) Validate() error {
	if k.Service == "" {
		return fmt.Errorf("service is required")
	}
	if k.MaxStaleness > 0 && k.MaxStaleness <= k.PollInterval {
		return fmt.Errorf("max_staleness (%s) must be greater than poll_interval (%s) to prevent flapping",
			time.Duration(k.MaxStaleness), time.Duration(k.PollInterval))
	}
	return nil
}

// Cleanup cleans up the module.
func (k *Kubernetes) Cleanup() error {
	if k.cancel != nil {
		k.cancel()
	}
	return nil
}

// GetUpstreams returns the upstreams.
func (k *Kubernetes) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	snap := k.upstreams.Load()
	if snap == nil {
		return nil, nil
	}

	// Degraded mode: fallback if stale or empty (MaxStaleness > 0 enables this)
	if k.MaxStaleness > 0 {
		isStale := time.Since(snap.LastUpdated) > time.Duration(k.MaxStaleness)
		isEmpty := len(snap.upstreams) == 0

		if isStale || isEmpty {
			fallbackPtr := k.fallbackUpstreams.Load()
			if fallbackPtr != nil && len(*fallbackPtr) > 0 {
				if k.isFallingBack.CompareAndSwap(false, true) {
					k.metricFallback.Set(1)
					if isStale {
						if k.client == nil {
							k.logger.Warn("service fallback active; kubernetes API discovery is disabled")
						} else {
							k.logger.Warn("service fallback active; API synchronization is stale")
						}
					} else {
						k.logger.Warn("service fallback active; no fine-grained endpoints found")
					}
				}
				fallback := *fallbackPtr
				upstreams := make([]*reverseproxy.Upstream, len(fallback))
				copy(upstreams, fallback)
				return upstreams, nil
			}
		}
	}

	// Return a copy of the slice to prevent external mutation by other modules.
	// The Upstream objects themselves are treated as immutable.
	upstreams := make([]*reverseproxy.Upstream, len(snap.upstreams))
	copy(upstreams, snap.upstreams)
	return upstreams, nil
}

func (k *Kubernetes) updateFallbackUpstreams(ctx context.Context) {
	ns := k.Namespace
	svcName := k.Service

	// Default DNS-based fallback
	dnsFallback := fmt.Sprintf("%s.%s.svc", svcName, ns)
	resolvedPort := k.Port

	var svc *corev1.Service
	var err error

	// Try to get explicit Service info from API if client is available
	if k.client != nil {
		svc, err = k.client.CoreV1().Services(ns).Get(ctx, svcName, metav1.GetOptions{})
		if err != nil {
			k.metricErrors.Inc()
		}
	} else {
		err = fmt.Errorf("kubernetes client not initialized")
	}

	var port int32
	var portName string
	var podPort int32

	if err == nil && svc != nil {
		var matchedPort *corev1.ServicePort
		if resolvedPort == "" {
			if len(svc.Spec.Ports) == 1 {
				matchedPort = &svc.Spec.Ports[0]
			}
		} else if pInt, err := strconv.Atoi(resolvedPort); err == nil {
			for i := range svc.Spec.Ports {
				if svc.Spec.Ports[i].Port == int32(pInt) {
					matchedPort = &svc.Spec.Ports[i]
					break
				}
			}
		} else {
			for i := range svc.Spec.Ports {
				if svc.Spec.Ports[i].Name == resolvedPort {
					matchedPort = &svc.Spec.Ports[i]
					break
				}
			}
		}

		if matchedPort != nil {
			port = matchedPort.Port
			portName = matchedPort.Name
			if matchedPort.TargetPort.Type == intstr.Int && matchedPort.TargetPort.IntVal != 0 {
				podPort = matchedPort.TargetPort.IntVal
			} else if matchedPort.TargetPort.Type == intstr.String {
				// For named targetPorts, we match ONLY by the Service Port name
				// in the EndpointSlice.
				podPort = 0
			} else {
				// K8s default: targetPort defaults to port if not specified
				podPort = matchedPort.Port
			}
		}

		// Update resolved state for EndpointSlice matching
		k.cacheMu.Lock()
		changed := k.resolvedPortName != portName || k.resolvedPodPort != podPort
		k.resolvedPortName = portName
		k.resolvedPodPort = podPort
		k.cacheMu.Unlock()

		// If the mapping changed and we currently have no upstreams, trigger a rebuild
		// to recover from "could not resolve port" state immediately.
		if changed && k.triggerUpdate != nil {
			snap := k.upstreams.Load()
			if snap == nil || len(snap.upstreams) == 0 {
				k.triggerUpdate(true)
			}
		}
	}

	// Determine new fallback address
	var newDial string
	isAPI := false
	if err == nil && svc != nil && port > 0 && svc.Spec.ClusterIP != "" && svc.Spec.ClusterIP != "None" {
		newDial = net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(int(port)))
		isAPI = true
	} else if resolvedPort != "" {
		newDial = net.JoinHostPort(dnsFallback, resolvedPort)
	} else {
		newDial = dnsFallback
	}

	// Only store and log if it changed
	oldFallback := k.fallbackUpstreams.Load()
	if oldFallback == nil || len(*oldFallback) == 0 || (*oldFallback)[0].Dial != newDial {
		fallback := []*reverseproxy.Upstream{{Dial: newDial}}
		k.fallbackUpstreams.Store(&fallback)

		if isAPI {
			k.logger.Debug("initialized API-based fallback upstream",
				zap.String("addr", newDial))
		} else {
			if err != nil && k.client != nil {
				k.logger.Warn("initialized DNS-based fallback upstream due to API error",
					zap.String("addr", newDial),
					zap.Error(err))
			} else {
				k.logger.Debug("initialized DNS-based fallback upstream",
					zap.String("addr", newDial))
			}
		}
	}
}

// UnmarshalCaddyfile unmarshals the Caddyfile.
func (k *Kubernetes) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		// Consume the directive name
		if d.Val() != "kubernetes" {
			continue
		}

		// Check for shortcut syntax: dynamic kubernetes [namespace/]<service>[:port]
		if d.NextArg() {
			val := d.Val()

			// Handle [namespace/]<service>
			if strings.Contains(val, "/") {
				parts := strings.Split(val, "/")
				if len(parts) != 2 {
					return d.Errf("invalid service format; expected [namespace/]<service>[:port]")
				}
				k.Namespace = parts[0]
				val = parts[1]
			}

			// Handle <service>[:port]
			if strings.Contains(val, ":") {
				parts := strings.Split(val, ":")
				if len(parts) != 2 {
					return d.Errf("invalid service format; expected [namespace/]<service>[:port]")
				}
				k.Service = parts[0]
				k.Port = parts[1]
			} else {
				k.Service = val
			}

			// If there are more arguments, it's an error
			if d.NextArg() {
				return d.ArgErr()
			}
		}

		for d.NextBlock(0) {
			val := d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}

			switch val {
			case "namespace":
				k.Namespace = d.Val()
			case "service":
				k.Service = d.Val()
			case "kubeconfig":
				k.Kubeconfig = d.Val()
			case "port":
				k.Port = d.Val()
			case "strict_init":
				k.StrictInit = d.Val() == "true"
			case "poll_interval", "startup_poll_timeout", "max_staleness":
				dur, err := caddy.ParseDuration(d.Val())
				if err != nil {
					return d.Errf("invalid %s: %v", val, err)
				}
				cDur := caddy.Duration(dur)
				switch val {
				case "poll_interval":
					k.PollInterval = cDur
				case "startup_poll_timeout":
					k.StartupPollTimeout = cDur
				case "max_staleness":
					k.MaxStaleness = cDur
				}
			case "allow_unready":
				k.AllowUnready = d.Val() == "true"
			case "allow_terminating":
				k.AllowTerminating = d.Val() == "true"
			default:
				return d.Errf("unrecognized subdirective: %s", d.Val())
			}
		}
	}
	return nil
}

var (
	upstreamsEndpoints = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "kubernetes_upstreams",
		Name:      "endpoints",
		Help:      "Number of active pods discovered for the service.",
	}, []string{"namespace", "service", "port"})

	upstreamsErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "caddy",
		Subsystem: "kubernetes_upstreams",
		Name:      "api_errors_total",
		Help:      "Total number of failed Kubernetes API calls (Watch/List/Get).",
	}, []string{"namespace", "service", "port"})

	upstreamsFallbackActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "caddy",
		Subsystem: "kubernetes_upstreams",
		Name:      "fallback_active",
		Help:      "Whether the module is currently routing via the Service Fallback (1) or fine-grained pods (0).",
	}, []string{"namespace", "service", "port"})
)

// Interface guards
var (
	_ caddy.Module                = (*Kubernetes)(nil)
	_ caddy.Provisioner           = (*Kubernetes)(nil)
	_ caddy.Validator             = (*Kubernetes)(nil)
	_ caddy.CleanerUpper          = (*Kubernetes)(nil)
	_ reverseproxy.UpstreamSource = (*Kubernetes)(nil)
	_ caddyfile.Unmarshaler       = (*Kubernetes)(nil)
)
