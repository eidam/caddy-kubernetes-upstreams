package kubernetes

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
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
)

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

	// Degraded mode: fallback if stale (MaxStaleness > 0 enables this)
	if k.MaxStaleness > 0 {
		if time.Since(snap.LastUpdated) > time.Duration(k.MaxStaleness) {
			if k.isFallingBack.CompareAndSwap(false, true) {
				k.metricFallback.Set(1)
				if k.client == nil {
					k.logger.Warn("service fallback active; kubernetes API discovery is disabled")
				} else {
					k.logger.Warn("service fallback active; API synchronization is stale")
				}
			}
			fallbackPtr := k.fallbackUpstreams.Load()
			if fallbackPtr != nil && len(*fallbackPtr) > 0 {
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

	// Try to get explicit ClusterIP from API if client is available
	if k.client != nil {
		svc, err = k.client.CoreV1().Services(ns).Get(ctx, svcName, metav1.GetOptions{})
		if err != nil {
			k.metricErrors.Inc()
		}
	} else {
		err = fmt.Errorf("kubernetes client not initialized")
	}

	if err == nil && svc != nil && svc.Spec.ClusterIP != "" && svc.Spec.ClusterIP != "None" {
		host := svc.Spec.ClusterIP
		var port int32
		var portName string

		// Resolve port number
		if resolvedPort == "" {
			if len(svc.Spec.Ports) == 1 {
				port = svc.Spec.Ports[0].Port
				portName = svc.Spec.Ports[0].Name
			}
		} else {
			if pInt, err := strconv.Atoi(resolvedPort); err == nil {
				// Search by port number in Service
				for _, p := range svc.Spec.Ports {
					if p.Port == int32(pInt) {
						port = p.Port
						portName = p.Name
						break
					}
				}
			} else {
				// Search by name in Service
				for _, p := range svc.Spec.Ports {
					if p.Name == resolvedPort {
						port = p.Port
						portName = p.Name
						break
					}
				}
			}
		}

		// Update resolvedPortName for EndpointSlice matching
		k.cacheMu.Lock()
		k.resolvedPortName = portName
		k.cacheMu.Unlock()

		if port > 0 {
			fallback := []*reverseproxy.Upstream{{
				Dial: fmt.Sprintf("%s:%d", host, port),
			}}
			k.fallbackUpstreams.Store(&fallback)
			k.logger.Debug("initialized API-based fallback upstream",
				zap.String("addr", fallback[0].Dial))
			return
		}
	}

	// Fallback to DNS if API failed or port not resolved
	var fallback []*reverseproxy.Upstream
	if resolvedPort != "" {
		fallback = []*reverseproxy.Upstream{{
			Dial: fmt.Sprintf("%s:%s", dnsFallback, resolvedPort),
		}}
	} else {
		// We don't even have a port, fallback is partial but better than nothing
		fallback = []*reverseproxy.Upstream{{
			Dial: dnsFallback,
		}}
	}
	k.fallbackUpstreams.Store(&fallback)

	if err != nil && k.client != nil {
		k.logger.Warn("initialized DNS-based fallback upstream due to API error",
			zap.String("addr", fallback[0].Dial),
			zap.Error(err))
	} else {
		k.logger.Debug("initialized DNS-based fallback upstream",
			zap.String("addr", fallback[0].Dial),
			zap.Error(err))
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
