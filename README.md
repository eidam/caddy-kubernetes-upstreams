# Caddy Kubernetes Upstreams

![Test Status](https://github.com/eidam/caddy-kubernetes-upstreams/actions/workflows/test.yml/badge.svg)
![License](https://img.shields.io/github/license/eidam/caddy-kubernetes-upstreams)
![Go Version](https://img.shields.io/github/go-mod/go-version/eidam/caddy-kubernetes-upstreams)

A high-performance Caddy dynamic upstream module that discovers backend endpoints using the **Kubernetes EndpointSlice API**.

## Motivation

Standard Kubernetes service discovery usually relies on **DNS SRV records** or the **ClusterIP**, which introduce several production pain points:
- **Latency**: Updates are delayed by DNS TTL and CoreDNS caching.
- **Limited Load Balancing**: Kubernetes `ClusterIP` routing is typically random/probabilistic (iptables). Caddy cannot use advanced algorithms like `least_conn` effectively because it only "sees" one IP.
- **Complexity**: SRV records often require switching services to "Headless" mode.
- **Instability**: Native Caddy modules often lose load balancer state (connection counts) when DNS records are refreshed.

This module talks **directly to the Kubernetes control plane**. It provides near-instant updates, works with any Service type, and preserves load balancer state across updates, enabling Caddy to perform true granular load balancing directly against Pod IPs.

## Technical Comparison

| Feature | Standard Service | DNS SRV (Headless) | **This Module** |
|---------|------------------|--------------------|-----------------|
| **Update Latency** | Fast (kube-proxy sync) | High (TTL/Caching) | **Near-Instant (Watch API)** |
| **Caddy LB Algorithms** | Ineffective (Random) | Effective | **Effective** |
| **LB State Stability** | Stable | Reset on DNS refresh | **Preserved** |
| **L7 Sticky Sessions** | Incompatible | Compatible | **Compatible** |
| **Observability** | Service Level | Pod Level | **Granular Metrics** |
| **Network Path** | Service Overhead (DNAT) | Direct Pod IP | **Direct Pod IP** |
| **API Fallback** | N/A | No | **Yes (to ClusterIP)** |
| **Permissions** | **None** | **None** | **RBAC Required** |

## Installation

```bash
xcaddy build --with github.com/eidam/caddy-kubernetes-upstreams
```

## Usage

### Caddyfile (Shortcut)
Supports `[namespace/]<service>[:port]`. If namespace is omitted, it is auto-detected.

```caddyfile
reverse_proxy {
    # Same namespace, auto-resolved port
    dynamic kubernetes my-service

    # Specific namespace and port number
    dynamic kubernetes other-ns/my-service:8080

    # Specific port name
    dynamic kubernetes my-service:http
}
```

### Full Configuration
```caddyfile
reverse_proxy {
    dynamic kubernetes my-service {
        namespace other-ns
        port 8080                # Optional: number or name
        kubeconfig ~/.kube/config # Optional: path to kubeconfig file
        
        watch true               # Real-time updates via Watch API (default: true)
        poll_interval 15s        # Periodic safety sync (default: 15s)
        
        max_staleness 1m         # Service Fallback threshold (default: 1m)
        strict_init false        # Fail Caddy startup if initial sync fails (default: false)
        
        allow_unready false      # Route to pods before they pass readiness (default: false)
        allow_terminating false  # Continue routing during graceful shutdown (default: false)
    }
}
```

### JSON Configuration
```json
{
	"handler": "reverse_proxy",
	"upstreams": {
		"source": "kubernetes",
		"service": "my-service",
		"namespace": "default",
		"port": "http",
		"watch": true,
		"strict_init": false,
		"max_staleness": "1m"
	}
}
```

## Features

- **O(1) Hot Path**: Zero-lock request path using atomic snapshot swaps.
- **Incremental Cache**: Processes granular Watch events to minimize API overhead.
- **LB State Stability**: Reuses `Upstream` pointers to preserve load balancer state (e.g. `least_conn` counts).
- **Resilient Startup**: Automatically falls back to the stable Service IP immediately if the API is slow during startup.
- **Service Fallback**: Automatically falls back to the stable Service IP if the API becomes unreachable.
- **Native IPv6**: Uses `net.JoinHostPort` for correct bracketing in dual-stack clusters.
- **Gapless Synchronization**: Uses `ResourceVersion` tracking and Bookmarks to ensure no missed events.

## Observability (Metrics)

This module exposes Prometheus metrics via Caddy's standard metrics endpoint (usually `:2019/metrics`). All metrics include `namespace`, `service`, and `port` labels.

| Metric Name | Type | Help |
|-------------|------|------|
| `caddy_kubernetes_upstreams_endpoints` | Gauge | Number of active pods discovered. |
| `caddy_kubernetes_upstreams_fallback_active` | Gauge | `1` if currently routing via Service Fallback, `0` if healthy. |
| `caddy_kubernetes_upstreams_api_errors_total` | Counter | Total number of failed Kubernetes API calls. |

## Service Fallback

If the Kubernetes API becomes unreachable and the local endpoint data exceeds `max_staleness` (default 1m), the module automatically routes traffic to the Service's stable **ClusterIP**.

The module attempts to fetch the ClusterIP via the API during startup. If permissions are restricted, it falls back to the standard internal DNS name (`service.namespace.svc.cluster.local`).

**Technical Note**: While falling back to the ClusterIP ensures connectivity, you will lose Caddy's granular load balancing (like `least_conn`) because Kubernetes ClusterIPs typically use simple random/probabilistic routing (iptables).

## Permissions (RBAC)

Full namespaced configuration for Caddy:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: caddy
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: caddy-upstream-reader
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get"] # Required only if using Service Fallback
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: caddy-upstream-binding
subjects:
  - kind: ServiceAccount
    name: caddy
roleRef:
  kind: Role
  name: caddy-upstream-reader
  apiGroup: rbac.authorization.k8s.io
```

## License

MIT License - see [LICENSE](LICENSE) for details.
