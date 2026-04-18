# Caddy Kubernetes Upstreams

![Test Status](https://github.com/eidam/caddy-kubernetes-upstreams/actions/workflows/test.yml/badge.svg)
![License](https://img.shields.io/github/license/eidam/caddy-kubernetes-upstreams)
![Go Version](https://img.shields.io/github/go-mod/go-version/eidam/caddy-kubernetes-upstreams)

A high-performance Caddy dynamic upstream module that discovers backend endpoints using the **Kubernetes EndpointSlice API**.

## Motivation

Standard Kubernetes service discovery usually relies on **DNS SRV records** or the **ClusterIP**, which introduce several production pain points:
- **Latency**: Updates for DNS-based discovery are delayed by TTL and CoreDNS caching.
- **Limited Load Balancing**: Kubernetes `ClusterIP` routing is performed at Layer 4 (iptables/IPVS) and is typically random or probabilistic. Caddy cannot use advanced Layer 7 algorithms like `least_conn` effectively because it only "sees" one destination IP.
- **Complexity**: SRV records often require switching services to "Headless" mode, complicating standard service discovery for other consumers.
- **Instability**: Native Caddy modules often reset load balancer state (e.g., active request counts) when DNS records are refreshed, causing traffic imbalances.

This module talks **directly to the Kubernetes control plane** using high-efficiency **Informers**. It provides near-instant updates, works with any Service type, and preserves load balancer state across updates, enabling Caddy to perform true granular load balancing directly against Pod IPs.

## Technical Comparison

| Feature | Standard Service | DNS SRV (Headless) | **This Module** |
|---------|------------------|--------------------|-----------------|
| **Mechanism** | **Static (ClusterIP)** | **Poll (DNS TTL)** | **Push (Watch API)** |
| **Update Latency** | Fast (Kube-proxy sync) | High (TTL/Caching) | **Real-time (<100ms)** |
| **Caddy LB Algorithms** | Ineffective (L4 Random) | Effective (L7) | **Effective (L7)** |
| **LB State Stability** | Stable (Single IP) | Reset on DNS refresh | **Preserved** |
| **L7 Sticky Sessions** | Incompatible | Compatible | **Compatible** |
| **Observability** | Service Level | Pod Level | **Granular Pod Metrics** |
| **Network Path** | Service DNAT/IPVS | Direct Pod IP | **Direct Pod IP** |
| **API Fallback** | N/A | No | **Yes (to ClusterIP)** |
| **Permissions** | None | None | **RBAC Required** |

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
        
        poll_interval 15s        # Informer resync interval (default: 15s)
        
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
		"strict_init": false,
		"max_staleness": "1m"
	}
}
```

## Features

- **Event-Driven Informer**: Uses Kubernetes `SharedIndexInformer` for high-efficiency, low-latency updates.
- **Debounced Updates**: Coalesces rapid watch events (100ms window) to prevent CPU spikes during large deployments.
- **O(1) Hot Path**: Zero-lock request path using atomic snapshot swaps.
- **LB State Stability**: Reuses `Upstream` pointers to preserve load balancer state (e.g., `least_conn` counts).
- **Resilient Startup**: Automatically falls back to the stable ClusterIP immediately if the API is slow during startup.
- **Service Fallback**: Automatically falls back to the stable ClusterIP if the API becomes unreachable.
- **Native IPv6**: Uses `net.JoinHostPort` for correct bracketing in dual-stack clusters.

## Observability (Metrics)

This module exposes Prometheus metrics via Caddy's standard metrics endpoint (usually `:2019/metrics`). All metrics include `namespace`, `service`, and `port` labels.

| Metric Name | Type | Help |
|-------------|------|------|
| `caddy_kubernetes_upstreams_endpoints` | Gauge | Number of active pods discovered. |
| `caddy_kubernetes_upstreams_fallback_active` | Gauge | `1` if currently routing via Service Fallback, `0` if healthy. |
| `caddy_kubernetes_upstreams_api_errors_total` | Counter | Total number of failed Kubernetes API calls (Watch/List/Get). |

## Service Fallback

If the Kubernetes API becomes unreachable and the local endpoint data exceeds `max_staleness` (default 1m), the module automatically routes traffic to the Service's stable **ClusterIP**.

The module attempts to fetch the ClusterIP via the API during startup. If permissions are restricted, it falls back to the standard internal DNS name (`service.namespace.svc`).

**Technical Note**: While falling back to the ClusterIP ensures connectivity, you will lose Caddy's granular Layer 7 load balancing (like `least_conn`) because Kubernetes ClusterIPs typically use Layer 4 random/probabilistic routing.

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

## Troubleshooting

If Caddy is not routing correctly, enable debug logging in your global Caddyfile options:

```caddyfile
{
    debug
}
```

This module emits helpful diagnostics at the `DEBUG` level, such as:
- `initial sync complete`: Confirms the Informer has populated the first batch of endpoints.
- `could not resolve port for slice`: Indicates a mismatch between the configured port and the EndpointSlice data.

You can also monitor the Prometheus metrics (e.g., `caddy_kubernetes_upstreams_fallback_active`) to see if the API connection is healthy.

## License

MIT License - see [LICENSE](LICENSE) for details.
