package kubernetes

import (
	"net"
	"sort"
	"strconv"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	discoveryv1 "k8s.io/api/discovery/v1"
)

func (k *Kubernetes) buildUpstreams(slices []discoveryv1.EndpointSlice) []*reverseproxy.Upstream {
	var upstreams []*reverseproxy.Upstream
	seen := make(map[string]struct{})

	if k.upstreamPool == nil {
		k.upstreamPool = make(map[string]*reverseproxy.Upstream)
	}

	for _, slice := range slices {
		// Resolve port for this slice. If it fails, we can't route to any endpoints in it.
		portNum, found := k.resolvePort(slice.Ports)
		if !found {
			if c := k.logger.Check(zapcore.DebugLevel, "could not resolve port for slice"); c != nil {
				c.Write(
					zap.String("slice", slice.Name),
					zap.String("service", k.Service),
					zap.String("requested_port", k.Port),
				)
			}
			continue
		}

		for _, endpoint := range slice.Endpoints {
			if len(endpoint.Addresses) == 0 {
				continue
			}

			// Extract conditions with proper fallbacks.
			isReady := endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready
			isServing := endpoint.Conditions.Serving != nil && *endpoint.Conditions.Serving
			isTerminating := endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating

			// If Serving is not set, fallback to Ready.
			canServe := isServing
			if endpoint.Conditions.Serving == nil {
				canServe = isReady
			}

			// Filtering logic:
			// 1. Skip if it cannot serve, unless we explicitly allow unready endpoints.
			if !k.AllowUnready && !canServe {
				continue
			}

			// 2. Skip if it is terminating, unless we explicitly allow terminating endpoints.
			if !k.AllowTerminating && isTerminating {
				continue
			}

			for _, addr := range endpoint.Addresses {
				// Use net.JoinHostPort to correctly handle IPv6 addresses
				dial := net.JoinHostPort(addr, strconv.Itoa(int(portNum)))
				if _, ok := seen[dial]; ok {
					continue
				}
				seen[dial] = struct{}{}

				// Reuse the pointer if the address is already in the pool.
				// This preserves Caddy's long-lived state (like active request counts)
				// for that specific upstream across updates.
				ups, ok := k.upstreamPool[dial]
				if !ok {
					ups = &reverseproxy.Upstream{Dial: dial}
					k.upstreamPool[dial] = ups
				}
				upstreams = append(upstreams, ups)
			}
		}
	}

	// Prune the pool of addresses that are no longer present in any slice.
	// This prevents memory leaks as Pods are deleted.
	for dial := range k.upstreamPool {
		if _, ok := seen[dial]; !ok {
			delete(k.upstreamPool, dial)
		}
	}

	// Sort for stability
	sort.Slice(upstreams, func(i, j int) bool {
		return upstreams[i].Dial < upstreams[j].Dial
	})

	return upstreams
}

func (k *Kubernetes) resolvePort(ports []discoveryv1.EndpointPort) (int32, bool) {
	if len(ports) == 0 {
		return 0, false
	}

	// If no port specified, and there's only one port, use it.
	if k.Port == "" {
		if len(ports) > 1 {
			portName := "<unnamed>"
			if ports[0].Name != nil {
				portName = *ports[0].Name
			}
			k.logger.Warn("multiple ports available but none specified; picking the first one",
				zap.String("service", k.Service),
				zap.String("picked", portName))
		}
		if ports[0].Port != nil {
			return *ports[0].Port, true
		}
		return 0, false
	}

	// Try as name (either resolved Service port name, or resolved targetPort name)
	if k.resolvedPortName != "" {
		for _, p := range ports {
			if p.Name != nil && *p.Name == k.resolvedPortName {
				if p.Port != nil {
					return *p.Port, true
				}
			}
		}
		// If the Service port is named, we MUST find that name in the EndpointSlice.
		return 0, false
	}

	// Try numeric (resolved numeric targetPort or matching numeric service port)
	if k.resolvedPodPort != 0 {
		for _, p := range ports {
			if p.Port != nil && *p.Port == k.resolvedPodPort {
				return *p.Port, true
			}
		}
	}

	return 0, false
}
