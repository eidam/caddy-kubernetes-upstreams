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
