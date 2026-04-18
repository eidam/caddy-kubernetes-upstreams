package kubernetes

import (
	"strconv"

	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
)

func (k *Kubernetes) resolvePort(ports []discoveryv1.EndpointPort) (int32, bool) {
	if len(ports) == 0 {
		return 0, false
	}

	if k.Port == "" {
		// If no port specified, and there's only one port, use it.
		// If there are multiple ports, pick the first one and warn.
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

	// Try as name (either configured name or resolved name from service port number)
	// This takes precedence because if we resolved a numeric Service port to a name,
	// we MUST use that name to find the correct port in the EndpointSlice (which
	// might have a different numeric port if targetPort is used).
	for _, p := range ports {
		if p.Name != nil {
			if *p.Name == k.Port || (k.resolvedPortName != "" && *p.Name == k.resolvedPortName) {
				if p.Port != nil {
					return *p.Port, true
				}
			}
		}
	}

	// Try numeric second
	if pInt, err := strconv.Atoi(k.Port); err == nil {
		for _, p := range ports {
			if p.Port != nil && *p.Port == int32(pInt) {
				return *p.Port, true
			}
		}
	}

	return 0, false
}
