package kubernetes

import (
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
)

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
