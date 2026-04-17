package kubernetes

import (
	"context"
	"time"
)

func (k *Kubernetes) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(k.PollInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			k.rebuild(ctx)
		}
	}
}
