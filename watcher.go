package kubernetes

import (
	"context"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func (k *Kubernetes) run(ctx context.Context) {
	if k.MaxStaleness > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(k.PollInterval))
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					updateCtx, cancel := context.WithTimeout(ctx, defaultFallbackTimeout)
					k.updateFallbackUpstreams(updateCtx)
					cancel()
				}
			}
		}()
	}

	// Always use the informer
	k.startInformer(ctx)
}

func (k *Kubernetes) startInformer(ctx context.Context) {
	// We use a custom ListWatch with a specific LabelSelector instead of a 
	// standard EndpointSliceInformer from a SharedInformerFactory.
	// 
	// This is a deliberate design choice:
	// 1. Efficiency: Standard Informers listen to ALL EndpointSlices in the 
	//    namespace/cluster. By scoping the watch to a specific service, we 
	//    drastically reduce CPU/memory overhead and network traffic, especially 
	//    in large clusters with thousands of services.
	// 2. Lightweight: It avoids the dependency on a global SharedInformerFactory,
	//    keeping the module self-contained and easy to initialize.
	//
	// If a user needs many upstreams, they are likely better served by a 
	// full Ingress or Gateway controller.
	labelSelector := "kubernetes.io/service-name=" + k.Service

	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = labelSelector
			return k.client.DiscoveryV1().EndpointSlices(k.Namespace).List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = labelSelector
			return k.client.DiscoveryV1().EndpointSlices(k.Namespace).Watch(ctx, options)
		},
	}

	k.informer = cache.NewSharedIndexInformer(
		listWatch,
		&discoveryv1.EndpointSlice{},
		time.Duration(k.PollInterval),
		cache.Indexers{},
	)

	if err := k.informer.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
		k.metricErrors.Inc()
		k.logger.Error("informer watch error", zap.Error(err))
	}); err != nil {
		k.logger.Warn("failed to set watch error handler", zap.Error(err))
	}

	// Timer for coalescing rapid watch events or resyncs
	const coalesceDuration = 100 * time.Millisecond
	var coalesceTimer *time.Timer
	updatePending := false

	triggerUpdate := func(rebuild bool) {
		k.cacheMu.Lock()
		defer k.cacheMu.Unlock()

		if rebuild {
			k.needsRebuild = true
		}

		if updatePending {
			// A sync is already scheduled, it will pick up the current state
			return
		}

		updatePending = true
		if coalesceTimer == nil {
			coalesceTimer = time.AfterFunc(coalesceDuration, func() {
				k.cacheMu.Lock()
				defer k.cacheMu.Unlock()

				updatePending = false

				if k.needsRebuild {
					k.syncSnapshot()
					k.needsRebuild = false
				} else {
					k.refreshTimestamp()
				}
			})
		} else {
			coalesceTimer.Reset(coalesceDuration)
		}
	}

	k.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			triggerUpdate(true)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// oldObj == newObj indicates a periodic resync (heartbeat)
			// rather than an actual change in the Kubernetes cluster.
			triggerUpdate(oldObj != newObj)
		},
		DeleteFunc: func(obj interface{}) {
			triggerUpdate(true)
		},
	})

	go k.informer.Run(ctx.Done())

	// Wait for the initial cache sync
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), k.informer.HasSynced) {
			// Perform the initial sync synchronously to ensure endpoints are
			// populated before Caddy starts routing traffic.
			k.cacheMu.Lock()
			k.syncSnapshot()
			k.needsRebuild = false
			k.cacheMu.Unlock()

			select {
			case <-k.ready:
			default:
				close(k.ready)
			}
		}
	}()
}

func (k *Kubernetes) syncSnapshot() {
	if k.informer == nil {
		return
	}

	objs := k.informer.GetStore().List()
	items := make([]discoveryv1.EndpointSlice, 0, len(objs))
	for _, obj := range objs {
		if slice, ok := obj.(*discoveryv1.EndpointSlice); ok {
			items = append(items, *slice)
		}
	}

	upstreams := k.buildUpstreams(items)
	k.storeSnapshot(upstreams)
}

func (k *Kubernetes) refreshTimestamp() {
	snap := k.upstreams.Load()
	if snap == nil {
		k.syncSnapshot()
		return
	}
	// Re-use the existing immutable upstream list, just update the timestamp
	k.storeSnapshot(snap.upstreams)
}

func (k *Kubernetes) storeSnapshot(upstreams []*reverseproxy.Upstream) {
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   upstreams,
		LastUpdated: time.Now(),
	})
	k.metricEndpoints.Set(float64(len(upstreams)))

	if k.isFallingBack.Load() {
		k.metricFallback.Set(0)
		k.logger.Info("recovered from service fallback; API synchronization restored")
		k.isFallingBack.Store(false)
	}
}
