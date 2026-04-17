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

	// Start the informer
	k.startInformer(ctx)
}

func (k *Kubernetes) startInformer(ctx context.Context) {
	labelSelector := "kubernetes.io/service-name=" + k.Service

	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = labelSelector
			res, err := k.client.DiscoveryV1().EndpointSlices(k.Namespace).List(ctx, options)
			if err != nil {
				k.metricErrors.Inc()
			}
			return res, err
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = labelSelector
			res, err := k.client.DiscoveryV1().EndpointSlices(k.Namespace).Watch(ctx, options)
			if err != nil {
				k.metricErrors.Inc()
			}
			return res, err
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

	// Debounce timer for coalescing rapid watch events
	const debounceDuration = 100 * time.Millisecond
	var debounceTimer *time.Timer
	updatePending := false

	triggerUpdate := func() {
		k.cacheMu.Lock()
		defer k.cacheMu.Unlock()

		if !updatePending {
			updatePending = true
			if debounceTimer == nil {
				debounceTimer = time.AfterFunc(debounceDuration, func() {
					k.cacheMu.Lock()
					k.syncSnapshot()
					updatePending = false
					k.cacheMu.Unlock()
				})
			} else {
				debounceTimer.Reset(debounceDuration)
			}
		}
	}

	k.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			triggerUpdate()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			triggerUpdate()
		},
		DeleteFunc: func(obj interface{}) {
			triggerUpdate()
		},
	})

	go k.informer.Run(ctx.Done())

	// Wait for the initial cache sync
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), k.informer.HasSynced) {
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

func (k *Kubernetes) storeSnapshot(upstreams []*reverseproxy.Upstream) {
	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   upstreams,
		LastUpdated: time.Now(),
	})
	k.metricEndpoints.Set(float64(len(upstreams)))

	if k.isFallingBack {
		k.metricFallback.Set(0)
		k.logger.Info("recovered from service fallback; API synchronization restored")
		k.isFallingBack = false
	}
}