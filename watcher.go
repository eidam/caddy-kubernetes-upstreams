package kubernetes

import (
	"context"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

func (k *Kubernetes) run(ctx context.Context) {
	// Initial sync
	k.rebuild(ctx)

	go k.watchLoop(ctx)
	go k.pollLoop(ctx)
}

func (k *Kubernetes) watchLoop(ctx context.Context) {
	if k.Watch == nil || !*k.Watch {
		return
	}

	// Debounce timer for coalescing rapid watch events (e.g. during a large rollout)
	const debounceDuration = 100 * time.Millisecond
	var debounceTimer *time.Timer
	updatePending := false

	// Backoff for reconnection failures
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		k.cacheMu.Lock()
		rv := k.lastResourceVersion
		k.cacheMu.Unlock()

		if c := k.logger.Check(zapcore.DebugLevel, "starting watch"); c != nil {
			c.Write(
				zap.String("namespace", k.Namespace),
				zap.String("service", k.Service),
				zap.String("resource_version", rv),
			)
		}

		w, err := k.client.DiscoveryV1().EndpointSlices(k.Namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector:   "kubernetes.io/service-name=" + k.Service,
			ResourceVersion: rv,
		})
		if err != nil {
			k.metricErrors.Inc()
			// If resource version is too old, we must perform a full list to re-sync.
			// We reset backoff here because we are establishing a new "truth".
			if errors.IsResourceExpired(err) || errors.IsGone(err) {
				k.logger.Warn("watch resource version expired; forcing full re-sync",
					zap.String("version", rv))
				k.rebuild(ctx)
				backoff = time.Second
				continue
			}

			// If watch is forbidden (RBAC), log once and stop the watch loop.
			// The pollLoop will continue to provide eventual consistency.
			if errors.IsForbidden(err) {
				k.logger.Warn("watch permission denied; falling back to periodic polling only",
					zap.Error(err))
				return
			}

			k.logger.Error("failed to start watch; backing off",
				zap.Error(err),
				zap.Duration("backoff", backoff))

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > time.Minute {
					backoff = time.Minute
				}
				continue
			}
		}

		// Successful connection: reset backoff
		backoff = time.Second

		// Run the watch event processing in a block to ensure w.Stop() is called via defer
		// if we ever add an early return/break from inside the loop.
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-w.ResultChan():
					if !ok {
						return
					}

					if event.Type == watch.Error {
						k.metricErrors.Inc()
						k.logger.Error("watch event error", zap.Any("object", event.Object))
						return
					}

					// Handle Bookmark events to progress the ResourceVersion even when idle.
					if event.Type == watch.Bookmark {
						if meta, ok := event.Object.(metav1.Object); ok {
							k.cacheMu.Lock()
							if isNewer(meta.GetResourceVersion(), k.lastResourceVersion) {
								k.lastResourceVersion = meta.GetResourceVersion()
							}
							k.cacheMu.Unlock()
						}
						continue
					}

					obj, ok := event.Object.(*discoveryv1.EndpointSlice)
					if !ok {
						continue
					}

					if c := k.logger.Check(zapcore.DebugLevel, "watch event received"); c != nil {
						c.Write(
							zap.String("type", string(event.Type)),
							zap.String("name", obj.Name),
							zap.String("version", obj.ResourceVersion),
						)
					}

					k.cacheMu.Lock()
					if !isNewer(obj.ResourceVersion, k.lastResourceVersion) {
						k.cacheMu.Unlock()
						continue
					}
					k.lastResourceVersion = obj.ResourceVersion

					if k.slicesCache == nil {
						k.slicesCache = make(map[string]discoveryv1.EndpointSlice)
					}

					switch event.Type {
					case watch.Added, watch.Modified:
						k.slicesCache[obj.Name] = *obj
					case watch.Deleted:
						delete(k.slicesCache, obj.Name)
					}

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
					k.cacheMu.Unlock()
				}
			}
		}()

		// Small delay if the watch closed instantly to avoid tight loop
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (k *Kubernetes) rebuild(ctx context.Context) {
	if c := k.logger.Check(zapcore.DebugLevel, "rebuilding upstreams (full poll)"); c != nil {
		c.Write(
			zap.String("namespace", k.Namespace),
			zap.String("service", k.Service),
		)
	}

	start := time.Now()
	// Use a timeout for the API call to avoid hanging
	listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	slices, err := k.client.DiscoveryV1().EndpointSlices(k.Namespace).List(listCtx, metav1.ListOptions{
		LabelSelector: "kubernetes.io/service-name=" + k.Service,
	})
	if err != nil {
		k.metricErrors.Inc()
		k.logger.Error("failed to list endpointslices", zap.Error(err))

		// If we are already stale and failed this poll, log a warning about fallback.
		if k.MaxStaleness > 0 {
			snap := k.upstreams.Load()
			if snap != nil && time.Since(snap.LastUpdated) > time.Duration(k.MaxStaleness) {
				if !k.isFallingBack {
					k.metricFallback.Set(1)
					k.logger.Warn("using service fallback; API data is stale",
						zap.Duration("staleness", time.Since(snap.LastUpdated)),
						zap.Duration("max_staleness", time.Duration(k.MaxStaleness)))
					k.isFallingBack = true
				}
			}
		}
		return
	}
	k.metricSyncTiming.Observe(time.Since(start).Seconds())

	k.cacheMu.Lock()
	defer k.cacheMu.Unlock()

	// 1. New data: perform full update
	if isNewer(slices.ResourceVersion, k.lastResourceVersion) {
		k.lastResourceVersion = slices.ResourceVersion
		k.slicesCache = make(map[string]discoveryv1.EndpointSlice)
		for _, s := range slices.Items {
			k.slicesCache[s.Name] = s
		}
		k.syncSnapshot()

		if c := k.logger.Check(zapcore.DebugLevel, "rebuild complete (new version)"); c != nil {
			c.Write(
				zap.String("version", slices.ResourceVersion),
				zap.Int("slices", len(slices.Items)),
			)
		}
	} else if slices.ResourceVersion == k.lastResourceVersion {
		// 2. Same data: treat as a health heartbeat
		k.refreshTimestamp()
		if c := k.logger.Check(zapcore.DebugLevel, "rebuild complete (heartbeat)"); c != nil {
			c.Write(zap.String("version", slices.ResourceVersion))
		}
	} else {
		// 3. Stale data: ignore
		if c := k.logger.Check(zapcore.DebugLevel, "skipping stale poll result"); c != nil {
			c.Write(
				zap.String("list_version", slices.ResourceVersion),
				zap.String("current_version", k.lastResourceVersion),
			)
		}
		return
	}

	// Signal ready on first successful rebuild
	select {
	case <-k.ready:
	default:
		close(k.ready)
	}
}

func (k *Kubernetes) syncSnapshot() {
	items := make([]discoveryv1.EndpointSlice, 0, len(k.slicesCache))
	for _, s := range k.slicesCache {
		items = append(items, s)
	}

	upstreams := k.buildUpstreams(items)
	now := time.Now()

	k.upstreams.Store(&kubernetesSnapshot{
		upstreams:   upstreams,
		LastUpdated: now,
	})
	k.metricEndpoints.Set(float64(len(upstreams)))

	if k.isFallingBack {
		k.metricFallback.Set(0)
		k.logger.Info("recovered from service fallback; API synchronization restored")
		k.isFallingBack = false
	}
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

	if k.isFallingBack {
		k.metricFallback.Set(0)
		k.logger.Info("recovered from service fallback; API synchronization restored")
		k.isFallingBack = false
	}
}
