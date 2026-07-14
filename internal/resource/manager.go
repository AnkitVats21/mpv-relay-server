package resource

import (
	"context"
	"sync"
	"time"
)

type ResourceManager struct {
	liveStreamGate chan struct{}
	prefetchGate   chan struct{}

	activeCancelFunc context.CancelFunc
	mu               sync.Mutex
}

// New creates and returns a new ResourceManager.
func New() *ResourceManager {
	return &ResourceManager{
		liveStreamGate: make(chan struct{}, 1),
		prefetchGate:   make(chan struct{}, 1),
	}
}

// AcquireLiveStream forcefully cancels any existing live stream, then acquires
// the gate. Returns a streamCtx (tied to both the HTTP request ctx and manual
// override) and a release func that must be deferred by the caller.
func (rm *ResourceManager) AcquireLiveStream(ctx context.Context) (context.Context, context.CancelFunc) {
	rm.mu.Lock()
	if rm.activeCancelFunc != nil {
		rm.activeCancelFunc() // kill previous live stream immediately
	}
	streamCtx, cancel := context.WithCancel(ctx)
	rm.activeCancelFunc = cancel
	rm.mu.Unlock()

	rm.liveStreamGate <- struct{}{} // acquire (near-instant after kill above)

	releaseFunc := func() {
		cancel()
		<-rm.liveStreamGate
	}
	return streamCtx, releaseFunc
}

// AcquireLiveStreamCancellable is like AcquireLiveStream but returns the cancel
// function separately from the gate-release function.
// This allows the stream context to be cancelled externally (e.g. Pause) without
// immediately releasing the gate semaphore.
//
//	- cancelFn  cancels the stream context (safe to call multiple times)
//	- releaseFn releases the gate; must be deferred by the caller
func (rm *ResourceManager) AcquireLiveStreamCancellable(ctx context.Context) (streamCtx context.Context, cancelFn context.CancelFunc, releaseFn func()) {
	rm.mu.Lock()
	if rm.activeCancelFunc != nil {
		rm.activeCancelFunc()
	}
	streamCtx, cancelFn = context.WithCancel(ctx)
	rm.activeCancelFunc = cancelFn
	rm.mu.Unlock()

	rm.liveStreamGate <- struct{}{}

	releaseFn = func() {
		cancelFn()
		<-rm.liveStreamGate
	}
	return streamCtx, cancelFn, releaseFn
}

// AcquirePrefetch blocks until the prefetch gate is free and the live stream
// gate is not holding a cache-miss slot. Returns a release func.
// Must respect the passed ctx for cancellation.
func (rm *ResourceManager) AcquirePrefetch(ctx context.Context) (context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if rm.IsLiveStreamActive() {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ticker.C:
				continue
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case rm.prefetchGate <- struct{}{}:
			// Double check if live stream active just after acquiring, to avoid race conditions.
			if rm.IsLiveStreamActive() {
				<-rm.prefetchGate
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ticker.C:
					continue
				}
			}
			// In case the context got cancelled but select chose this case, check again.
			if err := ctx.Err(); err != nil {
				<-rm.prefetchGate
				return nil, err
			}
			releaseFunc := func() {
				<-rm.prefetchGate
			}
			return releaseFunc, nil
		case <-ticker.C:
			// Just loop and check IsLiveStreamActive again
		}
	}
}

// IsLiveStreamActive returns true if the live stream gate is currently held.
func (rm *ResourceManager) IsLiveStreamActive() bool {
	return len(rm.liveStreamGate) > 0
}
