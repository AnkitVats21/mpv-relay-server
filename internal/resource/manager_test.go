package resource

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestAcquireLiveStream_Concurrency(t *testing.T) {
	rm := New()
	ctx := context.Background()

	ctx1, release1 := rm.AcquireLiveStream(ctx)
	if !rm.IsLiveStreamActive() {
		t.Fatal("expected live stream to be active")
	}

	// In a real HTTP handler, the release function is deferred and runs when the handler exits (which happens when context is cancelled)
	go func() {
		<-ctx1.Done()
		release1()
	}()

	// Verify that the second acquisition cancels the first and acquires the gate
	ctx2, release2 := rm.AcquireLiveStream(ctx)

	select {
	case <-ctx1.Done():
		// first stream was cancelled, as expected
	case <-time.After(1 * time.Second):
		t.Fatal("expected first stream context to be cancelled")
	}

	select {
	case <-ctx2.Done():
		t.Fatal("expected second stream context to not be cancelled")
	default:
		// as expected, second stream is not cancelled
	}

	if !rm.IsLiveStreamActive() {
		t.Fatal("expected live stream to still be active after second acquisition")
	}

	// Release second stream, now it should not be active
	release2()

	if rm.IsLiveStreamActive() {
		t.Fatal("expected live stream to not be active after release")
	}
}

func TestAcquirePrefetch_Cancellation(t *testing.T) {
	rm := New()

	// Cancel context beforehand
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rm.AcquirePrefetch(ctx)
	if err == nil {
		t.Fatal("expected AcquirePrefetch to return an error with cancelled context")
	}
}

func TestAcquirePrefetch_BlocksOnLiveStream(t *testing.T) {
	rm := New()
	ctx := context.Background()

	// 1. Acquire live stream
	_, releaseLive := rm.AcquireLiveStream(ctx)
	defer releaseLive()

	// 2. Try to acquire prefetch - should block or fail because live stream is active
	prefetchCtx, cancelPrefetch := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancelPrefetch()

	_, err := rm.AcquirePrefetch(prefetchCtx)
	if err == nil {
		t.Fatal("expected AcquirePrefetch to fail or time out because live stream is active")
	}

	// 3. Release live stream in background, then acquire prefetch
	rm2 := New()
	_, releaseLive2 := rm2.AcquireLiveStream(ctx)

	acquiredChan := make(chan struct{})
	go func() {
		releasePrefetch, err := rm2.AcquirePrefetch(ctx)
		if err != nil {
			t.Errorf("failed to acquire prefetch: %v", err)
			return
		}
		defer releasePrefetch()
		close(acquiredChan)
	}()

	// Wait a bit to ensure it is blocked
	select {
	case <-acquiredChan:
		t.Fatal("prefetch acquired while live stream was active")
	case <-time.After(150 * time.Millisecond):
		// still blocked, good
	}

	// Now release live stream
	releaseLive2()

	// It should acquire now
	select {
	case <-acquiredChan:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for prefetch to acquire after live stream released")
	}
}

func TestIsLiveStreamActive_ConcurrentReads(t *testing.T) {
	rm := New()
	ctx := context.Background()

	_, release := rm.AcquireLiveStream(ctx)
	defer release()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = rm.IsLiveStreamActive()
			}
		}()
	}
	wg.Wait()
}
