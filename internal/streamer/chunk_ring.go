package streamer

import (
	"context"
	"errors"
	"sync"
)

// ErrEvicted is returned by ChunkRing.Get when the requested chunk has been
// evicted from the sliding window (it's too old). The caller should respond
// with HTTP 410 Gone.
var ErrEvicted = errors.New("chunk evicted from ring buffer")

// ErrDone is returned by ChunkRing.Get when the producer has finished and the
// requested chunk index is past the end of the stream. The caller should
// respond with HTTP 204 No Content.
var ErrDone = errors.New("stream finished")

// Chunk holds one fixed-size slice of raw PCM audio together with its
// monotonically increasing index.
type Chunk struct {
	Index  uint32
	Data   []byte // len ≤ ChunkSize; last chunk of a track may be shorter
	IsLast bool   // true when producer hits io.EOF — signals X-Last-Chunk: true
}

// ChunkRing is a fixed-capacity circular buffer of Chunks.
//
// Producer (ffmpeg goroutine) calls Add() to append chunks sequentially.
// When the ring is full the oldest chunk is silently evicted to make room.
//
// Consumers (HTTP chunk handlers) call Get(index) which blocks until the
// requested chunk is available, or returns ErrEvicted / ErrDone.
type ChunkRing struct {
	mu        sync.Mutex
	cond      *sync.Cond
	slots     []Chunk // len == capacity, used as circular array
	capacity  int
	nextWrite uint32 // absolute index of the next chunk to be written
	filled    int    // number of valid slots currently occupied
	done      bool   // producer finished (ffmpeg exited)
}

// newChunkRing creates a ring with the configured WindowSize capacity.
func newChunkRing() *ChunkRing {
	r := &ChunkRing{
		slots:    make([]Chunk, WindowSize),
		capacity: WindowSize,
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Add stores a chunk produced by ffmpeg. If the ring is full the oldest chunk
// is overwritten (evicted). Safe to call concurrently with Get.
func (r *ChunkRing) Add(chunk Chunk) {
	r.mu.Lock()
	slot := int(chunk.Index) % r.capacity
	r.slots[slot] = chunk
	r.nextWrite = chunk.Index + 1
	if r.filled < r.capacity {
		r.filled++
	}
	r.cond.Broadcast()
	r.mu.Unlock()
}

// Done signals that the producer has finished. All blocked Get calls will
// wake and re-evaluate their conditions.
func (r *ChunkRing) Done() {
	r.mu.Lock()
	r.done = true
	r.cond.Broadcast()
	r.mu.Unlock()
}

// Get blocks until chunk index is available in the ring window, then returns
// it. Returns ErrEvicted if the chunk was already overwritten, or ErrDone if
// the producer finished before producing that chunk.
//
// ctx is checked on each wake-up so that cancelled requests return promptly.
func (r *ChunkRing) Get(ctx context.Context, index uint32) (Chunk, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		// Context cancelled (e.g. ESP closed connection)
		if ctx.Err() != nil {
			return Chunk{}, ctx.Err()
		}

		if r.filled > 0 {
			windowStart := r.nextWrite - uint32(r.filled)

			// Chunk has been evicted
			if index < windowStart {
				return Chunk{}, ErrEvicted
			}

			// Chunk is available in the window
			if index < r.nextWrite {
				slot := int(index) % r.capacity
				return r.slots[slot], nil
			}
		}

		// Producer finished before this index was produced
		if r.done && index >= r.nextWrite {
			return Chunk{}, ErrDone
		}

		// Wait for the next Add() or Done() call
		r.cond.Wait()
	}
}

// WindowStart returns the absolute index of the oldest available chunk.
// Returns 0 if the ring is empty.
func (r *ChunkRing) WindowStart() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.filled == 0 {
		return 0
	}
	return r.nextWrite - uint32(r.filled)
}

// NextWrite returns the index of the next chunk that will be written.
func (r *ChunkRing) NextWrite() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nextWrite
}

// IsDone reports whether the producer has finished.
func (r *ChunkRing) IsDone() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}
