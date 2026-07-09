package eviction

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/ankitm/mpv-relay/internal/db"
)

// ActiveTrackProvider defines an interface to get the currently active video ID.
type ActiveTrackProvider interface {
	GetActiveVideoID() string
}

// Worker monitors the media cache size and evicts older files when space limits are exceeded.
type Worker struct {
	db            *db.DB
	maxBytes      int64         // from config: CACHE_MAX_BYTES
	checkInterval time.Duration // default: 10 minutes
	log           *slog.Logger
	activeTrack   ActiveTrackProvider
}

// New creates a new eviction Worker.
func New(db *db.DB, maxBytes int64) *Worker {
	return &Worker{
		db:            db,
		maxBytes:      maxBytes,
		checkInterval: 10 * time.Minute,
		log:           slog.Default().With("pkg", "eviction"),
	}
}

// SetActiveTrackProvider configures the provider of the currently active track/video ID.
func (w *Worker) SetActiveTrackProvider(provider ActiveTrackProvider) {
	w.activeTrack = provider
}

// Start launches the background eviction loop. Blocks until ctx is cancelled.
func (w *Worker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, _, err := w.RunOnce(ctx); err != nil {
				w.log.Error("eviction pass failed", "err", err)
			}
		}
	}
}

// RunOnce performs a single eviction pass. Can be called manually via MQTT clear_cache.
// Returns number of files deleted and bytes freed.
func (w *Worker) RunOnce(ctx context.Context) (filesDeleted int, bytesFreed int64, err error) {
	// 1. Query db.GetCacheTotalSize() → sum of file_size_bytes
	total, err := w.db.GetCacheTotalSize()
	if err != nil {
		return 0, 0, err
	}

	// 2. If total <= maxBytes: return early (nothing to do)
	if total <= w.maxBytes {
		return 0, 0, nil
	}

	// 3. excess = total - w.maxBytes
	excess := total - w.maxBytes

	// 4. rows = db.GetLRUMediaFiles(excess)   // returns oldest files until cumulative size >= excess
	rows, err := w.db.GetLRUMediaFiles(excess)
	if err != nil {
		return 0, 0, err
	}

	var activeVideoID string
	if w.activeTrack != nil {
		activeVideoID = w.activeTrack.GetActiveVideoID()
	}

	w.log.Info("Running cache eviction pass",
		"totalBytes", total,
		"maxBytes", w.maxBytes,
		"excessBytes", excess,
		"candidateCount", len(rows),
		"activeVideoID", activeVideoID,
	)

	// 5. For each row:
	for _, row := range rows {
		// Do not evict a file whose video_id matches the currently active Video ID
		if activeVideoID != "" && row.VideoID == activeVideoID {
			w.log.Info("Skipping eviction of active video ID", "videoID", row.VideoID)
			continue
		}

		// a. os.Remove(row.FilePath)
		if row.FilePath != "" {
			if err := os.Remove(row.FilePath); err != nil {
				if os.IsNotExist(err) {
					w.log.Warn("Evicted file already gone from disk", "filePath", row.FilePath, "videoID", row.VideoID)
				} else {
					w.log.Warn("Failed to delete file from disk during eviction", "filePath", row.FilePath, "err", err)
				}
			}
		}

		// b. db.DeleteCacheByVideoID(row.VideoID)
		if err := w.db.DeleteCacheByVideoID(row.VideoID); err != nil {
			w.log.Error("Failed to delete DB cache entry during eviction", "videoID", row.VideoID, "err", err)
			continue
		}

		// c. Accumulate counters
		filesDeleted++
		bytesFreed += row.FileSizeBytes
	}

	w.log.Info("Cache eviction pass completed", "filesDeleted", filesDeleted, "bytesFreed", bytesFreed)
	return filesDeleted, bytesFreed, nil
}
