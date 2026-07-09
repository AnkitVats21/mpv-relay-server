// Package queue manages the play queue, autoplay pool, and background caching.
package queue

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ankitm/mpv-relay/internal/config"
	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/mpv"
	"github.com/ankitm/mpv-relay/internal/resolver"
	"github.com/ankitm/mpv-relay/internal/resource"
)

// StreamerInterface avoids circular imports between streamer and queue.
type StreamerInterface interface {
	StartStream(videoID, title string) error
}

// QueueItem is what we expose over MQTT for queue listings.
type QueueItem struct {
	Position     int    `json:"position"`
	VideoID      string `json:"video_id"`
	Title        string `json:"title"`
	Uploader     string `json:"uploader"`
	Duration     int    `json:"duration"`
	ThumbnailURL string `json:"thumbnail_url"`
	Cached       bool   `json:"cached"`
	Source       string `json:"source,omitempty"` // "queue" | "autoplay"
}

// Manager is the central play queue with autoplay pool.
type Manager struct {
	mpv      *mpv.Client
	resolver *resolver.Resolver
	db       *db.DB
	cfg      *config.Config
	publish  func(map[string]any) // mqtt publish_fn
	log      *slog.Logger

	streamer StreamerInterface
	rm       *resource.ResourceManager

	mu             sync.Mutex
	current        *resolver.ResolvedTrack
	recordPath     string
	autoplayOn     bool
	autoplayPool   []*resolver.ResolvedTrack
	nextAutoplay   *resolver.ResolvedTrack
	historyStack              []string
	isNavBack                 bool
	wasPlayingBeforeAssistant bool
	assistantActive           bool

	dlMu    sync.Mutex
	dlQueue []*resolver.ResolvedTrack
	dlBusy  bool
}

// New creates a Manager.
func New(m *mpv.Client, res *resolver.Resolver, database *db.DB, cfg *config.Config, publish func(map[string]any), rm *resource.ResourceManager) *Manager {
	mgr := &Manager{
		mpv:        m,
		resolver:   res,
		db:         database,
		cfg:        cfg,
		publish:    publish,
		autoplayOn: true,
		log:        slog.Default().With("pkg", "queue"),
		rm:         rm,
	}
	go mgr.startPrefetchWorker()
	return mgr
}

// SetStreamer sets the streamer dependency.
func (m *Manager) SetStreamer(s StreamerInterface) {
	m.mu.Lock()
	m.streamer = s
	m.mu.Unlock()
}

// ── Autoplay ──────────────────────────────────────────────────────────────────

func (m *Manager) SetAutoplay(enabled bool) {
	m.mu.Lock()
	m.autoplayOn = enabled
	if !enabled {
		m.autoplayPool = nil
		m.nextAutoplay = nil
	}
	m.mu.Unlock()
	m.log.Info("Autoplay set", "enabled", enabled)
}

func (m *Manager) IsAutoplayEnabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.autoplayOn
}

// ── State Machine Transitions ──────────────────────────────────────────────────

func (m *Manager) advanceQueue() {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, err := m.db.GetNextPending()
	if err != nil {
		m.log.Error("advanceQueue: failed to get next pending track", "err", err)
		return
	}
	if entry == nil {
		m.log.Info("advanceQueue: no more pending tracks in queue")
		if m.autoplayOn && len(m.autoplayPool) > 0 {
			next := m.autoplayPool[0]
			m.autoplayPool = m.autoplayPool[1:]
			if len(m.autoplayPool) > 0 {
				m.nextAutoplay = m.autoplayPool[0]
			} else {
				m.nextAutoplay = nil
			}
			m.log.Info("advanceQueue: auto-advancing via AUTOPLAY", "title", next.Title)
			m.mu.Unlock()
			go m.playTrackFromAutoplay(next)
			m.mu.Lock()
		} else {
			m.log.Info("advanceQueue: entering idle state")
			m.current = nil
			m.mu.Unlock()
			m.PublishStatus()
			m.mu.Lock()
		}
		return
	}

	m.log.Info("advanceQueue: playing next track from queue", "videoID", entry.VideoID, "title", entry.Title)
	m.mu.Unlock()
	go m.playQueueEntry(entry)
	m.mu.Lock()
}

func (m *Manager) markPlaying(id int64) {
	_ = m.db.SetQueueStatus(id, "PLAYING")
	_ = m.db.SetQueueStarted(id)
}

func (m *Manager) markCompleted(id int64) {
	_ = m.db.SetQueueStatus(id, "COMPLETED")
	m.advanceQueue()
}

func (m *Manager) markFailed(id int64) {
	_ = m.db.SetQueueStatus(id, "FAILED")
	if m.publish != nil {
		m.publish(map[string]any{
			"type":    "error",
			"message": "Playback failed",
		})
	}
	m.advanceQueue()
}

// ── Public API ────────────────────────────────────────────────────────────────

// PlayNow resolves a query and starts playing immediately (clears queue + pool).
func (m *Manager) PlayNow(query string, download bool) *resolver.ResolvedTrack {
	track, err := m.resolver.Resolve(query)
	if err != nil || track == nil {
		m.log.Error("PlayNow: could not resolve query", "query", query, "err", err)
		return nil
	}
	if !download {
		track.SkipDownload = true
	}
	if track.Duration > 1200 {
		m.log.Warn("PlayNow: Ignoring track exceeding 20 minutes", "title", track.Title, "duration", track.Duration)
		if m.publish != nil {
			m.publish(map[string]any{"type": "error", "message": "Tracks longer than 20 minutes are not allowed."})
		}
		return nil
	}

	m.mu.Lock()
	m.autoplayPool = nil
	m.nextAutoplay = nil
	m.wasPlayingBeforeAssistant = false
	if m.current != nil && !m.isNavBack {
		m.historyStack = append(m.historyStack, m.current.Query)
	}
	m.isNavBack = false
	m.mu.Unlock()

	// Cancel any current PLAYING track
	playing, err := m.db.GetPlayingEntry()
	if err == nil && playing != nil {
		m.log.Info("PlayNow: cancelling current playing track", "id", playing.ID, "title", playing.Title)
		_ = m.db.SetQueueStatus(playing.ID, "COMPLETED")
	}

	// Insert row into playback_queue with status PENDING
	entry := db.QueueEntry{
		VideoID: track.VideoID,
		Title:   track.Title,
		Status:  "PENDING",
		Source:  "web",
		AddedAt: time.Now(),
	}
	id, err := m.db.EnqueueTrack(entry)
	if err != nil {
		m.log.Error("PlayNow: failed to enqueue track", "err", err)
		return nil
	}
	entry.ID = id

	// If cached on disk -> update status to READY
	cached := false
	if track.FilePath != "" && fileExists(track.FilePath) {
		cached = true
	} else {
		if row, err := m.db.LookupMediaCache(track.VideoID); err == nil && row != nil && row.FilePath != "" {
			if fileExists(row.FilePath) {
				cached = true
				track.FilePath = row.FilePath
			}
		}
	}
	if cached {
		_ = m.db.SetQueueStatus(entry.ID, "READY")
		entry.Status = "READY"
	}

	// Call m.streamer.StartStream
	err = m.streamer.StartStream(track.VideoID, track.Title)
	if err != nil {
		m.log.Error("PlayNow: streamer.StartStream failed", "err", err)
		_ = m.db.SetQueueStatus(entry.ID, "FAILED")
		if m.publish != nil {
			m.publish(map[string]any{
				"type":    "error",
				"message": "Playback failed to start",
			})
		}
		return nil
	}

	m.markPlaying(entry.ID)

	m.mu.Lock()
	m.current = track
	m.mu.Unlock()

	_ = m.db.IncrementPlayCount(track.Query)
	_ = m.db.SavePlayHistory(track.Query, track.Title)

	if m.publish != nil {
		m.publish(map[string]any{
			"type":           "playing",
			"title":          track.Title,
			"uploader":       track.Uploader,
			"duration":       track.Duration,
			"query":          track.Query,
			"thumbnail_path": track.ThumbnailPath,
			"thumbnail_url":  track.ThumbnailURL,
			"related_videos": track.RelatedVideos,
		})
	}
	m.PublishStatus()

	go m.monitorStreamCompletion(entry.ID)
	go m.enrichRecommendations(track)

	return track
}

// QueueAdd appends a query to the end of the queue.
func (m *Manager) QueueAdd(query string, download bool) {
	go func() {
		if m.publish != nil {
			m.publish(map[string]any{"type": "resolving", "query": query})
		}

		track, err := m.resolver.Resolve(query)
		if err != nil || track == nil {
			m.log.Error("QueueAdd: could not resolve query", "query", query)
			if m.publish != nil {
				m.publish(map[string]any{"type": "error", "message": "Could not resolve query: " + query})
			}
			return
		}
		if !download {
			track.SkipDownload = true
		}
		if track.Duration > 1200 {
			m.log.Warn("QueueAdd: Ignoring track exceeding 20 minutes", "title", track.Title, "duration", track.Duration)
			if m.publish != nil {
				m.publish(map[string]any{"type": "error", "message": "Tracks longer than 20 minutes are not allowed."})
			}
			return
		}

		entry := db.QueueEntry{
			VideoID: track.VideoID,
			Title:   track.Title,
			Status:  "PENDING",
			Source:  "web",
			AddedAt: time.Now(),
		}

		cached := false
		if track.FilePath != "" && fileExists(track.FilePath) {
			cached = true
		} else {
			if row, err := m.db.LookupMediaCache(track.VideoID); err == nil && row != nil && row.FilePath != "" {
				if fileExists(row.FilePath) {
					cached = true
					track.FilePath = row.FilePath
				}
			}
		}
		if cached {
			entry.Status = "READY"
		}

		_, err = m.db.EnqueueTrack(entry)
		if err != nil {
			m.log.Error("QueueAdd: failed to enqueue track", "err", err)
			return
		}

		if m.publish != nil {
			m.publish(map[string]any{
				"type":            "queued",
				"title":           track.Title,
				"query":           query,
				"video_id":        track.VideoID,
				"insert_at_front": false,
			})
		}
		m.PublishQueueInfo()

		playing, err := m.db.GetPlayingEntry()
		if err == nil && playing == nil {
			m.log.Info("QueueAdd: nothing playing, advancing queue immediately")
			m.advanceQueue()
		}
	}()
}

// PlayNext prepends a query to the front of the queue.
func (m *Manager) PlayNext(query string, download bool) {
	go func() {
		if m.publish != nil {
			m.publish(map[string]any{"type": "resolving", "query": query})
		}

		track, err := m.resolver.Resolve(query)
		if err != nil || track == nil {
			m.log.Error("PlayNext: could not resolve query", "query", query)
			if m.publish != nil {
				m.publish(map[string]any{"type": "error", "message": "Could not resolve query: " + query})
			}
			return
		}
		if !download {
			track.SkipDownload = true
		}
		if track.Duration > 1200 {
			m.log.Warn("PlayNext: Ignoring track exceeding 20 minutes", "title", track.Title, "duration", track.Duration)
			if m.publish != nil {
				m.publish(map[string]any{"type": "error", "message": "Tracks longer than 20 minutes are not allowed."})
			}
			return
		}

		entry := db.QueueEntry{
			VideoID: track.VideoID,
			Title:   track.Title,
			Status:  "PENDING",
			Source:  "web",
			AddedAt: time.Now(),
		}

		cached := false
		if track.FilePath != "" && fileExists(track.FilePath) {
			cached = true
		} else {
			if row, err := m.db.LookupMediaCache(track.VideoID); err == nil && row != nil && row.FilePath != "" {
				if fileExists(row.FilePath) {
					cached = true
					track.FilePath = row.FilePath
				}
			}
		}
		if cached {
			entry.Status = "READY"
		}

		_, err = m.db.EnqueueTrackAtFront(entry)
		if err != nil {
			m.log.Error("PlayNext: failed to enqueue track at front", "err", err)
			return
		}

		if m.publish != nil {
			m.publish(map[string]any{
				"type":            "queued",
				"title":           track.Title,
				"query":           query,
				"video_id":        track.VideoID,
				"insert_at_front": true,
			})
		}
		m.PublishQueueInfo()

		playing, err := m.db.GetPlayingEntry()
		if err == nil && playing == nil {
			m.log.Info("PlayNext: nothing playing, advancing queue immediately")
			m.advanceQueue()
		}
	}()
}

// Skip stops the current track (forces completion and advances).
func (m *Manager) Skip() {
	m.mu.Lock()
	m.wasPlayingBeforeAssistant = false
	m.mu.Unlock()

	playing, err := m.db.GetPlayingEntry()
	if err == nil && playing != nil {
		m.log.Info("Skip: marking playing entry as COMPLETED", "id", playing.ID, "title", playing.Title)
		_ = m.db.SetQueueStatus(playing.ID, "COMPLETED")
	}

	ctx, cancel := context.WithCancel(context.Background())
	_, release := m.rm.AcquireLiveStream(ctx)
	release()
	cancel()

	m.advanceQueue()
}

// ClearQueue removes all pending items.
func (m *Manager) ClearQueue() {
	_ = m.db.ClearQueue()
	m.log.Info("Queue cleared")
}

// StopAll clears the queue, autoplay pool, history, and stops active streams.
func (m *Manager) StopAll() {
	m.mu.Lock()
	m.current = nil
	m.autoplayPool = nil
	m.nextAutoplay = nil
	m.historyStack = nil
	m.recordPath = ""
	m.wasPlayingBeforeAssistant = false
	m.assistantActive = false
	m.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	_, release := m.rm.AcquireLiveStream(ctx)
	release()
	cancel()

	playing, err := m.db.GetPlayingEntry()
	if err == nil && playing != nil {
		_ = m.db.SetQueueStatus(playing.ID, "COMPLETED")
	}

	_ = m.db.ClearQueue()

	m.log.Info("StopAll: queue, autoplay pool, and history cleared")
	m.PublishStatus()
}

// Shuffle randomises the pending queue.
func (m *Manager) Shuffle() {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries, err := m.db.ListQueue()
	if err != nil {
		m.log.Error("Shuffle: failed to list queue", "err", err)
		return
	}

	var shuffleList []db.QueueEntry
	for _, entry := range entries {
		if entry.Status == "PENDING" || entry.Status == "READY" {
			shuffleList = append(shuffleList, entry)
		}
	}

	if len(shuffleList) <= 1 {
		m.log.Info("Shuffle: not enough tracks to shuffle")
		return
	}

	rand.Shuffle(len(shuffleList), func(i, j int) {
		shuffleList[i], shuffleList[j] = shuffleList[j], shuffleList[i]
	})

	_ = m.db.ClearQueue()

	for _, entry := range shuffleList {
		_, _ = m.db.EnqueueTrack(entry)
	}

	m.log.Info("Shuffle: queue shuffled in DB", "count", len(shuffleList))
}

// ListQueue returns a snapshot of the pending queue items.
func (m *Manager) ListQueue() []QueueItem {
	entries, err := m.db.ListQueue()
	if err != nil {
		m.log.Error("ListQueue: failed to list queue from DB", "err", err)
		return nil
	}
	items := make([]QueueItem, 0, len(entries))
	for i, entry := range entries {
		var uploader, thumbURL string
		var duration int
		var cached bool
		if song, err := m.db.LookupVideoID(entry.VideoID); err == nil && song != nil {
			uploader = song.Uploader
			duration = song.Duration
			thumbURL = song.ThumbnailURL
			cached = song.FilePath != "" && fileExists(song.FilePath)
		}
		items = append(items, QueueItem{
			Position:     i + 1,
			VideoID:      entry.VideoID,
			Title:        entry.Title,
			Uploader:     uploader,
			Duration:     duration,
			ThumbnailURL: thumbURL,
			Cached:       cached,
			Source:       entry.Source,
		})
	}
	return items
}

// CurrentTrack returns the currently playing track.
func (m *Manager) CurrentTrack() *resolver.ResolvedTrack {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

// Previous plays the previously played track from the history stack.
func (m *Manager) Previous() {
	m.mu.Lock()
	m.wasPlayingBeforeAssistant = false
	m.mu.Unlock()

	history, err := m.db.GetHistory(2)
	if err != nil || len(history) < 2 {
		m.log.Info("Previous: no previous track in history")
		return
	}
	prev := history[1]

	playing, err := m.db.GetPlayingEntry()
	if err == nil && playing != nil {
		_ = m.db.SetQueueStatus(playing.ID, "COMPLETED")
		
		cached := false
		if song, err := m.db.LookupVideoID(playing.VideoID); err == nil && song != nil {
			if song.FilePath != "" && fileExists(song.FilePath) {
				cached = true
			}
		}
		status := "PENDING"
		if cached {
			status = "READY"
		}
		
		reEnqueueCurrent := db.QueueEntry{
			VideoID: playing.VideoID,
			Title:   playing.Title,
			Status:  status,
			Source:  playing.Source,
			AddedAt: time.Now(),
		}
		_, _ = m.db.EnqueueTrackAtFront(reEnqueueCurrent)
	}

	prevEntry := db.QueueEntry{
		VideoID: prev.VideoID,
		Title:   prev.Title,
		Status:  "PENDING",
		Source:  "web",
		AddedAt: time.Now(),
	}
	_, err = m.db.EnqueueTrackAtFront(prevEntry)
	if err != nil {
		m.log.Error("Previous: failed to enqueue previous track at front", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	_, release := m.rm.AcquireLiveStream(ctx)
	release()
	cancel()

	m.advanceQueue()
}

// PublishPlaybackState broadcasts only the playback metrics.
func (m *Manager) PublishPlaybackState() {
	if m.publish == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("PublishPlaybackState panic", "err", r)
		}
	}()

	status := m.mpv.GetStatus()
	m.publish(map[string]any{
		"type":     "status",
		"state":    status.State,
		"position": status.Position,
		"duration": status.Duration,
		"volume":   status.Volume,
	})
}

// PublishTrackInfo broadcasts current track details.
func (m *Manager) PublishTrackInfo() {
	if m.publish == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("PublishTrackInfo panic", "err", r)
		}
	}()

	payload := map[string]any{
		"type": "status",
	}

	current := m.CurrentTrack()
	if current != nil {
		payload["title"] = current.Title
		payload["uploader"] = current.Uploader
		payload["thumbnail_path"] = current.ThumbnailPath
		payload["thumbnail_url"] = current.ThumbnailURL

		related := make([]map[string]any, 0, len(current.RelatedVideos))
		for _, v := range current.RelatedVideos {
			isCached := false
			if row, err := m.db.LookupVideoID(v.ID); err == nil && row != nil {
				isCached = row.FilePath != "" && fileExists(row.FilePath)
			}
			related = append(related, map[string]any{
				"id":       v.ID,
				"title":    v.Title,
				"uploader": v.Uploader,
				"duration": v.Duration,
				"cached":   isCached,
			})
		}
		payload["related_videos"] = related
	} else {
		payload["title"] = "No Track Playing"
		payload["uploader"] = "Unknown Uploader"
		payload["thumbnail_path"] = ""
		payload["thumbnail_url"] = nil
		payload["related_videos"] = []any{}
	}

	m.publish(payload)
}

// PublishQueueInfo broadcasts manual queue and autoplay pool items.
func (m *Manager) PublishQueueInfo() {
	if m.publish == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("PublishQueueInfo panic", "err", r)
		}
	}()

	m.mu.Lock()
	nextAP := m.nextAutoplay
	poolSnap := append([]*resolver.ResolvedTrack(nil), m.autoplayPool...)
	autoplayOn := m.autoplayOn
	m.mu.Unlock()

	qItems := m.ListQueue()
	qLen := len(qItems)

	payload := map[string]any{
		"type":         "status",
		"queue_length": qLen,
		"autoplay":     autoplayOn,
	}

	if nextAP != nil {
		payload["next_autoplay"] = map[string]any{
			"title":    nextAP.Title,
			"video_id": nextAP.VideoID,
			"cached":   nextAP.FilePath != "" && fileExists(nextAP.FilePath),
		}
	} else {
		payload["next_autoplay"] = nil
	}

	qIDs := map[string]bool{}
	upNext := make([]map[string]any, 0, len(qItems)+len(poolSnap))
	for _, qi := range qItems {
		qIDs[qi.VideoID] = true
		item := map[string]any{
			"position":      qi.Position,
			"video_id":      qi.VideoID,
			"title":         qi.Title,
			"uploader":      qi.Uploader,
			"duration":      qi.Duration,
			"thumbnail_url": qi.ThumbnailURL,
			"cached":        qi.Cached,
			"source":        "queue",
		}
		upNext = append(upNext, item)
	}
	for _, t := range poolSnap {
		if qIDs[t.VideoID] {
			continue
		}
		upNext = append(upNext, map[string]any{
			"position":      len(upNext) + 1,
			"video_id":      t.VideoID,
			"title":         t.Title,
			"uploader":      t.Uploader,
			"duration":      t.Duration,
			"thumbnail_url": t.ThumbnailURL,
			"cached":        t.FilePath != "" && fileExists(t.FilePath),
			"source":        "autoplay",
		})
	}
	payload["up_next"] = upNext

	m.publish(payload)
}

// PublishStatus broadcasts all status updates.
func (m *Manager) PublishStatus() {
	m.PublishPlaybackState()
	m.PublishTrackInfo()
	m.PublishQueueInfo()
}

// ── Download queue (Legacy Sequential Handler) ──────────────────────────────

func (m *Manager) AddToDownloadQueue(videoID string) {
	m.log.Info("Download request received", "videoID", videoID)
	go func() {
		var track *resolver.ResolvedTrack
		row, err := m.db.LookupVideoID(videoID)
		if err == nil && row != nil {
			track = dbRowToTrack(row)
		} else {
			u := "https://www.youtube.com/watch?v=" + videoID
			track, _ = m.resolver.Resolve(u)
		}

		if track == nil {
			m.log.Error("Could not resolve video for download", "videoID", videoID)
			if m.publish != nil {
				m.publish(map[string]any{"type": "download_failed", "video_id": videoID, "error": "Could not resolve"})
			}
			return
		}

		if track.FilePath != "" && fileExists(track.FilePath) {
			if m.publish != nil {
				m.publish(map[string]any{"type": "download_completed", "video_id": videoID, "success": true, "already_cached": true})
			}
			return
		}

		m.dlMu.Lock()
		for _, t := range m.dlQueue {
			if t.VideoID == track.VideoID {
				m.dlMu.Unlock()
				m.log.Info("Video already in download queue", "videoID", videoID)
				return
			}
		}
		m.dlQueue = append(m.dlQueue, track)
		m.dlMu.Unlock()

		if m.publish != nil {
			m.publish(map[string]any{"type": "download_queued", "video_id": videoID, "title": track.Title})
		}
		m.processNextDownload()
	}()
}

func (m *Manager) processNextDownload() {
	m.dlMu.Lock()
	if m.dlBusy || len(m.dlQueue) == 0 {
		m.dlMu.Unlock()
		return
	}
	track := m.dlQueue[0]
	m.dlQueue = m.dlQueue[1:]
	m.dlBusy = true
	m.dlMu.Unlock()

	m.log.Info("Starting sequential download", "videoID", track.VideoID, "title", track.Title)
	if m.publish != nil {
		m.publish(map[string]any{"type": "download_started", "video_id": track.VideoID, "title": track.Title})
	}

	m.resolver.StartBackgroundDownload(track, func(success bool) {
		m.dlMu.Lock()
		m.dlBusy = false
		m.dlMu.Unlock()
		if m.publish != nil {
			m.publish(map[string]any{"type": "download_completed", "video_id": track.VideoID, "success": success})
		}
		m.PublishStatus()
		m.processNextDownload()
	})
}

// SearchSongs executes a YouTube search and publishes results.
func (m *Manager) SearchSongs(query string) {
	go func() {
		results, err := m.resolver.SearchYouTube(query, 5)
		if err != nil {
			m.log.Error("Search failed", "query", query, "err", err)
			return
		}
		items := make([]map[string]any, 0, len(results))
		for _, r := range results {
			items = append(items, map[string]any{
				"id": r.ID, "title": r.Title,
				"uploader": r.Uploader, "duration": r.Duration,
			})
		}
		if m.publish != nil {
			m.publish(map[string]any{"type": "search_results", "query": query, "results": items})
		}
	}()
}

// ── Background Prefetch Worker ───────────────────────────────────────────────

func (m *Manager) startPrefetchWorker() {
	m.log.Info("Starting background prefetch worker")
	for {
		entry, err := m.db.GetNextForPrefetch()
		if err != nil {
			m.log.Error("Prefetch worker: failed to get next pending track", "err", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if entry == nil {
			time.Sleep(5 * time.Second)
			continue
		}

		if err := m.db.SetQueueStatus(entry.ID, "PREFETCHING"); err != nil {
			m.log.Error("Prefetch worker: failed to set status to PREFETCHING", "id", entry.ID, "err", err)
			time.Sleep(5 * time.Second)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		release, err := m.rm.AcquirePrefetch(ctx)
		cancel()
		if err != nil {
			m.log.Info("Prefetch worker: resource manager busy or live stream active, skipping prefetch for now", "videoID", entry.VideoID)
			_ = m.db.SetQueueStatus(entry.ID, "PENDING")
			time.Sleep(5 * time.Second)
			continue
		}

		m.log.Info("Prefetch worker: downloading track", "videoID", entry.VideoID, "title", entry.Title)
		outputPath := filepath.Join(m.cfg.MediaDir, entry.VideoID+".%(ext)s")

		cmd := exec.Command("nice", "-n", "10", m.resolver.YtDlpBin(), "-f", "bestaudio", "-o", outputPath, "--", entry.VideoID)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		err = cmd.Run()
		if err != nil {
			m.log.Error("Prefetch worker: download failed", "videoID", entry.VideoID, "err", err, "stderr", stderr.String())
			_ = m.db.SetQueueStatus(entry.ID, "FAILED")
			if m.publish != nil {
				m.publish(map[string]any{
					"type":     "PREFETCH_FAILED",
					"cmd":      "PREFETCH_FAILED",
					"video_id": entry.VideoID,
					"error":    err.Error(),
				})
			}
			release()
			continue
		}

		filePath := findDownloadedFile(m.cfg.MediaDir, entry.VideoID)
		if filePath == "" {
			m.log.Error("Prefetch worker: download completed but file not found on disk", "videoID", entry.VideoID)
			_ = m.db.SetQueueStatus(entry.ID, "FAILED")
			if m.publish != nil {
				m.publish(map[string]any{
					"type":     "PREFETCH_FAILED",
					"cmd":      "PREFETCH_FAILED",
					"video_id": entry.VideoID,
					"error":    "Downloaded file not found",
				})
			}
			release()
			continue
		}

		var size int64
		if fi, err := os.Stat(filePath); err == nil {
			size = fi.Size()
		}

		duration := 0
		if song, err := m.db.LookupVideoID(entry.VideoID); err == nil && song != nil {
			duration = song.Duration
		}

		mediaEntry := db.MediaCacheEntry{
			VideoID:         entry.VideoID,
			Title:           entry.Title,
			FilePath:        filePath,
			FileSizeBytes:   size,
			DurationSeconds: duration,
			LastAccessedAt:  time.Now(),
			CreatedAt:       time.Now(),
		}
		if err := m.db.UpsertMediaCache(mediaEntry); err != nil {
			m.log.Warn("Prefetch worker: failed to upsert media cache", "videoID", entry.VideoID, "err", err)
		}

		if err := m.db.MarkVideoDownloaded(entry.VideoID, filePath); err != nil {
			m.log.Warn("Prefetch worker: failed to update song cache file_path", "videoID", entry.VideoID, "err", err)
		}

		m.log.Info("Prefetch worker: download completed successfully", "videoID", entry.VideoID, "path", filePath)
		_ = m.db.SetQueueStatus(entry.ID, "READY")
		m.PublishQueueInfo()

		release()
	}
}

// ── Internals ─────────────────────────────────────────────────────────────────

func (m *Manager) playQueueEntry(entry *db.QueueEntry) {
	err := m.streamer.StartStream(entry.VideoID, entry.Title)
	if err != nil {
		m.log.Error("playQueueEntry: failed to start stream", "videoID", entry.VideoID, "err", err)
		m.markFailed(entry.ID)
		return
	}

	m.markPlaying(entry.ID)

	var track *resolver.ResolvedTrack
	if song, err := m.db.LookupVideoID(entry.VideoID); err == nil && song != nil {
		track = dbRowToTrack(song)
	} else {
		track = &resolver.ResolvedTrack{
			VideoID:    entry.VideoID,
			Title:      entry.Title,
			WebpageURL: "https://www.youtube.com/watch?v=" + entry.VideoID,
		}
	}

	m.mu.Lock()
	m.current = track
	m.mu.Unlock()

	_ = m.db.IncrementPlayCount(track.Query)
	_ = m.db.SavePlayHistory(track.Query, track.Title)

	if m.publish != nil {
		m.publish(map[string]any{
			"type":           "playing",
			"title":          track.Title,
			"uploader":       track.Uploader,
			"duration":       track.Duration,
			"query":          track.Query,
			"thumbnail_path": track.ThumbnailPath,
			"thumbnail_url":  track.ThumbnailURL,
			"related_videos": track.RelatedVideos,
		})
	}
	m.PublishStatus()

	go m.monitorStreamCompletion(entry.ID)
	go m.enrichRecommendations(track)
}

func (m *Manager) playTrackFromAutoplay(track *resolver.ResolvedTrack) {
	entry := db.QueueEntry{
		VideoID: track.VideoID,
		Title:   track.Title,
		Status:  "PENDING",
		Source:  "autoplay",
		AddedAt: time.Now(),
	}
	id, err := m.db.EnqueueTrack(entry)
	if err != nil {
		m.log.Error("playTrackFromAutoplay: failed to enqueue autoplay track", "err", err)
		return
	}
	entry.ID = id

	m.playQueueEntry(&entry)
}

func (m *Manager) monitorStreamCompletion(entryID int64) {
	for i := 0; i < 20; i++ {
		if m.rm.IsLiveStreamActive() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	for {
		time.Sleep(500 * time.Millisecond)
		if !m.rm.IsLiveStreamActive() {
			break
		}
	}

	entry, err := m.db.GetQueueEntry(entryID)
	if err == nil && entry != nil && entry.Status == "PLAYING" {
		m.log.Info("monitorStreamCompletion: stream ended naturally, marking COMPLETED", "id", entryID)
		m.markCompleted(entryID)
	}
}

func (m *Manager) enrichRecommendations(track *resolver.ResolvedTrack) {
	m.mu.Lock()
	poolIDs := map[string]bool{}
	for _, t := range m.autoplayPool {
		poolIDs[t.VideoID] = true
	}
	m.mu.Unlock()

	entries, err := m.db.ListQueue()
	if err == nil {
		m.mu.Lock()
		for _, entry := range entries {
			poolIDs[entry.VideoID] = true
		}
		m.mu.Unlock()
	}

	history, _ := m.db.GetHistory(10)
	playedIDs := map[string]bool{track.VideoID: true}
	for _, h := range history {
		if h.VideoID != "" {
			playedIDs[h.VideoID] = true
		}
	}

	var newCandidates []db.RelatedVideo
	for _, v := range track.RelatedVideos {
		if v.ID == "" || poolIDs[v.ID] || playedIDs[v.ID] {
			continue
		}
		if v.Duration > 0 && v.Duration > 1200 {
			continue
		}
		newCandidates = append(newCandidates, v)
		poolIDs[v.ID] = true
	}

	if len(newCandidates) == 0 {
		m.log.Info("No new recommendations for autoplay pool", "title", track.Title)
		return
	}

	const maxPoolSize = 50
	m.log.Info("Pooling recommendations (lazy, no yt-dlp)", "count", len(newCandidates))
	m.mu.Lock()
	newNextAutoplay := false
	for _, c := range newCandidates {
		if len(m.autoplayPool) >= maxPoolSize {
			m.log.Info("Autoplay pool full — skipping remaining candidates", "max", maxPoolSize)
			break
		}
		webURL := "https://www.youtube.com/watch?v=" + c.ID
		minimal := &resolver.ResolvedTrack{
			Query:        webURL,
			VideoID:      c.ID,
			Title:        c.Title,
			Uploader:     c.Uploader,
			Duration:     c.Duration,
			WebpageURL:   webURL,
			ThumbnailURL: "https://i.ytimg.com/vi/" + c.ID + "/hqdefault.jpg",
		}
		m.autoplayPool = append(m.autoplayPool, minimal)
		if m.nextAutoplay == nil {
			m.nextAutoplay = minimal
			newNextAutoplay = true
		}
		m.log.Info("Pooled autoplay candidate", "title", minimal.Title, "pool_size", len(m.autoplayPool))
	}
	m.mu.Unlock()

	if newNextAutoplay {
		go m.enrichNextAutoplay()
	}
	m.PublishQueueInfo()
}

func (m *Manager) enrichNextAutoplay() {
	m.mu.Lock()
	if len(m.autoplayPool) == 0 {
		m.mu.Unlock()
		return
	}
	next := m.autoplayPool[0]
	m.mu.Unlock()

	if next.RelatedVideos != nil {
		return
	}

	full, err := m.resolver.Resolve(next.WebpageURL)
	if err != nil || full == nil {
		m.log.Warn("Could not enrich next autoplay track", "videoID", next.VideoID, "err", err)
		return
	}

	if full.Duration > 1200 {
		m.log.Warn("Removing enriched autoplay track exceeding 20 minutes", "title", full.Title, "duration", full.Duration)
		m.mu.Lock()
		if len(m.autoplayPool) > 0 && m.autoplayPool[0].VideoID == next.VideoID {
			m.autoplayPool = m.autoplayPool[1:]
		}
		if len(m.autoplayPool) > 0 {
			m.nextAutoplay = m.autoplayPool[0]
		} else {
			m.nextAutoplay = nil
		}
		newNext := m.nextAutoplay
		m.mu.Unlock()

		m.PublishQueueInfo()
		if newNext != nil {
			go m.enrichNextAutoplay()
		}
		return
	}

	m.mu.Lock()
	if len(m.autoplayPool) > 0 && m.autoplayPool[0].VideoID == next.VideoID {
		m.autoplayPool[0] = full
	}
	if m.nextAutoplay != nil && m.nextAutoplay.VideoID == next.VideoID {
		m.nextAutoplay = full
	}
	m.mu.Unlock()

	m.log.Info("Enriched next autoplay track", "title", full.Title)
	m.resolver.StartBackgroundDownload(full, func(success bool) {
		m.log.Info("Pre-cache done for next autoplay track", "title", full.Title, "success", success)
		m.PublishQueueInfo()
	})
	m.PublishQueueInfo()
}

func (m *Manager) onEOF(event map[string]any) {
	// Left as no-op or fallback since now driven by StartStream + HTTP streamer context completion.
}

// ── helpers ───────────────────────────────────────────────────────────────────

func findDownloadedFile(mediaDir, videoID string) string {
	files, err := os.ReadDir(mediaDir)
	if err != nil {
		return ""
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), videoID+".") {
			ext := filepath.Ext(f.Name())
			if ext != ".part" && ext != ".ytdl" && ext != ".tmp" {
				return filepath.Join(mediaDir, f.Name())
			}
		}
	}
	return ""
}

func dbRowToTrack(row *db.SongRow) *resolver.ResolvedTrack {
	return &resolver.ResolvedTrack{
		Query:         row.Query,
		VideoID:       row.VideoID,
		Title:         row.Title,
		Uploader:      row.Uploader,
		Duration:      row.Duration,
		WebpageURL:    row.WebpageURL,
		FilePath:      row.FilePath,
		ThumbnailPath: row.ThumbnailPath,
		ThumbnailURL:  row.ThumbnailURL,
		RelatedVideos: row.RelatedVideos,
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Pause pauses playback.
func (m *Manager) Pause() {
	m.mu.Lock()
	m.wasPlayingBeforeAssistant = false
	m.mu.Unlock()
	_ = m.mpv.Pause()
}

// Resume resumes playback.
func (m *Manager) Resume() {
	m.mu.Lock()
	if m.assistantActive {
		m.wasPlayingBeforeAssistant = true
		m.mu.Unlock()
		m.log.Info("Resume ignored: assistant is active, but marked to play on assistant end")
		return
	}
	m.wasPlayingBeforeAssistant = false
	m.mu.Unlock()
	_ = m.mpv.Resume()
}

// AssistantPause handles pausing for wake word/assistant conversation start.
func (m *Manager) AssistantPause() {
	status := m.mpv.GetStatus()

	m.mu.Lock()
	m.assistantActive = true
	if status.State == "playing" {
		m.wasPlayingBeforeAssistant = true
	}
	wasPlaying := m.wasPlayingBeforeAssistant
	m.mu.Unlock()

	if wasPlaying {
		m.log.Info("AssistantPause: player was playing, pausing now")
		_ = m.mpv.Pause()
	} else {
		m.log.Info("AssistantPause: player was not playing, doing nothing")
	}
}

// AssistantPlay handles resuming after assistant conversation ends.
func (m *Manager) AssistantPlay() {
	m.mu.Lock()
	m.assistantActive = false
	shouldResume := m.wasPlayingBeforeAssistant
	m.wasPlayingBeforeAssistant = false
	m.mu.Unlock()

	if shouldResume {
		m.log.Info("AssistantPlay: player was playing before, resuming now")
		_ = m.mpv.Resume()
	} else {
		m.log.Info("AssistantPlay: player was not playing before, doing nothing")
	}
}
