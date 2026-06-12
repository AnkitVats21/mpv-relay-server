// Package queue manages the play queue, autoplay pool, and background caching.
//
// Caching strategy (mirrors Python):
//   - MPV stream-record saves audio while playing to MUSIC_CACHE_DIR/<id>.mkv
//   - On end-file(eof): mark complete in DB → next play uses local file
//   - On end-file(stop/error): delete incomplete recording
package queue

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ankitm/mpv-relay/internal/config"
	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/mpv"
	"github.com/ankitm/mpv-relay/internal/resolver"
)

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

	mu             sync.Mutex
	queue          []*resolver.ResolvedTrack
	current        *resolver.ResolvedTrack
	recordPath     string
	autoplayOn     bool
	autoplayPool   []*resolver.ResolvedTrack
	nextAutoplay   *resolver.ResolvedTrack
	ignoreNextEOF  bool // set during track transitions to swallow spurious end-file
	historyStack   []string
	isNavBack      bool

	dlMu    sync.Mutex
	dlQueue []*resolver.ResolvedTrack
	dlBusy  bool
}

// New creates a Manager and registers the MPV end-file event handler.
func New(m *mpv.Client, res *resolver.Resolver, database *db.DB, cfg *config.Config, publish func(map[string]any)) *Manager {
	mgr := &Manager{
		mpv:        m,
		resolver:   res,
		db:         database,
		cfg:        cfg,
		publish:    publish,
		autoplayOn: true,
		log:        slog.Default().With("pkg", "queue"),
	}
	m.OnEvent("end-file", mgr.onEOF)
	return mgr
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

// ── Public API ────────────────────────────────────────────────────────────────

// PlayNow resolves a query and starts playing immediately (clears queue + pool).
func (m *Manager) PlayNow(query string) *resolver.ResolvedTrack {
	m.mu.Lock()
	m.queue = nil
	m.autoplayPool = nil
	m.nextAutoplay = nil
	m.mu.Unlock()
	return m.resolveAndPlay(query)
}

// QueueAdd appends a query to the end of the queue.
func (m *Manager) QueueAdd(query string) {
	go m.resolveAndEnqueue(query, false)
}

// PlayNext prepends a query to the front of the queue.
func (m *Manager) PlayNext(query string) {
	go m.resolveAndEnqueue(query, true)
}

// Skip stops the current track (EOF handler advances to next).
func (m *Manager) Skip() { _ = m.mpv.Stop() }

// ClearQueue removes all pending items.
func (m *Manager) ClearQueue() {
	m.mu.Lock()
	m.queue = nil
	m.mu.Unlock()
	m.log.Info("Queue cleared")
}

// StopAll clears the queue, autoplay pool, history, and stops MPV.
func (m *Manager) StopAll() {
	m.mu.Lock()
	m.queue = nil
	m.current = nil
	m.autoplayPool = nil
	m.nextAutoplay = nil
	m.historyStack = nil
	m.recordPath = ""
	m.mu.Unlock()
	_ = m.mpv.Stop()
	m.log.Info("StopAll: queue, autoplay pool, and history cleared")
	m.PublishStatus()
}

// Shuffle randomises the pending queue.
func (m *Manager) Shuffle() {
	m.mu.Lock()
	rand.Shuffle(len(m.queue), func(i, j int) { m.queue[i], m.queue[j] = m.queue[j], m.queue[i] })
	m.mu.Unlock()
	m.log.Info("Queue shuffled", "len", len(m.queue))
}

// ListQueue returns a snapshot of the pending queue items.
func (m *Manager) ListQueue() []QueueItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]QueueItem, 0, len(m.queue))
	for i, t := range m.queue {
		items = append(items, QueueItem{
			Position:     i + 1,
			VideoID:      t.VideoID,
			Title:        t.Title,
			Uploader:     t.Uploader,
			Duration:     t.Duration,
			ThumbnailURL: t.ThumbnailURL,
			Cached:       t.FilePath != "" && fileExists(t.FilePath),
		})
	}
	return items
}

// CurrentTrack returns the currently playing track (may attempt recovery).
func (m *Manager) CurrentTrack() *resolver.ResolvedTrack {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		m.recoverCurrentTrack()
	}
	return m.current
}

// Previous plays the previously played track from the history stack.
func (m *Manager) Previous() {
	m.mu.Lock()
	if len(m.historyStack) == 0 {
		m.mu.Unlock()
		m.log.Info("No previous track in history stack")
		return
	}
	if m.current != nil {
		m.queue = append([]*resolver.ResolvedTrack{m.current}, m.queue...)
	}
	prevQuery := m.historyStack[len(m.historyStack)-1]
	m.historyStack = m.historyStack[:len(m.historyStack)-1]
	m.isNavBack = true
	m.mu.Unlock()

	m.log.Info("Playing previous track", "query", prevQuery)
	go m.resolveAndPlay(prevQuery)
}

// PublishStatus broadcasts a full status payload on the MQTT status topic.
func (m *Manager) PublishStatus() {
	if m.publish == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("PublishStatus panic", "err", r)
		}
	}()

	status := m.mpv.GetStatus()
	payload := map[string]any{
		"type":     "status",
		"state":    status.State,
		"position": status.Position,
		"duration": status.Duration,
		"volume":   status.Volume,
	}

	current := m.CurrentTrack()
	if current != nil {
		payload["title"] = current.Title
		payload["uploader"] = current.Uploader
		payload["thumbnail_path"] = current.ThumbnailPath
		payload["thumbnail_url"] = current.ThumbnailURL

		// Annotate related videos with cached status
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
	}

	m.mu.Lock()
	qLen := len(m.queue)
	nextAP := m.nextAutoplay
	poolSnap := append([]*resolver.ResolvedTrack(nil), m.autoplayPool...)
	autoplayOn := m.autoplayOn
	m.mu.Unlock()

	payload["queue_length"] = qLen
	payload["autoplay"] = autoplayOn

	if nextAP != nil {
		payload["next_autoplay"] = map[string]any{
			"title":    nextAP.Title,
			"video_id": nextAP.VideoID,
			"cached":   nextAP.FilePath != "" && fileExists(nextAP.FilePath),
		}
	} else {
		payload["next_autoplay"] = nil
	}

	// Build unified up_next list: manual queue first, then autoplay pool
	qItems := m.ListQueue()
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

// ── Download queue ────────────────────────────────────────────────────────────

// AddToDownloadQueue resolves and enqueues a video_id for background download.
func (m *Manager) AddToDownloadQueue(videoID string) {
	m.log.Info("Download request received", "videoID", videoID)
	go func() {
		var track *resolver.ResolvedTrack

		// Check DB first
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

// ── Internals ─────────────────────────────────────────────────────────────────

func (m *Manager) resolveAndPlay(query string) *resolver.ResolvedTrack {
	track, err := m.resolver.Resolve(query)
	if err != nil || track == nil {
		m.log.Error("Could not resolve query", "query", query, "err", err)
		return nil
	}
	m.playTrack(track)
	return track
}

func (m *Manager) resolveAndEnqueue(query string, front bool) {
	if m.publish != nil {
		m.publish(map[string]any{"type": "resolving", "query": query})
	}

	track, err := m.resolver.Resolve(query)
	if err != nil || track == nil {
		m.log.Error("Could not resolve queue query", "query", query)
		if m.publish != nil {
			m.publish(map[string]any{"type": "error", "message": "Could not resolve query: " + query})
		}
		return
	}

	m.mu.Lock()
	idle := m.current == nil || m.mpv.IsIdle()
	m.mu.Unlock()

	if idle {
		m.log.Info("Nothing playing — playing immediately", "title", track.Title)
		m.playTrack(track)
		return
	}

	m.mu.Lock()
	if front {
		m.queue = append([]*resolver.ResolvedTrack{track}, m.queue...)
		m.log.Info("Prepended (Play Next)", "title", track.Title)
	} else {
		m.queue = append(m.queue, track)
		m.log.Info("Appended to queue", "title", track.Title, "pos", len(m.queue))
	}
	m.mu.Unlock()

	if m.publish != nil {
		m.publish(map[string]any{
			"type":           "queued",
			"title":          track.Title,
			"query":          query,
			"video_id":       track.VideoID,
			"insert_at_front": front,
		})
	}
	m.PublishStatus()
}

func (m *Manager) playNext() {
	var next *resolver.ResolvedTrack
	isAutoplay := false

	m.mu.Lock()
	if len(m.queue) > 0 {
		next = m.queue[0]
		m.queue = m.queue[1:]
	} else if m.autoplayOn && len(m.autoplayPool) > 0 {
		next = m.autoplayPool[0]
		m.autoplayPool = m.autoplayPool[1:]
		if len(m.autoplayPool) > 0 {
			m.nextAutoplay = m.autoplayPool[0]
		} else {
			m.nextAutoplay = nil
		}
		isAutoplay = true
	} else {
		m.current = nil
		m.recordPath = ""
		m.nextAutoplay = nil
		m.mu.Unlock()
		m.log.Info("Queue exhausted — entering idle state")
		return
	}
	m.mu.Unlock()

	if isAutoplay {
		m.log.Info("Auto-advancing via AUTOPLAY", "title", next.Title)
		// If the track is minimal (no RelatedVideos), resolve it now so
		// prefetchWorker can build recommendations. Should be a fast DB
		// cache hit since enrichNextAutoplay ran in the background.
		if next.RelatedVideos == nil {
			if full, err := m.resolver.Resolve(next.WebpageURL); err == nil && full != nil {
				next = full
			}
		}
	} else {
		m.log.Info("Auto-advancing via QUEUE", "title", next.Title)
	}
	go m.playTrack(next)
}

func (m *Manager) playTrack(track *resolver.ResolvedTrack) {
	m.mu.Lock()
	if m.current != nil || !m.mpv.IsIdle() {
		// A track is playing/loading; the loadfile will cause MPV to fire
		// end-file(stop) AND potentially a synthesised end-file(eof) via the
		// idle-active edge. Suppress both until we're settled.
		m.ignoreNextEOF = true
	}
	if m.current != nil && !m.isNavBack {
		m.historyStack = append(m.historyStack, m.current.Query)
	}
	m.isNavBack = false
	m.mu.Unlock()

	var recordPath string
	if track.FilePath != "" && fileExists(track.FilePath) {
		m.log.Info("Playing LOCAL file", "path", track.FilePath)
		m.mpv.Loadfile(track.FilePath, "")
	} else if m.resolver.IsDownloading(track.VideoID) {
		m.log.Info("Streaming directly (BG download in progress)", "title", track.Title)
		m.mpv.Loadfile(track.WebpageURL, "")
	} else {
		recordPath = filepath.Join(m.cfg.MusicCacheDir, track.VideoID+".mkv")
		m.log.Info("Streaming with stream-record", "title", track.Title, "record", recordPath)
		m.mpv.Loadfile(track.WebpageURL, recordPath)
	}
	_ = m.mpv.Resume()

	m.mu.Lock()
	m.current = track
	m.recordPath = recordPath
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
	go m.prefetchWorker(track)
}

func (m *Manager) prefetchWorker(track *resolver.ResolvedTrack) {
	// 1. Pre-cache the next manually queued track (if any).
	m.mu.Lock()
	var nextQueued *resolver.ResolvedTrack
	if len(m.queue) > 0 {
		nextQueued = m.queue[0]
	}
	m.mu.Unlock()

	if nextQueued != nil {
		m.log.Info("Prefetching next queued track", "title", nextQueued.Title)
		m.resolver.StartBackgroundDownload(nextQueued, func(success bool) {
			m.log.Info("Pre-cache done for queued track", "title", nextQueued.Title, "success", success)
			m.PublishStatus()
		})
	}

	// 2. Build dedup sets from existing pool + play history.
	m.mu.Lock()
	poolIDs := map[string]bool{}
	for _, t := range m.autoplayPool {
		poolIDs[t.VideoID] = true
	}
	m.mu.Unlock()

	history, _ := m.db.GetHistory(10)
	playedIDs := map[string]bool{track.VideoID: true}
	for _, h := range history {
		if h.VideoID != "" {
			playedIDs[h.VideoID] = true
		}
	}

	// 3. Filter to only genuinely new candidates.
	var newCandidates []db.RelatedVideo
	for _, v := range track.RelatedVideos {
		if v.ID == "" || poolIDs[v.ID] || playedIDs[v.ID] {
			continue
		}
		newCandidates = append(newCandidates, v)
		poolIDs[v.ID] = true
	}

	if len(newCandidates) == 0 {
		m.log.Info("No new recommendations for autoplay pool", "title", track.Title)
		return
	}

	// 4. Add new candidates as MINIMAL tracks — zero yt-dlp calls.
	//    Cap pool at 50 total; skip additions once full.
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

	// 5. Whenever nextAutoplay was just set for the first time, pre-resolve
	//    and pre-download that track in the background (1 yt-dlp call max).
	if newNextAutoplay {
		go m.enrichNextAutoplay()
	}
	m.PublishStatus()
}

// enrichNextAutoplay resolves the first unresolved (minimal) entry in the
// autoplay pool via a single yt-dlp call, then starts a background audio
// download for it. All other pool entries remain minimal until they play.
func (m *Manager) enrichNextAutoplay() {
	m.mu.Lock()
	if len(m.autoplayPool) == 0 {
		m.mu.Unlock()
		return
	}
	next := m.autoplayPool[0]
	m.mu.Unlock()

	if next.RelatedVideos != nil {
		return // already fully resolved
	}

	full, err := m.resolver.Resolve(next.WebpageURL)
	if err != nil || full == nil {
		m.log.Warn("Could not enrich next autoplay track", "videoID", next.VideoID, "err", err)
		return
	}

	// Replace the minimal entry with the fully resolved track.
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
		m.PublishStatus()
	})
	m.PublishStatus()
}

// onEOF handles MPV end-file events.
func (m *Manager) onEOF(event map[string]any) {
	reason, _ := event["reason"].(string)
	if reason == "" {
		reason = "unknown"
	}
	m.log.Info("MPV end-file", "reason", reason)

	m.mu.Lock()
	if m.ignoreNextEOF {
		m.ignoreNextEOF = false
		m.mu.Unlock()
		m.log.Info("Ignoring spurious end-file during track transition", "reason", reason)
		return
	}
	track := m.current
	recPath := m.recordPath
	m.mu.Unlock()

	if reason == "eof" {
		if track != nil && recPath != "" && fileExists(recPath) {
			size := fileSizeMB(recPath)
			m.log.Info("Recording complete", "path", recPath, "size_mb", size)
			_ = m.db.MarkFileDownloaded(track.Query, recPath)
		}
		m.playNext()
	} else {
		// Incomplete recording — delete partial file
		if recPath != "" && fileExists(recPath) {
			if err := os.Remove(recPath); err != nil {
				m.log.Warn("Could not delete partial recording", "path", recPath, "err", err)
			} else {
				m.log.Info("Deleted incomplete recording", "reason", reason, "path", recPath)
			}
		}
		if reason == "stop" {
			m.playNext()
		} else {
			m.mu.Lock()
			m.current = nil
			m.recordPath = ""
			m.mu.Unlock()
		}
	}
}

// recoverCurrentTrack attempts to reconstruct m.current from MPV's active path/URL.
// Must be called with m.mu held.
func (m *Manager) recoverCurrentTrack() {
	if m.mpv.IsIdle() {
		return
	}
	path := m.mpv.GetPath()
	if path == "" {
		return
	}

	var videoID string
	if strings.Contains(path, "youtube.com") || strings.Contains(path, "youtu.be") {
		parsed, err := url.Parse(path)
		if err == nil {
			if strings.Contains(parsed.Host, "youtube.com") {
				videoID = parsed.Query().Get("v")
			} else if strings.Contains(parsed.Host, "youtu.be") {
				videoID = strings.TrimPrefix(parsed.Path, "/")
			}
		}
	} else {
		base := filepath.Base(path)
		if strings.HasSuffix(base, ".mkv") {
			videoID = strings.TrimSuffix(base, ".mkv")
		} else if strings.HasSuffix(base, ".jpg") {
			videoID = strings.TrimSuffix(base, ".jpg")
		}
	}

	if videoID == "" {
		return
	}

	row, err := m.db.LookupVideoID(videoID)
	if err != nil || row == nil {
		return
	}
	track := dbRowToTrack(row)
	m.current = track
	if rec := m.mpv.GetStreamRecord(); rec != "" {
		m.recordPath = rec
	}
	m.log.Info("Recovered active track from MPV path", "path", path, "title", track.Title)
}

// ── helpers ───────────────────────────────────────────────────────────────────

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

func fileSizeMB(path string) float64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(fi.Size()) / (1024 * 1024)
}

// jsonMarshal is a convenience used in status payloads.
func jsonMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

var _ = jsonMarshal // suppress unused warning
