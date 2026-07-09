// Package router validates and dispatches incoming MQTT command payloads.
//
// All commands arrive as JSON with a "cmd" field.
// Each command is dispatched to a goroutine so the MQTT callback never blocks.
package router

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/eviction"
	"github.com/ankitm/mpv-relay/internal/queue"
	"github.com/ankitm/mpv-relay/internal/streamer"
)

var allowedCmds = map[string]bool{
	"play": true, "queue": true, "pause": true, "resume": true, "stop": true,
	"next": true, "previous": true, "seek": true, "volume": true, "mute": true,
	"shuffle": true, "clear": true, "status": true, "queue_list": true,
	"history": true, "autoplay": true, "download": true, "search": true,
	"play_next": true, "get_cached_songs": true, "assistant_pause": true,
	"assistant_play": true,
	"device_config_get": true, "device_config_set": true,
	"gemini_config_get": true, "gemini_config_set": true,
	"stream_status": true, "prefetch_status": true, "clear_cache": true,
}

// Router dispatches MQTT commands to queue/streamer/db handlers.
type Router struct {
	q              *queue.Manager
	streamer       *streamer.Streamer
	db             *db.DB
	publish        func(map[string]any)
	publishMqttRaw func(string, []byte)
	log            *slog.Logger
	evictWorker    *eviction.Worker
}

// New creates a Router.
func New(q *queue.Manager, s *streamer.Streamer, database *db.DB, publish func(map[string]any), publishMqttRaw func(string, []byte)) *Router {
	return &Router{
		q:              q,
		streamer:       s,
		db:             database,
		publish:        publish,
		publishMqttRaw: publishMqttRaw,
		log:            slog.Default().With("pkg", "router"),
	}
}

// SetEvictionWorker sets the eviction worker for the router.
func (r *Router) SetEvictionWorker(w *eviction.Worker) {
	r.evictWorker = w
}

// Dispatch is called by the MQTT handler for every incoming message.
// It parses JSON, validates the command, and fires off a goroutine.
func (r *Router) Dispatch(raw string) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		r.log.Warn("Non-JSON payload received", "raw", raw)
		r.publish(map[string]any{"type": "error", "message": "Payload must be valid JSON"})
		return
	}

	cmd := strings.ToLower(strings.TrimSpace(strVal(payload, "cmd")))
	if cmd == "" {
		r.publish(map[string]any{"type": "error", "message": "Missing 'cmd' field"})
		return
	}
	if !allowedCmds[cmd] {
		r.publish(map[string]any{"type": "error", "message": "Unknown command: '" + cmd + "'"})
		return
	}

	go r.run(cmd, payload)
}

func (r *Router) run(cmd string, p map[string]any) {
	defer func() {
		if rec := recover(); rec != nil {
			r.log.Error("Command handler panic", "cmd", cmd, "err", rec)
			r.publish(map[string]any{"type": "error", "message": "Internal server error"})
		}
	}()

	switch cmd {
	case "play":
		r.cmdPlay(p)
	case "queue":
		r.cmdQueue(p)
	case "pause":
		r.cmdPause()
	case "resume":
		r.cmdResume()
	case "assistant_pause":
		r.cmdAssistantPause()
	case "assistant_play":
		r.cmdAssistantPlay()
	case "stop":
		r.cmdStop()
	case "next":
		r.cmdNext()
	case "previous":
		r.cmdPrevious()
	case "seek":
		r.cmdSeek(p)
	case "volume":
		r.cmdVolume(p)
	case "mute":
		r.cmdMute()
	case "shuffle":
		r.cmdShuffle()
	case "clear":
		r.cmdClear()
	case "status":
		r.cmdStatus()
	case "queue_list":
		r.cmdQueueList()
	case "history":
		r.cmdHistory()
	case "autoplay":
		r.cmdAutoplay(p)
	case "download":
		r.cmdDownload(p)
	case "search":
		r.cmdSearch(p)
	case "play_next":
		r.cmdPlayNext(p)
	case "get_cached_songs":
		r.cmdGetCachedSongs(p)
	case "device_config_get":
		r.cmdDeviceConfigGet()
	case "device_config_set":
		r.cmdDeviceConfigSet(p)
	case "gemini_config_get":
		r.cmdGeminiConfigGet()
	case "gemini_config_set":
		r.cmdGeminiConfigSet(p)
	case "stream_status":
		r.cmdStreamStatus()
	case "prefetch_status":
		r.cmdPrefetchStatus()
	case "clear_cache":
		r.cmdClearCache(p)
	default:
		r.publish(map[string]any{"type": "error", "message": "Unhandled command: " + cmd})
	}
}

// ── Handlers ─────────────────────────────────────────────────────────────────

func (r *Router) cmdPlay(p map[string]any) {
	query := strings.TrimSpace(strVal(p, "query"))
	if query == "" {
		r.publish(map[string]any{"type": "error", "message": "'play' requires a 'query' field"})
		return
	}
	download := true
	if val, ok := p["download"]; ok {
		if b, ok2 := val.(bool); ok2 {
			download = b
		}
	}
	r.publish(map[string]any{"type": "resolving", "query": query})
	track := r.q.PlayNow(query, download)
	if track == nil {
		r.publish(map[string]any{"type": "error", "message": "Could not find: '" + query + "'"})
	}
}

func (r *Router) cmdQueue(p map[string]any) {
	query := strings.TrimSpace(strVal(p, "query"))
	if query == "" {
		r.publish(map[string]any{"type": "error", "message": "'queue' requires a 'query' field"})
		return
	}
	download := true
	if val, ok := p["download"]; ok {
		if b, ok2 := val.(bool); ok2 {
			download = b
		}
	}
	r.publish(map[string]any{"type": "resolving", "query": query})
	r.q.QueueAdd(query, download)
}

func (r *Router) cmdPause() {
	r.q.Pause()
	r.publish(map[string]any{"type": "state", "state": "paused"})
}

func (r *Router) cmdResume() {
	r.q.Resume()
	r.publish(map[string]any{"type": "state", "state": "playing"})
}

func (r *Router) cmdAssistantPause() {
	r.q.AssistantPause()
}

func (r *Router) cmdAssistantPlay() {
	r.q.AssistantPlay()
}

func (r *Router) cmdStop() {
	r.q.StopAll()
	r.publish(map[string]any{"type": "state", "state": "stopped"})
}

func (r *Router) cmdNext() {
	r.q.Skip()
	r.publish(map[string]any{"type": "state", "state": "skipping"})
}

func (r *Router) cmdPrevious() {
	r.q.Previous()
}

func (r *Router) cmdSeek(p map[string]any) {
	r.publish(map[string]any{"type": "error", "message": "not supported in stream mode"})
}

func (r *Router) cmdVolume(p map[string]any) {
	r.publish(map[string]any{"type": "error", "message": "not supported in stream mode"})
}

func (r *Router) cmdMute() {
	r.publish(map[string]any{"type": "error", "message": "not supported in stream mode"})
}

func (r *Router) cmdShuffle() {
	r.q.Shuffle()
	items := r.q.ListQueue()
	r.publish(map[string]any{"type": "queue", "items": items})
}

func (r *Router) cmdClear() {
	r.q.ClearQueue()
	r.publish(map[string]any{"type": "queue", "items": []any{}})
}

func (r *Router) cmdStatus() {
	r.q.PublishStatus()
}

func (r *Router) cmdAutoplay(p map[string]any) {
	enabled := true
	if v, ok := p["enabled"]; ok {
		if b, ok := v.(bool); ok {
			enabled = b
		}
	}
	r.q.SetAutoplay(enabled)
	r.q.PublishQueueInfo()
}

func (r *Router) cmdQueueList() {
	items := r.q.ListQueue()
	r.publish(map[string]any{"type": "queue", "items": items})
}

func (r *Router) cmdHistory() {
	history, err := r.db.GetHistory(20)
	if err != nil {
		r.publish(map[string]any{"type": "error", "message": err.Error()})
		return
	}
	items := make([]map[string]any, 0, len(history))
	for _, h := range history {
		items = append(items, map[string]any{
			"id":             h.ID,
			"query":          h.Query,
			"title":          h.Title,
			"played_at":      h.PlayedAt,
			"video_id":       h.VideoID,
			"thumbnail_path": h.ThumbnailPath,
			"thumbnail_url":  h.ThumbnailURL,
			"uploader":       h.Uploader,
			"duration":       h.Duration,
			"cached":         h.Cached,
		})
	}
	r.publish(map[string]any{"type": "history", "items": items})
}

func (r *Router) cmdDownload(p map[string]any) {
	videoID := strings.TrimSpace(strVal(p, "video_id"))
	if videoID == "" {
		r.publish(map[string]any{"type": "error", "message": "Missing 'video_id' for download command"})
		return
	}
	r.q.AddToDownloadQueue(videoID)
}

func (r *Router) cmdSearch(p map[string]any) {
	query := strings.TrimSpace(strVal(p, "query"))
	if query == "" {
		r.publish(map[string]any{"type": "error", "message": "Missing 'query' for search command"})
		return
	}
	r.q.SearchSongs(query)
}

func (r *Router) cmdPlayNext(p map[string]any) {
	query := strings.TrimSpace(strVal(p, "query"))
	if query == "" {
		r.publish(map[string]any{"type": "error", "message": "Missing 'query' for play_next command"})
		return
	}
	download := true
	if val, ok := p["download"]; ok {
		if b, ok2 := val.(bool); ok2 {
			download = b
		}
	}
	r.q.PlayNext(query, download)
}

func (r *Router) cmdGetCachedSongs(p map[string]any) {
	page := 1
	limit := 20
	if v, ok := p["page"]; ok {
		if f, ok2 := toFloat64(v); ok2 {
			page = int(f)
		}
	}
	if v, ok := p["limit"]; ok {
		if f, ok2 := toFloat64(v); ok2 {
			limit = int(f)
		}
	}
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 20
	}

	songs, total, err := r.db.GetCachedSongs(page, limit)
	if err != nil {
		r.publish(map[string]any{"type": "error", "message": err.Error()})
		return
	}

	items := make([]map[string]any, 0, len(songs))
	for _, s := range songs {
		items = append(items, map[string]any{
			"query":          s.Query,
			"video_id":       s.VideoID,
			"title":          s.Title,
			"uploader":       s.Uploader,
			"duration":       s.Duration,
			"thumbnail_path": s.ThumbnailPath,
			"thumbnail_url":  s.ThumbnailURL,
			"file_path":      s.FilePath,
			"cached":         true,
		})
	}

	hasMore := (page * limit) < total
	r.publish(map[string]any{
		"type":     "cached_songs_list",
		"page":     page,
		"limit":    limit,
		"total":    total,
		"has_more": hasMore,
		"items":    items,
	})
}

func (r *Router) cmdDeviceConfigGet() {
	if r.publishMqttRaw != nil {
		r.publishMqttRaw("device/waveshare/config/get", []byte{})
	}
}

func (r *Router) cmdDeviceConfigSet(p map[string]any) {
	content := strVal(p, "content")
	if r.publishMqttRaw != nil {
		r.publishMqttRaw("device/waveshare/config/set", []byte(content))
	}
}

func (r *Router) cmdGeminiConfigGet() {
	if r.publishMqttRaw != nil {
		r.publishMqttRaw("device/waveshare/gemini/get", []byte{})
	}
}

func (r *Router) cmdGeminiConfigSet(p map[string]any) {
	content := strVal(p, "content")
	if r.publishMqttRaw != nil {
		r.publishMqttRaw("device/waveshare/gemini/set", []byte(content))
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func (r *Router) cmdStreamStatus() {
	videoID, bytesSent, uptime := r.streamer.GetSessionInfo()
	r.publish(map[string]any{
		"type":            "stream_status",
		"active_video_id": videoID,
		"bytes_sent":      bytesSent,
		"uptime_ms":       uptime.Milliseconds(),
	})
}

func (r *Router) cmdPrefetchStatus() {
	pending, prefetching, ready, err := r.db.GetPrefetchStatusCounts()
	if err != nil {
		r.publish(map[string]any{"type": "error", "message": "Failed to get prefetch status: " + err.Error()})
		return
	}
	r.publish(map[string]any{
		"type":        "prefetch_status",
		"pending":     pending,
		"prefetching": prefetching,
		"ready":       ready,
	})
}

func (r *Router) cmdClearCache(p map[string]any) {
	videoID := strings.TrimSpace(strVal(p, "video_id"))
	if videoID != "" {
		row, err := r.db.LookupMediaCache(videoID)
		if err != nil {
			r.publish(map[string]any{"type": "error", "message": "Failed to lookup cache: " + err.Error()})
			return
		}
		if row != nil && row.FilePath != "" {
			_ = os.Remove(row.FilePath)
		}
		if err := r.db.DeleteCacheByVideoID(videoID); err != nil {
			r.publish(map[string]any{"type": "error", "message": "Failed to delete from cache: " + err.Error()})
			return
		}
		r.publish(map[string]any{"type": "clear_cache_success", "video_id": videoID})
		return
	}

	if err := r.evictCacheLRU(); err != nil {
		r.publish(map[string]any{"type": "error", "message": "Failed to evict LRU cache: " + err.Error()})
		return
	}
	r.publish(map[string]any{"type": "clear_cache_success", "video_id": ""})
}

func (r *Router) evictCacheLRU() error {
	w := r.evictWorker
	if w == nil {
		maxBytes := int64(5368709120) // default 5 GB
		if envVal := os.Getenv("CACHE_MAX_BYTES"); envVal != "" {
			if val, err := strconv.ParseInt(envVal, 10, 64); err == nil {
				maxBytes = val
			}
		}
		w = eviction.New(r.db, maxBytes)
		if r.streamer != nil {
			w.SetActiveTrackProvider(r.streamer)
		}
	}

	_, _, err := w.RunOnce(context.Background())
	return err
}

func strVal(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}
