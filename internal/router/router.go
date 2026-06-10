// Package router validates and dispatches incoming MQTT command payloads.
//
// All commands arrive as JSON with a "cmd" field.
// Each command is dispatched to a goroutine so the MQTT callback never blocks.
package router

import (
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/mpv"
	"github.com/ankitm/mpv-relay/internal/queue"
)

var allowedCmds = map[string]bool{
	"play": true, "queue": true, "pause": true, "resume": true, "stop": true,
	"next": true, "previous": true, "seek": true, "volume": true, "mute": true,
	"shuffle": true, "clear": true, "status": true, "queue_list": true,
	"history": true, "autoplay": true, "download": true, "search": true,
	"play_next": true, "get_cached_songs": true,
}

// Router dispatches MQTT commands to queue/mpv/db handlers.
type Router struct {
	q       *queue.Manager
	mpv     *mpv.Client
	db      *db.DB
	publish func(map[string]any)
	log     *slog.Logger
}

// New creates a Router.
func New(q *queue.Manager, m *mpv.Client, database *db.DB, publish func(map[string]any)) *Router {
	return &Router{
		q:       q,
		mpv:     m,
		db:      database,
		publish: publish,
		log:     slog.Default().With("pkg", "router"),
	}
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
	r.publish(map[string]any{"type": "resolving", "query": query})
	track := r.q.PlayNow(query)
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
	r.publish(map[string]any{"type": "resolving", "query": query})
	r.q.QueueAdd(query)
}

func (r *Router) cmdPause() {
	_ = r.mpv.Pause()
	r.publish(map[string]any{"type": "state", "state": "paused"})
}

func (r *Router) cmdResume() {
	_ = r.mpv.Resume()
	r.publish(map[string]any{"type": "state", "state": "playing"})
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
	raw, ok := p["seconds"]
	if !ok {
		r.publish(map[string]any{"type": "error", "message": "'seek' requires a numeric 'seconds' field"})
		return
	}
	secs, ok := toFloat64(raw)
	if !ok {
		r.publish(map[string]any{"type": "error", "message": "'seek' seconds must be a number"})
		return
	}
	_ = r.mpv.Seek(secs)
	r.publish(map[string]any{"type": "seeked", "seconds": secs})
}

func (r *Router) cmdVolume(p map[string]any) {
	raw, ok := p["level"]
	if !ok {
		r.publish(map[string]any{"type": "error", "message": "'volume' requires an integer 'level' field"})
		return
	}
	level, ok := toFloat64(raw)
	if !ok {
		r.publish(map[string]any{"type": "error", "message": "'volume' level must be a number"})
		return
	}
	_ = r.mpv.SetVolume(int(level))
	r.publish(map[string]any{"type": "volume", "level": int(level)})
}

func (r *Router) cmdMute() {
	_ = r.mpv.Mute()
	r.publish(map[string]any{"type": "state", "state": "mute_toggled"})
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
	r.q.PublishStatus()
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
	r.q.PlayNext(query)
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

// ── helpers ───────────────────────────────────────────────────────────────────

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
