// Package resolver resolves music queries via yt-dlp and manages local caching.
package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ankitm/mpv-relay/internal/config"
	"github.com/ankitm/mpv-relay/internal/db"
)

// ResolvedTrack holds everything we know about a playable track.
type ResolvedTrack struct {
	Query         string
	VideoID       string
	Title         string
	Uploader      string
	Duration      int
	WebpageURL    string
	FilePath      string // "" = not yet cached locally
	ThumbnailPath string
	ThumbnailURL  string
	RelatedVideos []db.RelatedVideo
	SkipDownload  bool
}

// Resolver resolves queries via yt-dlp, manages thumbnail downloads and
// background audio caching.
type Resolver struct {
	db              *db.DB
	cfg             *config.Config
	ytdlpBin        string
	activeDownloads sync.Map // videoID → struct{}
	log             *slog.Logger
}

// New creates a Resolver, locating the yt-dlp binary automatically.
func New(database *db.DB, cfg *config.Config) *Resolver {
	return &Resolver{
		db:       database,
		cfg:      cfg,
		ytdlpBin: findYtDlp(),
		log:      slog.Default().With("pkg", "resolver"),
	}
}

// YtDlpBin returns the path to the resolved yt-dlp binary.
func (r *Resolver) YtDlpBin() string {
	return r.ytdlpBin
}

// findYtDlp looks for the yt-dlp binary in this order:
//  1. YTDLP_BIN env var
//  2. Sibling `bin/yt-dlp` of the running executable
//  3. PATH
func findYtDlp() string {
	if v := os.Getenv("YTDLP_BIN"); v != "" {
		return v
	}
	exe, _ := os.Executable()
	candidate := filepath.Join(filepath.Dir(exe), "bin", "yt-dlp")
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	if p, err := exec.LookPath("yt-dlp"); err == nil {
		return p
	}
	return "yt-dlp" // will fail at runtime with a clear error
}

// IsDownloading reports whether a background download is active for videoID.
func (r *Resolver) IsDownloading(videoID string) bool {
	_, ok := r.activeDownloads.Load(videoID)
	return ok
}

// ── Public ────────────────────────────────────────────────────────────────────

// Resolve returns a ResolvedTrack for the given query.
// It checks the DB cache first; on miss it shells out to yt-dlp.
func (r *Resolver) Resolve(query string) (*ResolvedTrack, error) {
	q := normalize(query)

	// ── 1. Cache hit ──────────────────────────────────────────────────────────
	row, err := r.db.LookupQuery(q)
	if err != nil {
		return nil, fmt.Errorf("db lookup: %w", err)
	}
	if row != nil {
		track := songRowToTrack(row, q)

		// Heal missing thumbnail fields
		if track.VideoID != "" && (track.ThumbnailURL == "" || track.ThumbnailPath == "") {
			if track.ThumbnailURL == "" {
				track.ThumbnailURL = ytThumbURL(track.VideoID)
			}
			if track.ThumbnailPath == "" {
				track.ThumbnailPath = r.downloadThumbnail(track.ThumbnailURL, track.VideoID)
			}
			_ = r.db.SaveSong(trackToSongRow(track))
		}

		// Heal missing related videos
		if track.VideoID != "" && len(track.RelatedVideos) == 0 {
			r.log.Info("Cache HIT missing related videos — fetching on the fly", "title", track.Title)
			related := r.fetchRelatedVideos(track.VideoID)
			track.RelatedVideos = related
			row2 := trackToSongRow(track)
			_ = r.db.SaveSong(row2)
		}

		r.log.Info("Cache HIT", "query", q, "title", track.Title)
		return track, nil
	}

	// ── 2. yt-dlp search ─────────────────────────────────────────────────────
	r.log.Info("Cache MISS — running yt-dlp", "query", q)
	track, err := r.searchYtDlp(q)
	if err != nil || track == nil {
		return nil, err
	}

	// Download thumbnail
	if track.ThumbnailURL != "" {
		track.ThumbnailPath = r.downloadThumbnail(track.ThumbnailURL, track.VideoID)
	}

	// ── 3. Save to DB ─────────────────────────────────────────────────────────
	if err := r.db.SaveSong(trackToSongRow(track)); err != nil {
		r.log.Warn("Failed to cache song in DB", "err", err)
	}
	return track, nil
}

// SearchYouTube runs a flat yt-dlp search and returns up to limit results.
func (r *Resolver) SearchYouTube(query string, limit int) ([]db.RelatedVideo, error) {
	r.log.Info("Searching YouTube", "query", query, "limit", limit)
	cmd := []string{
		r.ytdlpBin,
		"--no-playlist", "--flat-playlist", "-j",
		fmt.Sprintf("ytsearch%d:%s", limit, query),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, cmd[0], cmd[1:]...).Output()
	if err != nil {
		return nil, fmt.Errorf("yt-dlp search: %w", err)
	}

	var results []db.RelatedVideo
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			continue
		}
		dur, _ := data["duration"].(float64)
		durInt := int(dur)
		if durInt > 1200 {
			r.log.Info("Filtering out search result exceeding 20 minutes", "title", strVal(data, "title"), "duration", durInt)
			continue
		}
		results = append(results, db.RelatedVideo{
			ID:       strVal(data, "id"),
			Title:    strVal(data, "title"),
			Uploader: firstNonEmpty(strVal(data, "uploader"), strVal(data, "channel")),
			Duration: durInt,
		})
	}
	return results, nil
}

// StartBackgroundDownload kicks off a yt-dlp download in a goroutine.
// onComplete is called with success=true/false when done.
func (r *Resolver) StartBackgroundDownload(track *ResolvedTrack, onComplete func(bool)) {
	var target string
	webmTarget := filepath.Join(r.cfg.MusicCacheDir, track.VideoID+".webm")
	mkvTarget := filepath.Join(r.cfg.MusicCacheDir, track.VideoID+".mkv")

	if fileExists(webmTarget) {
		target = webmTarget
	} else if fileExists(mkvTarget) {
		target = mkvTarget
	} else {
		target = webmTarget
	}

	// Already on disk
	if fileExists(target) {
		_ = r.db.MarkFileDownloaded(track.Query, target)

		// Ensure entry exists in media_cache as well
		var size int64
		if fi, err := os.Stat(target); err == nil {
			size = fi.Size()
		}
		mediaEntry := db.MediaCacheEntry{
			VideoID:         track.VideoID,
			Title:           track.Title,
			FilePath:        target,
			FileSizeBytes:   size,
			DurationSeconds: track.Duration,
			LastAccessedAt:  time.Now(),
			CreatedAt:       time.Now(),
		}
		if err := r.db.UpsertMediaCache(mediaEntry); err != nil {
			r.log.Warn("Failed to heal media cache entry", "videoID", track.VideoID, "err", err)
		}

		if onComplete != nil {
			onComplete(true)
		}
		return
	}

	// Already in progress
	if _, loaded := r.activeDownloads.LoadOrStore(track.VideoID, struct{}{}); loaded {
		r.log.Info("BG download already in progress", "videoID", track.VideoID)
		return
	}

	go r.downloadWorker(track, target, onComplete)
}

// ── Internals ─────────────────────────────────────────────────────────────────

func (r *Resolver) searchYtDlp(query string) (*ResolvedTrack, error) {
	args := []string{
		"--js-runtimes", "node",
		"--no-playlist", "-j", "--skip-download",
		"ytsearch1:" + query,
	}
	r.log.Info("Resolver exec", "bin", r.ytdlpBin, "args", args)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, r.ytdlpBin, args...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		r.log.Debug("yt-dlp stderr", "output", stderr.String())
		return nil, fmt.Errorf("yt-dlp: %w", err)
	}
	if stderr.Len() > 0 {
		r.log.Debug("yt-dlp stderr", "output", stderr.String())
	}

	raw := strings.TrimSpace(stdout.String())
	if raw == "" {
		return nil, fmt.Errorf("yt-dlp returned no output")
	}

	// Last non-empty line starting with '{'
	jsonLine := ""
	for _, ln := range reverseLines(raw) {
		if strings.HasPrefix(strings.TrimSpace(ln), "{") {
			jsonLine = ln
			break
		}
	}
	if jsonLine == "" {
		return nil, fmt.Errorf("yt-dlp output had no JSON line")
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(jsonLine), &data); err != nil {
		return nil, fmt.Errorf("yt-dlp JSON parse: %w", err)
	}

	videoID := strVal(data, "id")
	thumbURL := strVal(data, "thumbnail")
	if thumbURL == "" {
		if thumbs, ok := data["thumbnails"].([]any); ok && len(thumbs) > 0 {
			if last, ok := thumbs[len(thumbs)-1].(map[string]any); ok {
				thumbURL = strVal(last, "url")
			}
		}
	}

	related := r.fetchRelatedVideos(videoID)
	dur, _ := data["duration"].(float64)

	webURL := strVal(data, "webpage_url")
	if webURL == "" {
		webURL = strVal(data, "url")
	}

	return &ResolvedTrack{
		Query:         query,
		VideoID:       videoID,
		Title:         firstNonEmpty(strVal(data, "title"), "Unknown"),
		Uploader:      firstNonEmpty(strVal(data, "uploader"), strVal(data, "channel")),
		Duration:      int(dur),
		WebpageURL:    webURL,
		ThumbnailURL:  thumbURL,
		RelatedVideos: related,
	}, nil
}

// fetchRelatedVideos coordinates recommendation fetching, trying YouTube Music first,
// and falling back to standard YouTube if YTM returns nothing.
func (r *Resolver) fetchRelatedVideos(videoID string) []db.RelatedVideo {
	if videoID == "" {
		return nil
	}

	var related []db.RelatedVideo
	// 1. Try YouTube Music first
	related = r.fetchYTMRecommendations(videoID)
	if len(related) > 0 {
		r.log.Info("Fetched related videos from YouTube Music", "videoID", videoID, "count", len(related))
	} else {
		r.log.Warn("YouTube Music recommendations returned empty; falling back to standard YouTube watch page", "videoID", videoID)
		// 2. Fallback to standard YouTube watch page
		related = r.fetchStandardYTRecommendations(videoID)
	}

	// Filter out recommendations exceeding 20 minutes (1200 seconds)
	var filtered []db.RelatedVideo
	for _, v := range related {
		if v.Duration > 0 && v.Duration > 1200 {
			r.log.Info("Filtering out related track exceeding 20 minutes", "title", v.Title, "duration", v.Duration)
			continue
		}
		filtered = append(filtered, v)
	}
	return filtered
}

// fetchYTMRecommendations scrapes ytInitialData from the YouTube Music watch page.
func (r *Resolver) fetchYTMRecommendations(videoID string) []db.RelatedVideo {
	watchURL := "https://music.youtube.com/watch?v=" + videoID

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", watchURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		r.log.Warn("Failed to fetch YouTube Music page", "videoID", videoID, "err", err)
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	html := string(body)

	jsonStr := extractYtInitialData(html)
	if jsonStr == "" {
		return nil
	}

	var ytData map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &ytData); err != nil {
		return nil
	}

	panelVideos := findKeyRecursive(ytData, "playlistPanelVideoRenderer")
	var related []db.RelatedVideo

	for _, raw := range panelVideos {
		renderer, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		contentID := strVal(renderer, "videoId")
		if contentID == "" || contentID == videoID {
			continue
		}

		title := firstNonEmpty(getText(renderer, "title"), "Unknown")
		uploader := firstNonEmpty(getText(renderer, "shortBylineText"), getText(renderer, "longBylineText"), "Unknown")
		durationStr := getText(renderer, "lengthText")
		duration := 0
		if durationStr != "" {
			duration = parseDuration(durationStr)
		}

		related = append(related, db.RelatedVideo{
			ID:       contentID,
			Title:    title,
			Uploader: uploader,
			Duration: duration,
		})
	}

	return related
}

// fetchStandardYTRecommendations scrapes ytInitialData from the standard YouTube watch page.
func (r *Resolver) fetchStandardYTRecommendations(videoID string) []db.RelatedVideo {
	watchURL := "https://www.youtube.com/watch?v=" + videoID

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", watchURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		r.log.Warn("Failed to fetch YouTube page for related videos", "videoID", videoID, "err", err)
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	html := string(body)

	// Extract ytInitialData JSON using brace counting (robust against multiline)
	jsonStr := extractYtInitialData(html)
	if jsonStr == "" {
		r.log.Warn("Could not find ytInitialData in HTML", "videoID", videoID)
		return nil
	}

	var ytData map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &ytData); err != nil {
		r.log.Warn("Failed to parse ytInitialData JSON", "videoID", videoID, "err", err)
		return nil
	}

	lockups := findKeyRecursive(ytData, "lockupViewModel")
	var related []db.RelatedVideo

	for _, raw := range lockups {
		lv, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		contentID := strVal(lv, "contentId")
		contentType := strVal(lv, "contentType")
		if contentType != "LOCKUP_CONTENT_TYPE_VIDEO" || contentID == "" || contentID == videoID {
			continue
		}

		// Title
		title := "Unknown"
		if meta, ok := lv["metadata"].(map[string]any); ok {
			if lmMeta, ok := meta["lockupMetadataViewModel"].(map[string]any); ok {
				if t, ok := lmMeta["title"].(map[string]any); ok {
					title = firstNonEmpty(strVal(t, "content"), "Unknown")
				}
			}
		}

		// Uploader
		uploader := "Unknown"
		if meta, ok := lv["metadata"].(map[string]any); ok {
			if lmMeta, ok := meta["lockupMetadataViewModel"].(map[string]any); ok {
				if metaRows, ok := getPath(lmMeta, "metadata", "contentMetadataViewModel", "metadataRows").([]any); ok && len(metaRows) > 0 {
					if row0, ok := metaRows[0].(map[string]any); ok {
						if parts, ok := row0["metadataParts"].([]any); ok && len(parts) > 0 {
							if p0, ok := parts[0].(map[string]any); ok {
								if txt, ok := p0["text"].(map[string]any); ok {
									uploader = firstNonEmpty(strVal(txt, "content"), "Unknown")
								}
							}
						}
					}
				}
			}
		}

		// Duration
		duration := 0
		overlays := findKeyRecursive(lv, "thumbnailBottomOverlayViewModel")
		if len(overlays) > 0 {
			if ov, ok := overlays[0].(map[string]any); ok {
				if badges, ok := ov["badges"].([]any); ok && len(badges) > 0 {
					if b0, ok := badges[0].(map[string]any); ok {
						if bvm, ok := b0["thumbnailBadgeViewModel"].(map[string]any); ok {
							if ds := strVal(bvm, "text"); ds != "" {
								duration = parseDuration(ds)
							}
						}
					}
				}
			}
		}

		related = append(related, db.RelatedVideo{
			ID:       contentID,
			Title:    title,
			Uploader: uploader,
			Duration: duration,
		})
	}

	r.log.Info("Fetched related videos from standard YouTube", "videoID", videoID, "count", len(related))
	return related
}

// getText parses YouTube-formatted text objects (supporting simpleText or runs).
func getText(m map[string]any, key string) string {
	obj, ok := m[key].(map[string]any)
	if !ok {
		return ""
	}
	if s, ok := obj["simpleText"].(string); ok && s != "" {
		return s
	}
	if runs, ok := obj["runs"].([]any); ok && len(runs) > 0 {
		if r0, ok := runs[0].(map[string]any); ok {
			if s, ok := r0["text"].(string); ok {
				return s
			}
		}
	}
	return ""
}

func (r *Resolver) downloadThumbnail(thumbURL, videoID string) string {
	if thumbURL == "" {
		return ""
	}
	target := filepath.Join(r.cfg.MusicCacheDir, videoID+".jpg")
	if fileExists(target) {
		return target
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", thumbURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := client.Do(req)
	if err != nil {
		r.log.Warn("Failed to download thumbnail", "url", thumbURL, "err", err)
		return ""
	}
	defer resp.Body.Close()

	f, err := os.Create(target)
	if err != nil {
		return ""
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		os.Remove(target)
		return ""
	}
	r.log.Info("Thumbnail downloaded", "videoID", videoID, "path", target)
	return target
}

func (r *Resolver) downloadWorker(track *ResolvedTrack, target string, onComplete func(bool)) {
	defer r.activeDownloads.Delete(track.VideoID)

	r.log.Info("BG download starting", "title", track.Title, "target", target)
	args := []string{
		"--js-runtimes", "node",
		"--no-playlist", "-f", "bestaudio/best",
		"-o", target,
		track.WebpageURL,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, r.ytdlpBin, args...)
	out, err := cmd.CombinedOutput()
	success := false
	if err == nil && fileExists(target) {
		r.log.Info("BG download complete", "target", target)
		_ = r.db.MarkFileDownloaded(track.Query, target)
		track.FilePath = target

		// Ensure entry exists in media_cache as well
		var size int64
		if fi, err := os.Stat(target); err == nil {
			size = fi.Size()
		}
		mediaEntry := db.MediaCacheEntry{
			VideoID:         track.VideoID,
			Title:           track.Title,
			FilePath:        target,
			FileSizeBytes:   size,
			DurationSeconds: track.Duration,
			LastAccessedAt:  time.Now(),
			CreatedAt:       time.Now(),
		}
		if err := r.db.UpsertMediaCache(mediaEntry); err != nil {
			r.log.Warn("Failed to insert media cache entry", "videoID", track.VideoID, "err", err)
		}

		success = true
	} else {
		r.log.Warn("BG download failed", "err", err, "output", string(out))
	}

	if onComplete != nil {
		onComplete(success)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

var ytInitialDataRe = regexp.MustCompile(`(?:var\s+)?ytInitialData\s*=\s*`)

// extractYtInitialData finds ytInitialData in the HTML and extracts the JSON
// by brace-counting (robust, doesn't rely on a trailing semicolon + DOTALL regex).
func extractYtInitialData(html string) string {
	loc := ytInitialDataRe.FindStringIndex(html)
	if loc == nil {
		return ""
	}
	start := loc[1]
	if start >= len(html) || html[start] != '{' {
		return ""
	}

	depth := 0
	inStr := false
	escaped := false
	for i := start; i < len(html); i++ {
		c := html[i]
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' && inStr {
			escaped = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 {
				return html[start : i+1]
			}
		}
	}
	return ""
}

// findKeyRecursive recursively finds all values for a given key in a JSON-like structure.
func findKeyRecursive(v any, key string) []any {
	var results []any
	switch val := v.(type) {
	case map[string]any:
		if found, ok := val[key]; ok {
			results = append(results, found)
		}
		for _, child := range val {
			results = append(results, findKeyRecursive(child, key)...)
		}
	case []any:
		for _, item := range val {
			results = append(results, findKeyRecursive(item, key)...)
		}
	}
	return results
}

// getPath walks a nested map using variadic string keys.
func getPath(m map[string]any, keys ...string) any {
	var cur any = m
	for _, k := range keys {
		if cm, ok := cur.(map[string]any); ok {
			cur = cm[k]
		} else {
			return nil
		}
	}
	return cur
}

func parseDuration(s string) int {
	parts := strings.Split(s, ":")
	switch len(parts) {
	case 2:
		m, _ := strconv.Atoi(parts[0])
		sec, _ := strconv.Atoi(parts[1])
		return m*60 + sec
	case 3:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		sec, _ := strconv.Atoi(parts[2])
		return h*3600 + m*60 + sec
	}
	return 0
}

func reverseLines(s string) []string {
	lines := strings.Split(s, "\n")
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}
	return lines
}

func strVal(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func ytThumbURL(videoID string) string {
	return "https://i.ytimg.com/vi/" + url.PathEscape(videoID) + "/hqdefault.jpg"
}

func normalize(q string) string {
	return strings.ToLower(strings.TrimSpace(q))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func songRowToTrack(row *db.SongRow, query string) *ResolvedTrack {
	q := query
	if q == "" {
		q = row.Query
	}
	return &ResolvedTrack{
		Query:         q,
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

func trackToSongRow(t *ResolvedTrack) *db.SongRow {
	return &db.SongRow{
		Query:         t.Query,
		VideoID:       t.VideoID,
		Title:         t.Title,
		Uploader:      t.Uploader,
		Duration:      t.Duration,
		WebpageURL:    t.WebpageURL,
		FilePath:      t.FilePath,
		ThumbnailPath: t.ThumbnailPath,
		ThumbnailURL:  t.ThumbnailURL,
		RelatedVideos: t.RelatedVideos,
	}
}
