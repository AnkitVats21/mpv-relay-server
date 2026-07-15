package streamer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ankitm/mpv-relay/internal/db"
)

// Manifest is the JSON payload returned by GET /stream/manifest.
type Manifest struct {
	Token          string `json:"token"`
	VideoID        string `json:"video_id"`
	Title          string `json:"title"`
	ChunkSizeBytes int    `json:"chunk_size_bytes"`
	ChunkDurMs     int    `json:"chunk_duration_ms"`
	ByteRate       int    `json:"byte_rate"`
	SampleRate     int    `json:"sample_rate"`
	Channels       int    `json:"channels"`
	Format         string `json:"format"`
	FromChunk      uint32 `json:"from_chunk"`
	TotalChunks    int    `json:"total_chunks"` // -1 if unknown (live stream)
}

// handleManifest serves GET /stream/manifest?token=T[&from_chunk=N].
//
// It validates the token, resolves the cached file path, starts the ffmpeg
// producer goroutine, and returns a JSON Manifest so the ESP knows the chunk
// parameters before it starts requesting individual chunks.
func (s *Streamer) handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	clientIP := extractIP(r)

	if !s.ValidateToken(token, clientIP) {
		s.log.Warn("Manifest: token validation failed", "token", token, "clientIP", clientIP)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	fromChunk := uint32(0)
	if fc := r.URL.Query().Get("from_chunk"); fc != "" {
		if v, err := strconv.ParseUint(fc, 10, 32); err == nil {
			fromChunk = uint32(v)
		}
	}

	s.session.RLock()
	videoID := s.session.ActiveVideoID
	title := s.session.ActiveTitle
	s.session.RUnlock()

	s.log.Info("Manifest request", "videoID", videoID, "fromChunk", fromChunk, "clientIP", clientIP)

	// Touch LRU timestamp
	_ = s.db.TouchMediaCache(videoID)

	// Resolve cached file path
	cachedPath, durationSec := s.resolveCachedPath(videoID)

	// Compute seek time from chunk index
	seekSec := 0.0
	if fromChunk > 0 {
		seekSec = float64(fromChunk) * float64(ChunkDurationMs) / 1000.0
	}

	// Start (or restart) the ffmpeg producer into a fresh ring buffer
	s.startProducer(cachedPath, videoID, seekSec, fromChunk)

	// Compute total_chunks: known only for cached files with a valid duration
	totalChunks := -1
	if durationSec > 0 {
		totalChunks = int(math.Ceil(durationSec * float64(BytesPerSec) / float64(ChunkSize)))
	}

	manifest := Manifest{
		Token:          token,
		VideoID:        videoID,
		Title:          title,
		ChunkSizeBytes: ChunkSize,
		ChunkDurMs:     ChunkDurationMs,
		ByteRate:       BytesPerSec,
		SampleRate:     32000,
		Channels:       1,
		Format:         "pcm_s16le",
		FromChunk:      fromChunk,
		TotalChunks:    totalChunks,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(manifest)
}

// handleChunk serves GET /stream/chunk?token=T&index=N.
//
// It blocks until chunk N is available in the ring buffer, then writes the raw
// PCM bytes. Returns 410 if the chunk was evicted, 204 if the stream ended.
func (s *Streamer) handleChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	clientIP := extractIP(r)

	if !s.ValidateToken(token, clientIP) {
		s.log.Warn("Chunk: token validation failed", "token", token, "clientIP", clientIP)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	indexStr := r.URL.Query().Get("index")
	index64, err := strconv.ParseUint(indexStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid index", http.StatusBadRequest)
		return
	}
	index := uint32(index64)

	ring := s.currentRing()
	if ring == nil {
		// Producer not started yet (manifest not called first, or session cleared)
		http.Error(w, "no active stream", http.StatusServiceUnavailable)
		return
	}

	chunk, err := ring.Get(r.Context(), index)
	switch err {
	case nil:
		// served below
	case ErrEvicted:
		s.log.Warn("Chunk evicted", "index", index,
			"windowStart", ring.WindowStart(), "windowEnd", ring.NextWrite())
		http.Error(w, "chunk evicted — reconnect from current window", http.StatusGone)
		return
	case ErrDone:
		// Stream ended naturally; no more chunks
		w.WriteHeader(http.StatusNoContent)
		return
	default:
		// Context cancelled (client disconnected)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Chunk-Index", strconv.FormatUint(uint64(index), 10))
	w.Header().Set("X-Chunk-Duration-Ms", strconv.Itoa(ChunkDurationMs))
	w.Header().Set("X-Last-Chunk", strconv.FormatBool(chunk.IsLast))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(chunk.Data)

	// If this was the last chunk, clear the session so the queue manager can
	// advance to the next track. The queue's monitorStreamCompletion detects
	// the resource gate being released by the (now-finished) producer.
	if chunk.IsLast {
		s.log.Info("Last chunk served — stream complete", "index", index)
		s.ClearSession()
	}
}

// ── helpers ────────────────────────────────────────────────────────────────────

// resolveCachedPath returns the local file path and duration for videoID.
// Returns ("", 0) if no cached file exists (caller will use yt-dlp).
func (s *Streamer) resolveCachedPath(videoID string) (filePath string, durationSec float64) {
	// 1. media_cache table (preferred — has duration)
	if entry, err := s.db.LookupMediaCache(videoID); err == nil && entry != nil && entry.FilePath != "" {
		if _, err := os.Stat(entry.FilePath); err == nil {
			return entry.FilePath, float64(entry.DurationSeconds)
		}
	}

	// 2. song_cache table (fallback)
	if row, err := s.db.LookupVideoID(videoID); err == nil && row != nil && row.FilePath != "" {
		if _, err := os.Stat(row.FilePath); err == nil {
			// Found in song_cache but not media_cache. Heal media_cache!
			var size int64
			if fi, err := os.Stat(row.FilePath); err == nil {
				size = fi.Size()
			}
			mediaEntry := db.MediaCacheEntry{
				VideoID:         videoID,
				Title:           row.Title,
				FilePath:        row.FilePath,
				FileSizeBytes:   size,
				DurationSeconds: row.Duration,
				LastAccessedAt:  time.Now(),
				CreatedAt:       time.Now(),
			}
			_ = s.db.UpsertMediaCache(mediaEntry)

			return row.FilePath, float64(row.Duration)
		}
	}

	// 3. Filesystem scan of mediaDir
	for _, ext := range []string{".mkv", ".webm", ".opus", ".m4a", ".mp3"} {
		p := filepath.Join(s.mediaDir, videoID+ext)
		if _, err := os.Stat(p); err == nil {
			// Found on disk, but missing from both db tables. Heal both!
			var size int64
			if fi, err := os.Stat(p); err == nil {
				size = fi.Size()
			}
			title := videoID
			duration := 0

			// 3a. Save to song_cache
			newSong := &db.SongRow{
				Query:      videoID,
				VideoID:    videoID,
				Title:      title,
				Duration:   duration,
				FilePath:   p,
				LastPlayed: float64(time.Now().UnixMilli()) / 1000.0,
			}
			_ = s.db.SaveSong(newSong)

			// 3b. Save to media_cache
			mediaEntry := db.MediaCacheEntry{
				VideoID:         videoID,
				Title:           title,
				FilePath:        p,
				FileSizeBytes:   size,
				DurationSeconds: duration,
				LastAccessedAt:  time.Now(),
				CreatedAt:       time.Now(),
			}
			_ = s.db.UpsertMediaCache(mediaEntry)

			return p, 0
		}
	}

	return "", 0 // cache miss → yt-dlp
}

// extractIP pulls the real client IP from standard proxy headers or RemoteAddr.
func extractIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if idx := strings.Index(v, ","); idx != -1 {
			return strings.TrimSpace(v[:idx])
		}
		return strings.TrimSpace(v)
	}
	if v := r.Header.Get("X-Real-IP"); v != "" {
		return strings.TrimSpace(v)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// SetCancelFn and CancelStream are kept for compatibility with resource manager.
func (s *Streamer) SetCancelFn(_ context.CancelFunc) {}
func (s *Streamer) cancelStream()                     { s.stopProducer() }

// handleStream serves GET /stream?song_id=ID.
// It retrieves the cached original file, starts a transcoding ffmpeg pipeline to mono Opus .ogg,
// and streams it to the client. If the file is still downloading, it will wait for new data.
func (s *Streamer) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	songID := r.URL.Query().Get("song_id")
	if songID == "" {
		http.Error(w, "Missing song_id parameter", http.StatusBadRequest)
		return
	}

	s.log.Info("Stream request for Ogg", "songID", songID)

	// Touch LRU cache
	_ = s.db.TouchMediaCache(songID)

	var origPath string
	// Wait up to 10 seconds for the file to start downloading and be created if it's a cache miss
	started := time.Now()
	for {
		origPath = s.findOriginalFile(songID)
		if origPath != "" {
			break
		}
		if time.Since(started) > 10*time.Second {
			s.log.Error("Stream: timeout waiting for original file to exist", "songID", songID)
			http.Error(w, "Song not found", http.StatusNotFound)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	file, err := os.Open(origPath)
	if err != nil {
		s.log.Error("Stream: failed to open original file", "songID", songID, "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Set headers for Ogg streaming
	w.Header().Set("Content-Type", "audio/ogg")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Transfer-Encoding", "chunked")

	// Start ffmpeg process to transcode on-the-fly to mono Opus in Ogg format
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	ffmpegBin := os.Getenv("FFMPEG_BIN")
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}

	cmd := exec.CommandContext(ctx, ffmpegBin,
		"-i", "-",
		"-c:a", "libopus",
		"-ar", "48000",
		"-ac", "1",
		"-f", "ogg",
		"-",
	)

	pipeIn, err := cmd.StdinPipe()
	if err != nil {
		s.log.Error("Stream: failed to get ffmpeg stdin pipe", "songID", songID, "err", err)
		return
	}

	pipeOut, err := cmd.StdoutPipe()
	if err != nil {
		s.log.Error("Stream: failed to get ffmpeg stdout pipe", "songID", songID, "err", err)
		pipeIn.Close()
		return
	}

	var ffmpegStderr bytes.Buffer
	cmd.Stderr = &ffmpegStderr

	if err := cmd.Start(); err != nil {
		s.log.Error("Stream: failed to start ffmpeg transcoder", "songID", songID, "err", err)
		pipeIn.Close()
		return
	}

	// Goroutine to read from high-quality file (growing if downloading) and write to ffmpeg stdin
	go func() {
		defer pipeIn.Close()
		buf := make([]byte, 8192)
		for {
			n, err := file.Read(buf)
			if n > 0 {
				if _, werr := pipeIn.Write(buf[:n]); werr != nil {
					// ffmpeg stdin closed or broken
					return
				}
			}

			if err == io.EOF {
				// If downloading is still in progress, wait for more data
				if s.isDownloading(songID) {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Final check: read any remaining bytes
				n, err = file.Read(buf)
				if n > 0 {
					pipeIn.Write(buf[:n])
				}
				break
			} else if err != nil {
				s.log.Error("Stream read error", "songID", songID, "err", err)
				break
			}
		}
	}()

	// Read ffmpeg stdout and write directly to http response
	flusher, hasFlush := w.(http.Flusher)
	bufOut := make([]byte, 8192)
	for {
		n, err := pipeOut.Read(bufOut)
		if n > 0 {
			if _, werr := w.Write(bufOut[:n]); werr != nil {
				s.log.Info("Stream client disconnected during playback", "songID", songID)
				cancel()
				break
			}
			if hasFlush {
				flusher.Flush()
			}
		}
		if err == io.EOF {
			break
		} else if err != nil {
			s.log.Error("Stream pipeOut read error", "songID", songID, "err", err)
			break
		}
	}

	// Wait for ffmpeg to exit
	if err := cmd.Wait(); err != nil {
		s.log.Warn("ffmpeg stream transcoder exited", "songID", songID, "err", err, "stderr", ffmpegStderr.String())
	}
	s.log.Info("Stream request for Ogg completed", "songID", songID)
}

// unused — ring fills ring context is producer-owned
var _ context.Context
