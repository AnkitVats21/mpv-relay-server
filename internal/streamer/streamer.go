package streamer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/resolver"
	"github.com/ankitm/mpv-relay/internal/resource"
)

// MQTTPublisher is the minimal interface the streamer needs to publish events.
type MQTTPublisher interface {
	Publish(payload map[string]any)
	PublishRaw(topic string, data []byte, retain ...bool)
}

// Streamer manages the ffmpeg producer, chunk ring buffer, and HTTP endpoints.
type Streamer struct {
	db             *db.DB
	rm             *resource.ResourceManager
	session        *LiveSession
	mqtt           MQTTPublisher
	log            *slog.Logger
	mediaDir       string // local cache directory
	streamerURL    string // base URL for chunk requests (e.g. http://host:9000)
	httpStreamPort int
	isDownloading  func(string) bool
	startDownload  func(*resolver.ResolvedTrack, func(bool))

	mu           sync.Mutex
	connChan     chan struct{}      // signals manifest handler that producer started
	ring         *ChunkRing        // active ring buffer; replaced on each new stream
	producerCtx  context.Context   // cancelled to stop the running producer
	producerStop context.CancelFunc
}

func New(database *db.DB, rm *resource.ResourceManager, mqtt MQTTPublisher, mediaDir string, httpStreamPort int, isDownloading func(string) bool, startDownload func(*resolver.ResolvedTrack, func(bool))) *Streamer {
	url := os.Getenv("STREAMER_URL")
	if url == "" {
		url = "http://localhost:9000"
	}
	return &Streamer{
		db:             database,
		rm:             rm,
		session:        &LiveSession{},
		mqtt:           mqtt,
		log:            slog.Default().With("pkg", "streamer"),
		mediaDir:       mediaDir,
		streamerURL:    url,
		httpStreamPort: httpStreamPort,
		isDownloading:  isDownloading,
		startDownload:  startDownload,
	}
}

// RegisterHTTPHandler mounts /stream/manifest, /stream/chunk, and /stream on mux.
func (s *Streamer) RegisterHTTPHandler(mux *http.ServeMux) {
	mux.HandleFunc("/stream/manifest", s.handleManifest)
	mux.HandleFunc("/stream/chunk", s.handleChunk)
	mux.HandleFunc("/stream", s.handleStream)
}

// ── Public API used by queue.Manager ──────────────────────────────────────────

// StartStream issues a token, publishes START_STREAM via MQTT, and returns
// immediately (the producer starts when the ESP hits /stream/manifest).
//
// fromChunk > 0 causes the manifest URL to include &from_chunk=N so the ESP
// resumes from that chunk index (used for pause/resume).
func (s *Streamer) StartStream(videoID, title string, fromChunk ...uint32) error {
	s.session.Lock()
	s.session.ActiveVideoID = videoID
	s.session.Unlock()

	// ── New Ogg streaming support ──
	baseURL := s.getStreamerBaseURL()
	playbackURL := fmt.Sprintf("%s/stream?song_id=%s", baseURL, videoID)

	filePath, _ := s.resolveCachedPath(videoID)
	if filePath != "" && fileExists(filePath) {
		s.log.Info("Ogg Stream: song is already cached on disk, publishing URL immediately", "videoID", videoID, "path", filePath)
		s.publishMediaUrl(videoID, playbackURL)
		return nil
	}

	s.log.Info("Ogg Stream: song is not cached on disk, initiating background download", "videoID", videoID)
	track := &resolver.ResolvedTrack{
		VideoID:    videoID,
		Title:      title,
		WebpageURL: "https://www.youtube.com/watch?v=" + videoID,
	}

	// Trigger the download asynchronously (saves in high-quality format)
	go func() {
		s.startDownload(track, nil)
	}()

	// Wait in a separate goroutine until "enough data" is available to stream
	go func() {
		started := time.Now()
		var candidatePath string
		threshold := int64(200 * 1024) // 200 KB

		for {
			if time.Since(started) > 15*time.Second {
				s.log.Error("Ogg Stream: timeout waiting for enough data to start streaming", "videoID", videoID)
				return
			}

			// Check if active song changed while we were waiting
			if s.GetActiveVideoID() != videoID {
				s.log.Info("Ogg Stream: active song changed during download wait, cancelling publish", "videoID", videoID)
				return
			}

			if candidatePath == "" {
				candidatePath = s.findOriginalFile(videoID)
			}

			if candidatePath != "" {
				if fi, err := os.Stat(candidatePath); err == nil {
					if fi.Size() >= threshold {
						break
					}
				}
			}
			time.Sleep(200 * time.Millisecond)
		}

		// Double check if active song changed
		if s.GetActiveVideoID() != videoID {
			s.log.Info("Ogg Stream: active song changed during download wait, cancelling publish", "videoID", videoID)
			return
		}

		s.log.Info("Ogg Stream: enough data available, publishing playback URL", "videoID", videoID, "url", playbackURL)
		s.publishMediaUrl(videoID, playbackURL)
	}()

	return nil
}

func (s *Streamer) findOriginalFile(videoID string) string {
	for _, ext := range []string{".mkv", ".webm", ".m4a", ".mp3", ".opus", ".ogg", ".wav"} {
		p := filepath.Join(s.mediaDir, videoID+ext)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func (s *Streamer) publishMediaUrl(videoID, url string) {
	payload := map[string]any{
		"song_id":  videoID,
		"song_url": url,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		s.log.Error("publishMediaUrl: failed to marshal payload", "err", err)
		return
	}
	s.mqtt.PublishRaw("device/waveshare/media", data)
}

func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func (s *Streamer) getStreamerBaseURL() string {
	scheme := "http"
	if os.Getenv("STREAMER_HTTPS") == "true" {
		scheme = "https"
	}

	if s.streamerURL != "" {
		rawURL := s.streamerURL
		if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
			rawURL = scheme + "://" + rawURL
		}

		if u, err := url.Parse(rawURL); err == nil {
			host := u.Hostname()
			if host != "" && host != "localhost" && host != "127.0.0.1" && host != "0.0.0.0" {
				if scheme == "https" {
					u.Scheme = "https"
					u.Host = host // strips port if present
				}
				return strings.TrimSuffix(u.String(), "/")
			}
		}
	}

	if scheme == "https" {
		return fmt.Sprintf("https://%s", getLocalIP())
	}
	return fmt.Sprintf("http://%s:%d", getLocalIP(), s.httpStreamPort)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ResumeStream resumes from the saved LastChunk in the paused session.
func (s *Streamer) ResumeStream() error {
	videoID, title, lastChunk, ok := s.GetPausedSession()
	if !ok {
		return fmt.Errorf("no paused session to resume")
	}
	s.log.Info("Resuming stream", "videoID", videoID, "fromChunk", lastChunk)
	return s.StartStream(videoID, title, lastChunk)
}

// CancelStream stops the currently running producer (used by Pause).
func (s *Streamer) CancelStream() {
	s.stopProducer()
}

// SavePause records the exact chunk index at pause time (exact, no heuristic).
func (s *Streamer) SavePause(lastChunk uint32) {
	s.session.Lock()
	s.session.State = SessionPaused
	s.session.LastChunk = lastChunk
	s.session.SessionToken = ""
	s.session.PausedAt = time.Now()
	s.session.Unlock()
}

// GetPausedSession returns (videoID, title, lastChunk, true) when the session
// is paused, or zero values + false otherwise.
func (s *Streamer) GetPausedSession() (videoID, title string, lastChunk uint32, ok bool) {
	s.session.RLock()
	defer s.session.RUnlock()
	if s.session.State != SessionPaused || s.session.ActiveVideoID == "" {
		return "", "", 0, false
	}
	return s.session.ActiveVideoID, s.session.ActiveTitle, s.session.LastChunk, true
}

// GetSessionInfo returns session metadata for status publishing.
func (s *Streamer) GetSessionInfo() (videoID string, bytesSent int64, uptime time.Duration) {
	s.session.RLock()
	defer s.session.RUnlock()
	var up time.Duration
	if !s.session.StartedAt.IsZero() {
		up = time.Since(s.session.StartedAt)
	}
	// bytesSent is approximated from chunks delivered
	ring := s.currentRing()
	if ring != nil {
		bytesSent = int64(ring.WindowStart()) * ChunkSize
	}
	return s.session.ActiveVideoID, bytesSent, up
}

// GetActiveVideoID returns the currently active video ID.
func (s *Streamer) GetActiveVideoID() string {
	s.session.RLock()
	defer s.session.RUnlock()
	return s.session.ActiveVideoID
}

// ── Producer management ────────────────────────────────────────────────────────

func (s *Streamer) currentRing() *ChunkRing {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring
}

// startProducer launches the ffmpeg goroutine into a new ChunkRing.
// Any existing producer is stopped first.
func (s *Streamer) startProducer(filePath, videoID string, seekSec float64, fromChunk uint32) {
	s.stopProducer()

	ctx, cancel := context.WithCancel(context.Background())

	ring := newChunkRing()

	s.mu.Lock()
	s.ring = ring
	s.producerCtx = ctx
	s.producerStop = cancel
	s.mu.Unlock()

	go s.runProducer(ctx, ring, filePath, videoID, seekSec, fromChunk)
}

// stopProducer cancels the producer context and waits for it to exit.
func (s *Streamer) stopProducer() {
	s.mu.Lock()
	cancel := s.producerStop
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// runProducer reads ffmpeg stdout and fills the ring buffer.
func (s *Streamer) runProducer(ctx context.Context, ring *ChunkRing, filePath, videoID string, seekSec float64, fromChunk uint32) {
	defer ring.Done()

	var stderrBuf bytes.Buffer

	if filePath != "" {
		c := BuildFFmpegFromFile(ctx, filePath, seekSec)
		c.Stderr = &stderrBuf
		pipe, err := c.StdoutPipe()
		if err != nil {
			s.log.Error("ffmpeg StdoutPipe failed", "err", err)
			return
		}
		if err := c.Start(); err != nil {
			s.log.Error("ffmpeg Start failed", "err", err)
			return
		}
		s.fillRing(ctx, ring, pipe, fromChunk)
		if werr := c.Wait(); werr != nil && ctx.Err() == nil {
			s.log.Error("ffmpeg exited with error", "err", werr, "stderr", stderrBuf.String())
		}
	} else {
		// Cache miss: yt-dlp | ffmpeg
		ytCmd := BuildYtDlpStream(ctx, videoID)
		ffCmd := BuildFFmpegFromStdin(ctx)

		ytPipe, err := ytCmd.StdoutPipe()
		if err != nil {
			s.log.Error("yt-dlp StdoutPipe failed", "err", err)
			return
		}
		ffCmd.Stdin = ytPipe

		ffPipe, err := ffCmd.StdoutPipe()
		if err != nil {
			s.log.Error("ffmpeg StdoutPipe failed", "err", err)
			return
		}

		var ytErr, ffStderr bytes.Buffer
		ytCmd.Stderr = &ytErr
		ffCmd.Stderr = &ffStderr

		if err := ytCmd.Start(); err != nil {
			s.log.Error("yt-dlp Start failed", "err", err)
			return
		}
		if err := ffCmd.Start(); err != nil {
			s.log.Error("ffmpeg Start failed", "err", err)
			_ = ytCmd.Process.Kill()
			return
		}

		s.fillRing(ctx, ring, ffPipe, fromChunk)

		if err := ytCmd.Wait(); err != nil && ctx.Err() == nil {
			s.log.Error("yt-dlp exited with error", "err", err, "stderr", ytErr.String())
			s.mqtt.Publish(map[string]any{
				"type":     "PLAYBACK_FAILED",
				"cmd":      "PLAYBACK_FAILED",
				"video_id": videoID,
				"error":    fmt.Sprintf("yt-dlp: %v", err),
			})
		}
		if err := ffCmd.Wait(); err != nil && ctx.Err() == nil {
			s.log.Error("ffmpeg exited with error", "err", err, "stderr", ffStderr.String())
		}
	}

	s.log.Info("Producer finished", "fromChunk", fromChunk)
}

// fillRing reads PCM bytes from r and pushes fixed-size chunks into the ring.
// fromChunk is the absolute index of the first chunk (for resume support).
func (s *Streamer) fillRing(ctx context.Context, ring *ChunkRing, r io.Reader, fromChunk uint32) {
	buf := make([]byte, ChunkSize)
	index := fromChunk

	for {
		if ctx.Err() != nil {
			return
		}

		n, err := io.ReadFull(r, buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			isLast := err == io.EOF || err == io.ErrUnexpectedEOF
			ring.Add(Chunk{Index: index, Data: data, IsLast: isLast})
			index++
		}
		if err != nil {
			return
		}
	}
}

// ── Expiry worker ──────────────────────────────────────────────────────────────

// StartExpiryWorker starts a goroutine that clears PAUSED sessions older than
// the configured timeout (default 30 minutes, env PAUSED_SESSION_TIMEOUT_MINUTES).
func (s *Streamer) StartExpiryWorker(ctx context.Context) {
	timeout := 30
	if v := os.Getenv("PAUSED_SESSION_TIMEOUT_MINUTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeout = n
		}
	}
	dur := time.Duration(timeout) * time.Minute

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.session.RLock()
				state := s.session.State
				pausedAt := s.session.PausedAt
				s.session.RUnlock()

				if state == SessionPaused && !pausedAt.IsZero() && time.Since(pausedAt) > dur {
					s.log.Info("Expiring stale PAUSED session",
						"paused_for", time.Since(pausedAt).Round(time.Second))
					s.ClearSession()
					s.mqtt.Publish(map[string]any{
						"type":   "state",
						"state":  "stopped",
						"reason": "pause_timeout",
					})
				}
			}
		}
	}()
}
