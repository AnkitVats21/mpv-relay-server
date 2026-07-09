package streamer

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/resource"
)

type MQTTPublisher interface {
	Publish(payload map[string]any)
}

type Streamer struct {
	db          *db.DB
	rm          *resource.ResourceManager
	session     *LiveSession
	mqtt        MQTTPublisher
	log         *slog.Logger
	mediaDir    string // path to local cache dir (from config)
	streamerURL string // server base URL

	mu       sync.Mutex
	connChan chan struct{}
}

func New(database *db.DB, rm *resource.ResourceManager, mqtt MQTTPublisher, mediaDir string) *Streamer {
	url := os.Getenv("STREAMER_URL")
	if url == "" {
		url = "http://localhost:9000"
	}

	return &Streamer{
		db:          database,
		rm:          rm,
		session:     &LiveSession{},
		mqtt:        mqtt,
		log:         slog.Default().With("pkg", "streamer"),
		mediaDir:    mediaDir,
		streamerURL: url,
	}
}

// RegisterHTTPHandler mounts GET /stream on the given *http.ServeMux
func (s *Streamer) RegisterHTTPHandler(mux *http.ServeMux) {
	mux.HandleFunc("/stream", s.handleStream)
}

// StartStream is called by the queue coordinator when a track is up next.
// It issues a token, publishes MQTT START_STREAM with the token + server URL,
// and waits for the ESP32 to connect (or times out after 10s).
func (s *Streamer) StartStream(videoID, title string) error {
	s.mu.Lock()
	if s.connChan != nil {
		// Close existing waiting channel to notify any stale waiters
		close(s.connChan)
	}
	s.connChan = make(chan struct{}, 1)
	ch := s.connChan
	s.mu.Unlock()

	token, err := s.IssueToken(videoID)
	if err != nil {
		return fmt.Errorf("failed to issue token: %w", err)
	}

	streamURL := fmt.Sprintf("%s/stream?token=%s", s.streamerURL, token)
	s.log.Info("Starting stream, publishing MQTT START_STREAM", "videoID", videoID, "token", token, "url", streamURL)

	payload := map[string]any{
		"type":     "START_STREAM",
		"cmd":      "START_STREAM",
		"video_id": videoID,
		"title":    title,
		"token":    token,
		"url":      streamURL,
	}
	s.mqtt.Publish(payload)

	// Wait for the ESP32 to connect (or times out after 10s)
	select {
	case <-ch:
		s.log.Info("ESP32 client connected successfully")
		return nil
	case <-time.After(10 * time.Second):
		s.log.Warn("Timeout waiting for ESP32 connection")
		s.ClearSession()
		return fmt.Errorf("timeout waiting for ESP32 connection")
	}
}
