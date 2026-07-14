package streamer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/resolver"
	"github.com/ankitm/mpv-relay/internal/resource"
)

type MockMQTT struct {
	mu       sync.Mutex
	payloads []map[string]any
}

func (m *MockMQTT) Publish(p map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payloads = append(m.payloads, p)
}

func (m *MockMQTT) PublishRaw(topic string, data []byte, retain ...bool) {
	var p map[string]any
	_ = json.Unmarshal(data, &p)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payloads = append(m.payloads, map[string]any{
		"raw_topic": topic,
		"raw_data":  p,
	})
}

// helper: create streamer + httptest server
func newTestStreamer(t *testing.T, tempDir string) (*Streamer, *httptest.Server) {
	t.Helper()
	dbPath := filepath.Join(tempDir, "test.db")
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	rm := resource.New()
	mqtt := &MockMQTT{}
	isDownloading := func(videoID string) bool { return false }
	startDownload := func(track *resolver.ResolvedTrack, onComplete func(bool)) {
		if onComplete != nil {
			onComplete(true)
		}
	}
	s := New(database, rm, mqtt, tempDir, 8765, isDownloading, startDownload)

	mux := http.NewServeMux()
	s.RegisterHTTPHandler(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return s, srv
}

func getFFmpegBin(t *testing.T) string {
	t.Helper()
	v := os.Getenv("FFMPEG_BIN")
	if v == "" {
		v = "ffmpeg"
	}
	if _, err := exec.LookPath(v); err != nil {
		t.Skipf("%s not in PATH", v)
	}
	return v
}

// ── TestChunkRing ─────────────────────────────────────────────────────────────

func TestChunkRing_AddGet(t *testing.T) {
	ring := newChunkRing()

	// Add WindowSize+5 chunks, verifying eviction
	total := WindowSize + 5
	for i := 0; i < total; i++ {
		ring.Add(Chunk{Index: uint32(i), Data: []byte{byte(i)}})
	}

	// First 5 should be evicted
	for i := 0; i < 5; i++ {
		_, err := ring.Get(context.Background(), uint32(i))
		if err != ErrEvicted {
			t.Errorf("chunk %d: expected ErrEvicted, got %v", i, err)
		}
	}

	// Chunks 5..total-1 should be available
	for i := 5; i < total; i++ {
		ch, err := ring.Get(context.Background(), uint32(i))
		if err != nil {
			t.Errorf("chunk %d: unexpected error: %v", i, err)
		}
		if ch.Data[0] != byte(i) {
			t.Errorf("chunk %d: expected data %d, got %d", i, i, ch.Data[0])
		}
	}
}

func TestChunkRing_Done(t *testing.T) {
	ring := newChunkRing()
	ring.Add(Chunk{Index: 0, Data: []byte{1}, IsLast: true})
	ring.Done()

	ch, err := ring.Get(context.Background(), 0)
	if err != nil {
		t.Fatalf("chunk 0: unexpected error: %v", err)
	}
	if !ch.IsLast {
		t.Error("expected IsLast=true on final chunk")
	}

	// Index past end → ErrDone
	_, err = ring.Get(context.Background(), 1)
	if err != ErrDone {
		t.Errorf("past end: expected ErrDone, got %v", err)
	}
}

func TestStreamer_OggStream(t *testing.T) {
	tempDir := t.TempDir()
	s, srv := newTestStreamer(t, tempDir)

	// Create a dummy song file in the tempDir (mediaDir)
	songID := "test_ogg_song"
	filePath := filepath.Join(tempDir, songID+".webm")
	if err := os.WriteFile(filePath, []byte("dummy webm data"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Register in media_cache
	if err := s.db.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         songID,
		Title:           "Test Ogg Song",
		FilePath:        filePath,
		FileSizeBytes:   int64(len("dummy webm data")),
		DurationSeconds: 10,
	}); err != nil {
		t.Fatalf("failed to upsert media cache: %v", err)
	}

	// Set streamer port and dependencies mock for testing
	s.httpStreamPort = 8765

	// Trigger StartStream
	err := s.StartStream(songID, "Test Ogg Song")
	if err != nil {
		t.Fatalf("StartStream failed: %v", err)
	}

	// Verify MQTT published to device/waveshare/media
	mqtt := s.mqtt.(*MockMQTT)
	var foundMediaPublish bool
	for _, p := range mqtt.payloads {
		if p["raw_topic"] == "device/waveshare/media" {
			foundMediaPublish = true
			raw := p["raw_data"].(map[string]any)
			if raw["song_id"] != songID {
				t.Errorf("expected song_id %s, got %v", songID, raw["song_id"])
			}
			expectedURL := fmt.Sprintf("http://%s:8765/stream?song_id=%s", getLocalIP(), songID)
			if raw["song_url"] != expectedURL {
				t.Errorf("expected song_url %s, got %v", expectedURL, raw["song_url"])
			}
		}
	}
	if !foundMediaPublish {
		t.Error("expected MQTT publish on device/waveshare/media, got none")
	}

	// Get stream URL and fetch
	resp, err := http.Get(fmt.Sprintf("%s/stream?song_id=%s", srv.URL, songID))
	if err != nil {
		t.Fatalf("stream GET failed: %v", err)
	}
	resp.Body.Close()
}

func TestStreamer_OggStream_CustomHTTPSURL(t *testing.T) {
	tempDir := t.TempDir()
	s, _ := newTestStreamer(t, tempDir)

	songID := "test_https_song"
	filePath := filepath.Join(tempDir, songID+".webm")
	if err := os.WriteFile(filePath, []byte("dummy webm data"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	if err := s.db.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         songID,
		Title:           "Test HTTPS Song",
		FilePath:        filePath,
		FileSizeBytes:   int64(len("dummy webm data")),
		DurationSeconds: 10,
	}); err != nil {
		t.Fatalf("failed to upsert media cache: %v", err)
	}

	// Set custom HTTPS streamer URL
	s.streamerURL = "https://stream.ankitm.xyz"

	err := s.StartStream(songID, "Test HTTPS Song")
	if err != nil {
		t.Fatalf("StartStream failed: %v", err)
	}

	mqtt := s.mqtt.(*MockMQTT)
	var foundMediaPublish bool
	for _, p := range mqtt.payloads {
		if p["raw_topic"] == "device/waveshare/media" {
			foundMediaPublish = true
			raw := p["raw_data"].(map[string]any)
			expectedURL := "https://stream.ankitm.xyz/stream?song_id=" + songID
			if raw["song_url"] != expectedURL {
				t.Errorf("expected song_url %s, got %v", expectedURL, raw["song_url"])
			}
		}
	}
	if !foundMediaPublish {
		t.Error("expected MQTT publish on device/waveshare/media, got none")
	}
}

func TestStreamer_OggStream_HTTPSSetting(t *testing.T) {
	os.Setenv("STREAMER_HTTPS", "true")
	defer os.Unsetenv("STREAMER_HTTPS")

	tempDir := t.TempDir()
	s, _ := newTestStreamer(t, tempDir)

	songID := "test_https_setting_song"
	filePath := filepath.Join(tempDir, songID+".webm")
	if err := os.WriteFile(filePath, []byte("dummy webm data"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	if err := s.db.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         songID,
		Title:           "Test HTTPS Setting",
		FilePath:        filePath,
		FileSizeBytes:   int64(len("dummy webm data")),
		DurationSeconds: 10,
	}); err != nil {
		t.Fatalf("failed to upsert media cache: %v", err)
	}

	// 1. Test fallback URL using dynamic local IP (should use https scheme)
	s.streamerURL = ""
	s.httpStreamPort = 8765
	err := s.StartStream(songID, "Test HTTPS Setting")
	if err != nil {
		t.Fatalf("StartStream failed: %v", err)
	}

	mqtt := s.mqtt.(*MockMQTT)
	var foundMediaPublish bool
	for _, p := range mqtt.payloads {
		if p["raw_topic"] == "device/waveshare/media" {
			foundMediaPublish = true
			raw := p["raw_data"].(map[string]any)
			expectedURL := fmt.Sprintf("https://%s/stream?song_id=%s", getLocalIP(), songID)
			if raw["song_url"] != expectedURL {
				t.Errorf("expected fallback URL to be %s, got %v", expectedURL, raw["song_url"])
			}
		}
	}
	if !foundMediaPublish {
		t.Error("expected MQTT publish on device/waveshare/media, got none")
	}

	// Reset mock payloads
	mqtt.mu.Lock()
	mqtt.payloads = nil
	mqtt.mu.Unlock()

	// 2. Test custom scheme-less URL with port (should automatically prepend, use https scheme, and strip port)
	s.streamerURL = "stream.ankitm.xyz:8765"
	err = s.StartStream(songID, "Test HTTPS Setting")
	if err != nil {
		t.Fatalf("StartStream failed: %v", err)
	}

	foundMediaPublish = false
	for _, p := range mqtt.payloads {
		if p["raw_topic"] == "device/waveshare/media" {
			foundMediaPublish = true
			raw := p["raw_data"].(map[string]any)
			expectedURL := "https://stream.ankitm.xyz/stream?song_id=" + songID
			if raw["song_url"] != expectedURL {
				t.Errorf("expected custom URL to be %s, got %v", expectedURL, raw["song_url"])
			}
		}
	}
	if !foundMediaPublish {
		t.Error("expected MQTT publish on device/waveshare/media, got none")
	}
}
