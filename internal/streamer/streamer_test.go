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
	"time"

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

// ── TestStreamer_InvalidToken ──────────────────────────────────────────────────

func TestStreamer_InvalidToken(t *testing.T) {
	tempDir := t.TempDir()
	_, srv := newTestStreamer(t, tempDir)

	for _, path := range []string{
		"/stream/manifest",
		"/stream/manifest?token=bad_token",
		"/stream/chunk?token=bad_token&index=0",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s failed: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s: expected 403, got %d", path, resp.StatusCode)
		}
	}
}

// ── TestStreamer_CacheHit ──────────────────────────────────────────────────────

func TestStreamer_CacheHit(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not in PATH")
	}

	tempDir := t.TempDir()

	// Generate a 1-second audio file
	audioPath := filepath.Join(tempDir, "test_video_id.mkv")
	if err := exec.Command("ffmpeg", "-f", "lavfi", "-i", "sine=d=1",
		"-c:a", "flac", "-y", audioPath).Run(); err != nil {
		t.Fatalf("failed to generate test audio: %v", err)
	}

	s, srv := newTestStreamer(t, tempDir)

	// Register in media_cache with known duration
	if err := s.db.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "test_video_id",
		Title:           "Test Song",
		FilePath:        audioPath,
		FileSizeBytes:   1024,
		DurationSeconds: 1,
	}); err != nil {
		t.Fatalf("failed to upsert media cache: %v", err)
	}

	// Issue token
	if err := s.StartStream("test_video_id", "Test Song"); err != nil {
		t.Fatalf("StartStream failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	s.session.RLock()
	token := s.session.SessionToken
	s.session.RUnlock()
	if token == "" {
		t.Fatal("expected token to be generated")
	}

	// ── GET /stream/manifest ──
	resp, err := http.Get(fmt.Sprintf("%s/stream/manifest?token=%s", srv.URL, token))
	if err != nil {
		t.Fatalf("manifest request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("manifest: expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("manifest Content-Type: expected application/json, got %q", ct)
	}

	var manifest Manifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		t.Fatalf("failed to decode manifest: %v", err)
	}
	if manifest.VideoID != "test_video_id" {
		t.Errorf("manifest VideoID: expected test_video_id, got %q", manifest.VideoID)
	}
	if manifest.ChunkSizeBytes != ChunkSize {
		t.Errorf("manifest ChunkSizeBytes: expected %d, got %d", ChunkSize, manifest.ChunkSizeBytes)
	}
	// 1 second at 64kB/s → 1 chunk (ceil(64000/32768) = 2)
	if manifest.TotalChunks < 1 {
		t.Errorf("manifest TotalChunks: expected ≥1, got %d", manifest.TotalChunks)
	}

	// ── GET /stream/chunk?index=0 ──
	// Give the producer a moment to fill chunk 0
	time.Sleep(300 * time.Millisecond)

	chunkResp, err := http.Get(fmt.Sprintf("%s/stream/chunk?token=%s&index=0", srv.URL, token))
	if err != nil {
		t.Fatalf("chunk 0 request failed: %v", err)
	}
	defer chunkResp.Body.Close()

	if chunkResp.StatusCode != http.StatusOK {
		body := make([]byte, 256)
		n, _ := chunkResp.Body.Read(body)
		t.Fatalf("chunk 0: expected 200, got %d (%s)", chunkResp.StatusCode, body[:n])
	}
	if ci := chunkResp.Header.Get("X-Chunk-Index"); ci != "0" {
		t.Errorf("X-Chunk-Index: expected 0, got %q", ci)
	}

	// Verify MQTT published START_STREAM with manifest_url
	mqtt := s.mqtt.(*MockMQTT)
	mqtt.mu.Lock()
	defer mqtt.mu.Unlock()
	if len(mqtt.payloads) == 0 {
		t.Error("expected MQTT START_STREAM payload, got none")
	} else {
		p := mqtt.payloads[0]
		if p["type"] != "START_STREAM" {
			t.Errorf("expected START_STREAM, got %v", p["type"])
		}
		if p["manifest_url"] == nil {
			t.Error("expected manifest_url in START_STREAM payload")
		}
	}
}

// ── TestStreamer_Takeover ─────────────────────────────────────────────────────

func TestStreamer_Takeover(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not in PATH")
	}

	tempDir := t.TempDir()

	// 5-second file is enough — longer risks chunk 0 being evicted in CI
	audioPath := filepath.Join(tempDir, "takeover_vid.mkv")
	if err := exec.Command("ffmpeg", "-f", "lavfi", "-i", "sine=d=5",
		"-c:a", "flac", "-y", audioPath).Run(); err != nil {
		t.Fatalf("failed to generate test audio: %v", err)
	}

	s, srv := newTestStreamer(t, tempDir)
	if err := s.db.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:       "takeover_vid",
		Title:         "Takeover Song",
		FilePath:      audioPath,
		FileSizeBytes: 1024,
	}); err != nil {
		t.Fatalf("failed to upsert media cache: %v", err)
	}

	// Stream 1
	if err := s.StartStream("takeover_vid", "Takeover Song"); err != nil {
		t.Fatalf("StartStream 1 failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	s.session.RLock()
	token1 := s.session.SessionToken
	s.session.RUnlock()

	// Manifest for stream 1 → starts producer 1
	resp1, err := http.Get(fmt.Sprintf("%s/stream/manifest?token=%s", srv.URL, token1))
	if err != nil {
		t.Fatalf("manifest 1 failed: %v", err)
	}
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("manifest 1: expected 200, got %d", resp1.StatusCode)
	}

	// Request chunk 0 immediately — ring buffer starts filling right after manifest
	// Use a short poll to wait for chunk 0 to be available without a fixed sleep
	var cr1 *http.Response
	for i := 0; i < 20; i++ {
		cr1, err = http.Get(fmt.Sprintf("%s/stream/chunk?token=%s&index=0", srv.URL, token1))
		if err != nil {
			t.Fatalf("chunk 0 stream 1 failed: %v", err)
		}
		if cr1.StatusCode == http.StatusOK {
			cr1.Body.Close()
			break
		}
		cr1.Body.Close()
		time.Sleep(50 * time.Millisecond)
	}
	if cr1 == nil || cr1.StatusCode != http.StatusOK {
		t.Fatalf("chunk 0 stream 1: expected 200 eventually, got %d", cr1.StatusCode)
	}

	// Stream 2 (takeover) — stopProducer kills producer 1, issues new token
	if err := s.StartStream("takeover_vid", "Takeover Song 2"); err != nil {
		t.Fatalf("StartStream 2 failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	s.session.RLock()
	token2 := s.session.SessionToken
	s.session.RUnlock()

	if token1 == token2 {
		t.Fatal("expected new token for stream 2")
	}

	// Old token must 403
	r, _ := http.Get(fmt.Sprintf("%s/stream/chunk?token=%s&index=1", srv.URL, token1))
	if r != nil {
		r.Body.Close()
		if r.StatusCode != http.StatusForbidden {
			t.Errorf("stale token: expected 403, got %d", r.StatusCode)
		}
	}

	// Manifest for stream 2
	resp2, err := http.Get(fmt.Sprintf("%s/stream/manifest?token=%s", srv.URL, token2))
	if err != nil {
		t.Fatalf("manifest 2 failed: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("manifest 2: expected 200, got %d", resp2.StatusCode)
	}

	// Wait for chunk 0 on stream 2
	for i := 0; i < 20; i++ {
		cr2, err := http.Get(fmt.Sprintf("%s/stream/chunk?token=%s&index=0", srv.URL, token2))
		if err != nil {
			t.Fatalf("chunk 0 stream 2 failed: %v", err)
		}
		if cr2.StatusCode == http.StatusOK {
			cr2.Body.Close()
			return
		}
		cr2.Body.Close()
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("chunk 0 stream 2: never returned 200")
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
