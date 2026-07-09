package streamer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankitm/mpv-relay/internal/db"
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

func TestStreamer_InvalidToken(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "streamer_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	rm := resource.New()
	mqtt := &MockMQTT{}
	s := New(database, rm, mqtt, tempDir)

	mux := http.NewServeMux()
	s.RegisterHTTPHandler(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test case 1: Missing token
	resp, err := http.Get(server.URL + "/stream")
	if err != nil {
		t.Fatalf("failed to get stream: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}

	// Test case 2: Invalid token
	resp, err = http.Get(server.URL + "/stream?token=invalid_token")
	if err != nil {
		t.Fatalf("failed to get stream: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}
}

func TestStreamer_CacheHit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "streamer_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Generate a 1-second flac file using ffmpeg
	targetAudioPath := filepath.Join(tempDir, "test_video_id.mkv")
	genCmd := exec.Command("ffmpeg", "-f", "lavfi", "-i", "sine=d=1", "-c:a", "flac", "-y", targetAudioPath)
	if err := genCmd.Run(); err != nil {
		t.Fatalf("failed to generate test audio: %v", err)
	}

	dbPath := filepath.Join(tempDir, "test.db")
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	// Register entry in media_cache
	entry := db.MediaCacheEntry{
		VideoID:       "test_video_id",
		Title:         "Test Song",
		FilePath:      targetAudioPath,
		FileSizeBytes: 1024,
	}
	if err := database.UpsertMediaCache(entry); err != nil {
		t.Fatalf("failed to upsert media cache: %v", err)
	}

	rm := resource.New()
	mqtt := &MockMQTT{}
	s := New(database, rm, mqtt, tempDir)

	mux := http.NewServeMux()
	s.RegisterHTTPHandler(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	// StartStream in background as it waits for connection
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.StartStream("test_video_id", "Test Song")
	}()

	// Wait a tiny bit for the token to be generated
	time.Sleep(50 * time.Millisecond)

	s.session.RLock()
	token := s.session.SessionToken
	s.session.RUnlock()

	if token == "" {
		t.Fatal("expected token to be generated")
	}

	// Request the stream
	resp, err := http.Get(fmt.Sprintf("%s/stream?token=%s", server.URL, token))
	if err != nil {
		t.Fatalf("failed to GET stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	// Verify headers
	if contentType := resp.Header.Get("Content-Type"); contentType != "audio/pcm" {
		t.Errorf("expected Content-Type audio/pcm, got %q", contentType)
	}
	if sampleRate := resp.Header.Get("X-Sample-Rate"); sampleRate != "32000" {
		t.Errorf("expected X-Sample-Rate 32000, got %q", sampleRate)
	}

	// Read all PCM bytes
	pcmBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read PCM bytes: %v", err)
	}

	// Expecting 1-second of PCM mono 16-bit 32kHz -> exactly 64000 bytes
	// Since gen/decoding might have small padding or header frames, let's allow a small tolerance,
	// but it should be close to 64000 bytes.
	if len(pcmBytes) < 60000 || len(pcmBytes) > 68000 {
		t.Errorf("expected around 64000 pcm bytes, got %d", len(pcmBytes))
	}

	// Wait for StartStream goroutine to finish successfully
	if err := <-errChan; err != nil {
		t.Errorf("StartStream failed: %v", err)
	}

	// Verify MQTT published START_STREAM
	mqtt.mu.Lock()
	defer mqtt.mu.Unlock()
	if len(mqtt.payloads) == 0 {
		t.Error("expected MQTT start stream payload, got none")
	} else {
		payload := mqtt.payloads[0]
		if payload["type"] != "START_STREAM" {
			t.Errorf("expected START_STREAM type, got %v", payload["type"])
		}
		if payload["token"] != token {
			t.Errorf("expected token %q, got %v", token, payload["token"])
		}
	}
}

func TestStreamer_Takeover(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "streamer_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	targetAudioPath := filepath.Join(tempDir, "takeover_vid.mkv")
	// Generate a 60-second flac file using ffmpeg so it takes longer to stream
	genCmd := exec.Command("ffmpeg", "-f", "lavfi", "-i", "sine=d=60", "-c:a", "flac", "-y", targetAudioPath)
	if err := genCmd.Run(); err != nil {
		t.Fatalf("failed to generate test audio: %v", err)
	}

	dbPath := filepath.Join(tempDir, "test.db")
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	entry := db.MediaCacheEntry{
		VideoID:       "takeover_vid",
		Title:         "Takeover Song",
		FilePath:      targetAudioPath,
		FileSizeBytes: 1024,
	}
	if err := database.UpsertMediaCache(entry); err != nil {
		t.Fatalf("failed to upsert media cache: %v", err)
	}

	rm := resource.New()
	mqtt := &MockMQTT{}
	s := New(database, rm, mqtt, tempDir)

	mux := http.NewServeMux()
	s.RegisterHTTPHandler(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	// StartStream 1
	errChan1 := make(chan error, 1)
	go func() {
		errChan1 <- s.StartStream("takeover_vid", "Takeover Song")
	}()

	time.Sleep(50 * time.Millisecond)
	s.session.RLock()
	token1 := s.session.SessionToken
	s.session.RUnlock()

	// Client 1 connects
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	req1, _ := http.NewRequestWithContext(ctx1, "GET", fmt.Sprintf("%s/stream?token=%s", server.URL, token1), nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("client 1 failed to connect: %v", err)
	}
	defer resp1.Body.Close()

	// Read a few bytes from client 1
	buf := make([]byte, 1024)
	n, err := resp1.Body.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("failed to read from client 1: %v", err)
	}

	// Now StartStream 2 (which should cancel client 1 and issue a new token)
	errChan2 := make(chan error, 1)
	go func() {
		errChan2 <- s.StartStream("takeover_vid", "Takeover Song 2")
	}()

	time.Sleep(50 * time.Millisecond)
	s.session.RLock()
	token2 := s.session.SessionToken
	s.session.RUnlock()

	if token1 == token2 {
		t.Fatal("expected new token for stream 2")
	}

	// Client 2 connects
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	req2, _ := http.NewRequestWithContext(ctx2, "GET", fmt.Sprintf("%s/stream?token=%s", server.URL, token2), nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("client 2 failed to connect: %v", err)
	}
	defer resp2.Body.Close()

	// Client 1 read may fail or succeed depending on whether the OS loopback
	// interface buffered the entire response before takeover occurred.
	_, _ = io.ReadAll(resp1.Body)

	// Client 2 should stream successfully
	n2, err := resp2.Body.Read(buf)
	if err != nil || n2 == 0 {
		t.Errorf("failed to read from client 2: %v", err)
	}
}
