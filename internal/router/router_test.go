package router

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/resolver"
	"github.com/ankitm/mpv-relay/internal/resource"
	"github.com/ankitm/mpv-relay/internal/streamer"
)

type mockMqttPublisher struct {
	mu       sync.Mutex
	messages []map[string]any
}

func (m *mockMqttPublisher) Publish(payload map[string]any) {
	m.mu.Lock()
	m.messages = append(m.messages, payload)
	m.mu.Unlock()
}

func (m *mockMqttPublisher) PublishRaw(topic string, data []byte, retain ...bool) {
	// no-op for tests
}

func TestRouter_StreamStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "router_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer d.Close()

	rm := resource.New()
	mqttPub := &mockMqttPublisher{}

	isDownloading := func(videoID string) bool { return false }
	startDownload := func(track *resolver.ResolvedTrack, onComplete func(bool)) {
		if onComplete != nil {
			onComplete(true)
		}
	}
	s := streamer.New(d, rm, mqttPub, tempDir, 8765, isDownloading, startDownload)

	// Issue token to start a session
	token, err := s.IssueToken("video123", "Test Video")
	if err != nil {
		t.Fatalf("failed to issue token: %v", err)
	}

	// Validate it to mark started
	ok := s.ValidateToken(token, "127.0.0.1")
	if !ok {
		t.Fatalf("failed to validate token")
	}

	var published []map[string]any
	var mu sync.Mutex
	publishFn := func(payload map[string]any) {
		mu.Lock()
		published = append(published, payload)
		mu.Unlock()
	}

	r := New(nil, s, d, publishFn, nil)

	r.Dispatch(`{"cmd": "stream_status"}`)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	msg := published[0]
	mu.Unlock()

	if msg["type"] != "stream_status" {
		t.Errorf("expected type stream_status, got %v", msg["type"])
	}
	if msg["active_video_id"] != "video123" {
		t.Errorf("expected active_video_id video123, got %v", msg["active_video_id"])
	}
}

func TestRouter_PrefetchStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "router_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer d.Close()

	// Enqueue some tracks with different statuses
	_, _ = d.EnqueueTrack(db.QueueEntry{VideoID: "v1", Title: "t1", Status: "PENDING", AddedAt: time.Now()})
	_, _ = d.EnqueueTrack(db.QueueEntry{VideoID: "v2", Title: "t2", Status: "PREFETCHING", AddedAt: time.Now()})
	_, _ = d.EnqueueTrack(db.QueueEntry{VideoID: "v3", Title: "t3", Status: "READY", AddedAt: time.Now()})

	var published []map[string]any
	var mu sync.Mutex
	publishFn := func(payload map[string]any) {
		mu.Lock()
		published = append(published, payload)
		mu.Unlock()
	}

	r := New(nil, nil, d, publishFn, nil)

	r.Dispatch(`{"cmd": "prefetch_status"}`)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	msg := published[0]
	mu.Unlock()

	if msg["type"] != "prefetch_status" {
		t.Errorf("expected type prefetch_status, got %v", msg["type"])
	}
	if msg["pending"] != 1 || msg["prefetching"] != 1 || msg["ready"] != 1 {
		t.Errorf("expected counts 1, 1, 1, got %v, %v, %v", msg["pending"], msg["prefetching"], msg["ready"])
	}
}

func TestRouter_ClearCache_Specific(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "router_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer d.Close()

	videoID := "test_clear_cache_specific"
	filePath := filepath.Join(tempDir, "file.mp3")
	if err := os.WriteFile(filePath, []byte("audio data"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Insert into media_cache
	err = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         videoID,
		Title:           "title",
		FilePath:        filePath,
		FileSizeBytes:   10,
		DurationSeconds: 120,
	})
	if err != nil {
		t.Fatalf("failed to insert media cache: %v", err)
	}

	// Insert into song_cache with matching video_id
	_ = d.SaveSong(&db.SongRow{
		VideoID:  videoID,
		Query:    "query",
		Title:    "title",
		FilePath: filePath,
	})

	var published []map[string]any
	var mu sync.Mutex
	publishFn := func(payload map[string]any) {
		mu.Lock()
		published = append(published, payload)
		mu.Unlock()
	}

	r := New(nil, nil, d, publishFn, nil)

	r.Dispatch(fmt.Sprintf(`{"cmd": "clear_cache", "video_id": "%s"}`, videoID))
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	msg := published[0]
	mu.Unlock()

	if msg["type"] != "clear_cache_success" {
		t.Errorf("expected type clear_cache_success, got %v", msg["type"])
	}
	if msg["video_id"] != videoID {
		t.Errorf("expected video_id %s, got %v", videoID, msg["video_id"])
	}

	// Check file is deleted
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Errorf("expected file to be deleted, stat returned: %v", err)
	}

	// Check DB rows
	row, err := d.LookupMediaCache(videoID)
	if err != nil || row != nil {
		t.Errorf("expected media cache row to be deleted, got: %+v, err: %v", row, err)
	}

	song, _ := d.LookupQuery("query")
	if song != nil && song.FilePath != "" {
		t.Errorf("expected song_cache file_path to be empty, got: %s", song.FilePath)
	}
}

func TestRouter_ClearCache_LRU(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "router_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	d, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer d.Close()

	// Write three files, total size 15 bytes
	fp1 := filepath.Join(tempDir, "f1.mp3")
	fp2 := filepath.Join(tempDir, "f2.mp3")
	fp3 := filepath.Join(tempDir, "f3.mp3")

	_ = os.WriteFile(fp1, []byte("abc"), 0o644)
	_ = os.WriteFile(fp2, []byte("defgh"), 0o644)
	_ = os.WriteFile(fp3, []byte("ijklmn"), 0o644)

	// Access order (oldest first): v1, v2, v3
	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v1",
		Title:           "t1",
		FilePath:        fp1,
		FileSizeBytes:   3,
		DurationSeconds: 12,
	})
	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v2",
		Title:           "t2",
		FilePath:        fp2,
		FileSizeBytes:   5,
		DurationSeconds: 12,
	})
	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v3",
		Title:           "t3",
		FilePath:        fp3,
		FileSizeBytes:   7,
		DurationSeconds: 12,
	})

	// We set CACHE_MAX_BYTES to 8. Total is 15. Excess is 7.
	// LRU should evict v1 (3 bytes) and v2 (5 bytes) to bring total size under 8.
	os.Setenv("CACHE_MAX_BYTES", "8")
	defer os.Unsetenv("CACHE_MAX_BYTES")

	var published []map[string]any
	var mu sync.Mutex
	publishFn := func(payload map[string]any) {
		mu.Lock()
		published = append(published, payload)
		mu.Unlock()
	}

	r := New(nil, nil, d, publishFn, nil)

	r.Dispatch(`{"cmd": "clear_cache"}`)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	msg := published[0]
	mu.Unlock()

	if msg["type"] != "clear_cache_success" {
		t.Errorf("expected type clear_cache_success, got %v", msg["type"])
	}

	// Verify f1 and f2 are deleted, but f3 remains
	if _, err := os.Stat(fp1); !os.IsNotExist(err) {
		t.Errorf("expected f1 to be deleted")
	}
	if _, err := os.Stat(fp2); !os.IsNotExist(err) {
		t.Errorf("expected f2 to be deleted")
	}
	if _, err := os.Stat(fp3); err != nil {
		t.Errorf("expected f3 to exist, got: %v", err)
	}

	// Check DB rows
	r1, _ := d.LookupMediaCache("v1")
	r2, _ := d.LookupMediaCache("v2")
	r3, _ := d.LookupMediaCache("v3")
	if r1 != nil || r2 != nil {
		t.Errorf("expected v1 and v2 DB rows to be deleted")
	}
	if r3 == nil {
		t.Errorf("expected v3 DB row to exist")
	}
}

func TestRouter_Placeholders(t *testing.T) {
	var published []map[string]any
	var mu sync.Mutex
	publishFn := func(payload map[string]any) {
		mu.Lock()
		published = append(published, payload)
		mu.Unlock()
	}

	r := New(nil, nil, nil, publishFn, nil)

	// seek → error (not supported in stream mode)
	r.Dispatch(`{"cmd": "seek", "seconds": 10}`)
	// volume and mute → device_cmd (handled on-device)
	r.Dispatch(`{"cmd": "volume", "value": 50}`)
	r.Dispatch(`{"cmd": "mute"}`)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(published) != 3 {
		t.Fatalf("expected 3 published messages, got %d: %v", len(published), published)
	}

	// Collect by (type, cmd) — goroutine order is not guaranteed
	byKey := map[string]map[string]any{}
	for _, msg := range published {
		key := fmt.Sprintf("%v|%v", msg["type"], msg["cmd"])
		byKey[key] = msg
	}

	if _, ok := byKey["error|<nil>"]; !ok {
		t.Errorf("seek: expected an error response, got messages: %v", published)
	}
	if msg, ok := byKey["device_cmd|volume"]; !ok {
		t.Errorf("volume: expected device_cmd response, got messages: %v", published)
	} else if msg["value"] == nil {
		t.Errorf("volume: expected value field in device_cmd, got: %v", msg)
	}
	if _, ok := byKey["device_cmd|mute"]; !ok {
		t.Errorf("mute: expected device_cmd response, got messages: %v", published)
	}
}
