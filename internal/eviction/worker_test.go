package eviction

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankitm/mpv-relay/internal/db"
)

type mockActiveTrackProvider struct {
	activeVideoID string
}

func (m *mockActiveTrackProvider) GetActiveVideoID() string {
	return m.activeVideoID
}

func TestWorker_RunOnce_UnderLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "eviction_test")
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

	// Cache limit = 100 bytes. Insert 1 file of 50 bytes.
	filePath := filepath.Join(tempDir, "f1.mp3")
	_ = os.WriteFile(filePath, []byte("some contents"), 0o644)

	err = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:       "v1",
		Title:         "title1",
		FilePath:      filePath,
		FileSizeBytes: 50,
	})
	if err != nil {
		t.Fatalf("failed to insert media: %v", err)
	}

	w := New(d, 100)
	filesDeleted, bytesFreed, err := w.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if filesDeleted != 0 || bytesFreed != 0 {
		t.Errorf("expected (0, 0) deleted, got (%d, %d)", filesDeleted, bytesFreed)
	}

	// Verify file still exists on disk
	if _, err := os.Stat(filePath); err != nil {
		t.Errorf("file should exist: %v", err)
	}

	// Verify db row still exists
	row, err := d.LookupMediaCache("v1")
	if err != nil || row == nil {
		t.Errorf("db row should exist, got: %v, %v", row, err)
	}
}

func TestWorker_RunOnce_OverLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "eviction_test")
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

	// Cache limit = 10 bytes. Insert 3 files totalling 15 bytes.
	// fp1 (3 bytes), fp2 (5 bytes), fp3 (7 bytes).
	// Access order (oldest first): fp1, fp2, fp3.
	fp1 := filepath.Join(tempDir, "f1.mp3")
	fp2 := filepath.Join(tempDir, "f2.mp3")
	fp3 := filepath.Join(tempDir, "f3.mp3")

	_ = os.WriteFile(fp1, []byte("abc"), 0o644)
	_ = os.WriteFile(fp2, []byte("defgh"), 0o644)
	_ = os.WriteFile(fp3, []byte("ijklmn"), 0o644)

	t1 := time.Now().Add(-30 * time.Minute)
	t2 := time.Now().Add(-20 * time.Minute)
	t3 := time.Now().Add(-10 * time.Minute)

	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v1",
		Title:           "t1",
		FilePath:        fp1,
		FileSizeBytes:   3,
		LastAccessedAt:  t1,
	})
	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v2",
		Title:           "t2",
		FilePath:        fp2,
		FileSizeBytes:   5,
		LastAccessedAt:  t2,
	})
	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v3",
		Title:           "t3",
		FilePath:        fp3,
		FileSizeBytes:   7,
		LastAccessedAt:  t3,
	})

	w := New(d, 10)
	// Total is 15. Excess is 5.
	// db.GetLRUMediaFiles(5) will return v1 (3) and v2 (5), total 8 >= 5.
	// It should evict v1 and v2.
	filesDeleted, bytesFreed, err := w.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if filesDeleted != 2 {
		t.Errorf("expected 2 files deleted, got %d", filesDeleted)
	}
	if bytesFreed != 8 {
		t.Errorf("expected 8 bytes freed, got %d", bytesFreed)
	}

	// Verify f1 and f2 deleted, f3 remains
	if _, err := os.Stat(fp1); !os.IsNotExist(err) {
		t.Errorf("f1 should be deleted")
	}
	if _, err := os.Stat(fp2); !os.IsNotExist(err) {
		t.Errorf("f2 should be deleted")
	}
	if _, err := os.Stat(fp3); err != nil {
		t.Errorf("f3 should exist: %v", err)
	}

	// Verify DB rows
	r1, _ := d.LookupMediaCache("v1")
	r2, _ := d.LookupMediaCache("v2")
	r3, _ := d.LookupMediaCache("v3")
	if r1 != nil || r2 != nil {
		t.Errorf("v1 and v2 DB rows should be deleted")
	}
	if r3 == nil {
		t.Errorf("v3 DB row should exist")
	}
}

func TestWorker_RunOnce_ActiveVideoSkipped(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "eviction_test")
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

	// Cache limit = 10 bytes. Insert 3 files totalling 15 bytes.
	// fp1 (3 bytes) - active!
	// fp2 (5 bytes)
	// fp3 (7 bytes)
	fp1 := filepath.Join(tempDir, "f1.mp3")
	fp2 := filepath.Join(tempDir, "f2.mp3")
	fp3 := filepath.Join(tempDir, "f3.mp3")

	_ = os.WriteFile(fp1, []byte("abc"), 0o644)
	_ = os.WriteFile(fp2, []byte("defgh"), 0o644)
	_ = os.WriteFile(fp3, []byte("ijklmn"), 0o644)

	t1 := time.Now().Add(-30 * time.Minute)
	t2 := time.Now().Add(-20 * time.Minute)
	t3 := time.Now().Add(-10 * time.Minute)

	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v1",
		Title:           "t1",
		FilePath:        fp1,
		FileSizeBytes:   3,
		LastAccessedAt:  t1,
	})
	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v2",
		Title:           "t2",
		FilePath:        fp2,
		FileSizeBytes:   5,
		LastAccessedAt:  t2,
	})
	_ = d.UpsertMediaCache(db.MediaCacheEntry{
		VideoID:         "v3",
		Title:           "t3",
		FilePath:        fp3,
		FileSizeBytes:   7,
		LastAccessedAt:  t3,
	})

	w := New(d, 10)
	w.SetActiveTrackProvider(&mockActiveTrackProvider{activeVideoID: "v1"})

	filesDeleted, bytesFreed, err := w.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Since v1 is active, we skip it.
	// Only v2 is evicted.
	if filesDeleted != 1 {
		t.Errorf("expected 1 file deleted (v2), got %d", filesDeleted)
	}
	if bytesFreed != 5 {
		t.Errorf("expected 5 bytes freed, got %d", bytesFreed)
	}

	// Verify fp1 exists, fp2 deleted, fp3 exists
	if _, err := os.Stat(fp1); err != nil {
		t.Errorf("fp1 (active) should not be deleted")
	}
	if _, err := os.Stat(fp2); !os.IsNotExist(err) {
		t.Errorf("fp2 should be deleted")
	}
	if _, err := os.Stat(fp3); err != nil {
		t.Errorf("fp3 should exist")
	}

	// Verify DB rows
	r1, _ := d.LookupMediaCache("v1")
	r2, _ := d.LookupMediaCache("v2")
	r3, _ := d.LookupMediaCache("v3")
	if r1 == nil {
		t.Errorf("v1 DB row should exist")
	}
	if r2 != nil {
		t.Errorf("v2 DB row should be deleted")
	}
	if r3 == nil {
		t.Errorf("v3 DB row should exist")
	}
}

func TestWorker_StartStop(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "eviction_test")
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

	w := New(d, 100)
	w.checkInterval = 10 * time.Millisecond // very short interval for test

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		w.Start(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// success, stopped cleanly
	case <-time.After(200 * time.Millisecond):
		t.Errorf("worker did not stop cleanly when context was cancelled")
	}
}
