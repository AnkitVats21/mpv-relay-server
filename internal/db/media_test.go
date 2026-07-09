package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMediaCacheAndQueue(t *testing.T) {
	// Create a temporary database file
	tempDir, err := os.MkdirTemp("", "db_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	d, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer d.Close()

	// 1. Test media_cache helpers
	t.Run("MediaCacheHelpers", func(t *testing.T) {
		entry1 := MediaCacheEntry{
			VideoID:         "vid1",
			Title:           "Title One",
			FilePath:        "/path/to/vid1.mp3",
			FileSizeBytes:   1024,
			DurationSeconds: 120,
		}
		entry2 := MediaCacheEntry{
			VideoID:         "vid2",
			Title:           "Title Two",
			FilePath:        "/path/to/vid2.mp3",
			FileSizeBytes:   2048,
			DurationSeconds: 180,
		}

		// Test UpsertMediaCache
		if err := d.UpsertMediaCache(entry1); err != nil {
			t.Errorf("failed to upsert entry1: %v", err)
		}
		if err := d.UpsertMediaCache(entry2); err != nil {
			t.Errorf("failed to upsert entry2: %v", err)
		}

		// Verify upsert works (override title and size)
		entry1Updated := entry1
		entry1Updated.Title = "Title One Updated"
		entry1Updated.FileSizeBytes = 1500
		if err := d.UpsertMediaCache(entry1Updated); err != nil {
			t.Errorf("failed to update entry1: %v", err)
		}

		// Test GetLRUMediaFiles
		// We expect entry1 (1500 bytes) and entry2 (2048 bytes)
		// Since entry2 was upserted after entry1, its last_accessed_at is newer,
		// so entry1 should be oldest.
		// Wait, let's touch entry1 to make it newer!
		time.Sleep(1 * time.Second) // wait a bit so timestamps differ
		if err := d.TouchMediaCache("entry1_non_existent"); err != nil {
			t.Errorf("TouchMediaCache on non-existent failed: %v", err)
		}
		if err := d.TouchMediaCache("vid1"); err != nil {
			t.Errorf("failed to touch vid1: %v", err)
		}

		// Now vid2 last_accessed_at is older than vid1.
		// So LRU order should be vid2, then vid1.
		files, err := d.GetLRUMediaFiles(3000)
		if err != nil {
			t.Fatalf("failed to get LRU files: %v", err)
		}
		if len(files) != 2 {
			t.Errorf("expected 2 files, got %d", len(files))
		} else {
			if files[0].VideoID != "vid2" {
				t.Errorf("expected oldest file to be vid2, got %s", files[0].VideoID)
			}
			if files[1].VideoID != "vid1" {
				t.Errorf("expected second file to be vid1, got %s", files[1].VideoID)
			}
		}

		// Get LRU files with limit sum.
		// Since vid2 is 2048 bytes, a limit of 2000 bytes should return only vid2.
		filesLimit, err := d.GetLRUMediaFiles(2000)
		if err != nil {
			t.Fatalf("failed to get LRU files with limit: %v", err)
		}
		if len(filesLimit) != 1 {
			t.Errorf("expected 1 file for limit 2000, got %d", len(filesLimit))
		} else if filesLimit[0].VideoID != "vid2" {
			t.Errorf("expected file to be vid2, got %s", filesLimit[0].VideoID)
		}

		// Test DeleteMediaCache
		if err := d.DeleteMediaCache("vid2"); err != nil {
			t.Errorf("failed to delete vid2: %v", err)
		}
		filesAfterDelete, err := d.GetLRUMediaFiles(10000)
		if err != nil {
			t.Fatalf("failed to get LRU files: %v", err)
		}
		if len(filesAfterDelete) != 1 || filesAfterDelete[0].VideoID != "vid1" {
			t.Errorf("expected only vid1 remaining, got: %+v", filesAfterDelete)
		}
	})

	// 2. Test playback_queue helpers
	t.Run("PlaybackQueueHelpers", func(t *testing.T) {
		t1 := QueueEntry{
			VideoID: "qvid1",
			Title:   "Queue Title One",
			Status:  "PENDING",
			Source:  "web",
		}
		t2 := QueueEntry{
			VideoID: "qvid2",
			Title:   "Queue Title Two",
			Status:  "PENDING",
			Source:  "gemini_voice",
		}

		// Test EnqueueTrack
		id1, err := d.EnqueueTrack(t1)
		if err != nil {
			t.Fatalf("failed to enqueue t1: %v", err)
		}
		id2, err := d.EnqueueTrack(t2)
		if err != nil {
			t.Fatalf("failed to enqueue t2: %v", err)
		}

		// Test GetNextPending (should return t1 since it's PENDING and has lower ID)
		nextPending, err := d.GetNextPending()
		if err != nil {
			t.Fatalf("failed to get next pending: %v", err)
		}
		if nextPending == nil || nextPending.ID != id1 {
			t.Errorf("expected next pending to be id1 (%d), got: %+v", id1, nextPending)
		}

		// Test GetNextForPrefetch (should return t1 since it's PENDING)
		nextPrefetch, err := d.GetNextForPrefetch()
		if err != nil {
			t.Fatalf("failed to get next for prefetch: %v", err)
		}
		if nextPrefetch == nil || nextPrefetch.ID != id1 {
			t.Errorf("expected prefetch to be id1, got: %+v", nextPrefetch)
		}

		// Set queue status of id1 to READY
		if err := d.SetQueueStatus(id1, "READY"); err != nil {
			t.Errorf("failed to set status: %v", err)
		}

		// GetNextPending should still return id1 since status is READY (eligible)
		nextPending, err = d.GetNextPending()
		if err != nil {
			t.Fatalf("failed to get next pending after READY: %v", err)
		}
		if nextPending == nil || nextPending.ID != id1 {
			t.Errorf("expected next pending to still be id1, got: %+v", nextPending)
		}

		// GetNextForPrefetch should now return id2 since id1 is READY, and id2 is still PENDING
		nextPrefetch, err = d.GetNextForPrefetch()
		if err != nil {
			t.Fatalf("failed to get next for prefetch after id1 READY: %v", err)
		}
		if nextPrefetch == nil || nextPrefetch.ID != id2 {
			t.Errorf("expected prefetch to be id2 (%d), got: %+v", id2, nextPrefetch)
		}

		// Test SetQueueStarted (updates started_at to CURRENT_TIMESTAMP)
		if err := d.SetQueueStarted(id1); err != nil {
			t.Errorf("failed to set queue started: %v", err)
		}

		// Check if started_at is now set
		// We'll list the queue to inspect it
		list, err := d.ListQueue()
		if err != nil {
			t.Fatalf("failed to list queue: %v", err)
		}
		if len(list) != 2 {
			t.Errorf("expected 2 items in queue, got %d", len(list))
		} else {
			var found1 bool
			for _, item := range list {
				if item.ID == id1 {
					found1 = true
					if item.StartedAt == nil {
						t.Errorf("expected started_at to be non-nil for id1")
					} else {
						// verify it's reasonably close to now (e.g. within 10 seconds)
						now := time.Now()
						diff := now.Sub(*item.StartedAt)
						if diff < -10*time.Second || diff > 10*time.Second {
							t.Errorf("expected started_at (%v) to be close to now (%v)", *item.StartedAt, now)
						}
					}
				}
			}
			if !found1 {
				t.Errorf("failed to find id1 in ListQueue")
			}
		}

		// Test ListQueue filtering: COMPLETED and FAILED should be excluded
		if err := d.SetQueueStatus(id1, "COMPLETED"); err != nil {
			t.Errorf("failed to set status COMPLETED: %v", err)
		}
		if err := d.SetQueueStatus(id2, "FAILED"); err != nil {
			t.Errorf("failed to set status FAILED: %v", err)
		}

		listEmpty, err := d.ListQueue()
		if err != nil {
			t.Fatalf("failed to list queue after completions: %v", err)
		}
		if len(listEmpty) != 0 {
			t.Errorf("expected empty list since all are completed/failed, got: %+v", listEmpty)
		}

		// Test ClearQueue: deletes non-PLAYING rows.
		// Let's enqueue a PLAYING track and a PENDING track.
		idPlaying, err := d.EnqueueTrack(QueueEntry{
			VideoID: "playing_vid",
			Title:   "Playing Title",
			Status:  "PLAYING",
		})
		if err != nil {
			t.Fatalf("failed to enqueue playing: %v", err)
		}
		idPending, err := d.EnqueueTrack(QueueEntry{
			VideoID: "pending_vid",
			Title:   "Pending Title",
			Status:  "PENDING",
		})
		if err != nil {
			t.Fatalf("failed to enqueue pending: %v", err)
		}

		if err := d.ClearQueue(); err != nil {
			t.Errorf("failed to clear queue: %v", err)
		}

		// Now only the PLAYING track should remain.
		// ListQueue will list non-completed/failed tracks, which should include the PLAYING track.
		listAfterClear, err := d.ListQueue()
		if err != nil {
			t.Fatalf("failed to list queue after clear: %v", err)
		}
		if len(listAfterClear) != 1 {
			t.Errorf("expected 1 item remaining (the playing track), got %d", len(listAfterClear))
		} else {
			if listAfterClear[0].ID != idPlaying {
				t.Errorf("expected remaining track to be idPlaying (%d), got %d", idPlaying, listAfterClear[0].ID)
			}
		}

		// Check if the PENDING track was actually deleted.
		// We'll try to find it by getting next pending, which should be nil now.
		nextPendingAfterClear, err := d.GetNextPending()
		if err != nil {
			t.Fatalf("failed to get next pending: %v", err)
		}
		if nextPendingAfterClear != nil && nextPendingAfterClear.ID == idPending {
			t.Errorf("pending track was not deleted by ClearQueue")
		}
	})
}
