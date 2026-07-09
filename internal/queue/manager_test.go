package queue

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankitm/mpv-relay/internal/config"
	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/resolver"
	"github.com/ankitm/mpv-relay/internal/resource"
)

type mockStreamer struct {
	mu         sync.Mutex
	startCalls []string
	startErr   error
	onStart    func()
}

func (ms *mockStreamer) StartStream(videoID, title string) error {
	ms.mu.Lock()
	ms.startCalls = append(ms.startCalls, videoID)
	err := ms.startErr
	onStart := ms.onStart
	ms.mu.Unlock()

	if err != nil {
		return err
	}
	if onStart != nil {
		go onStart()
	}
	return nil
}

func waitCondition(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestQueueManager(t *testing.T) {
	// Create temporary directory for DB and media
	tempDir, err := os.MkdirTemp("", "queue_test")
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

	cfg := &config.Config{
		MediaDir:       tempDir,
		MusicCacheDir:  tempDir,
	}

	res := resolver.New(d, cfg)
	rm := resource.New()

	var mqttMessages []map[string]any
	var mqttMu sync.Mutex
	publishFn := func(msg map[string]any) {
		mqttMu.Lock()
		mqttMessages = append(mqttMessages, msg)
		mqttMu.Unlock()
	}

	mgr := New(res, d, cfg, publishFn, rm)
	streamer := &mockStreamer{}
	mgr.SetStreamer(streamer)

	// Disable autoplay so it doesn't auto-advance to recommendations during tests
	mgr.SetAutoplay(false)

	// Popular real youtube video IDs
	vids := [3]string{"dQw4w9WgXcQ", "9bZkp7q19f0", "kJQP7kiw5Fk"}
	titles := [3]string{"Rick Astley - Never Gonna Give You Up", "PSY - GANGNAM STYLE", "Luis Fonsi - Despacito"}
	uploaders := [3]string{"Rick Astley", "officialpsy", "LuisFonsiVEVO"}
	durations := [3]int{212, 252, 282}

	// Using a dummyRelated video with empty ID blocks both resolver healing
	// and background recommendation fetching from making any network calls/DB writes.
	dummyRelated := []db.RelatedVideo{
		{ID: "", Title: "", Uploader: "", Duration: 0},
	}

	song1 := &resolver.ResolvedTrack{
		Query:         "song1",
		VideoID:       vids[0],
		Title:         titles[0],
		Uploader:      uploaders[0],
		Duration:      durations[0],
		WebpageURL:    "https://youtube.com/watch?v=" + vids[0],
		ThumbnailURL:  "https://i.ytimg.com/vi/" + vids[0] + "/hqdefault.jpg",
		RelatedVideos: dummyRelated,
	}
	song2 := &resolver.ResolvedTrack{
		Query:         "song2",
		VideoID:       vids[1],
		Title:         titles[1],
		Uploader:      uploaders[1],
		Duration:      durations[1],
		WebpageURL:    "https://youtube.com/watch?v=" + vids[1],
		ThumbnailURL:  "https://i.ytimg.com/vi/" + vids[1] + "/hqdefault.jpg",
		RelatedVideos: dummyRelated,
	}
	song3 := &resolver.ResolvedTrack{
		Query:         "song3",
		VideoID:       vids[2],
		Title:         titles[2],
		Uploader:      uploaders[2],
		Duration:      durations[2],
		WebpageURL:    "https://youtube.com/watch?v=" + vids[2],
		ThumbnailURL:  "https://i.ytimg.com/vi/" + vids[2] + "/hqdefault.jpg",
		RelatedVideos: dummyRelated,
	}

	_ = d.SaveSong(&db.SongRow{
		Query:         song1.Query,
		VideoID:       song1.VideoID,
		Title:         song1.Title,
		Uploader:      song1.Uploader,
		Duration:      song1.Duration,
		WebpageURL:    song1.WebpageURL,
		ThumbnailURL:  song1.ThumbnailURL,
		RelatedVideos: dummyRelated,
	})
	_ = d.SaveSong(&db.SongRow{
		Query:         song2.Query,
		VideoID:       song2.VideoID,
		Title:         song2.Title,
		Uploader:      song2.Uploader,
		Duration:      song2.Duration,
		WebpageURL:    song2.WebpageURL,
		ThumbnailURL:  song2.ThumbnailURL,
		RelatedVideos: dummyRelated,
	})
	_ = d.SaveSong(&db.SongRow{
		Query:         song3.Query,
		VideoID:       song3.VideoID,
		Title:         song3.Title,
		Uploader:      song3.Uploader,
		Duration:      song3.Duration,
		WebpageURL:    song3.WebpageURL,
		ThumbnailURL:  song3.ThumbnailURL,
		RelatedVideos: dummyRelated,
	})

	t.Run("PlayNow triggers streaming and sets PLAYING state", func(t *testing.T) {
		streamer.mu.Lock()
		streamer.startCalls = nil
		streamer.onStart = func() {
			_, release := rm.AcquireLiveStream(context.Background())
			go func() {
				time.Sleep(50 * time.Millisecond)
				release()
			}()
		}
		streamer.mu.Unlock()

		track := mgr.PlayNow("song1", false)
		if track == nil || track.VideoID != vids[0] {
			t.Fatalf("expected track %s, got %+v", vids[0], track)
		}

		// Check database status of the track is PLAYING
		var playing *db.QueueEntry
		ok := waitCondition(2*time.Second, func() bool {
			playing, _ = d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[0]
		})
		if !ok {
			t.Fatalf("expected playing track to be %s, got %+v", vids[0], playing)
		}

		// Wait for monitorStreamCompletion to detect disconnection and mark COMPLETED
		ok = waitCondition(2*time.Second, func() bool {
			playing, _ = d.GetPlayingEntry()
			return playing == nil
		})
		if !ok {
			t.Fatalf("expected playing track to be completed (nil), got %+v", playing)
		}
	})

	t.Run("QueueAdd enqueues tracks and handles play transition", func(t *testing.T) {
		streamer.mu.Lock()
		streamer.startCalls = nil
		streamer.onStart = func() {
			_, release := rm.AcquireLiveStream(context.Background())
			go func() {
				time.Sleep(50 * time.Millisecond)
				release()
			}()
		}
		streamer.mu.Unlock()

		mgr.QueueAdd("song2", false)
		// Wait for song2 to be resolved and play before enqueuing song3
		ok := waitCondition(2*time.Second, func() bool {
			playing, _ := d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[1]
		})
		if !ok {
			playing, _ := d.GetPlayingEntry()
			t.Fatalf("expected %s to be playing first, got %+v", vids[1], playing)
		}

		mgr.QueueAdd("song3", false)

		// List queue should show song3 queued
		var list []QueueItem
		ok = waitCondition(2*time.Second, func() bool {
			list = mgr.ListQueue()
			return len(list) > 0
		})
		if !ok {
			t.Fatalf("expected queued items, got 0")
		}

		// Wait for vids[1] to finish, which should trigger vids[2] to play
		ok = waitCondition(2*time.Second, func() bool {
			playing, _ := d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[2]
		})
		if !ok {
			playing, _ := d.GetPlayingEntry()
			t.Fatalf("expected %s to start playing after first finished, got %+v", vids[2], playing)
		}
	})

	t.Run("Skip marks current COMPLETED and plays next", func(t *testing.T) {
		mgr.StopAll()
		time.Sleep(50 * time.Millisecond)

		streamer.mu.Lock()
		streamer.startCalls = nil
		streamer.onStart = func() {
			_, release := rm.AcquireLiveStream(context.Background())
			go func() {
				time.Sleep(500 * time.Millisecond)
				release()
			}()
		}
		streamer.mu.Unlock()

		// Enqueue song2
		mgr.QueueAdd("song2", false)
		ok := waitCondition(2*time.Second, func() bool {
			playing, _ := d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[1]
		})
		if !ok {
			t.Fatalf("expected playing to be %s", vids[1])
		}

		// Enqueue song3
		mgr.QueueAdd("song3", false)
		ok = waitCondition(2*time.Second, func() bool {
			list := mgr.ListQueue()
			return len(list) > 0
		})
		if !ok {
			t.Fatalf("expected song3 to be queued")
		}

		// Skip it
		mgr.Skip()

		// Wait for song3 to play
		ok = waitCondition(2*time.Second, func() bool {
			playing, _ := d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[2]
		})
		if !ok {
			t.Fatalf("expected playing to be %s after skip", vids[2])
		}
	})

	t.Run("Previous plays track before current", func(t *testing.T) {
		mgr.StopAll()
		time.Sleep(50 * time.Millisecond)

		streamer.mu.Lock()
		streamer.startCalls = nil
		streamer.onStart = func() {
			_, release := rm.AcquireLiveStream(context.Background())
			go func() {
				time.Sleep(500 * time.Millisecond)
				release()
			}()
		}
		streamer.mu.Unlock()

		// Play song1, then play song2
		mgr.PlayNow("song1", false)
		ok := waitCondition(2*time.Second, func() bool {
			playing, _ := d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[0]
		})
		if !ok {
			t.Fatalf("expected playing to be %s", vids[0])
		}

		mgr.PlayNow("song2", false)
		ok = waitCondition(2*time.Second, func() bool {
			playing, _ := d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[1]
		})
		if !ok {
			t.Fatalf("expected playing to be %s", vids[1])
		}

		// Call Previous -> should play song1
		mgr.Previous()

		ok = waitCondition(2*time.Second, func() bool {
			playing, _ := d.GetPlayingEntry()
			return playing != nil && playing.VideoID == vids[0]
		})
		if !ok {
			playing, _ := d.GetPlayingEntry()
			t.Fatalf("expected playing to be %s after Previous, got %+v", vids[0], playing)
		}
	})
}
