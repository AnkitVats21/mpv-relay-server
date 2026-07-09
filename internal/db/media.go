package db

import (
	"database/sql"
	"time"
)

// MediaCacheEntry tracks files on disk with LRU metadata.
type MediaCacheEntry struct {
	VideoID         string
	Title           string
	FilePath        string
	FileSizeBytes   int64
	DurationSeconds int
	LastAccessedAt  time.Time
	CreatedAt       time.Time
}

// QueueEntry represents a persistent playback queue item.
type QueueEntry struct {
	ID        int64
	VideoID   string
	Title     string
	Status    string
	Source    string
	AddedAt   time.Time
	StartedAt *time.Time
}

// Helper to format Go time.Time consistently as a UTC string matching SQLite's CURRENT_TIMESTAMP.
func toDBTime(t time.Time) string {
	if t.IsZero() {
		return time.Now().UTC().Format("2006-01-02 15:04:05")
	}
	return t.UTC().Format("2006-01-02 15:04:05")
}

// Helper to format Go *time.Time consistently as a UTC string pointer or nil.
func toDBTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := toDBTime(*t)
	return &s
}

// UpsertMediaCache adds or updates a media cache entry.
func (d *DB) UpsertMediaCache(entry MediaCacheEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	lastAccessedStr := toDBTime(entry.LastAccessedAt)
	createdAtStr := toDBTime(entry.CreatedAt)

	const q = `
	INSERT INTO media_cache (video_id, title, file_path, file_size_bytes, duration_seconds, last_accessed_at, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(video_id) DO UPDATE SET
		title = excluded.title,
		file_path = excluded.file_path,
		file_size_bytes = excluded.file_size_bytes,
		duration_seconds = excluded.duration_seconds,
		last_accessed_at = excluded.last_accessed_at
	`
	_, err := d.db.Exec(q, entry.VideoID, entry.Title, entry.FilePath, entry.FileSizeBytes, entry.DurationSeconds, lastAccessedStr, createdAtStr)
	return err
}

// TouchMediaCache updates the last_accessed_at timestamp for a media file to the current database time.
func (d *DB) TouchMediaCache(videoID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	const q = `UPDATE media_cache SET last_accessed_at = CURRENT_TIMESTAMP WHERE video_id = ?`
	_, err := d.db.Exec(q, videoID)
	return err
}

// LookupMediaCache retrieves a media cache entry by video_id.
// Returns nil, nil if the entry is not found in the cache.
func (d *DB) LookupMediaCache(videoID string) (*MediaCacheEntry, error) {
	const q = `
	SELECT video_id, title, file_path, file_size_bytes, duration_seconds, last_accessed_at, created_at
	FROM media_cache
	WHERE video_id = ?
	`
	var entry MediaCacheEntry
	var lastAccessed, createdAt time.Time
	err := d.db.QueryRow(q, videoID).Scan(
		&entry.VideoID,
		&entry.Title,
		&entry.FilePath,
		&entry.FileSizeBytes,
		&entry.DurationSeconds,
		&lastAccessed,
		&createdAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entry.LastAccessedAt = lastAccessed
	entry.CreatedAt = createdAt
	return &entry, nil
}


// GetLRUMediaFiles returns cached media entries ordered by oldest access time until their total size meets or exceeds limitBytes.
func (d *DB) GetLRUMediaFiles(limitBytes int64) ([]MediaCacheEntry, error) {
	const q = `
	SELECT video_id, title, file_path, file_size_bytes, duration_seconds, last_accessed_at, created_at
	FROM media_cache
	ORDER BY last_accessed_at ASC
	`
	rows, err := d.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []MediaCacheEntry
	var sum int64
	for rows.Next() {
		var entry MediaCacheEntry
		var lastAccessed, createdAt time.Time
		err := rows.Scan(
			&entry.VideoID,
			&entry.Title,
			&entry.FilePath,
			&entry.FileSizeBytes,
			&entry.DurationSeconds,
			&lastAccessed,
			&createdAt,
		)
		if err != nil {
			return nil, err
		}
		entry.LastAccessedAt = lastAccessed
		entry.CreatedAt = createdAt

		entries = append(entries, entry)
		sum += entry.FileSizeBytes
		if sum >= limitBytes {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

// DeleteMediaCache deletes a file entry from the cache.
func (d *DB) DeleteMediaCache(videoID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	const q = `DELETE FROM media_cache WHERE video_id = ?`
	_, err := d.db.Exec(q, videoID)
	return err
}

// EnqueueTrack appends a new track to the playback queue.
func (d *DB) EnqueueTrack(track QueueEntry) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	status := track.Status
	if status == "" {
		status = "PENDING"
	}
	addedAtStr := toDBTime(track.AddedAt)
	startedAtStr := toDBTimePtr(track.StartedAt)

	const q = `
	INSERT INTO playback_queue (video_id, title, status, source, added_at, started_at)
	VALUES (?, ?, ?, ?, ?, ?)
	`
	res, err := d.db.Exec(q, track.VideoID, track.Title, status, track.Source, addedAtStr, startedAtStr)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// GetNextPending returns the next track that is either PENDING or READY.
func (d *DB) GetNextPending() (*QueueEntry, error) {
	const q = `
	SELECT id, video_id, title, status, source, added_at, started_at
	FROM playback_queue
	WHERE status IN ('PENDING', 'READY')
	ORDER BY id ASC
	LIMIT 1
	`
	var entry QueueEntry
	var addedAt time.Time
	var startedAt *time.Time
	err := d.db.QueryRow(q).Scan(
		&entry.ID,
		&entry.VideoID,
		&entry.Title,
		&entry.Status,
		&entry.Source,
		&addedAt,
		&startedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entry.AddedAt = addedAt
	entry.StartedAt = startedAt
	return &entry, nil
}

// GetNextForPrefetch returns the next track that is in status PENDING.
func (d *DB) GetNextForPrefetch() (*QueueEntry, error) {
	const q = `
	SELECT id, video_id, title, status, source, added_at, started_at
	FROM playback_queue
	WHERE status = 'PENDING'
	ORDER BY id ASC
	LIMIT 1
	`
	var entry QueueEntry
	var addedAt time.Time
	var startedAt *time.Time
	err := d.db.QueryRow(q).Scan(
		&entry.ID,
		&entry.VideoID,
		&entry.Title,
		&entry.Status,
		&entry.Source,
		&addedAt,
		&startedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entry.AddedAt = addedAt
	entry.StartedAt = startedAt
	return &entry, nil
}

// SetQueueStatus updates the status of a specific queue entry.
func (d *DB) SetQueueStatus(id int64, status string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	const q = `UPDATE playback_queue SET status = ? WHERE id = ?`
	_, err := d.db.Exec(q, status, id)
	return err
}

// SetQueueStarted updates the started_at timestamp to CURRENT_TIMESTAMP.
func (d *DB) SetQueueStarted(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	const q = `UPDATE playback_queue SET started_at = CURRENT_TIMESTAMP WHERE id = ?`
	_, err := d.db.Exec(q, id)
	return err
}

// ListQueue returns all non-completed, non-failed tracks ordered by id ASC.
func (d *DB) ListQueue() ([]QueueEntry, error) {
	const q = `
	SELECT id, video_id, title, status, source, added_at, started_at
	FROM playback_queue
	WHERE status NOT IN ('COMPLETED', 'FAILED')
	ORDER BY id ASC
	`
	rows, err := d.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []QueueEntry
	for rows.Next() {
		var entry QueueEntry
		var addedAt time.Time
		var startedAt *time.Time
		err := rows.Scan(
			&entry.ID,
			&entry.VideoID,
			&entry.Title,
			&entry.Status,
			&entry.Source,
			&addedAt,
			&startedAt,
		)
		if err != nil {
			return nil, err
		}
		entry.AddedAt = addedAt
		entry.StartedAt = startedAt
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

// ClearQueue deletes all rows from the playback queue that are not in PLAYING status.
func (d *DB) ClearQueue() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	const q = `DELETE FROM playback_queue WHERE status != 'PLAYING'`
	_, err := d.db.Exec(q)
	return err
}

// GetPlayingEntry returns the currently playing queue entry, if any.
func (d *DB) GetPlayingEntry() (*QueueEntry, error) {
	const q = `
	SELECT id, video_id, title, status, source, added_at, started_at
	FROM playback_queue
	WHERE status = 'PLAYING'
	LIMIT 1
	`
	var entry QueueEntry
	var addedAt time.Time
	var startedAt *time.Time
	err := d.db.QueryRow(q).Scan(
		&entry.ID,
		&entry.VideoID,
		&entry.Title,
		&entry.Status,
		&entry.Source,
		&addedAt,
		&startedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entry.AddedAt = addedAt
	entry.StartedAt = startedAt
	return &entry, nil
}

// GetQueueEntry returns a specific queue entry by id.
func (d *DB) GetQueueEntry(id int64) (*QueueEntry, error) {
	const q = `
	SELECT id, video_id, title, status, source, added_at, started_at
	FROM playback_queue
	WHERE id = ?
	`
	var entry QueueEntry
	var addedAt time.Time
	var startedAt *time.Time
	err := d.db.QueryRow(q, id).Scan(
		&entry.ID,
		&entry.VideoID,
		&entry.Title,
		&entry.Status,
		&entry.Source,
		&addedAt,
		&startedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entry.AddedAt = addedAt
	entry.StartedAt = startedAt
	return &entry, nil
}

// MarkVideoDownloaded updates the file path of any song in song_cache with matching video_id.
func (d *DB) MarkVideoDownloaded(videoID, filePath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE song_cache SET file_path = ? WHERE video_id = ?", filePath, videoID)
	return err
}

// EnqueueTrackAtFront inserts a track at the front of the queue by assigning it an ID smaller than any existing ID.
func (d *DB) EnqueueTrackAtFront(track QueueEntry) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var minID int64
	err := d.db.QueryRow("SELECT COALESCE(MIN(id), 0) FROM playback_queue").Scan(&minID)
	if err != nil {
		return 0, err
	}
	newID := minID - 1

	status := track.Status
	if status == "" {
		status = "PENDING"
	}
	addedAtStr := toDBTime(track.AddedAt)
	startedAtStr := toDBTimePtr(track.StartedAt)

	const q = `
	INSERT INTO playback_queue (id, video_id, title, status, source, added_at, started_at)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	_, err = d.db.Exec(q, newID, track.VideoID, track.Title, status, track.Source, addedAtStr, startedAtStr)
	if err != nil {
		return 0, err
	}
	return newID, nil
}

// GetPrefetchStatusCounts returns the count of playback_queue rows in PENDING, PREFETCHING, and READY states.
func (d *DB) GetPrefetchStatusCounts() (pending, prefetching, ready int, err error) {
	const q = `
	SELECT 
		COALESCE(SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'PREFETCHING' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END), 0)
	FROM playback_queue
	`
	err = d.db.QueryRow(q).Scan(&pending, &prefetching, &ready)
	return pending, prefetching, ready, err
}

// DeleteCacheByVideoID deletes a cached media entry and clears file_path in song_cache.
func (d *DB) DeleteCacheByVideoID(videoID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. Delete from media_cache
	_, err1 := d.db.Exec("DELETE FROM media_cache WHERE video_id = ?", videoID)
	// 2. Set file_path = NULL in song_cache
	_, err2 := d.db.Exec("UPDATE song_cache SET file_path = NULL WHERE video_id = ?", videoID)

	if err1 != nil {
		return err1
	}
	return err2
}

// GetTotalCacheSize returns the sum of file_size_bytes of all entries in media_cache.
func (d *DB) GetTotalCacheSize() (int64, error) {
	const q = `SELECT COALESCE(SUM(file_size_bytes), 0) FROM media_cache`
	var sum int64
	err := d.db.QueryRow(q).Scan(&sum)
	return sum, err
}

// GetCacheTotalSize returns the sum of file_size_bytes for all rows in media_cache.
func (d *DB) GetCacheTotalSize() (int64, error) {
	return d.GetTotalCacheSize()
}



