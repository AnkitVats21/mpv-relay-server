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
