// Package db provides the SQLite persistence layer.
//
// Tables:
//
//	song_cache    — search query → resolved video metadata + local file path
//	play_history  — rolling log of every played track
package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite" // register sqlite driver
)

const ddl = `
CREATE TABLE IF NOT EXISTS song_cache (
    query          TEXT PRIMARY KEY,
    video_id       TEXT NOT NULL,
    title          TEXT,
    uploader       TEXT,
    duration       INTEGER,
    webpage_url    TEXT,
    file_path      TEXT,
    thumbnail_path TEXT,
    thumbnail_url  TEXT,
    related_videos TEXT,
    play_count     INTEGER DEFAULT 0,
    last_played    REAL    DEFAULT 0
);

CREATE TABLE IF NOT EXISTS play_history (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    query     TEXT,
    title     TEXT,
    played_at REAL NOT NULL
);
`

// SongRow mirrors the song_cache table.
type SongRow struct {
	Query         string
	VideoID       string
	Title         string
	Uploader      string
	Duration      int
	WebpageURL    string
	FilePath      string // empty = not yet cached
	ThumbnailPath string
	ThumbnailURL  string
	RelatedVideos []RelatedVideo // deserialized from JSON column
	PlayCount     int
	LastPlayed    float64
}

// RelatedVideo is one entry in the related_videos JSON array.
type RelatedVideo struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Uploader string `json:"uploader"`
	Duration int    `json:"duration"`
}

// HistoryRow mirrors a joined play_history + song_cache row.
type HistoryRow struct {
	ID            int
	Query         string
	Title         string
	PlayedAt      float64
	VideoID       string
	ThumbnailPath string
	ThumbnailURL  string
	Uploader      string
	Duration      int
	FilePath      string
	Cached        bool
}

// DB wraps the SQLite connection with write serialisation.
type DB struct {
	db  *sql.DB
	mu  sync.Mutex // serialises all writes
	log *slog.Logger
}

// Open opens (or creates) the database at path and runs schema migrations.
func Open(path string) (*DB, error) {
	sqldb, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}
	// Allow concurrent reads from multiple goroutines.
	sqldb.SetMaxOpenConns(5)

	if _, err := sqldb.Exec(ddl); err != nil {
		return nil, fmt.Errorf("db schema: %w", err)
	}

	d := &DB{db: sqldb, log: slog.Default().With("pkg", "db")}
	if err := d.migrate(); err != nil {
		return nil, err
	}
	d.log.Info("DB ready", "path", path)
	return d, nil
}

// migrate adds columns that may be missing in older DB files.
func (d *DB) migrate() error {
	rows, err := d.db.Query("PRAGMA table_info(song_cache)")
	if err != nil {
		return err
	}
	defer rows.Close()

	existing := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return err
		}
		existing[name] = true
	}

	additions := map[string]string{
		"thumbnail_path": "TEXT",
		"thumbnail_url":  "TEXT",
		"related_videos": "TEXT",
	}
	mutated := false
	for col, typ := range additions {
		if !existing[col] {
			if _, err := d.db.Exec(fmt.Sprintf("ALTER TABLE song_cache ADD COLUMN %s %s", col, typ)); err != nil {
				return fmt.Errorf("migrate add %s: %w", col, err)
			}
			mutated = true
		}
	}
	if mutated {
		d.log.Info("DB schema migrated (added thumbnail/related fields)")
	}
	return nil
}

// ── Cache ─────────────────────────────────────────────────────────────────────

// LookupQuery returns the cached song for the given query, or nil on miss.
func (d *DB) LookupQuery(query string) (*SongRow, error) {
	const q = `SELECT query, video_id, title, uploader, duration, webpage_url,
	             file_path, thumbnail_path, thumbnail_url, related_videos, play_count, last_played
	           FROM song_cache WHERE query = ?`
	return d.scanSong(d.db.QueryRow(q, normQ(query)))
}

// LookupVideoID returns the first cached song matching video_id, or nil.
func (d *DB) LookupVideoID(videoID string) (*SongRow, error) {
	const q = `SELECT query, video_id, title, uploader, duration, webpage_url,
	             file_path, thumbnail_path, thumbnail_url, related_videos, play_count, last_played
	           FROM song_cache WHERE video_id = ? LIMIT 1`
	return d.scanSong(d.db.QueryRow(q, videoID))
}

func (d *DB) scanSong(row *sql.Row) (*SongRow, error) {
	s := &SongRow{}
	var relJSON sql.NullString
	var filePath, thumbPath, thumbURL sql.NullString
	err := row.Scan(
		&s.Query, &s.VideoID, &s.Title, &s.Uploader, &s.Duration, &s.WebpageURL,
		&filePath, &thumbPath, &thumbURL, &relJSON, &s.PlayCount, &s.LastPlayed,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.FilePath = filePath.String
	s.ThumbnailPath = thumbPath.String
	s.ThumbnailURL = thumbURL.String
	if relJSON.Valid && relJSON.String != "" {
		_ = json.Unmarshal([]byte(relJSON.String), &s.RelatedVideos)
	}
	return s, nil
}

// SaveSong upserts a resolved track into the cache.
func (d *DB) SaveSong(s *SongRow) error {
	relJSON := ""
	if s.RelatedVideos != nil {
		b, _ := json.Marshal(s.RelatedVideos)
		relJSON = string(b)
	}

	const q = `
	INSERT INTO song_cache (query, video_id, title, uploader, duration, webpage_url,
	                        file_path, thumbnail_path, thumbnail_url, related_videos)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(query) DO UPDATE SET
	    video_id       = excluded.video_id,
	    title          = excluded.title,
	    uploader       = excluded.uploader,
	    duration       = excluded.duration,
	    webpage_url    = excluded.webpage_url,
	    file_path      = COALESCE(excluded.file_path, song_cache.file_path),
	    thumbnail_path = COALESCE(excluded.thumbnail_path, song_cache.thumbnail_path),
	    thumbnail_url  = COALESCE(excluded.thumbnail_url, song_cache.thumbnail_url),
	    related_videos = COALESCE(excluded.related_videos, song_cache.related_videos)`

	fp := sql.NullString{String: s.FilePath, Valid: s.FilePath != ""}
	tp := sql.NullString{String: s.ThumbnailPath, Valid: s.ThumbnailPath != ""}
	tu := sql.NullString{String: s.ThumbnailURL, Valid: s.ThumbnailURL != ""}
	rj := sql.NullString{String: relJSON, Valid: relJSON != ""}

	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(q, normQ(s.Query), s.VideoID, s.Title, s.Uploader, s.Duration,
		s.WebpageURL, fp, tp, tu, rj)
	return err
}

// MarkFileDownloaded sets the local file path once the .mkv download is complete.
func (d *DB) MarkFileDownloaded(query, filePath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE song_cache SET file_path = ? WHERE query = ?", filePath, normQ(query))
	if err == nil {
		d.log.Info("DB: local file recorded", "query", query, "path", filePath)
	}
	return err
}

// IncrementPlayCount updates play_count and last_played for a query.
func (d *DB) IncrementPlayCount(query string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(
		"UPDATE song_cache SET play_count = play_count + 1, last_played = ? WHERE query = ?",
		float64(time.Now().UnixMilli())/1000.0, normQ(query),
	)
	return err
}

// ClearMissingFilePath sets file_path to NULL when the file no longer exists.
func (d *DB) ClearMissingFilePath(query string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec("UPDATE song_cache SET file_path = NULL WHERE query = ?", normQ(query))
	return err
}

// ── History ───────────────────────────────────────────────────────────────────

// SavePlayHistory appends a play event.
func (d *DB) SavePlayHistory(query, title string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(
		"INSERT INTO play_history (query, title, played_at) VALUES (?, ?, ?)",
		query, title, float64(time.Now().UnixMilli())/1000.0,
	)
	return err
}

// GetHistory returns the n most recently played tracks, joined with cache metadata.
func (d *DB) GetHistory(n int) ([]HistoryRow, error) {
	const q = `
	SELECT h.id, h.query, h.title, h.played_at,
	       c.video_id, c.thumbnail_path, c.thumbnail_url, c.uploader, c.duration, c.file_path
	FROM play_history h
	LEFT JOIN song_cache c ON LOWER(h.query) = LOWER(c.query)
	ORDER BY h.played_at DESC LIMIT ?`

	rows, err := d.db.Query(q, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []HistoryRow
	for rows.Next() {
		var h HistoryRow
		var vid, tp, tu, up, fp sql.NullString
		var dur sql.NullInt64
		if err := rows.Scan(&h.ID, &h.Query, &h.Title, &h.PlayedAt,
			&vid, &tp, &tu, &up, &dur, &fp); err != nil {
			return nil, err
		}
		h.VideoID = vid.String
		h.ThumbnailPath = tp.String
		h.ThumbnailURL = tu.String
		h.Uploader = up.String
		h.Duration = int(dur.Int64)
		h.FilePath = fp.String
		h.Cached = h.FilePath != "" && fileExists(h.FilePath)
		result = append(result, h)
	}
	return result, rows.Err()
}

// GetCachedSongs returns paginated songs that have a valid local file.
func (d *DB) GetCachedSongs(page, limit int) ([]SongRow, int, error) {
	const q = `SELECT query, video_id, title, uploader, duration,
	                  thumbnail_path, thumbnail_url, file_path
	           FROM song_cache WHERE file_path IS NOT NULL ORDER BY title ASC`

	rows, err := d.db.Query(q)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var all []SongRow
	for rows.Next() {
		var s SongRow
		var tp, tu, fp sql.NullString
		if err := rows.Scan(&s.Query, &s.VideoID, &s.Title, &s.Uploader, &s.Duration,
			&tp, &tu, &fp); err != nil {
			return nil, 0, err
		}
		s.ThumbnailPath = tp.String
		s.ThumbnailURL = tu.String
		s.FilePath = fp.String
		if s.FilePath != "" && fileExists(s.FilePath) {
			all = append(all, s)
		} else if s.FilePath != "" {
			_ = d.ClearMissingFilePath(s.Query)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	total := len(all)
	offset := (page - 1) * limit
	if offset >= total {
		return nil, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return all[offset:end], total, nil
}

// Close shuts down the database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

// ── helpers ───────────────────────────────────────────────────────────────────

func normQ(q string) string {
	// mirror Python: query.strip().lower()
	var out []byte
	for i := 0; i < len(q); i++ {
		c := q[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		out = append(out, c)
	}
	// trim leading/trailing spaces
	s := string(out)
	start, end := 0, len(s)
	for start < end && s[start] == ' ' {
		start++
	}
	for end > start && s[end-1] == ' ' {
		end--
	}
	return s[start:end]
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
