package streamer

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// SessionState represents the lifecycle state of a streaming session.
type SessionState int

const (
	SessionIdle    SessionState = iota // no active track
	SessionPlaying                     // chunk producer is running
	SessionPaused                      // producer stopped; LastChunk saved for resume
)

type LiveSession struct {
	sync.RWMutex
	ActiveVideoID string
	ActiveTitle   string
	SessionToken  string
	ClientIP      string // locked on first manifest request
	StartedAt     time.Time

	// Pause / resume state (chunk-index based — exact, no heuristic)
	State     SessionState
	LastChunk uint32    // chunk index at pause; seek = LastChunk × ChunkDurationMs / 1000
	PausedAt  time.Time // used by expiry goroutine
}

// GenerateToken creates a cryptographically random 32-byte hex token.
func GenerateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// IssueToken creates a new token and resets all connection-level fields.
// videoID and title are preserved across pause/resume.
func (s *Streamer) IssueToken(videoID, title string) (string, error) {
	token, err := GenerateToken()
	if err != nil {
		return "", err
	}

	s.session.Lock()
	s.session.ActiveVideoID = videoID
	s.session.ActiveTitle = title
	s.session.SessionToken = token
	s.session.ClientIP = ""
	s.session.StartedAt = time.Time{}
	s.session.State = SessionPlaying
	s.session.LastChunk = 0
	s.session.PausedAt = time.Time{}
	s.session.Unlock()

	return token, nil
}

// ValidateToken checks the token and IP-pins the session on first call.
func (s *Streamer) ValidateToken(token, clientIP string) bool {
	s.session.Lock()
	defer s.session.Unlock()

	if token == "" || s.session.SessionToken != token {
		return false
	}

	if s.session.ClientIP == "" {
		s.session.ClientIP = clientIP
		s.session.StartedAt = time.Now()
		return true
	}

	return s.session.ClientIP == clientIP
}

// ClearSession resets all session fields to the idle state.
func (s *Streamer) ClearSession() {
	s.session.Lock()
	s.session.ActiveVideoID = ""
	s.session.ActiveTitle = ""
	s.session.SessionToken = ""
	s.session.ClientIP = ""
	s.session.StartedAt = time.Time{}
	s.session.State = SessionIdle
	s.session.LastChunk = 0
	s.session.PausedAt = time.Time{}
	s.session.Unlock()
}
