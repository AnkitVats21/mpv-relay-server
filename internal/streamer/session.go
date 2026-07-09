package streamer

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

type LiveSession struct {
	sync.RWMutex
	ActiveVideoID string
	SessionToken  string
	ClientIP      string // locked on first connection
	BytesSent     int64
	StartedAt     time.Time
}

// GenerateToken creates a cryptographically random 32-byte hex token.
func GenerateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// IssueToken creates a new token and sets it on the session along with the video ID.
// It resets the ClientIP, BytesSent, and StartedAt for the new session.
func (s *Streamer) IssueToken(videoID string) (string, error) {
	token, err := GenerateToken()
	if err != nil {
		return "", err
	}

	s.session.Lock()
	s.session.ActiveVideoID = videoID
	s.session.SessionToken = token
	s.session.ClientIP = ""
	s.session.BytesSent = 0
	s.session.StartedAt = time.Time{}
	s.session.Unlock()

	return token, nil
}

// ValidateToken checks if the token is valid.
// It locks the ClientIP on the first call and rejects requests from other IPs.
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

// ClearSession resets all session fields.
func (s *Streamer) ClearSession() {
	s.session.Lock()
	s.session.ActiveVideoID = ""
	s.session.SessionToken = ""
	s.session.ClientIP = ""
	s.session.BytesSent = 0
	s.session.StartedAt = time.Time{}
	s.session.Unlock()
}
