package streamer

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (s *Streamer) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.URL.Query().Get("token")
	if token == "" {
		s.log.Warn("Stream request missing token")
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Extract client IP
	clientIP := r.Header.Get("X-Forwarded-For")
	if clientIP == "" {
		clientIP = r.Header.Get("X-Real-IP")
	}
	if clientIP == "" {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			clientIP = host
		} else {
			clientIP = r.RemoteAddr
		}
	}
	if idx := strings.Index(clientIP, ","); idx != -1 {
		clientIP = strings.TrimSpace(clientIP[:idx])
	}

	if !s.ValidateToken(token, clientIP) {
		s.log.Warn("Token validation failed", "token", token, "clientIP", clientIP)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	s.session.RLock()
	videoID := s.session.ActiveVideoID
	s.session.RUnlock()

	s.log.Info("Stream request authorized", "token", token, "videoID", videoID, "clientIP", clientIP)

	// Touch DB media cache to update last accessed time (LRU)
	if err := s.db.TouchMediaCache(videoID); err != nil {
		s.log.Warn("Failed to touch media cache in DB", "videoID", videoID, "err", err)
	}

	// Check disk for cached file
	var cachedPath string
	entry, err := s.db.LookupMediaCache(videoID)
	if err == nil && entry != nil && entry.FilePath != "" {
		if _, err := os.Stat(entry.FilePath); err == nil {
			cachedPath = entry.FilePath
		}
	}

	if cachedPath == "" {
		// Fallback: check song_cache table
		row, err := s.db.LookupVideoID(videoID)
		if err == nil && row != nil && row.FilePath != "" {
			if _, err := os.Stat(row.FilePath); err == nil {
				cachedPath = row.FilePath
			}
		}
	}

	if cachedPath == "" {
		// Fallback: search common formats in mediaDir
		for _, ext := range []string{".mkv", ".opus", ".m4a", ".mp3"} {
			p := filepath.Join(s.mediaDir, videoID+ext)
			if _, err := os.Stat(p); err == nil {
				cachedPath = p
				break
			}
		}
	}

	// Parse optional seek seconds (forward compatible, default 0)
	seekSeconds := 0.0
	if seekStr := r.URL.Query().Get("seek"); seekStr != "" {
		if val, err := strconv.ParseFloat(seekStr, 64); err == nil {
			seekSeconds = val
		}
	}

	// Acquire resource gate
	streamCtx, release := s.rm.AcquireLiveStream(r.Context())
	defer release()

	// Set response headers
	w.Header().Set("Content-Type", "audio/pcm")
	w.Header().Set("X-Sample-Rate", "32000")
	w.Header().Set("X-Bit-Depth", "16")
	w.Header().Set("X-Channels", "1")
	w.WriteHeader(http.StatusOK)

	// Stream logic
	if cachedPath != "" {
		s.log.Info("Cache HIT — streaming local file", "videoID", videoID, "path", cachedPath, "seekSeconds", seekSeconds)
		cmd := BuildFFmpegFromFile(streamCtx, cachedPath, seekSeconds)

		ffmpegStdout, err := cmd.StdoutPipe()
		if err != nil {
			s.log.Error("Failed to get FFmpeg stdout pipe", "err", err)
			return
		}

		var ffmpegStderr bytes.Buffer
		cmd.Stderr = &ffmpegStderr

		if err := cmd.Start(); err != nil {
			s.log.Error("Failed to start FFmpeg command", "err", err)
			return
		}

		// Notify StartStream that client has connected
		s.mu.Lock()
		ch := s.connChan
		s.mu.Unlock()
		if ch != nil {
			select {
			case ch <- struct{}{}:
			default:
			}
		}

		bytesSent, _ := io.Copy(w, ffmpegStdout)
		s.session.Lock()
		s.session.BytesSent = bytesSent
		s.session.Unlock()

		cmdErr := cmd.Wait()
		s.session.Lock()
		if s.session.SessionToken == token {
			s.session.ActiveVideoID = ""
			s.session.SessionToken = ""
			s.session.ClientIP = ""
			s.session.BytesSent = 0
			s.session.StartedAt = time.Time{}
		}
		s.session.Unlock()

		if cmdErr != nil && streamCtx.Err() == nil {
			s.log.Error("FFmpeg command failed", "err", cmdErr, "stderr", ffmpegStderr.String())
		} else {
			s.log.Info("Finished cache hit stream", "videoID", videoID, "bytesSent", bytesSent)
		}
	} else {
		s.log.Info("Cache MISS — streaming from yt-dlp pipe", "videoID", videoID)
		ytCmd := BuildYtDlpStream(streamCtx, videoID)
		ffmpegCmd := BuildFFmpegFromStdin(streamCtx)

		ytStdout, err := ytCmd.StdoutPipe()
		if err != nil {
			s.log.Error("Failed to get yt-dlp stdout pipe", "err", err)
			return
		}
		ffmpegCmd.Stdin = ytStdout

		ffmpegStdout, err := ffmpegCmd.StdoutPipe()
		if err != nil {
			s.log.Error("Failed to get FFmpeg stdout pipe", "err", err)
			return
		}

		var ytStderr, ffmpegStderr bytes.Buffer
		ytCmd.Stderr = &ytStderr
		ffmpegCmd.Stderr = &ffmpegStderr

		if err := ytCmd.Start(); err != nil {
			s.log.Error("Failed to start yt-dlp command", "err", err)
			return
		}

		if err := ffmpegCmd.Start(); err != nil {
			s.log.Error("Failed to start FFmpeg command", "err", err)
			_ = ytCmd.Process.Kill()
			return
		}

		// Notify StartStream that client has connected
		s.mu.Lock()
		ch := s.connChan
		s.mu.Unlock()
		if ch != nil {
			select {
			case ch <- struct{}{}:
			default:
			}
		}

		bytesSent, _ := io.Copy(w, ffmpegStdout)
		s.session.Lock()
		s.session.BytesSent = bytesSent
		s.session.Unlock()

		ytErr := ytCmd.Wait()
		ffmpegErr := ffmpegCmd.Wait()
		s.session.Lock()
		if s.session.SessionToken == token {
			s.session.ActiveVideoID = ""
			s.session.SessionToken = ""
			s.session.ClientIP = ""
			s.session.BytesSent = 0
			s.session.StartedAt = time.Time{}
		}
		s.session.Unlock()

		if ytErr != nil {
			if streamCtx.Err() == nil {
				s.log.Error("yt-dlp command failed", "err", ytErr, "stderr", ytStderr.String())
				// Publish PLAYBACK_FAILED event
				s.mqtt.Publish(map[string]any{
					"type":     "PLAYBACK_FAILED",
					"cmd":      "PLAYBACK_FAILED",
					"video_id": videoID,
					"error":    fmt.Sprintf("yt-dlp failed: %v", ytErr),
				})
			}
		}

		if ffmpegErr != nil && streamCtx.Err() == nil {
			s.log.Error("FFmpeg command failed", "err", ffmpegErr, "stderr", ffmpegStderr.String())
		}

		if streamCtx.Err() == nil && ytErr == nil && ffmpegErr == nil {
			s.log.Info("Finished cache miss stream", "videoID", videoID, "bytesSent", bytesSent)
		}
	}
}
