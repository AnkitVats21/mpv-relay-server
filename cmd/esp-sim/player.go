package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type StreamPlayer struct {
	mu          sync.Mutex
	cancel      context.CancelFunc
	bytesPulled int64
	lastPoll    time.Time
	lastBytes   int64
	lastURL     string
	lastToken   string
}

type trackingReader struct {
	io.Reader
	p *StreamPlayer
}

func (tr *trackingReader) Read(buf []byte) (int, error) {
	n, err := tr.Reader.Read(buf)
	if n > 0 {
		tr.p.mu.Lock()
		tr.p.bytesPulled += int64(n)
		tr.p.mu.Unlock()
	}
	return n, err
}

func (p *StreamPlayer) IsStreaming() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cancel != nil
}

// Connect starts an HTTP GET /stream?token=TOKEN, drains to io.Discard.
// Blocks until EOF or context cancel. Reports 403 as an error.
func (p *StreamPlayer) Connect(ctx context.Context, streamURL string) error {
	p.mu.Lock()
	p.bytesPulled = 0
	p.lastBytes = 0
	p.lastPoll = time.Now()
	p.lastURL = streamURL
	if u, err := url.Parse(streamURL); err == nil {
		p.lastToken = u.Query().Get("token")
	}
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		if p.cancel != nil {
			p.cancel()
			p.cancel = nil
		}
		p.mu.Unlock()
	}()

	req, err := http.NewRequestWithContext(ctx, "GET", streamURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("http forbidden 403")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	tr := &trackingReader{Reader: resp.Body, p: p}
	_, err = io.Copy(io.Discard, tr)
	return err
}

// Disconnect cancels the active stream.
func (p *StreamPlayer) Disconnect() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
}

// Reconnect re-connects using the last known URL + token.
func (p *StreamPlayer) Reconnect(ctx context.Context) error {
	p.mu.Lock()
	lastURL := p.lastURL
	p.mu.Unlock()

	if lastURL == "" {
		return fmt.Errorf("no last URL to reconnect to")
	}

	return p.Connect(ctx, lastURL)
}

// Throughput returns bytes/sec since last call (for monitoring).
func (p *StreamPlayer) Throughput() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if p.lastPoll.IsZero() {
		p.lastPoll = now
		p.lastBytes = p.bytesPulled
		return 0
	}

	duration := now.Sub(p.lastPoll).Seconds()
	if duration <= 0 {
		return 0
	}

	bytes := p.bytesPulled - p.lastBytes
	throughput := float64(bytes) / duration

	p.lastPoll = now
	p.lastBytes = p.bytesPulled

	return throughput
}
