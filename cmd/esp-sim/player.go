package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// PCMConfig describes the raw PCM format the server emits.
// Hardcoded to match pipeline.go constants (32000 Hz, mono, s16le).
type PCMConfig struct {
	SampleRate int // Hz
	Channels   int // 1 = mono
	BitDepth   int // 16
}

// DefaultPCMConfig matches the server's pipeline.go constants exactly.
var DefaultPCMConfig = PCMConfig{
	SampleRate: 32000,
	Channels:   1,
	BitDepth:   16,
}

// PCMStats is returned from Connect and describes the audio health of the stream.
type PCMStats struct {
	TotalSamples        int64
	ClippingCount       int64   // samples at ±32767
	MaxSilenceRun       int64   // longest consecutive zero-sample run
	ClippingPercent     float64 // ClippingCount/TotalSamples * 100
	SilenceWarning      bool    // silence run > 5000 samples (~156 ms at 32kHz)
	ClippingWarning     bool    // clipping > 10% of samples
}

// StreamPlayer holds state for the HTTP PCM stream connection.
type StreamPlayer struct {
	mu          sync.Mutex
	cancel      context.CancelFunc
	bytesPulled int64
	lastPoll    time.Time
	lastBytes   int64
	lastURL     string
	lastToken   string
}

// IsStreaming returns true if a stream is currently active.
func (p *StreamPlayer) IsStreaming() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cancel != nil
}

// Connect starts an HTTP GET /stream?token=TOKEN, drains to io.Discard (or a WAV
// file if dumpWAVPath is non-empty). Blocks until EOF or context cancel.
// Reports 403 as an error. Returns PCMStats after the stream ends.
func (p *StreamPlayer) Connect(ctx context.Context, streamURL string, dumpWAVPath string, pcmCfg PCMConfig) (PCMStats, error) {
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
		return PCMStats{}, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return PCMStats{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		return PCMStats{}, fmt.Errorf("http forbidden 403")
	}
	if resp.StatusCode != http.StatusOK {
		return PCMStats{}, fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	// Set up the sink: io.Discard, or a WAV file writer.
	var sink io.Writer = io.Discard
	var ww *wavWriter
	if dumpWAVPath != "" {
		ww, err = newWAVWriter(dumpWAVPath, pcmCfg)
		if err != nil {
			return PCMStats{}, fmt.Errorf("open WAV file: %w", err)
		}
		sink = ww
	}

	// healthTracker inspects PCM samples as they flow through.
	ht := &healthTracker{cfg: pcmCfg}

	// Track bytes via trackingReader, then tee into healthTracker.
	tr := &trackingReader{Reader: resp.Body, p: p}
	multiSink := io.MultiWriter(sink, ht)
	_, copyErr := io.Copy(multiSink, tr)

	if ww != nil {
		if closeErr := ww.Close(); closeErr != nil && copyErr == nil {
			copyErr = closeErr
		}
	}

	return ht.stats(), copyErr
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
func (p *StreamPlayer) Reconnect(ctx context.Context, dumpWAVPath string, pcmCfg PCMConfig) (PCMStats, error) {
	p.mu.Lock()
	lastURL := p.lastURL
	p.mu.Unlock()

	if lastURL == "" {
		return PCMStats{}, fmt.Errorf("no last URL to reconnect to")
	}
	return p.Connect(ctx, lastURL, dumpWAVPath, pcmCfg)
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

// ---------------------------------------------------------------------------
// trackingReader – counts bytes pulled through the reader.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// healthTracker – inspects PCM samples for silence and clipping.
// ---------------------------------------------------------------------------

const (
	silenceThreshold    = int16(32)    // treat |sample| < 32 as silence
	clipValue           = int16(32767) // ±32767 = clipping
	silenceRunWarnLimit = int64(5000)  // ~156 ms at 32 kHz → WARN
	clippingWarnPct     = 10.0         // >10% clipping → WARN
)

type healthTracker struct {
	cfg PCMConfig

	mu               sync.Mutex
	totalSamples     int64
	clippingCount    int64
	silenceRun       int64 // current run of silence samples
	maxSilenceRun    int64
	leftover         []byte // partial sample bytes carried between Write calls
}

func (ht *healthTracker) Write(p []byte) (int, error) {
	bytesPerSample := ht.cfg.BitDepth / 8
	data := p

	// Prepend any leftover bytes from the previous Write.
	if len(ht.leftover) > 0 {
		data = append(ht.leftover, p...)
		ht.leftover = nil
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	for len(data) >= bytesPerSample {
		sample := int16(binary.LittleEndian.Uint16(data[:bytesPerSample]))
		data = data[bytesPerSample:]
		ht.totalSamples++

		abs := sample
		if abs < 0 {
			abs = -abs
		}

		// Silence check
		if abs < silenceThreshold {
			ht.silenceRun++
			if ht.silenceRun > ht.maxSilenceRun {
				ht.maxSilenceRun = ht.silenceRun
			}
		} else {
			ht.silenceRun = 0
		}

		// Clipping check
		if sample == clipValue || sample == -clipValue || sample == math.MinInt16 {
			ht.clippingCount++
		}
	}

	// Save any incomplete sample for the next Write call.
	if len(data) > 0 {
		ht.leftover = append(ht.leftover[:0], data...)
	}

	return len(p), nil
}

func (ht *healthTracker) stats() PCMStats {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	var clippingPct float64
	if ht.totalSamples > 0 {
		clippingPct = float64(ht.clippingCount) / float64(ht.totalSamples) * 100
	}
	return PCMStats{
		TotalSamples:    ht.totalSamples,
		ClippingCount:   ht.clippingCount,
		MaxSilenceRun:   ht.maxSilenceRun,
		ClippingPercent: clippingPct,
		SilenceWarning:  ht.maxSilenceRun > silenceRunWarnLimit,
		ClippingWarning: clippingPct > clippingWarnPct,
	}
}

// ---------------------------------------------------------------------------
// wavWriter – writes a RIFF/WAV file from raw PCM bytes.
// ---------------------------------------------------------------------------

type wavWriter struct {
	f   *os.File
	cfg PCMConfig
}

func newWAVWriter(path string, cfg PCMConfig) (*wavWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &wavWriter{f: f, cfg: cfg}
	// Write a placeholder header; sizes will be patched on Close().
	if err := w.writeHeader(0); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

// writeHeader writes a 44-byte standard WAV header.
// dataSize is the number of PCM data bytes (0 = placeholder).
func (w *wavWriter) writeHeader(dataSize uint32) error {
	byteRate := uint32(w.cfg.SampleRate * w.cfg.Channels * (w.cfg.BitDepth / 8))
	blockAlign := uint16(w.cfg.Channels * (w.cfg.BitDepth / 8))

	buf := make([]byte, 44)
	copy(buf[0:4], "RIFF")
	binary.LittleEndian.PutUint32(buf[4:8], 36+dataSize)  // ChunkSize
	copy(buf[8:12], "WAVE")
	copy(buf[12:16], "fmt ")
	binary.LittleEndian.PutUint32(buf[16:20], 16)                          // SubChunk1Size (PCM = 16)
	binary.LittleEndian.PutUint16(buf[20:22], 1)                           // AudioFormat (PCM = 1)
	binary.LittleEndian.PutUint16(buf[22:24], uint16(w.cfg.Channels))      // NumChannels
	binary.LittleEndian.PutUint32(buf[24:28], uint32(w.cfg.SampleRate))    // SampleRate
	binary.LittleEndian.PutUint32(buf[28:32], byteRate)                    // ByteRate
	binary.LittleEndian.PutUint16(buf[32:34], blockAlign)                  // BlockAlign
	binary.LittleEndian.PutUint16(buf[34:36], uint16(w.cfg.BitDepth))      // BitsPerSample
	copy(buf[36:40], "data")
	binary.LittleEndian.PutUint32(buf[40:44], dataSize)                    // SubChunk2Size

	_, err := w.f.WriteAt(buf, 0)
	return err
}

func (w *wavWriter) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// Close patches the WAV header with the real data size and closes the file.
func (w *wavWriter) Close() error {
	size, err := w.f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = w.f.Close()
		return err
	}
	dataSize := uint32(size) - 44 // subtract header size
	if err := w.writeHeader(dataSize); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
