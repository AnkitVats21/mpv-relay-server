// Package mpv provides a JSON-RPC client for the MPV media player IPC socket.
//
// Features:
//   - Persistent bidirectional Unix socket connection
//   - Background reader goroutine routes replies to callers via channels
//   - Event callbacks for MPV events (end-file, property-change, …)
//   - Auto-reconnect every 3 seconds when disconnected
//   - Synthesises an "end-file" event when idle-active transitions false→true
package mpv

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EventHandler is a callback for a named MPV event.
type EventHandler func(event map[string]any)

// PlayerStatus is a snapshot of the current player state.
type PlayerStatus struct {
	State    string  `json:"state"`
	Title    string  `json:"title"`
	Position float64 `json:"position"`
	Duration float64 `json:"duration"`
	Volume   float64 `json:"volume"`
}

// reply holds the response to a single IPC command.
type reply struct {
	Error string          `json:"error"`
	Data  json.RawMessage `json:"data"`
}

// Client is a thread-safe MPV IPC socket client.
type Client struct {
	socketPath string
	log        *slog.Logger

	connMu  sync.Mutex
	conn    net.Conn
	running atomic.Bool

	reqMu   sync.Mutex
	reqID   int
	pending map[int]chan reply

	handlersMu sync.RWMutex
	handlers   map[string][]EventHandler

	obsID   int
	wasIdle *bool // tracks previous idle-active state for edge detection

	connected atomic.Bool
}

// New creates a Client and immediately starts the reconnect loop.
func New(socketPath string) *Client {
	c := &Client{
		socketPath: socketPath,
		log:        slog.Default().With("pkg", "mpv"),
		pending:    make(map[int]chan reply),
		handlers:   make(map[string][]EventHandler),
	}
	c.running.Store(true)
	go c.reconnectLoop()
	return c
}

// ── Connection management ─────────────────────────────────────────────────────

func (c *Client) connect() bool {
	if _, err := os.Stat(c.socketPath); err != nil {
		c.log.Warn("MPV socket not found — is mpv running?", "path", c.socketPath)
		return false
	}

	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		c.log.Error("MPV connect failed", "err", err)
		return false
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	c.connected.Store(true)

	go c.readerLoop()
	c.subscribeEvents()

	c.log.Info("Connected to MPV socket", "path", c.socketPath)
	return true
}

func (c *Client) subscribeEvents() {
	c.reqMu.Lock()
	c.obsID++
	obsID := c.obsID
	c.reqMu.Unlock()

	payload, _ := json.Marshal(map[string]any{
		"command": []any{"observe_property", obsID, "idle-active"},
	})
	c.connMu.Lock()
	if c.conn != nil {
		_, _ = fmt.Fprintf(c.conn, "%s\n", payload)
	}
	c.connMu.Unlock()
	c.log.Debug("Subscribed to idle-active", "obs_id", obsID)
}

func (c *Client) reconnectLoop() {
	for c.running.Load() {
		if !c.connected.Load() {
			c.connect()
		}
		time.Sleep(3 * time.Second)
	}
}

// ── Reader goroutine ─────────────────────────────────────────────────────────

func (c *Client) readerLoop() {
	var buf []byte
	tmp := make([]byte, 4096)

	for c.running.Load() && c.connected.Load() {
		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()
		if conn == nil {
			break
		}

		n, err := conn.Read(tmp)
		if err != nil {
			if c.running.Load() {
				c.log.Error("MPV reader error", "err", err)
			}
			break
		}
		buf = append(buf, tmp[:n]...)

		for {
			idx := indexByte(buf, '\n')
			if idx < 0 {
				break
			}
			line := strings.TrimSpace(string(buf[:idx]))
			buf = buf[idx+1:]
			if line != "" {
				c.dispatch(line)
			}
		}
	}

	c.connected.Store(false)
	// Unblock all callers waiting for replies
	c.reqMu.Lock()
	for _, ch := range c.pending {
		select {
		case ch <- reply{Error: "disconnected"}:
		default:
		}
	}
	c.reqMu.Unlock()
}

func (c *Client) dispatch(line string) {
	var msg map[string]any
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		return
	}

	// ── Command reply ─────────────────────────────────────────────────────────
	if _, hasReqID := msg["request_id"]; hasReqID {
		if _, hasErr := msg["error"]; hasErr {
			reqIDf, _ := msg["request_id"].(float64)
			reqID := int(reqIDf)
			r := reply{}
			if e, ok := msg["error"].(string); ok {
				r.Error = e
			}
			if d, ok := msg["data"]; ok {
				r.Data, _ = json.Marshal(d)
			}
			c.reqMu.Lock()
			ch, ok := c.pending[reqID]
			delete(c.pending, reqID)
			c.reqMu.Unlock()
			if ok {
				select {
				case ch <- r:
				default:
				}
			}
			return
		}
	}

	// ── Event ─────────────────────────────────────────────────────────────────
	if eventName, ok := msg["event"].(string); ok {
		// Intercept idle-active to synthesise end-file(eof)
		if eventName == "property-change" {
			if name, ok := msg["name"].(string); ok && name == "idle-active" {
				idleNow := false
				if d, ok := msg["data"].(bool); ok {
					idleNow = d
				}
				c.log.Debug("idle-active changed", "idle", idleNow)
				if idleNow && c.wasIdle != nil && !*c.wasIdle {
					c.log.Info("MPV went idle — synthesising end-file(eof)")
					synth := map[string]any{"event": "end-file", "reason": "eof"}
					c.dispatchEvent("end-file", synth)
				}
				c.wasIdle = &idleNow
			}
		}
		c.dispatchEvent(eventName, msg)
	}
}

func (c *Client) dispatchEvent(name string, msg map[string]any) {
	c.handlersMu.RLock()
	handlers := append([]EventHandler(nil), c.handlers[name]...)
	c.handlersMu.RUnlock()

	for _, fn := range handlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.log.Error("Event handler panic", "event", name, "err", r)
				}
			}()
			fn(msg)
		}()
	}
}

// ── Send / receive ────────────────────────────────────────────────────────────

func (c *Client) send(payload map[string]any, timeout time.Duration) (*reply, error) {
	if !c.connected.Load() {
		return nil, fmt.Errorf("mpv not connected")
	}

	c.reqMu.Lock()
	c.reqID++
	reqID := c.reqID
	ch := make(chan reply, 1)
	c.pending[reqID] = ch
	c.reqMu.Unlock()

	payload["request_id"] = reqID
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("mpv not connected")
	}
	if _, err := conn.Write(data); err != nil {
		c.connected.Store(false)
		c.reqMu.Lock()
		delete(c.pending, reqID)
		c.reqMu.Unlock()
		return nil, fmt.Errorf("mpv write: %w", err)
	}

	select {
	case r := <-ch:
		return &r, nil
	case <-time.After(timeout):
		c.reqMu.Lock()
		delete(c.pending, reqID)
		c.reqMu.Unlock()
		return nil, fmt.Errorf("mpv reply timeout (req_id=%d)", reqID)
	}
}

// ── Public API ────────────────────────────────────────────────────────────────

// SendCommand sends a raw IPC command list (e.g. ["loadfile", url, "replace"]).
func (c *Client) SendCommand(cmd []any) error {
	_, err := c.send(map[string]any{"command": cmd}, 5*time.Second)
	return err
}

// GetProperty reads an MPV property. Returns the raw JSON data.
func (c *Client) GetProperty(prop string) (json.RawMessage, error) {
	r, err := c.send(map[string]any{"command": []any{"get_property", prop}}, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if r.Error != "success" && r.Error != "" {
		return nil, fmt.Errorf("mpv property error: %s", r.Error)
	}
	return r.Data, nil
}

// SetProperty sets an MPV property.
func (c *Client) SetProperty(prop string, value any) error {
	r, err := c.send(map[string]any{"command": []any{"set_property", prop, value}}, 5*time.Second)
	if err != nil {
		return err
	}
	if r.Error != "success" && r.Error != "" {
		return fmt.Errorf("set_property %s: %s", prop, r.Error)
	}
	return nil
}

// OnEvent registers a callback for an MPV event (e.g. "end-file").
func (c *Client) OnEvent(name string, fn EventHandler) {
	c.handlersMu.Lock()
	c.handlers[name] = append(c.handlers[name], fn)
	c.handlersMu.Unlock()
}

// Loadfile instructs MPV to load a file/URL, replacing current playback.
// Pass recordPath="" to disable stream-record.
func (c *Client) Loadfile(fileURL, recordPath string) {
	if recordPath != "" {
		_ = c.SetProperty("stream-record", recordPath)
	} else {
		_ = c.SetProperty("stream-record", "")
	}
	_ = c.SendCommand([]any{"loadfile", fileURL, "replace"})
}

// Pause pauses playback.
func (c *Client) Pause() error { return c.SetProperty("pause", true) }

// Resume resumes playback.
func (c *Client) Resume() error { return c.SetProperty("pause", false) }

// Stop clears stream-record and stops playback.
func (c *Client) Stop() error {
	_ = c.SetProperty("stream-record", "")
	return c.SendCommand([]any{"stop"})
}

// Seek seeks relative to current position.
func (c *Client) Seek(seconds float64) error {
	return c.SendCommand([]any{"seek", seconds, "relative"})
}

// SetVolume sets volume clamped to [0, 150].
func (c *Client) SetVolume(level int) error {
	if level < 0 {
		level = 0
	}
	if level > 150 {
		level = 150
	}
	return c.SetProperty("volume", level)
}

// Mute toggles mute.
func (c *Client) Mute() error { return c.SendCommand([]any{"cycle", "mute"}) }

// GetStatus returns a structured snapshot of current player state.
func (c *Client) GetStatus() *PlayerStatus {
	paused := c.getBool("pause")
	state := "playing"
	if paused {
		state = "paused"
	}
	title := c.getString("media-title")
	if title == "" {
		title = c.getString("filename")
	}
	return &PlayerStatus{
		State:    state,
		Title:    title,
		Position: roundF(c.getFloat("time-pos")),
		Duration: roundF(c.getFloat("duration")),
		Volume:   roundF(c.getFloat("volume")),
	}
}

// IsIdle returns true when MPV is in --idle mode with no file loaded.
func (c *Client) IsIdle() bool {
	return c.getBool("idle-active")
}

// GetPath returns the currently loaded file/URL path.
func (c *Client) GetPath() string {
	return c.getString("path")
}

// GetStreamRecord returns the current stream-record path.
func (c *Client) GetStreamRecord() string {
	return c.getString("stream-record")
}

// Close shuts down the client.
func (c *Client) Close() {
	c.running.Store(false)
	c.connMu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.connMu.Unlock()
}

// ── property helpers ──────────────────────────────────────────────────────────

func (c *Client) getBool(prop string) bool {
	data, err := c.GetProperty(prop)
	if err != nil || data == nil {
		return false
	}
	var v bool
	_ = json.Unmarshal(data, &v)
	return v
}

func (c *Client) getFloat(prop string) float64 {
	data, err := c.GetProperty(prop)
	if err != nil || data == nil {
		return 0
	}
	var v float64
	_ = json.Unmarshal(data, &v)
	return v
}

func (c *Client) getString(prop string) string {
	data, err := c.GetProperty(prop)
	if err != nil || data == nil {
		return ""
	}
	var v string
	_ = json.Unmarshal(data, &v)
	return v
}

func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

func roundF(f float64) float64 {
	return math.Round(f)
}
