// MPV Relay Server — Go port of the Python prototype.
//
// Start command (from the mpv-relay-backend-go directory):
//
//	./mpv-relay
//
// Assumes mpv is already running:
//
//	mpv --idle --no-video --input-ipc-server=/tmp/mpvsocket
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/ankitm/mpv-relay/internal/config"
	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/eviction"
	mqtthandler "github.com/ankitm/mpv-relay/internal/mqtt"
	"github.com/ankitm/mpv-relay/internal/queue"
	"github.com/ankitm/mpv-relay/internal/resource"
	"github.com/ankitm/mpv-relay/internal/resolver"
	"github.com/ankitm/mpv-relay/internal/router"
	"github.com/ankitm/mpv-relay/internal/streamer"
	"github.com/ankitm/mpv-relay/internal/ws"
)

const pidFile = "/tmp/mpv-relay.pid"

func main() {
	// ── PID lock ──────────────────────────────────────────────────────────────
	if err := acquirePIDLock(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	defer os.Remove(pidFile)

	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Config error:", err)
		os.Exit(1)
	}

	// ── Logging ───────────────────────────────────────────────────────────────
	log := setupLogging(cfg.LogPath)

	log.Info(strings.Repeat("═", 60))
	log.Info("  MPV Relay Server starting …")
	log.Info("  Broker", "host", cfg.MQTTBroker, "port", cfg.MQTTPort)
	log.Info("  Cache",  "dir", cfg.MusicCacheDir)
	log.Info("  DB",     "path", cfg.DBPath)
	log.Info("  Logs",   "path", cfg.LogPath)
	log.Info(strings.Repeat("═", 60))

	// ── Layer construction (dependency injection) ─────────────────────────────
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	database, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Error("Failed to open database", "err", err)
		os.Exit(1)
	}

	res := resolver.New(database, cfg)

	// Both MQTT & WS handlers need publish_fn and publish_mqtt_raw; create with no-ops first, then replace.
	var mqttH *mqtthandler.Handler
	var wsHub *ws.Hub

	publishFn := func(payload map[string]any) {
		if wsHub != nil {
			wsHub.BroadcastJSON(payload)
		}
	}

	publishMqttRawFn := func(topic string, data []byte) {
		if mqttH != nil {
			mqttH.PublishRaw(topic, data)
		}
	}

	var rtr *router.Router
	mqttMsgHandler := func(topic string, payload []byte) {
		if rtr == nil {
			return
		}
		if topic == cfg.TopicCmd {
			rtr.Dispatch(string(payload))
		} else if topic == "device/waveshare/config/status" {
			if wsHub != nil {
				wsHub.BroadcastJSON(map[string]any{
					"type":    "device_config",
					"content": string(payload),
				})
			}
		} else if topic == "device/waveshare/gemini/status" {
			if wsHub != nil {
				wsHub.BroadcastJSON(map[string]any{
					"type":    "gemini_config",
					"content": string(payload),
				})
			}
		}
	}
	mqttH = mqtthandler.New(cfg, mqttMsgHandler)

	rm := resource.New()

	streamerInst := streamer.New(database, rm, mqttH, cfg.MediaDir, cfg.HTTPStreamPort, res.IsDownloading, res.StartBackgroundDownload)

	qMgr := queue.New(res, database, cfg, publishFn, rm)
	qMgr.SetStreamer(streamerInst)

	rtr = router.New(qMgr, streamerInst, database, publishFn, publishMqttRawFn)

	evictWorker := eviction.New(database, cfg.CacheMaxBytes)
	evictWorker.SetActiveTrackProvider(streamerInst)
	rtr.SetEvictionWorker(evictWorker)
	go evictWorker.Start(rootCtx)

	// Initialize WebSocket hub
	wsHub = ws.NewHub(rtr.Dispatch)
	go wsHub.Run()

	wsMux := http.NewServeMux()
	wsMux.Handle("/ws", wsHub)

	go func() {
		log.Info("Starting HTTP & WebSocket server", "addr", cfg.WSAddr)
		if err := http.ListenAndServe(cfg.WSAddr, wsMux); err != nil {
			log.Error("WebSocket server listen failed", "err", err)
		}
	}()

	// ── Startup recovery ──────────────────────────────────────────────────────
	// Mark any queue entries left in PLAYING state from a previous crash as FAILED
	// so advanceQueue() can continue normally on reconnect.
	qMgr.RecoverFromRestart()

	// ── Connect ───────────────────────────────────────────────────────────────
	if err := mqttH.Connect(); err != nil {
		log.Error("MQTT connect failed", "err", err)
		os.Exit(1)
	}

	// Start the paused-session expiry goroutine
	streamerInst.StartExpiryWorker(rootCtx)

	// Mount HTTP stream handler
	streamMux := http.NewServeMux()
	streamerInst.RegisterHTTPHandler(streamMux)

	// WriteTimeout = 0: the /stream endpoint is a long-lived connection.
	// The stream handler manages its own lifecycle via context cancellation.
	// A short WriteTimeout would kill legitimate long-paused connections.
	streamServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPStreamPort),
		Handler:      streamMux,
		WriteTimeout: 0,
		ReadTimeout:  30 * time.Second,
	}

	// Start HTTP server in goroutine
	go func() {
		log.Info("HTTP stream server listening", "addr", streamServer.Addr)
		if err := streamServer.ListenAndServe(); err != nil {
			log.Error("HTTP stream server failed — triggering shutdown", "err", err)
			cancel()
		}
	}()

	// Set GOMEMLIMIT
	if os.Getenv("GOMEMLIMIT") == "" {
		// Soft fallback: set it programmatically via runtime/debug
		debug.SetMemoryLimit(350 * 1024 * 1024)
	}

	log.Info("✅ Relay active — listening", "topic", cfg.TopicCmd)

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Info("Caught signal — shutting down", "signal", sig)
	cancel()

	mqttH.Disconnect()
	if err := database.Close(); err != nil {
		log.Warn("DB close error", "err", err)
	}
	log.Info("Goodbye.")
}

// acquirePIDLock writes our PID to pidFile, or exits if another instance runs.
func acquirePIDLock() error {
	if data, err := os.ReadFile(pidFile); err == nil {
		oldPID, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err == nil {
			// Check if that process is alive (kill -0)
			proc, err := os.FindProcess(oldPID)
			if err == nil {
				if err := proc.Signal(syscall.Signal(0)); err == nil {
					return fmt.Errorf("another mpv-relay instance is already running (PID %d)", oldPID)
				}
			}
		}
		// Stale lock — safe to overwrite
	}
	return os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o644)
}

// setupLogging configures slog with stdout + rotating file handlers.
func setupLogging(logPath string) *slog.Logger {
	// Ensure log directory exists
	_ = os.MkdirAll(filepath.Dir(logPath), 0o755)

	fileWriter := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    10,   // MB
		MaxBackups: 5,
		Compress:   false,
	}

	multi := io.MultiWriter(os.Stdout, fileWriter)

	handler := slog.NewTextHandler(multi, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	log := slog.New(handler)
	slog.SetDefault(log)
	return log
}
