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
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/ankitm/mpv-relay/internal/config"
	"github.com/ankitm/mpv-relay/internal/db"
	"github.com/ankitm/mpv-relay/internal/mpv"
	mqtthandler "github.com/ankitm/mpv-relay/internal/mqtt"
	"github.com/ankitm/mpv-relay/internal/queue"
	"github.com/ankitm/mpv-relay/internal/resolver"
	"github.com/ankitm/mpv-relay/internal/router"
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
	log.Info("  Socket", "path", cfg.MPVSocket)
	log.Info("  Cache",  "dir", cfg.MusicCacheDir)
	log.Info("  DB",     "path", cfg.DBPath)
	log.Info("  Logs",   "path", cfg.LogPath)
	log.Info(strings.Repeat("═", 60))

	// ── Layer construction (dependency injection) ─────────────────────────────
	database, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Error("Failed to open database", "err", err)
		os.Exit(1)
	}

	mpvClient := mpv.New(cfg.MPVSocket)
	res := resolver.New(database, cfg)

	// mqtt handler needs publish_fn; create with a no-op first, then replace.
	var mqttH *mqtthandler.Handler
	publishFn := func(payload map[string]any) {
		if mqttH != nil {
			mqttH.PublishJSON(payload)
		}
	}

	qMgr := queue.New(mpvClient, res, database, cfg, publishFn)
	rtr := router.New(qMgr, mpvClient, database, publishFn)

	mqttH = mqtthandler.New(cfg, rtr.Dispatch)

	// ── Connect ───────────────────────────────────────────────────────────────
	if err := mqttH.Connect(); err != nil {
		log.Error("MQTT connect failed", "err", err)
		os.Exit(1)
	}

	log.Info("✅ Relay active — listening", "topic", cfg.TopicCmd)

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Info("Caught signal — shutting down", "signal", sig)

	mqttH.Disconnect()
	mpvClient.Close()
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
