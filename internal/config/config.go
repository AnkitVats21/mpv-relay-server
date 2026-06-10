// Package config loads the .env file and exposes typed configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration for the relay server.
type Config struct {
	// MQTT
	MQTTBroker   string
	MQTTPort     int
	MQTTUsername string
	MQTTPassword string
	MQTTClientID string
	TopicCmd     string
	TopicStatus  string

	// MPV
	MPVSocket string

	// Paths
	MusicCacheDir string
	DBPath        string
	LogPath       string
}

// Load reads the .env file located next to the running binary (or current dir),
// then populates and returns a Config. Panics on any missing required variable.
func Load() (*Config, error) {
	// Try loading .env from the executable's directory, then cwd.
	exe, _ := os.Executable()
	candidates := []string{
		filepath.Join(filepath.Dir(exe), ".env"),
		".env",
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			_ = godotenv.Load(p)
			break
		}
	}

	cfg := &Config{
		MQTTBroker:   require("MQTT_BROKER"),
		MQTTUsername: require("MQTT_USERNAME"),
		MQTTPassword: require("MQTT_PASSWORD"),
		MQTTClientID: getOr("MQTT_CLIENT_ID", "mpv-relay-server"),
		TopicCmd:     getOr("MQTT_TOPIC_CMD", "mpv/command"),
		TopicStatus:  getOr("MQTT_TOPIC_STATUS", "mpv/status"),
		MPVSocket:    getOr("MPV_SOCKET", "/tmp/mpvsocket"),
		MusicCacheDir: expandHome(getOr("MUSIC_CACHE_DIR", "~/mpv-relay/media")),
		DBPath:        expandHome(getOr("DB_PATH", "~/mpv-relay/data/relay.db")),
		LogPath:       expandHome(getOr("LOG_PATH", "~/mpv-relay/logs/relay.log")),
	}

	port, err := strconv.Atoi(getOr("MQTT_PORT", "8883"))
	if err != nil {
		return nil, fmt.Errorf("invalid MQTT_PORT: %w", err)
	}
	cfg.MQTTPort = port

	// Ensure directories exist
	for _, dir := range []string{
		cfg.MusicCacheDir,
		filepath.Dir(cfg.DBPath),
		filepath.Dir(cfg.LogPath),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("cannot create directory %s: %w", dir, err)
		}
	}

	return cfg, nil
}

func require(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("[CONFIG] Missing required env var: %s", key))
	}
	return v
}

func getOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func expandHome(path string) string {
	if len(path) > 1 && path[:2] == "~/" {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, path[2:])
	}
	return path
}
