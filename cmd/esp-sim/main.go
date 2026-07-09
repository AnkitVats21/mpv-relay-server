package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ankitm/mpv-relay/internal/config"
)

func main() {
	serverFlag := flag.String("server", "", "Base URL of the relay HTTP stream server")
	timeoutFlag := flag.Int("timeout", 120, "Max total scenario runtime in seconds")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] <scenario>\n\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "Scenarios:\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  basic_play           Play one track for 15s\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  play_pause_resume    Play, pause 8s, resume, stop after 10s\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  queue_and_skip       Queue 3 tracks, skip through them\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  disconnect_reconnect Drop TCP mid-stream, reconnect with same token\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  cache_miss_live      Force yt-dlp live pipe (no cached file)\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  all                  Run all scenarios sequentially, print summary\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Flags:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	scenario := args[0]
	validScenarios := map[string]bool{
		"basic_play":           true,
		"play_pause_resume":    true,
		"queue_and_skip":       true,
		"disconnect_reconnect": true,
		"cache_miss_live":      true,
		"all":                  true,
	}

	if !validScenarios[scenario] {
		fmt.Fprintf(os.Stderr, "Error: Unknown scenario '%s'\n\n", scenario)
		flag.Usage()
		os.Exit(1)
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	serverOverride := *serverFlag
	if serverOverride == "" {
		serverOverride = os.Getenv("STREAMER_URL")
		if serverOverride == "" {
			serverOverride = "http://localhost:8765"
		}
	}

	// Setup logging
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	log.Info("Starting ESP32 Simulator Test Agent", "server", serverOverride, "timeout", *timeoutFlag)

	// Connect to MQTT
	mqttClient, err := NewSimMQTT(cfg)
	if err != nil {
		log.Error("Failed to initialize MQTT simulator client", "err", err)
		os.Exit(1)
	}
	defer mqttClient.Disconnect()

	player := &StreamPlayer{}

	var results []ScenarioResult

	if scenario == "all" {
		scenariosToRun := []string{
			"basic_play",
			"play_pause_resume",
			"queue_and_skip",
			"disconnect_reconnect",
			"cache_miss_live",
		}

		for _, sc := range scenariosToRun {
			res := RunScenario(sc, mqttClient, player, serverOverride, *timeoutFlag, log)
			results = append(results, res)
			// Wait a brief moment between scenarios to let server queue/player clear
			time.Sleep(2 * time.Second)
		}
	} else {
		res := RunScenario(scenario, mqttClient, player, serverOverride, *timeoutFlag, log)
		results = append(results, res)
	}

	// Print summary table
	fmt.Println()
	fmt.Printf("%-25s %-8s %-10s %-9s %-9s\n", "SCENARIO", "PASSED", "DURATION", "MIN_BPS", "MAX_BPS")
	allPassed := true
	for _, res := range results {
		passedStr := "✅"
		if !res.Passed {
			passedStr = "❌"
			allPassed = false
		}
		errSuffix := ""
		if res.Error != "" {
			errSuffix = "   ← " + res.Error
		}
		fmt.Printf("%-25s %-8s %-10s %-9.0f %-9.0f%s\n",
			res.Name, passedStr, fmt.Sprintf("%.1fs", res.Duration.Seconds()),
			res.MinThroughputBps, res.MaxThroughputBps, errSuffix)
	}

	if !allPassed {
		os.Exit(1)
	}
	os.Exit(0)
}
