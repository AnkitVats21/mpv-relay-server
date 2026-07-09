package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Step struct {
	WaitMs  int
	Action  string
	Payload map[string]any
}

type ScenarioResult struct {
	Name             string
	Steps            int
	Passed           bool
	Error            string
	Duration         time.Duration
	MaxThroughputBps float64
	MinThroughputBps float64

	// PCM audio health (populated after stream ends)
	PCM PCMStats
}

func (r ScenarioResult) HealthWarnings() []string {
	var w []string
	if r.PCM.SilenceWarning {
		ms := float64(r.PCM.MaxSilenceRun) / float64(DefaultPCMConfig.SampleRate) * 1000
		w = append(w, fmt.Sprintf("SILENCE: longest run %d samples (%.0f ms)", r.PCM.MaxSilenceRun, ms))
	}
	if r.PCM.ClippingWarning {
		w = append(w, fmt.Sprintf("CLIPPING: %.1f%% of samples at ±32767", r.PCM.ClippingPercent))
	}
	return w
}

func getSteps(name string) []Step {
	switch name {
	case "basic_play":
		return []Step{
			{WaitMs: 0, Action: "play", Payload: map[string]any{"query": "lofi hip hop radio"}},
			{WaitMs: 15000, Action: "stop"},
		}
	case "play_pause_resume":
		return []Step{
			{WaitMs: 0, Action: "play", Payload: map[string]any{"query": "Pink Floyd Comfortably Numb"}},
			{WaitMs: 8000, Action: "pause"},
			{WaitMs: 4000, Action: "resume"},
			{WaitMs: 10000, Action: "stop"},
		}
	case "queue_and_skip":
		return []Step{
			{WaitMs: 0, Action: "play", Payload: map[string]any{"query": "Bohemian Rhapsody Queen"}},
			{WaitMs: 0, Action: "queue", Payload: map[string]any{"query": "Hotel California Eagles"}},
			{WaitMs: 0, Action: "queue", Payload: map[string]any{"query": "Stairway to Heaven Led Zeppelin"}},
			{WaitMs: 10000, Action: "skip"},
			{WaitMs: 10000, Action: "skip"},
			{WaitMs: 10000, Action: "stop"},
		}
	case "disconnect_reconnect":
		return []Step{
			{WaitMs: 0, Action: "play", Payload: map[string]any{"query": "Daft Punk Get Lucky"}},
			{WaitMs: 5000, Action: "disconnect"}, // drop HTTP connection mid-stream
			{WaitMs: 2000, Action: "reconnect"},  // re-GET /stream with same token
			{WaitMs: 8000, Action: "stop"},
		}
	case "cache_miss_live":
		return []Step{
			// Force live yt-dlp pipe by using a fresh URL not on disk
			{WaitMs: 0, Action: "play", Payload: map[string]any{"query": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"}},
			{WaitMs: 20000, Action: "stop"},
		}
	default:
		return nil
	}
}

func resolveStreamURL(rawURL string, serverOverride string) (string, error) {
	if serverOverride == "" {
		return rawURL, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	overrideU, err := url.Parse(serverOverride)
	if err != nil {
		return "", err
	}
	u.Scheme = overrideU.Scheme
	u.Host = overrideU.Host
	return u.String(), nil
}

// wavPathForScenario returns the WAV dump path for a given scenario name.
// Returns "" if dumpWAVBase is empty (no dump requested).
// For "all" runs each scenario gets its own file: base_<name>.wav
func wavPathForScenario(dumpWAVBase, scenarioName string) string {
	if dumpWAVBase == "" {
		return ""
	}
	// Strip .wav suffix so we can append _<name>.wav
	base := strings.TrimSuffix(dumpWAVBase, ".wav")
	return fmt.Sprintf("%s_%s.wav", base, scenarioName)
}

// RunScenario executes all steps of the named scenario sequentially.
// dumpWAVBase: if non-empty, the first stream of this scenario is saved as
//   <dumpWAVBase>_<scenarioName>.wav so you can play it back with aplay/VLC.
func RunScenario(name string, mqtt *SimMQTT, player *StreamPlayer, serverOverride string, timeoutSec int, dumpWAVBase string, log *slog.Logger) ScenarioResult {
	startTime := time.Now()
	log.Info("Running scenario", "name", name)

	dumpPath := wavPathForScenario(dumpWAVBase, name)
	if dumpPath != "" {
		log.Info("WAV dump enabled", "path", dumpPath)
	}

	// Drain stale START_STREAM messages from previous scenarios
	// to prevent their expired tokens from causing a false 403.
	mqtt.FlushStartStream(500 * time.Millisecond)

	// Clean up player state from any previous scenario
	player.Disconnect()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	var mu sync.Mutex
	var minThroughput float64 = -1
	var maxThroughput float64 = 0
	var failureError string
	var latestPCMStats PCMStats
	wavUsed := false // only dump WAV for the first stream connection

	failScenario := func(reason string) {
		mu.Lock()
		if failureError == "" {
			failureError = reason
		}
		mu.Unlock()
		cancel()
	}

	mergePCM := func(s PCMStats) {
		mu.Lock()
		latestPCMStats = s
		mu.Unlock()
	}

	// 1. Background START_STREAM listener: auto-connects the HTTP stream
	//    whenever the server sends a START_STREAM message.
	go func() {
		for {
			waitCtx, waitCancel := context.WithCancel(ctx)
			urlStr, _, _, _, err := mqtt.WaitForStartStream(waitCtx)
			waitCancel()
			if err != nil {
				return
			}
			resolvedURL, err := resolveStreamURL(urlStr, serverOverride)
			if err != nil {
				log.Error("Failed to resolve stream URL", "err", err)
				continue
			}

			// Disconnect any active stream first
			player.Disconnect()

			// Determine whether to dump WAV for this connection
			mu.Lock()
			var connectDump string
			if !wavUsed && dumpPath != "" {
				connectDump = dumpPath
				wavUsed = true
			}
			mu.Unlock()

			log.Info("Auto-connecting to stream URL", "url", resolvedURL, "dump_wav", connectDump)
			go func() {
				stats, err := player.Connect(ctx, resolvedURL, connectDump, DefaultPCMConfig)
				mergePCM(stats)
				if connectDump != "" && stats.TotalSamples > 0 {
					log.Info("WAV dump complete",
						"path", connectDump,
						"samples", stats.TotalSamples,
						"clipping_pct", fmt.Sprintf("%.2f%%", stats.ClippingPercent),
						"max_silence_run", stats.MaxSilenceRun,
					)
					if stats.SilenceWarning {
						log.Warn("PCM health: long silence detected",
							"max_silence_samples", stats.MaxSilenceRun)
					}
					if stats.ClippingWarning {
						log.Warn("PCM health: high clipping rate",
							"clipping_pct", fmt.Sprintf("%.1f%%", stats.ClippingPercent))
					}
				}
				if err != nil {
					if ctx.Err() == nil && !strings.Contains(err.Error(), "context canceled") {
						log.Error("Player connection failed", "err", err)
						if strings.Contains(err.Error(), "403") {
							failScenario("HTTP stream forbidden 403")
						}
					}
				}
			}()
		}
	}()

	// 2. Background throughput checker: uses a rolling average (totalBytes/elapsed
	//    since first byte) with a 6-second startup grace period, so bursty delivery
	//    from the server doesn't cause false failures.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		var rollingStart time.Time
		var rollingStartBytes int64
		var lastTotal int64
		consecutiveLow := 0

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !player.IsStreaming() {
					// Stream not active; reset rolling window for next connection.
					rollingStart = time.Time{}
					consecutiveLow = 0
					continue
				}

				total := player.TotalBytesPulled()

				// Detect a new Connect() call (bytesPulled resets to 0).
				if rollingStart.IsZero() || total < lastTotal {
					rollingStart = time.Now()
					rollingStartBytes = total
					consecutiveLow = 0
				}
				lastTotal = total

				elapsed := time.Since(rollingStart).Seconds()
				if elapsed < 6.0 {
					log.Info("Throughput grace period", "elapsed_s", fmt.Sprintf("%.1f", elapsed))
					continue
				}

				avgRate := float64(total-rollingStartBytes) / elapsed

				mu.Lock()
				if minThroughput < 0 || avgRate < minThroughput {
					minThroughput = avgRate
				}
				if avgRate > maxThroughput {
					maxThroughput = avgRate
				}
				mu.Unlock()

				log.Info("Throughput check (rolling avg)",
					"bytes_per_sec", fmt.Sprintf("%.0f", avgRate),
					"elapsed_s", fmt.Sprintf("%.1f", elapsed),
				)

				if avgRate < 64000 {
					consecutiveLow++
					log.Warn("Low throughput sample",
						"avg_bytes_per_sec", fmt.Sprintf("%.0f", avgRate),
						"consecutive", consecutiveLow,
					)
					if consecutiveLow >= 3 {
						log.Error("Throughput below 64kB/s for 3+ consecutive rolling checks",
							"consecutive", consecutiveLow)
						failScenario("throughput below threshold")
						return
					}
				} else {
					consecutiveLow = 0
				}
			}
		}
	}()

	// 3. Execute scenario steps.
	steps := getSteps(name)
	for idx, step := range steps {
		if step.WaitMs > 0 {
			select {
			case <-ctx.Done():
				mu.Lock()
				errStr := failureError
				pcm := latestPCMStats
				minT, maxT := minThroughput, maxThroughput
				mu.Unlock()
				if errStr == "" {
					errStr = "scenario cancelled or timed out"
				}
				if minT < 0 {
					minT = 0
				}
				return ScenarioResult{
					Name:             name,
					Steps:            idx,
					Passed:           false,
					Error:            errStr,
					Duration:         time.Since(startTime),
					MinThroughputBps: minT,
					MaxThroughputBps: maxT,
					PCM:              pcm,
				}
			case <-time.After(time.Duration(step.WaitMs) * time.Millisecond):
			}
		}

		log.Info("Executing step", "action", step.Action, "payload", step.Payload)
		switch step.Action {
		case "play":
			err := mqtt.Publish("play", step.Payload)
			if err != nil {
				failScenario("mqtt publish failed: " + err.Error())
				break
			}
			// Wait for the server to respond with START_STREAM (auto-connect goroutine handles HTTP)
			waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
			_, _, _, _, err = mqtt.WaitForStartStream(waitCtx)
			waitCancel()
			if err != nil {
				failScenario("wait for start stream: " + err.Error())
				break
			}

		case "queue":
			err := mqtt.Publish("queue", step.Payload)
			if err != nil {
				failScenario("mqtt publish failed: " + err.Error())
				break
			}

		case "skip":
			err := mqtt.Publish("next", nil)
			if err != nil {
				failScenario("mqtt publish failed: " + err.Error())
				break
			}

		case "stop":
			err := mqtt.Publish("stop", nil)
			if err != nil {
				failScenario("mqtt publish failed: " + err.Error())
				break
			}
			player.Disconnect()

		case "pause":
			err := mqtt.Publish("pause", nil)
			if err != nil {
				failScenario("mqtt publish failed: " + err.Error())
				break
			}

		case "resume":
			err := mqtt.Publish("resume", nil)
			if err != nil {
				failScenario("mqtt publish failed: " + err.Error())
				break
			}

		case "disconnect":
			player.Disconnect()

		case "reconnect":
			go func() {
				mu.Lock()
				var reconnectDump string
				if !wavUsed && dumpPath != "" {
					reconnectDump = dumpPath
					wavUsed = true
				}
				mu.Unlock()

				stats, err := player.Reconnect(ctx, reconnectDump, DefaultPCMConfig)
				mergePCM(stats)
				if err != nil {
					if ctx.Err() == nil && !strings.Contains(err.Error(), "context canceled") {
						log.Error("Player reconnect failed", "err", err)
						if strings.Contains(err.Error(), "403") {
							failScenario("reconnect forbidden 403")
						}
					}
				}
			}()
		}

		mu.Lock()
		isFailed := failureError != ""
		mu.Unlock()
		if isFailed {
			break
		}
	}

	// Make sure we stop any active player
	player.Disconnect()

	mu.Lock()
	errStr := failureError
	passed := errStr == ""
	minT := minThroughput
	maxT := maxThroughput
	pcm := latestPCMStats
	mu.Unlock()

	if minT < 0 {
		minT = 0
	}

	return ScenarioResult{
		Name:             name,
		Steps:            len(steps),
		Passed:           passed,
		Error:            errStr,
		Duration:         time.Since(startTime),
		MinThroughputBps: minT,
		MaxThroughputBps: maxT,
		PCM:              pcm,
	}
}
