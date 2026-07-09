package main

import (
	"context"
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
			{WaitMs: 5000, Action: "disconnect"},
			{WaitMs: 2000, Action: "reconnect"},
			{WaitMs: 8000, Action: "stop"},
		}
	case "cache_miss_live":
		return []Step{
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

func RunScenario(name string, mqtt *SimMQTT, player *StreamPlayer, serverOverride string, timeoutSec int, log *slog.Logger) ScenarioResult {
	startTime := time.Now()
	log.Info("Running scenario", "name", name)

	// Clean up player state
	player.Disconnect()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	var mu sync.Mutex
	var minThroughput float64 = -1
	var maxThroughput float64 = 0
	var failureError string

	failScenario := func(reason string) {
		mu.Lock()
		if failureError == "" {
			failureError = reason
		}
		mu.Unlock()
		cancel()
	}

	// 1. Start background START_STREAM listener
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

			log.Info("Auto-connecting to stream URL", "url", resolvedURL)
			go func() {
				if err := player.Connect(ctx, resolvedURL); err != nil {
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

	// 2. Start background throughput checker
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		consecutiveLow := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if player.IsStreaming() {
					throughput := player.Throughput()
					log.Info("Throughput check", "bytes_per_sec", throughput)

					mu.Lock()
					if minThroughput < 0 || throughput < minThroughput {
						minThroughput = throughput
					}
					if throughput > maxThroughput {
						maxThroughput = throughput
					}
					mu.Unlock()

					if throughput < 64000 {
						consecutiveLow++
						log.Warn("Low throughput sample", "throughput", throughput, "consecutive", consecutiveLow)
						if consecutiveLow > 3 {
							log.Error("Throughput below threshold for more than 3 consecutive samples", "consecutive", consecutiveLow)
							failScenario("throughput below threshold")
							return
						}
					} else {
						consecutiveLow = 0
					}
				}
			}
		}
	}()

	// Get steps
	steps := getSteps(name)
	for idx, step := range steps {
		// Sleep wait time or exit if cancelled
		if step.WaitMs > 0 {
			select {
			case <-ctx.Done():
				mu.Lock()
				errStr := failureError
				mu.Unlock()
				if errStr == "" {
					errStr = "scenario cancelled or timed out"
				}
				return ScenarioResult{
					Name:             name,
					Steps:            idx,
					Passed:           false,
					Error:            errStr,
					Duration:         time.Since(startTime),
					MinThroughputBps: minThroughput,
					MaxThroughputBps: maxThroughput,
				}
			case <-time.After(time.Duration(step.WaitMs) * time.Millisecond):
			}
		}

		// Run action
		log.Info("Executing step", "action", step.Action, "payload", step.Payload)
		switch step.Action {
		case "play":
			err := mqtt.Publish("play", step.Payload)
			if err != nil {
				failScenario("mqtt publish failed: " + err.Error())
				break
			}
			// Wait for START_STREAM
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
				if err := player.Reconnect(ctx); err != nil {
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
	}
}
