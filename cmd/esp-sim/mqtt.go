package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/ankitm/mpv-relay/internal/config"
)

type SimMQTT struct {
	client      pahomqtt.Client
	cmdTopic    string
	statusTopic string
	log         *slog.Logger

	mu         sync.Mutex
	startChans []chan startStreamMsg
}

type startStreamMsg struct {
	URL     string
	Token   string
	VideoID string
	Title   string
}

func NewSimMQTT(cfg *config.Config) (*SimMQTT, error) {
	s := &SimMQTT{
		cmdTopic:    cfg.TopicCmd,
		statusTopic: cfg.TopicStatus,
		log:         slog.Default().With("pkg", "sim-mqtt"),
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("ssl://%s:%d", cfg.MQTTBroker, cfg.MQTTPort)).
		SetClientID("esp32-sim-client").
		SetUsername(cfg.MQTTUsername).
		SetPassword(cfg.MQTTPassword).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetKeepAlive(60).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: false})

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		s.log.Info("MQTT Simulator connected — subscribing", "topic", s.statusTopic)
		token := c.Subscribe(s.statusTopic, 1, s.onMsgReceived)
		token.Wait()
		if err := token.Error(); err != nil {
			s.log.Error("MQTT simulator subscribe failed", "topic", s.statusTopic, "err", err)
		}
	})

	s.client = pahomqtt.NewClient(opts)

	token := s.client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt connect failed: %w", err)
	}

	return s, nil
}

// Publish sends {cmd: action, ...payload} to cmdTopic.
func (m *SimMQTT) Publish(action string, payload map[string]any) error {
	m.log.Info("MQTT Publish", "action", action, "payload", payload)
	fullPayload := make(map[string]any)
	for k, v := range payload {
		fullPayload[k] = v
	}
	fullPayload["cmd"] = action

	data, err := json.Marshal(fullPayload)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	token := m.client.Publish(m.cmdTopic, 1, false, data)
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("publish error: %w", err)
	}
	return nil
}

// WaitForStartStream blocks until a START_STREAM message arrives on statusTopic,
// or until ctx is cancelled. Returns (url, token, videoID, title).
func (m *SimMQTT) WaitForStartStream(ctx context.Context) (url, token, videoID, title string, err error) {
	ch := make(chan startStreamMsg, 1)

	m.mu.Lock()
	m.startChans = append(m.startChans, ch)
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		for i, c := range m.startChans {
			if c == ch {
				m.startChans = append(m.startChans[:i], m.startChans[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
	}()

	select {
	case msg := <-ch:
		return msg.URL, msg.Token, msg.VideoID, msg.Title, nil
	case <-ctx.Done():
		return "", "", "", "", ctx.Err()
	}
}

func (m *SimMQTT) onMsgReceived(_ pahomqtt.Client, msg pahomqtt.Message) {
	var payload map[string]any
	if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
		return
	}

	mType, _ := payload["type"].(string)
	if mType == "" {
		mType, _ = payload["cmd"].(string)
	}

	if mType == "START_STREAM" {
		url, _ := payload["url"].(string)
		token, _ := payload["token"].(string)
		videoID, _ := payload["video_id"].(string)
		title, _ := payload["title"].(string)

		m.log.Info("START_STREAM received", "url", url, "token", token, "videoID", videoID, "title", title)

		msgVal := startStreamMsg{
			URL:     url,
			Token:   token,
			VideoID: videoID,
			Title:   title,
		}

		m.mu.Lock()
		for _, ch := range m.startChans {
			select {
			case ch <- msgVal:
			default:
			}
		}
		m.mu.Unlock()
	}
}

// FlushStartStream drains any START_STREAM messages that arrive within d.
// Call this at the start of each scenario to discard stale tokens from
// previous scenarios that the broker may still deliver.
func (m *SimMQTT) FlushStartStream(d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		_, _, _, _, err := m.WaitForStartStream(ctx)
		if err != nil {
			return
		}
		m.log.Info("FlushStartStream: discarded stale START_STREAM")
	}
}

func (m *SimMQTT) Disconnect() {
	if m.client != nil && m.client.IsConnected() {
		m.client.Disconnect(250)
	}
}
