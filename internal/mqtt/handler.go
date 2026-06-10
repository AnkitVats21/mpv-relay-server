// Package mqtt provides the HiveMQ TLS MQTT client for the relay server.
package mqtt

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/ankitm/mpv-relay/internal/config"
)

// Handler wraps paho MQTT with TLS, LWT, and publish helpers.
type Handler struct {
	client    pahomqtt.Client
	cfg       *config.Config
	onCommand func(payload string)
	log       *slog.Logger
}

// New creates a Handler. onCommand is called for every message on TopicCmd.
// Call Connect() to actually connect.
func New(cfg *config.Config, onCommand func(string)) *Handler {
	h := &Handler{
		cfg:       cfg,
		onCommand: onCommand,
		log:       slog.Default().With("pkg", "mqtt"),
	}

	lwt, _ := json.Marshal(map[string]any{"state": "offline", "source": "mpv-relay"})

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("ssl://%s:%d", cfg.MQTTBroker, cfg.MQTTPort)).
		SetClientID(cfg.MQTTClientID).
		SetUsername(cfg.MQTTUsername).
		SetPassword(cfg.MQTTPassword).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetKeepAlive(60).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: false}).
		SetWill(cfg.TopicStatus, string(lwt), 1, true).
		SetOnConnectHandler(h.onConnect).
		SetConnectionLostHandler(h.onDisconnect)

	h.client = pahomqtt.NewClient(opts)
	return h
}

// Connect dials the broker (non-blocking after handshake).
func (h *Handler) Connect() error {
	h.log.Info("Connecting to MQTT broker", "host", h.cfg.MQTTBroker, "port", h.cfg.MQTTPort)
	token := h.client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt connect: %w", err)
	}
	return nil
}

// Disconnect publishes an offline notice and disconnects cleanly.
func (h *Handler) Disconnect() {
	h.PublishJSON(map[string]any{"state": "offline", "source": "mpv-relay"}, true)
	h.client.Disconnect(500)
}

// PublishJSON marshals payload and publishes it to the status topic.
func (h *Handler) PublishJSON(payload map[string]any, retain ...bool) {
	r := len(retain) > 0 && retain[0]
	data, err := json.Marshal(payload)
	if err != nil {
		h.log.Error("Failed to marshal publish payload", "err", err)
		return
	}
	token := h.client.Publish(h.cfg.TopicStatus, 1, r, data)
	token.Wait()
	if err := token.Error(); err != nil {
		h.log.Error("MQTT publish error", "err", err)
	}
}

// PublishOnline announces the relay as online (clears retained LWT).
func (h *Handler) PublishOnline() {
	h.PublishJSON(map[string]any{"state": "online", "source": "mpv-relay"}, true)
}

// PublishError publishes an error event.
func (h *Handler) PublishError(message string) {
	h.PublishJSON(map[string]any{"type": "error", "message": message})
}

// ── Internal callbacks ────────────────────────────────────────────────────────

func (h *Handler) onConnect(client pahomqtt.Client) {
	h.log.Info("MQTT connected — subscribing", "topic", h.cfg.TopicCmd)
	token := client.Subscribe(h.cfg.TopicCmd, 1, h.onMessage)
	token.Wait()
	if err := token.Error(); err != nil {
		h.log.Error("MQTT subscribe failed", "err", err)
		return
	}
	h.PublishOnline()
}

func (h *Handler) onDisconnect(_ pahomqtt.Client, err error) {
	if err != nil {
		h.log.Warn("MQTT unexpectedly disconnected — paho will retry", "err", err)
	} else {
		h.log.Info("MQTT cleanly disconnected")
	}
}

func (h *Handler) onMessage(_ pahomqtt.Client, msg pahomqtt.Message) {
	payload := string(msg.Payload())
	h.log.Debug("MQTT ←", "topic", msg.Topic(), "payload", payload)
	if h.onCommand != nil {
		h.onCommand(payload)
	}
}
