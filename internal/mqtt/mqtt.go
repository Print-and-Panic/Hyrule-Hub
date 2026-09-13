// Package mqtt implements core.NetworkBroker over MQTT using the Eclipse
// Paho client. It knows nothing about Ocarina of Time: topics, payloads,
// and QoS are supplied by the caller.
package mqtt

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/core"
	paho "github.com/eclipse/paho.mqtt.golang"
)

// subChanBuffer bounds each subscription channel. A slow consumer drops
// messages rather than stalling the Paho read loop, per the NetworkBroker
// contract.
const subChanBuffer = 256

// Client implements core.NetworkBroker.
type Client struct {
	client paho.Client

	mu   sync.Mutex
	subs []*subscription
}

// NewClient builds a client for brokerURL. clientID must be unique per
// player per room so the broker can hold a persistent session (missed QoS
// messages are redelivered on reconnect).
func NewClient(brokerURL, clientID string) *Client {
	opts := paho.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID)
	opts.SetCleanSession(false) // Remember our missed items if we disconnect!
	opts.SetAutoReconnect(true)
	opts.SetResumeSubs(true)
	opts.SetConnectTimeout(10 * time.Second)

	return &Client{client: paho.NewClient(opts)}
}

// Connect implements core.NetworkBroker.
func (c *Client) Connect(ctx context.Context) error {
	token := c.client.Connect()
	if err := waitToken(ctx, token); err != nil {
		return fmt.Errorf("MQTT connection failed: %w", err)
	}
	return nil
}

// Publish implements core.NetworkBroker.
func (c *Client) Publish(ctx context.Context, msg core.Message) error {
	token := c.client.Publish(msg.Topic, byte(msg.QoS), msg.Retain, msg.Payload)
	if err := waitToken(ctx, token); err != nil {
		return fmt.Errorf("failed to publish to %q: %w", msg.Topic, err)
	}
	return nil
}

// Subscribe implements core.NetworkBroker. The returned channel is owned by
// the client and closed by Disconnect.
func (c *Client) Subscribe(ctx context.Context, topic string, qos core.QoS) (<-chan core.Message, error) {
	sub := &subscription{ch: make(chan core.Message, subChanBuffer)}

	token := c.client.Subscribe(topic, byte(qos), func(_ paho.Client, m paho.Message) {
		sub.deliver(core.Message{Topic: m.Topic(), Payload: bytes.Clone(m.Payload())})
	})
	if err := waitToken(ctx, token); err != nil {
		return nil, fmt.Errorf("failed to subscribe to %q: %w", topic, err)
	}

	c.mu.Lock()
	c.subs = append(c.subs, sub)
	c.mu.Unlock()

	return sub.ch, nil
}

// Disconnect implements core.NetworkBroker.
func (c *Client) Disconnect(_ context.Context) error {
	c.client.Disconnect(250) // Wait 250ms to finish inflight messages

	c.mu.Lock()
	for _, s := range c.subs {
		s.close()
	}
	c.subs = nil
	c.mu.Unlock()

	return nil
}

// waitToken blocks until a Paho token completes or ctx is cancelled.
func waitToken(ctx context.Context, token paho.Token) error {
	select {
	case <-token.Done():
		return token.Error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// subscription wraps a receive channel so Disconnect can close it safely
// even if a Paho callback is mid-delivery.
type subscription struct {
	mu     sync.Mutex
	ch     chan core.Message
	closed bool
}

// deliver performs a non-blocking send. It runs on a Paho callback
// goroutine, so it must never block.
func (s *subscription) deliver(msg core.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	select {
	case s.ch <- msg:
	default:
		log.Printf("mqtt: subscription buffer full, dropping message on %q", msg.Topic)
	}
}

func (s *subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}
