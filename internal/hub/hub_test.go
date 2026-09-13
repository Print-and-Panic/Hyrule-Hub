package hub

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/core"
)

// fakeBroker is an in-memory core.NetworkBroker. Published messages are
// recorded; subscriptions are keyed by topic so tests can inject messages.
type fakeBroker struct {
	mu        sync.Mutex
	published []core.Message
	subs      map[string]chan core.Message
	failures  int // remaining Publish calls that should fail
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{subs: make(map[string]chan core.Message)}
}

func (f *fakeBroker) Connect(context.Context) error { return nil }

func (f *fakeBroker) Disconnect(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.subs {
		close(ch)
	}
	return nil
}

func (f *fakeBroker) Subscribe(_ context.Context, topic string, _ core.QoS) (<-chan core.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan core.Message, 16)
	f.subs[topic] = ch
	return ch, nil
}

func (f *fakeBroker) Publish(_ context.Context, msg core.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return errors.New("broker unavailable")
	}
	f.published = append(f.published, msg)
	return nil
}

// deliver injects a raw message as if the broker received it on topic.
func (f *fakeBroker) deliver(t *testing.T, topic string, payload any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal test payload: %v", err)
	}
	f.mu.Lock()
	ch, ok := f.subs[topic]
	f.mu.Unlock()
	if !ok {
		t.Fatalf("no subscription for topic %q", topic)
	}
	ch <- core.Message{Topic: topic, Payload: raw}
}

// publishedTo returns messages published to topic, in order.
func (f *fakeBroker) publishedTo(topic string) []core.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.Message
	for _, m := range f.published {
		if m.Topic == topic {
			out = append(out, m)
		}
	}
	return out
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func startHub(h *Hub) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	return cancel, done
}

func assertStopped(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation (goroutine leak)")
	}
}

func testConfig() Config {
	return Config{PlayerName: "Link", WorldNumber: 1, RoomName: "test-room"}
}

func TestHubPublishesLocationCheckAsItemGrant(t *testing.T) {
	broker := newFakeBroker()
	h := New(testConfig(), broker)
	cancel, done := startHub(h)
	defer cancel()

	h.Events() <- core.GameEvent{
		Kind:      core.EventKindLocationCheck,
		Payload:   core.LocationCheck{World: 3, Item: 0x0029, OutgoingKey: 42},
		EmittedAt: time.Now(),
	}

	wantTopic := "pnp/rooms/test-room/players/3/items"
	waitFor(t, 2*time.Second, "item grant publish", func() bool {
		return len(broker.publishedTo(wantTopic)) == 1
	})

	msg := broker.publishedTo(wantTopic)[0]
	if msg.QoS != core.QoSExactlyOnce {
		t.Errorf("QoS = %v, want QoSExactlyOnce", msg.QoS)
	}
	var grant core.ItemGrant
	if err := json.Unmarshal(msg.Payload, &grant); err != nil {
		t.Fatalf("unparseable grant payload: %v", err)
	}
	// World must be the SENDER (1), so the receiving game knows who it
	// came from. The key must survive the round trip for dedup.
	if grant.World != 1 || grant.Item != 0x0029 || grant.Key != 42 {
		t.Errorf("unexpected grant: %s", grant)
	}

	assertStopped(t, cancel, done)
}

func TestHubDeduplicatesOutgoingKey(t *testing.T) {
	broker := newFakeBroker()
	h := New(testConfig(), broker)
	cancel, done := startHub(h)
	defer cancel()

	for range 2 {
		h.Events() <- core.GameEvent{
			Kind:      core.EventKindLocationCheck,
			Payload:   core.LocationCheck{World: 3, Item: 0x0029, OutgoingKey: 42},
			EmittedAt: time.Now(),
		}
	}

	wantTopic := "pnp/rooms/test-room/players/3/items"
	time.Sleep(200 * time.Millisecond)
	if got := len(broker.publishedTo(wantTopic)); got != 1 {
		t.Errorf("published %d messages for duplicate key, want 1", got)
	}

	assertStopped(t, cancel, done)
}

func TestHubRetriesFailedPublish(t *testing.T) {
	broker := newFakeBroker()
	broker.failures = 1 // first publish fails; the retry tick must recover

	h := New(testConfig(), broker)
	cancel, done := startHub(h)
	defer cancel()

	h.Events() <- core.GameEvent{
		Kind:      core.EventKindLocationCheck,
		Payload:   core.LocationCheck{World: 3, Item: 0x0029, OutgoingKey: 42},
		EmittedAt: time.Now(),
	}

	wantTopic := "pnp/rooms/test-room/players/3/items"
	waitFor(t, 2*time.Second, "retried publish to land", func() bool {
		return len(broker.publishedTo(wantTopic)) == 1
	})

	assertStopped(t, cancel, done)
}

func TestHubForwardsIncomingItemToBridge(t *testing.T) {
	broker := newFakeBroker()
	h := New(testConfig(), broker)
	cancel, done := startHub(h)
	defer cancel()

	waitFor(t, 2*time.Second, "item subscription", func() bool {
		broker.mu.Lock()
		defer broker.mu.Unlock()
		_, ok := broker.subs["pnp/rooms/test-room/players/1/items"]
		return ok
	})

	broker.deliver(t, "pnp/rooms/test-room/players/1/items",
		core.ItemGrant{World: 7, Item: 0x0051, Key: 9001})

	select {
	case grant := <-h.Grants():
		if grant.World != 7 || grant.Item != 0x0051 || grant.Key != 9001 {
			t.Errorf("unexpected grant: %s", grant)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("grant not forwarded to bridge channel")
	}

	assertStopped(t, cancel, done)
}

func TestHubBroadcastsNameOnStart(t *testing.T) {
	broker := newFakeBroker()
	h := New(testConfig(), broker)
	cancel, done := startHub(h)
	defer cancel()

	wantTopic := "pnp/rooms/test-room/players/1/name"
	waitFor(t, 2*time.Second, "retained name broadcast", func() bool {
		return len(broker.publishedTo(wantTopic)) == 1
	})

	msg := broker.publishedTo(wantTopic)[0]
	if !msg.Retain {
		t.Error("name broadcast was not retained")
	}
	var name core.PlayerName
	if err := json.Unmarshal(msg.Payload, &name); err != nil {
		t.Fatalf("unparseable name payload: %v", err)
	}
	if name.PlayerID != 1 || name.Name != "Link" {
		t.Errorf("unexpected name payload: %+v", name)
	}

	assertStopped(t, cancel, done)
}

func TestHubForwardsRemoteNamesToBridge(t *testing.T) {
	broker := newFakeBroker()
	h := New(testConfig(), broker)
	cancel, done := startHub(h)
	defer cancel()

	wildcard := "pnp/rooms/test-room/players/+/name"
	waitFor(t, 2*time.Second, "name subscription", func() bool {
		broker.mu.Lock()
		defer broker.mu.Unlock()
		_, ok := broker.subs[wildcard]
		return ok
	})

	// Our own retained announcement echoes back: must be ignored.
	broker.deliver(t, wildcard, core.PlayerName{PlayerID: 1, Name: "Link"})
	// A remote player's name must reach the bridge.
	broker.deliver(t, wildcard, core.PlayerName{PlayerID: 2, Name: "Zelda"})

	select {
	case name := <-h.Names():
		if name.PlayerID != 2 || name.Name != "Zelda" {
			t.Errorf("unexpected name: %+v", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("remote name not forwarded to bridge channel")
	}

	assertStopped(t, cancel, done)
}
