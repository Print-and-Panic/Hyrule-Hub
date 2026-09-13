// Package hub is the orchestrator of Hyrule Hub. It owns the channels that
// connect the MemoryBridge (emulator side) to the NetworkBroker (server
// side) and contains all mapping between game domain types and wire
// topics/encodings. Neither the bridge nor the broker knows the other
// exists.
//
// Data flow:
//
//	emulator --LocationCheck--> Hub --ItemGrant JSON--> players/{world}/items
//	players/{id}/items --JSON--> Hub --ItemGrant--> emulator
//	players/+/name     --JSON--> Hub --PlayerName--> emulator
package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/core"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

// channel buffer sizes. Generous buffers absorb bursts (e.g. a dungeon of
// items found at once) without back-pressuring the bridge.
const (
	eventChanBuffer = 256
	grantChanBuffer = 256
	nameChanBuffer  = 32
)

// retryInterval is how often the Hub retries publishes that previously
// failed (e.g. while the broker connection was down).
const retryInterval = 500 * time.Millisecond

// Config is the player-supplied session configuration.
type Config struct {
	// PlayerName is broadcast to the room so other games can display it.
	PlayerName string
	// WorldNumber is this player's world ID (1-based).
	WorldNumber uint8
	// RoomName scopes every topic so unrelated sessions never mix.
	RoomName string
}

// pendingItem is a LocationCheck whose publish has not succeeded yet. The
// game has already released its semaphore, so these must be retried until
// they reach the broker or the process exits.
type pendingItem struct {
	target uint16 // destination world (topic), NOT the grant's World field
	grant  core.ItemGrant
}

// Hub owns the channels between the bridge and the broker. Construct it
// with New, hand its channel ends to the bridge, then call Run.
type Hub struct {
	cfg    Config
	broker core.NetworkBroker

	// Channel ownership: the Hub creates all three; the bridge receives
	// the send end of events and the receive ends of grants/names.
	events chan core.GameEvent
	grants chan core.ItemGrant
	names  chan core.PlayerName
}

// New builds a Hub. The broker must be connected before Run is called.
func New(cfg Config, broker core.NetworkBroker) *Hub {
	return &Hub{
		cfg:    cfg,
		broker: broker,
		events: make(chan core.GameEvent, eventChanBuffer),
		grants: make(chan core.ItemGrant, grantChanBuffer),
		names:  make(chan core.PlayerName, nameChanBuffer),
	}
}

// Events returns the channel end the MemoryBridge emits on.
func (h *Hub) Events() chan<- core.GameEvent { return h.events }

// Grants returns the channel end the MemoryBridge consumes from.
func (h *Hub) Grants() <-chan core.ItemGrant { return h.grants }

// Names returns the channel end the MemoryBridge consumes player names from.
func (h *Hub) Names() <-chan core.PlayerName { return h.names }

// Run subscribes to this player's item topic and the room's name wildcard,
// announces the local player, then routes traffic until ctx is cancelled.
// It returns ctx.Err() on shutdown, or a fatal error if a subscription is
// lost underneath it.
func (h *Hub) Run(ctx context.Context) error {
	itemMsgs, err := h.broker.Subscribe(ctx, h.itemTopic(uint16(h.cfg.WorldNumber)), core.QoSExactlyOnce)
	if err != nil {
		return fmt.Errorf("failed to subscribe to items: %w", err)
	}
	nameMsgs, err := h.broker.Subscribe(ctx, h.namesWildcardTopic(), core.QoSAtLeastOnce)
	if err != nil {
		return fmt.Errorf("failed to subscribe to names: %w", err)
	}

	// Retained announcement so late joiners learn our name immediately.
	if err := h.publishName(ctx); err != nil {
		log.Printf("hub: failed to broadcast name (continuing): %v", err)
	}

	retry := time.NewTicker(retryInterval)
	defer retry.Stop()

	// Loop-local state (concurrency rule 2: single-goroutine ownership).
	sentKeys := make(map[uint64]struct{})
	var pending []pendingItem
	players := make(map[uint8]string)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev := <-h.events:
			switch ev.Kind {
			case core.EventKindLocationCheck:
				check, ok := ev.Payload.(core.LocationCheck)
				if !ok {
					log.Printf("hub: malformed LocationCheck payload %T", ev.Payload)
					continue
				}
				if _, dup := sentKeys[check.OutgoingKey]; dup {
					continue
				}
				// World in the payload is the SENDER (us): the receiving
				// game writes it into its "from player" field.
				grant := core.ItemGrant{
					World: uint16(h.cfg.WorldNumber),
					Item:  check.Item,
					Key:   check.OutgoingKey,
				}
				if err := h.publishGrant(ctx, check.World, grant); err != nil {
					log.Printf("hub: publish to world %d failed, will retry: %v", check.World, err)
					pending = append(pending, pendingItem{target: check.World, grant: grant})
					continue
				}
				sentKeys[check.OutgoingKey] = struct{}{}
				log.Printf("hub: sent %s to world %d (key %d)",
					randomizer.OotItem(check.Item), check.World, check.OutgoingKey)

			case core.EventKindError:
				if err, ok := ev.Payload.(error); ok {
					log.Printf("hub: bridge error: %v", err)
				}

			default:
				log.Printf("hub: unknown event kind %d", ev.Kind)
			}

		case msg, ok := <-itemMsgs:
			if !ok {
				return fmt.Errorf("item subscription closed")
			}
			var grant core.ItemGrant
			if err := json.Unmarshal(msg.Payload, &grant); err != nil {
				log.Printf("hub: dropping unparseable item message: %v", err)
				continue
			}
			select {
			case h.grants <- grant:
				log.Printf("hub: received %s from world %d (key %d)",
					randomizer.OotItem(grant.Item), grant.World, grant.Key)
			case <-ctx.Done():
				return ctx.Err()
			}

		case msg, ok := <-nameMsgs:
			if !ok {
				return fmt.Errorf("name subscription closed")
			}
			var name core.PlayerName
			if err := json.Unmarshal(msg.Payload, &name); err != nil {
				log.Printf("hub: dropping unparseable name message: %v", err)
				continue
			}
			if name.PlayerID == h.cfg.WorldNumber {
				continue // our own retained announcement
			}
			players[name.PlayerID] = name.Name
			select {
			case h.names <- name:
			case <-ctx.Done():
				return ctx.Err()
			}

		case <-retry.C:
			if len(pending) == 0 {
				continue
			}
			remaining := pending[:0]
			for _, p := range pending {
				if err := h.publishGrant(ctx, p.target, p.grant); err != nil {
					remaining = append(remaining, p)
					continue
				}
				sentKeys[p.grant.Key] = struct{}{}
				log.Printf("hub: retry delivered %s to world %d (key %d)",
					randomizer.OotItem(p.grant.Item), p.target, p.grant.Key)
			}
			pending = remaining
		}
	}
}

// publishGrant serializes a grant and publishes it to the target world's
// item topic at the contract-required exactly-once QoS.
func (h *Hub) publishGrant(ctx context.Context, targetWorld uint16, grant core.ItemGrant) error {
	payload, err := json.Marshal(grant)
	if err != nil {
		return fmt.Errorf("failed to marshal item grant: %w", err)
	}
	return h.broker.Publish(ctx, core.Message{
		Topic:   h.itemTopic(targetWorld),
		Payload: payload,
		QoS:     core.QoSExactlyOnce,
	})
}

// publishName announces the local player on its retained name topic.
func (h *Hub) publishName(ctx context.Context) error {
	payload, err := json.Marshal(core.PlayerName{
		PlayerID: h.cfg.WorldNumber,
		Name:     h.cfg.PlayerName,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal player name: %w", err)
	}
	return h.broker.Publish(ctx, core.Message{
		Topic:   h.nameTopic(h.cfg.WorldNumber),
		Payload: payload,
		QoS:     core.QoSAtLeastOnce,
		Retain:  true,
	})
}

func (h *Hub) itemTopic(world uint16) string {
	return fmt.Sprintf("pnp/rooms/%s/players/%d/items", h.cfg.RoomName, world)
}

func (h *Hub) nameTopic(playerID uint8) string {
	return fmt.Sprintf("pnp/rooms/%s/players/%d/name", h.cfg.RoomName, playerID)
}

func (h *Hub) namesWildcardTopic() string {
	return fmt.Sprintf("pnp/rooms/%s/players/+/name", h.cfg.RoomName)
}
