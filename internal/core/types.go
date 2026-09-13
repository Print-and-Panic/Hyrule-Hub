// Package core defines the concurrency-safe contracts and domain types for
// Hyrule Hub, an Ocarina of Time Multiworld client.
//
// # ARCHITECTURE OVERVIEW
//
// The system is split into three strictly decoupled layers, wired together
// with Dependency Injection and Go channels:
//
//	┌───────────────┐   events (chan GameEvent)   ┌─────────────┐
//	│  MemoryBridge │ ──────────────────────────▶ │             │
//	│  (game logic) │                             │     Hub     │  (orchestrator,
//	│               │ ◀────────────────────────── │             │   owns all channels)
//	└───────────────┘   grants (chan ItemGrant)   └──────┬──────┘
//	                                                     │ Publish/Subscribe
//	                                              ┌──────▼──────┐
//	                                              │ NetworkBroker│
//	                                              │ (server I/O) │
//	                                              └─────────────┘
//
// CONCURRENCY RULES (binding on all implementations):
//
//  1. Channel ownership transfer: sending a value on a channel transfers
//     ownership. The sender MUST NOT retain, mutate, or read the value
//     after sending. All payload types are passed by value for this reason.
//  2. No shared mutable state: implementations MUST NOT share pointers,
//     slices, or maps across goroutines. Internal state is owned by a
//     single goroutine (typically the one running Start).
//  3. Context cancellation is the ONLY shutdown signal. Implementations
//     MUST NOT expose Stop() methods or close channels they did not create.
//     The sender closes a channel; the receiver never does.
//  4. No interface in this package may reference a concrete transport
//     (RetroArch, MQTT, etc.). Game logic and server logic must remain
//     separable and independently testable.
package core

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

// Item is an Ocarina of Time item identifier, as defined by the multiworld
// coop context in emulator memory.
type Item uint16

// ItemGrant is an item being delivered INTO the local player's game from
// another world (incoming direction: network -> emulator).
type ItemGrant struct {
	// World is the ID of the world the item originated from.
	World uint16 `json:"world"`
	// Item is the item to grant to the local player.
	Item Item `json:"item"`
	// Key is the server-assigned deduplication/ack key for this grant.
	Key uint64 `json:"key"`
}

func (g ItemGrant) String() string {
	return fmt.Sprintf("ItemGrant{World: %d, Item: %d, Key: %d}", g.World, g.Item, g.Key)
}

// PlayerName associates a world/player ID with a display name. Name
// updates flow in both directions: the Hub broadcasts the local player's
// name at startup, and remote name updates are written into emulator
// memory so the game can display who sent each item.
type PlayerName struct {
	// PlayerID is the world number the name belongs to (1-based).
	PlayerID uint8 `json:"playerId"`
	// Name is the player's display name.
	Name string `json:"name"`
}

// LocationCheck is an item the local player has found that belongs to
// another world (outgoing direction: emulator -> network).
type LocationCheck struct {
	// World is the ID of the world that should receive the item.
	World uint16 `json:"world"`
	// Item is the item that was found.
	Item Item `json:"item"`
	// OutgoingKey is the unique key the game generated for this check.
	// It acts as a semaphore: the game holds it until the client
	// acknowledges delivery by clearing it in memory.
	OutgoingKey uint64 `json:"outgoingKey"`
}

func (l LocationCheck) String() string {
	return fmt.Sprintf("LocationCheck{World: %d, Item: %d, OutgoingKey: %d}", l.World, l.Item, l.OutgoingKey)
}

// EventKind discriminates the payload of a GameEvent.
type EventKind uint8

const (
	EventKindUnknown EventKind = iota
	// EventKindLocationCheck indicates Payload is a LocationCheck.
	EventKindLocationCheck
	// EventKindError indicates Payload is an error. Errors emitted as events
	// are non-fatal; fatal errors are returned from Start.
	EventKindError
)

// GameEvent is the unit of communication from the MemoryBridge to the rest
// of the system. It is passed by value over channels; ownership transfers
// to the receiver on send (see concurrency rule 1).
type GameEvent struct {
	Kind      EventKind
	Payload   any // LocationCheck or error, per Kind
	EmittedAt time.Time
}

// ---------------------------------------------------------------------------
// Memory layer (game logic)
// ---------------------------------------------------------------------------

// MemoryAccessor is the generic low-level primitive for reading and writing
// emulator memory. It is intentionally emulator-agnostic: the RetroArch
// implementation (network command interface over UDP) is one possible
// backend. Addresses are N64 virtual addresses; implementations are
// responsible for physical translation and endianness handling.
//
// Implementations must be safe for concurrent use, but callers should note
// that most transports serialize I/O internally anyway.
type MemoryAccessor interface {
	// ReadMemory reads size bytes starting at vAddr.
	ReadMemory(ctx context.Context, vAddr uint32, size uint32) ([]byte, error)
	// WriteMemory writes data starting at vAddr.
	WriteMemory(ctx context.Context, vAddr uint32, data []byte) error
}

// MemoryBridge translates raw emulator memory into domain events and applies
// domain commands back to memory. It is constructed via Dependency
// Injection with a MemoryAccessor and its communication channels:
//
//	bridge := retroarch.NewBridge(accessor, eventsOut, grantsIn)
//
// The bridge MUST NOT know that a network exists. It reads memory, emits
// GameEvents, and consumes ItemGrants. Nothing more.
type MemoryBridge interface {
	// Start begins the polling loop and blocks until ctx is cancelled or a
	// fatal error occurs. It MUST be called exactly once.
	//
	// REQUIRED LOOP SHAPE (binding on all implementations):
	//
	//	events := time.NewTicker(time.Second / 30) // 30fps poll rate
	//	defer events.Stop()
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			return ctx.Err()
	//		case grant := <-grantsIn:
	//			// apply incoming item to emulator memory
	//		case <-events.C:
	//			// poll memory, emit GameEvents on eventsOut
	//		default:
	//			// NON-BLOCKING is mandatory: never let one case starve the
	//			// others. If no case is ready, fall through. Implementations
	//			// may park on a small sleep here to avoid a hot spin, but MUST
	//			// NOT block on I/O while holding internal locks.
	//		}
	//	}
	//
	// Rules:
	//   - The loop MUST use a non-blocking select with a default case.
	//   - Memory polling MUST be driven by a 30fps ticker
	//     (time.NewTicker(time.Second / 30)), not a free-running loop.
	//   - All sends on the injected events channel MUST themselves be
	//     non-blocking (select with default) so a slow consumer can never
	//     deadlock the bridge; dropped events are acceptable, corrupted
	//     memory is not.
	//   - Start MUST NOT close the injected channels; it does not own them.
	Start(ctx context.Context) error
}

// ---------------------------------------------------------------------------
// Network layer (server logic)
// ---------------------------------------------------------------------------

// QoS mirrors MQTT semantics. The server side is expected to be MQTT, and
// the reference deployment runs on full computers, so the default contract
// is QoSExactlyOnce (MQTT QoS 2): the overhead is acceptable and item grants
// must not be duplicated.
type QoS uint8

const (
	QoSAtMostOnce QoS = iota
	QoSAtLeastOnce
	// QoSExactlyOnce is the REQUIRED level for item grant and location check
	// traffic.
	QoSExactlyOnce
)

// Message is the pub/sub envelope. Payload is an opaque, already-encoded
// byte slice: the broker MUST NOT know about game domain types, and the
// game layer MUST NOT know about the wire encoding. Serialization (e.g.
// JSON of ItemGrant/LocationCheck) happens at the Hub boundary.
type Message struct {
	Topic   string
	Payload []byte
	QoS     QoS
	// Retain mirrors MQTT semantics: whether the broker should persist the
	// last message on the topic for future subscribers.
	Retain bool
}

// NetworkBroker is a generic pub/sub broker. It is constructed via
// Dependency Injection with its connection configuration and knows nothing
// about Ocarina of Time, items, or emulator memory.
//
// The Hub subscribes to grant topics and publishes location-check topics;
// the mapping between domain types and topics lives in the Hub, not here.
type NetworkBroker interface {
	// Connect establishes the session and blocks until the connection is
	// ready or ctx is cancelled. Reconnect-after-drop behavior is an
	// implementation detail, but publishers/subscribers MUST observe
	// reconnection as resumed delivery, not as errors, where possible.
	Connect(ctx context.Context) error

	// Publish sends a message. Delivery guarantees follow Message.QoS.
	// Publish MUST NOT block indefinitely; it must respect ctx cancellation.
	Publish(ctx context.Context, msg Message) error

	// Subscribe registers interest in a topic and returns a receive channel.
	// The broker owns and closes the returned channel on Disconnect or ctx
	// cancellation; the receiver MUST NOT close it. Buffering the channel
	// is REQUIRED so that a slow consumer cannot stall the network read
	// loop.
	Subscribe(ctx context.Context, topic string, qos QoS) (<-chan Message, error)

	// Disconnect tears down the session and closes all subscription
	// channels created by Subscribe.
	Disconnect(ctx context.Context) error
}
