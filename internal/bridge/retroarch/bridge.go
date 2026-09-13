package retroarch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/core"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

// Coop context layout in emulator memory. The coop context is the shared
// structure the multiworld ROM uses to exchange items with the client. The
// ROM allocates it dynamically: coopContextPointerAddress holds the N64
// virtual address of the context, resolved at runtime by resolveBase.
const (
	coopContextPointerAddress = 0x8040_0000

	playerIDOffset     = 0x0004 // incoming semaphore block (8 bytes)
	outgoingItemOffset = 0x0010 // outgoing item (4 bytes: item u16, world u16)
	playerNamesOffset  = 0x0014 // player name table (8 bytes per player ID)
	outgoingKeyOffset  = 0x0c1c // outgoing key semaphore (8 bytes)
)

// pollInterval is the mandated 30fps memory poll rate.
const pollInterval = time.Second / 30

// idleSleep parks the default branch of the select loop so the mandated
// non-blocking shape does not become a hot spin.
const idleSleep = time.Millisecond

// BlockedSemaphoreError is returned when the game's incoming-item semaphore
// is still occupied by a previous grant. It is retryable: the game will
// consume the pending item and zero the semaphore on its own schedule.
type BlockedSemaphoreError struct {
	PlayerID uint8
	Item     core.Item
}

func (e *BlockedSemaphoreError) Error() string {
	return fmt.Sprintf("semaphore is blocked by player %d with item %d", e.PlayerID, e.Item)
}

// Retryable reports that the operation may succeed on a later attempt.
func (e *BlockedSemaphoreError) Retryable() bool { return true }

// Bridge implements core.MemoryBridge. It translates coop-context memory
// into core.GameEvents and applies core.ItemGrants and core.PlayerNames back
// to memory. It is constructed via Dependency Injection and knows nothing
// about the network.
type Bridge struct {
	ma     core.MemoryAccessor
	events chan<- core.GameEvent
	grants <-chan core.ItemGrant
	names  <-chan core.PlayerName
}

// NewBridge injects the bridge's dependencies. The bridge takes ownership
// of neither channel: it never closes them, and all sends on events are
// non-blocking. grants and names may be nil to disable those inputs.
func NewBridge(ma core.MemoryAccessor, events chan<- core.GameEvent, grants <-chan core.ItemGrant, names <-chan core.PlayerName) *Bridge {
	return &Bridge{
		ma:     ma,
		events: events,
		grants: grants,
		names:  names,
	}
}

// Start implements core.MemoryBridge. It follows the loop shape mandated by
// the core contract: a non-blocking select with a default case, memory
// polling driven by a 30fps ticker, and context cancellation as the only
// shutdown signal. All mutable state (base, pending, pendingNames) is owned
// by this goroutine.
func (b *Bridge) Start(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	grants := b.grants
	names := b.names

	// base is the resolved coop context address. It stays zero until the
	// ROM has initialized the pointer at coopContextPointerAddress.
	var base uint32
	resolveWarned := false

	// pending holds a grant that could not be applied yet because the
	// game's incoming semaphore was occupied. It is retried on every tick.
	var pending *core.ItemGrant

	// pendingNames holds name updates received before the coop context
	// resolved (or that failed to write). Latest value wins per player.
	pendingNames := make(map[uint8]core.PlayerName)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case grant, ok := <-grants:
			if !ok {
				// The Hub closed the channel; disable this case forever.
				grants = nil
				continue
			}
			if pending != nil {
				b.emit(core.GameEvent{
					Kind:      core.EventKindError,
					Payload:   fmt.Errorf("dropping grant %s: previous grant %s still blocked by game semaphore", grant, *pending),
					EmittedAt: time.Now(),
				})
				continue
			}
			g := grant
			pending = &g

		case name, ok := <-names:
			if !ok {
				names = nil
				continue
			}
			pendingNames[name.PlayerID] = name

		case <-ticker.C:
			if base == 0 {
				resolved, err := b.resolveBase(ctx)
				if err != nil {
					if !resolveWarned {
						b.emit(core.GameEvent{
							Kind:      core.EventKindError,
							Payload:   err,
							EmittedAt: time.Now(),
						})
						resolveWarned = true
					}
					continue
				}
				base = resolved
				resolveWarned = false
			}

			for id, name := range pendingNames {
				if err := b.writeName(ctx, base, name); err != nil {
					b.emit(core.GameEvent{
						Kind:      core.EventKindError,
						Payload:   fmt.Errorf("failed to write name for player %d: %w", id, err),
						EmittedAt: time.Now(),
					})
					continue
				}
				delete(pendingNames, id)
			}

			if pending != nil {
				if err := b.pushIncoming(ctx, base, *pending); err != nil {
					var blocked *BlockedSemaphoreError
					if !errors.As(err, &blocked) {
						// Hard failure: report and drop rather than retry
						// forever on a broken emulator.
						b.emit(core.GameEvent{
							Kind:      core.EventKindError,
							Payload:   fmt.Errorf("failed to apply grant %s: %w", *pending, err),
							EmittedAt: time.Now(),
						})
						pending = nil
					}
					// Blocked: keep pending and retry next tick.
				} else {
					pending = nil
				}
			}

			if err := b.pollOutgoing(ctx, base); err != nil {
				b.emit(core.GameEvent{
					Kind:      core.EventKindError,
					Payload:   err,
					EmittedAt: time.Now(),
				})
			}

		default:
			// Non-blocking is mandatory: never let one case starve the
			// others. Park briefly to avoid a hot spin.
			time.Sleep(idleSleep)
		}
	}
}

// resolveBase reads the coop context pointer the ROM maintains at
// coopContextPointerAddress. A zero or non-N64 pointer means the ROM has not
// initialized the context yet; callers should retry on a later tick.
func (b *Bridge) resolveBase(ctx context.Context) (uint32, error) {
	buf, err := b.ma.ReadMemory(ctx, coopContextPointerAddress, 4)
	if err != nil {
		return 0, fmt.Errorf("failed to read coop context pointer: %w", err)
	}
	ptr := binary.BigEndian.Uint32(buf)
	if ptr&0xFF000000 != 0x80000000 {
		return 0, fmt.Errorf("coop context pointer %#08x is not a valid N64 address (ROM not initialized?)", ptr)
	}
	return ptr, nil
}

// pollOutgoing checks the game's outgoing-item semaphore. A non-zero key
// means the local player found an item belonging to another world.
func (b *Bridge) pollOutgoing(ctx context.Context, base uint32) error {
	// Check the key first and minimize memory reads.
	keyBytes, err := b.ma.ReadMemory(ctx, base+outgoingKeyOffset, 8)
	if err != nil {
		return fmt.Errorf("failed to read outgoing key: %w", err)
	}
	key := binary.BigEndian.Uint64(keyBytes)
	if key == 0 {
		return nil
	}

	itemBytes, err := b.ma.ReadMemory(ctx, base+outgoingItemOffset, 4)
	if err != nil {
		return fmt.Errorf("failed to read outgoing item: %w", err)
	}

	check := core.LocationCheck{
		World:       binary.BigEndian.Uint16(itemBytes[2:4]),
		Item:        core.Item(binary.BigEndian.Uint16(itemBytes[0:2])),
		OutgoingKey: key,
	}

	// Only clear the semaphore if the event was accepted. If the consumer's
	// buffer is full, the key stays set and we retry next tick: a dropped
	// event must never become a lost item.
	if !b.emit(core.GameEvent{
		Kind:      core.EventKindLocationCheck,
		Payload:   check,
		EmittedAt: time.Now(),
	}) {
		return nil
	}

	return b.clearOutgoing(ctx, base)
}

// clearOutgoing releases the game's outgoing semaphore. The key is cleared
// before the item so an interruption between the two writes cannot produce
// a key paired with a stale item.
func (b *Bridge) clearOutgoing(ctx context.Context, base uint32) error {
	zeroBuf := make([]byte, 8)

	if err := b.ma.WriteMemory(ctx, base+outgoingKeyOffset, zeroBuf); err != nil {
		return fmt.Errorf("failed to clear outgoing key: %w", err)
	}
	if err := b.ma.WriteMemory(ctx, base+outgoingItemOffset, zeroBuf[:4]); err != nil {
		return fmt.Errorf("failed to clear outgoing item: %w", err)
	}
	return nil
}

// pushIncoming writes a grant into the game's incoming semaphore block, but
// only if the game has consumed the previous grant (both fields zeroed).
// grant.World is the SOURCE world: the game displays it as the item's sender.
func (b *Bridge) pushIncoming(ctx context.Context, base uint32, grant core.ItemGrant) error {
	buf, err := b.ma.ReadMemory(ctx, base+playerIDOffset, 8)
	if err != nil {
		return fmt.Errorf("failed to read semaphore block: %w", err)
	}

	currIncomingPlayer := binary.BigEndian.Uint16(buf[2:4])
	currIncomingItem := binary.BigEndian.Uint16(buf[4:6])

	if currIncomingPlayer != 0 || currIncomingItem != 0 {
		return &BlockedSemaphoreError{
			PlayerID: uint8(currIncomingPlayer),
			Item:     core.Item(currIncomingItem),
		}
	}

	binary.BigEndian.PutUint16(buf[2:4], grant.World)
	binary.BigEndian.PutUint16(buf[4:6], uint16(grant.Item))

	if err := b.ma.WriteMemory(ctx, base+playerIDOffset, buf); err != nil {
		return fmt.Errorf("failed to write item: %w", err)
	}
	return nil
}

// writeName encodes a player name into the OoT charset and writes its
// 8-byte slot in the game's player name table. Per-name writes keep the
// UDP datagrams small; the full table is never sent in one command.
func (b *Bridge) writeName(ctx context.Context, base uint32, name core.PlayerName) error {
	encoded := randomizer.EncodeOoTName(name.Name)
	addr := base + playerNamesOffset + uint32(name.PlayerID)*8
	if err := b.ma.WriteMemory(ctx, addr, encoded[:]); err != nil {
		return fmt.Errorf("failed to write player name: %w", err)
	}
	return nil
}

// emit performs the mandated non-blocking send on the events channel. It
// reports whether the event was accepted.
func (b *Bridge) emit(ev core.GameEvent) bool {
	select {
	case b.events <- ev:
		return true
	default:
		return false
	}
}
