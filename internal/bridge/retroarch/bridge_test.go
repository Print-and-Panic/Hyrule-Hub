package retroarch

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/core"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

// testCoopContextBase is the address the fake ROM "allocates" for the coop
// context. The bridge must read it from coopContextPointerAddress before
// touching any of the offsets.
const testCoopContextBase = 0x80401000

// fakeMemory is a sparse in-memory core.MemoryAccessor. Addresses are far
// too high for a slice, so bytes are kept in a map.
type fakeMemory struct {
	mu  sync.Mutex
	mem map[uint32]byte
}

// newInitializedMemory returns fake memory with the coop context pointer
// already populated, as a booted multiworld ROM would leave it.
func newInitializedMemory() *fakeMemory {
	f := &fakeMemory{mem: make(map[uint32]byte)}
	f.writeUint32(coopContextPointerAddress, testCoopContextBase)
	return f
}

func (f *fakeMemory) ReadMemory(_ context.Context, vAddr uint32, size uint32) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]byte, size)
	for i := range size {
		out[i] = f.mem[vAddr+i]
	}
	return out, nil
}

func (f *fakeMemory) WriteMemory(_ context.Context, vAddr uint32, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i, b := range data {
		f.mem[vAddr+uint32(i)] = b
	}
	return nil
}

func (f *fakeMemory) readUint64(vAddr uint32) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf [8]byte
	for i := range buf {
		buf[i] = f.mem[vAddr+uint32(i)]
	}
	return binary.BigEndian.Uint64(buf[:])
}

func (f *fakeMemory) readUint16(vAddr uint32) uint16 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return binary.BigEndian.Uint16([]byte{f.mem[vAddr], f.mem[vAddr+1]})
}

func (f *fakeMemory) writeUint64(vAddr uint32, v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	for i, b := range buf {
		f.mem[vAddr+uint32(i)] = b
	}
}

func (f *fakeMemory) writeUint32(vAddr uint32, v uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	for i, b := range buf {
		f.mem[vAddr+uint32(i)] = b
	}
}

func (f *fakeMemory) writeUint16(vAddr uint32, v uint16) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mem[vAddr] = byte(v >> 8)
	f.mem[vAddr+1] = byte(v)
}

// waitFor polls cond until it holds or the timeout elapses.
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

// startBridge launches Start in a goroutine and returns a cancel func and a
// channel that receives Start's return value.
func startBridge(b *Bridge) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Start(ctx) }()
	return cancel, done
}

// assertStopped verifies Start returned ctx.Canceled promptly.
func assertStopped(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Start returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation (goroutine leak)")
	}
}

func TestBridgeEmitsLocationCheckAndClearsSemaphore(t *testing.T) {
	mem := newInitializedMemory()
	// Game has an outgoing item: key 42, item 0x0029, world 3.
	mem.writeUint64(testCoopContextBase+outgoingKeyOffset, 42)
	mem.writeUint16(testCoopContextBase+outgoingItemOffset, 0x0029)
	mem.writeUint16(testCoopContextBase+outgoingItemOffset+2, 3)

	events := make(chan core.GameEvent, 4)
	bridge := NewBridge(mem, events, nil, nil)
	cancel, done := startBridge(bridge)
	defer cancel()

	select {
	case ev := <-events:
		if ev.Kind != core.EventKindLocationCheck {
			t.Fatalf("event kind = %v, want EventKindLocationCheck", ev.Kind)
		}
		check, ok := ev.Payload.(core.LocationCheck)
		if !ok {
			t.Fatalf("payload type = %T, want core.LocationCheck", ev.Payload)
		}
		if check.OutgoingKey != 42 || check.Item != 0x0029 || check.World != 3 {
			t.Errorf("unexpected payload: %s", check)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no LocationCheck event emitted")
	}

	// The bridge must release the game's semaphore after emitting.
	waitFor(t, 2*time.Second, "outgoing key to be cleared", func() bool {
		return mem.readUint64(testCoopContextBase+outgoingKeyOffset) == 0
	})
	if got := mem.readUint16(testCoopContextBase + outgoingItemOffset); got != 0 {
		t.Errorf("outgoing item not cleared, got %04x", got)
	}

	assertStopped(t, cancel, done)
}

func TestBridgeDoesNotClearSemaphoreWhenEventDropped(t *testing.T) {
	mem := newInitializedMemory()
	mem.writeUint64(testCoopContextBase+outgoingKeyOffset, 42)
	mem.writeUint16(testCoopContextBase+outgoingItemOffset, 0x0029)
	mem.writeUint16(testCoopContextBase+outgoingItemOffset+2, 3)

	// Unbuffered channel with no receiver: the non-blocking send always
	// fails, so the semaphore must stay set (no lost items).
	events := make(chan core.GameEvent)
	bridge := NewBridge(mem, events, nil, nil)
	cancel, done := startBridge(bridge)

	// Let several ticks elapse.
	time.Sleep(200 * time.Millisecond)

	if got := mem.readUint64(testCoopContextBase + outgoingKeyOffset); got != 42 {
		t.Errorf("semaphore cleared despite dropped event: key = %d, want 42", got)
	}

	assertStopped(t, cancel, done)
}

func TestBridgeAppliesIncomingGrant(t *testing.T) {
	mem := newInitializedMemory()

	events := make(chan core.GameEvent, 4)
	grants := make(chan core.ItemGrant, 1)
	bridge := NewBridge(mem, events, grants, nil)
	cancel, done := startBridge(bridge)
	defer cancel()

	grants <- core.ItemGrant{World: 7, Item: 0x0051, Key: 9001}

	waitFor(t, 2*time.Second, "grant to be written to incoming block", func() bool {
		return mem.readUint16(testCoopContextBase+playerIDOffset+2) == 7 &&
			mem.readUint16(testCoopContextBase+playerIDOffset+4) == 0x0051
	})

	assertStopped(t, cancel, done)
}

func TestBridgeRetriesBlockedGrant(t *testing.T) {
	mem := newInitializedMemory()
	// Game's incoming semaphore is occupied by a previous grant.
	mem.writeUint16(testCoopContextBase+playerIDOffset+2, 1)
	mem.writeUint16(testCoopContextBase+playerIDOffset+4, 0x0010)

	events := make(chan core.GameEvent, 4)
	grants := make(chan core.ItemGrant, 1)
	bridge := NewBridge(mem, events, grants, nil)
	cancel, done := startBridge(bridge)
	defer cancel()

	grants <- core.ItemGrant{World: 7, Item: 0x0051, Key: 9001}

	// While blocked, the new grant must NOT overwrite the pending one.
	time.Sleep(200 * time.Millisecond)
	if got := mem.readUint16(testCoopContextBase + playerIDOffset + 4); got != 0x0010 {
		t.Fatalf("blocked grant overwrote semaphore: item = %04x, want 0010", got)
	}

	// The game consumes the pending item (zeroes the semaphore)...
	mem.writeUint16(testCoopContextBase+playerIDOffset+2, 0)
	mem.writeUint16(testCoopContextBase+playerIDOffset+4, 0)

	// ...and the bridge must retry and land the grant.
	waitFor(t, 2*time.Second, "blocked grant to be retried", func() bool {
		return mem.readUint16(testCoopContextBase+playerIDOffset+2) == 7 &&
			mem.readUint16(testCoopContextBase+playerIDOffset+4) == 0x0051
	})

	assertStopped(t, cancel, done)
}

func TestBridgeWritesPlayerName(t *testing.T) {
	mem := newInitializedMemory()

	events := make(chan core.GameEvent, 4)
	names := make(chan core.PlayerName, 1)
	bridge := NewBridge(mem, events, nil, names)
	cancel, done := startBridge(bridge)
	defer cancel()

	names <- core.PlayerName{PlayerID: 2, Name: "LINK"}

	want := randomizer.EncodeOoTName("LINK")
	slot := uint32(testCoopContextBase + playerNamesOffset + 2*8)
	waitFor(t, 2*time.Second, "player name to be written", func() bool {
		memBytes, _ := mem.ReadMemory(context.Background(), slot, 8)
		for i := range want {
			if memBytes[i] != want[i] {
				return false
			}
		}
		return true
	})

	assertStopped(t, cancel, done)
}

func TestBridgeBuffersNamesUntilBaseResolves(t *testing.T) {
	// Pointer is zero: the ROM has not initialized the coop context yet.
	mem := &fakeMemory{mem: make(map[uint32]byte)}

	events := make(chan core.GameEvent, 4)
	names := make(chan core.PlayerName, 1)
	bridge := NewBridge(mem, events, nil, names)
	cancel, done := startBridge(bridge)
	defer cancel()

	names <- core.PlayerName{PlayerID: 1, Name: "ZELDA"}

	// The name must not be written while the pointer is invalid; an error
	// event should report the unresolved context instead.
	select {
	case ev := <-events:
		if ev.Kind != core.EventKindError {
			t.Fatalf("event kind = %v, want EventKindError", ev.Kind)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no error event for unresolved coop context")
	}

	// ROM finishes booting: the pointer becomes valid.
	mem.writeUint32(coopContextPointerAddress, testCoopContextBase)

	want := randomizer.EncodeOoTName("ZELDA")
	slot := uint32(testCoopContextBase + playerNamesOffset + 1*8)
	waitFor(t, 2*time.Second, "buffered name to be written", func() bool {
		memBytes, _ := mem.ReadMemory(context.Background(), slot, 8)
		for i := range want {
			if memBytes[i] != want[i] {
				return false
			}
		}
		return true
	})

	assertStopped(t, cancel, done)
}

func TestBridgeClosedGrantsChannelDoesNotHotLoop(t *testing.T) {
	mem := newInitializedMemory()

	events := make(chan core.GameEvent, 4)
	grants := make(chan core.ItemGrant)
	close(grants) // Hub closed the channel before we started.

	bridge := NewBridge(mem, events, grants, nil)
	cancel, done := startBridge(bridge)

	// A closed grants channel is always ready to receive; if the loop did
	// not disable the case it would spin forever and never shut down
	// cleanly. Surviving 200ms and stopping promptly proves it.
	time.Sleep(200 * time.Millisecond)
	assertStopped(t, cancel, done)
}

func TestBridgeStopsImmediatelyWhenAlreadyCancelled(t *testing.T) {
	mem := newInitializedMemory()
	events := make(chan core.GameEvent, 4)
	bridge := NewBridge(mem, events, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- bridge.Start(ctx) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Start returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return on pre-cancelled context")
	}
}
