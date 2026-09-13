// Package retroarch implements the core.MemoryAccessor and core.MemoryBridge
// contracts against RetroArch's network command interface (UDP, default
// port 55355), running an Ocarina of Time multiworld ROM.
package retroarch

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// DefaultAddr is RetroArch's default network command interface address.
const DefaultAddr = "127.0.0.1:55355"

// ioTimeout bounds every single request/response round trip with the
// emulator. At the mandated 30fps poll rate, one frame is 1/30s; a command
// that takes longer than a frame is treated as failed so the bridge loop
// is never stalled by a hung emulator.
const ioTimeout = time.Second / 30

// Accessor implements core.MemoryAccessor over RetroArch's UDP network
// command interface. It is safe for concurrent use; the transport
// serializes all I/O internally.
type Accessor struct {
	addr string

	mu        sync.Mutex
	conn      *net.UDPConn
	ioBuffer  []byte
	connected bool
}

// NewAccessor returns an Accessor targeting addr. If addr is empty,
// DefaultAddr is used. Connect must be called before ReadMemory/WriteMemory.
func NewAccessor(addr string) *Accessor {
	if addr == "" {
		addr = DefaultAddr
	}
	return &Accessor{addr: addr}
}

// Connect opens the UDP session. It is idempotent.
func (a *Accessor) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.connected {
		return nil
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "udp", a.addr)
	if err != nil {
		return fmt.Errorf("failed to dial RetroArch at %s: %w", a.addr, err)
	}
	a.conn = conn.(*net.UDPConn)
	a.ioBuffer = make([]byte, 65535)
	a.connected = true

	return nil
}

// Close terminates the UDP session. It is idempotent.
func (a *Accessor) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.connected = false
	if a.conn != nil {
		err := a.conn.Close()
		a.conn = nil
		return err
	}
	return nil
}

// toPhysical translates an N64 virtual address to a physical address.
func toPhysical(vAddr uint32) uint32 {
	return vAddr & 0x00FFFFFF
}

// ReadMemory implements core.MemoryAccessor. size must be a multiple of 4.
func (a *Accessor) ReadMemory(ctx context.Context, vAddr uint32, size uint32) ([]byte, error) {
	if size%4 != 0 {
		return nil, fmt.Errorf("read size must be a multiple of 4, got %d", size)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.connected {
		return nil, fmt.Errorf("not connected")
	}

	phys := toPhysical(vAddr)
	cmd := fmt.Sprintf("READ_CORE_MEMORY %x %d\n", phys, size)

	a.drainLocked()

	if _, err := a.conn.Write([]byte(cmd)); err != nil {
		return nil, fmt.Errorf("failed to write command: %w", err)
	}

	a.setDeadlineLocked(ctx)

	n, err := a.conn.Read(a.ioBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	response := a.ioBuffer[:n]

	// Parse the RetroArch ASCII format:
	//   READ_CORE_MEMORY <addr> <hex byte> <hex byte> ...
	parts := bytes.Fields(bytes.TrimSpace(response))
	if len(parts) < 3 || !bytes.Equal(parts[0], []byte("READ_CORE_MEMORY")) {
		return nil, fmt.Errorf("invalid response format. Received: %s", response)
	}

	if bytes.Equal(parts[2], []byte("-1")) {
		return nil, fmt.Errorf("RetroArch returned -1: invalid physical address %x", phys)
	}

	hexStrings := parts[2:]
	if uint32(len(hexStrings)) < size {
		return nil, fmt.Errorf("expected %d bytes, got %d. Response: %s", size, len(hexStrings), response)
	}

	out := make([]byte, size)
	for i := range size {
		val, err := decodeHexByte(hexStrings[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse byte at index %d (%s): %w", i, hexStrings[i], err)
		}
		out[i] = val
	}

	return unSwapBytes(out), nil
}

// WriteMemory implements core.MemoryAccessor. len(data) must be a multiple
// of 4. The caller's slice is not modified.
func (a *Accessor) WriteMemory(ctx context.Context, vAddr uint32, data []byte) error {
	if len(data)%4 != 0 {
		return fmt.Errorf("write size must be a multiple of 4, got %d", len(data))
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.connected {
		return fmt.Errorf("not connected")
	}

	// Copy before swapping so we never mutate the caller's slice.
	swapped := unSwapBytes(bytes.Clone(data))

	a.drainLocked()

	cmdPrefix := fmt.Sprintf("WRITE_CORE_MEMORY %08x ", toPhysical(vAddr))
	idx := copy(a.ioBuffer, cmdPrefix)

	for i, b := range swapped {
		if i > 0 {
			a.ioBuffer[idx] = ' ' // Space required between bytes
			idx++
		}
		a.ioBuffer[idx] = hexChar(b >> 4)     // High nibble
		a.ioBuffer[idx+1] = hexChar(b & 0x0F) // Low nibble
		idx += 2
	}
	a.ioBuffer[idx] = '\n'
	idx++

	if _, err := a.conn.Write(a.ioBuffer[:idx]); err != nil {
		return fmt.Errorf("failed to write memory: %w", err)
	}

	a.setDeadlineLocked(ctx)

	if _, err := a.conn.Read(a.ioBuffer); err != nil {
		return fmt.Errorf("failed to read write confirmation: %w", err)
	}

	return nil
}

// drainLocked discards any stale datagrams sitting in the socket buffer so
// the next read corresponds to the next command. Must hold a.mu.
func (a *Accessor) drainLocked() {
	a.conn.SetReadDeadline(time.Now())
	for {
		if _, err := a.conn.Read(a.ioBuffer); err != nil {
			break
		}
	}
}

// setDeadlineLocked bounds the next read to ioTimeout, or the context's
// deadline if it is sooner. Must hold a.mu.
func (a *Accessor) setDeadlineLocked(ctx context.Context) {
	deadline := time.Now().Add(ioTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	a.conn.SetReadDeadline(deadline)
}

// ------------------ Helper Functions ------------------

// unSwapBytes reverses the byte order of a slice in 4-byte chunks, in place.
// Mupen stores 32-bit words backwards on x86, so every word crossing the
// wire (in either direction) needs its outer and inner bytes swapped.
func unSwapBytes(data []byte) []byte {
	for i := 0; i+3 < len(data); i += 4 {
		data[i], data[i+1], data[i+2], data[i+3] = data[i+3], data[i+2], data[i+1], data[i]
	}
	return data
}

func decodeHexByte(b []byte) (byte, error) {
	if len(b) != 2 {
		return 0, fmt.Errorf("expected 2 bytes, got %d", len(b))
	}

	var val byte
	for i := range 2 {
		val <<= 4
		switch {
		case b[i] >= '0' && b[i] <= '9':
			val |= b[i] - '0'
		case b[i] >= 'a' && b[i] <= 'f':
			val |= b[i] - 'a' + 10
		case b[i] >= 'A' && b[i] <= 'F':
			val |= b[i] - 'A' + 10
		default:
			return 0, fmt.Errorf("invalid hex char: %c", b[i])
		}
	}
	return val, nil
}

func hexChar(nibble byte) byte {
	if nibble <= 9 {
		return nibble + '0'
	}
	return nibble - 10 + 'A'
}
