package retroarch

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type RetroArch struct {
	connectionLock sync.Mutex
	isConnected    bool
	conn           *net.UDPConn
	ioBuffer       []byte
}

func (r *RetroArch) Connect(ctx context.Context) error {
	if r.isConnected {
		return nil
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "udp", "127.0.0.1:55355")
	if err != nil {
		return err
	}
	r.conn = conn.(*net.UDPConn)

	r.ioBuffer = make([]byte, 65535)

	return nil
}

func (r *RetroArch) Disconnect() error {
	if r.conn != nil {
		r.conn.Close()
	}
	r.isConnected = false
	return nil
}

func (r *RetroArch) ToPhysical(vAddr uint32) uint32 {
	return vAddr & 0x00FFFFFF
}

func (r *RetroArch) ReadMemory(ctx context.Context, vAddr uint32, size uint32) ([]byte, error) {
	if size%4 != 0 {
		return nil, fmt.Errorf("read size must be a multiple of 4, got %d", size)
	}

	phys := r.ToPhysical(vAddr)
	cmd := fmt.Sprintf("READ_CORE_MEMORY %x %d\n", phys, size)

	r.connectionLock.Lock()
	defer r.connectionLock.Unlock()

	// Clear any pending data in the UDP buffer before sending a new command
	r.conn.SetReadDeadline(time.Now())
	for {
		_, err := r.conn.Read(r.ioBuffer)
		if err != nil {
			break
		}
	}

	// Send the command
	if _, err := r.conn.Write([]byte(cmd)); err != nil {
		return nil, fmt.Errorf("failed to write command: %w", err)
	}

	// Trick UDP conn into using a context
	deadline := time.Now().Add(time.Second / 30)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	r.conn.SetReadDeadline(deadline)

	// Read the entire UDP response packet
	n, err := r.conn.Read(r.ioBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	response := r.ioBuffer[:n]

	// Parse the RetroArch ASCII format
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

	// Convert hex strings to a raw byte slice
	out := make([]byte, size)
	for i := range size {
		val, err := decodeHexByte(hexStrings[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse byte at index %d (%s): %w", i, hexStrings[i], err)
		}
		out[i] = byte(val)
	}

	out = unSwapBytes(out)

	return out, nil
}

func (r *RetroArch) WriteMemory(ctx context.Context, vAddr uint32, data []byte) error {
	r.connectionLock.Lock()
	defer r.connectionLock.Unlock()

	data = unSwapBytes(data)

	// Discard stale data
	r.conn.SetReadDeadline(time.Now())
	for {
		if _, err := r.conn.Read(r.ioBuffer); err != nil {
			break
		}
	}

	cmdPrefix := fmt.Sprintf("WRITE_CORE_MEMORY %08x ", r.ToPhysical(vAddr))
	idx := copy(r.ioBuffer, cmdPrefix)

	for i, b := range data {
		if i > 0 {
			r.ioBuffer[idx] = ' ' // Space required between bytes
			idx++
		}
		r.ioBuffer[idx] = hexChar(b >> 4)     // High nibble
		r.ioBuffer[idx+1] = hexChar(b & 0x0F) // Low nibble
		idx += 2
	}
	r.ioBuffer[idx] = '\n'
	idx++

	if _, err := r.conn.Write(r.ioBuffer[:idx]); err != nil {
		return fmt.Errorf("failed to write memory: %w", err)
	}

	deadline := time.Now().Add(time.Second / 30)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	r.conn.SetReadDeadline(deadline)

	if _, err := r.conn.Read(r.ioBuffer); err != nil {
		return fmt.Errorf("failed to read write confirmation: %w", err)
	}

	return nil
}

// ------------------ Helper Functions ------------------

// unSwapBytes reverses the byte order of a slice
func unSwapBytes(data []byte) []byte {
	// Because Mupen stores 32-bit words backwards on x86, we iterate through
	// our byte slice in chunks of 4 and swap the outer and inner bytes.
	for i := 0; i < len(data); i += 4 {
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
