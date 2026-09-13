package retroarch

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRetroArch is a fake RetroArch network command interface. It stores
// the exact wire bytes it receives (already word-swapped by the Accessor)
// in a sparse physical memory map.
type fakeRetroArch struct {
	conn *net.UDPConn

	mu  sync.Mutex
	mem map[uint32]byte

	// readResponse, when non-nil, overrides the read handler's response.
	// Guarded by mu; set via setReadResponse.
	readResponse func(phys uint32, size uint32) string
}

// setReadResponse installs a read handler override, safe to call while the
// server is running.
func (f *fakeRetroArch) setReadResponse(fn func(phys uint32, size uint32) string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readResponse = fn
}

func startFakeRetroArch(t *testing.T) *fakeRetroArch {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("failed to start fake server: %v", err)
	}

	f := &fakeRetroArch{conn: conn, mem: make(map[uint32]byte)}
	go f.serve()
	t.Cleanup(func() { f.conn.Close() })
	return f
}

func (f *fakeRetroArch) addr() string {
	return f.conn.LocalAddr().String()
}

func (f *fakeRetroArch) serve() {
	buf := make([]byte, 65535)
	for {
		n, remote, err := f.conn.ReadFromUDP(buf)
		if err != nil {
			return // closed
		}
		f.handle(remote, strings.TrimSpace(string(buf[:n])))
	}
}

func (f *fakeRetroArch) handle(remote *net.UDPAddr, cmd string) {
	fields := strings.Fields(cmd)
	if len(fields) < 2 {
		return
	}

	var phys uint32
	fmt.Sscanf(fields[1], "%x", &phys)

	switch fields[0] {
	case "READ_CORE_MEMORY":
		var size uint32
		fmt.Sscanf(fields[2], "%d", &size)

		f.mu.Lock()
		override := f.readResponse
		var resp string
		if override != nil {
			resp = override(phys, size)
		} else {
			parts := make([]string, size)
			for i := range size {
				parts[i] = fmt.Sprintf("%02x", f.mem[phys+i])
			}
			resp = fmt.Sprintf("READ_CORE_MEMORY %x %s", phys, strings.Join(parts, " "))
		}
		f.mu.Unlock()
		f.conn.WriteToUDP([]byte(resp), remote)

	case "WRITE_CORE_MEMORY":
		f.mu.Lock()
		for i, hexByte := range fields[2:] {
			var b byte
			fmt.Sscanf(hexByte, "%02x", &b)
			f.mem[phys+uint32(i)] = b
		}
		f.mu.Unlock()
		f.conn.WriteToUDP([]byte(fmt.Sprintf("WRITE_CORE_MEMORY %x", phys)), remote)
	}
}

// physicalByte returns the raw wire byte stored at a physical address.
func (f *fakeRetroArch) physicalByte(phys uint32) byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mem[phys]
}

func newConnectedAccessor(t *testing.T, addr string) *Accessor {
	t.Helper()
	a := NewAccessor(addr)
	if err := a.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

func TestReadWriteRoundTrip(t *testing.T) {
	server := startFakeRetroArch(t)
	a := newConnectedAccessor(t, server.addr())
	ctx := context.Background()

	// 16 bytes of test data at the coop context address.
	want := []byte{
		0x01, 0x02, 0x03, 0x04,
		0xDE, 0xAD, 0xBE, 0xEF,
		0x00, 0x00, 0x00, 0x2A,
		0xFF, 0xFF, 0xFF, 0xFF,
	}

	if err := a.WriteMemory(ctx, 0x80400000, want); err != nil {
		t.Fatalf("WriteMemory failed: %v", err)
	}

	got, err := a.ReadMemory(ctx, 0x80400000, uint32(len(want)))
	if err != nil {
		t.Fatalf("ReadMemory failed: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("round trip mismatch:\n got: %x\nwant: %x", got, want)
	}
}

func TestPhysicalAddressTranslation(t *testing.T) {
	server := startFakeRetroArch(t)
	a := newConnectedAccessor(t, server.addr())
	ctx := context.Background()

	// Virtual address 0x80400000 must map to physical 0x00400000.
	if err := a.WriteMemory(ctx, 0x80400000, []byte{0x11, 0x22, 0x33, 0x44}); err != nil {
		t.Fatalf("WriteMemory failed: %v", err)
	}

	// The Mupen word swap means the wire (and thus physical memory) holds
	// the word reversed.
	wantWire := []byte{0x44, 0x33, 0x22, 0x11}
	for i, want := range wantWire {
		if got := server.physicalByte(0x00400000 + uint32(i)); got != want {
			t.Errorf("physical byte %d: got %02x, want %02x", i, got, want)
		}
	}
}

func TestReadMemoryRejectsUnalignedSize(t *testing.T) {
	server := startFakeRetroArch(t)
	a := newConnectedAccessor(t, server.addr())

	if _, err := a.ReadMemory(context.Background(), 0x80400000, 3); err == nil {
		t.Error("expected error for non-multiple-of-4 read size, got nil")
	}
}

func TestWriteMemoryRejectsUnalignedSize(t *testing.T) {
	server := startFakeRetroArch(t)
	a := newConnectedAccessor(t, server.addr())

	if err := a.WriteMemory(context.Background(), 0x80400000, []byte{1, 2, 3}); err == nil {
		t.Error("expected error for non-multiple-of-4 write size, got nil")
	}
}

func TestWriteMemoryDoesNotMutateCallerSlice(t *testing.T) {
	server := startFakeRetroArch(t)
	a := newConnectedAccessor(t, server.addr())

	data := []byte{0x01, 0x02, 0x03, 0x04}
	if err := a.WriteMemory(context.Background(), 0x80400000, data); err != nil {
		t.Fatalf("WriteMemory failed: %v", err)
	}

	want := []byte{0x01, 0x02, 0x03, 0x04}
	if string(data) != string(want) {
		t.Errorf("caller slice mutated: got %x, want %x", data, want)
	}
}

func TestReadMemoryInvalidResponse(t *testing.T) {
	server := startFakeRetroArch(t)
	server.setReadResponse(func(phys uint32, size uint32) string {
		return "GARBAGE RESPONSE"
	})
	a := newConnectedAccessor(t, server.addr())

	if _, err := a.ReadMemory(context.Background(), 0x80400000, 4); err == nil {
		t.Error("expected error for malformed response, got nil")
	}
}

func TestReadMemoryMinusOneResponse(t *testing.T) {
	server := startFakeRetroArch(t)
	server.setReadResponse(func(phys uint32, size uint32) string {
		return fmt.Sprintf("READ_CORE_MEMORY %x -1", phys)
	})
	a := newConnectedAccessor(t, server.addr())

	if _, err := a.ReadMemory(context.Background(), 0x80400000, 4); err == nil {
		t.Error("expected error for -1 response, got nil")
	}
}

func TestReadMemoryTimeout(t *testing.T) {
	server := startFakeRetroArch(t)
	server.setReadResponse(func(phys uint32, size uint32) string {
		return "" // empty datagram: valid UDP, no usable content
	})
	a := newConnectedAccessor(t, server.addr())

	// A dead emulator must not stall the caller beyond the frame budget.
	// Use a context deadline of 50ms to prove ctx deadlines are honored.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := a.ReadMemory(ctx, 0x80400000, 4)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected error for empty response, got nil")
	}
	if elapsed > time.Second {
		t.Errorf("read took %v, should fail within a frame budget", elapsed)
	}
}

func TestOperationsBeforeConnect(t *testing.T) {
	a := NewAccessor("127.0.0.1:1")

	if _, err := a.ReadMemory(context.Background(), 0x80400000, 4); err == nil {
		t.Error("expected error reading before Connect, got nil")
	}
	if err := a.WriteMemory(context.Background(), 0x80400000, []byte{1, 2, 3, 4}); err == nil {
		t.Error("expected error writing before Connect, got nil")
	}
}

func TestConnectIdempotent(t *testing.T) {
	server := startFakeRetroArch(t)
	a := NewAccessor(server.addr())
	defer a.Close()

	if err := a.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect failed: %v", err)
	}
	if err := a.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect failed: %v", err)
	}
}
