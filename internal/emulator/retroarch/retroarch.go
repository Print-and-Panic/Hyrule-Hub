package retroarch

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

const (
	coopContextAddress = 0x8040_0000
	coopContextSize    = 0xc24
)

type RetroArch struct {
	connectionLock    sync.Mutex
	transactionLock   sync.Mutex
	isConnected       bool
	conn              *net.UDPConn
	coopContextOffset uint32
}

func (r *RetroArch) Connect() error {
	if r.isConnected {
		return nil
	}

	serverAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:55355")
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return err
	}

	r.conn = conn

	c, err := r.ReadMemory(coopContextAddress, 4)
	if err != nil {
		return fmt.Errorf("failed to read coop context start: %w", err)
	}

	// Heuristic: N64 pointers usually start with 0x80.
	// We try reading it directly first, then try unswapping.
	ptrDirect := binary.BigEndian.Uint32(c)
	ptrSwapped := binary.BigEndian.Uint32(unSwapBytes(append([]byte{}, c...))) // Don't modify c yet

	if (ptrDirect & 0xFF000000) == 0x80000000 {
		r.coopContextOffset = ptrDirect
		log.Printf("Co-op context pointer read directly: %x\n", r.coopContextOffset)
	} else if (ptrSwapped & 0xFF000000) == 0x80000000 {
		r.coopContextOffset = ptrSwapped
		log.Printf("Co-op context pointer read after unswap: %x\n", r.coopContextOffset)
	} else {
		// Fallback to swapped if neither looks like a valid pointer, but log it.
		r.coopContextOffset = ptrSwapped
		log.Printf("Warning: Co-op context pointer doesn't look valid: direct=%x, swapped=%x. Using swapped.\n", ptrDirect, ptrSwapped)
	}

	if r.coopContextOffset == 0 {
		return fmt.Errorf("co-op context pointer is null (is the randomizer context initialized?)")
	}

	r.isConnected = true
	return nil
}

func (r *RetroArch) Disconnect() error {

	if r.conn != nil {
		r.conn.Close()
	}
	r.isConnected = false
	return nil
}

func (r *RetroArch) ReadCoopContext() (*randomizer.CoopContext, error) {
	bytes, err := r.ReadMemory(r.coopContextOffset, coopContextSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read coop context: %w", err)
	}

	bytes = unSwapBytes(bytes)
	version := binary.BigEndian.Uint32(bytes[VersionOffset : VersionOffset+4])
	log.Printf("Co-op Context Version: %d\n", version)

	ctx := &randomizer.CoopContext{
		Version:                  version,
		PlayerID:                 bytes[PlayerIDOffset],
		PlayerNameID:             bytes[PlayerNameIDOffset],
		IncomingPlayer:           binary.BigEndian.Uint16(bytes[IncomingPlayerOffset : IncomingPlayerOffset+2]),
		IncomingItem:             randomizer.OotItem(binary.BigEndian.Uint16(bytes[IncomingItemOffset : IncomingItemOffset+2])),
		MWSendOwnItems:           bytes[MWSendOwnItemsOffset],
		MWProgressiveItemsEnable: bytes[MWProgressiveItemsEnableOffset],
		OutgoingItem:             randomizer.OotItem(binary.BigEndian.Uint16(bytes[OutgoingItemOffset : OutgoingItemOffset+2])),
		OutgoingPlayer:           binary.BigEndian.Uint16(bytes[OutgoingPlayerOffset : OutgoingPlayerOffset+2]),

		CFGFileSelectHash: randomizer.HashIconFromByteArray(bytes[CfgFileSelectHashOffset : CfgFileSelectHashOffset+5]),
		OutgoingKey:       binary.BigEndian.Uint64(bytes[OutgoingKeyOffset : OutgoingKeyOffset+8]),
	}

	for i := range ctx.PlayerNames {
		nameBytes := bytes[PlayerNamesOffset+i*8 : PlayerNamesOffset+i*8+8]
		ctx.PlayerNames[i] = string(nameBytes)
	}

	return ctx, nil
}

func (r *RetroArch) ProcessOutgoingItem(publishFunc func(randomizer.OutgoingItem) error) error {
	r.transactionLock.Lock()
	defer r.transactionLock.Unlock()

	bytes, err := r.ReadMemory(r.coopContextOffset+OutgoingItemOffset, 4)
	if err != nil {
		return fmt.Errorf("failed to read outgoing item: %w", err)
	}

	bytes = unSwapBytes(bytes)

	out := randomizer.OutgoingItem{
		World: binary.BigEndian.Uint16(bytes[2:4]),
		Item:  randomizer.OotItem(binary.BigEndian.Uint16(bytes[0:2])),
	}

	outgoingKeyBytes, err := r.ReadMemory(r.coopContextOffset+OutgoingKeyOffset, 8)
	if err != nil {
		return fmt.Errorf("failed to read outgoing key: %w", err)
	}

	outgoingKeyBytes = unSwapBytes(outgoingKeyBytes)
	out.OutgoingKey = binary.BigEndian.Uint64(outgoingKeyBytes)

	if out.OutgoingKey == 0 {
		return nil
	}

	log.Printf("Found outgoing item: %s to player %d (Key: %d)\n", out.Item, out.World, out.OutgoingKey)

	// Release the lock during the network operation!
	r.transactionLock.Unlock()
	err = publishFunc(out)
	r.transactionLock.Lock()

	if err != nil {
		return fmt.Errorf("network publish failed: %w", err)
	}

	log.Printf("Successfully published item %d, clearing from RAM...\n", out.OutgoingKey)
	if err := r.ClearOutgoingItem(); err != nil {
		return fmt.Errorf("published successfully, but failed to clear RAM: %w", err)
	}

	return nil
}

func (r *RetroArch) WriteIncomingItem(item randomizer.IncomingItem) error {
	waitTime := time.Second / 30 // 30 FPS polling
	timeout := time.After(5 * time.Second)

	var loadBytes []byte

SemaphoreCheck:
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for semaphore to clear (last seen: %x)", loadBytes)
		default:
			r.transactionLock.Lock()

			bytes, err := r.ReadMemory(r.coopContextOffset+PlayerIDOffset, 8)
			if err != nil {
				r.transactionLock.Unlock()
				return fmt.Errorf("failed to read semaphore block: %w", err)
			}

			loadBytes = unSwapBytes(bytes)

			currIncomingPlayer := binary.BigEndian.Uint16(loadBytes[2:4])
			currIncomingItem := binary.BigEndian.Uint16(loadBytes[4:6])

			if currIncomingPlayer == 0 && currIncomingItem == 0 {
				// Semaphore is empty! We KEEP the lock here so no other thread
				// can claim it before we inject our item, and break the loop.
				break SemaphoreCheck
			}

			// Semaphore is full. Unlock so other processes can run, then wait.
			r.transactionLock.Unlock()
			log.Printf("Semaphore is full (P:%d I:%d), waiting...\n", currIncomingPlayer, currIncomingItem)
			time.Sleep(waitTime)
		}
	}

	// AT THIS POINT: We still hold the transactionLock from the successful iteration.
	defer r.transactionLock.Unlock()

	binary.BigEndian.PutUint16(loadBytes[2:4], item.World)
	binary.BigEndian.PutUint16(loadBytes[4:6], uint16(item.Item))
	writeBytes := unSwapBytes(loadBytes)

	log.Printf("Injecting item %s from world %d\n", item.Item, item.World)
	if err := r.WriteMemory(r.coopContextOffset+PlayerIDOffset, writeBytes); err != nil {
		return fmt.Errorf("failed to write incoming item: %w", err)
	}

	return nil
}

func (r *RetroArch) ClearOutgoingItem() error {
	bytes := make([]byte, 4)

	writeBytes := unSwapBytes(bytes)

	if err := r.WriteMemory(r.coopContextOffset+OutgoingItemOffset, writeBytes); err != nil {
		return fmt.Errorf("failed to clear outgoing item: %w", err)
	}

	outgoingKeyBytes := make([]byte, 8)
	writeBytes = unSwapBytes(outgoingKeyBytes)

	if err := r.WriteMemory(r.coopContextOffset+OutgoingKeyOffset, writeBytes); err != nil {
		return fmt.Errorf("failed to clear outgoing key: %w", err)
	}

	return nil
}

func (r *RetroArch) ToPhysical(vAddr uint32) uint32 {
	return vAddr & 0x00FFFFFF
}

func (r *RetroArch) ReadMemory(vAddr uint32, size uint32) ([]byte, error) {
	if size%4 != 0 {
		return nil, fmt.Errorf("read size must be a multiple of 4, got %d", size)
	}

	phys := r.ToPhysical(vAddr)
	cmd := fmt.Sprintf("READ_CORE_MEMORY %x %d\n", phys, size)

	r.connectionLock.Lock()
	defer r.connectionLock.Unlock()

	// Clear any pending data in the UDP buffer before sending a new command
	r.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	discardBuf := make([]byte, 65535)
	for {
		_, err := r.conn.Read(discardBuf)
		if err != nil {
			break
		}
	}

	if _, err := r.conn.Write([]byte(cmd)); err != nil {
		return nil, fmt.Errorf("failed to write command: %w", err)
	}

	err := r.conn.SetReadDeadline(time.Now().Add(1000 * time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Read the entire UDP response packet
	respBuf := make([]byte, 65535)
	n, err := r.conn.Read(respBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	response := string(respBuf[:n])

	// Parse the RetroArch ASCII format
	parts := strings.Fields(strings.TrimSpace(response))
	if len(parts) < 3 || parts[0] != "READ_CORE_MEMORY" {
		return nil, fmt.Errorf("invalid response format. Received: %s", response)
	}

	if parts[2] == "-1" {
		return nil, fmt.Errorf("RetroArch returned -1: invalid physical address %x", phys)
	}

	hexStrings := parts[2:]
	if uint32(len(hexStrings)) < size {
		return nil, fmt.Errorf("expected %d bytes, got %d. Response: %s", size, len(hexStrings), response)
	}

	// Convert hex strings to a raw byte slice
	out := make([]byte, size)
	for i := range size {
		val, err := strconv.ParseUint(hexStrings[i], 16, 8)
		if err != nil {
			return nil, fmt.Errorf("failed to parse byte at index %d (%s): %w", i, hexStrings[i], err)
		}
		out[i] = byte(val)
	}

	return out, nil
}

func (r *RetroArch) WriteMemory(vAddr uint32, data []byte) error {
	hexStr := make([]string, len(data))
	for i, b := range data {
		hexStr[i] = fmt.Sprintf("%02x", b)
	}

	address := r.ToPhysical(vAddr)
	cmd := fmt.Sprintf("WRITE_CORE_MEMORY %08x %s\n", address, strings.Join(hexStr, " "))

	r.connectionLock.Lock()
	defer r.connectionLock.Unlock()

	// Discard stale data
	r.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	discardBuf := make([]byte, 65535)
	for {
		_, err := r.conn.Read(discardBuf)
		if err != nil {
			break
		}
	}

	_, err := r.conn.Write([]byte(cmd))
	if err != nil {
		return fmt.Errorf("failed to write memory: %w", err)
	}

	// UDP write responses should be fast
	r.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	respBuf := make([]byte, 65535)
	_, err = r.conn.Read(respBuf)
	if err != nil {
		return fmt.Errorf("failed to read write confirmation: %w", err)
	}

	return nil
}

func (r *RetroArch) WritePlayerNames(names map[uint8]string) error {
	// Create the full 2048-byte block (256 players * 8 bytes)
	// We fill the entire block with 0xDF (Spaces) so empty slots are blank
	writeBytes := make([]byte, 2048)
	for i := range writeBytes {
		writeBytes[i] = 0xDF
	}

	for playerID, name := range names {
		encoded := randomizer.EncodeOoTName(name)

		// Player ID 1 goes to byte offset 8. Player ID 2 goes to 16.
		startIndex := int(playerID) * 8
		copy(writeBytes[startIndex:startIndex+8], encoded[:])
	}

	writeBytes = unSwapBytes(writeBytes)

	// Send the massive block to RetroArch
	targetAddr := r.coopContextOffset + PlayerNamesOffset
	return r.WriteMemory(targetAddr, writeBytes)
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
