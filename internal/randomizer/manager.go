package randomizer

import (
	"context"
	"encoding/binary"
	"fmt"
)

type MemoryAccessor interface {
	ReadMemory(ctx context.Context, vAddr uint32, size uint32) ([]byte, error)
	WriteMemory(ctx context.Context, vAddr uint32, data []byte) error
}

type Manager struct {
	ma MemoryAccessor
}

func NewManager(ma MemoryAccessor) (*Manager, error) {
	m := &Manager{
		ma: ma,
	}
	return m, nil
}

func (m *Manager) CheckForOutgoingItem(ctx context.Context) (*OutgoingItem, error) {
	// Check the key first and minimize network calls
	outgoingKeyBytes, err := m.ma.ReadMemory(ctx, coopContextAddress+OutgoingKeyOffset, 8)
	if err != nil {
		return nil, fmt.Errorf("failed to read outgoing key: %w", err)
	}
	var out OutgoingItem
	out.OutgoingKey = binary.BigEndian.Uint64(outgoingKeyBytes)
	if out.OutgoingKey == 0 {
		return nil, nil
	}

	// Non-zero key means we have an item for a player. Go ahead with the rest of the calls
	bytes, err := m.ma.ReadMemory(ctx, coopContextAddress+OutgoingItemOffset, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to read outgoing item: %w", err)
	}

	out.World = binary.BigEndian.Uint16(bytes[2:4])
	out.Item = OotItem(binary.BigEndian.Uint16(bytes[0:2]))

	return &out, nil
}

func (m *Manager) ClearOutgoingItem(ctx context.Context) error {
	// Clear the semaphore
	zeroBuf := make([]byte, 8)

	// Clear Key first and then Item just incase network breaks in between writies
	if err := m.ma.WriteMemory(ctx, coopContextAddress+OutgoingKeyOffset, zeroBuf); err != nil {
		return fmt.Errorf("failed to clear outgoing key: %w", err)
	}
	if err := m.ma.WriteMemory(ctx, coopContextAddress+OutgoingItemOffset, zeroBuf[:4]); err != nil {
		return fmt.Errorf("failed to clear outgoing item: %w", err)
	}
	return nil
}

func (m *Manager) PushIncomingItem(ctx context.Context, item IncomingItem) error {
	bytes, err := m.ma.ReadMemory(ctx, coopContextAddress+PlayerIDOffset, 8)
	if err != nil {
		return fmt.Errorf("failed to read semaphore block: %w", err)
	}

	currIncomingPlayer := binary.BigEndian.Uint16(bytes[2:4])
	currIncomingItem := binary.BigEndian.Uint16(bytes[4:6])

	if currIncomingPlayer != 0 || currIncomingItem != 0 {
		return &BlockedSemaphoreError{
			PlayerID: uint8(currIncomingPlayer),
			Item:     OotItem(currIncomingItem),
		}
	}

	binary.BigEndian.PutUint16(bytes[2:4], item.World)
	binary.BigEndian.PutUint16(bytes[4:6], uint16(item.Item))

	err = m.ma.WriteMemory(ctx, coopContextAddress+PlayerIDOffset, bytes)
	if err != nil {
		// Make this retryable? It could have created corrupt data in the emulator
		return fmt.Errorf("failed to write item: %w", err)
	}
	return nil

}
