package emulator

import (
	"encoding/binary"
	"sync"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/emulator/retroarch"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

// MockEmulator simulates the RetroArch memory space natively in Go.
type MockEmulator struct {
	mu  sync.RWMutex
	ram map[uint32]byte
}

func NewMockEmulator() *MockEmulator {
	return &MockEmulator{
		ram: make(map[uint32]byte),
	}
}

func (m *MockEmulator) Connect() error {
	return nil
}

func (m *MockEmulator) Disconnect() error {
	return nil
}

func (m *MockEmulator) ReadMemory(address uint32, size uint32) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]byte, size)
	for i := uint32(0); i < size; i++ {
		result[i] = m.ram[address+i]
	}
	return result, nil
}

func (m *MockEmulator) WriteMemory(address uint32, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, b := range data {
		m.ram[address+uint32(i)] = b
	}
	return nil
}

func (m *MockEmulator) ReadCoopContext() (*randomizer.CoopContext, error) {
	return &randomizer.CoopContext{PlayerID: 1}, nil
}

func (m *MockEmulator) ProcessOutgoingItem(publishFunc func(randomizer.OutgoingItem) error) error {
	// Read the outgoing item from mock RAM
	itemBytes, _ := m.ReadMemory(retroarch.OutgoingItemOffset, 2)
	playerBytes, _ := m.ReadMemory(retroarch.OutgoingPlayerOffset, 2)
	keyBytes, _ := m.ReadMemory(retroarch.OutgoingKeyOffset, 8)

	item := randomizer.OutgoingItem{
		Item:        randomizer.OotItem(binary.BigEndian.Uint16(itemBytes)),
		World:       binary.BigEndian.Uint16(playerBytes),
		OutgoingKey: binary.BigEndian.Uint64(keyBytes),
	}

	if item.OutgoingKey == 0 {
		return nil
	}

	if err := publishFunc(item); err != nil {
		return err
	}

	// Clear it
	m.WriteMemory(retroarch.OutgoingItemOffset, make([]byte, 2))
	m.WriteMemory(retroarch.OutgoingKeyOffset, make([]byte, 8))
	return nil
}

func (m *MockEmulator) WriteIncomingItem(item randomizer.IncomingItem) error {
	// Simple mock write
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, item.World)
	m.WriteMemory(retroarch.IncomingPlayerOffset, buf)

	binary.BigEndian.PutUint16(buf, uint16(item.Item))
	m.WriteMemory(retroarch.IncomingItemOffset, buf)
	return nil
}

func (m *MockEmulator) WritePlayerNames(names map[uint8]string) error {
	return nil
}
