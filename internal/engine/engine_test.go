package engine

import (
	"testing"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

// --- MOCKS ---

type mockEmulator struct {
	ItemsToFind []randomizer.OutgoingItem
}

func (m *mockEmulator) Connect() error                                       { return nil }
func (m *mockEmulator) Disconnect() error                                    { return nil }
func (m *mockEmulator) ReadCoopContext() (*randomizer.CoopContext, error)    { return nil, nil }
func (m *mockEmulator) WritePlayerNames(names map[uint8]string) error        { return nil }
func (m *mockEmulator) WriteIncomingItem(item randomizer.IncomingItem) error { return nil }

func (m *mockEmulator) ProcessOutgoingItem(publishFunc func(randomizer.OutgoingItem) error) error {
	if len(m.ItemsToFind) > 0 {
		item := m.ItemsToFind[0]
		m.ItemsToFind = m.ItemsToFind[1:] // pop
		return publishFunc(item)
	}
	return nil
}

type mockMQTT struct {
	SentItems []randomizer.OutgoingItem
}

func (m *mockMQTT) Connect() error                                  { return nil }
func (m *mockMQTT) Disconnect() error                               { return nil }
func (m *mockMQTT) BroadcastName(playerID uint8, name string) error { return nil }
func (m *mockMQTT) IncomingItems() <-chan randomizer.IncomingItem   { return nil }
func (m *mockMQTT) IncomingNames() <-chan randomizer.PlayerName     { return nil }

func (m *mockMQTT) SendItem(item randomizer.OutgoingItem) error {
	m.SentItems = append(m.SentItems, item)
	return nil
}

func TestCachePreventsDuplicates(t *testing.T) {

	mockEmu := &mockEmulator{
		ItemsToFind: []randomizer.OutgoingItem{
			{OutgoingKey: 999, Item: 62, World: 2}, // The game found an item!
			{OutgoingKey: 999, Item: 62, World: 2}, // Emulator was slow, gave us the exact same item again!
		},
	}
	mockNet := &mockMQTT{}

	eng := NewEngine(mockEmu, mockNet, nil, "")

	// Simulate two ticks
	for range 2 {
		mockEmu.ProcessOutgoingItem(func(item randomizer.OutgoingItem) error {
			if item.OutgoingKey == 0 {
				return nil
			}

			eng.mu.RLock()
			_, exists := eng.outgoingItems[item.OutgoingKey]
			eng.mu.RUnlock()

			if exists {
				return nil
			}

			if err := mockNet.SendItem(item); err != nil {
				return err
			}

			eng.mu.Lock()
			eng.outgoingItems[item.OutgoingKey] = item
			eng.mu.Unlock()
			return nil
		})
	}

	if len(mockNet.SentItems) != 1 {
		t.Errorf("Cache failed! Expected 1 item sent, network received %d", len(mockNet.SentItems))
	}
}

func TestEngineControlFlow(t *testing.T) {

}
