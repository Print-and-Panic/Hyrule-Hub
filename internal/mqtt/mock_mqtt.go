package mqtt

import (
	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

type MockMQTT struct {
	SentItems         chan randomizer.OutgoingItem
	IncomingItemsChan chan randomizer.IncomingItem
	IncomingNamesChan chan randomizer.PlayerName
}

func NewMockMQTT() *MockMQTT {
	return &MockMQTT{
		SentItems:         make(chan randomizer.OutgoingItem, 100),
		IncomingItemsChan: make(chan randomizer.IncomingItem, 100),
		IncomingNamesChan: make(chan randomizer.PlayerName, 100),
	}
}

func (m *MockMQTT) Connect() error {
	return nil
}

func (m *MockMQTT) Disconnect() error {
	return nil
}

func (m *MockMQTT) SendItem(item randomizer.OutgoingItem) error {
	m.SentItems <- item
	return nil
}

func (m *MockMQTT) BroadcastName(playerID uint8, name string) error {
	return nil
}

func (m *MockMQTT) IncomingItems() <-chan randomizer.IncomingItem {
	return m.IncomingItemsChan
}

func (m *MockMQTT) IncomingNames() <-chan randomizer.PlayerName {
	return m.IncomingNamesChan
}
