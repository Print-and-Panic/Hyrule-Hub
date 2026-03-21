package mqtt

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type pahoClient struct {
	client   mqtt.Client
	roomHash string
	playerID uint8

	itemChan chan randomizer.IncomingItem
	nameChan chan randomizer.PlayerName
}

// NewMQTTClient initializes the structs and channels but does not connect yet
func NewMQTTClient(brokerURI, roomHash string, playerID uint8) *pahoClient {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURI)
	// Unique Client ID is critical for Persistent Sessions
	opts.SetClientID(fmt.Sprintf("pnp-client-%s-%d", roomHash, playerID))
	opts.SetCleanSession(false) // Remember our missed items if we disconnect!

	return &pahoClient{
		roomHash: roomHash,
		playerID: playerID,
		itemChan: make(chan randomizer.IncomingItem, 100), // Buffer handles sudden item bursts
		nameChan: make(chan randomizer.PlayerName, 32),
		client:   mqtt.NewClient(opts),
	}
}

func (m *pahoClient) Connect() error {
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("MQTT connection failed: %w", token.Error())
	}

	// Subscribe to OUR items ONLY
	itemTopic := fmt.Sprintf("pnp/rooms/%s/players/%d/items", m.roomHash, m.playerID)
	if token := m.client.Subscribe(itemTopic, 1, m.onIncomingItem); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to items: %w", token.Error())
	}

	// Subscribe to EVERYONE'S names using the '+' wildcard
	nameTopic := fmt.Sprintf("pnp/rooms/%s/players/+/name", m.roomHash)
	if token := m.client.Subscribe(nameTopic, 1, m.onIncomingName); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to names: %w", token.Error())
	}

	return nil
}

func (m *pahoClient) Disconnect() error {
	m.client.Disconnect(250) // Wait 250ms to finish inflight messages
	return nil
}

// --- PUBLISHERS ---

func (m *pahoClient) SendItem(item randomizer.OutgoingItem) error {
	// Route it to the target player's topic
	topic := fmt.Sprintf("pnp/rooms/%s/players/%d/items", m.roomHash, item.World)

	payload, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	// Publish with QoS 1 (At least once), Retained FALSE
	token := m.client.Publish(topic, 1, false, payload)

	// token.Wait() blocks until the broker physically ACKs the message.
	token.Wait()
	return token.Error()
}

func (m *pahoClient) BroadcastName(playerID uint8, name string) error {
	topic := fmt.Sprintf("pnp/rooms/%s/players/%d/name", m.roomHash, playerID)

	update := randomizer.PlayerName{PlayerID: playerID, Name: name}
	payload, err := json.Marshal(update)
	if err != nil {
		return err
	}

	// Publish with QoS 1, Retained TRUE.
	token := m.client.Publish(topic, 1, true, payload)
	token.Wait()
	return token.Error()
}

// --- CALLBACKS ---

func (m *pahoClient) onIncomingItem(client mqtt.Client, msg mqtt.Message) {
	var item randomizer.IncomingItem
	if err := json.Unmarshal(msg.Payload(), &item); err != nil {
		log.Printf("Network error: failed to parse incoming item payload: %v", err)
		return
	}
	// Push into the buffered channel so the main thread can consume it
	m.itemChan <- item
}

func (m *pahoClient) onIncomingName(client mqtt.Client, msg mqtt.Message) {
	var update randomizer.PlayerName
	if err := json.Unmarshal(msg.Payload(), &update); err != nil {
		log.Printf("Network error: failed to parse name update: %v", err)
		return
	}
	m.nameChan <- update
}

// --- CHANNEL GETTERS ---

func (m *pahoClient) IncomingItems() <-chan randomizer.IncomingItem {
	return m.itemChan
}

func (m *pahoClient) IncomingNames() <-chan randomizer.PlayerName {
	return m.nameChan
}
