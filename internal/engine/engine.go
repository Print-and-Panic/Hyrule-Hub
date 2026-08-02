package engine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

const (
	coopContextAddress = 0x8040_0000
	coopContextSize    = 0xc24
)

type Engine struct {
	incomingItems chan randomizer.IncomingItem
	outgoingItems chan randomizer.OutgoingItem

	incomingNames chan randomizer.PlayerName
	outgoingNames chan randomizer.PlayerName

	playerName map[uint8]string

	n64 N64
}

// This forces the Engine to care about memory structure
type N64 interface {
	ReadMemory(ctx context.Context, vAddr uint32, size uint32) ([]byte, error)
	WriteMemory(ctx context.Context, vAddr uint32, data []byte) error
}

// This might too tightly couple Engine to MQTT
type MWServer interface {
	Publish(ctx context.Context, topic string, QoS byte, retained bool, payload any) error
	Subscribe(ctx context.Context, topic string, QoS byte, incomingFunc func(context.Context, json.RawMessage) error) error
}

func NewEngine(n64 N64) *Engine {
	e := &Engine{}
	e.incomingItems = make(chan randomizer.IncomingItem, 256)
	e.outgoingItems = make(chan randomizer.OutgoingItem, 256)

	e.incomingNames = make(chan randomizer.PlayerName, 256)
	e.outgoingNames = make(chan randomizer.PlayerName, 10)

	e.n64 = n64
	return e
}

func (e *Engine) Run(ctx context.Context) error {
	// Needs to handle receiving items, broadcasting items, and new player names
	ticker := time.NewTicker(time.Second / 30) // No need to poll any faster
	defer ticker.Stop()

	incomingItemQueue := make([]randomizer.IncomingItem, 0, 256)
	incomingNameQueue := make([]randomizer.PlayerName, 0, 256)

	for {
		select {
		case <-ctx.Done():
			// Do cleanup work
			return ctx.Err()
		case name, ok := <-e.incomingNames:
			if !ok {
				return fmt.Errorf("incoming names channel closed")
			}
			incomingNameQueue = append(incomingNameQueue, name)
		case item, ok := <-e.incomingItems:
			if !ok {
				return fmt.Errorf("incoming items channel closed")
			}
			incomingItemQueue = append(incomingItemQueue, item)
		case item, ok := <-e.outgoingItems:
			if !ok {
				return fmt.Errorf("outgoing items channel closed")
			}
			err := e.publishItem(ctx, item)
			if err != nil {
				log.Printf("error publishing item to queue: %s", err.Error())
			}
		case <-ticker.C:
			// Check for items
			item, err := e.pollN64(ctx)
			if err == nil && item != nil {
				e.outgoingItems <- *item
			} else {
				log.Printf("error polling N64: %s", err.Error())
			}

			// Process Queues
			if len(incomingNameQueue) > 0 {
				e.playerName[incomingNameQueue[0].PlayerID] = incomingNameQueue[0].Name
				err := e.flushNames(ctx)
				if err == nil {
					incomingNameQueue = incomingNameQueue[1:]
				} else {
					log.Printf("failed to flush names to N64: %s", err.Error())
				}
			}

			if len(incomingItemQueue) > 0 {
				err := e.handleIncomingItem(ctx, incomingItemQueue[0])
				if err == nil {
					incomingItemQueue = incomingItemQueue[1:]
				} else {
					log.Printf("failed to write incoming item to N64: %s", err.Error())
				}
			}
		}
	}
}

func (e *Engine) pollN64(ctx context.Context) (*randomizer.OutgoingItem, error) {
	bytes, err := e.n64.ReadMemory(ctx, coopContextAddress+OutgoingItemOffset, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to read outgoing item: %w", err)
	}

	out := randomizer.OutgoingItem{
		World: binary.BigEndian.Uint16(bytes[2:4]),
		Item:  randomizer.OotItem(binary.BigEndian.Uint16(bytes[0:2])),
	}

	outgoingKeyBytes, err := e.n64.ReadMemory(ctx, coopContextAddress+OutgoingKeyOffset, 8)
	if err != nil {
		return nil, fmt.Errorf("failed to read outgoing key: %w", err)
	}
	out.OutgoingKey = binary.BigEndian.Uint64(outgoingKeyBytes)

	// Clear the semaphore
	zeroBuf := make([]byte, 8)
	if err := e.n64.WriteMemory(ctx, coopContextAddress+OutgoingItemOffset, zeroBuf[:4]); err != nil {
		return nil, fmt.Errorf("failed to clear outgoing item: %w", err)
	}
	if err := e.n64.WriteMemory(ctx, coopContextAddress+OutgoingKeyOffset, zeroBuf); err != nil {
		return nil, fmt.Errorf("failed to clear outgoing key: %w", err)
	}

	return &out, nil

}

func (e *Engine) publishItem(ctx context.Context, item randomizer.OutgoingItem) error {

}
