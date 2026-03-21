package main

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/emulator"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/emulator/retroarch"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/mqtt"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

func TestGameState_Flow(t *testing.T) {
	emu := emulator.NewMockEmulator()
	mw := mqtt.NewMockMQTT()

	game := &GameState{
		emu:           emu,
		mw:            mw,
		playerNames:   make(map[uint8]string),
		outgoingItems: make(map[uint64]randomizer.OutgoingItem),
		incomingItems: make(map[uint64]randomizer.IncomingItem),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test Incoming Names
	go game.handleIncomingNames(ctx)

	nameUpdate := randomizer.PlayerName{PlayerID: 1, Name: "Navi"}
	mw.IncomingNamesChan <- nameUpdate

	// Give it a moment to process
	time.Sleep(100 * time.Millisecond)

	game.mu.RLock()
	if name, ok := game.playerNames[1]; !ok || name != "Navi" {
		t.Errorf("expected player 1 to be Navi, got %v", name)
	}
	game.mu.RUnlock()

	// Test Incoming Items
	go game.handleIncomingItems(ctx)

	incoming := randomizer.IncomingItem{Key: 123, Item: 1, World: 2}
	mw.IncomingItemsChan <- incoming

	// Give it a moment to process the worker
	time.Sleep(200 * time.Millisecond)

	// Verify it was written to Mock RAM
	pBytes, _ := emu.ReadMemory(retroarch.IncomingPlayerOffset, 2)
	iBytes, _ := emu.ReadMemory(retroarch.IncomingItemOffset, 2)

	if binary.BigEndian.Uint16(pBytes) != 2 {
		t.Errorf("expected incoming player 2, got %d", binary.BigEndian.Uint16(pBytes))
	}
	if binary.BigEndian.Uint16(iBytes) != 1 {
		t.Errorf("expected incoming item 1, got %d", binary.BigEndian.Uint16(iBytes))
	}

	// Test Outgoing Items
	// Manually write an item to Mock RAM
	buf2 := make([]byte, 2)
	binary.BigEndian.PutUint16(buf2, 10) // Item ID
	emu.WriteMemory(retroarch.OutgoingItemOffset, buf2)

	binary.BigEndian.PutUint16(buf2, 5) // Target Player
	emu.WriteMemory(retroarch.OutgoingPlayerOffset, buf2)

	buf8 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf8, 999) // Key
	emu.WriteMemory(retroarch.OutgoingKeyOffset, buf8)

	// Simulate the main loop's ticker for one iteration
	err := game.emu.ProcessOutgoingItem(func(item randomizer.OutgoingItem) error {
		if item.OutgoingKey == 0 {
			return nil
		}
		game.mu.RLock()
		_, exists := game.outgoingItems[item.OutgoingKey]
		game.mu.RUnlock()
		if exists {
			return nil
		}

		if err := game.mw.SendItem(item); err != nil {
			return err
		}

		game.mu.Lock()
		game.outgoingItems[item.OutgoingKey] = item
		game.mu.Unlock()
		return nil
	})

	if err != nil {
		t.Fatalf("failed to process outgoing item: %v", err)
	}

	// Verify it was sent to MQTT mock
	select {
	case sent := <-mw.SentItems:
		if sent.OutgoingKey != 999 {
			t.Errorf("expected outgoing key 999, got %d", sent.OutgoingKey)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for outgoing item to be sent")
	}
}
