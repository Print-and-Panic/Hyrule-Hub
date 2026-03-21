package main

import (
	"context"
	"embed"
	"encoding/json"
	"log"
	"maps"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/emulator/retroarch"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/mqtt"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/server"
)

var (
	stateFilePath = "state.json"
)

//go:embed ui/dist
var frontendAssets embed.FS

type OoTEmulator interface {
	// Connect establishes a connection to the emulator.
	Connect() error
	Disconnect() error
	// Read the cooperative context from the emulator
	ReadCoopContext() (*randomizer.CoopContext, error)
	// Process outgoing item from the emulator
	ProcessOutgoingItem(publishFunc func(randomizer.OutgoingItem) error) error
	// Write the incoming item to the emulator
	WriteIncomingItem(randomizer.IncomingItem) error
	//Write Names
	WritePlayerNames(names map[uint8]string) error
}

type MWServer interface {
	// Lifecycle
	Connect() error
	Disconnect() error

	// Outgoing Actions
	SendItem(item randomizer.OutgoingItem) error
	BroadcastName(playerID uint8, name string) error

	// Incoming Streams
	IncomingItems() <-chan randomizer.IncomingItem
	IncomingNames() <-chan randomizer.PlayerName
}

type FrontendServer interface {
	// Serve the frontend assets
	Serve(embed.FS) (<-chan server.ClientConfig, error)
}

type GameState struct {
	emu OoTEmulator
	mw  MWServer
	ui  FrontendServer

	mu            sync.RWMutex
	playerNames   map[uint8]string
	outgoingItems map[uint64]randomizer.OutgoingItem
	incomingItems map[uint64]randomizer.IncomingItem
}

type GameStateSnapshot struct {
	PlayerNames   map[uint8]string
	OutgoingItems []randomizer.OutgoingItem
	IncomingItems []randomizer.IncomingItem
}

func main() {

	uiServer := &server.UIServer{}

	configChan, err := uiServer.Serve(frontendAssets)
	if err != nil {
		log.Printf("failed to serve UI: %v\n", err)
		return
	}

	config := <-configChan

	retroArch := new(retroarch.RetroArch)
	mqttClient := mqtt.NewMQTTClient(config.MQTTUrl, config.RoomName, config.WorldNumber)

	game := GameState{
		emu:           retroArch,
		mw:            mqttClient,
		ui:            uiServer,
		playerNames:   make(map[uint8]string),
		outgoingItems: make(map[uint64]randomizer.OutgoingItem),
		incomingItems: make(map[uint64]randomizer.IncomingItem),
	}

	if err := game.loadState(); err != nil {
		log.Printf("Warning: Failed to load state. Starting fresh. Error: %v\n", err)
	} else {
		log.Printf("Loaded state from disk: %d outgoing, %d incoming items.\n",
			len(game.outgoingItems), len(game.incomingItems))
	}

	err = game.emu.Connect()
	if err != nil {
		log.Printf("failed to connect to emulator: %v\n", err)
		return
	}
	defer game.emu.Disconnect()

	err = game.mw.Connect()
	if err != nil {
		log.Printf("failed to connect to MW server: %v\n", err)
		return
	}
	defer game.mw.Disconnect()

	coopCtx, err := game.emu.ReadCoopContext()
	if err != nil {
		log.Printf("failed to read coop context: %v\n", err)
		return
	}

	if coopCtx == nil {
		log.Println("coop context is nil")
		return
	}

	// Write player names
	err = game.mw.BroadcastName(config.WorldNumber, config.PlayerName)
	if err != nil {
		log.Printf("failed to broadcast name: %v\n", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go game.handleIncomingItems(ctx)
	go game.handleIncomingNames(ctx)

	ticker := time.NewTicker(time.Second / 30)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down...")
			return
		case <-ticker.C:
			err := game.emu.ProcessOutgoingItem(func(item randomizer.OutgoingItem) error {
				logItem(item)
				if item.OutgoingKey == 0 {
					return nil
				}

				// Safely check cache
				log.Printf("Checking cache for outgoing key: %d\n", item.OutgoingKey)
				game.mu.RLock()
				_, exists := game.outgoingItems[item.OutgoingKey]
				game.mu.RUnlock()

				if exists {
					log.Printf("Item already processed: %d\n", item.OutgoingKey)
					return nil // We already successfully processed this
				}

				// SEND TO NETWORK FIRST. If this fails, we return the error and try again next tick.
				if err := game.mw.SendItem(item); err != nil {
					log.Printf("Failed to send item to network: %v\n", err)
					return err
				}

				// NETWORK SUCCESS. Now lock, cache, and save.
				log.Printf("Network success, locking and caching item: %v\n", item)
				game.mu.Lock()
				defer game.mu.Unlock() // Ensure unlock happens even if save fails

				game.outgoingItems[item.OutgoingKey] = item
				return game.saveStateLocked() // Call the internal locked version
			})

			if err != nil {
				log.Printf("Warning: failed to process outgoing item: %v\n", err)
			}
		}
	}

}

// --- WORKER ROUTINES ---

func (game *GameState) handleIncomingItems(ctx context.Context) {
	// Sequential worker to maintain order without blocking the network receiver
	workerChan := make(chan randomizer.IncomingItem, 256)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case item := <-workerChan:
				if err := game.emu.WriteIncomingItem(item); err != nil {
					log.Printf("failed to write incoming item: %v\n", err)
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down incoming items handler...")
			return
		case item := <-game.mw.IncomingItems():
			game.mu.Lock()
			game.incomingItems[item.Key] = item
			game.saveStateLocked() // Optional: save immediately when receiving an item
			game.mu.Unlock()

			// Queue it for the emulator worker
			select {
			case workerChan <- item:
			default:
				log.Printf("Warning: Emulator worker queue is full, dropping item %d\n", item.Key)
			}
		}
	}
}

func (game *GameState) handleIncomingNames(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down incoming names handler...")
			return
		case name := <-game.mw.IncomingNames():
			game.mu.Lock()
			game.playerNames[name.PlayerID] = name.Name
			game.saveStateLocked()
			// Take a copy of the map to send to the emulator without holding the lock
			namesCopy := make(map[uint8]string)
			maps.Copy(namesCopy, game.playerNames)
			game.mu.Unlock()

			game.emu.WritePlayerNames(namesCopy)
		}
	}
}

// --- STATE MANAGEMENT ---

func (game *GameState) loadState() error {
	data, err := os.ReadFile(stateFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Normal on first run
		}
		return err
	}

	var snapshot GameStateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	game.mu.Lock()
	defer game.mu.Unlock()

	for _, item := range snapshot.OutgoingItems {
		game.outgoingItems[item.OutgoingKey] = item
	}
	for _, item := range snapshot.IncomingItems {
		game.incomingItems[item.Key] = item
	}
	if snapshot.PlayerNames != nil {
		game.playerNames = snapshot.PlayerNames
	}
	return nil
}

func (game *GameState) saveStateLocked() error {
	var snapshot GameStateSnapshot

	for _, item := range game.outgoingItems {
		snapshot.OutgoingItems = append(snapshot.OutgoingItems, item)
	}
	for _, item := range game.incomingItems {
		snapshot.IncomingItems = append(snapshot.IncomingItems, item)
	}
	snapshot.PlayerNames = make(map[uint8]string)
	maps.Copy(snapshot.PlayerNames, game.playerNames)

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	log.Printf("Saving state: %d outgoing, %d incoming items cached.\n",
		len(game.outgoingItems), len(game.incomingItems))

	return os.WriteFile(stateFilePath, data, 0644)
}

func logItem(item randomizer.OutgoingItem) {
	if item.OutgoingKey == 0 {
		return
	}
	log.Printf("Item: World=%d, Item=%d, OutgoingKey=%d\n", item.World, item.Item, item.OutgoingKey)
}
