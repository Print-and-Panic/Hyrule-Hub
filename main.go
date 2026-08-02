package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/emulator/retroarch"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/engine"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/mqtt"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/server"
)

//go:embed ui/dist
var frontendAssets embed.FS

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	uiServer := &server.UIServer{}
	configChan, err := uiServer.Serve(frontendAssets)
	if err != nil {
		return fmt.Errorf("failed to serve UI: %w", err)
	}
	// Rewrite to use custom server for clean shutdowns
	// defer uiServer.Shutdown()

	var config server.ClientConfig
	select {
	case <-ctx.Done():
		log.Println("Shutdown requested before config received.")
		return nil
	case config = <-configChan:
		log.Println("Configuration received, booting engine...")
	}

	retroArch := new(retroarch.RetroArch)
	if err := retroArch.Connect(ctx); err != nil {
		return fmt.Errorf("emulator connection failed: %w", err)
	}
	defer retroArch.Disconnect()

	mqttClient := mqtt.NewMQTTClient(config.MQTTUrl, config.RoomName, config.WorldNumber)
	if err := mqttClient.Connect(ctx); err != nil {
		return fmt.Errorf("MQTT connection failed: %w", err)
	}
	defer mqttClient.Disconnect()

	mqttClient.BroadcastName(ctx, config.WorldNumber, config.PlayerName)

	app := engine.NewEngine(retroArch, mqttClient, uiServer, "state.json")

	log.Println("Hyrule-Hub successfully started. Waiting for items...")

	if err := app.Start(ctx); err != nil {
		return fmt.Errorf("tracker crashed: %w", err)
	}

	return nil

}

func main() {

	if err := run(); err != nil {
		log.Fatalf("Fatal: %v", err)
	}
	log.Println("Graceful shutdown complete.")

}
