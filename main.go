package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/bridge/retroarch"
	"github.com/Print-and-Panic/Hyrule-Hub/internal/hub"
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

	var config server.ClientConfig
	select {
	case <-ctx.Done():
		log.Println("Shutdown requested before config received.")
		return nil
	case config = <-configChan:
		log.Println("Configuration received, booting engine...")
	}

	accessor := retroarch.NewAccessor(os.Getenv("RETROARCH_ADDR"))
	if err := accessor.Connect(ctx); err != nil {
		return fmt.Errorf("emulator connection failed: %w", err)
	}
	defer accessor.Close()

	clientID := fmt.Sprintf("pnp-client-%s-%d", config.RoomName, config.WorldNumber)
	broker := mqtt.NewClient(config.MQTTUrl, clientID)
	if err := broker.Connect(ctx); err != nil {
		return fmt.Errorf("MQTT connection failed: %w", err)
	}
	defer broker.Disconnect(context.Background())

	h := hub.New(hub.Config{
		PlayerName:  config.PlayerName,
		WorldNumber: config.WorldNumber,
		RoomName:    config.RoomName,
	}, broker)

	bridge := retroarch.NewBridge(accessor, h.Events(), h.Grants(), h.Names())

	log.Println("Hyrule-Hub successfully started. Waiting for items...")

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return bridge.Start(gctx) })
	g.Go(func() error { return h.Run(gctx) })

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
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
