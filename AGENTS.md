# Hyrule-Hub

A zero-config MQTT router + web overlay for Ocarina of Time Randomizer
Multiworld on RetroArch. When a player finds an item belonging to another
world, it routes over MQTT to that player's game.

## Architecture

Three strictly decoupled layers, wired by channels owned by the Hub
(see `internal/core/types.go` for the binding contracts — read it first):

```
RetroArch UDP ── Accessor ── Bridge ──events──▶ Hub ──Publish──▶ MQTT
   (55355)         │             ▲grants/names  │                  ▲
                   │             └──────────────┴──Subscribe───────┘
             core.MemoryAccessor              core.NetworkBroker
```

- `internal/core` — interfaces (`MemoryAccessor`, `MemoryBridge`,
  `NetworkBroker`) and domain types (`Item`, `ItemGrant`, `LocationCheck`,
  `PlayerName`, `GameEvent`, `Message`, `QoS`). No transport knowledge.
  Concurrency rules are documented in the package comment and are binding.
- `internal/bridge/retroarch` — `Accessor` implements `MemoryAccessor`
  over RetroArch's network command interface (UDP 55355,
  `READ/WRITE_CORE_MEMORY`, word-swapped wire bytes). `Bridge` implements
  `MemoryBridge`: 30fps poll of the coop context, emits `GameEvent`s,
  applies `ItemGrant`s and `PlayerName`s to memory.
- `internal/hub` — orchestrator. Owns `events`/`grants`/`names` channels,
  maps domain types to topics + JSON, dedups outgoing items by key,
  retries failed publishes, broadcasts the player name (retained).
- `internal/mqtt` — `core.NetworkBroker` over Eclipse Paho. Transport
  only; knows nothing about the game.
- `internal/server` — HTTP server: embeds the Svelte UI and receives
  `ClientConfig` over `/ws` (websocket is config-in only; no server→UI
  push exists yet).
- `internal/randomizer` — OoT domain data only: `ItemNames` map,
  `OotItem`, `EncodeOoTName` charset encoder.
- `cmd/faken64` — dev harness: fake RetroArch network interface +
  simulated game. See "End-to-end testing" below.
- `ui/` — Svelte 5 + Vite frontend, embedded via `//go:embed ui/dist`.

## Domain facts (easy to get wrong)

- The coop context is **not** at a fixed address. The ROM publishes a
  pointer at `0x8040_0000`; the bridge resolves it lazily each tick until
  the ROM initializes it. Never hard-code the context base.
- All N64 memory crossing the wire is **word-swapped** (Mupen stores
  32-bit words byte-reversed). `Accessor` unswaps internally — code above
  it always works in N64 big-endian order.
- `ItemGrant.World` is the **sender's** world (the game displays it as
  the item's origin); the publish *topic* carries the target world.
- Outgoing items: the game holds `outgoingKey` as a semaphore. The
  bridge only clears it after the Hub accepts the event — never clear
  before the consumer has the item.
- Incoming items: write into the 8-byte semaphore block at
  `base+0x0004` only when `incomingPlayer` and `incomingItem` are both
  zero (`BlockedSemaphoreError` is retryable).
- Topics: `pnp/rooms/{room}/players/{world}/items` (QoS 2) and
  `pnp/rooms/{room}/players/{id}/name` (retained, wildcard `+/name` sub).

## Build & test

**`ui/dist` must exist before any Go command compiles the module**
(`main.go` embeds it; it is gitignored):

```bash
cd ui && npm ci && npm run build && cd ..
go build ./...          # compile
go vet ./...
go test ./...           # unit tests
go test -race ./...     # with race detector — run before committing
```

Requires Go 1.25.6+ and Node 20+.

## Runtime env vars

- `PORT` — UI server port (default `8080`). Two local instances need
  distinct ports.
- `RETROARCH_ADDR` — emulator address (default `127.0.0.1:55355`).

## End-to-end testing without a ROM

```bash
docker run -d --name hyrule-mosquitto -p 1883:1883 eclipse-mosquitto

go run ./cmd/faken64 -addr 127.0.0.1:55355 -world 1 -target 2
go run ./cmd/faken64 -addr 127.0.0.1:55356 -world 2 -target 1

PORT=8080 RETROARCH_ADDR=127.0.0.1:55355 go run .
PORT=8081 RETROARCH_ADDR=127.0.0.1:55356 go run .
# then submit config via each UI (localhost:8080 / :8081),
# same room name, broker tcp://localhost:1883

mosquitto_sub -t 'pnp/rooms/#' -v   # watch all item/name traffic
```

The `/live-test` skill has a full runbook.

## Conventions

- Pass everything by value over channels; sending transfers ownership
  (core rule 1). Never close a channel you didn't create (rule 3).
- Bridge emits events with **non-blocking** sends; a dropped event must
  never lose an item — retry via the game's semaphore instead.
- New emulator-facing behavior belongs in `internal/bridge/retroarch`
  with a fake-memory test; network mapping belongs in `internal/hub`
  with a fake-broker test. Follow the existing test helpers
  (`fakeMemory`, `fakeBroker`, `waitFor`, `startBridge`/`startHub`).

## Known gaps (deliberate, as of the rewrite)

- No `state.json` persistence — dedup is in-memory only.
- No server→UI websocket push — the tracker UI shows a static message.
- File-select hash icons / coop-context read-back not implemented.
