---
name: live-test
description: Run a local two-player multiworld session without a ROM using faken64 and mosquitto
triggers:
  - user
  - model
allowed-tools:
  - exec
  - read
---

Run an end-to-end test of Hyrule-Hub on one machine: two fake N64
consoles, two hub instances, one MQTT broker. Items should flow in both
directions and appear in each fake console's log as
`>>> received <item> from world N! <<<`.

## 1. Start the broker

```bash
docker start hyrule-mosquitto 2>/dev/null || \
  docker run -d --name hyrule-mosquitto -p 1883:1883 eclipse-mosquitto
```

Verify anonymous access works:

```bash
mosquitto_sub -t 'pnp/test/#' -C 1 -W 3 &
mosquitto_pub -t 'pnp/test/hello' -m ping
```

## 2. Build binaries

```bash
cd ui && npm ci && npm run build && cd ..   # ui/dist is embedded; required
go build -o /tmp/hyrule-hub .
go build -o /tmp/faken64 ./cmd/faken64
```

## 3. Start the fake consoles (background)

```bash
/tmp/faken64 -addr 127.0.0.1:55355 -world 1 -target 2 -interval 8s
/tmp/faken64 -addr 127.0.0.1:55356 -world 2 -target 1 -interval 13s
```

Each logs `ROM booted, coop context at 0x80401000` once the pointer is
planted. Flags: `-world` is this player's ID, `-target` is where its
generated items go, `-interval` is the item cadence, `-boot` delays the
coop-context pointer appearing.

## 4. Start the hub instances (background)

```bash
PORT=8080 RETROARCH_ADDR=127.0.0.1:55355 /tmp/hyrule-hub
PORT=8081 RETROARCH_ADDR=127.0.0.1:55356 /tmp/hyrule-hub
```

## 5. Submit config

Each instance needs one `ClientConfig` JSON message on `/ws`. Either open
`http://localhost:8080` and `:8081` in a browser and submit the form, or
send it headlessly with a small gorilla/websocket client:

```json
// to ws://localhost:8080/ws
{"playerName":"Link","worldNumber":1,"mqttUrl":"tcp://localhost:1883","roomName":"panic-run"}
// to ws://localhost:8081/ws
{"playerName":"Zelda","worldNumber":2,"mqttUrl":"tcp://localhost:1883","roomName":"panic-run"}
```

## 6. What success looks like

- Hub logs: `hub: sent GI_X to world N (key K)` /
  `hub: received GI_X from world N (key K)`.
- faken64 logs: `[game] found GI_X for world N`, then
  `item sent ... (key K acknowledged)`, and on the other console
  `>>> received GI_X from world N! <<<`.
- Retained names on the broker:
  `mosquitto_sub -t 'pnp/rooms/panic-run/players/+/name' -v -C 2 -W 3`
  should print both players immediately.

## 7. Inspect / debug

```bash
mosquitto_sub -t 'pnp/rooms/#' -v   # all item + name traffic
```

If items don't flow: check the hub log for `coop context pointer ... is
not a valid N64 address` (faken64 still booting or `-addr` mismatch) and
for publish errors (broker not reachable).

## 8. Cleanup

Kill the background processes; leave the mosquitto container running if
it was already up, or `docker stop hyrule-mosquitto` if you started it.
