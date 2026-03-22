[![Tests](https://github.com/Print-and-Panic/Hyrule-Hub/actions/workflows/test.yml/badge.svg)](https://github.com/Print-and-Panic/Hyrule-Hub/actions/workflows/test.yml)

# Hyrule-Hub 🗡️

A zero-configuration MQTT router and web overlay for Ocarina of Time Randomizer Multiworld. 

Built as a lightweight alternative to traditional multiworld servers with support for RetroArch.

Originally developed for the [Print and Panic](https://www.youtube.com/@printandpanic) YouTube channel.

## ✨ Features

* **Zero-Config Deployment:** The entire Svelte frontend is compiled and embedded into the Go executable. One file runs the whole stack.
* **RetroArch Integration:** Works seamlessly with RetroArch running Mupen64Plus.
* **Cross-Platform:** Native binaries compiled for Windows, macOS, and Linux.

## 🚀 Quick Start (For Players)

1. Download the latest executable for your OS from the **[Releases](#)** tab.
2. Run the executable. A local web server will instantly start.
3. Open your browser and navigate to `http://localhost:8080`.
4. Enter your Player Name, World Number, MQTT Broker URL, and Room Name.
5. Click **Connect to ROM** and start finding items!

## 🛠️ Development Setup (For Contributors)

If you want to build the tracker from source or tweak the Svelte UI, you will need **Go 1.25+** and **Node.js 20+**.

### 1. Build the Frontend
The Go backend expects the compiled Svelte assets to exist in the `ui/dist` folder before it can embed them.

```bash
cd ui
npm install
npm run build
```

### 2. Run the Backend
```bash
go run main.go
```

The web interface will be available at `http://localhost:8080`.
