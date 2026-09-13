package server

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow connections from our local Svelte app
	},
}

type ClientConfig struct {
	PlayerName  string `json:"playerName"`
	WorldNumber uint8  `json:"worldNumber"`
	MQTTUrl     string `json:"mqttUrl"`
	RoomName    string `json:"roomName"`
}

type UIServer struct {
}

func (s *UIServer) Serve(assets embed.FS) (<-chan ClientConfig, error) {
	// We have to strip the "ui/dist" prefix so the web server
	// serves the index.html at the root "/" path.
	distFolder, err := fs.Sub(assets, "ui/dist")
	if err != nil {
		return nil, err
	}

	configChan := make(chan ClientConfig, 1)

	// Serve the static files
	http.Handle("/", http.FileServer(http.FS(distFolder)))
	http.HandleFunc("/ws", HandleWebSocket(configChan))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Println("==================================================")
	log.Printf("🌐 UI running at: http://localhost:%s\n", port)
	log.Println("==================================================")

	// Start the server in a background goroutine
	go func() {
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Printf("UI Server failed: %v", err)
		}
	}()
	return configChan, nil
}

// HandleWebSocket intercepts the /ws route
// We pass a channel in so the HTTP server can talk to main()
func HandleWebSocket(configChan chan<- ClientConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Failed to upgrade websocket: %v", err)
			return
		}
		defer conn.Close()

		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				break
			}

			if messageType == websocket.TextMessage {
				var config ClientConfig
				if err := json.Unmarshal(payload, &config); err == nil {
					// First config wins; drop extras rather than blocking
					// this read loop once main() has consumed it.
					select {
					case configChan <- config:
					default:
					}
				}
			}
		}
	}
}
