package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func dialWS(t *testing.T, handler http.HandlerFunc) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestWebSocketDeliversConfig(t *testing.T) {
	configChan := make(chan ClientConfig, 1)
	conn := dialWS(t, HandleWebSocket(configChan))

	want := ClientConfig{
		PlayerName:  "Link",
		WorldNumber: 1,
		MQTTUrl:     "tcp://localhost:1883",
		RoomName:    "test-room",
	}
	if err := conn.WriteJSON(want); err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	select {
	case got := <-configChan:
		if got != want {
			t.Errorf("config mismatch: got %+v, want %+v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("config not delivered to channel")
	}
}

func TestWebSocketSecondConfigDoesNotBlock(t *testing.T) {
	configChan := make(chan ClientConfig, 1)
	conn := dialWS(t, HandleWebSocket(configChan))

	cfg := ClientConfig{PlayerName: "Link", WorldNumber: 1, MQTTUrl: "tcp://x", RoomName: "r"}
	for range 3 {
		if err := conn.WriteJSON(cfg); err != nil {
			t.Fatalf("WriteJSON failed: %v", err)
		}
	}

	// Only the first config is delivered; the rest are dropped without
	// stalling the read loop. Prove the loop is alive by checking the
	// connection still accepts writes after the buffer fills.
	time.Sleep(100 * time.Millisecond)
	if got := len(configChan); got != 1 {
		t.Errorf("config channel holds %d configs, want 1", got)
	}
	if err := conn.WriteJSON(cfg); err != nil {
		t.Errorf("connection stalled after extra configs: %v", err)
	}
}

func TestWebSocketIgnoresMalformedPayload(t *testing.T) {
	configChan := make(chan ClientConfig, 1)
	conn := dialWS(t, HandleWebSocket(configChan))

	if err := conn.WriteMessage(websocket.TextMessage, []byte("{not json")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// A valid config after the garbage must still be delivered.
	want := ClientConfig{PlayerName: "Zelda", WorldNumber: 2, MQTTUrl: "tcp://x", RoomName: "r"}
	if err := conn.WriteJSON(want); err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	select {
	case got := <-configChan:
		if got != want {
			t.Errorf("config mismatch: got %+v, want %+v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("config not delivered after malformed payload")
	}
}
