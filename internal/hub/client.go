package hub

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 512
	sendBufferSize = 256
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Client представляє одне WebSocket-з'єднання.
type Client struct {
	hub     *Hub
	conn    *websocket.Conn
	send    chan []byte
	room    string
	address string
	done    chan struct{}

	closeOnce sync.Once
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Помилка Upgrade: %v", err)
		return
	}

	room := strings.TrimSpace(r.URL.Query().Get("room"))
	if room == "" {
		room = "general"
	}

	client := &Client{
		hub:     h,
		conn:    conn,
		send:    make(chan []byte, sendBufferSize),
		room:    room,
		address: r.RemoteAddr,
		done:    make(chan struct{}),
	}

	h.register <- client

	go client.writePump()
	client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))

	c.conn.SetPongHandler(func(string) error {
		log.Printf("[HEARTBEAT] Pong отримано від %s", c.address)
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		text := strings.TrimSpace(string(message))
		if text == "" {
			continue
		}

		c.hub.broadcast <- Message{
			Room:   c.room,
			Sender: c.address,
			Text:   text,
			Time:   time.Now(),
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case message := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			log.Printf("[HEARTBEAT] Ping надіслано клієнту %s", c.address)
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-c.done:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		}
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}
