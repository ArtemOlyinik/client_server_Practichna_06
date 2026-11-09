package hub

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Message описує повідомлення, яке пересилається між клієнтами однієї кімнати.
type Message struct {
	Room   string
	Sender string
	Text   string
	Time   time.Time
}

func (m Message) Format() string {
	return fmt.Sprintf("[%s] %s: %s", m.Room, m.Sender, m.Text)
}

// Hub керує реєстрацією клієнтів і розсилкою повідомлень.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan Message
	register   chan *Client
	unregister chan *Client
	done       chan struct{}
	closeOnce  sync.Once
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan Message, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		done:       make(chan struct{}),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Printf("[WS] Новий клієнт підключився до кімнати [%s]: %s", client.room, client.address)

		case client := <-h.unregister:
			if _, exists := h.clients[client]; exists {
				delete(h.clients, client)
				client.close()
				log.Printf("[WS] Клієнт %s вийшов з кімнати [%s]", client.address, client.room)
			}

		case message := <-h.broadcast:
			log.Printf("[HUB BROADCAST] [Кімната: %s] Отримано повідомлення: %q", message.Room, message.Text)
			payload := []byte(message.Format())
			sent := 0

			for client := range h.clients {
				if client.room != message.Room {
					continue
				}
				select {
				case client.send <- payload:
					sent++
				default:
					log.Printf("[HUB BROADCAST] Черга клієнта %s переповнена", client.address)
				}
			}

			log.Printf("[HUB BROADCAST] Сповіщено клієнтів у кімнаті [%s]: %d", message.Room, sent)

		case <-h.done:
			for client := range h.clients {
				client.close()
			}
			return
		}
	}
}

func (h *Hub) Shutdown() {
	h.closeOnce.Do(func() {
		close(h.done)
	})
}
