package sse

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"
)

type Metrics struct {
	Time       string `json:"time"`
	Goroutines int    `json:"goroutines"`
	AllocKB    uint64 `json:"alloc_kb"`
}

// Handler віддає безперервний потік метрик у форматі Server-Sent Events.
func Handler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	log.Printf("[SSE] Нове підключення клієнта телеметрії: %s", r.RemoteAddr)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	send := func() bool {
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)

		metrics := Metrics{
			Time:       time.Now().Format(time.RFC3339),
			Goroutines: runtime.NumGoroutine(),
			AllocKB:    memory.Alloc / 1024,
		}

		payload, err := json.Marshal(metrics)
		if err != nil {
			return false
		}

		if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	if !send() {
		return
	}

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[SSE] Клієнт %s розірвав з'єднання. Очищення ресурсів виконано.", r.RemoteAddr)
			return
		case <-ticker.C:
			if !send() {
				return
			}
		}
	}
}
