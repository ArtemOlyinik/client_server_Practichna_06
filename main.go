package main

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/ArtemOlyinik/client_server_Practichna_06/internal/hub"
	"github.com/ArtemOlyinik/client_server_Practichna_06/internal/sse"
)

func main() {
	log.SetFlags(0)
	log.Println("=== Сервіс реального часу (SSE & WebSockets Gateway) ===")

	realtimeHub := hub.NewHub()
	go realtimeHub.Run()
	log.Println("[СЕРВЕР] Диспетчер WebSocket Hub успішно запущено")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/metrics/stream", sse.Handler)
	mux.HandleFunc("/api/v1/ws", realtimeHub.ServeWS)
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("[СЕРВЕР] HTTP-слухач відкрито на порту :8080")

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[СЕРВЕР] Помилка HTTP-сервера: %v", err)
	}
}
