package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	url := os.Getenv("SSE_URL")
	if url == "" {
		url = "http://localhost:8080/api/v1/metrics/stream"
	}

	response, err := http.Get(url)
	if err != nil {
		log.Fatalf("[SSE] Помилка підключення: %v", err)
	}
	defer response.Body.Close()

	fmt.Printf("[SSE] Content-Type: %s\n", response.Header.Get("Content-Type"))
	fmt.Println("[SSE] Отримуємо потік метрик...")

	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[SSE] Потік завершено: %v", err)
	}
}
