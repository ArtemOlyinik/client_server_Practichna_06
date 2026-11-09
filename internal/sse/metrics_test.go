package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(Handler))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("не вдалося створити запит: %v", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("не вдалося підключитися до SSE: %v", err)
	}
	defer response.Body.Close()

	if contentType := response.Header.Get("Content-Type"); contentType != "text/event-stream" {
		t.Fatalf("очікували text/event-stream, отримали %s", contentType)
	}

	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("не вдалося прочитати подію: %v", err)
	}

	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data: ") {
		t.Fatalf("очікували рядок data:, отримали %q", line)
	}

	var metrics Metrics
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &metrics); err != nil {
		t.Fatalf("не вдалося розібрати JSON метрик: %v", err)
	}
	if metrics.Goroutines == 0 {
		t.Fatal("кількість горутин має бути більшою за нуль")
	}
}
