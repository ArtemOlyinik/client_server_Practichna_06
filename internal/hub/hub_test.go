package hub

import "testing"

func TestMessageFormat(t *testing.T) {
	message := Message{
		Room:   "alerts",
		Sender: "127.0.0.1:53225",
		Text:   "Увага! Перегрів вузла node-03",
	}

	expected := "[alerts] 127.0.0.1:53225: Увага! Перегрів вузла node-03"
	if got := message.Format(); got != expected {
		t.Fatalf("очікували %q, отримали %q", expected, got)
	}
}
