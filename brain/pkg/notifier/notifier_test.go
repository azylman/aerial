package notifier

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestModelUnavailableMessage(t *testing.T) {
	msg := ModelUnavailableMessage()
	expected := "Apologies, the AI model is currently unavailable or being rate limited. Please try again in a few moments."
	if msg != expected {
		t.Errorf("Expected %q, got %q", expected, msg)
	}
}

func TestStaticFallback(t *testing.T) {
	// 503 / high demand / rate limit should return factual bare ModelUnavailableMessage
	msg503 := StaticFallback("API returned Error 503: unavailable")
	if msg503 != ModelUnavailableMessage() {
		t.Errorf("Expected bare ModelUnavailableMessage for 503, got: %q", msg503)
	}

	// Session reset / corrupt
	msgReset := StaticFallback("Resetting session due to conversation corrupted")
	if !strings.Contains(msgReset, "refreshed our conversation") && !strings.Contains(msgReset, "session") {
		t.Errorf("Expected session reset fallback message, got: %q", msgReset)
	}

	// Poison pill
	msgPoison := StaticFallback("poison pill: dropped crashed message")
	if !strings.Contains(msgPoison, "repeated crashes") {
		t.Errorf("Expected poison pill fallback message, got: %q", msgPoison)
	}

	// Watchdog timeout
	msgWatchdog := StaticFallback("execution terminated by watchdog: inactivity timeout exceeded")
	if !strings.Contains(msgWatchdog, "timed out") {
		t.Errorf("Expected watchdog timeout fallback message, got: %q", msgWatchdog)
	}

	// General error
	msgGeneral := StaticFallback("Fatal unknown error")
	if !strings.Contains(msgGeneral, "hiccup") {
		t.Errorf("Expected general hiccup fallback message, got: %q", msgGeneral)
	}
}

func TestGenerateSessionResetAndPoisonPillFallback(t *testing.T) {
	// Empty API key should immediately return fallback
	resReset := GenerateSessionResetMessage("agy", "")
	if !strings.Contains(resReset, "refreshed our conversation") {
		t.Errorf("Expected reset fallback when apiKey is empty, got: %q", resReset)
	}

	resPoison := GeneratePoisonPillMessage("agy", "", "SELECT * FROM huge_table")
	if !strings.Contains(resPoison, "repeated crashes") {
		t.Errorf("Expected poison fallback when apiKey is empty, got: %q", resPoison)
	}

	resPoisonEmpty := GeneratePoisonPillMessage("agy", "", "")
	if !strings.Contains(resPoisonEmpty, "repeated crashes") {
		t.Errorf("Expected poison fallback when snippet is empty, got: %q", resPoisonEmpty)
	}
}

func TestGenerateDynamicNotificationWithMock(t *testing.T) {
	// Using echo as the agy binary simulator (fails JSON parse and returns fallback)
	res := GenerateDynamicNotification("echo", "test-api-key", "Test description")
	if res == "" {
		t.Error("Expected non-empty dynamic notification from mock")
	}
	if !strings.Contains(res, "✨") && !strings.Contains(res, "🌸") {
		t.Errorf("Expected fallback notification with emojis, got: %q", res)
	}
}

func TestGenerateDynamicNotification_SuccessfulAgyRun(t *testing.T) {
	tmpDir := t.TempDir()
	mockBin := filepath.Join(tmpDir, "mock_agy.sh")
	scriptContent := `#!/bin/sh
cat << 'EOF'
{"response": "Hey bestie! ✨ Everything is running smoothly now! 🌸"}
EOF
`
	if err := os.WriteFile(mockBin, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	res := GenerateDynamicNotification(mockBin, "valid_key", "session reset due to context corruption")
	if !strings.Contains(res, "bestie") {
		t.Errorf("Expected dynamic notification from mock agy, got: %q", res)
	}

	// Test with poison pill context description
	resPoison := GenerateDynamicNotification(mockBin, "valid_key", "a message caused repeated crashes and had to be dropped")
	if !strings.Contains(resPoison, "bestie") {
		t.Errorf("Expected dynamic notification for poison pill, got: %q", resPoison)
	}

	// Test with 503 outage context description
	res503 := GenerateDynamicNotification(mockBin, "valid_key", "Error 503: unavailable")
	if !strings.Contains(res503, "bestie") {
		t.Errorf("Expected dynamic notification for 503 outage, got: %q", res503)
	}
}

func TestFormatDurationHuman(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{d: 0, want: "a few moments"},
		{d: -5 * time.Second, want: "a few moments"},
		{d: 45 * time.Second, want: "45s"},
		{d: 15 * time.Minute, want: "15m"},
		{d: 16*time.Minute + 58*time.Second, want: "16m 58s"},
		{d: 1 * time.Hour, want: "1h"},
		{d: 1*time.Hour + 15*time.Minute, want: "1h 15m"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatDurationHuman(tt.d)
			if got != tt.want {
				t.Errorf("FormatDurationHuman(%v) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}

func TestFormatQuotaPauseMessage(t *testing.T) {
	runAt := time.Unix(1725678900, 0)
	dur := 16*time.Minute + 58*time.Second

	// Scheduled = true
	msgScheduled := FormatQuotaPauseMessage(dur, runAt, true, false)
	if !strings.Contains(msgScheduled, "**16m 58s**") {
		t.Errorf("Expected **16m 58s** in scheduled message, got: %s", msgScheduled)
	}
	if !strings.Contains(msgScheduled, "<t:1725678900:R>") {
		t.Errorf("Expected live Discord countdown <t:1725678900:R>, got: %s", msgScheduled)
	}
	if !strings.Contains(msgScheduled, "automatically scheduled a retry") {
		t.Errorf("Expected scheduled confirmation, got: %s", msgScheduled)
	}
	if !strings.Contains(msgScheduled, "GEMINI_API_KEY") {
		t.Errorf("Expected GEMINI_API_KEY advice, got: %s", msgScheduled)
	}

	// Scheduled = false (DB failure)
	msgNotScheduled := FormatQuotaPauseMessage(dur, runAt, false, false)
	if strings.Contains(msgNotScheduled, "automatically scheduled a retry") {
		t.Errorf("Did not expect scheduled confirmation when scheduled=false, got: %s", msgNotScheduled)
	}
	if !strings.Contains(msgNotScheduled, "ping me again") {
		t.Errorf("Expected re-ask prompt, got: %s", msgNotScheduled)
	}

	// Circuit breaker = true
	msgCircuit := FormatQuotaPauseMessage(dur, runAt, false, true)
	if !strings.Contains(msgCircuit, "again after a scheduled auto-retry") {
		t.Errorf("Expected circuit breaker notice, got: %s", msgCircuit)
	}
	if !strings.Contains(msgCircuit, "paused automated retries") {
		t.Errorf("Expected paused automated retries notice, got: %s", msgCircuit)
	}
}
