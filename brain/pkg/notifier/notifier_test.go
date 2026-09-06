package notifier

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
