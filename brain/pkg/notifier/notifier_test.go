package notifier

import (
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
