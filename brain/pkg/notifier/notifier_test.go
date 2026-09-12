package notifier

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestModelUnavailableMessage(t *testing.T) {
	msg := ModelUnavailableMessage()
	expected := "The AI model is currently unavailable or rate-limited. Please try again in a few moments."
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

	// Quota
	msgQuota := StaticFallback("resource_exhausted: quota reached")
	if !strings.Contains(msgQuota, "quota limit reached") {
		t.Errorf("Expected quota fallback message, got: %q", msgQuota)
	}

	// Session reset / corrupt
	msgReset := StaticFallback("Resetting session due to conversation corrupted")
	if !strings.Contains(msgReset, "became corrupted and has been reset") && !strings.Contains(msgReset, "session") {
		t.Errorf("Expected session reset fallback message, got: %q", msgReset)
	}

	// Poison pill
	msgPoison := StaticFallback("poison pill: dropped crashed message")
	if !strings.Contains(msgPoison, "repeated process crashes") {
		t.Errorf("Expected poison pill fallback message, got: %q", msgPoison)
	}

	// Watchdog timeout
	msgWatchdog := StaticFallback("execution terminated by watchdog: inactivity timeout exceeded")
	if !strings.Contains(msgWatchdog, "timed out while processing") {
		t.Errorf("Expected watchdog timeout fallback message, got: %q", msgWatchdog)
	}

	// General error
	msgGeneral := StaticFallback("Fatal unknown error")
	if !strings.Contains(msgGeneral, "unexpected error occurred") {
		t.Errorf("Expected general fallback message, got: %q", msgGeneral)
	}
}

func TestGenerateSessionResetAndPoisonPillFallback(t *testing.T) {
	// Nil runner should immediately return fallback
	resReset := GenerateSessionResetMessage("agy", "")
	if !strings.Contains(resReset, "became corrupted and has been reset") {
		t.Errorf("Expected reset fallback when runner is nil, got: %q", resReset)
	}

	resPoison := GeneratePoisonPillMessage("agy", "", "SELECT * FROM huge_table")
	if !strings.Contains(resPoison, "repeated process crashes") {
		t.Errorf("Expected poison fallback when runner is nil, got: %q", resPoison)
	}

	resPoisonEmpty := GeneratePoisonPillMessage("agy", "", "")
	if !strings.Contains(resPoisonEmpty, "repeated process crashes") {
		t.Errorf("Expected poison fallback when snippet is empty, got: %q", resPoisonEmpty)
	}
}

func TestGenerateDynamicNotificationWithMock(t *testing.T) {
	// Using echo as the agy binary simulator (fails JSON parse and returns fallback)
	res := GenerateDynamicNotification("echo", "test-api-key", "Test description")
	if res == "" {
		t.Error("Expected non-empty dynamic notification from mock")
	}
	if !strings.Contains(res, "unexpected error occurred") {
		t.Errorf("Expected neutral fallback notification, got: %q", res)
	}
}

func TestGenerateDynamicNotification_SuccessfulAgyRun(t *testing.T) {
	mockRunner := func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		return `{"response": "Hey bestie! ✨ Everything is running smoothly now! 🌸"}`, "", 0, nil
	}

	res := GenerateDynamicNotification("agy", "valid_key", "session reset due to context corruption", mockRunner)
	if !strings.Contains(res, "bestie") {
		t.Errorf("Expected dynamic notification from mock agy, got: %q", res)
	}

	// Test with poison pill context description
	resPoison := GenerateDynamicNotification("agy", "valid_key", "a message caused repeated crashes and had to be dropped", mockRunner)
	if !strings.Contains(resPoison, "bestie") {
		t.Errorf("Expected dynamic notification for poison pill, got: %q", resPoison)
	}

	// Test with watchdog timeout context description
	resWatchdog := GenerateDynamicNotification("agy", "valid_key", "execution timed out while working on the request", mockRunner)
	if !strings.Contains(resWatchdog, "bestie") {
		t.Errorf("Expected dynamic notification for watchdog timeout, got: %q", resWatchdog)
	}

	// Test with non-transient execution error context description
	resNonTransient := GenerateDynamicNotification("agy", "valid_key", "execution failed with non-transient error", mockRunner)
	if !strings.Contains(resNonTransient, "bestie") {
		t.Errorf("Expected dynamic notification for non-transient error, got: %q", resNonTransient)
	}

	// Test with 503 outage context description
	res503 := GenerateDynamicNotification("agy", "valid_key", "Error 503: unavailable", mockRunner)
	if !strings.Contains(res503, "bestie") {
		t.Errorf("Expected dynamic notification for 503 outage, got: %q", res503)
	}

	// Test OAuth environment: apiKey is empty, runner still succeeds
	resOAuth := GenerateDynamicNotification("agy", "", "session reset due to context corruption", mockRunner)
	if !strings.Contains(resOAuth, "bestie") {
		t.Errorf("Expected dynamic notification in OAuth mode with empty apiKey, got: %q", resOAuth)
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
	if !strings.Contains(msgScheduled, "automatically scheduled") {
		t.Errorf("Expected scheduled confirmation, got: %s", msgScheduled)
	}
	if !strings.Contains(msgScheduled, "GEMINI_API_KEY") {
		t.Errorf("Expected GEMINI_API_KEY advice, got: %s", msgScheduled)
	}

	// Scheduled = false (DB failure)
	msgNotScheduled := FormatQuotaPauseMessage(dur, runAt, false, false)
	if strings.Contains(msgNotScheduled, "automatically scheduled") {
		t.Errorf("Did not expect scheduled confirmation when scheduled=false, got: %s", msgNotScheduled)
	}
	if !strings.Contains(msgNotScheduled, "try again once quota refreshes") {
		t.Errorf("Expected re-ask prompt, got: %s", msgNotScheduled)
	}

	// Circuit breaker = true
	msgCircuit := FormatQuotaPauseMessage(dur, runAt, false, true)
	if !strings.Contains(msgCircuit, "following a scheduled retry") {
		t.Errorf("Expected circuit breaker notice, got: %s", msgCircuit)
	}
	if !strings.Contains(msgCircuit, "Automated retries have been paused") && !strings.Contains(msgCircuit, "paused automated retries") {
		t.Errorf("Expected paused automated retries notice, got: %s", msgCircuit)
	}
}

func TestGenerateDynamicNotification_FailureCases(t *testing.T) {
	// 1. Runner returns failure output classified by ClassifyError
	mockFailRunner := func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		return "Error: model is overloaded with error 503", "Service Unavailable", 1, nil
	}
	res := GenerateDynamicNotification("agy", "test-key", "some context", mockFailRunner)
	if !strings.Contains(res, "unexpected error occurred") && !strings.Contains(res, "Apologies") {
		t.Errorf("Expected static fallback on failure classification, got %q", res)
	}

	// 2. Runner returns empty response in parsed output
	mockEmptyRunner := func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		return `{"response": "   "}`, "", 0, nil
	}
	resEmpty := GenerateDynamicNotification("agy", "test-key", "some context", mockEmptyRunner)
	if !strings.Contains(resEmpty, "unexpected error occurred") && !strings.Contains(resEmpty, "Apologies") {
		t.Errorf("Expected static fallback on empty response, got %q", resEmpty)
	}

	// 3. Fallback descriptions triggers
	resPoison := GeneratePoisonPillMessage("agy", "test-key", "crash prompt", mockFailRunner)
	if !strings.Contains(resPoison, "repeated process crashes and was skipped") {
		t.Errorf("Expected poison pill fallback, got %q", resPoison)
	}

	resSession := GenerateSessionResetMessage("agy", "test-key", mockFailRunner)
	if !strings.Contains(resSession, "context became corrupted and has been reset") {
		t.Errorf("Expected session reset fallback, got %q", resSession)
	}
}
