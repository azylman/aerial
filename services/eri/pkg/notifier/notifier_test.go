package notifier

import (
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

	// Capacity
	msgCapacity := StaticFallback("resource_exhausted: You have exhausted your capacity on this model")
	if !strings.Contains(msgCapacity, "Model capacity temporarily reached") {
		t.Errorf("Expected capacity fallback message, got: %q", msgCapacity)
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

	// Auth failures (including "authentication failed or timed out" from agy)
	msgAuthTimedOut := StaticFallback("authentication failed or timed out")
	if !strings.Contains(msgAuthTimedOut, "Authentication failed") {
		t.Errorf("Expected auth failure message for 'authentication failed or timed out', got: %q", msgAuthTimedOut)
	}
	if strings.Contains(msgAuthTimedOut, "timed out") {
		t.Errorf("Did not expect timeout message for auth failure, got: %q", msgAuthTimedOut)
	}

	msgAuthKey := StaticFallback("execution failed with non-transient error: invalid api key")
	if !strings.Contains(msgAuthKey, "Authentication failed") {
		t.Errorf("Expected auth failure message for invalid api key, got: %q", msgAuthKey)
	}

	// Exhausted retries
	msgExhausted := StaticFallback("Execution failed after exhausting all retry attempts")
	if !strings.Contains(msgExhausted, "exhausting retry attempts") {
		t.Errorf("Expected exhausted fallback message, got: %q", msgExhausted)
	}

	// General error
	msgGeneral := StaticFallback("Fatal unknown error")
	if !strings.Contains(msgGeneral, "unexpected error occurred") {
		t.Errorf("Expected general fallback message, got: %q", msgGeneral)
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

	// Capacity Pause (resetDur <= 60s): Scheduled = true
	durCap := 30 * time.Second
	msgCapScheduled := FormatQuotaPauseMessage(durCap, runAt, true, false)
	if !strings.Contains(msgCapScheduled, "Model capacity temporarily reached") {
		t.Errorf("Expected capacity message, got: %s", msgCapScheduled)
	}
	if !strings.Contains(msgCapScheduled, "Capacity clears in **30s**") {
		t.Errorf("Expected 'Capacity clears in **30s**', got: %s", msgCapScheduled)
	}
	if !strings.Contains(msgCapScheduled, "<t:1725678900:R>") {
		t.Errorf("Expected live Discord countdown tag, got: %s", msgCapScheduled)
	}
	if strings.Contains(msgCapScheduled, "Personal subscription") {
		t.Errorf("Did not expect personal subscription mention in capacity message, got: %s", msgCapScheduled)
	}

	// Capacity Pause: Scheduled = false (unscheduled)
	msgCapUnscheduled := FormatQuotaPauseMessage(durCap, runAt, false, false)
	if !strings.Contains(msgCapUnscheduled, "Model capacity temporarily reached") {
		t.Errorf("Expected capacity message, got: %s", msgCapUnscheduled)
	}
	if !strings.Contains(msgCapUnscheduled, "Please try again in a moment") {
		t.Errorf("Expected 'Please try again in a moment', got: %s", msgCapUnscheduled)
	}

	// Capacity Pause: Circuit breaker = true
	msgCapCircuit := FormatQuotaPauseMessage(durCap, runAt, false, true)
	if !strings.Contains(msgCapCircuit, "Model capacity temporarily reached again following a scheduled retry") {
		t.Errorf("Expected capacity circuit breaker notice, got: %s", msgCapCircuit)
	}
	if strings.Contains(msgCapCircuit, "GEMINI_API_KEY") {
		t.Errorf("Did not expect GEMINI_API_KEY advice in capacity circuit breaker, got: %s", msgCapCircuit)
	}

	// Zero timestamp (runAt.IsZero())
	msgZeroTime := FormatQuotaPauseMessage(durCap, time.Time{}, true, false)
	if strings.Contains(msgZeroTime, "<t:") {
		t.Errorf("Did not expect timestamp tag when runAt is zero, got: %s", msgZeroTime)
	}
}
