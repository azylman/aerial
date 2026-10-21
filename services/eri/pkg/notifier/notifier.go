package notifier

import (
	"fmt"
	"strings"
	"time"
)

// ModelUnavailableMessage returns a bare, factual message for 503/429 rate limit or outage errors.
func ModelUnavailableMessage() string {
	return "The AI model is currently unavailable or rate-limited. Please try again in a few moments."
}

// FormatDurationHuman renders durations in a friendly, conversational format.
func FormatDurationHuman(d time.Duration) string {
	if d <= 0 {
		return "a few moments"
	}
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 {
		if m > 0 {
			return fmt.Sprintf("%dh %dm", h, m)
		}
		return fmt.Sprintf("%dh", h)
	}
	if m > 0 {
		if s > 0 {
			return fmt.Sprintf("%dm %ds", m, s)
		}
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%ds", s)
}

// FormatQuotaPauseMessage crafts Aerial's neutral quota pause message with live Discord relative timestamp.
func FormatQuotaPauseMessage(resetDur time.Duration, runAt time.Time, scheduled bool, isCircuitBreak bool) string {
	durHuman := FormatDurationHuman(resetDur)
	var countdown string
	if !runAt.IsZero() {
		countdown = fmt.Sprintf(" (<t:%d:R>)", runAt.Unix())
	}

	// Model capacity / rate limit throttles include short countdowns (<= 5s) and 70s bucket rollover fallbacks.
	isCapacity := resetDur <= 90*time.Second

	if isCircuitBreak {
		if isCapacity {
			return "Model capacity temporarily reached again following a scheduled retry. I have paused automated retries on this turn to prevent a retry loop. Please try again in a moment."
		}
		return "Personal subscription quota limit reached again following a scheduled retry. I have paused automated retries on this turn to prevent a retry loop. Add `GEMINI_API_KEY` into your environment to unlock pay-as-you-go access, or try again once quota resets."
	}

	if scheduled {
		if isCapacity {
			return fmt.Sprintf("Model capacity temporarily reached. Capacity clears in **%s**%s. I have automatically scheduled a retry for when capacity clears.", durHuman, countdown)
		}
		return fmt.Sprintf("Personal subscription quota limit reached. Quota resets in **%s**%s. I have automatically scheduled a retry for when quota refreshes. (Alternatively, add `GEMINI_API_KEY` into `.env` to unlock pay-as-you-go access immediately.)", durHuman, countdown)
	}

	if isCapacity {
		return fmt.Sprintf("Model capacity temporarily reached. Capacity clears in **%s**%s. Please try again in a moment.", durHuman, countdown)
	}
	return fmt.Sprintf("Personal subscription quota limit reached. Quota resets in **%s**%s. Please try again once quota refreshes, or add `GEMINI_API_KEY` into `.env` to unlock direct access.", durHuman, countdown)
}

// StaticFallback returns a neutral default notification based on the error context.
func StaticFallback(contextDescription string) string {
	lower := strings.ToLower(contextDescription)
	if strings.Contains(lower, "capacity") {
		return "Model capacity temporarily reached. Please try again in a moment."
	}
	if strings.Contains(lower, "quota") || strings.Contains(lower, "individual quota") || strings.Contains(lower, "resource_exhausted") {
		return "Personal subscription quota limit reached. Please try again once quota refreshes, or add `GEMINI_API_KEY` into `.env` to bypass subscription limits."
	}
	if strings.Contains(lower, "503") || strings.Contains(lower, "unavailable") || strings.Contains(lower, "high demand") || strings.Contains(lower, "rate limit") {
		return ModelUnavailableMessage()
	}
	if strings.Contains(lower, "poison") || strings.Contains(lower, "crash") || strings.Contains(lower, "dropped") {
		return "The message caused repeated process crashes and was skipped to restore normal operation. Please try rephrasing or simplifying the request."
	}
	if strings.Contains(lower, "reset") || strings.Contains(lower, "corrupt") || strings.Contains(lower, "session") {
		return "The conversation context became corrupted and has been reset. Please try sending your message again."
	}
	if strings.Contains(lower, "authentication failed") || strings.Contains(lower, "auth failed") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "invalid api key") || strings.Contains(lower, "invalid_api_key") {
		return "Authentication failed while processing the request. Please check your API credentials or re-authenticate."
	}
	if strings.Contains(lower, "watchdog") || strings.Contains(lower, "inactivity") || strings.Contains(lower, "max duration") || strings.Contains(lower, "timed out") {
		return "Execution timed out while processing the request. Please try again or break the request into smaller steps."
	}
	if strings.Contains(lower, "exhausting") || strings.Contains(lower, "exhausted all") {
		return "Execution failed after exhausting retry attempts. Please check request parameters or try again later."
	}
	return "An unexpected error occurred while processing the request. Please try again."
}
