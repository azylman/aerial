package notifier

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
)

// ModelUnavailableMessage returns a bare, factual message for 503/429 rate limit or outage errors.
func ModelUnavailableMessage() string {
	return "Apologies, the AI model is currently unavailable or being rate limited. Please try again in a few moments."
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

// FormatQuotaPauseMessage crafts Aerial's signature friendly heads-up message with live Discord relative timestamp.
func FormatQuotaPauseMessage(resetDur time.Duration, runAt time.Time, scheduled bool, isCircuitBreak bool) string {
	durHuman := FormatDurationHuman(resetDur)
	var countdown string
	if !runAt.IsZero() {
		countdown = fmt.Sprintf(" (<t:%d:R>)", runAt.Unix())
	}

	if isCircuitBreak {
		return "I've hit Google's personal subscription quota limit again after a scheduled auto-retry! ✨\n\nTo prevent getting locked in a retry loop, I've paused automated retries on this turn. You can add `GEMINI_API_KEY` into your environment to unlock unlimited pay-as-you-go access, or ping me again once limits have refreshed! 🌸"
	}

	if scheduled {
		return fmt.Sprintf("I've hit Google's personal subscription quota limit. My brain bucket resets in **%s**%s! ✨\n\nI've automatically scheduled a retry for when the quota refreshes, so I'll answer you right then! (Or if you don't want to wait, add `GEMINI_API_KEY` into `.env` to unlock unlimited pay-as-you-go access immediately.) 🌸", durHuman, countdown)
	}

	return fmt.Sprintf("I've hit Google's personal subscription quota limit. My brain bucket resets in **%s**%s! ✨\n\nPlease ping me again once my brain bucket refreshes, or add `GEMINI_API_KEY` into `.env` to unlock unlimited access! 🌸", durHuman, countdown)
}

// StaticFallback returns a persona-compliant default notification based on the error context.
func StaticFallback(contextDescription string) string {
	lower := strings.ToLower(contextDescription)
	if strings.Contains(lower, "quota") || strings.Contains(lower, "individual quota") || strings.Contains(lower, "resource_exhausted") {
		return "I've hit Google's personal subscription quota limit! ✨ My brain bucket is currently cooling down. Please try again in a little bit, or add `GEMINI_API_KEY` into `.env` to bypass subscription limits! 🌸"
	}
	if strings.Contains(lower, "503") || strings.Contains(lower, "unavailable") || strings.Contains(lower, "high demand") || strings.Contains(lower, "rate limit") {
		return ModelUnavailableMessage()
	}
	if strings.Contains(lower, "poison") || strings.Contains(lower, "crash") || strings.Contains(lower, "dropped") {
		return "I'm so sorry, darling! ✨ Your message caused repeated crashes and had to be skipped to restore normal operation. Please try rephrasing your request! 🌸"
	}
	if strings.Contains(lower, "reset") || strings.Contains(lower, "corrupt") || strings.Contains(lower, "session") {
		return "I ran into an issue with our previous session context, so I've refreshed our conversation! ✨ Please try sending your message again! 🌸"
	}
	if strings.Contains(lower, "watchdog") || strings.Contains(lower, "inactivity") || strings.Contains(lower, "max duration") {
		return "I'm so sorry, darling! ✨ My execution timed out while working on your request. Please try again or break your request into smaller steps! 🌸"
	}
	return "I'm so sorry, darling! ✨ I ran into a temporary hiccup with the AI service. Please try sending your message again in just a moment! 🌸"
}

// GenerateSessionResetMessage uses a lightweight agy call to synthesize a persona-aligned reset notice, with static fallback.
func GenerateSessionResetMessage(agyBin, apiKey string, runnerFns ...runner.RunnerFunc) string {
	return GenerateDynamicNotification(agyBin, apiKey, "session reset due to context corruption", runnerFns...)
}

// GeneratePoisonPillMessage uses a lightweight agy call to synthesize a notice explaining the message caused repeated crashes and had to be dropped, with static fallback.
func GeneratePoisonPillMessage(agyBin, apiKey, promptSnippet string, runnerFns ...runner.RunnerFunc) string {
	desc := "a message caused repeated crashes and had to be dropped"
	if strings.TrimSpace(promptSnippet) != "" {
		desc = fmt.Sprintf("a message caused repeated crashes and had to be dropped (message snippet: %q)", promptSnippet)
	}
	return GenerateDynamicNotification(agyBin, apiKey, desc, runnerFns...)
}

// GenerateDynamicNotification attempts to generate a persona-compliant message using a lightweight agy call,
// falling back to static predefined persona messages on error, timeout, or if no runner function is provided.
func GenerateDynamicNotification(agyBin, apiKey, contextDescription string, runnerFns ...runner.RunnerFunc) string {
	start := time.Now()
	trigger := "error"
	lowerDesc := strings.ToLower(contextDescription)
	if strings.Contains(lowerDesc, "session reset") {
		trigger = "session_reset"
	} else if strings.Contains(lowerDesc, "poison") || strings.Contains(lowerDesc, "dropped") {
		trigger = "poison_pill"
	} else if strings.Contains(lowerDesc, "503") || strings.Contains(lowerDesc, "unavailable") {
		trigger = "outage"
	}

	outcome := "static_fallback"
	defer func() {
		metrics.RecordFallbackNotification(trigger, outcome, time.Since(start))
	}()

	fallback := StaticFallback(contextDescription)
	var runnerFn runner.RunnerFunc
	if len(runnerFns) > 0 {
		runnerFn = runnerFns[0]
	}
	if runnerFn == nil || agyBin == "" || apiKey == "" {
		return fallback
	}

	prompt := fmt.Sprintf("You are Aerial. Generate a single, short, warm, and friendly Discord notification message (1-2 sentences with sparkle emojis ✨🌸) explaining the following situation to the user:\nSituation: %s\nOutput ONLY the final message text without markdown fences or quotes.", contextDescription)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stdout, stderr, exitCode, err := runnerFn(ctx, agyBin, prompt, "", apiKey, "", 1)
	if err != nil || exitCode != 0 {
		return fallback
	}

	isFailure, _, _, _ := runner.ClassifyError(exitCode, stdout, stderr)
	if isFailure {
		return fallback
	}

	resp, parseErr := runner.ParseAgyOutput(stdout)
	if parseErr != nil {
		return fallback
	}

	result := strings.TrimSpace(resp.Response)
	result = strings.Trim(result, "\"'\n\r ")
	if result == "" {
		return fallback
	}

	outcome = "dynamic"
	return result
}
