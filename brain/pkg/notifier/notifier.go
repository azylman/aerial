package notifier

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

// ModelUnavailableMessage returns a bare, factual message for 503/429 rate limit or outage errors.
func ModelUnavailableMessage() string {
	return "Apologies, the AI model is currently unavailable or being rate limited. Please try again in a few moments."
}

// StaticFallback returns a persona-compliant default notification based on the error context.
func StaticFallback(contextDescription string) string {
	lower := strings.ToLower(contextDescription)
	if strings.Contains(lower, "503") || strings.Contains(lower, "unavailable") || strings.Contains(lower, "high demand") || strings.Contains(lower, "rate limit") {
		return ModelUnavailableMessage()
	}
	if strings.Contains(lower, "poison") || strings.Contains(lower, "crash") || strings.Contains(lower, "dropped") {
		return "I'm so sorry, darling! ✨ Your message caused repeated crashes and had to be skipped to restore normal operation. Please try rephrasing your request! 🌸"
	}
	if strings.Contains(lower, "reset") || strings.Contains(lower, "corrupt") || strings.Contains(lower, "session") {
		return "I ran into an issue with our previous session context, so I've refreshed our conversation! ✨ Please try sending your message again! 🌸"
	}
	return "I'm so sorry, darling! ✨ I ran into a temporary hiccup with the AI service. Please try sending your message again in just a moment! 🌸"
}

// GenerateSessionResetMessage uses a lightweight agy call to synthesize a persona-aligned reset notice, with static fallback.
func GenerateSessionResetMessage(agyBin, apiKey string) string {
	return GenerateDynamicNotification(agyBin, apiKey, "session reset due to context corruption")
}

// GeneratePoisonPillMessage uses a lightweight agy call to synthesize a notice explaining the message caused repeated crashes and had to be dropped, with static fallback.
func GeneratePoisonPillMessage(agyBin, apiKey, promptSnippet string) string {
	desc := "a message caused repeated crashes and had to be dropped"
	if strings.TrimSpace(promptSnippet) != "" {
		desc = fmt.Sprintf("a message caused repeated crashes and had to be dropped (message snippet: %q)", promptSnippet)
	}
	return GenerateDynamicNotification(agyBin, apiKey, desc)
}

// GenerateDynamicNotification attempts to generate a persona-compliant message using a lightweight agy call,
// falling back to static predefined persona messages on error or timeout.
func GenerateDynamicNotification(agyBin, apiKey, contextDescription string) string {
	fallback := StaticFallback(contextDescription)
	if agyBin == "" || apiKey == "" {
		return fallback
	}

	prompt := fmt.Sprintf("You are Aerial. Generate a single, short, warm, and friendly Discord notification message (1-2 sentences with sparkle emojis ✨🌸) explaining the following situation to the user:\nSituation: %s\nOutput ONLY the final message text without markdown fences or quotes.", contextDescription)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stdout, stderr, exitCode, err := runner.RunAgy(ctx, agyBin, prompt, "", apiKey, "", 1)
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

	return result
}
