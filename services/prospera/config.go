package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Config encapsulates configuration settings for services/prospera.
type Config struct {
	Port                  string
	DatabaseURL           string
	TwilioAccountSID      string
	TwilioAuthToken       string
	TwilioFromNumber      string
	PublicWebhookBaseURL  string
	GeminiAPIKey          string
	GeminiLiveModel       string
	GeminiFallbackModel   string
	BrainURL              string
	GoogleMCPURL          string
	CovenURL              string
	WhisperURL            string
	DefaultDiscordChannel string
	CallbackWindowMinutes int
	MaxConcurrentCalls    int
	AllowFrom             []string
}

var e164Regex = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

// NormalizeE164 normalizes a raw phone number into strict ITU-T E.164 format (+[1-9][0-9]{1,14}).
func NormalizeE164(raw string) (string, error) {
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\r', '\n', '-', '.', '(', ')', '/', '\\':
			return -1
		default:
			return r
		}
	}, raw)

	if cleaned == "" {
		return "", fmt.Errorf("empty phone number")
	}

	if strings.HasPrefix(cleaned, "+") {
		if !e164Regex.MatchString(cleaned) {
			return "", fmt.Errorf("invalid E.164 phone number: %q", raw)
		}
		return cleaned, nil
	}

	// 11-digit US number starting with 1
	if len(cleaned) == 11 && strings.HasPrefix(cleaned, "1") {
		return "+" + cleaned, nil
	}

	// 10-digit standard US number without country code
	if len(cleaned) == 10 && cleaned[0] >= '2' && cleaned[0] <= '9' {
		return "+1" + cleaned, nil
	}

	return "", fmt.Errorf("invalid E.164 phone number: %q", raw)
}

func parsePositiveInt(val string, defaultVal int, name string) (int, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return defaultVal, nil
	}
	n, err := strconv.Atoi(trimmed)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("config: invalid %s %q: must be positive integer", name, trimmed)
	}
	return n, nil
}

func parseFromNumber(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	normFrom, err := NormalizeE164(trimmed)
	if err != nil {
		return "", fmt.Errorf("config: invalid TWILIO_FROM_NUMBER: %w", err)
	}
	return normFrom, nil
}

func parseAllowFrom(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return []string{}, nil
	}
	parts := strings.Split(trimmed, ",")
	var allowed []string
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item == "" {
			continue
		}
		norm, err := NormalizeE164(item)
		if err != nil {
			return nil, fmt.Errorf("config: invalid ALLOW_FROM number %q: %w", item, err)
		}
		allowed = append(allowed, norm)
	}
	return allowed, nil
}

// LoadConfig loads and validates configuration from environment variables.
func LoadConfig(getenv func(string) string) (*Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}

	cfg := &Config{
		Port:                  getenv("PORT"),
		DatabaseURL:           strings.TrimSpace(getenv("DATABASE_URL")),
		TwilioAccountSID:      strings.TrimSpace(getenv("TWILIO_ACCOUNT_SID")),
		TwilioAuthToken:       strings.TrimSpace(getenv("TWILIO_AUTH_TOKEN")),
		PublicWebhookBaseURL:  strings.TrimSpace(getenv("PUBLIC_WEBHOOK_BASE_URL")),
		GeminiAPIKey:          strings.TrimSpace(getenv("GEMINI_API_KEY")),
		GeminiLiveModel:       strings.TrimSpace(getenv("GEMINI_LIVE_MODEL")),
		GeminiFallbackModel:   strings.TrimSpace(getenv("GEMINI_FALLBACK_MODEL")),
		BrainURL:              strings.TrimSpace(getenv("BRAIN_URL")),
		GoogleMCPURL:          strings.TrimSpace(getenv("GOOGLE_MCP_URL")),
		CovenURL:              strings.TrimSpace(getenv("COVEN_URL")),
		WhisperURL:            strings.TrimSpace(getenv("WHISPER_URL")),
		DefaultDiscordChannel: strings.TrimSpace(getenv("DEFAULT_DISCORD_CHANNEL")),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("config: DATABASE_URL is required")
	}

	if cfg.Port == "" {
		cfg.Port = "4019"
	}
	if cfg.PublicWebhookBaseURL == "" {
		cfg.PublicWebhookBaseURL = "https://webhooks.zylman.com"
	}
	if cfg.GeminiAPIKey == "" {
		cfg.GeminiAPIKey = strings.TrimSpace(getenv("GEMINI_HARNESS_API_KEY"))
	}
	if cfg.GeminiLiveModel == "" {
		cfg.GeminiLiveModel = "gemini-3.8-live"
	}
	if cfg.GeminiFallbackModel == "" {
		cfg.GeminiFallbackModel = "gemini-3.1-flash-live-preview"
	}
	if cfg.BrainURL == "" {
		cfg.BrainURL = "http://eri.aerial"
	}
	if cfg.GoogleMCPURL == "" {
		cfg.GoogleMCPURL = "http://google-mcp.aerial/mcp"
	}
	if cfg.CovenURL == "" {
		cfg.CovenURL = "http://coven.aerial"
	}
	if cfg.WhisperURL == "" {
		cfg.WhisperURL = "http://orin-voice.aerial"
	}
	if cfg.DefaultDiscordChannel == "" {
		cfg.DefaultDiscordChannel = strings.TrimSpace(getenv("DISCORD_CHANNEL_ID"))
		if cfg.DefaultDiscordChannel == "" {
			cfg.DefaultDiscordChannel = "prospera"
		}
	}

	fromNum, err := parseFromNumber(getenv("TWILIO_FROM_NUMBER"))
	if err != nil {
		return nil, err
	}
	cfg.TwilioFromNumber = fromNum

	cbWindow, err := parsePositiveInt(getenv("CALLBACK_WINDOW_MINUTES"), 10080, "CALLBACK_WINDOW_MINUTES")
	if err != nil {
		return nil, err
	}
	cfg.CallbackWindowMinutes = cbWindow

	maxConc, err := parsePositiveInt(getenv("MAX_CONCURRENT_CALLS"), 2, "MAX_CONCURRENT_CALLS")
	if err != nil {
		return nil, err
	}
	cfg.MaxConcurrentCalls = maxConc

	allowList, err := parseAllowFrom(getenv("ALLOW_FROM"))
	if err != nil {
		return nil, err
	}
	cfg.AllowFrom = allowList

	return cfg, nil
}
