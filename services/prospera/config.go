package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/prospera.yaml"

var localConfigPath = "/local/config.yaml"

// ResolveConfigPath returns the config file path, checking CONFIG_PATH env,
// then /local/config.yaml (if size > 0), falling back to fallbackPath.
func ResolveConfigPath(fallbackPath string) string {
	if fallbackPath != "" {
		return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, fallbackPath)
	}
	return httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", localConfigPath, DefaultConfigPath, "config.yaml")
}

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
	GeminiVoiceName       string
	BrainURL              string
	EriURL                string
	GoogleMCPURL          string
	CovenURL              string
	WhisperURL            string
	DefaultDiscordChannel string
	CallbackWindowMinutes int
	MaxConcurrentCalls    int
	AllowFrom             []string
}

// ProsperaYAMLConfig defines the declarative file schema for prospera.yaml.
type ProsperaYAMLConfig struct {
	Port                  string `yaml:"port"`
	EriURL                string `yaml:"eri_url"`
	GoogleMCPURL          string `yaml:"google_mcp_url"`
	CovenURL              string `yaml:"coven_url"`
	WhisperURL            string `yaml:"whisper_url"`
	DefaultDiscordChannel string `yaml:"default_discord_channel"`
	PublicWebhookBaseURL  string `yaml:"public_webhook_base_url"`
	GeminiLiveModel       string `yaml:"gemini_live_model"`
	GeminiFallbackModel   string `yaml:"gemini_fallback_model"`
	GeminiVoiceName       string `yaml:"gemini_voice_name"`
	CallbackWindowMinutes int    `yaml:"callback_window_minutes"`
	MaxConcurrentCalls    int    `yaml:"max_concurrent_calls"`
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

	var candidate string
	switch {
	case strings.HasPrefix(cleaned, "+"):
		candidate = cleaned
	case len(cleaned) == 11 && strings.HasPrefix(cleaned, "1"):
		candidate = "+" + cleaned
	case len(cleaned) == 10 && cleaned[0] >= '2' && cleaned[0] <= '9':
		candidate = "+1" + cleaned
	default:
		return "", fmt.Errorf("invalid E.164 phone number: %q", raw)
	}

	if !e164Regex.MatchString(candidate) {
		return "", fmt.Errorf("invalid E.164 phone number: %q", raw)
	}
	return candidate, nil
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

// LoadConfig loads and strictly validates configuration from the file at path using os.Getenv.
func LoadConfig(path string) (*Config, error) {
	return LoadConfigFile(path, os.Getenv)
}

func validateProsperaYAML(ycfg ProsperaYAMLConfig, cleanPath string, eriURL string) error {
	if strings.TrimSpace(ycfg.Port) == "" {
		return fmt.Errorf("config: port is required in %s", cleanPath)
	}
	if eriURL == "" {
		return fmt.Errorf("config: eri_url is required in %s", cleanPath)
	}
	if strings.TrimSpace(ycfg.GoogleMCPURL) == "" {
		return fmt.Errorf("config: google_mcp_url is required in %s", cleanPath)
	}
	if strings.TrimSpace(ycfg.CovenURL) == "" {
		return fmt.Errorf("config: coven_url is required in %s", cleanPath)
	}
	if strings.TrimSpace(ycfg.WhisperURL) == "" {
		return fmt.Errorf("config: whisper_url is required in %s", cleanPath)
	}
	if strings.TrimSpace(ycfg.DefaultDiscordChannel) == "" {
		return fmt.Errorf("config: default_discord_channel is required in %s", cleanPath)
	}
	if strings.TrimSpace(ycfg.GeminiLiveModel) == "" {
		return fmt.Errorf("config: gemini_live_model is required in %s", cleanPath)
	}
	if strings.TrimSpace(ycfg.GeminiFallbackModel) == "" {
		return fmt.Errorf("config: gemini_fallback_model is required in %s", cleanPath)
	}
	if strings.TrimSpace(ycfg.GeminiVoiceName) == "" {
		return fmt.Errorf("config: gemini_voice_name is required in %s", cleanPath)
	}
	if ycfg.CallbackWindowMinutes <= 0 {
		return fmt.Errorf("config: callback_window_minutes must be positive in %s", cleanPath)
	}
	if ycfg.MaxConcurrentCalls <= 0 {
		return fmt.Errorf("config: max_concurrent_calls must be positive in %s", cleanPath)
	}
	return nil
}

// LoadConfigFile loads and strictly validates configuration from the YAML file at path and secrets from getenv.
func LoadConfigFile(path string, getenv func(string) string) (*Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}

	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return nil, fmt.Errorf("config: path cannot be empty")
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read %s: %w", cleanPath, err)
	}

	var ycfg ProsperaYAMLConfig
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	eriURL := strings.TrimSpace(ycfg.EriURL)
	if eriURL == "" {
		eriURL = strings.TrimSpace(getenv("ERI_URL"))
	}

	if err := validateProsperaYAML(ycfg, cleanPath, eriURL); err != nil {
		return nil, err
	}

	publicWebhookBaseURL := strings.TrimRight(strings.TrimSpace(ycfg.PublicWebhookBaseURL), "/")

	dbURL := strings.TrimSpace(getenv("DATABASE_URL"))
	if dbURL == "" {
		return nil, fmt.Errorf("config: DATABASE_URL is required")
	}

	geminiKey := strings.TrimSpace(getenv("GEMINI_HARNESS_API_KEY"))

	fromNum, err := parseFromNumber(getenv("TWILIO_FROM_NUMBER"))
	if err != nil {
		return nil, err
	}

	allowList, err := parseAllowFrom(getenv("ALLOW_FROM"))
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:                  strings.TrimSpace(ycfg.Port),
		DatabaseURL:           dbURL,
		TwilioAccountSID:      strings.TrimSpace(getenv("TWILIO_ACCOUNT_SID")),
		TwilioAuthToken:       strings.TrimSpace(getenv("TWILIO_AUTH_TOKEN")),
		TwilioFromNumber:      fromNum,
		PublicWebhookBaseURL:  publicWebhookBaseURL,
		GeminiAPIKey:          geminiKey,
		GeminiLiveModel:       strings.TrimSpace(ycfg.GeminiLiveModel),
		GeminiFallbackModel:   strings.TrimSpace(ycfg.GeminiFallbackModel),
		GeminiVoiceName:       strings.TrimSpace(ycfg.GeminiVoiceName),
		BrainURL:              eriURL,
		EriURL:                eriURL,
		GoogleMCPURL:          strings.TrimSpace(ycfg.GoogleMCPURL),
		CovenURL:              strings.TrimSpace(ycfg.CovenURL),
		WhisperURL:            strings.TrimSpace(ycfg.WhisperURL),
		DefaultDiscordChannel: strings.TrimSpace(ycfg.DefaultDiscordChannel),
		CallbackWindowMinutes: ycfg.CallbackWindowMinutes,
		MaxConcurrentCalls:    ycfg.MaxConcurrentCalls,
		AllowFrom:             allowList,
	}, nil
}
