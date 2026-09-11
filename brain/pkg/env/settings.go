package env

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// SyncSettings writes ~/.gemini/antigravity-cli/settings.json matching the authentication mode.
func (p *Provisioner) SyncSettings(apiKey, model string) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	configDir := filepath.Join(p.homeDir, ".gemini", "antigravity-cli")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	settingsPath := filepath.Join(configDir, "settings.json")
	settings := make(map[string]interface{})

	if data, err := os.ReadFile(settingsPath); err == nil {
		if err := json.Unmarshal(data, &settings); err != nil {
			log.Printf("Corrupted settings.json detected at %s, recreating: %v", settingsPath, err)
			settings = make(map[string]interface{})
		}
	}

	if apiKey == "" {
		// OAuth / ADC mode: modelProvider must NOT be set to "gemini"
		if mp, exists := settings["modelProvider"]; exists {
			log.Printf("Purging modelProvider=%v from %s (OAuth/ADC mode requires omitting modelProvider)", mp, settingsPath)
			delete(settings, "modelProvider")
		}
		delete(settings, "apiKey")
	} else {
		// API Key authentication mode
		settings["apiKey"] = apiKey
		settings["modelProvider"] = "gemini"
	}

	if model != "" {
		settings["model"] = model
	}

	updatedData, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings.json: %w", err)
	}

	return p.writeAtomic(settingsPath, string(updatedData))
}

// EnsureAgySettings is a deprecated no-op helper for callers without an explicit home directory.
func EnsureAgySettings(apiKey, model string) error {
	return EnsureAgySettingsForHome("", apiKey, model)
}

// EnsureAgySettingsForHome configures settings.json under a specific home directory.
func EnsureAgySettingsForHome(homeDir, apiKey, model string) error {
	cleanHome := strings.TrimSpace(homeDir)
	if cleanHome == "" {
		return nil
	}
	return New(cleanHome, "").SyncSettings(apiKey, model)
}
