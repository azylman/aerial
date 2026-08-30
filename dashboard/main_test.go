package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSanitizeEnvVars(t *testing.T) {
	input := []string{
		"GEMINI_API_KEY=secret_key_123",
		"DISCORD_TOKEN=bot_token_456",
		"DISCORD_BOT_TOKEN=bot_token_789",
		"GITHUB_PAT=pat_xyz",
		"HA_TOKEN=ha_abc",
		"PORT=8080",
		"AGY_MODEL=Gemini 3.6 Flash",
	}

	sanitized := SanitizeEnvVars(input)

	for _, env := range sanitized {
		if env == "GEMINI_API_KEY=secret_key_123" ||
			env == "DISCORD_TOKEN=bot_token_456" ||
			env == "DISCORD_BOT_TOKEN=bot_token_789" ||
			env == "GITHUB_PAT=pat_xyz" ||
			env == "HA_TOKEN=ha_abc" {
			t.Errorf("found unsanitized secret in output: %s", env)
		}
	}
}

func TestStatusHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/status", nil)
	rr := httptest.NewRecorder()

	statusHandler(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}

	if len(resp.Services) == 0 {
		t.Errorf("expected services in response, got 0")
	}

	for _, svc := range resp.Services {
		if svc.UptimeSeconds < 0 {
			t.Errorf("service %s has negative uptime: %d", svc.Name, svc.UptimeSeconds)
		}
	}
}
