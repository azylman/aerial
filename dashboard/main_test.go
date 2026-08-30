package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

func TestFactsHandler_Success(t *testing.T) {
	// Mock brain upstream server
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/facts" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("limit") != "25" || q.Get("category") != "user_preference" {
			t.Errorf("unexpected upstream query: %s", r.URL.RawQuery)
		}
		resp := FactsAPIResponse{
			Facts: []FactItem{
				{
					ID:         1,
					Category:   "user_preference",
					FactText:   "Arcane likes tea",
					Importance: 0.9,
					ThreadID:   "thread-123",
					CreatedAt:  time.Now().UTC(),
				},
			},
			Total:  1,
			Limit:  25,
			Offset: 0,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer mockBrain.Close()

	handler := factsHandler(mockBrain.URL)
	req := httptest.NewRequest("GET", "/api/facts?limit=25&category=user_preference", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var data FactsAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(data.Facts) != 1 || data.Facts[0].FactText != "Arcane likes tea" {
		t.Errorf("unexpected facts in response: %+v", data.Facts)
	}
}

func TestFactsHandler_DegradedFallback(t *testing.T) {
	// Offline / unreachable brain upstream
	handler := factsHandler("http://127.0.0.1:54321")
	req := httptest.NewRequest("GET", "/api/facts", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on brain offline, got %d", rr.Code)
	}

	var data FactsAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode degraded response: %v", err)
	}

	if data.Status != "degraded" {
		t.Errorf("expected status 'degraded', got %s", data.Status)
	}
	if data.Facts == nil {
		t.Errorf("expected non-nil empty facts array")
	}
}
