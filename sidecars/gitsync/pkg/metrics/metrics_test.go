package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsRegistryAndHandler(t *testing.T) {
	RecordPull("/share/aerial", "success", true, 1200*time.Millisecond)
	RecordPull("/share/aerial-config", "error", false, 500*time.Millisecond)
	RecordSyncRequest("periodic", "synced")
	RecordSyncRequest("webhook", "error")
	RecordReconciliation("success", 3500*time.Millisecond)
	RecordLastSync("/share/aerial", time.Now())

	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	expectedMetrics := []string{
		"aerial_gitsync_pulls_total",
		"aerial_gitsync_pull_duration_seconds",
		"aerial_gitsync_sync_requests_total",
		"aerial_gitsync_reconciliations_total",
		"aerial_gitsync_compose_duration_seconds",
		"aerial_gitsync_last_sync_timestamp_seconds",
		"aerial_gitsync_build_info",
	}

	for _, m := range expectedMetrics {
		if !strings.Contains(body, m) {
			t.Errorf("expected metrics output to contain %q, but it was missing", m)
		}
	}
}

func TestNormalizeRepoName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/share/aerial", "aerial"},
		{"/share/aerial-config", "aerial-config"},
		{"/share/aerial-config/", "aerial-config"},
		{"aerial", "aerial"},
		{"", "unknown"},
		{"/", "unknown"},
	}

	for _, tt := range tests {
		got := NormalizeRepoName(tt.input)
		if got != tt.expected {
			t.Errorf("NormalizeRepoName(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}

func TestSanitizeStatus(t *testing.T) {
	tests := []struct {
		err      error
		expected string
	}{
		{nil, "success"},
		{errors.New("context deadline exceeded"), "timeout"},
		{errors.New("compose validation failed: bad yaml"), "validation_failed"},
		{errors.New("index.lock active"), "lock_active"},
		{errors.New("git dir not found: /repo/.git"), "not_found"},
		{errors.New("reset failed: fatal"), "git_error"},
		{errors.New("compose up failed: exit status 1"), "apply_failed"},
		{errors.New("unrecognized socket glitch"), "error"},
	}

	for _, tt := range tests {
		got := SanitizeStatus(tt.err)
		if got != tt.expected {
			t.Errorf("SanitizeStatus(%v) = %q; want %q", tt.err, got, tt.expected)
		}
	}
}
