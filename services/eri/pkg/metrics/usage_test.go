package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const claudeUsageFixture = `{"five_hour":{"utilization":56.0,"resets_at":"2026-11-09T19:59:59.859729+00:00"},"seven_day":{"utilization":19.0,"resets_at":"2026-11-14T02:59:59.859753+00:00"},"seven_day_opus":null,"seven_day_sonnet":null,"extra_usage":{"is_enabled":false}}`

func TestClaudeUsageToGroups(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, payload string
		want          []QuotaGroup
		wantErr       bool
	}{
		{name: "live fixture", payload: claudeUsageFixture, want: []QuotaGroup{{Name: "Claude", Buckets: []QuotaBucket{
			{ID: "claude-five-hour", Window: "5h", ResetTime: "2026-11-09T19:59:59Z", RemainingFraction: 0.44},
			{ID: "claude-seven-day", Window: "weekly", ResetTime: "2026-11-14T02:59:59Z", RemainingFraction: 0.81},
		}}}},
		{name: "all windows with clamping and bad reset", payload: `{"five_hour":{"utilization":120,"resets_at":"2026-11-09T12:00:00-08:00"},"seven_day":{"utilization":-5,"resets_at":"bogus"},"seven_day_opus":{"utilization":50,"resets_at":null},"seven_day_sonnet":{"utilization":null,"resets_at":"2026-11-14T02:59:59Z"}}`, want: []QuotaGroup{{Name: "Claude", Buckets: []QuotaBucket{
			{ID: "claude-five-hour", Window: "5h", ResetTime: "2026-11-09T20:00:00Z", RemainingFraction: 0},
			{ID: "claude-seven-day", Window: "weekly", ResetTime: "", RemainingFraction: 1},
			{ID: "claude-seven-day-opus", Window: "weekly-opus", ResetTime: "", RemainingFraction: 0.5},
		}}}},
		{name: "sonnet only", payload: `{"seven_day_sonnet":{"utilization":25,"resets_at":"2026-11-14T02:59:59Z"}}`, want: []QuotaGroup{{Name: "Claude", Buckets: []QuotaBucket{
			{ID: "claude-seven-day-sonnet", Window: "weekly-sonnet", ResetTime: "2026-11-14T02:59:59Z", RemainingFraction: 0.75},
		}}}},
		{name: "all null", payload: `{"five_hour":null,"seven_day":null,"seven_day_opus":null,"seven_day_sonnet":null}`, wantErr: true},
		{name: "malformed", payload: `{bad`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ClaudeUsageToGroups([]byte(tc.payload))
			if (err != nil) != tc.wantErr {
				t.Fatalf("ClaudeUsageToGroups() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ClaudeUsageToGroups() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestRecordClaudeUsageJSON(t *testing.T) {
	tests := []struct {
		name, payload, wantCheck string
		wantErr                  bool
	}{
		{"fixture", claudeUsageFixture, `aerial_brain_usage_quota_remaining_fraction{bucket_id="claude-five-hour",disabled="false",group="Claude",window="5h"} 0.44`, false},
		{"reset gauge", claudeUsageFixture, `aerial_brain_usage_quota_reset_timestamp_seconds{bucket_id="claude-five-hour",group="Claude",window="5h"} 1.794254399e+09`, false},
		{"all null", `{"five_hour":null}`, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := RecordClaudeUsageJSON([]byte(tc.payload)); (err != nil) != tc.wantErr {
				t.Fatalf("RecordClaudeUsageJSON() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantCheck == "" {
				return
			}
			rec := httptest.NewRecorder()
			Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if !strings.Contains(rec.Body.String(), tc.wantCheck) {
				t.Errorf("missing %q in:\n%s", tc.wantCheck, rec.Body.String())
			}
		})
	}
}

func TestNewClaudeUsageRunner(t *testing.T) {
	t.Parallel()
	const token = "sk-ant-oat01-secret-token"
	tests := []struct {
		name     string
		status   int
		body     string
		wantBody string
		wantErr  string
	}{
		{name: "ok", status: http.StatusOK, body: claudeUsageFixture, wantBody: claudeUsageFixture},
		{name: "forbidden", status: http.StatusForbidden, body: `{"error":"bad token ` + token + `"}`, wantErr: "claude usage endpoint returned HTTP 403"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			seen := make(chan http.Header, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/oauth/usage" || r.Method != http.MethodGet {
					http.NotFound(w, r)
					return
				}
				seen <- r.Header.Clone()
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			out, err := NewClaudeUsageRunner(srv.Client(), srv.URL, token)(context.Background())
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr || strings.Contains(err.Error(), token) {
					t.Fatalf("error = %v, want %q without token", err, tc.wantErr)
				}
			} else if err != nil || string(out) != tc.wantBody {
				t.Fatalf("got (%q, %v), want %q", out, err, tc.wantBody)
			}
			h := <-seen
			if h.Get("Authorization") != "Bearer "+token || h.Get("Anthropic-Beta") != "oauth-2025-04-20" {
				t.Errorf("unexpected headers: Authorization=%q anthropic-beta=%q", h.Get("Authorization"), h.Get("Anthropic-Beta"))
			}
		})
	}
}

func TestNewClaudeUsageRunnerTransportError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	tests := []struct{ name, baseURL string }{
		{"closed server", srv.URL},
		{"invalid url", "http://bad host"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewClaudeUsageRunner(srv.Client(), tc.baseURL, "tok")(context.Background()); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDefaultClaudeUsageRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, token string
		isTest      bool
		wantNil     bool
	}{
		{"test mode", "tok", true, true},
		{"blank token", "  ", false, true},
		{"enabled", "tok", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := DefaultClaudeUsageRunner(tc.token, tc.isTest); (got == nil) != tc.wantNil {
				t.Errorf("DefaultClaudeUsageRunner(%q, %v) nil = %v, want %v", tc.token, tc.isTest, got == nil, tc.wantNil)
			}
		})
	}
}

func TestRecordUsageJSON(t *testing.T) {
	validPayload := `{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[` +
		`{"name":"Gemini Models","buckets":[{"id":"gemini-weekly","window":"weekly","remaining_fraction":0.25,"reset_time":"2026-10-20T02:57:50Z"},{"id":"gemini-5h","window":"5h","disabled":true,"remaining_fraction":0.5}]},` +
		`{"name":"Claude and GPT models","buckets":[{"id":"","window":"weekly","remaining_fraction":1.0,"reset_time":"invalid-time"}]}]}}}`
	tests := []struct {
		name, payload, wantCheck string
		wantErr                  bool
	}{
		{"valid", validPayload, `aerial_brain_usage_quota_remaining_fraction{bucket_id="gemini-weekly",disabled="false",group="Gemini Models",window="weekly"} 0.25`, false},
		{"malformed", `{bad`, "", true},
		{"empty", `{"status":"SUCCESS","command":{"data":{"groups":[]}}}`, "", true},
	}
	for _, tc := range tests {
		if err := RecordUsageJSON([]byte(tc.payload)); (err != nil) != tc.wantErr {
			t.Fatalf("%s: RecordUsageJSON() error = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
		if tc.wantCheck != "" {
			rec := httptest.NewRecorder()
			Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if !strings.Contains(rec.Body.String(), tc.wantCheck) {
				t.Errorf("%s: missing %q in:\n%s", tc.name, tc.wantCheck, rec.Body.String())
			}
			if len(LatestQuotaGroups()) == 0 {
				t.Errorf("%s: expected LatestQuotaGroups() to be populated", tc.name)
			}
			if LatestQuotaUpdatedAt().IsZero() {
				t.Errorf("%s: expected LatestQuotaUpdatedAt() to be populated", tc.name)
			}
		}
	}
}

func TestStartUsagePollerAndRunner(t *testing.T) {
	StartUsagePoller(context.Background(), 0, nil, RecordUsageJSON)
	StartUsagePoller(context.Background(), time.Second, func(context.Context) ([]byte, error) { return nil, nil }, nil)
	dir := t.TempDir()
	script := filepath.Join(dir, "mock-agy.sh")
	payload := `{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"G","buckets":[{"id":"b1","window":"5h","remaining_fraction":0.75}]}]}}}`
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho '"+payload+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := NewAgyUsageRunner(script, dir, []string{"HOME=/old", "FOO=bar"})(context.Background()); err != nil || !strings.Contains(string(out), `"b1"`) {
		t.Fatalf("NewAgyUsageRunner failed: err=%v, out=%s", err, string(out))
	}
	if DefaultUsageRunner("", "", nil, true) != nil || DefaultUsageRunner("", "", nil, false) == nil {
		t.Fatal("unexpected DefaultUsageRunner test airgapping behavior")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan int, 4)
	count := 0
	StartUsagePoller(ctx, 5*time.Millisecond, func(ctx context.Context) ([]byte, error) {
		count++
		calls <- count
		switch count {
		case 1:
			return nil, os.ErrInvalid
		case 2:
			return []byte(`{bad`), nil
		}
		cancel()
		return []byte(payload), nil
	}, RecordUsageJSON)
	for range 3 {
		<-calls
	}
}
