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
			{ID: "claude-seven-day", Window: "weekly", ResetTime: "2026-11-14T02:59:59Z", RemainingFraction: 0.81},
			{ID: "claude-five-hour", Window: "5h", ResetTime: "2026-11-09T19:59:59Z", RemainingFraction: 0.44},
		}}}},
		{name: "all windows with clamping and bad reset", payload: `{"five_hour":{"utilization":120,"resets_at":"2026-11-09T12:00:00-08:00"},"seven_day":{"utilization":-5,"resets_at":"bogus"},"seven_day_opus":{"utilization":50,"resets_at":null},"seven_day_sonnet":{"utilization":null,"resets_at":"2026-11-14T02:59:59Z"}}`, want: []QuotaGroup{{Name: "Claude", Buckets: []QuotaBucket{
			{ID: "claude-seven-day", Window: "weekly", ResetTime: "", RemainingFraction: 1},
			{ID: "claude-seven-day-opus", Window: "weekly-opus", ResetTime: "", RemainingFraction: 0.5},
			{ID: "claude-five-hour", Window: "5h", ResetTime: "2026-11-09T20:00:00Z", RemainingFraction: 0},
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

func testRecordUsageMetricEndpoint(t *testing.T, recordFn func([]byte) error, payload, wantCheck string, wantErr bool) {
	t.Helper()
	if err := recordFn([]byte(payload)); (err != nil) != wantErr {
		t.Fatalf("recordFn() error = %v, wantErr %v", err, wantErr)
	}
	if wantCheck == "" {
		return
	}
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	if !strings.Contains(rec.Body.String(), wantCheck) {
		t.Errorf("missing %q in:\n%s", wantCheck, rec.Body.String())
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
			testRecordUsageMetricEndpoint(t, RecordClaudeUsageJSON, tc.payload, tc.wantCheck, tc.wantErr)
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

const codexUsageFixture = `{"rate_limit":{"primary_window":{"used_percent":56.0,"reset_at":1794254399},"secondary_window":{"used_percent":19.0,"reset_at":1794625199}}}`

func TestCodexUsageToGroups(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, payload string
		want          []QuotaGroup
		wantErr       bool
	}{
		{name: "fixture", payload: codexUsageFixture, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-seven-day", Window: "weekly", ResetTime: "2026-11-14T02:59:59Z", RemainingFraction: 0.81},
			{ID: "codex-five-hour", Window: "5h", ResetTime: "2026-11-09T19:59:59Z", RemainingFraction: 0.44},
		}}}},
		{name: "clamping and zero reset", payload: `{"rate_limit":{"primary_window":{"used_percent":120.0,"reset_at":0},"secondary_window":{"used_percent":-5.0,"reset_at":null}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-seven-day", Window: "weekly", ResetTime: "", RemainingFraction: 1},
			{ID: "codex-five-hour", Window: "5h", ResetTime: "", RemainingFraction: 0},
		}}}},
		{name: "primary window only", payload: `{"rate_limit":{"primary_window":{"used_percent":25.0,"reset_at":1794254399}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-five-hour", Window: "5h", ResetTime: "2026-11-09T19:59:59Z", RemainingFraction: 0.75},
		}}}},
		{name: "null windows", payload: `{"rate_limit":{"primary_window":null,"secondary_window":null}}`, wantErr: true},
		{name: "missing rate_limit", payload: `{}`, wantErr: true},
		{name: "malformed", payload: `{bad`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := CodexUsageToGroups([]byte(tc.payload))
			if (err != nil) != tc.wantErr {
				t.Fatalf("CodexUsageToGroups() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("CodexUsageToGroups() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestRecordCodexUsageJSON(t *testing.T) {
	tests := []struct {
		name, payload, wantCheck string
		wantErr                  bool
	}{
		{"fixture", codexUsageFixture, `aerial_brain_usage_quota_remaining_fraction{bucket_id="codex-five-hour",disabled="false",group="Codex",window="5h"} 0.44`, false},
		{"reset gauge", codexUsageFixture, `aerial_brain_usage_quota_reset_timestamp_seconds{bucket_id="codex-five-hour",group="Codex",window="5h"} 1.794254399e+09`, false},
		{"missing rate limit", `{}`, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testRecordUsageMetricEndpoint(t, RecordCodexUsageJSON, tc.payload, tc.wantCheck, tc.wantErr)
		})
	}
}

func TestNewCodexUsageRunner(t *testing.T) {
	t.Parallel()
	const token = "secret-codex-access-token"
	const accountID = "chatgpt-acc-789"
	tmp := t.TempDir()
	authPath := filepath.Join(tmp, "auth.json")
	_ = os.WriteFile(authPath, []byte(`{"tokens":{"access_token":"`+token+`","account_id":"`+accountID+`"}}`), 0o600)

	tests := []struct {
		name        string
		status      int
		body        string
		authContent string
		wantBody    string
		wantErr     string
	}{
		{name: "ok", status: http.StatusOK, body: codexUsageFixture, wantBody: codexUsageFixture},
		{name: "forbidden", status: http.StatusForbidden, body: `{"error":"forbidden"}`, wantErr: "codex usage endpoint returned HTTP 403"},
		{name: "no token in auth", authContent: `{"tokens":{}}`, wantErr: "has no access token"},
		{name: "malformed auth", authContent: `{invalid`, wantErr: "unmarshal codex auth"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			filePath := authPath
			if tc.authContent != "" {
				filePath = filepath.Join(t.TempDir(), "auth.json")
				_ = os.WriteFile(filePath, []byte(tc.authContent), 0o600)
			}
			seen := make(chan http.Header, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/wham/usage" || r.Method != http.MethodGet {
					http.NotFound(w, r)
					return
				}
				seen <- r.Header.Clone()
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			out, err := NewCodexUsageRunner(srv.Client(), srv.URL, filePath)(context.Background())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil || string(out) != tc.wantBody {
				t.Fatalf("got (%q, %v), want %q", out, err, tc.wantBody)
			}
			if tc.wantErr == "" {
				h := <-seen
				if h.Get("Authorization") != "Bearer "+token || h.Get("Chatgpt-Account-Id") != accountID {
					t.Errorf("unexpected headers: Authorization=%q Chatgpt-Account-Id=%q", h.Get("Authorization"), h.Get("Chatgpt-Account-Id"))
				}
			}
		})
	}
}

func TestNewCodexUsageRunnerTransportError(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	authPath := filepath.Join(tmp, "auth.json")
	_ = os.WriteFile(authPath, []byte(`{"tokens":{"access_token":"tok","account_id":"acc"}}`), 0o600)

	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	tests := []struct{ name, baseURL, authPath string }{
		{"closed server", srv.URL, authPath},
		{"invalid url", "http://bad host", authPath},
		{"missing auth file", srv.URL, filepath.Join(tmp, "nonexistent.json")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewCodexUsageRunner(srv.Client(), tc.baseURL, tc.authPath)(context.Background()); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDefaultCodexUsageRunner(t *testing.T) {
	t.Parallel()
	check := func(p string, isTest bool, wantNil bool) {
		if got := DefaultCodexUsageRunner(p, isTest); (got == nil) != wantNil {
			t.Errorf("DefaultCodexUsageRunner(%q, %v) nil = %v, want %v", p, isTest, got == nil, wantNil)
		}
	}
	check("/path/to/auth.json", true, true)
	check("  ", false, true)
	check("/path/to/auth.json", false, false)
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
			Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
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
