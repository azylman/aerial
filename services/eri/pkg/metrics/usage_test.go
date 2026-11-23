package metrics

import (
	"cmp"
	"context"
	"fmt"
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

// newUsageEndpoint serves status/body on GET path and records the request headers it saw.
func newUsageEndpoint(t *testing.T, path string, status int, body string) (*httptest.Server, <-chan http.Header) {
	t.Helper()
	seen := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		seen <- r.Header.Clone()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, seen
}

// usageEndpointHeader returns the headers the endpoint recorded, or an empty set when no request reached it.
func usageEndpointHeader(seen <-chan http.Header) http.Header {
	select {
	case h := <-seen:
		return h
	default:
		return http.Header{}
	}
}

func TestNewClaudeUsageRunner(t *testing.T) {
	t.Parallel()
	const token = "sk-ant-oat01-secret-token"
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	tests := []struct {
		name     string
		baseURL  string // "" targets the live fake endpoint
		status   int
		body     string
		wantBody string
		wantErr  string
		wantAuth string // Authorization header the endpoint saw; "" means no request reached it
	}{
		{name: "ok", status: http.StatusOK, body: claudeUsageFixture, wantBody: claudeUsageFixture, wantAuth: "Bearer " + token},
		{name: "forbidden", status: http.StatusForbidden, body: `{"error":"bad token ` + token + `"}`, wantErr: "claude usage endpoint returned HTTP 403", wantAuth: "Bearer " + token},
		{name: "closed server", baseURL: closed.URL, wantErr: "connection refused"},
		{name: "invalid url", baseURL: "http://bad host", wantErr: "invalid character"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, seen := newUsageEndpoint(t, "/api/oauth/usage", tc.status, tc.body)
			out, err := NewClaudeUsageRunner(srv.Client(), cmp.Or(tc.baseURL, srv.URL), token)(context.Background())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || strings.Contains(err.Error(), token) {
					t.Fatalf("error = %v, want %q without token", err, tc.wantErr)
				}
			} else if err != nil || string(out) != tc.wantBody {
				t.Fatalf("got (%q, %v), want %q", out, err, tc.wantBody)
			}
			h := usageEndpointHeader(seen)
			if h.Get("Authorization") != tc.wantAuth {
				t.Errorf("Authorization = %q, want %q", h.Get("Authorization"), tc.wantAuth)
			}
			if tc.wantAuth != "" && h.Get("Anthropic-Beta") != "oauth-2025-04-20" {
				t.Errorf("anthropic-beta = %q, want oauth-2025-04-20", h.Get("Anthropic-Beta"))
			}
		})
	}
}

// TestDefaultUsageRunners covers both subscription runners, which share the (credential, isTest) contract.
func TestDefaultUsageRunners(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, credential string
		isTest           bool
		wantNil          bool
	}{
		{"test mode", "/path/to/auth.json", true, true},
		{"blank credential", "  ", false, true},
		{"enabled", "/path/to/auth.json", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := DefaultClaudeUsageRunner(tc.credential, tc.isTest); (got == nil) != tc.wantNil {
				t.Errorf("DefaultClaudeUsageRunner(%q, %v) nil = %v, want %v", tc.credential, tc.isTest, got == nil, tc.wantNil)
			}
			if got := DefaultCodexUsageRunner(tc.credential, tc.isTest); (got == nil) != tc.wantNil {
				t.Errorf("DefaultCodexUsageRunner(%q, %v) nil = %v, want %v", tc.credential, tc.isTest, got == nil, tc.wantNil)
			}
		})
	}
}

const codexUsageFixture = `{"rate_limit":{"primary_window":{"limit_window_seconds":18000,"used_percent":56.0,"reset_at":1794254399},"secondary_window":{"limit_window_seconds":604800,"used_percent":19.0,"reset_at":1794625199}}}`

func TestCodexUsageToGroups(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, payload string
		want          []QuotaGroup
		wantErr       bool
	}{
		{name: "primary weekly only", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":604800,"used_percent":2}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.98},
		}}}},
		{name: "fixture", payload: codexUsageFixture, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-seven-day", Window: "weekly", ResetTime: "2026-11-14T02:59:59Z", RemainingFraction: 0.81},
			{ID: "codex-five-hour", Window: "5h", ResetTime: "2026-11-09T19:59:59Z", RemainingFraction: 0.44},
		}}}},
		{name: "clamping and zero reset", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":18000,"used_percent":120.0,"reset_at":0},"secondary_window":{"limit_window_seconds":604800,"used_percent":-5.0,"reset_at":null}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-seven-day", Window: "weekly", ResetTime: "", RemainingFraction: 1},
			{ID: "codex-five-hour", Window: "5h", ResetTime: "", RemainingFraction: 0},
		}}}},
		{name: "primary window only", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":18000,"used_percent":25.0,"reset_at":1794254399}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-five-hour", Window: "5h", ResetTime: "2026-11-09T19:59:59Z", RemainingFraction: 0.75},
		}}}},
		{name: "secondary weekly", payload: `{"rate_limit":{"primary_window":null,"secondary_window":{"limit_window_seconds":604800,"used_percent":2}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.98}}}}},
		{name: "swapped pair", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":604800,"used_percent":2},"secondary_window":{"limit_window_seconds":18000,"used_percent":25}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.98}, {ID: "codex-five-hour", Window: "5h", RemainingFraction: 0.75}}}}},
		{name: "supported with unsupported", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":604800,"used_percent":2},"secondary_window":{"limit_window_seconds":60,"used_percent":25}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.98}}}}},
		{name: "missing percent duplicate skipped", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":604800,"used_percent":2},"secondary_window":{"limit_window_seconds":604800}}}`, want: []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.98}}}}},
		{name: "missing duration", payload: `{"rate_limit":{"primary_window":{"used_percent":2},"secondary_window":null}}`, wantErr: true},
		{name: "zero duration", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":0,"used_percent":2},"secondary_window":null}}`, wantErr: true},
		{name: "negative duration", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":-1,"used_percent":2},"secondary_window":null}}`, wantErr: true},
		{name: "unsupported duration", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":60,"used_percent":2},"secondary_window":null}}`, wantErr: true},
		{name: "missing percent", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":18000},"secondary_window":null}}`, wantErr: true},
		{name: "duplicate weekly", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":604800,"used_percent":2},"secondary_window":{"limit_window_seconds":604800,"used_percent":3}}}`, wantErr: true},
		{name: "duplicate five hour", payload: `{"rate_limit":{"primary_window":{"limit_window_seconds":18000,"used_percent":2},"secondary_window":{"limit_window_seconds":18000,"used_percent":3}}}`, wantErr: true},
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
	const weekly = `{"rate_limit":{"primary_window":{"limit_window_seconds":604800,"used_percent":2,"reset_at":1794625199}}}`
	weeklyGroups := []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.98, ResetTime: "2026-11-14T02:59:59Z"}}}}
	tests := []struct {
		name            string
		payloads        []string
		wantErr         bool
		want            []QuotaGroup
		present, absent []string
	}{
		{"both windows", []string{codexUsageFixture}, false, []QuotaGroup{{Name: "Codex", Buckets: []QuotaBucket{
			{ID: "codex-seven-day", Window: "weekly", RemainingFraction: 0.81, ResetTime: "2026-11-14T02:59:59Z"},
			{ID: "codex-five-hour", Window: "5h", RemainingFraction: 0.44, ResetTime: "2026-11-09T19:59:59Z"},
		}}}, []string{`aerial_brain_usage_quota_remaining_fraction{bucket_id="codex-five-hour",disabled="false",group="Codex",window="5h"} 0.44`, `aerial_brain_usage_quota_reset_timestamp_seconds{bucket_id="codex-five-hour",group="Codex",window="5h"} 1.794254399e+09`}, nil},
		{
			"both to weekly",
			[]string{codexUsageFixture, weekly},
			false, weeklyGroups,
			[]string{`aerial_brain_usage_quota_remaining_fraction{bucket_id="codex-seven-day",disabled="false",group="Codex",window="weekly"} 0.98`, `aerial_brain_usage_quota_reset_timestamp_seconds{bucket_id="codex-seven-day",group="Codex",window="weekly"} 1.794625199e+09`},
			[]string{`aerial_brain_usage_quota_remaining_fraction{bucket_id="codex-five-hour"`, `aerial_brain_usage_quota_reset_timestamp_seconds{bucket_id="codex-five-hour"`},
		},
		{"missing rate limit retains telemetry", []string{weekly, `{}`}, true, weeklyGroups, []string{`window="weekly"} 0.98`}, nil},
		{"duplicate retains telemetry", []string{weekly, `{"rate_limit":{"primary_window":{"limit_window_seconds":18000,"used_percent":5},"secondary_window":{"limit_window_seconds":18000,"used_percent":10}}}`}, true, weeklyGroups, []string{`window="weekly"} 0.98`}, []string{`bucket_id="codex-five-hour"`}},
		{"unsupported retains telemetry", []string{weekly, `{"rate_limit":{"primary_window":{"limit_window_seconds":60,"used_percent":5}}}`}, true, weeklyGroups, []string{`window="weekly"} 0.98`}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i, payload := range tc.payloads {
				wantErr := tc.wantErr && i == len(tc.payloads)-1
				if err := RecordCodexUsageJSON([]byte(payload)); (err != nil) != wantErr {
					t.Fatalf("record error = %v, wantErr %v", err, wantErr)
				}
			}
			if got := LatestQuotaSnapshot().Groups; !reflect.DeepEqual(got, tc.want) {
				t.Errorf("latest = %+v, want %+v", got, tc.want)
			}
			rec := httptest.NewRecorder()
			Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
			for _, want := range tc.present {
				if !strings.Contains(rec.Body.String(), want) {
					t.Errorf("missing %q", want)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(rec.Body.String(), absent) {
					t.Errorf("stale %q", absent)
				}
			}
		})
	}
}

func TestNewCodexUsageRunner(t *testing.T) {
	t.Parallel()
	const token = "secret-codex-access-token"
	const accountID = "chatgpt-acc-789"
	tmp := t.TempDir()
	writeAuth := func(name, content string) string {
		path := filepath.Join(tmp, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	authPath := writeAuth("auth.json", `{"tokens":{"access_token":"`+token+`","account_id":"`+accountID+`"}}`)
	noTokenPath := writeAuth("no-token.json", `{"tokens":{}}`)
	malformedPath := writeAuth("malformed.json", `{invalid`)
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	tests := []struct {
		name     string
		baseURL  string // "" targets the live fake endpoint
		authPath string
		status   int
		body     string
		wantBody string
		wantErr  string
		wantAuth string // Authorization header the endpoint saw; "" means no request reached it
	}{
		{name: "ok", authPath: authPath, status: http.StatusOK, body: codexUsageFixture, wantBody: codexUsageFixture, wantAuth: "Bearer " + token},
		{name: "forbidden", authPath: authPath, status: http.StatusForbidden, body: `{"error":"forbidden"}`, wantErr: "codex usage endpoint returned HTTP 403", wantAuth: "Bearer " + token},
		{name: "no token in auth", authPath: noTokenPath, wantErr: "has no access token"},
		{name: "malformed auth", authPath: malformedPath, wantErr: "unmarshal codex auth"},
		{name: "missing auth file", authPath: filepath.Join(tmp, "nonexistent.json"), wantErr: "read codex auth file"},
		{name: "closed server", baseURL: closed.URL, authPath: authPath, wantErr: "connection refused"},
		{name: "invalid url", baseURL: "http://bad host", authPath: authPath, wantErr: "invalid character"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, seen := newUsageEndpoint(t, "/wham/usage", tc.status, tc.body)
			out, err := NewCodexUsageRunner(srv.Client(), cmp.Or(tc.baseURL, srv.URL), tc.authPath)(context.Background())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil || string(out) != tc.wantBody {
				t.Fatalf("got (%q, %v), want %q", out, err, tc.wantBody)
			}
			h := usageEndpointHeader(seen)
			if h.Get("Authorization") != tc.wantAuth {
				t.Errorf("Authorization = %q, want %q", h.Get("Authorization"), tc.wantAuth)
			}
			if tc.wantAuth != "" && h.Get("Chatgpt-Account-Id") != accountID {
				t.Errorf("Chatgpt-Account-Id = %q, want %q", h.Get("Chatgpt-Account-Id"), accountID)
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
			Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
			if !strings.Contains(rec.Body.String(), tc.wantCheck) {
				t.Errorf("%s: missing %q in:\n%s", tc.name, tc.wantCheck, rec.Body.String())
			}
			if snap := LatestQuotaSnapshot(); len(snap.Groups) == 0 || snap.UpdatedAt.IsZero() {
				t.Errorf("%s: expected LatestQuotaSnapshot() to be populated, got %+v", tc.name, snap)
			}
		}
	}
}

func TestStartUsagePollerAndRunner(t *testing.T) {
	StartUsagePoller(context.Background(), 0, nil, RecordUsageJSON)
	StartUsagePoller(context.Background(), time.Second, func(context.Context) ([]byte, error) { return nil, nil }, nil)
	dir := t.TempDir()
	script := filepath.Join(dir, "mock-agy.sh")
	argsFile := filepath.Join(dir, "args.txt")
	payload := `{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"G","buckets":[{"id":"b1","window":"5h","remaining_fraction":0.75}]}]}}}`
	scriptBody := fmt.Sprintf("#!/bin/sh\necho \"$@\" > %q\necho '%s'\n", argsFile, payload)
	if err := os.WriteFile(script, []byte(scriptBody), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := NewAgyUsageRunner(script, dir, []string{"HOME=/old", "FOO=bar"})(context.Background()); err != nil || !strings.Contains(string(out), `"b1"`) {
		t.Fatalf("NewAgyUsageRunner failed: err=%v, out=%s", err, string(out))
	}
	capturedArgs, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("failed reading captured args: %v", err)
	}
	if !strings.Contains(string(capturedArgs), "--log-file "+os.DevNull) {
		t.Fatalf("expected --log-file %s in args: %s", os.DevNull, string(capturedArgs))
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

const museUsageFixture = `{"allowance":{"asOfMs":1795020360826,"source":"mint","tier":"Muse Code Power Usage","weekly":{"resetsAtMs":1795392000000,"usedPercent":20},"window":{"resetsAtMs":1795037083000,"usedPercent":8}},"hint":null,"reason":null,"schemaVersion":1}`

func TestMuseUsageToGroups(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, payload   string
		want            []QuotaGroup
		wantErr         bool
		wantErrContains []string
	}{
		{name: "live fixture", payload: museUsageFixture, want: []QuotaGroup{{Name: "Muse", Buckets: []QuotaBucket{
			{ID: "muse-seven-day", Window: "weekly", ResetTime: "2026-11-23T00:00:00Z", RemainingFraction: 0.8},
			{ID: "muse-five-hour", Window: "5h", ResetTime: "2026-11-18T21:24:43Z", RemainingFraction: 0.92},
		}}}},
		{name: "window only", payload: `{"allowance":{"window":{"resetsAtMs":1795037083000,"usedPercent":25}}}`, want: []QuotaGroup{{Name: "Muse", Buckets: []QuotaBucket{
			{ID: "muse-five-hour", Window: "5h", ResetTime: "2026-11-18T21:24:43Z", RemainingFraction: 0.75},
		}}}},
		{name: "clamping and zero reset", payload: `{"allowance":{"weekly":{"resetsAtMs":0,"usedPercent":120},"window":{"usedPercent":-5}}}`, want: []QuotaGroup{{Name: "Muse", Buckets: []QuotaBucket{
			{ID: "muse-seven-day", Window: "weekly", ResetTime: "", RemainingFraction: 0},
			{ID: "muse-five-hour", Window: "5h", ResetTime: "", RemainingFraction: 1},
		}}}},
		{name: "missing allowance", payload: `{"hint":null}`, wantErr: true},
		{name: "missing allowance surfaces reason and hint", payload: `{"reason":"credential_rejected","hint":"run muse auth login"}`, wantErr: true, wantErrContains: []string{"contains no allowance object", "credential_rejected", "run muse auth login"}},
		{name: "all null windows", payload: `{"allowance":{"weekly":null,"window":null}}`, wantErr: true},
		{name: "malformed", payload: `{bad`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := MuseUsageToGroups([]byte(tc.payload))
			if (err != nil) != tc.wantErr {
				t.Fatalf("MuseUsageToGroups() error = %v, wantErr %v", err, tc.wantErr)
			}
			for _, wantSub := range tc.wantErrContains {
				if err == nil || !strings.Contains(err.Error(), wantSub) {
					t.Errorf("MuseUsageToGroups() error = %v, want substring %q", err, wantSub)
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("MuseUsageToGroups() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestRecordMuseUsageJSON(t *testing.T) {
	tests := []struct {
		name, payload, wantCheck string
		wantErr                  bool
	}{
		{"fixture", museUsageFixture, `aerial_brain_usage_quota_remaining_fraction{bucket_id="muse-five-hour",disabled="false",group="Muse",window="5h"} 0.92`, false},
		{"reset gauge", museUsageFixture, `aerial_brain_usage_quota_reset_timestamp_seconds{bucket_id="muse-seven-day",group="Muse",window="weekly"} 1.795392e+09`, false},
		{"all null", `{"allowance":{"weekly":null}}`, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testRecordUsageMetricEndpoint(t, RecordMuseUsageJSON, tc.payload, tc.wantCheck, tc.wantErr)
		})
	}
}

func mockMuseScriptBody(failStderr string) string {
	if failStderr != "" {
		return "#!/bin/sh\necho '" + failStderr + "' >&2\nexit 1\n"
	}
	return `#!/bin/sh
if [ "$1" = "usage" ] && [ "$2" = "--json" ]; then
	echo "HOME=$HOME"
	echo "XDG_CONFIG_HOME=$XDG_CONFIG_HOME"
	echo "XDG_DATA_HOME=$XDG_DATA_HOME"
	echo "MUSE_AUTH_PATH=$MUSE_AUTH_PATH"
	echo '` + museUsageFixture + `'
else
	echo 'bad args' >&2
	exit 1
fi
`
}

func checkMuseRunnerHomeEnv(t *testing.T, dir, out string) {
	t.Helper()
	for _, check := range []string{
		"HOME=" + dir,
		"XDG_CONFIG_HOME=" + filepath.Join(dir, ".config"),
		"XDG_DATA_HOME=" + filepath.Join(dir, ".local", "share"),
		"MUSE_AUTH_PATH=" + filepath.Join(dir, ".config", "muse", "auth.json"),
	} {
		if !strings.Contains(out, check) {
			t.Errorf("NewMuseUsageRunner output missing expected env %q; got output:\n%s", check, out)
		}
	}
	if strings.Contains(out, "/old") {
		t.Errorf("NewMuseUsageRunner output contains leaked old env; got output:\n%s", out)
	}
}

func TestMuseUsageRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		failStderr      string
		wantErr         bool
		wantErrContains string
	}{
		{name: "success pins home env"},
		{name: "failure surfaces stderr", failStderr: "credential_rejected: run muse auth login", wantErr: true, wantErrContains: "credential_rejected: run muse auth login"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			script := filepath.Join(dir, "mock-muse.sh")
			if err := os.WriteFile(script, []byte(mockMuseScriptBody(tc.failStderr)), 0o755); err != nil {
				t.Fatal(err)
			}
			baseEnv := []string{
				"HOME=/old",
				"XDG_CONFIG_HOME=/old/config",
				"XDG_DATA_HOME=/old/data",
				"MUSE_AUTH_PATH=/old/auth.json",
				"FOO=bar",
			}
			out, err := NewMuseUsageRunner(script, dir, baseEnv)(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewMuseUsageRunner() error = %v, wantErr %v (out=%s)", err, tc.wantErr, string(out))
			}
			if tc.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrContains)) {
				t.Errorf("NewMuseUsageRunner() error = %v, want substring %q", err, tc.wantErrContains)
			}
			if tc.wantErr {
				return
			}
			if !strings.Contains(string(out), `"Muse Code Power Usage"`) {
				t.Fatalf("NewMuseUsageRunner failed: err=%v, out=%s", err, string(out))
			}
			checkMuseRunnerHomeEnv(t, dir, string(out))
		})
	}
	if got := DefaultMuseUsageRunner("", "", nil, true); got != nil {
		t.Error("DefaultMuseUsageRunner in test mode = non-nil, want nil")
	}
	if got := DefaultMuseUsageRunner("", "", nil, false); got == nil {
		t.Error("DefaultMuseUsageRunner outside test mode = nil, want non-nil")
	}
}
