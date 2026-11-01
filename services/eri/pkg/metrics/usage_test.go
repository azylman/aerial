package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
	StartUsagePoller(context.Background(), 0, nil)
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
	})
	for range 3 {
		<-calls
	}
}
