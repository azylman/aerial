package egressscan

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

const (
	poolSecret = "handler-test-secret-value-7q2w"
	// fakePAT is split so this file never holds a token-shaped literal.
	fakePAT = "gh" + "p_" + "aaaaaaaaaabbbbbbbbbbccccccccccdddddd"
)

func blocklistConfig() *config.Config {
	return config.NewFromData(&config.ConfigData{EgressScan: config.EgressScanConfig{BlockedMCP: []string{"discord", "github/add_issue_comment"}}})
}

func TestHandler(t *testing.T) {
	tests := []struct {
		name         string
		remoteAddr   string
		body         string
		pool         string
		wantStatus   int
		wantBody     string
		wantLogLines int
	}{
		{
			name: "blocklisted value hit blocked", remoteAddr: "127.0.0.1:4000", pool: poolSecret,
			body:       `{"server":"discord","tool":"send","arguments":{"content":"` + poolSecret + `"}}`,
			wantStatus: http.StatusOK, wantBody: `{"blocked":true,"violations":[{"category":"InfisicalSecret","sample":"ha***2w"}]}`, wantLogLines: 1,
		},
		{
			name: "blocklisted tool regex hit blocked and masked", remoteAddr: "[::1]:4000", pool: poolSecret,
			body:       `{"server":"GitHub","tool":"add_issue_comment","arguments":"{\"body\":\"` + fakePAT + `\"}"}`,
			wantStatus: http.StatusOK, wantBody: `{"blocked":true,"violations":[{"category":"GitHubPAT","sample":"gh***dd"}]}`, wantLogLines: 1,
		},
		{
			name: "blocklisted clean arguments pass", remoteAddr: "127.0.0.1:4000", pool: poolSecret,
			body:       `{"server":"discord","tool":"send","arguments":{"content":"hi"}}`,
			wantStatus: http.StatusOK, wantBody: `{"blocked":false,"violations":[]}`, wantLogLines: 1,
		},
		{
			name: "non-blocklisted hit passes unscanned", remoteAddr: "127.0.0.1:4000", pool: poolSecret,
			body:       `{"server":"github","tool":"get_file_contents","arguments":{"q":"` + poolSecret + `"}}`,
			wantStatus: http.StatusOK, wantBody: `{"blocked":false,"violations":[]}`, wantLogLines: 1,
		},
		{
			name: "blocklisted with empty pool fails closed", remoteAddr: "127.0.0.1:4000",
			body:       `{"server":"discord","tool":"send","arguments":{"content":"hi"}}`,
			wantStatus: http.StatusServiceUnavailable, wantBody: `{"error":"no registered secret values loaded"}`, wantLogLines: 1,
		},
		{
			name: "non-blocklisted with empty pool passes", remoteAddr: "127.0.0.1:4000",
			body:       `{"server":"kernel","tool":"browser_type","arguments":{"text":"hi"}}`,
			wantStatus: http.StatusOK, wantBody: `{"blocked":false,"violations":[]}`, wantLogLines: 1,
		},
		{
			name: "malformed json rejected", remoteAddr: "127.0.0.1:4000", pool: poolSecret,
			body:       `{"server":"discord","tool":` + poolSecret,
			wantStatus: http.StatusBadRequest, wantBody: `{"error":"invalid json payload"}`, wantLogLines: 1,
		},
		{
			name: "empty tool rejected", remoteAddr: "127.0.0.1:4000", pool: poolSecret,
			body:       `{"server":"discord","tool":"  ","arguments":{}}`,
			wantStatus: http.StatusBadRequest, wantBody: `{"error":"server and tool are required"}`, wantLogLines: 1,
		},
		{
			name: "empty server rejected", remoteAddr: "127.0.0.1:4000", pool: poolSecret,
			body:       `{"tool":"send","arguments":{}}`,
			wantStatus: http.StatusBadRequest, wantBody: `{"error":"server and tool are required"}`, wantLogLines: 1,
		},
		{
			name: "oversized body rejected", remoteAddr: "127.0.0.1:4000", pool: poolSecret,
			body:       strings.Repeat("a", maxRequestBytes+1),
			wantStatus: http.StatusRequestEntityTooLarge, wantBody: `{"error":"request body too large"}`, wantLogLines: 1,
		},
		{
			name: "non-loopback caller refused", remoteAddr: "192.0.2.10:4000", pool: poolSecret,
			body:       `{"server":"discord","tool":"send","arguments":{"content":"` + poolSecret + `"}}`,
			wantStatus: http.StatusForbidden, wantBody: "forbidden",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer.ResetSensitiveTokensForTest()
			defer sanitizer.ResetSensitiveTokensForTest()
			if tc.pool != "" {
				sanitizer.RegisterSensitiveTokens(tc.pool)
			}
			var logs bytes.Buffer
			log.SetOutput(&logs)
			defer log.SetOutput(os.Stderr)

			mux := http.NewServeMux()
			Register(mux, blocklistConfig())
			req := httptest.NewRequest(http.MethodPost, Path, strings.NewReader(tc.body))
			req.RemoteAddr = tc.remoteAddr
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tc.wantBody {
				t.Errorf("body = %s, want %s", got, tc.wantBody)
			}
			if got := strings.Count(logs.String(), "[egress-scan]"); got != tc.wantLogLines {
				t.Errorf("egress-scan log lines = %d, want %d", got, tc.wantLogLines)
			}
			for _, raw := range []string{poolSecret, fakePAT} {
				if strings.Contains(rec.Body.String(), raw) || strings.Contains(logs.String(), raw) {
					t.Fatalf("raw value leaked into response or log")
				}
			}
		})
	}
}
