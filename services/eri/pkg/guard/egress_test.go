package guard

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/sanitizer"
)

const (
	egressPlainSecret  = "egress-test-secret-value-9f3k"
	egressQuotedSecret = `egr"ess-se\cret-val-42`
	// egressQuotedSecretB64 is base64.StdEncoding of egressQuotedSecret.
	egressQuotedSecretB64 = "ZWdyImVzcy1zZVxjcmV0LXZhbC00Mg=="
)

// jsonStringArgs encodes payload as a JSON string, the shape agy sends Arguments in.
func jsonStringArgs(payload string) json.RawMessage {
	b, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return b
}

func TestEvaluateEgress(t *testing.T) {
	tests := []struct {
		name        string
		secret      string
		args        json.RawMessage
		blocked     bool
		wantBlocked bool
		want        []EgressViolation
	}{
		{
			name: "blocklisted plain value hit", secret: egressPlainSecret, blocked: true, wantBlocked: true,
			args: json.RawMessage(`{"content":"note: ` + egressPlainSecret + `"}`),
			want: []EgressViolation{{Category: "InfisicalSecret", Sample: "eg***3k"}},
		},
		{
			name: "blocklisted JSON-escaped value hit via decoded leaf", secret: egressQuotedSecret, blocked: true, wantBlocked: true,
			args: json.RawMessage(`{"content":"egr\"ess-se\\cret-val-42"}`),
			want: []EgressViolation{{Category: "InfisicalSecret", Sample: "eg***42"}},
		},
		{
			name: "blocklisted base64 value hit", secret: egressQuotedSecret, blocked: true, wantBlocked: true,
			args: json.RawMessage(`{"content":"` + egressQuotedSecretB64 + `"}`),
			want: []EgressViolation{{Category: "InfisicalSecret", Sample: "ZW***=="}},
		},
		{
			name: "blocklisted value nested in JSON-string arguments", secret: egressQuotedSecret, blocked: true, wantBlocked: true,
			args: jsonStringArgs(`{"message":{"body":"egr\"ess-se\\cret-val-42"}}`),
			want: []EgressViolation{{Category: "InfisicalSecret", Sample: "eg***42"}},
		},
		{
			name: "blocklisted value in object key", secret: egressPlainSecret, blocked: true, wantBlocked: true,
			args: json.RawMessage(`{"` + egressPlainSecret + `":1}`),
			want: []EgressViolation{{Category: "InfisicalSecret", Sample: "eg***3k"}},
		},
		{
			name: "blocklisted regex hit masked", blocked: true, wantBlocked: true,
			args: json.RawMessage(`{"body":"token ` + testPAT + `"}`),
			want: []EgressViolation{{Category: "GitHubPAT", Sample: "gh***44"}},
		},
		{
			name: "blocklisted clean arguments", secret: egressPlainSecret, blocked: true,
			args: json.RawMessage(`{"body":"hello","n":3,"list":["a",null,true]}`),
			want: []EgressViolation{},
		},
		{
			name: "blocklisted private address and cluster host are not secrets", secret: egressPlainSecret, blocked: true,
			args: json.RawMessage(`{"url":"http://haro.aerial/","ip":"192.168.1.20"}`),
			want: []EgressViolation{},
		},
		{
			name: "blocklisted invalid JSON still scans raw bytes", secret: egressPlainSecret, blocked: true, wantBlocked: true,
			args: json.RawMessage(`not json ` + egressPlainSecret),
			want: []EgressViolation{{Category: "InfisicalSecret", Sample: "eg***3k"}},
		},
		{
			name: "blocklisted empty arguments", secret: egressPlainSecret, blocked: true,
			want: []EgressViolation{},
		},
		{
			name: "non-blocklisted destination with a hit is not scanned", secret: egressPlainSecret,
			args: json.RawMessage(`{"body":"` + egressPlainSecret + ` ` + testPAT + `"}`),
			want: []EgressViolation{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer.ResetSensitiveTokensForTest()
			defer sanitizer.ResetSensitiveTokensForTest()
			if tc.secret != "" {
				sanitizer.RegisterSensitiveTokens(tc.secret)
			}
			req := EgressScanRequest{Server: "discord", Tool: "send", Arguments: tc.args}
			got := EvaluateEgress(req, tc.blocked, BuiltinPatterns)
			if got.Blocked != tc.wantBlocked {
				t.Errorf("Blocked = %t, want %t", got.Blocked, tc.wantBlocked)
			}
			if !reflect.DeepEqual(got.Violations, tc.want) {
				t.Errorf("Violations = %+v, want %+v", got.Violations, tc.want)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("marshal result: %v", err)
			}
			for _, raw := range []string{egressPlainSecret, egressQuotedSecret, egressQuotedSecretB64, testPAT} {
				if strings.Contains(string(encoded), raw) {
					t.Fatalf("result %s leaks raw value", encoded)
				}
			}
		})
	}
}
