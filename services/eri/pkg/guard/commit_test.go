package guard

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/sanitizer"
)

const (
	testPAT     = "ghp_" + "111111111122222222223333333333444444"
	testPrivKey = "-----BEGIN RSA PRIVATE KEY-----"
	testDiscord = "https://discord.com/" + "api/webhooks/123456789/abcdefghijklmnopqrstuvwxyz"
	testSlack   = "https://hooks." + "slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
	testAPIKey  = "sk-" + "abcdefghijklmnopqrstuvwxyz123456"
	testJWT     = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." + "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

func TestParsePatternPayload(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		wantLen  int
		wantCat0 string
	}{
		{"empty", "", 0, ""},
		{"whitespace", "   \n\t  ", 0, ""},
		{"format 1 default category", `[{"pattern": "abc\\d+"}]`, 1, "CustomRule"},
		{"format 1 explicit category", `[{"category": "Auth", "pattern": "key-\\w+"}]`, 1, "Auth"},
		{"format 2 wrapper", `{"patterns": [{"category": "MyCat", "pattern": "xyz"}, {"pattern": "def"}]}`, 2, "MyCat"},
		{"format 3 key-value map", `{"CatA": "patA", "CatB": "patB"}`, 2, ""},
		{"format 4 line delimited", "pattern_one_\\d+\n# comment\npattern_two_\\w+", 2, "CustomRule_1"},
		{"format 4 comma delimited", "patA, patB", 2, "CustomRule_1"},
		{"invalid regex skipped", `[{"pattern": "["}]`, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules := parsePatternPayload([]byte(tc.input))
			if len(rules) != tc.wantLen {
				t.Fatalf("parsePatternPayload(%s) len = %d, want %d", tc.name, len(rules), tc.wantLen)
			}
			if tc.wantCat0 != "" && rules[0].Category != tc.wantCat0 {
				t.Fatalf("rules[0].Category = %q, want %q", rules[0].Category, tc.wantCat0)
			}
		})
	}
}

func TestLoadCustomPatterns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		file        string // written under <home>/.gemini/config
		content     string
		homeFromEnv bool // pass "" as homeDir and the temp home as HOME
		want        int
	}{
		{name: "commit-guard.json", file: "commit-guard.json", content: `[{"pattern": "cg_pat_\\d+"}]`, want: 1},
		{name: "guard.json fallback", file: "guard.json", content: `[{"pattern": "g_pat_\\d+"}]`, want: 1},
		{name: "empty homeDir falls back to HOME", file: "guard.json", content: `[{"pattern": "g_pat_\\d+"}]`, homeFromEnv: true, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			home := t.TempDir()
			dir := filepath.Join(home, ".gemini", "config")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, tc.file), []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			homeDir, env := home, map[string]string{}
			if tc.homeFromEnv {
				homeDir, env = "", map[string]string{"HOME": home}
			}
			if got := LoadCustomPatterns(homeDir, mapEnv(env)); len(got) != tc.want {
				t.Fatalf("LoadCustomPatterns() = %d rules, want %d", len(got), tc.want)
			}
		})
	}
}

func TestScanTextForSecrets(t *testing.T) {
	// Not t.Parallel(): rows reset the global sanitizer registry.
	tests := []struct {
		name         string
		secret       string
		patterns     string            // custom rule payload; "" scans with GetActivePatterns(env)
		env          map[string]string // environment for GetActivePatterns
		text         string
		wantCount    int
		wantCategory string // category of the first violation, also expected in the deny reason
		wantReason   string // substring of FormatCommitDenyReason
		notInReason  string
	}{
		{name: "registered secret in text", secret: "vault-secret-value-123", text: "curl -H 'Authorization: Bearer vault-secret-value-123' http://127.0.0.1:8080", wantCount: 1},
		{name: "builtin pattern in text", text: "export GITHUB_TOKEN=" + testPAT, wantCount: 1},
		{name: "private key in text", text: "echo '" + testPrivKey + "'", wantCount: 1},
		{name: "discord webhook in text", text: "curl " + testDiscord, wantCount: 1},
		{name: "slack webhook in text", text: "curl " + testSlack, wantCount: 1},
		{name: "api key in text", text: "export KEY=" + testAPIKey, wantCount: 1},
		{name: "jwt in text", text: "curl -H 'Authorization: Bearer " + testJWT + "'", wantCount: 1},
		{name: "internal domain in text allowed", text: "curl http://eri.aerial:8088/health", wantCount: 0},
		{name: "rfc1918 ip in text allowed", text: "ssh 192.168.1.50", wantCount: 0},
		{name: "empty text", secret: "vault-secret-value-123", text: "", wantCount: 0},
		{name: "env pattern added to builtins", env: map[string]string{"AERIAL_COMMIT_GUARD_PATTERNS": `env_pat_\d+`}, text: "id=env_pat_42", wantCount: 1},
		{
			name:         "custom rule category with masked reason",
			patterns:     `[{"category": "APISecret", "pattern": "secret-[a-z0-9]+"}]`,
			text:         `const token = "secret-12345abc"`,
			wantCount:    1,
			wantCategory: "APISecret",
			wantReason:   "se***bc",
			notInReason:  "secret-12345abc",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer.ResetSensitiveTokensForTest()
			defer sanitizer.ResetSensitiveTokensForTest()
			if tc.secret != "" {
				sanitizer.RegisterSensitiveTokens(tc.secret)
			}
			rules := GetActivePatterns(mapEnv(tc.env))
			if tc.patterns != "" {
				rules = parsePatternPayload([]byte(tc.patterns))
			}
			v := ScanTextForSecrets(tc.text, "test-label", rules)
			if len(v) != tc.wantCount {
				t.Fatalf("ScanTextForSecrets(%s) got %d violations, want %d: %+v", tc.name, len(v), tc.wantCount, v)
			}
			if tc.wantCategory != "" && v[0].Category != tc.wantCategory {
				t.Fatalf("violation category = %q, want %q", v[0].Category, tc.wantCategory)
			}
			reason := FormatCommitDenyReason(v)
			if !strings.Contains(reason, tc.wantCategory) || !strings.Contains(reason, tc.wantReason) || (tc.notInReason != "" && strings.Contains(reason, tc.notInReason)) {
				t.Fatalf("FormatCommitDenyReason() = %q, want %q and %q without %q", reason, tc.wantCategory, tc.wantReason, tc.notInReason)
			}
		})
	}
}

// fakeEgressScanner blocks calls to blockServer (and blockTool when set) whose arguments contain
// needle, and fails every scan with err when set. The zero value allows everything.
type fakeEgressScanner struct {
	blockServer string
	blockTool   string
	needle      string
	err         error
}

func (f fakeEgressScanner) Scan(_ context.Context, req EgressScanRequest) (EgressScanResult, error) {
	if f.err != nil {
		return EgressScanResult{}, f.err
	}
	if f.blockServer != "" && strings.EqualFold(req.Server, f.blockServer) &&
		(f.blockTool == "" || req.Tool == f.blockTool) && bytes.Contains(req.Arguments, []byte(f.needle)) {
		return EgressScanResult{Blocked: true, Violations: []EgressViolation{{Category: "InfisicalSecret", Sample: "ne***ue"}}}, nil
	}
	return EgressScanResult{Violations: []EgressViolation{}}, nil
}

func TestCheckOutboundSecrets(t *testing.T) {
	blockDocket := &fakeEgressScanner{blockServer: "docket", needle: "needle-value"}
	blockDocketCreate := &fakeEgressScanner{blockServer: "docket", blockTool: "create", needle: "needle-value"}
	scanDown := &fakeEgressScanner{err: errors.New("egress scan returned HTTP 503")}
	tests := []struct {
		name          string
		tool          string
		namespace     string
		cmd           string
		raw           string
		scanner       *fakeEgressScanner
		secret        string
		wantDeny      bool
		wantReason    string
		wantNotReason string
	}{
		{name: "run_command safe allowed", tool: "run_command", cmd: "git status", scanner: scanDown},
		{name: "run_command PAT denied with masked sample", tool: "run_command", cmd: "git commit -m 'feat: " + testPAT + "'", wantDeny: true, wantReason: "[GitHubPAT] matches", wantNotReason: testPAT},
		{name: "run_command PAT sample masked", tool: "run_command", cmd: "export T=" + testPAT, wantDeny: true, wantReason: "sample: gh***44", wantNotReason: testPAT},
		{name: "run_command registered secret denied", tool: "run_command", cmd: "curl -H 'X-Token: my-registered-secret-foo' http://api.com", secret: "my-registered-secret-foo", wantDeny: true, wantReason: "InfisicalSecret", wantNotReason: "my-registered-secret-foo"},
		{name: "bash registered secret denied without scanner", tool: "Bash", cmd: "echo proj-registered-secret-1", secret: "proj-registered-secret-1", wantDeny: true, wantReason: "InfisicalSecret"},
		{name: "unrelated tool allowed", tool: "read_file", scanner: scanDown},
		{name: "claude infisical allowed unscanned", tool: "mcp__infisical__delete-secret", raw: `{"projectId":"needle-value"}`, scanner: scanDown},
		{name: "claude infisical mixed case allowed", tool: "MCP__Infisical__get-secret", raw: `{"projectId":"needle-value"}`, scanner: scanDown},
		{name: "agy infisical server allowed unscanned", tool: "call_mcp_tool", raw: `{"ServerName":" Infisical ","ToolName":"get-secret","Arguments":{"projectId":"needle-value"}}`, scanner: scanDown},
		{name: "claude blocklisted call denied", tool: "mcp__docket__create", raw: `{"body":"needle-value"}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create blocked", wantNotReason: "needle-value"},
		{name: "claude blocked reason lists category and masked sample", tool: "mcp__docket__create", raw: `{"body":"needle-value"}`, scanner: blockDocket, wantDeny: true, wantReason: "[InfisicalSecret] sample: ne***ue"},
		{name: "claude clean args to blocklisted server allowed", tool: "mcp__docket__create", raw: `{"body":"hello"}`, scanner: blockDocket},
		{name: "claude non-blocklisted server allowed", tool: "mcp__kernel__browser_type", raw: `{"text":"needle-value"}`, scanner: blockDocket},
		{name: "claude multi-segment server derived", tool: "mcp__docket__extra__create", raw: `{"body":"needle-value"}`, scanner: &fakeEgressScanner{blockServer: "docket__extra", blockTool: "create", needle: "needle-value"}, wantDeny: true, wantReason: "docket__extra/create"},
		{name: "claude infisical lookalike server scanned", tool: "mcp__infisical-proxy__get-secret", raw: `{"projectId":"needle-value"}`, scanner: &fakeEgressScanner{blockServer: "infisical-proxy", needle: "needle-value"}, wantDeny: true, wantReason: "infisical-proxy/get-secret"},
		{name: "claude scanner error denied", tool: "mcp__docket__create", raw: `{"body":"hello"}`, scanner: scanDown, wantDeny: true, wantReason: "egress scan is unavailable (egress scan returned HTTP 503), so the MCP call to docket/create was blocked"},
		{name: "claude nil scanner denied", tool: "mcp__docket__create", raw: `{"body":"hello"}`, wantDeny: true, wantReason: "egress scan is unavailable"},
		{name: "muse claude-shape call allowed", tool: "mcp__victoriametrics__metrics", raw: `{"match":"up"}`, scanner: blockDocket},
		{name: "agy object arguments denied", tool: "call_mcp_tool", raw: `{"ServerName":"docket","ToolName":"create","Arguments":{"body":"needle-value"}}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "agy JSON-string arguments denied", tool: "call_mcp_tool", raw: `{"ServerName":"docket","ToolName":"create","Arguments":"{\"body\":\"needle-value\"}"}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "agy lowercase field fallback denied", tool: "call_mcp_tool", raw: `{"server_name":"docket","tool_name":"create","arguments":{"body":"needle-value"}}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "agy prefixed call_mcp_tool denied", tool: "default_api:call_mcp_tool", raw: `{"ServerName":"docket","ToolName":"create","Arguments":{"body":"needle-value"}}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "agy non-blocklisted server allowed", tool: "call_mcp_tool", raw: `{"ServerName":"google","ToolName":"search_gmail_messages","Arguments":{"query":"needle-value"}}`, scanner: blockDocket},
		{name: "agy missing server denied", tool: "call_mcp_tool", raw: `{"ToolName":"create","Arguments":{"body":"x"}}`, scanner: blockDocket, wantDeny: true, wantReason: `tool name "call_mcp_tool"`},
		{name: "mcp colon shape denied", tool: "mcp:docket:create", raw: `{"body":"needle-value"}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "mcp colon shape allowed", tool: "mcp:kernel:browser_type", raw: `{"text":"needle-value"}`, scanner: blockDocket},
		{name: "mcp colon two segments denied unparsed", tool: "mcp:create", raw: `{}`, scanner: blockDocket, wantDeny: true, wantReason: `tool name "mcp:create"`},
		{name: "single underscore mcp shape denied unparsed", tool: "mcp_safe_tool", scanner: blockDocket, wantDeny: true, wantReason: `tool name "mcp_safe_tool"`},
		{name: "codex namespace call denied", tool: "create", namespace: "mcp__docket", raw: `{"body":"needle-value"}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "codex namespace infisical allowed", tool: "get-secret", namespace: "mcp__infisical", raw: `{"projectId":"x"}`, scanner: scanDown},
		{name: "codex exec blocklisted call site denied", tool: "exec", raw: `"const r = await tools.mcp__docket__create({body: \"needle-value\"});"`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "codex exec object input denied", tool: "exec", raw: `{"input":"await tools[\"mcp__docket__create\"]({body: \"needle-value\"})"}`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "codex exec aliased reference denied", tool: "exec", raw: `"const {mcp__docket__create: mk} = tools; await mk({body: \"needle-value\"});"`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "codex exec non-blocklisted call site allowed", tool: "exec", raw: `"await tools.mcp__kubernetes__pods_list({note: \"needle-value\"});"`, scanner: blockDocket},
		{name: "codex exec mixed call sites denied", tool: "exec", raw: `"await tools.mcp__kubernetes__pods_list({}); await tools.mcp__docket__create({body: \"needle-value\"});"`, scanner: blockDocketCreate, wantDeny: true, wantReason: "docket/create"},
		{name: "codex exec no MCP call sites unchanged", tool: "exec", raw: `"console.log(1 + 1)"`, scanner: scanDown},
		{name: "codex exec infisical call site allowed unscanned", tool: "exec", raw: `"await tools.mcp__infisical__get_secret({})"`, scanner: scanDown},
		{name: "codex exec scanner error denied", tool: "exec", raw: `"await tools.mcp__kubernetes__pods_list({})"`, scanner: scanDown, wantDeny: true, wantReason: "MCP call to kubernetes/pods_list was blocked"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer.ResetSensitiveTokensForTest()
			defer sanitizer.ResetSensitiveTokensForTest()
			if tc.secret != "" {
				sanitizer.RegisterSensitiveTokens(tc.secret)
			}
			raw := json.RawMessage(tc.raw)
			args := ParseArgs(raw)
			if tc.cmd != "" {
				args.CommandLine = tc.cmd
			}
			args.AdoptClaudeMCPCall(tc.tool, raw)
			args.Namespace = tc.namespace
			var scanner EgressScanner
			if tc.scanner != nil {
				scanner = *tc.scanner
			}
			dec := CheckOutboundSecrets(tc.tool, raw, args, mapEnv(nil), scanner)
			if (dec.Decision == DecisionDeny) != tc.wantDeny {
				t.Fatalf("decision = %v (reason %q), wantDeny %v", dec.Decision, dec.Reason, tc.wantDeny)
			}
			if tc.wantReason != "" && !strings.Contains(dec.Reason, tc.wantReason) {
				t.Fatalf("expected %q in reason, got %q", tc.wantReason, dec.Reason)
			}
			if tc.wantNotReason != "" && strings.Contains(dec.Reason, tc.wantNotReason) {
				t.Fatalf("reason leaks %q: %q", tc.wantNotReason, dec.Reason)
			}
		})
	}
}

func TestHTTPEgressScannerScan(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  int
		body    string
		url     string
		want    EgressScanResult
		wantErr string
	}{
		{name: "blocked result decoded", status: http.StatusOK, body: `{"blocked":true,"violations":[{"category":"InfisicalSecret","sample":"ab***yz"}]}`, want: EgressScanResult{Blocked: true, Violations: []EgressViolation{{Category: "InfisicalSecret", Sample: "ab***yz"}}}},
		{name: "clean result decoded", status: http.StatusOK, body: `{"blocked":false,"violations":[]}`, want: EgressScanResult{Violations: []EgressViolation{}}},
		{name: "503 is an error", status: http.StatusServiceUnavailable, body: `{"blocked":false}`, wantErr: "HTTP 503"},
		{name: "400 is an error", status: http.StatusBadRequest, body: `bad`, wantErr: "HTTP 400"},
		{name: "malformed body is an error", status: http.StatusOK, body: `{"blocked":`, wantErr: "decode egress scan response"},
		{name: "transport failure is an error", url: "http://127.0.0.1:1/internal/guard/egress-scan", wantErr: "egress scan request failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(egressEndpointStub(tc.status, tc.body))
			t.Cleanup(srv.Close)
			url := cmp.Or(tc.url, srv.URL+"/internal/guard/egress-scan")
			scanner := &HTTPEgressScanner{URL: url, Client: srv.Client()}
			got, err := scanner.Scan(context.Background(), EgressScanRequest{Server: "docket", Tool: "create", Arguments: json.RawMessage(`{"body":"x"}`)})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Scan error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Scan error = %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Scan = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// egressEndpointStub answers status/body for a well-formed POST naming docket/create and 418 otherwise.
func egressEndpointStub(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req EgressScanRequest
		if r.Method != http.MethodPost || r.URL.Path != "/internal/guard/egress-scan" ||
			r.Header.Get("Content-Type") != "application/json" ||
			json.NewDecoder(r.Body).Decode(&req) != nil || req.Server != "docket" || req.Tool != "create" ||
			string(req.Arguments) != `{"body":"x"}` {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}
