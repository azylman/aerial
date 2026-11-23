package sanitizer

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestSanitizeString_Tokens_TableDriven(t *testing.T) {
	rawPAT := "ghp_" + "1234567890"
	rawSecret := "super_secret_val_12345"
	rawSpecialPass := "super_secret_p@ss#word"
	rawDBURI := "postgresql://aerial_user:uri_embedded_pass_9876@db.internal.net:5432/aerial_db"
	b64Std := base64.StdEncoding.EncodeToString([]byte(rawSecret))
	b64Raw := base64.RawStdEncoding.EncodeToString([]byte(rawSecret))
	b64GitBasic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + rawPAT))
	rawPlainKey := "plain_api_key_value_77"
	b64GitBasicPlain := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + rawPlainKey))
	rawRefresh := "1//refresh_tok_abcdef_9876"
	rawAccess := "ya29.access_tok_value_5555"
	jsonCred := `{"client_id":"client-id-value-1234.apps","token_uri":"https://oauth2.example.test/token",` +
		`"token_type":"Bearer","scopes":["scope-one-value"],"id_token_email":"someone@example.test",` +
		`"refresh_token":"` + rawRefresh + `","nested":{"items":[{"access_token":"` + rawAccess + `"}]},"api_key":"short"}`

	tests := []struct {
		name     string
		tokens   []string
		input    string
		expected string
	}{
		{"empty string", []string{rawSecret}, "", ""},
		{
			"clean text and technical prose without registered secrets",
			[]string{rawSecret},
			"Where they stop at basic containers, foreign_key: user_id, and Bearer concepts.",
			"Where they stop at basic containers, foreign_key: user_id, and Bearer concepts.",
		},
		{
			"exact registered secret redacted",
			[]string{rawPAT, rawSecret},
			"Error using token " + rawPAT + " and " + rawSecret + " to access repo",
			"Error using token [REDACTED] and [REDACTED] to access repo",
		},
		{
			"base64 std and raw encoded secret redacted",
			[]string{rawSecret},
			"Encoded std=" + b64Std + " and raw=" + b64Raw + " in payload",
			"Encoded std=[REDACTED]== and raw=[REDACTED] in payload",
		},
		{
			"git http basic auth x-access-token base64 redacted for github token",
			[]string{rawPAT},
			"-c http.extraHeader=AUTHORIZATION: basic " + b64GitBasic,
			"-c http.extraHeader=AUTHORIZATION: basic [REDACTED]=",
		},
		{
			"non-github value has no git basic auth variant but its aligned base64 still masks",
			[]string{rawPlainKey},
			"basic " + b64GitBasicPlain,
			"basic eC1hY2Nlc3MtdG9rZW46[REDACTED]==",
		},
		{
			"json object secret registers credential leaves only",
			[]string{jsonCred},
			"refresh " + rawRefresh + " access " + rawAccess + " uri https://oauth2.example.test/token client client-id-value-1234.apps",
			"refresh [REDACTED] access [REDACTED] uri https://oauth2.example.test/token client client-id-value-1234.apps",
		},
		{
			"json object leaf gets encoded variants",
			[]string{jsonCred},
			"enc " + base64.StdEncoding.EncodeToString([]byte(rawRefresh)) + " esc " + strings.ReplaceAll(rawRefresh, "/", "%2F"),
			"enc [REDACTED]= esc [REDACTED]",
		},
		{
			"url-escaped secret password redacted",
			[]string{rawSpecialPass},
			"Connecting to postgresql://aerial_user:super_secret_p%40ss%23word@db.internal.net:5432/aerial_db?sslmode=disable",
			"Connecting to postgresql://aerial_user:[REDACTED]@db.internal.net:5432/aerial_db?sslmode=disable",
		},
		{
			"password extracted from registered database uri",
			[]string{rawDBURI, "https://example.com/no-user", "postgresql://user_only@db.internal.net:5432/db", "://invalid-uri"},
			"Password leaked: uri_embedded_pass_9876 in trace", "Password leaked: [REDACTED] in trace",
		},
		{
			"password with space in database uri redacted using userinfo encoding",
			[]string{"secret pass"},
			"Connecting to postgresql://aerial_user:secret%20pass@db.internal.net:5432/aerial_db?sslmode=disable",
			"Connecting to postgresql://aerial_user:[REDACTED]@db.internal.net:5432/aerial_db?sslmode=disable",
		},
		{"unregistered github token redacted via pattern", nil, "Found leak: ghp_1234567890abcdefghijklmnopqrstuvwxyz", "Found leak: [REDACTED]"},
		{
			"unregistered x-access-token auth redacted via pattern", nil, "push failed: https://x-access-token:my_secret_token@github.com/repo",
			"push failed: https://[REDACTED]@github.com/repo",
		},
		{
			"unregistered discord webhook url redacted via pattern", nil, "Webhook: https://discord.com/api/webhooks/123456789/abcdefghij-klmn alert",
			"Webhook: [REDACTED] alert",
		},
		{
			"unregistered openai and anthropic key redacted via pattern", nil, "API key: sk-proj-abcdefghijklmnopqrstuvwxyz in environment",
			"API key: [REDACTED] in environment",
		},
		{
			"unregistered google api key redacted via pattern", nil, "Google key: AIzaSyD-1234567890abcdefghijklmnopqrstuv used",
			"Google key: [REDACTED] used",
		},
		{
			"unregistered discord mfa token redacted via pattern", nil, "Session token: mfa.abcdefghijklmnopqrstuvwxyz0123456789 active",
			"Session token: [REDACTED] active",
		},
		{
			"unregistered jwt token redacted via pattern", nil,
			"Auth token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnopqrstuvwxyz0123456789",
			"Auth token: [REDACTED]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			defer ResetSensitiveTokensForTest()
			RegisterSensitiveTokens(tc.tokens...)

			got := SanitizeString(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeString(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizeMentions_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"user and role mentions", "Hello <@12345678> and <@!87654321> and <@&11223344>!", "Hello  and  and !"},
		{"channel and broadcast mentions", "Check <#99887766> @everyone and @here right now", "Check   and  right now"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeMentions(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeMentions(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestNormalizeWhitespace_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"tabs, newlines and multiple spaces", "   hello \t\t world \n\n this is \r a   test   ", "hello world this is a test"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := NormalizeWhitespace(tc.input)
			if got != tc.expected {
				t.Errorf("NormalizeWhitespace(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizePromptTags_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"opening and closing user request", "<user_request>execute task</user_request>", "<\\USER_REQUEST>execute task<\\/USER_REQUEST>"},
		{"channel instructions self-closing", "<channel_instructions />", "<\\/CHANNEL_INSTRUCTIONS>"},
		{"unknown tag left intact", "<custom_tag>hello</custom_tag>", "<custom_tag>hello</custom_tag>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizePromptTags(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizePromptTags(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizeAuthor_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"author with newlines and angle brackets", "  <Evil\nAuthor\r>  ", "EvilAuthor"},
		{"clean author", "NormalUser#1234", "NormalUser#1234"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeAuthor(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeAuthor(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestMaskTokenSample_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty", input: "", expected: "[REDACTED]"},
		{name: "short whitespace only", input: "   ", expected: "[REDACTED]"},
		{name: "short token less than 6 chars", input: "abc", expected: "[REDACTED]"},
		{name: "5 chars trimmed", input: "  abcde  ", expected: "[REDACTED]"},
		{name: "exactly 6 chars", input: "123456", expected: "12***56"},
		{name: "longer token", input: "ghp_secrettokenvalue12345", expected: "gh***45"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := MaskTokenSample(tc.input)
			if got != tc.expected {
				t.Errorf("MaskTokenSample(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestMatchSensitiveTokens_TableDriven(t *testing.T) {
	b64Sample := base64.StdEncoding.EncodeToString([]byte("secret_token_123"))
	tests := []struct {
		name     string
		tokens   []string
		input    string
		expected []string
	}{
		{"empty input", []string{"secret_token_123"}, "", nil},
		{"no registered tokens", nil, "safe string", nil},
		{"no matches", []string{"secret_token_123"}, "completely safe string", nil},
		{"single match", []string{"secret_token_123"}, "error: secret_token_123 failed", []string{"se***23"}},
		{
			"base64 encoded match deduplicated against raw unpadded prefix",
			[]string{"secret_token_123"},
			"error: " + b64Sample + " failed",
			[]string{"c2***Mw"},
		},
		{
			"multiple matches deduplicated and longest first",
			[]string{"token_alpha_long", "token_beta_longer_value", "token_short"},
			"contains token_alpha_long and token_beta_longer_value",
			[]string{"to***ue", "to***ng"},
		},
		{"short token masked as [REDACTED]", []string{"123456", "abcd"}, "123456 and abcd", []string{"12***56", "[REDACTED]"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			defer ResetSensitiveTokensForTest()

			RegisterSensitiveTokens(tc.tokens...)
			got := MatchSensitiveTokens(tc.input)
			if !slices.Equal(got, tc.expected) {
				t.Errorf("MatchSensitiveTokens(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestRegisterSensitiveTokens_TableDriven(t *testing.T) {
	// Not t.Parallel() because it mutates global sensitive tokens state
	tests := []struct {
		name   string
		tokens []string
		input  string
		want   string
	}{
		{
			"short token ignored and valid token registered",
			[]string{"abc", "   ", "my_super_secret_dynamic_token_123"},
			"failed with my_super_secret_dynamic_token_123 and abc", "failed with [REDACTED] and abc",
		},
		{
			"multiple tokens sorted by length",
			[]string{"secret_token_long", "secret_token_longer_value"},
			"values: secret_token_longer_value and secret_token_long", "values: [REDACTED] and [REDACTED]",
		},
		{
			"runtime github app installation token keeps git basic auth variant",
			[]string{"ghs_inst_123"},
			"basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ghs_inst_123")), "basic [REDACTED]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			defer ResetSensitiveTokensForTest()

			RegisterSensitiveTokens(tc.tokens...)
			if got := SanitizeString(tc.input); got != tc.want {
				t.Errorf("SanitizeString(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestReplaceSensitiveTokens_TableDriven(t *testing.T) {
	// Not t.Parallel() because it mutates global sensitive tokens state
	tests := []struct {
		name           string
		initialTokens  []string
		replacedTokens []string
		input          string
		want           string
	}{
		{
			name: "replaced tokens purge prior registered secrets", initialTokens: []string{"mycustomsecret_old_1111", "mycustomsecret_preserved_2222"},
			replacedTokens: []string{"mycustomsecret_preserved_2222", "mycustomsecret_new_3333"},
			input:          "values: mycustomsecret_old_1111 and mycustomsecret_preserved_2222 and mycustomsecret_new_3333",
			want:           "values: mycustomsecret_old_1111 and [REDACTED] and [REDACTED]",
		},
		{
			name: "empty replacement clears all registered tokens", initialTokens: []string{"mycustomsecret_purged_9999"},
			input: "plain text with mycustomsecret_purged_9999 included", want: "plain text with mycustomsecret_purged_9999 included",
		},
		{
			name: "replacement filters short tokens and sorts longest first", initialTokens: []string{"mycustomsecret_initial_0000"},
			replacedTokens: []string{"short", "shorter_val", "longest_replacement_mycustomsecret", "abc", "   "},
			input:          "items: longest_replacement_mycustomsecret and shorter_val and short and abc and mycustomsecret_initial_0000",
			want:           "items: [REDACTED] and [REDACTED] and [REDACTED] and abc and mycustomsecret_initial_0000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			defer ResetSensitiveTokensForTest()

			RegisterSensitiveTokens(tc.initialTokens...)
			ReplaceSensitiveTokens(tc.replacedTokens...)
			if got := SanitizeString(tc.input); got != tc.want {
				t.Errorf("SanitizeString(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestRegisterSharedSecrets_TableDriven(t *testing.T) {
	tempDir := t.TempDir()
	secretsDir := filepath.Join(tempDir, "shared_secrets")
	if err := os.MkdirAll(secretsDir, 0o755); err != nil {
		t.Fatalf("failed to create secrets dir: %v", err)
	}
	_ = os.WriteFile(filepath.Join(secretsDir, "SECRET_KEY"), []byte("shared_secret_key_val_7890"), 0o600)
	_ = os.WriteFile(filepath.Join(secretsDir, "EMPTY_KEY"), []byte("   "), 0o600)
	_ = os.WriteFile(filepath.Join(secretsDir, ".hidden"), []byte("ignore_me"), 0o600)
	_ = os.MkdirAll(filepath.Join(secretsDir, "subdir"), 0o755)
	oauthDir := filepath.Join(tempDir, "oauth_secrets")
	_ = os.MkdirAll(oauthDir, 0o755)
	_ = os.WriteFile(filepath.Join(oauthDir, "OAUTH_JSON"),
		[]byte(`{"refresh_token":"shared_refresh_tok_4321","token_uri":"https://oauth.example.test/tok"}`), 0o600)

	jsonFilePath := filepath.Join(tempDir, "shared_secrets.json")
	_ = os.WriteFile(jsonFilePath, []byte(`{"JSON_SECRET":"json_secret_key_val_1234"}`), 0o600)
	invalidJSON := filepath.Join(tempDir, "invalid.json")
	_ = os.WriteFile(invalidJSON, []byte(`{invalid`), 0o600)

	tests := []struct {
		name         string
		path         string
		input        string
		shouldRedact string
		preserved    string
	}{
		{
			"directory mount registers secrets and redacts them", secretsDir, "found shared_secret_key_val_7890 and ignore_me in text",
			"shared_secret_key_val_7890", "ignore_me",
		},
		{
			"json object secret file registers credential leaf only", oauthDir, "leak shared_refresh_tok_4321 via https://oauth.example.test/tok",
			"shared_refresh_tok_4321", "https://oauth.example.test/tok",
		},
		{"json file registers secrets and redacts them", jsonFilePath, "found json_secret_key_val_1234 in text", "json_secret_key_val_1234", ""},
		{"empty path leaves safe text unchanged", "", "safe text", "", "safe text"},
		{"missing path leaves safe text unchanged", filepath.Join(tempDir, "missing"), "safe text", "", "safe text"},
		{"invalid json leaves safe text unchanged", invalidJSON, "safe text", "", "safe text"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			defer ResetSensitiveTokensForTest()

			RegisterSharedSecrets(tc.path)
			got := SanitizeString(tc.input)

			if tc.shouldRedact != "" {
				if !strings.Contains(got, "[REDACTED]") || strings.Contains(got, tc.shouldRedact) {
					t.Errorf("expected %q to be redacted from %q", tc.shouldRedact, got)
				}
			}
			if tc.preserved != "" {
				if !strings.Contains(got, tc.preserved) {
					t.Errorf("expected %q to be preserved in %q", tc.preserved, got)
				}
			}
		})
	}
}

func TestValuePattern_TableDriven(t *testing.T) {
	metaVal := `a.b*c+d?(e)[f]{g}|h^$\i`
	b64Val := "dummy_b64_value_abc"
	urlVal := "dummy pass/with&chars"
	tests := []struct {
		name       string
		values     []string
		wantExact  string
		matches    []string
		nonMatches []string
		wantOrder  []string
	}{
		{
			name:      "duplicate values collapse and longest variant first",
			values:    []string{"abcd", "abcd", " abcd "},
			wantOrder: []string{"YWJjZA", "abcd"},
		},
		{
			name:      "github token gets unpadded git basic auth variant only",
			values:    []string{"ghp_abcd"},
			wantOrder: []string{"eC1hY2Nlc3MtdG9rZW46Z2hwX2FiY2Q", "Z2hwX2FiY2Q", "ghp_abcd"},
			matches:   []string{"Basic eC1hY2Nlc3MtdG9rZW46Z2hwX2FiY2Q=", "Z2hwX2FiY2Q="},
		},
		{
			name:       "json object value contributes credential leaves not the blob",
			values:     []string{`{"client_secret":"leaf_secret_123","token_uri":"https://uri.example.test","client_id":"cid_value_12345"}`},
			wantOrder:  []string{"bGVhZl9zZWNyZXRfMTIz", "leaf_secret_123"},
			matches:    []string{"x leaf_secret_123 y"},
			nonMatches: []string{"https://uri.example.test", "cid_value_12345", `"client_secret"`},
		},
		{
			name:       "json object without credential leaves matches nothing",
			values:     []string{`{"client_id":"cid_value_12345","password":"short"}`},
			wantExact:  `[^\s\S]`,
			nonMatches: []string{"cid_value_12345", "short"},
		},
		{
			name:      "longer value precedes shorter overlapping value",
			values:    []string{"wxyz", "wxyzwxyz"},
			wantOrder: []string{"wxyzwxyz", "wxyz"},
		},
		{
			name:       "no values matches nothing",
			values:     nil,
			wantExact:  `[^\s\S]`,
			nonMatches: []string{"", "anything", " "},
		},
		{
			name:       "only short and blank values matches nothing",
			values:     []string{"abc", "  ", ""},
			wantExact:  `[^\s\S]`,
			nonMatches: []string{"", "abc", "xx abc yy"},
		},
		{
			name:       "metacharacter heavy value matched literally",
			values:     []string{metaVal},
			matches:    []string{"prefix " + metaVal + " suffix"},
			nonMatches: []string{"abbbc+d", "a.b*c+d?(e)[f]{g}"},
		},
		{
			name:   "base64 variants matched inside surrounding text",
			values: []string{b64Val},
			matches: []string{
				"hdr=" + base64.StdEncoding.EncodeToString([]byte(b64Val)) + ";",
				"x" + base64.RawURLEncoding.EncodeToString([]byte(b64Val)) + "x",
				"pad " + base64.URLEncoding.EncodeToString([]byte(b64Val)),
			},
			nonMatches: []string{"dummy_b64_value", "unrelated text", "Basic eC1hY2Nlc3MtdG9rZW46"},
		},
		{
			name:       "url encoded variant matched inside surrounding text",
			values:     []string{urlVal},
			matches:    []string{"?q=dummy+pass%2Fwith%26chars&x=1", "raw dummy pass/with&chars end"},
			nonMatches: []string{"dummy+pass"},
		},
		{
			name:       "short value skipped while long value kept",
			values:     []string{"xyz", "longenough"},
			matches:    []string{"a longenough b"},
			nonMatches: []string{"xyz"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ValuePattern(tc.values)
			if tc.wantExact != "" && got != tc.wantExact {
				t.Fatalf("ValuePattern() = %q, want %q", got, tc.wantExact)
			}
			re, err := regexp.Compile(got)
			if err != nil {
				t.Fatalf("pattern does not compile: %v", err)
			}
			for _, s := range tc.matches {
				if !re.MatchString(s) {
					t.Errorf("pattern should match %q", s)
				}
			}
			for _, s := range tc.nonMatches {
				if re.MatchString(s) {
					t.Errorf("pattern should not match %q", s)
				}
			}
			assertValuePatternOrder(t, re, got, tc.wantOrder)
		})
	}
}

func assertValuePatternOrder(t *testing.T, re *regexp.Regexp, pattern string, wantOrder []string) {
	t.Helper()
	if len(wantOrder) == 0 {
		return
	}
	alts := strings.Split(pattern, "|")
	seen := make(map[string]bool, len(alts))
	for i, a := range alts {
		if seen[a] {
			t.Errorf("duplicate alternative %q", a)
		}
		seen[a] = true
		if i > 0 && len(alts[i-1]) < len(a) {
			t.Errorf("alternative %q (len %d) follows shorter %q", a, len(a), alts[i-1])
		}
	}
	last := -1
	for _, w := range wantOrder {
		idx := slices.Index(alts, regexp.QuoteMeta(w))
		if idx < 0 {
			t.Fatalf("missing alternative %q in %v", w, alts)
		}
		if idx < last {
			t.Errorf("alternative %q out of order in %v", w, alts)
		}
		last = idx
	}
	if m := re.FindString("--" + wantOrder[0] + "--"); m != wantOrder[0] {
		t.Errorf("leftmost match = %q, want longest %q", m, wantOrder[0])
	}
}

func TestOTTLStringBody_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{name: "empty", pattern: "", want: ""},
		{name: "plain", pattern: "abcd|efgh", want: "abcd|efgh"},
		{name: "match nothing", pattern: `[^\s\S]`, want: `[^\\s\\S]`},
		{name: "quote and backslash", pattern: `a"b\.c`, want: `a\"b\\.c`},
		{name: "quoted metachar value", pattern: ValuePattern([]string{`say "hi".now`}), want: ""},
	}
	unescape := strings.NewReplacer(`\\`, `\`, `\"`, `"`)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := OTTLStringBody(tc.pattern)
			if tc.want != "" && got != tc.want {
				t.Fatalf("OTTLStringBody(%q) = %q, want %q", tc.pattern, got, tc.want)
			}
			if back := unescape.Replace(got); back != tc.pattern {
				t.Errorf("round trip = %q, want %q", back, tc.pattern)
			}
		})
	}
}

func TestSensitiveTokenCount_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []string
		wantSome bool
	}{
		{name: "empty pool counts zero"},
		{name: "registered value counts", tokens: []string{"count_test_secret_value_42"}, wantSome: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			defer ResetSensitiveTokensForTest()
			ReplaceSensitiveTokens(tc.tokens...)
			if got := SensitiveTokenCount() > 0; got != tc.wantSome {
				t.Fatalf("SensitiveTokenCount() > 0 = %t, want %t", got, tc.wantSome)
			}
		})
	}
}
