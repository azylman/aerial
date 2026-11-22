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

	tests := []struct {
		name     string
		tokens   []string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			tokens:   []string{rawSecret},
			input:    "",
			expected: "",
		},
		{
			name:     "clean text and technical prose without registered secrets",
			tokens:   []string{rawSecret},
			input:    "Where they stop at basic containers, foreign_key: user_id, and Bearer concepts.",
			expected: "Where they stop at basic containers, foreign_key: user_id, and Bearer concepts.",
		},
		{
			name:     "exact registered secret redacted",
			tokens:   []string{rawPAT, rawSecret},
			input:    "Error using token " + rawPAT + " and " + rawSecret + " to access repo",
			expected: "Error using token [REDACTED] and [REDACTED] to access repo",
		},
		{
			name:     "base64 std and raw encoded secret redacted",
			tokens:   []string{rawSecret},
			input:    "Encoded std=" + b64Std + " and raw=" + b64Raw + " in payload",
			expected: "Encoded std=[REDACTED] and raw=[REDACTED] in payload",
		},
		{
			name:     "git http basic auth x-access-token base64 redacted",
			tokens:   []string{rawPAT},
			input:    "-c http.extraHeader=AUTHORIZATION: basic " + b64GitBasic,
			expected: "-c http.extraHeader=AUTHORIZATION: basic [REDACTED]",
		},
		{
			name:     "url-escaped secret password redacted",
			tokens:   []string{rawSpecialPass},
			input:    "Connecting to postgresql://aerial_user:super_secret_p%40ss%23word@db.internal.net:5432/aerial_db?sslmode=disable",
			expected: "Connecting to postgresql://aerial_user:[REDACTED]@db.internal.net:5432/aerial_db?sslmode=disable",
		},
		{
			name:     "password extracted from registered database uri",
			tokens:   []string{rawDBURI, "https://example.com/no-user", "postgresql://user_only@db.internal.net:5432/db", "://invalid-uri"},
			input:    "Password leaked: uri_embedded_pass_9876 in trace",
			expected: "Password leaked: [REDACTED] in trace",
		},
		{
			name:     "password with space in database uri redacted using userinfo encoding",
			tokens:   []string{"secret pass"},
			input:    "Connecting to postgresql://aerial_user:secret%20pass@db.internal.net:5432/aerial_db?sslmode=disable",
			expected: "Connecting to postgresql://aerial_user:[REDACTED]@db.internal.net:5432/aerial_db?sslmode=disable",
		},
		{
			name:     "unregistered github token redacted via pattern",
			tokens:   nil,
			input:    "Found leak: ghp_1234567890abcdefghijklmnopqrstuvwxyz",
			expected: "Found leak: [REDACTED]",
		},
		{
			name:     "unregistered x-access-token auth redacted via pattern",
			tokens:   nil,
			input:    "push failed: https://x-access-token:my_secret_token@github.com/repo",
			expected: "push failed: https://[REDACTED]@github.com/repo",
		},
		{
			name:     "unregistered discord webhook url redacted via pattern",
			tokens:   nil,
			input:    "Webhook: https://discord.com/api/webhooks/123456789/abcdefghij-klmn alert",
			expected: "Webhook: [REDACTED] alert",
		},
		{
			name:     "unregistered openai and anthropic key redacted via pattern",
			tokens:   nil,
			input:    "API key: sk-proj-abcdefghijklmnopqrstuvwxyz in environment",
			expected: "API key: [REDACTED] in environment",
		},
		{
			name:     "unregistered google api key redacted via pattern",
			tokens:   nil,
			input:    "Google key: AIzaSyD-1234567890abcdefghijklmnopqrstuv used",
			expected: "Google key: [REDACTED] used",
		},
		{
			name:     "unregistered discord mfa token redacted via pattern",
			tokens:   nil,
			input:    "Session token: mfa.abcdefghijklmnopqrstuvwxyz0123456789 active",
			expected: "Session token: [REDACTED] active",
		},
		{
			name:     "unregistered jwt token redacted via pattern",
			tokens:   nil,
			input:    "Auth token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnopqrstuvwxyz0123456789",
			expected: "Auth token: [REDACTED]",
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

func assertStringTransformTable(t *testing.T, tests []struct {
	name     string
	input    string
	expected string
}, fn func(string) string,
) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := fn(tc.input)
			if got != tc.expected {
				t.Errorf("transform(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSanitizeMentions_TableDriven(t *testing.T) {
	t.Parallel()

	assertStringTransformTable(t, []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "user and role mentions",
			input:    "Hello <@12345678> and <@!87654321> and <@&11223344>!",
			expected: "Hello  and  and !",
		},
		{
			name:     "channel and broadcast mentions",
			input:    "Check <#99887766> @everyone and @here right now",
			expected: "Check   and  right now",
		},
	}, SanitizeMentions)
}

func TestNormalizeWhitespace_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "tabs, newlines and multiple spaces",
			input:    "   hello \t\t world \n\n this is \r a   test   ",
			expected: "hello world this is a test",
		},
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
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "opening and closing user request",
			input:    "<user_request>execute task</user_request>",
			expected: "<\\USER_REQUEST>execute task<\\/USER_REQUEST>",
		},
		{
			name:     "channel instructions self-closing",
			input:    "<channel_instructions />",
			expected: "<\\/CHANNEL_INSTRUCTIONS>",
		},
		{
			name:     "unknown tag left intact",
			input:    "<custom_tag>hello</custom_tag>",
			expected: "<custom_tag>hello</custom_tag>",
		},
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

	assertStringTransformTable(t, []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "author with newlines and angle brackets",
			input:    "  <Evil\nAuthor\r>  ",
			expected: "EvilAuthor",
		},
		{
			name:     "clean author",
			input:    "NormalUser#1234",
			expected: "NormalUser#1234",
		},
	}, SanitizeAuthor)
}

func resetSensitiveTokens() {
	ResetSensitiveTokensForTest()
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
		{
			name:     "empty input",
			tokens:   []string{"secret_token_123"},
			input:    "",
			expected: nil,
		},
		{
			name:     "no registered tokens",
			tokens:   nil,
			input:    "safe string",
			expected: nil,
		},
		{
			name:     "no matches",
			tokens:   []string{"secret_token_123"},
			input:    "completely safe string",
			expected: nil,
		},
		{
			name:     "single match",
			tokens:   []string{"secret_token_123"},
			input:    "error: secret_token_123 failed",
			expected: []string{"se***23"},
		},
		{
			name:     "base64 encoded match deduplicated against raw unpadded prefix",
			tokens:   []string{"secret_token_123"},
			input:    "error: " + b64Sample + " failed",
			expected: []string{MaskTokenSample(b64Sample)},
		},
		{
			name:     "multiple matches deduplicated and longest first",
			tokens:   []string{"token_alpha_long", "token_beta_longer_value", "token_short"},
			input:    "contains token_alpha_long and token_beta_longer_value",
			expected: []string{"to***ue", "to***ng"},
		},
		{
			name:     "short token masked as [REDACTED]",
			tokens:   []string{"123456", "abcd"},
			input:    "123456 and abcd",
			expected: []string{"12***56", "[REDACTED]"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			defer ResetSensitiveTokensForTest()

			if len(tc.tokens) > 0 {
				RegisterSensitiveTokens(tc.tokens...)
			}
			got := MatchSensitiveTokens(tc.input)
			if !slices.Equal(got, tc.expected) {
				t.Errorf("MatchSensitiveTokens(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestResetSensitiveTokensForTest_TableDriven(t *testing.T) {
	tests := []struct {
		name  string
		token string
		input string
	}{
		{
			name:  "token cleared after reset",
			token: "my_temporary_secret_token_999",
			input: "log with my_temporary_secret_token_999",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetSensitiveTokensForTest()
			RegisterSensitiveTokens(tc.token)
			if len(MatchSensitiveTokens(tc.input)) == 0 {
				t.Fatalf("expected token %q to match before reset", tc.token)
			}
			ResetSensitiveTokensForTest()
			if len(MatchSensitiveTokens(tc.input)) != 0 {
				t.Fatalf("expected token %q to NOT match after reset", tc.token)
			}
		})
	}
}

func TestRegisterSensitiveTokens_TableDriven(t *testing.T) {
	// Not t.Parallel() because it mutates global sensitive tokens state
	tests := []struct {
		name         string
		tokens       []string
		input        string
		shouldRedact []string
	}{
		{
			name:         "short token ignored and valid token registered",
			tokens:       []string{"abc", "   ", "my_super_secret_dynamic_token_123"},
			input:        "failed with my_super_secret_dynamic_token_123 and abc",
			shouldRedact: []string{"my_super_secret_dynamic_token_123"},
		},
		{
			name:         "multiple tokens sorted by length",
			tokens:       []string{"secret_token_long", "secret_token_longer_value"},
			input:        "values: secret_token_longer_value and secret_token_long",
			shouldRedact: []string{"secret_token_longer_value", "secret_token_long"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSensitiveTokens()
			defer resetSensitiveTokens()

			RegisterSensitiveTokens(tc.tokens...)
			got := SanitizeString(tc.input)

			for _, redacted := range tc.shouldRedact {
				if strings.Contains(got, redacted) {
					t.Errorf("expected %q to be redacted from %q", redacted, got)
				}
			}
		})
	}
}

func TestReplaceSensitiveTokens_TableDriven(t *testing.T) {
	// Not t.Parallel() because it mutates global sensitive tokens state
	tests := []struct {
		name            string
		initialTokens   []string
		replacedTokens  []string
		input           string
		shouldRedact    []string
		shouldNotRedact []string
	}{
		{
			name:            "replaced tokens purge prior registered secrets",
			initialTokens:   []string{"mycustomsecret_old_1111", "mycustomsecret_preserved_2222"},
			replacedTokens:  []string{"mycustomsecret_preserved_2222", "mycustomsecret_new_3333"},
			input:           "values: mycustomsecret_old_1111 and mycustomsecret_preserved_2222 and mycustomsecret_new_3333",
			shouldRedact:    []string{"mycustomsecret_preserved_2222", "mycustomsecret_new_3333"},
			shouldNotRedact: []string{"mycustomsecret_old_1111"},
		},
		{
			name:            "empty replacement clears all registered tokens",
			initialTokens:   []string{"mycustomsecret_purged_9999"},
			replacedTokens:  nil,
			input:           "plain text with mycustomsecret_purged_9999 included",
			shouldRedact:    nil,
			shouldNotRedact: []string{"mycustomsecret_purged_9999"},
		},
		{
			name:            "replacement filters short tokens and sorts longest first",
			initialTokens:   []string{"mycustomsecret_initial_0000"},
			replacedTokens:  []string{"short", "shorter_val", "longest_replacement_mycustomsecret", "abc", "   "},
			input:           "items: longest_replacement_mycustomsecret and shorter_val and short and abc and mycustomsecret_initial_0000",
			shouldRedact:    []string{"longest_replacement_mycustomsecret", "shorter_val", "short"},
			shouldNotRedact: []string{"mycustomsecret_initial_0000", "abc"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSensitiveTokens()
			defer resetSensitiveTokens()

			if len(tc.initialTokens) > 0 {
				RegisterSensitiveTokens(tc.initialTokens...)
			}
			ReplaceSensitiveTokens(tc.replacedTokens...)
			got := SanitizeString(tc.input)

			for _, redacted := range tc.shouldRedact {
				if strings.Contains(got, redacted) {
					t.Errorf("expected %q to be redacted from %q", redacted, got)
				}
			}
			for _, notRedacted := range tc.shouldNotRedact {
				if !strings.Contains(got, notRedacted) {
					t.Errorf("expected %q to be preserved in %q", notRedacted, got)
				}
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
			name:         "directory mount registers secrets and redacts them",
			path:         secretsDir,
			input:        "found shared_secret_key_val_7890 and ignore_me in text",
			shouldRedact: "shared_secret_key_val_7890",
			preserved:    "ignore_me",
		},
		{
			name:         "json file registers secrets and redacts them",
			path:         jsonFilePath,
			input:        "found json_secret_key_val_1234 in text",
			shouldRedact: "json_secret_key_val_1234",
		},
		{
			name:      "empty path leaves safe text unchanged",
			path:      "",
			input:     "safe text",
			preserved: "safe text",
		},
		{
			name:      "missing path leaves safe text unchanged",
			path:      filepath.Join(tempDir, "missing"),
			input:     "safe text",
			preserved: "safe text",
		},
		{
			name:      "invalid json leaves safe text unchanged",
			path:      invalidJSON,
			input:     "safe text",
			preserved: "safe text",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSensitiveTokens()
			defer resetSensitiveTokens()

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
	}{
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
				"Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+b64Val)),
			},
			nonMatches: []string{"dummy_b64_value", "unrelated text"},
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
		})
	}
}

func TestValuePattern_Ordering_TableDriven(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   []string
	}{
		{
			name:   "duplicate values collapse and longest variant first",
			values: []string{"abcd", "abcd", " abcd "},
			want:   []string{"eC1hY2Nlc3MtdG9rZW46YWJjZA==", "eC1hY2Nlc3MtdG9rZW46YWJjZA", "YWJjZA==", "YWJjZA", "abcd"},
		},
		{
			name:   "longer value precedes shorter overlapping value",
			values: []string{"wxyz", "wxyzwxyz"},
			want:   []string{"wxyzwxyz", "wxyz"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			alts := strings.Split(ValuePattern(tc.values), "|")
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
			for _, w := range tc.want {
				idx := slices.Index(alts, regexp.QuoteMeta(w))
				if idx < 0 {
					t.Fatalf("missing alternative %q in %v", w, alts)
				}
				if idx < last {
					t.Errorf("alternative %q out of order in %v", w, alts)
				}
				last = idx
			}
			re := regexp.MustCompile(ValuePattern(tc.values))
			if got := re.FindString("--" + tc.want[0] + "--"); got != tc.want[0] {
				t.Errorf("leftmost match = %q, want longest %q", got, tc.want[0])
			}
		})
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
