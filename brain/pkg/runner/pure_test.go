package runner

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestBuildAgyArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    AgyArgsInput
		contains []string
		omits    []string
	}{
		{
			name:     "Defaults",
			input:    AgyArgsInput{},
			contains: []string{"--dangerously-skip-permissions", "--output-format", "stream-json"},
			omits:    []string{"--input-format", "--model", "--conversation"},
		},
		{
			name: "Interactive Run With Model and Conversation",
			input: AgyArgsInput{
				OutputFormat: "json",
				Model:        "gemini-2.5-flash",
				SessionID:    "11111111-2222-3333-4444-555555555555",
			},
			contains: []string{
				"--dangerously-skip-permissions",
				"--output-format", "json",
				"--model", "gemini-2.5-flash",
				"--conversation", "11111111-2222-3333-4444-555555555555",
			},
			omits: []string{"--input-format"},
		},
		{
			name: "Worker Mode",
			input: AgyArgsInput{
				WorkerMode:   true,
				OutputFormat: "stream-json",
				Model:        "gemini-pro",
				SessionID:    "22222222-3333-4444-5555-666666666666",
			},
			contains: []string{
				"--dangerously-skip-permissions",
				"--input-format", "stream-json",
				"--output-format", "stream-json",
				"--model", "gemini-pro",
				"--conversation", "22222222-3333-4444-5555-666666666666",
			},
			omits: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := BuildAgyArgs(tt.input)
			joined := strings.Join(args, " ")
			for _, c := range tt.contains {
				if !strings.Contains(joined, c) {
					t.Errorf("expected args to contain %q, got: %v", c, args)
				}
			}
			for _, o := range tt.omits {
				for _, arg := range args {
					if arg == o {
						t.Errorf("expected args to omit %q, but found in %v", o, args)
					}
				}
			}
		})
	}
}

func TestBuildAgyEnv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    AgyEnvInput
		contains []string
	}{
		{
			name:  "Defaults Only",
			input: AgyEnvInput{},
			contains: []string{
				"GIT_TERMINAL_PROMPT=0",
				"AGY_LOG_LEVEL=debug",
				"ANTIGRAVITY_LOG_LEVEL=debug",
			},
		},
		{
			name: "Full Configuration",
			input: AgyEnvInput{
				BaseEnv:  []string{"CUSTOM_BASE=1"},
				HomeDir:  "/custom/home",
				APIKey:   "secret-token-123",
				TargetID: "target-agent",
				ExtraEnv: []string{"EXTRA_KEY=foo"},
			},
			contains: []string{
				"CUSTOM_BASE=1",
				"GIT_TERMINAL_PROMPT=0",
				"AGY_LOG_LEVEL=debug",
				"ANTIGRAVITY_LOG_LEVEL=debug",
				"HOME=/custom/home",
				"USERPROFILE=/custom/home",
				"GEMINI_API_KEY=secret-token-123",
				"ANTIGRAVITY_API_KEY=secret-token-123",
				"GOOGLE_GENAI_API_KEY=secret-token-123",
				"AERIAL_TARGET_ID=target-agent",
				"EXTRA_KEY=foo",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := BuildAgyEnv(tt.input)
			joined := strings.Join(env, "\n")
			for _, c := range tt.contains {
				if !strings.Contains(joined, c) {
					t.Errorf("expected env to contain %q, got: %v", c, env)
				}
			}
		})
	}
}


func TestShouldRotateWorker(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		turnsUsed   int
		turnBudget  int
		rssBytes    uint64
		maxRSSBytes uint64
		isDead      bool
		wantRotate  bool
		wantReason  string
	}{
		{
			name:        "Healthy Worker Below Limits",
			turnsUsed:   3,
			turnBudget:  10,
			rssBytes:    100 * 1024 * 1024,
			maxRSSBytes: 500 * 1024 * 1024,
			isDead:      false,
			wantRotate:  false,
		},
		{
			name:       "Dead Worker Rotates Immediately",
			turnsUsed:  1,
			turnBudget: 10,
			isDead:     true,
			wantRotate: true,
			wantReason: "worker is dead",
		},
		{
			name:        "RSS Limit Exceeded",
			turnsUsed:   1,
			turnBudget:  10,
			rssBytes:    600 * 1024 * 1024,
			maxRSSBytes: 500 * 1024 * 1024,
			wantRotate:  true,
			wantReason:  "rss limit exceeded",
		},
		{
			name:        "Turn Budget Exceeded",
			turnsUsed:   10,
			turnBudget:  10,
			rssBytes:    100 * 1024 * 1024,
			maxRSSBytes: 500 * 1024 * 1024,
			wantRotate:  true,
			wantReason:  "turn budget reached",
		},
		{
			name:       "Zero Turn Budget Disables Turn Limit",
			turnsUsed:  100,
			turnBudget: 0,
			wantRotate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rotate, reason := ShouldRotateWorker(tt.turnsUsed, tt.turnBudget, tt.rssBytes, tt.maxRSSBytes, tt.isDead)
			if rotate != tt.wantRotate {
				t.Errorf("expected rotate=%v, got %v (reason: %q)", tt.wantRotate, rotate, reason)
			}
			if tt.wantReason != "" && !strings.Contains(reason, tt.wantReason) {
				t.Errorf("expected reason to contain %q, got: %q", tt.wantReason, reason)
			}
		})
	}
}

func TestParseInitEvent(t *testing.T) {
	t.Parallel()
	validUUID := "11111111-2222-3333-4444-555555555555"

	tests := []struct {
		name   string
		line   string
		wantID string
		wantOk bool
	}{
		{
			name:   "Valid NDJSON Init",
			line:   `{"event":"init","conversation_id":"` + validUUID + `"}`,
			wantID: validUUID,
			wantOk: true,
		},
		{
			name:   "Valid NDJSON Init With Trailing Text",
			line:   `log prefix {"event":"init","conversation_id":"` + validUUID + `"}`,
			wantID: validUUID,
			wantOk: true,
		},
		{
			name:   "Not An Init Event",
			line:   `{"event":"result","status":"SUCCESS"}`,
			wantOk: false,
		},
		{
			name:   "Non-UUID Conversation ID",
			line:   `{"event":"init","conversation_id":"not-a-valid-uuid"}`,
			wantOk: false,
		},
		{
			name:   "Empty String",
			line:   "",
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id, ok := ParseInitEvent(tt.line)
			if ok != tt.wantOk {
				t.Errorf("expected ok=%v, got %v", tt.wantOk, ok)
			}
			if ok && id != tt.wantID {
				t.Errorf("expected id=%q, got %q", tt.wantID, id)
			}
		})
	}
}

func TestIsResultEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		line string
		want bool
	}{
		{
			name: "Standard Result Event",
			line: `{"event":"result","result":{"status":"SUCCESS"}}`,
			want: true,
		},
		{
			name: "Compact Result Event",
			line: `{"event":"result"}`,
			want: true,
		},
		{
			name: "Init Event",
			line: `{"event":"init","conversation_id":"123"}`,
			want: false,
		},
		{
			name: "Arbitrary Log Line",
			line: `2026/09/12 12:00:00 Starting server...`,
			want: false,
		},
		{
			name: "Prefixed Log Line with Result Event",
			line: `[2026-09-22 13:00:00] {"event":"result","result":{"status":"SUCCESS"}}`,
			want: true,
		},
		{
			name: "Step Update with Result Text in Tool Output",
			line: `{"event":"step_update","tool_output":"task finished with result: OK"}`,
			want: false,
		},
		{
			name: "Step Update with Interior JSON in Tool Output",
			line: `{"event":"step_update","tool_output":"{\"event\":\"result\"}"}`,
			want: false,
		},
		{
			name: "Empty Line",
			line: `   `,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsResultEvent(tt.line); got != tt.want {
				t.Errorf("IsResultEvent(%q) = %v, want %v", tt.line, got, tt.want)
			}
		})
	}
}

type errWriter struct {
	err error
}

func (e *errWriter) Write(p []byte) (int, error) {
	return 0, e.err
}

func TestWriteWorkerTurn(t *testing.T) {
	t.Parallel()
	t.Run("Nil Writer", func(t *testing.T) {
		t.Parallel()
		err := WriteWorkerTurn(nil, "hello")
		if err == nil {
			t.Fatal("expected error on nil writer")
		}
	})

	t.Run("Valid Write", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		err := WriteWorkerTurn(&buf, "hello world")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := buf.String()
		if !strings.Contains(got, `"event":"user"`) || !strings.Contains(got, "hello world") || !strings.HasSuffix(got, "\n") {
			t.Errorf("unexpected output payload: %q", got)
		}
	})

	t.Run("Writer Error Surfaces", func(t *testing.T) {
		t.Parallel()
		expectedErr := errors.New("disk full")
		ew := &errWriter{err: expectedErr}
		err := WriteWorkerTurn(ew, "hello")
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("expected writer error to surface, got: %v", err)
		}
	})
}


func TestExtractSubagentID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "standard camelCase object",
			input:    `{"conversationId": "sub-123"}`,
			expected: "sub-123",
		},
		{
			name:     "snake_case object",
			input:    `{"conversation_id": "sub-456"}`,
			expected: "sub-456",
		},
		{
			name:     "array payload",
			input:    `[{"conversationId": "sub-789"}]`,
			expected: "sub-789",
		},
		{
			name:     "array payload with snake_case",
			input:    `[{"conversation_id": "sub-array-snake"}]`,
			expected: "sub-array-snake",
		},
		{
			name:     "text prefix with embedded JSON",
			input:    `Subagent conversation started with conversation ID: subagent-777, {"conversationId": "subagent-777"}`,
			expected: "subagent-777",
		},
		{
			name:     "nested subagents list",
			input:    `{"subagents": [{"conversationId": "sub-nested-1"}]}`,
			expected: "sub-nested-1",
		},
		{
			name:     "logger prefix with braces before JSON",
			input:    `[2026-09-19 {worker-1}] {"conversationId": "sub-logger-id"}`,
			expected: "sub-logger-id",
		},
		{
			name:     "empty input",
			input:    "",
			expected: "",
		},
		{
			name:     "unrelated string",
			input:    "Command completed with status 0",
			expected: "",
		},
		{
			name:     "embedded array with snake_case conversation_id",
			input:    `log prefix [{"conversation_id": "sub-alt-1"}]`,
			expected: "sub-alt-1",
		},
		{
			name:     "direct object with snake_case conversation_id",
			input:    `{"conversation_id": "sub-snake-1"}`,
			expected: "sub-snake-1",
		},
		{
			name:     "direct object with subagents snake_case",
			input:    `{"subagents": [{"conversation_id": "sub-snake-sub"}]}`,
			expected: "sub-snake-sub",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractSubagentID(tc.input)
			if got != tc.expected {
				t.Errorf("extractSubagentID(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestWriteWorkerTurn_NilWriter(t *testing.T) {
	if err := WriteWorkerTurn(nil, "hello"); err == nil {
		t.Errorf("expected error on nil writer")
	}
}

func TestActivityWriter_SessionProbe(t *testing.T) {
	w := NewActivityWriter("")
	n, err := w.Write(nil)
	if n != 0 || err != nil {
		t.Fatalf("expected 0, nil from empty write")
	}

	jsonPayload := []byte(`some log prefix {"session_id": "123e4567-e89b-12d3-a456-426614174000"} some suffix`)
	n, err = w.Write(jsonPayload)
	if err != nil || n != len(jsonPayload) {
		t.Fatalf("write failed: %v", err)
	}
	if w.SessionID() != "123e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("expected session ID extracted via sessionProbe, got %q", w.SessionID())
	}

	// Another writer testing reUpdateStream
	w2 := NewActivityWriter("")
	streamMsg := []byte("Starting conversation update stream for 223e4567-e89b-12d3-a456-426614174000\n")
	_, _ = w2.Write(streamMsg)
	if w2.SessionID() != "223e4567-e89b-12d3-a456-426614174000" {
		t.Errorf("expected session ID from update stream, got %q", w2.SessionID())
	}
}

func TestDefaultWatchdogOptions(t *testing.T) {
	opts := DefaultWatchdogOptions(5)
	if opts.InactivityTimeout == 0 {
		t.Errorf("expected non-zero InactivityTimeout")
	}
	if opts.MaxDuration == 0 {
		t.Errorf("expected non-zero MaxDuration")
	}
}
