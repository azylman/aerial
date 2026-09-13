package runner

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
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
			omits:    []string{"--input-format", "--model", "--print-timeout", "--conversation", "-p"},
		},
		{
			name: "Interactive Run With Model and Conversation",
			input: AgyArgsInput{
				OutputFormat: "json",
				Model:        "gemini-2.5-flash",
				SessionID:    "11111111-2222-3333-4444-555555555555",
				Prompt:       "Hello world",
				MaxDuration:  10 * time.Minute,
			},
			contains: []string{
				"--dangerously-skip-permissions",
				"--output-format", "json",
				"--model", "gemini-2.5-flash",
				"--print-timeout", "10m",
				"--conversation", "11111111-2222-3333-4444-555555555555",
				"-p", "Hello world",
			},
			omits: []string{"--input-format"},
		},
		{
			name: "Seconds MaxDuration",
			input: AgyArgsInput{
				MaxDuration: 45 * time.Second,
			},
			contains: []string{"--print-timeout", "45s"},
		},
		{
			name: "Worker Mode",
			input: AgyArgsInput{
				WorkerMode:   true,
				OutputFormat: "stream-json",
				Model:        "gemini-pro",
				Prompt:       "ignored prompt in worker mode",
				SessionID:    "22222222-3333-4444-5555-666666666666",
			},
			contains: []string{
				"--dangerously-skip-permissions",
				"--input-format", "stream-json",
				"--output-format", "stream-json",
				"--model", "gemini-pro",
			},
			omits: []string{"-p", "--conversation"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

func TestEvaluateWatchdogStatus(t *testing.T) {
	t.Parallel()
	now := time.Now()
	startTime := now.Add(-10 * time.Minute)

	tests := []struct {
		name         string
		input        WatchdogStatusInput
		wantAction   WatchdogAction
		reasonSubstr string
	}{
		{
			name: "Healthy Activity - No Timeout",
			input: WatchdogStatusInput{
				Now:               now,
				StartTime:         startTime,
				LastActivityTime:  now.Add(-30 * time.Second),
				InactivityTimeout: 5 * time.Minute,
				MaxDuration:       60 * time.Minute,
			},
			wantAction: WatchdogActionNone,
		},
		{
			name: "Inactivity Timeout Exceeded",
			input: WatchdogStatusInput{
				Now:               now,
				StartTime:         startTime,
				LastActivityTime:  now.Add(-6 * time.Minute),
				InactivityTimeout: 5 * time.Minute,
				MaxDuration:       60 * time.Minute,
			},
			wantAction:   WatchdogActionKillInactivity,
			reasonSubstr: "inactivity timeout exceeded",
		},
		{
			name: "Inactivity Rescued by Advancing Disk Activity",
			input: WatchdogStatusInput{
				Now:                  now,
				StartTime:            startTime,
				LastActivityTime:     now.Add(-6 * time.Minute),
				LastSeenDiskActivity: now.Add(-10 * time.Minute),
				LatestDiskActivity:   now.Add(-10 * time.Second),
				InactivityTimeout:    5 * time.Minute,
				MaxDuration:          60 * time.Minute,
			},
			wantAction: WatchdogActionNone,
		},
		{
			name: "Max Duration Exceeded",
			input: WatchdogStatusInput{
				Now:               now,
				StartTime:         now.Add(-65 * time.Minute),
				LastActivityTime:  now.Add(-10 * time.Second),
				InactivityTimeout: 5 * time.Minute,
				MaxDuration:       60 * time.Minute,
			},
			wantAction:   WatchdogActionKillMaxDuration,
			reasonSubstr: "max duration exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := EvaluateWatchdogStatus(tt.input)
			if decision.Action != tt.wantAction {
				t.Errorf("expected action %v, got %v (reason: %q)", tt.wantAction, decision.Action, decision.Reason)
			}
			if tt.reasonSubstr != "" && !strings.Contains(decision.Reason, tt.reasonSubstr) {
				t.Errorf("expected reason to contain %q, got: %q", tt.reasonSubstr, decision.Reason)
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
			name: "Empty Line",
			line: `   `,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
		err := WriteWorkerTurn(nil, "hello")
		if err == nil {
			t.Fatal("expected error on nil writer")
		}
	})

	t.Run("Valid Write", func(t *testing.T) {
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
		expectedErr := errors.New("disk full")
		ew := &errWriter{err: expectedErr}
		err := WriteWorkerTurn(ew, "hello")
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("expected writer error to surface, got: %v", err)
		}
	})
}

func TestEvaluateWatchdogStatus_Defaults(t *testing.T) {
	t.Parallel()
	now := time.Now()
	// Zero Now, InactivityTimeout <= 0, MaxDuration <= 0
	d1 := EvaluateWatchdogStatus(WatchdogStatusInput{
		StartTime:        now.Add(-2 * time.Hour),
		LastActivityTime: now,
	})
	if d1.Action != WatchdogActionKillMaxDuration {
		t.Errorf("expected max duration kill with default 60m cap, got %v", d1.Action)
	}

	d2 := EvaluateWatchdogStatus(WatchdogStatusInput{
		StartTime:        now,
		LastActivityTime: now.Add(-10 * time.Minute),
	})
	if d2.Action != WatchdogActionKillInactivity {
		t.Errorf("expected inactivity kill with default 5m cap, got %v", d2.Action)
	}
}
