package transcript

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

func TestClipContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		content   string
		headLimit int
		tailLimit int
		wantLen   int
		hasMarker bool
		want      string
	}{
		{
			name:      "short content untouched",
			content:   "hello world",
			headLimit: 10,
			tailLimit: 10,
			hasMarker: false,
			want:      "hello world",
		},
		{
			name:      "exact boundary content untouched",
			content:   "1234567890",
			headLimit: 5,
			tailLimit: 5,
			hasMarker: false,
			want:      "1234567890",
		},
		{
			name:      "oversized content clipped",
			content:   "HEAD-12345--MIDDLE-SPAM--67890-TAIL",
			headLimit: 10,
			tailLimit: 10,
			hasMarker: true,
			want:      "HEAD-12345" + TruncationMarker + "67890-TAIL",
		},
		{
			name:      "default limits when 0 passed",
			content:   strings.Repeat("a", 60000),
			headLimit: 0,
			tailLimit: 0,
			wantLen:   DefaultHeadLimit + len(TruncationMarker) + DefaultTailLimit,
			hasMarker: true,
		},
		{
			name:      "negative limits normalized to zero",
			content:   "some test content",
			headLimit: -5,
			tailLimit: 4,
			hasMarker: true,
			want:      "" + TruncationMarker + "tent",
		},
		{
			name:      "unicode multibyte runes preserved cleanly",
			content:   "🚀🌟🎉" + strings.Repeat("✨", 100) + "🔥⚡🌈",
			headLimit: 3,
			tailLimit: 3,
			hasMarker: true,
			want:      "🚀🌟🎉" + TruncationMarker + "🔥⚡🌈",
		},
		{
			name:      "default limits when both zero on short text",
			content:   "short content",
			headLimit: 0,
			tailLimit: 0,
			hasMarker: false,
			want:      "short content",
		},
		{
			name:      "default limits when both negative on short text",
			content:   "short content",
			headLimit: -10,
			tailLimit: -20,
			hasMarker: false,
			want:      "short content",
		},
		{
			name:      "oversized text with default limits 0 and -1",
			content:   strings.Repeat("x", DefaultHeadLimit+DefaultTailLimit+100),
			headLimit: 0,
			tailLimit: -1,
			wantLen:   DefaultHeadLimit + len(TruncationMarker) + DefaultTailLimit,
			hasMarker: true,
		},
		{
			name:      "negative head limit positive tail limit",
			content:   "0123456789abcdefghij",
			headLimit: -5,
			tailLimit: 5,
			hasMarker: true,
			want:      TruncationMarker + "fghij",
		},
		{
			name:      "positive head limit negative tail limit",
			content:   "0123456789abcdefghij",
			headLimit: 5,
			tailLimit: -5,
			hasMarker: true,
			want:      "01234" + TruncationMarker,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ClipContent(tc.content, tc.headLimit, tc.tailLimit)
			if tc.want != "" && got != tc.want {
				t.Fatalf("ClipContent() = %q, want %q", got, tc.want)
			}
			if tc.wantLen > 0 && len(got) != tc.wantLen {
				t.Fatalf("ClipContent() len = %d, wantLen %d", len(got), tc.wantLen)
			}
			if tc.hasMarker && !strings.Contains(got, TruncationMarker) {
				t.Fatalf("expected truncation marker in %q", got)
			}
			if !tc.hasMarker && strings.Contains(got, TruncationMarker) {
				t.Fatalf("did not expect truncation marker in %q", got)
			}
		})
	}
}

func TestCleanSessionSummary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		raw      string
		maxRunes int
		want     string
	}{
		{"strips memory metadata and request tags", "<USER_REQUEST>\n<retrieved_memory>\n- [id:1] fact\n</retrieved_memory>\n<USER_REQUEST>\nCheck the token metrics\nOnly formulate and output your final response once all immediate work is complete.\n</USER_REQUEST>\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\ntime\n</ADDITIONAL_METADATA>\n<USER_SETTINGS_CHANGE>\nmodel\n</USER_SETTINGS_CHANGE>", 0, "Check the token metrics"},
		{"unclosed block uses last user request tag", "<CHANNEL_HISTORY>unclosed...\n<USER_REQUEST>\nInvestigate build queue\n</USER_REQUEST>", 100, "Investigate build queue"},
		{"envelope-only content falls back to raw", "<USER_REQUEST></USER_REQUEST>", 100, "<USER_REQUEST></USER_REQUEST>"},
		{"truncates at maxRunes with marker", "0123456789abcdef", 10, "0123456789... [truncated]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CleanSessionSummary(tc.raw, tc.maxRunes); got != tc.want {
				t.Fatalf("CleanSessionSummary() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsSubstantiveStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stepType string
		toolName string
		content  string
		want     bool
	}{
		{
			name:     "empty content",
			stepType: "TOOL",
			toolName: "run_command",
			content:  "",
			want:     false,
		},
		{
			name:     "whitespace only content",
			stepType: "TOOL",
			toolName: "run_command",
			content:  "   \n\t  ",
			want:     false,
		},
		{
			name:     "heartbeat step type",
			stepType: "heartbeat",
			toolName: "",
			content:  "ping",
			want:     false,
		},
		{
			name:     "heartbeat tool name",
			stepType: "TOOL",
			toolName: "heartbeat",
			content:  "alive",
			want:     false,
		},
		{
			name:     "short heartbeat content",
			stepType: "GENERIC",
			toolName: "",
			content:  "heartbeat ack",
			want:     false,
		},
		{
			name:     "manage_task status tool name",
			stepType: "TOOL",
			toolName: "manage_task status",
			content:  `{"task_id":"123"}`,
			want:     false,
		},
		{
			name:     "manage_task polling status action",
			stepType: "TOOL",
			toolName: "manage_task",
			content:  `{"Action":"status","TaskId":"task-123"}`,
			want:     false,
		},
		{
			name:     "short manage_task status message",
			stepType: "GENERIC",
			toolName: "",
			content:  "checking manage_task status on background task",
			want:     false,
		},
		{
			name:     "substantive manage_task kill action",
			stepType: "TOOL",
			toolName: "manage_task",
			content:  `{"Action":"kill","TaskId":"task-999"}`,
			want:     true,
		},
		{
			name:     "substantive run_command execution",
			stepType: "TOOL",
			toolName: "run_command",
			content:  "go test ./... passed successfully with 100% coverage",
			want:     true,
		},
		{
			name:     "substantive user input",
			stepType: "USER_INPUT",
			toolName: "",
			content:  "Please refactor the scheduler to use persistent ticks.",
			want:     true,
		},
		{
			name:     "substantive planner response",
			stepType: "PLANNER_RESPONSE",
			toolName: "",
			content:  "I will now update the database interfaces.",
			want:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsSubstantiveStep(tc.stepType, tc.toolName, tc.content)
			if got != tc.want {
				t.Fatalf("IsSubstantiveStep(%q, %q, %q) = %v, want %v",
					tc.stepType, tc.toolName, tc.content, got, tc.want)
			}
		})
	}
}

func newSearchTestStore(t *testing.T, seedCase int) *dbtest.FakeStore {
	t.Helper()
	store := dbtest.NewFakeStore()
	ctx := context.Background()

	switch seedCase {
	case 1:
		_ = store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-1",
			Summary:   "Fixed Docker compose networking issue in staging environment",
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
		_ = store.BatchInsertTranscriptSteps(ctx, []db.TranscriptStep{
			{
				SessionID: "sess-1",
				StepIndex: 1,
				StepType:  "TOOL",
				ToolName:  "run_command",
				Content:   "docker compose up -d",
			},
		})
	case 2:
		_ = store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-2",
			Summary:   "Database migration runbook",
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
		_ = store.BatchInsertTranscriptSteps(ctx, []db.TranscriptStep{
			{
				SessionID: "sess-2",
				StepIndex: 1,
				StepType:  "TOOL",
				ToolName:  "run_command",
				Content:   "go run ./scripts/migrate.go --up",
			},
		})
	case 3:
		dummyEmb := make([]float32, db.ExpectedEmbeddingDim)
		dummyEmb[0] = 1.0
		_ = store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-3",
			Summary:   "Deploying Postgres pgvector instance",
			Embedding: dummyEmb,
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
		_ = store.BatchInsertTranscriptSteps(ctx, []db.TranscriptStep{
			{
				SessionID: "sess-3",
				StepIndex: 1,
				StepType:  "TOOL",
				ToolName:  "docker",
				Content:   "docker run --name pgvector -d pgvector/pgvector:pg16",
			},
		})
	case 4:
		_ = store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-4",
			Summary:   "Configured Grafana monitoring dashboards",
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
	}

	return store
}

func TestSearchTranscripts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		seedCase     int
		nilStore     bool
		closedStore  bool
		cancelCtx    bool
		nilCtx       bool
		embedderType string
		query        string
		mode         string
		sessionID    string
		toolName     string
		limit        int
		wantErr      bool
		wantSubstr   string
		wantSessions int
		wantSteps    int
		wantTotal    int
		wantMode     string
	}{
		{
			name:       "nil store returns error",
			nilStore:   true,
			query:      "query",
			mode:       "auto",
			limit:      10,
			wantErr:    true,
			wantSubstr: "transcript store cannot be nil",
		},
		{
			name:       "context cancelled returns error",
			cancelCtx:  true,
			query:      "query",
			mode:       "auto",
			limit:      10,
			wantErr:    true,
			wantSubstr: "context canceled",
		},
		{
			name:         "mode sessions searches only session summaries",
			seedCase:     1,
			query:        "Docker compose networking",
			mode:         "sessions",
			limit:        5,
			wantSessions: 1,
			wantSteps:    0,
			wantTotal:    1,
		},
		{
			name:         "mode steps searches only transcript steps",
			seedCase:     2,
			query:        "migrate.go",
			mode:         "steps",
			sessionID:    "sess-2",
			toolName:     "run_command",
			limit:        5,
			wantSessions: 0,
			wantSteps:    1,
			wantTotal:    1,
		},
		{
			name:         "mode auto searches both sessions and steps with embedder",
			seedCase:     3,
			embedderType: "dummy",
			query:        "pgvector",
			mode:         "auto",
			limit:        10,
			wantSessions: 1,
			wantSteps:    1,
			wantTotal:    2,
		},
		{
			name:         "embedder error falls back gracefully to text search",
			seedCase:     4,
			embedderType: "fail",
			query:        "Grafana",
			mode:         "sessions",
			limit:        10,
			wantSessions: 1,
			wantSteps:    0,
			wantTotal:    1,
		},
		{
			name:     "nil ctx and default limit",
			nilCtx:   true,
			query:    "any",
			mode:     "unknown_mode",
			limit:    0,
			wantMode: "auto",
		},
		{
			name:        "store search error propagation sessions",
			closedStore: true,
			query:       "query",
			mode:        "sessions",
			limit:       5,
			wantErr:     true,
		},
		{
			name:        "store search error propagation steps",
			closedStore: true,
			query:       "query",
			mode:        "steps",
			limit:       5,
			wantErr:     true,
		},
		{
			name:         "empty query with mode steps and session returns steps",
			seedCase:     2,
			query:        "",
			mode:         "steps",
			sessionID:    "sess-2",
			limit:        5,
			wantSessions: 0,
			wantSteps:    1,
			wantTotal:    1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var store db.TranscriptStore
			if !tc.nilStore {
				fakeStore := newSearchTestStore(t, tc.seedCase)
				if tc.closedStore {
					_ = fakeStore.Close()
				}
				store = fakeStore
			}

			var ctx context.Context
			if tc.nilCtx {
				ctx = nil
			} else {
				ctx = context.Background()
				if tc.cancelCtx {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
			}

			var embedder func(ctx context.Context, text string) ([]float32, error)
			switch tc.embedderType {
			case "dummy":
				embedder = func(ctx context.Context, text string) ([]float32, error) {
					v := make([]float32, db.ExpectedEmbeddingDim)
					v[0] = 1.0
					return v, nil
				}
			case "fail":
				embedder = func(ctx context.Context, text string) ([]float32, error) {
					return nil, errors.New("ollama service unavailable")
				}
			}

			res, err := SearchTranscripts(ctx, store, embedder, tc.query, tc.mode, tc.sessionID, tc.toolName, tc.limit)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("SearchTranscripts() unexpected error: %v", err)
			}

			if tc.wantMode != "" && res.Mode != tc.wantMode {
				t.Errorf("expected Mode %q, got %q", tc.wantMode, res.Mode)
			}
			if tc.wantSessions != 0 || tc.wantSteps != 0 || tc.wantTotal != 0 {
				if len(res.Sessions) != tc.wantSessions {
					t.Errorf("expected %d sessions, got %d", tc.wantSessions, len(res.Sessions))
				}
				if len(res.Steps) != tc.wantSteps {
					t.Errorf("expected %d steps, got %d", tc.wantSteps, len(res.Steps))
				}
				if res.Total != tc.wantTotal {
					t.Errorf("expected Total %d, got %d", tc.wantTotal, res.Total)
				}
			}
			if tc.mode == "steps" && tc.sessionID != "" && len(res.Steps) > 0 {
				if res.Steps[0].SessionID != tc.sessionID {
					t.Errorf("expected step SessionID %q, got %q", tc.sessionID, res.Steps[0].SessionID)
				}
			}
		})
	}
}

func TestDefaultSyncOptions(t *testing.T) {
	t.Parallel()
	opts := DefaultSyncOptions()
	if opts.BatchLimit != DefaultBatchLimit {
		t.Errorf("expected BatchLimit = %d, got %d", DefaultBatchLimit, opts.BatchLimit)
	}
	if opts.IdleThreshold != 10*time.Minute {
		t.Errorf("expected IdleThreshold = 10m, got %v", opts.IdleThreshold)
	}
	if opts.HeadLimit != 15000 {
		t.Errorf("expected HeadLimit = 15000, got %d", opts.HeadLimit)
	}
	if opts.TailLimit != 35000 {
		t.Errorf("expected TailLimit = 35000, got %d", opts.TailLimit)
	}
}

func TestGetTranscriptStepWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilStore    bool
		closedStore bool
		cancelCtx   bool
		nilCtx      bool
		sessionID   string
		stepIndex   int
		context     int
		wantErr     bool
		wantSubstr  string
		wantCount   int
		wantContext int
		wantCenter  int
	}{
		{
			name:       "nil store returns error",
			nilStore:   true,
			sessionID:  "sess-win-1",
			stepIndex:  5,
			context:    2,
			wantErr:    true,
			wantSubstr: "transcript store cannot be nil",
		},
		{
			name:        "closed store returns error",
			closedStore: true,
			sessionID:   "sess-win-1",
			stepIndex:   5,
			context:     2,
			wantErr:     true,
			wantSubstr:  "get transcript step range",
		},
		{
			name:       "cancelled context returns error",
			cancelCtx:  true,
			sessionID:  "sess-win-1",
			stepIndex:  5,
			context:    2,
			wantErr:    true,
			wantSubstr: "context canceled",
		},
		{
			name:        "nil context succeeds with background",
			nilCtx:      true,
			sessionID:   "sess-win-1",
			stepIndex:   5,
			context:     2,
			wantCount:   5,
			wantContext: 2,
			wantCenter:  5,
		},
		{
			name:        "normal center step window",
			sessionID:   "sess-win-1",
			stepIndex:   5,
			context:     2,
			wantCount:   5,
			wantContext: 2,
			wantCenter:  5,
		},
		{
			name:        "start clamped to zero at low step",
			sessionID:   "sess-win-1",
			stepIndex:   1,
			context:     3,
			wantCount:   5, // steps 0, 1, 2, 3, 4
			wantContext: 3,
			wantCenter:  1,
		},
		{
			name:        "negative context defaults to 2",
			sessionID:   "sess-win-1",
			stepIndex:   5,
			context:     -1,
			wantCount:   5,
			wantContext: 2,
			wantCenter:  5,
		},
		{
			name:        "excessive context clamped to 20",
			sessionID:   "sess-win-1",
			stepIndex:   5,
			context:     50,
			wantCount:   10, // all 10 steps 0..9
			wantContext: 20,
			wantCenter:  5,
		},
		{
			name:        "empty session returns zero steps",
			sessionID:   "",
			stepIndex:   5,
			context:     2,
			wantCount:   0,
			wantContext: 2,
			wantCenter:  5,
		},
		{
			name:        "unknown session returns zero steps",
			sessionID:   "sess-nonexistent",
			stepIndex:   5,
			context:     2,
			wantCount:   0,
			wantContext: 2,
			wantCenter:  5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var store db.TranscriptStore
			if !tc.nilStore {
				fake := dbtest.NewFakeStore()
				ctx := context.Background()
				_ = fake.UpsertSessionSummary(ctx, db.SessionSummary{SessionID: "sess-win-1"})
				steps := make([]db.TranscriptStep, 10)
				for i := 0; i < 10; i++ {
					steps[i] = db.TranscriptStep{
						SessionID: "sess-win-1",
						StepIndex: i,
						StepType:  "tool_call",
						ToolName:  "run_command",
						Content:   fmt.Sprintf("echo step %d", i),
						CreatedAt: time.Now().UTC(),
					}
				}
				_ = fake.BatchInsertTranscriptSteps(ctx, steps)
				if tc.closedStore {
					_ = fake.Close()
				}
				store = fake
			}

			var ctx context.Context
			if tc.nilCtx {
				ctx = nil
			} else {
				ctx = context.Background()
				if tc.cancelCtx {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
			}

			res, err := GetTranscriptStepWindow(ctx, store, tc.sessionID, tc.stepIndex, tc.context)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(res.Steps) != tc.wantCount {
				t.Errorf("got %d steps, want %d", len(res.Steps), tc.wantCount)
			}
			if res.Context != tc.wantContext {
				t.Errorf("got context=%d, want %d", res.Context, tc.wantContext)
			}
			if res.CenterStep != tc.wantCenter {
				t.Errorf("got center step=%d, want %d", res.CenterStep, tc.wantCenter)
			}
			if res.Steps == nil {
				t.Errorf("res.Steps should not be nil")
			}
		})
	}
}

func TestExecuteTranscriptSearch_Options(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		opts         SearchOptions
		nilStore     bool
		wantSessions int
		wantSteps    int
		wantTotal    int
		wantErr      bool
	}{
		{
			name:     "nil store returns error",
			nilStore: true,
			opts:     SearchOptions{Mode: "auto", Query: "test"},
			wantErr:  true,
		},
		{
			name: "empty query with mode steps and session id",
			opts: SearchOptions{
				Mode:      "steps",
				SessionID: "sess-2",
			},
			wantSteps: 1,
			wantTotal: 1,
		},
		{
			name: "empty query with mode steps and session filter",
			opts: SearchOptions{
				Mode:          "steps",
				SessionFilter: "sess-2",
			},
			wantSteps: 1,
			wantTotal: 1,
		},
		{
			name: "empty query with mode auto returns zero results",
			opts: SearchOptions{
				Mode: "auto",
			},
			wantTotal: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var store db.TranscriptStore
			if !tc.nilStore {
				store = newSearchTestStore(t, 2)
			}

			res, err := ExecuteTranscriptSearch(context.Background(), store, nil, tc.opts)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(res.Sessions) != tc.wantSessions {
				t.Errorf("got %d sessions, want %d", len(res.Sessions), tc.wantSessions)
			}
			if len(res.Steps) != tc.wantSteps {
				t.Errorf("got %d steps, want %d", len(res.Steps), tc.wantSteps)
			}
			if res.Total != tc.wantTotal {
				t.Errorf("got total %d, want %d", res.Total, tc.wantTotal)
			}
		})
	}
}
