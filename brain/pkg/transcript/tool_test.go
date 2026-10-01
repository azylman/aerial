package transcript

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ClipContent(tc.content, tc.headLimit, tc.tailLimit)
			if tc.want != "" && got != tc.want {
				t.Fatalf("ClipContent() = %q, want %q", got, tc.want)
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

func TestSearchTranscripts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("nil store returns error", func(t *testing.T) {
		t.Parallel()
		_, err := SearchTranscripts(ctx, nil, nil, "query", "auto", "", "", 10)
		if err == nil || !strings.Contains(err.Error(), "transcript store cannot be nil") {
			t.Fatalf("expected nil store error, got %v", err)
		}
	})

	t.Run("context cancelled returns error", func(t *testing.T) {
		t.Parallel()
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		store := db.NewFakeStore()
		_, err := SearchTranscripts(cancelledCtx, store, nil, "query", "auto", "", "", 10)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("mode sessions searches only session summaries", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		err := store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-1",
			Summary:   "Fixed Docker compose networking issue in staging environment",
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("upsert summary: %v", err)
		}
		err = store.BatchInsertTranscriptSteps(ctx, []db.TranscriptStep{
			{
				SessionID: "sess-1",
				StepIndex: 1,
				StepType:  "TOOL",
				ToolName:  "run_command",
				Content:   "docker compose up -d",
			},
		})
		if err != nil {
			t.Fatalf("batch insert: %v", err)
		}

		res, err := SearchTranscripts(ctx, store, nil, "Docker compose networking", "sessions", "", "", 5)
		if err != nil {
			t.Fatalf("SearchTranscripts failed: %v", err)
		}
		if len(res.Sessions) != 1 {
			t.Fatalf("expected 1 session hit, got %d", len(res.Sessions))
		}
		if len(res.Steps) != 0 {
			t.Fatalf("expected 0 step hits in sessions mode, got %d", len(res.Steps))
		}
		if res.Total != 1 {
			t.Fatalf("expected Total = 1, got %d", res.Total)
		}
	})

	t.Run("mode steps searches only transcript steps", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		err := store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-2",
			Summary:   "Database migration runbook",
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("upsert summary: %v", err)
		}
		err = store.BatchInsertTranscriptSteps(ctx, []db.TranscriptStep{
			{
				SessionID: "sess-2",
				StepIndex: 1,
				StepType:  "TOOL",
				ToolName:  "run_command",
				Content:   "go run ./scripts/migrate.go --up",
			},
		})
		if err != nil {
			t.Fatalf("batch insert: %v", err)
		}

		res, err := SearchTranscripts(ctx, store, nil, "migrate.go", "steps", "sess-2", "run_command", 5)
		if err != nil {
			t.Fatalf("SearchTranscripts failed: %v", err)
		}
		if len(res.Sessions) != 0 {
			t.Fatalf("expected 0 session hits in steps mode, got %d", len(res.Sessions))
		}
		if len(res.Steps) != 1 {
			t.Fatalf("expected 1 step hit, got %d", len(res.Steps))
		}
		if res.Steps[0].SessionID != "sess-2" {
			t.Fatalf("unexpected session ID in step: %s", res.Steps[0].SessionID)
		}
	})

	t.Run("mode auto searches both sessions and steps with embedder", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		dummyEmb := make([]float32, db.ExpectedEmbeddingDim)
		dummyEmb[0] = 1.0

		err := store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-3",
			Summary:   "Deploying Postgres pgvector instance",
			Embedding: dummyEmb,
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("upsert summary: %v", err)
		}
		err = store.BatchInsertTranscriptSteps(ctx, []db.TranscriptStep{
			{
				SessionID: "sess-3",
				StepIndex: 1,
				StepType:  "TOOL",
				ToolName:  "docker",
				Content:   "docker run --name pgvector -d pgvector/pgvector:pg16",
			},
		})
		if err != nil {
			t.Fatalf("batch insert: %v", err)
		}

		mockEmbedder := func(ctx context.Context, text string) ([]float32, error) {
			v := make([]float32, db.ExpectedEmbeddingDim)
			v[0] = 1.0
			return v, nil
		}

		res, err := SearchTranscripts(ctx, store, mockEmbedder, "pgvector", "auto", "", "", 10)
		if err != nil {
			t.Fatalf("SearchTranscripts auto failed: %v", err)
		}
		if len(res.Sessions) != 1 {
			t.Fatalf("expected 1 session hit, got %d", len(res.Sessions))
		}
		if len(res.Steps) != 1 {
			t.Fatalf("expected 1 step hit, got %d", len(res.Steps))
		}
		if res.Total != 2 {
			t.Fatalf("expected Total = 2, got %d", res.Total)
		}
	})

	t.Run("embedder error falls back gracefully to text search", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		err := store.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID: "sess-4",
			Summary:   "Configured Grafana monitoring dashboards",
			IsSettled: true,
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("upsert summary: %v", err)
		}

		errEmbedder := func(ctx context.Context, text string) ([]float32, error) {
			return nil, errors.New("ollama service unavailable")
		}

		res, err := SearchTranscripts(ctx, store, errEmbedder, "Grafana", "sessions", "", "", 10)
		if err != nil {
			t.Fatalf("expected graceful fallback on embedder error, got %v", err)
		}
		if len(res.Sessions) != 1 {
			t.Fatalf("expected text search match despite embedder error, got %d hits", len(res.Sessions))
		}
	})

	t.Run("nil ctx and default limit", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		res, err := SearchTranscripts(nil, store, nil, "any", "unknown_mode", "", "", 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "auto" {
			t.Errorf("expected mode normalized to auto, got %s", res.Mode)
		}
	})

	t.Run("store search error propagation", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		_ = store.Close()
		_, err := SearchTranscripts(ctx, store, nil, "query", "sessions", "", "", 5)
		if err == nil {
			t.Fatalf("expected error from closed store on sessions search")
		}
		_, err = SearchTranscripts(ctx, store, nil, "query", "steps", "", "", 5)
		if err == nil {
			t.Fatalf("expected error from closed store on steps search")
		}
	})
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
