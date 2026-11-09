package transcript

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

func writeTranscriptFile(t *testing.T, baseDir, sessionID, content string, mtime time.Time) string {
	t.Helper()
	logDir := filepath.Join(baseDir, sessionID, ".system_generated", "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	tPath := filepath.Join(logDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}
	if !mtime.IsZero() {
		mtime = mtime.Truncate(time.Second)
		if err := os.Chtimes(tPath, mtime, mtime); err != nil {
			t.Fatalf("chtimes failed: %v", err)
		}
	}
	return tPath
}

type sessionFileFixture struct {
	sessionID string
	content   string
	age       time.Duration
}

func TestSyncTranscripts_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		files                 []sessionFileFixture
		priorSummary          *db.SessionSummary
		llmMode               string
		withEmbedder          bool
		idleThreshold         time.Duration
		batchLimit            int
		secondPassFiles       []sessionFileFixture
		secondPassAge         time.Duration
		wantScanned           int
		wantSynced            int
		wantStepsInserted     int
		wantSummaries         int
		wantSkippedUnchanged  int
		wantErrors            int
		wantSettled           *bool
		wantLastIndexed       int
		wantSummaryText       string
		secondPassSteps       int
		secondPassLastIndexed int
	}{
		{
			name: "basic sweep and settled with embedder and llm",
			files: []sessionFileFixture{
				{
					sessionID: "sess-alpha-001",
					content: strings.Join([]string{
						`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Please check system health and memory usage"}`,
						`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:02Z","tool_calls":[{"name":"run_command","args":{"CommandLine":"free -m"}},{"name":"manage_task","args":{"Action":"status"}}]}`,
						`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","created_at":"2026-10-01T10:00:05Z","content":"total: 32000, used: 8000, free: 24000"}`,
						`{"step_index":3,"source":"MODEL","type":"GENERIC","status":"DONE","created_at":"2026-10-01T10:00:06Z","content":"{\"status\":\"running\"}"}`,
					}, "\n") + "\n",
					age: 1 * time.Hour,
				},
			},
			llmMode:           "success",
			withEmbedder:      true,
			wantScanned:       1,
			wantSynced:        1,
			wantStepsInserted: 3,
			wantSummaries:     1,
			wantSettled:       new(true),
			wantLastIndexed:   3,
			wantSummaryText:   "Checked system health and free memory: 24GB available.",
		},
		{
			name: "unchanged mtime skipped on second sweep",
			files: []sessionFileFixture{
				{
					sessionID: "sess-skip-002",
					content:   `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Hello world"}` + "\n",
					age:       2 * time.Hour,
				},
			},
			wantScanned:          1,
			wantSynced:           1,
			wantSkippedUnchanged: 1,
		},
		{
			name: "partial line at EOF tolerated without error",
			files: []sessionFileFixture{
				{
					sessionID: "sess-partial-003",
					content: strings.Join([]string{
						`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Run long command"}`,
						`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:02Z","tool_calls":[{"name":"run_command","args":{"CommandLine":"sleep 5"}}]}`,
						`{"step_index":2,"source":"MODEL","type":"TOOL","stat`,
					}, "\n"),
					age: 1 * time.Hour,
				},
			},
			wantStepsInserted: 2,
			wantErrors:        0,
		},
		{
			name: "corrupted intermediate line recorded as error and continues",
			files: []sessionFileFixture{
				{
					sessionID: "sess-corrupt-004",
					content: strings.Join([]string{
						`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Valid start"}`,
						`{corrupt_intermediate_json_line_here`,
						`{"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:04Z","content":"Recovered valid line"}`,
					}, "\n") + "\n",
					age: 1 * time.Hour,
				},
			},
			wantStepsInserted: 2,
			wantErrors:        1,
		},
		{
			name: "truncation detected resets watermark and reindexes steps",
			priorSummary: &db.SessionSummary{
				SessionID:       "sess-trunc-005",
				Summary:         "Old prior session summary",
				LastIndexedStep: 10,
				LastMtime:       time.Now().Add(-2 * time.Hour),
				IsSettled:       true,
				CreatedAt:       time.Now().Add(-3 * time.Hour),
			},
			files: []sessionFileFixture{
				{
					sessionID: "sess-trunc-005",
					content: strings.Join([]string{
						`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T11:00:00Z","content":"Rotated session restart"}`,
						`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T11:00:02Z","content":"Fresh start acknowledged"}`,
					}, "\n") + "\n",
					age: 10 * time.Minute,
				},
			},
			idleThreshold:     5 * time.Minute,
			wantStepsInserted: 2,
			wantLastIndexed:   1,
		},
		{
			name: "historical session without LLM falls back to step 0 prompt",
			files: []sessionFileFixture{
				{
					sessionID: "sess-hist-006",
					content:   `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-01T10:00:00Z","content":"Deploy mirrormere dashboard v2"}` + "\n",
					age:       48 * time.Hour,
				},
			},
			wantSummaries:   1,
			wantSummaryText: "Deploy mirrormere dashboard v2",
		},
		{
			name: "LLM error falls back to step 0 prompt",
			files: []sessionFileFixture{
				{
					sessionID: "sess-fallback-007",
					content: strings.Join([]string{
						`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Initial prompt before LLM error"}`,
						`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:02Z","content":"Some model reply"}`,
					}, "\n") + "\n",
					age: 1 * time.Hour,
				},
			},
			llmMode:         "error",
			idleThreshold:   5 * time.Minute,
			wantSummaries:   1,
			wantSummaryText: "Initial prompt before LLM error",
		},
		{
			name: "active session not settled",
			files: []sessionFileFixture{
				{
					sessionID: "sess-active-008",
					content:   `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Active work in progress"}` + "\n",
					age:       10 * time.Second,
				},
			},
			idleThreshold:     10 * time.Minute,
			wantStepsInserted: 1,
			wantSettled:       new(false),
		},
		{
			name: "batch limit caps synced sessions",
			files: []sessionFileFixture{
				{sessionID: "sess-batch-001", content: `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"Batch task 1"}` + "\n", age: 1 * time.Hour},
				{sessionID: "sess-batch-002", content: `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"Batch task 2"}` + "\n", age: 1 * time.Hour},
				{sessionID: "sess-batch-003", content: `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"Batch task 3"}` + "\n", age: 1 * time.Hour},
			},
			batchLimit: 1,
			wantSynced: 1,
		},
		{
			name: "timestamps filtering and incremental watermark skipping",
			files: []sessionFileFixture{
				{
					sessionID: "sess-timestamps-001",
					content: strings.Join([]string{
						"",
						`{"step_index":0,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Initial thinking before user"}`,
						`{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:01.123456789Z","content":"User follow-up prompt"}`,
						`{"step_index":2,"source":"MODEL","type":"HEARTBEAT","status":"DONE","created_at":"invalid-timestamp","content":"heartbeat ping"}`,
						"",
					}, "\n") + "\n",
					age: 1 * time.Hour,
				},
			},
			secondPassFiles: []sessionFileFixture{
				{
					sessionID: "sess-timestamps-001",
					content: strings.Join([]string{
						`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","created_at":"2026-10-01T12:00:02Z","content":"Old step 2"}`,
						`{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T12:00:03Z","content":"New step 3"}`,
					}, "\n") + "\n",
					age: 30 * time.Minute,
				},
			},
			wantStepsInserted:     2,
			secondPassSteps:       1,
			secondPassLastIndexed: 3,
		},
		{
			name: "settled session re-summarization when delta > 10",
			priorSummary: &db.SessionSummary{
				SessionID:            "sess-resummarize",
				Summary:              "Initial settled summary",
				LastIndexedStep:      2,
				SummaryStepWatermark: 2,
				LastMtime:            time.Now().Add(-2 * time.Hour),
				IsSettled:            true,
			},
			files: []sessionFileFixture{
				{
					sessionID: "sess-resummarize",
					content: func() string {
						stepLines := make([]string, 0, 16)
						for i := range 16 {
							stepLines = append(stepLines, fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","content":"Step %d"}`, i, i))
						}
						return strings.Join(stepLines, "\n") + "\n"
					}(),
					age: 1 * time.Hour,
				},
			},
			idleThreshold: 10 * time.Minute,
			wantSummaries: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			tempDir := t.TempDir()
			store := dbtest.NewFakeStore()

			if tc.priorSummary != nil {
				if err := store.UpsertSessionSummary(ctx, *tc.priorSummary); err != nil {
					t.Fatalf("upsert prior summary: %v", err)
				}
			}

			for _, f := range tc.files {
				writeTranscriptFile(t, tempDir, f.sessionID, f.content, time.Now().Add(-f.age))
			}

			var llm func(ctx context.Context, prompt string) (string, error)
			switch tc.llmMode {
			case "success":
				llm = func(ctx context.Context, prompt string) (string, error) {
					return "Checked system health and free memory: 24GB available.", nil
				}
			case "error":
				llm = func(ctx context.Context, prompt string) (string, error) {
					return "", errors.New("rate limit exceeded")
				}
			}

			var embedder func(ctx context.Context, text string) ([]float32, error)
			if tc.withEmbedder {
				embedder = func(ctx context.Context, text string) ([]float32, error) {
					vec := make([]float32, db.ExpectedEmbeddingDim)
					vec[0] = 0.88
					return vec, nil
				}
			}

			batchLimit := tc.batchLimit
			if batchLimit == 0 {
				batchLimit = 10
			}
			idle := tc.idleThreshold
			if idle == 0 {
				idle = 10 * time.Minute
			}

			opts := SyncOptions{
				BatchLimit:    batchLimit,
				IdleThreshold: idle,
				BrainDir:      tempDir,
				HeadLimit:     1000,
				TailLimit:     1000,
			}

			stats, err := SyncTranscripts(ctx, store, tempDir, embedder, llm, opts)
			if err != nil {
				t.Fatalf("SyncTranscripts failed: %v", err)
			}

			if tc.wantScanned != 0 && stats.Scanned != tc.wantScanned {
				t.Errorf("Scanned = %d, want %d", stats.Scanned, tc.wantScanned)
			}
			if tc.wantSynced != 0 && stats.Synced != tc.wantSynced {
				t.Errorf("Synced = %d, want %d", stats.Synced, tc.wantSynced)
			}
			if tc.wantStepsInserted != 0 && stats.StepsInserted != tc.wantStepsInserted {
				t.Errorf("StepsInserted = %d, want %d", stats.StepsInserted, tc.wantStepsInserted)
			}
			if tc.wantSummaries != 0 && stats.SummariesGenerated != tc.wantSummaries {
				t.Errorf("SummariesGenerated = %d, want %d", stats.SummariesGenerated, tc.wantSummaries)
			}
			if stats.Errors != tc.wantErrors {
				t.Errorf("Errors = %d, want %d", stats.Errors, tc.wantErrors)
			}

			if len(tc.secondPassFiles) > 0 || tc.wantSkippedUnchanged > 0 {
				for _, f := range tc.secondPassFiles {
					age := f.age
					if tc.secondPassAge != 0 {
						age = tc.secondPassAge
					}
					writeTranscriptFile(t, tempDir, f.sessionID, f.content, time.Now().Add(-age))
				}
				stats2, err := SyncTranscripts(ctx, store, tempDir, embedder, llm, opts)
				if err != nil {
					t.Fatalf("SyncTranscripts second pass failed: %v", err)
				}
				if tc.wantSkippedUnchanged != 0 && stats2.SkippedUnchanged != tc.wantSkippedUnchanged {
					t.Errorf("second pass SkippedUnchanged = %d, want %d", stats2.SkippedUnchanged, tc.wantSkippedUnchanged)
				}
				if tc.secondPassSteps != 0 && stats2.StepsInserted != tc.secondPassSteps {
					t.Errorf("second pass StepsInserted = %d, want %d", stats2.StepsInserted, tc.secondPassSteps)
				}
			}

			if len(tc.files) > 0 {
				primarySession := tc.files[0].sessionID
				states, err := store.GetSessionSyncStates(ctx)
				if err != nil {
					t.Fatalf("GetSessionSyncStates: %v", err)
				}
				expectedLastIndexed := tc.wantLastIndexed
				if tc.secondPassLastIndexed != 0 {
					expectedLastIndexed = tc.secondPassLastIndexed
				}
				if tc.wantSettled != nil || expectedLastIndexed != 0 {
					st, ok := states[primarySession]
					if !ok {
						t.Fatalf("session %q missing from sync states", primarySession)
					}
					if tc.wantSettled != nil && st.IsSettled != *tc.wantSettled {
						t.Errorf("IsSettled = %v, want %v", st.IsSettled, *tc.wantSettled)
					}
					if expectedLastIndexed != 0 && st.LastIndexedStep != expectedLastIndexed {
						t.Errorf("LastIndexedStep = %d, want %d", st.LastIndexedStep, expectedLastIndexed)
					}
				}
				if tc.wantSummaryText != "" {
					res, err := SearchTranscripts(ctx, store, embedder, tc.wantSummaryText, "sessions", "", "", 10)
					if err != nil || len(res.Sessions) == 0 {
						t.Fatalf("SearchTranscripts(%q) err=%v, sessions=%d", tc.wantSummaryText, err, len(res.Sessions))
					}
					if res.Sessions[0].Summary != tc.wantSummaryText {
						t.Errorf("summary = %q, want %q", res.Sessions[0].Summary, tc.wantSummaryText)
					}
				}
			}
		})
	}
}

type mockTranscriptStore struct {
	db.TranscriptStore

	failGetSyncStates bool
	failBatchInsert   bool
	failUpsertSummary bool
}

func (m *mockTranscriptStore) GetSessionSyncStates(ctx context.Context) (map[string]db.SessionSyncState, error) {
	if m.failGetSyncStates {
		return nil, errors.New("simulated sync states query failure")
	}
	return m.TranscriptStore.GetSessionSyncStates(ctx)
}

func (m *mockTranscriptStore) UpsertSessionSummary(ctx context.Context, summary db.SessionSummary) error {
	if m.failUpsertSummary {
		return errors.New("simulated upsert summary error")
	}
	return m.TranscriptStore.UpsertSessionSummary(ctx, summary)
}

func (m *mockTranscriptStore) BatchInsertTranscriptSteps(ctx context.Context, steps []db.TranscriptStep) error {
	if m.failBatchInsert {
		return errors.New("simulated batch insert error")
	}
	return m.TranscriptStore.BatchInsertTranscriptSteps(ctx, steps)
}

func newMockTranscriptStore(base db.TranscriptStore, failGetSyncStates, failBatchInsert, failUpsertSummary bool) *mockTranscriptStore {
	return &mockTranscriptStore{
		TranscriptStore:   base,
		failGetSyncStates: failGetSyncStates,
		failBatchInsert:   failBatchInsert,
		failUpsertSummary: failUpsertSummary,
	}
}

func TestSyncTranscripts_ErrorsAndEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		nilStore            bool
		emptyBrainDir       bool
		nonExistentBrainDir bool
		cancelCtx           bool
		nilCtx              bool
		hasSession          bool
		socketFile          bool
		emptyFile           bool
		prePopActive        bool
		activeFile          bool
		failGetSyncStates   bool
		failBatchInsert     bool
		failUpsertSummary   bool
		opts                SyncOptions
		zeroOpts            bool
		wantErr             bool
		isContextCanceled   bool
		wantSubstr          string
		wantSweepErrors     int
		wantSynced          int
		wantSummaries       int
		wantStepsInserted   int
		wantLastIndexed     int
	}{
		{
			name:       "nil store returns error",
			nilStore:   true,
			wantErr:    true,
			wantSubstr: "transcript store cannot be nil",
		},
		{
			name:          "empty brain dir returns error",
			emptyBrainDir: true,
			wantErr:       true,
			wantSubstr:    "brain directory cannot be empty",
		},
		{
			name:                "non-existent brain dir returns error",
			nonExistentBrainDir: true,
			wantErr:             true,
		},
		{
			name:              "context cancelled returns context canceled error",
			hasSession:        true,
			cancelCtx:         true,
			zeroOpts:          true,
			wantErr:           true,
			isContextCanceled: true,
		},
		{
			name:       "nil context falls back to background without error",
			hasSession: true,
			nilCtx:     true,
			wantSynced: 1,
		},
		{
			name:              "error loading session sync states propagated",
			hasSession:        true,
			failGetSyncStates: true,
			wantErr:           true,
			wantSubstr:        "load session sync states",
		},
		{
			name:            "unopenable socket file recorded as error in sweep",
			socketFile:      true,
			wantSweepErrors: 1,
		},
		{
			name:              "batch insert error increments errors and inserts 0 steps",
			hasSession:        true,
			failBatchInsert:   true,
			wantSweepErrors:   1,
			wantStepsInserted: 0,
		},
		{
			name:              "upsert settled session error increments errors and generates 0 summaries",
			hasSession:        true,
			failUpsertSummary: true,
			wantSweepErrors:   1,
			wantSummaries:     0,
		},
		{
			name:            "empty transcript file generates summary with watermark -2",
			emptyFile:       true,
			wantSummaries:   1,
			wantLastIndexed: -2,
		},
		{
			name:              "empty transcript file upsert error increments errors",
			emptyFile:         true,
			failUpsertSummary: true,
			wantSweepErrors:   1,
		},
		{
			name:              "upsert active session progress error increments errors",
			prePopActive:      true,
			activeFile:        true,
			failUpsertSummary: true,
			wantSweepErrors:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tempDir := t.TempDir()

			var store db.TranscriptStore
			if !tc.nilStore {
				fakeStore := dbtest.NewFakeStore()
				if tc.prePopActive {
					_ = fakeStore.UpsertSessionSummary(context.Background(), db.SessionSummary{
						SessionID:       "sess-active-err",
						Summary:         "Initial active session",
						LastIndexedStep: 0,
						LastMtime:       time.Now().Add(-5 * time.Minute),
						IsSettled:       false,
					})
				}
				if tc.failGetSyncStates || tc.failBatchInsert || tc.failUpsertSummary {
					store = newMockTranscriptStore(fakeStore, tc.failGetSyncStates, tc.failBatchInsert, tc.failUpsertSummary)
				} else {
					store = fakeStore
				}
			}

			brainDir := tempDir
			if tc.emptyBrainDir {
				brainDir = ""
			} else if tc.nonExistentBrainDir {
				brainDir = "/non/existent/path/for/transcripts/test"
			}

			switch {
			case tc.hasSession:
				writeTranscriptFile(t, tempDir, "sess-err-01", `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Err test"}`+"\n", time.Now().Add(-1*time.Hour))
			case tc.socketFile:
				sockPath := filepath.Join(os.TempDir(), fmt.Sprintf("ae_t_%d.sock", time.Now().UnixNano()))
				l, err := net.Listen("unix", sockPath)
				if err != nil {
					t.Fatalf("net.Listen failed: %v", err)
				}
				defer l.Close()
				defer os.Remove(sockPath)

				sockDir := filepath.Join(tempDir, "sess-sock-err", ".system_generated", "logs")
				if err := os.MkdirAll(sockDir, 0o755); err != nil {
					t.Fatalf("mkdir failed: %v", err)
				}
				if err := os.Symlink(sockPath, filepath.Join(sockDir, "transcript.jsonl")); err != nil {
					t.Fatalf("symlink failed: %v", err)
				}
			case tc.emptyFile:
				writeTranscriptFile(t, tempDir, "sess-empty-file", "", time.Now().Add(-1*time.Hour))
			case tc.activeFile:
				content := strings.Join([]string{
					`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Step 0"}`,
					`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T12:00:05Z","content":"Step 1"}`,
				}, "\n") + "\n"
				writeTranscriptFile(t, tempDir, "sess-active-err", content, time.Now().Add(-5*time.Second))
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

			opts := tc.opts
			if opts.BrainDir == "" {
				opts.BrainDir = brainDir
			}
			if !tc.zeroOpts {
				if opts.BatchLimit == 0 {
					opts.BatchLimit = 10
				}
				if opts.IdleThreshold == 0 {
					opts.IdleThreshold = 10 * time.Minute
				}
			}

			stats, err := SyncTranscripts(ctx, store, brainDir, nil, nil, opts)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.isContextCanceled && !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context.Canceled, got %v", err)
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if stats.Errors != tc.wantSweepErrors {
				t.Errorf("Errors = %d, want %d", stats.Errors, tc.wantSweepErrors)
			}
			if tc.wantSynced != 0 && stats.Synced != tc.wantSynced {
				t.Errorf("Synced = %d, want %d", stats.Synced, tc.wantSynced)
			}
			if (tc.wantSummaries != 0 || tc.failUpsertSummary) && stats.SummariesGenerated != tc.wantSummaries {
				t.Errorf("SummariesGenerated = %d, want %d", stats.SummariesGenerated, tc.wantSummaries)
			}
			if (tc.wantStepsInserted != 0 || tc.failBatchInsert) && stats.StepsInserted != tc.wantStepsInserted {
				t.Errorf("StepsInserted = %d, want %d", stats.StepsInserted, tc.wantStepsInserted)
			}

			if tc.wantLastIndexed != 0 && store != nil {
				states, err := store.GetSessionSyncStates(context.Background())
				if err != nil {
					t.Fatalf("GetSessionSyncStates: %v", err)
				}
				if tc.emptyFile {
					if states["sess-empty-file"].LastIndexedStep != tc.wantLastIndexed {
						t.Errorf("LastIndexedStep = %d, want %d", states["sess-empty-file"].LastIndexedStep, tc.wantLastIndexed)
					}
				}
			}
		})
	}
}

func TestFindTranscriptFile_AlternativeLocations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := dbtest.NewFakeStore()

	// 1. system_generated/logs/transcript.jsonl (without leading dot)
	sess1 := "sess-nodot"
	dir1 := filepath.Join(tempDir, sess1, "system_generated", "logs")
	_ = os.MkdirAll(dir1, 0o755)
	_ = os.WriteFile(filepath.Join(dir1, "transcript.jsonl"), []byte(`{"step_index":0,"content":"no dot"}`+"\n"), 0o644)

	// 2. transcript.jsonl directly under session dir
	sess2 := "sess-root"
	dir2 := filepath.Join(tempDir, sess2)
	_ = os.MkdirAll(dir2, 0o755)
	_ = os.WriteFile(filepath.Join(dir2, "transcript.jsonl"), []byte(`{"step_index":0,"content":"root"}`+"\n"), 0o644)

	// 3. Hidden directory and regular file (should be ignored)
	_ = os.MkdirAll(filepath.Join(tempDir, ".hidden-dir"), 0o755)
	_ = os.WriteFile(filepath.Join(tempDir, "regular-file.txt"), []byte("not a dir"), 0o644)

	// 4. Session directory without any transcript file (should be ignored)
	_ = os.MkdirAll(filepath.Join(tempDir, "sess-empty-dir"), 0o755)

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, DefaultSyncOptions())
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.Synced != 2 {
		t.Errorf("expected 2 synced from alternative locations, got %d", stats.Synced)
	}
}

func TestParseTranscriptTimestamp(t *testing.T) {
	fallback := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		rawTime  string
		wantTime time.Time
	}{
		{name: "empty timestamp", rawTime: "", wantTime: fallback},
		{name: "invalid timestamp", rawTime: "invalid-date", wantTime: fallback},
		{name: "rfc3339nano", rawTime: "2026-10-01T10:00:00.123456789Z", wantTime: time.Date(2026, 10, 1, 10, 0, 0, 123456789, time.UTC)},
		{name: "rfc3339 standard", rawTime: "2026-10-01T10:00:00Z", wantTime: time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTranscriptTimestamp(tc.rawTime, fallback)
			if !got.Equal(tc.wantTime) {
				t.Fatalf("parseTranscriptTimestamp(%q) = %v, want %v", tc.rawTime, got, tc.wantTime)
			}
		})
	}
}

func TestExtractClaudeBlockText(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantStr string
	}{
		{name: "empty raw message", raw: "", wantStr: ""},
		{name: "quoted string", raw: `"simple text output"`, wantStr: "simple text output"},
		{name: "text blocks array", raw: `[{"type":"text","text":"first line"},{"type":"text","text":"second line"}]`, wantStr: "first line\nsecond line"},
		{name: "fallback raw json", raw: `{"key":"value"}`, wantStr: `{"key":"value"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractClaudeBlockText(json.RawMessage(tc.raw))
			if got != tc.wantStr {
				t.Fatalf("extractClaudeBlockText(%q) = %q, want %q", tc.raw, got, tc.wantStr)
			}
		})
	}
}

func TestFindTranscriptFile_PathResolution(t *testing.T) {
	tests := []struct {
		name      string
		relPath   string
		sessionID string
		wantFound bool
	}{
		{name: "not found", sessionID: "s-missing", wantFound: false},
		{name: "dot system generated logs", relPath: "s-dot/.system_generated/logs/transcript.jsonl", sessionID: "s-dot", wantFound: true},
		{name: "no dot system generated logs", relPath: "s-nodot/system_generated/logs/transcript.jsonl", sessionID: "s-nodot", wantFound: true},
		{name: "direct under session dir", relPath: "s-direct/transcript.jsonl", sessionID: "s-direct", wantFound: true},
		{name: "session id jsonl in brain", relPath: "s-brain.jsonl", sessionID: "s-brain", wantFound: true},
		{name: "claude relative 1", relPath: "../claude/projects/p1/s-claude1.jsonl", sessionID: "s-claude1", wantFound: true},
		{name: "claude relative 2", relPath: "../../../claude/projects/p2/s-claude2.jsonl", sessionID: "s-claude2", wantFound: true},
		{name: "claude relative dot claude", relPath: "../.claude/projects/p3/s-claude3.jsonl", sessionID: "s-claude3", wantFound: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			brainDir := filepath.Join(tmp, "sub1", "sub2", "brain")
			_ = os.MkdirAll(brainDir, 0o755)

			if tc.relPath != "" {
				fullPath := filepath.Join(brainDir, tc.relPath)
				_ = os.MkdirAll(filepath.Dir(fullPath), 0o755)
				_ = os.WriteFile(fullPath, []byte("content"), 0o644)
			}

			got := findTranscriptFile(brainDir, tc.sessionID)
			if tc.wantFound && got == "" {
				t.Fatalf("expected to find transcript file for %q", tc.sessionID)
			} else if !tc.wantFound && got != "" {
				t.Fatalf("expected not to find transcript file, got %q", got)
			}
		})
	}
}

func TestSyncTranscripts_ClaudeScenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		sessionID            string
		content              string
		customClaudeDir      bool
		withLLM              bool
		withEmbedder         bool
		secondPassContent    string
		wantSteps            int
		wantSummaries        int
		wantSettled          bool
		wantSkippedUnchanged int
		secondPassSteps      int
		wantStoredTools      []string // persisted tool_name per stored step, in step order
		wantToolResults      []string // parsed TOOL steps as "tool_call_id|tool_name|status|content"
	}{
		{
			name:      "claude transcript with prompt thinking tool use tool result and reply",
			sessionID: "sess-claude-full",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":"Inspect disk usage"}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:02Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Checking storage"},{"type":"tool_use","name":"run_command","input":{"CommandLine":"df -h"}}]}}`,
				`{"type":"user","timestamp":"2026-10-01T10:00:05Z","message":{"role":"user","content":[{"type":"tool_result","is_error":false,"content":"/dev/sda1 50G 20G 30G"}]}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:08Z","message":{"role":"assistant","content":[{"type":"text","text":"Disk usage is at 40%."}]}}`,
			}, "\n") + "\n",
			withLLM:              true,
			withEmbedder:         true,
			wantSteps:            4,
			wantSummaries:        1,
			wantSettled:          true,
			secondPassContent:    "",
			wantSkippedUnchanged: 1,
		},
		{
			name:      "claude transcript with tool error and raw assistant string and custom claude dir",
			sessionID: "sess-claude-custom-dir",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":"Test failing command"}}`,
				`{"type":"user","timestamp":"2026-10-01T10:00:02Z","message":{"role":"user","content":[{"type":"tool_result","is_error":true,"content":[{"type":"text","text":"command failed with code 1"}]}]}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:04Z","message":{"role":"assistant","content":"Recovering from error"}}`,
			}, "\n") + "\n",
			customClaudeDir: true,
			wantSteps:       3,
			wantSummaries:   1,
			wantSettled:     true,
			secondPassContent: strings.Join([]string{
				`{"type":"user","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":"Test failing command"}}`,
				`{"type":"user","timestamp":"2026-10-01T10:00:02Z","message":{"role":"user","content":[{"type":"tool_result","is_error":true,"content":[{"type":"text","text":"command failed with code 1"}]}]}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:04Z","message":{"role":"assistant","content":"Recovering from error"}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:06Z","message":{"role":"assistant","content":[{"type":"text","text":"Extra resolution step"}]}}`,
			}, "\n") + "\n",
			secondPassSteps: 4,
		},
		{
			name:      "claude parallel tool calls paired to shuffled tool results by id",
			sessionID: "sess-claude-parallel",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":"Survey the repo"}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:01Z","message":{"id":"msg_1","role":"assistant","content":[{"type":"tool_use","id":"toolu_a","name":"Bash","input":{"command":"ls"}}]}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:01Z","message":{"id":"msg_1","role":"assistant","content":[{"type":"tool_use","id":"toolu_b","name":"Read","input":{"file_path":"go.mod"}}]}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:01Z","message":{"id":"msg_1","role":"assistant","content":[{"type":"tool_use","id":"toolu_c","name":"Grep","input":{"pattern":"TODO"}}]}}`,
				`{"type":"user","timestamp":"2026-10-01T10:00:03Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_c","content":"main.go:12: TODO"},{"type":"tool_result","tool_use_id":"toolu_a","content":"go.mod main.go"},{"type":"tool_result","tool_use_id":"toolu_b","is_error":true,"content":[{"type":"text","text":"permission denied"}]}]}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:05Z","message":{"role":"assistant","content":[{"type":"text","text":"Survey complete."}]}}`,
			}, "\n") + "\n",
			wantSteps:       8,
			wantSummaries:   1,
			wantSettled:     true,
			wantStoredTools: []string{"", "Bash", "Read", "Grep", "Grep", "Bash", "Read", ""},
			wantToolResults: []string{
				"toolu_c|Grep|DONE|main.go:12: TODO",
				"toolu_a|Bash|DONE|go.mod main.go",
				"toolu_b|Read|ERROR|permission denied",
			},
		},
		{
			name:      "claude tool result with unknown tool use id keeps empty name",
			sessionID: "sess-claude-unknown-id",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":"Resume work"}}`,
				`{"type":"user","timestamp":"2026-10-01T10:00:02Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_gone","content":"stale output"}]}}`,
			}, "\n") + "\n",
			wantSteps:       2,
			wantSummaries:   1,
			wantSettled:     true,
			wantStoredTools: []string{"", ""},
			wantToolResults: []string{"toolu_gone||DONE|stale output"},
		},
		{
			name:      "claude transcript with unparseable or ambient messages skipped",
			sessionID: "sess-claude-skips",
			content: strings.Join([]string{
				`{"type":"other","message":{}}`,
				`{"type":"user","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":12345}}`,
				`{"type":"user","timestamp":"2026-10-01T10:00:02Z","message":{"role":"user","content":"Valid prompt"}}`,
				`{"type":"assistant","timestamp":"2026-10-01T10:00:04Z","message":{"role":"assistant","content":12345}}`,
			}, "\n") + "\n",
			wantSteps:     1,
			wantSummaries: 1,
			wantSettled:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			tmp := t.TempDir()
			brainDir := filepath.Join(tmp, "brain")
			_ = os.MkdirAll(brainDir, 0o755)
			store := dbtest.NewFakeStore()

			var claudeProjectDir string
			if tc.customClaudeDir {
				claudeProjectDir = filepath.Join(tmp, "custom-claude-projects")
			} else {
				claudeProjectDir = filepath.Join(brainDir, "..", "claude", "projects")
			}
			projDir := filepath.Join(claudeProjectDir, "-test-project")
			_ = os.MkdirAll(projDir, 0o755)

			filePath := filepath.Join(projDir, tc.sessionID+".jsonl")
			_ = os.WriteFile(filePath, []byte(tc.content), 0o644)
			t0 := time.Now().Add(-time.Hour).Truncate(time.Second)
			_ = os.Chtimes(filePath, t0, t0)

			opts := SyncOptions{
				BrainDir:      brainDir,
				BatchLimit:    10,
				IdleThreshold: 10 * time.Minute,
				HeadLimit:     1000,
				TailLimit:     1000,
			}
			if tc.customClaudeDir {
				opts.ClaudeProjectsDir = claudeProjectDir
			}

			var llm func(ctx context.Context, prompt string) (string, error)
			if tc.withLLM {
				llm = func(ctx context.Context, prompt string) (string, error) {
					return "Claude session summary: analyzed disk.", nil
				}
			}

			var embedder func(ctx context.Context, text string) ([]float32, error)
			if tc.withEmbedder {
				embedder = func(ctx context.Context, text string) ([]float32, error) {
					vec := make([]float32, db.ExpectedEmbeddingDim)
					vec[0] = 0.55
					return vec, nil
				}
			}

			stats, err := SyncTranscripts(ctx, store, brainDir, embedder, llm, opts)
			if err != nil {
				t.Fatalf("SyncTranscripts failed: %v", err)
			}
			if stats.StepsInserted != tc.wantSteps {
				t.Fatalf("StepsInserted = %d, want %d", stats.StepsInserted, tc.wantSteps)
			}
			if stats.SummariesGenerated != tc.wantSummaries {
				t.Fatalf("SummariesGenerated = %d, want %d", stats.SummariesGenerated, tc.wantSummaries)
			}

			if tc.wantStoredTools != nil {
				stored, err := store.GetTranscriptStepRange(ctx, tc.sessionID, 0, 1000)
				if err != nil {
					t.Fatalf("GetTranscriptStepRange failed: %v", err)
				}
				gotTools := make([]string, 0, len(stored))
				for _, s := range stored {
					gotTools = append(gotTools, s.ToolName)
				}
				if !slices.Equal(gotTools, tc.wantStoredTools) {
					t.Fatalf("stored tool names = %q, want %q", gotTools, tc.wantStoredTools)
				}
			}
			if tc.wantToolResults != nil {
				scan, err := scanSessionTranscriptFile(filePath, nil)
				if err != nil {
					t.Fatalf("scanSessionTranscriptFile failed: %v", err)
				}
				var gotResults []string
				for _, s := range scan.parsedSteps {
					if s.Type == "TOOL" {
						gotResults = append(gotResults, strings.Join([]string{s.ToolCallID, s.ToolName, s.Status, s.Content}, "|"))
					}
				}
				if !slices.Equal(gotResults, tc.wantToolResults) {
					t.Fatalf("parsed tool results = %q, want %q", gotResults, tc.wantToolResults)
				}
			}

			states, err := store.GetSessionSyncStates(ctx)
			if err != nil {
				t.Fatalf("GetSessionSyncStates failed: %v", err)
			}
			st, ok := states[tc.sessionID]
			if !ok {
				t.Fatalf("expected state for session %q", tc.sessionID)
			}
			if st.IsSettled != tc.wantSettled {
				t.Fatalf("IsSettled = %v, want %v", st.IsSettled, tc.wantSettled)
			}

			if tc.secondPassContent != "" || tc.wantSkippedUnchanged > 0 {
				if tc.secondPassContent != "" {
					_ = os.WriteFile(filePath, []byte(tc.secondPassContent), 0o644)
					t1 := time.Now().Add(-30 * time.Minute).Truncate(time.Second)
					_ = os.Chtimes(filePath, t1, t1)
				}
				stats2, err := SyncTranscripts(ctx, store, brainDir, embedder, llm, opts)
				if err != nil {
					t.Fatalf("SyncTranscripts second pass failed: %v", err)
				}
				if tc.wantSkippedUnchanged > 0 && stats2.SkippedUnchanged != tc.wantSkippedUnchanged {
					t.Fatalf("second pass SkippedUnchanged = %d, want %d", stats2.SkippedUnchanged, tc.wantSkippedUnchanged)
				}
				if tc.secondPassSteps > 0 && stats2.StepsInserted != tc.secondPassSteps {
					t.Fatalf("second pass StepsInserted = %d, want %d", stats2.StepsInserted, tc.secondPassSteps)
				}
			}
		})
	}
}
