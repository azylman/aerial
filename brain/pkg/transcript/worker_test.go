package transcript

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

func writeTranscriptFile(t *testing.T, baseDir, sessionID, content string, mtime time.Time) string {
	t.Helper()
	logDir := filepath.Join(baseDir, sessionID, ".system_generated", "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	tPath := filepath.Join(logDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(content), 0644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(tPath, mtime, mtime); err != nil {
			t.Fatalf("chtimes failed: %v", err)
		}
	}
	return tPath
}

func TestSyncTranscripts_BasicSweepAndSettled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-alpha-001"
	tContent := strings.Join([]string{
		`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Please check system health and memory usage"}`,
		`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:02Z","tool_calls":[{"name":"run_command","args":{"CommandLine":"free -m"}}]}`,
		`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","created_at":"2026-10-01T10:00:05Z","content":"total: 32000, used: 8000, free: 24000"}`,
	}, "\n") + "\n"

	oldMtime := time.Now().Add(-1 * time.Hour)
	writeTranscriptFile(t, tempDir, sessionID, tContent, oldMtime)

	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return "Checked system health and free memory: 24GB available.", nil
	}
	mockEmbedder := func(ctx context.Context, text string) ([]float32, error) {
		vec := make([]float32, db.ExpectedEmbeddingDim)
		vec[0] = 0.88
		return vec, nil
	}

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      tempDir,
		HeadLimit:     1000,
		TailLimit:     1000,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, mockEmbedder, mockLLM, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}

	if stats.Scanned != 1 {
		t.Errorf("expected 1 scanned, got %d", stats.Scanned)
	}
	if stats.Synced != 1 {
		t.Errorf("expected 1 synced, got %d", stats.Synced)
	}
	if stats.StepsInserted != 3 {
		t.Errorf("expected 3 steps inserted, got %d", stats.StepsInserted)
	}
	if stats.SummariesGenerated != 1 {
		t.Errorf("expected 1 summary generated, got %d", stats.SummariesGenerated)
	}

	// Verify database state
	states, err := store.GetSessionSyncStates(ctx)
	if err != nil {
		t.Fatalf("GetSessionSyncStates: %v", err)
	}
	st, ok := states[sessionID]
	if !ok {
		t.Fatalf("expected sync state for session %s", sessionID)
	}
	if !st.IsSettled {
		t.Errorf("expected is_settled = true for idle session")
	}
	if st.LastIndexedStep != 2 {
		t.Errorf("expected last_indexed_step = 2, got %d", st.LastIndexedStep)
	}

	// Verify SearchTranscripts finds the session
	res, err := SearchTranscripts(ctx, store, mockEmbedder, "memory usage", "auto", "", "", 10)
	if err != nil {
		t.Fatalf("SearchTranscripts: %v", err)
	}
	if len(res.Sessions) != 1 {
		t.Fatalf("expected 1 session hit, got %d", len(res.Sessions))
	}
	if res.Sessions[0].Summary != "Checked system health and free memory: 24GB available." {
		t.Errorf("unexpected summary text: %s", res.Sessions[0].Summary)
	}
}

func TestSyncTranscripts_UnchangedMtimeSkipped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-skip-002"
	tContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Hello world"}` + "\n"
	oldMtime := time.Now().Add(-2 * time.Hour)
	writeTranscriptFile(t, tempDir, sessionID, tContent, oldMtime)

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      tempDir,
	}

	// Run 1: Should sync
	stats1, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts run 1 failed: %v", err)
	}
	if stats1.Synced != 1 {
		t.Fatalf("expected 1 synced on run 1, got %d", stats1.Synced)
	}

	// Run 2: Without modifying the file, should skip unchanged
	stats2, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts run 2 failed: %v", err)
	}
	if stats2.SkippedUnchanged != 1 {
		t.Errorf("expected 1 skipped unchanged on run 2, got %d", stats2.SkippedUnchanged)
	}
	if stats2.Synced != 0 {
		t.Errorf("expected 0 synced on run 2, got %d", stats2.Synced)
	}
}

func TestSyncTranscripts_PartialLineAtEOF(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-partial-003"
	// Step 0 and 1 are valid. Step 2 at EOF is cut off mid-write by an active writer.
	tContent := strings.Join([]string{
		`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Run long command"}`,
		`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:02Z","tool_calls":[{"name":"run_command","args":{"CommandLine":"sleep 5"}}]}`,
		`{"step_index":2,"source":"MODEL","type":"TOOL","stat`, // partial unclosed line
	}, "\n")

	writeTranscriptFile(t, tempDir, sessionID, tContent, time.Now().Add(-1*time.Hour))

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      tempDir,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.Errors != 0 {
		t.Errorf("expected 0 errors for EOF partial line, got %d", stats.Errors)
	}
	if stats.StepsInserted != 2 {
		t.Errorf("expected 2 valid steps inserted, got %d", stats.StepsInserted)
	}
}

func TestSyncTranscripts_CorruptedIntermediateLine(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-corrupt-004"
	// Line 2 is corrupted JSON in the middle of the file
	tContent := strings.Join([]string{
		`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Valid start"}`,
		`{corrupt_intermediate_json_line_here`,
		`{"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:04Z","content":"Recovered valid line"}`,
	}, "\n") + "\n"

	writeTranscriptFile(t, tempDir, sessionID, tContent, time.Now().Add(-1*time.Hour))

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      tempDir,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.Errors != 1 {
		t.Errorf("expected 1 error for corrupted intermediate line, got %d", stats.Errors)
	}
	if stats.StepsInserted != 2 {
		t.Errorf("expected 2 valid steps inserted despite intermediate corrupt line, got %d", stats.StepsInserted)
	}
}

func TestSyncTranscripts_TruncationDetection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-trunc-005"

	// Pre-populate DB as if session had previously synced up to step 10
	err := store.UpsertSessionSummary(ctx, db.SessionSummary{
		SessionID:       sessionID,
		Summary:         "Old prior session summary",
		LastIndexedStep: 10,
		LastMtime:       time.Now().Add(-2 * time.Hour),
		IsSettled:       true,
		CreatedAt:       time.Now().Add(-3 * time.Hour),
	})
	if err != nil {
		t.Fatalf("pre-populate summary: %v", err)
	}

	// New file rewritten / rotated from step 0
	newContent := strings.Join([]string{
		`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T11:00:00Z","content":"Rotated session restart"}`,
		`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T11:00:02Z","content":"Fresh start acknowledged"}`,
	}, "\n") + "\n"

	newMtime := time.Now().Add(-10 * time.Minute)
	writeTranscriptFile(t, tempDir, sessionID, newContent, newMtime)

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 5 * time.Minute,
		BrainDir:      tempDir,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}

	// Truncation detected: all steps 0..1 inserted
	if stats.StepsInserted != 2 {
		t.Errorf("expected 2 steps inserted after truncation reset, got %d", stats.StepsInserted)
	}

	states, _ := store.GetSessionSyncStates(ctx)
	if states[sessionID].LastIndexedStep != 1 {
		t.Errorf("expected last_indexed_step = 1, got %d", states[sessionID].LastIndexedStep)
	}
}

func TestSyncTranscripts_HistoricalSessionWithoutLLM(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-hist-006"
	tContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-01T10:00:00Z","content":"Deploy mirrormere dashboard v2"}` + "\n"
	writeTranscriptFile(t, tempDir, sessionID, tContent, time.Now().Add(-48*time.Hour))

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      tempDir,
	}

	// No LLM provided
	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.SummariesGenerated != 1 {
		t.Errorf("expected 1 summary generated, got %d", stats.SummariesGenerated)
	}

	res, err := SearchTranscripts(ctx, store, nil, "mirrormere", "sessions", "", "", 5)
	if err != nil {
		t.Fatalf("SearchTranscripts failed: %v", err)
	}
	if len(res.Sessions) != 1 {
		t.Fatalf("expected 1 session hit, got %d", len(res.Sessions))
	}
	if res.Sessions[0].Summary != "Deploy mirrormere dashboard v2" {
		t.Errorf("expected Step-0 shortcut summary %q, got %q", "Deploy mirrormere dashboard v2", res.Sessions[0].Summary)
	}
}

func TestSyncTranscripts_LLMFallbackToStep0(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-fallback-007"
	tContent := strings.Join([]string{
		`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T10:00:00Z","content":"Initial prompt before LLM error"}`,
		`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T10:00:02Z","content":"Some model reply"}`,
	}, "\n") + "\n"
	writeTranscriptFile(t, tempDir, sessionID, tContent, time.Now().Add(-1*time.Hour))

	failingLLM := func(ctx context.Context, prompt string) (string, error) {
		return "", errors.New("rate limit exceeded")
	}

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 5 * time.Minute,
		BrainDir:      tempDir,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, failingLLM, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.SummariesGenerated != 1 {
		t.Fatalf("expected 1 summary generated, got %d", stats.SummariesGenerated)
	}

	res, err := SearchTranscripts(ctx, store, nil, "Initial prompt", "sessions", "", "", 5)
	if err != nil {
		t.Fatalf("SearchTranscripts failed: %v", err)
	}
	if len(res.Sessions) != 1 {
		t.Fatalf("expected 1 session hit, got %d", len(res.Sessions))
	}
	if res.Sessions[0].Summary != "Initial prompt before LLM error" {
		t.Errorf("expected fallback to step-0 prompt, got %q", res.Sessions[0].Summary)
	}
}

func TestSyncTranscripts_ActiveSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	sessionID := "sess-active-008"
	tContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Active work in progress"}` + "\n"
	// Active: modified just 10 seconds ago
	recentMtime := time.Now().Add(-10 * time.Second)
	writeTranscriptFile(t, tempDir, sessionID, tContent, recentMtime)

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute, // Session is active (< 10m)
		BrainDir:      tempDir,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.StepsInserted != 1 {
		t.Errorf("expected 1 step inserted, got %d", stats.StepsInserted)
	}

	states, _ := store.GetSessionSyncStates(ctx)
	if states[sessionID].IsSettled {
		t.Errorf("expected IsSettled = false for active session")
	}
}

func TestSyncTranscripts_BatchLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	// Create 3 sessions
	for i := 1; i <= 3; i++ {
		sid := fmt.Sprintf("sess-batch-%03d", i)
		content := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"Batch task %d"}`+"\n", i)
		writeTranscriptFile(t, tempDir, sid, content, time.Now().Add(-1*time.Hour))
	}

	opts := SyncOptions{
		BatchLimit: 1, // Only allow 1 per sweep
		BrainDir:   tempDir,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.Synced != 1 {
		t.Errorf("expected BatchLimit to cap synced at 1, got %d", stats.Synced)
	}
}

func TestSyncTranscripts_ErrorCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("nil store", func(t *testing.T) {
		t.Parallel()
		_, err := SyncTranscripts(ctx, nil, "/tmp", nil, nil, SyncOptions{})
		if err == nil || !strings.Contains(err.Error(), "transcript store cannot be nil") {
			t.Fatalf("expected nil store error, got %v", err)
		}
	})

	t.Run("empty brain dir", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		_, err := SyncTranscripts(ctx, store, "", nil, nil, SyncOptions{})
		if err == nil || !strings.Contains(err.Error(), "brain directory cannot be empty") {
			t.Fatalf("expected empty brain dir error, got %v", err)
		}
	})

	t.Run("non-existent brain dir", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		_, err := SyncTranscripts(ctx, store, "/non/existent/path/for/transcripts/test", nil, nil, SyncOptions{})
		if err == nil {
			t.Fatalf("expected error for non-existent brain dir")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		t.Parallel()
		store := db.NewFakeStore()
		tempDir := t.TempDir()
		writeTranscriptFile(t, tempDir, "sess-cancel", `{"step_index":0,"content":"hello"}`, time.Now().Add(-1*time.Hour))

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := SyncTranscripts(cancelCtx, store, tempDir, nil, nil, SyncOptions{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
}

func TestFindTranscriptFile_AlternativeLocations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	// 1. system_generated/logs/transcript.jsonl (without leading dot)
	sess1 := "sess-nodot"
	dir1 := filepath.Join(tempDir, sess1, "system_generated", "logs")
	_ = os.MkdirAll(dir1, 0755)
	_ = os.WriteFile(filepath.Join(dir1, "transcript.jsonl"), []byte(`{"step_index":0,"content":"no dot"}`+"\n"), 0644)

	// 2. transcript.jsonl directly under session dir
	sess2 := "sess-root"
	dir2 := filepath.Join(tempDir, sess2)
	_ = os.MkdirAll(dir2, 0755)
	_ = os.WriteFile(filepath.Join(dir2, "transcript.jsonl"), []byte(`{"step_index":0,"content":"root"}`+"\n"), 0644)

	// 3. Hidden directory and regular file (should be ignored)
	_ = os.MkdirAll(filepath.Join(tempDir, ".hidden-dir"), 0755)
	_ = os.WriteFile(filepath.Join(tempDir, "regular-file.txt"), []byte("not a dir"), 0644)

	// 4. Session directory without any transcript file (should be ignored)
	_ = os.MkdirAll(filepath.Join(tempDir, "sess-empty-dir"), 0755)

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, DefaultSyncOptions())
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.Synced != 2 {
		t.Errorf("expected 2 synced from alternative locations, got %d", stats.Synced)
	}
}

type mockTranscriptStore struct {
	db.TranscriptStore
	getSessionSyncStatesFn       func(ctx context.Context) (map[string]db.SessionSyncState, error)
	upsertSessionSummaryFn       func(ctx context.Context, summary db.SessionSummary) error
	batchInsertTranscriptStepsFn func(ctx context.Context, steps []db.TranscriptStep) error
}

func (m *mockTranscriptStore) GetSessionSyncStates(ctx context.Context) (map[string]db.SessionSyncState, error) {
	if m.getSessionSyncStatesFn != nil {
		return m.getSessionSyncStatesFn(ctx)
	}
	return m.TranscriptStore.GetSessionSyncStates(ctx)
}

func (m *mockTranscriptStore) UpsertSessionSummary(ctx context.Context, summary db.SessionSummary) error {
	if m.upsertSessionSummaryFn != nil {
		return m.upsertSessionSummaryFn(ctx, summary)
	}
	return m.TranscriptStore.UpsertSessionSummary(ctx, summary)
}

func (m *mockTranscriptStore) BatchInsertTranscriptSteps(ctx context.Context, steps []db.TranscriptStep) error {
	if m.batchInsertTranscriptStepsFn != nil {
		return m.batchInsertTranscriptStepsFn(ctx, steps)
	}
	return m.TranscriptStore.BatchInsertTranscriptSteps(ctx, steps)
}

func TestClipContent_NegativeAndDefaultLimits(t *testing.T) {
	t.Parallel()

	// 1. Default limits when both <= 0
	shortText := "short content"
	clippedShort := ClipContent(shortText, 0, 0)
	if clippedShort != shortText {
		t.Errorf("expected %q, got %q", shortText, clippedShort)
	}

	clippedNeg := ClipContent(shortText, -10, -20)
	if clippedNeg != shortText {
		t.Errorf("expected %q, got %q", shortText, clippedNeg)
	}

	// Oversized text with default limits
	oversized := strings.Repeat("x", DefaultHeadLimit+DefaultTailLimit+100)
	clippedOversized := ClipContent(oversized, 0, -1)
	expectedLen := DefaultHeadLimit + len(TruncationMarker) + DefaultTailLimit
	if len(clippedOversized) != expectedLen {
		t.Errorf("expected length %d, got %d", expectedLen, len(clippedOversized))
	}
	if !strings.Contains(clippedOversized, TruncationMarker) {
		t.Errorf("expected truncation marker in oversized content")
	}

	// 2. Negative headLimit, positive tailLimit
	text := "0123456789abcdefghij"
	clippedHeadNeg := ClipContent(text, -5, 5)
	expectedHeadNeg := TruncationMarker + "fghij"
	if clippedHeadNeg != expectedHeadNeg {
		t.Errorf("expected %q, got %q", expectedHeadNeg, clippedHeadNeg)
	}

	// 3. Positive headLimit, negative tailLimit
	clippedTailNeg := ClipContent(text, 5, -5)
	expectedTailNeg := "01234" + TruncationMarker
	if clippedTailNeg != expectedTailNeg {
		t.Errorf("expected %q, got %q", expectedTailNeg, clippedTailNeg)
	}
}

func TestSyncTranscripts_NilContextAndErrors(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	sessionID := "sess-nil-ctx-001"
	tContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Hello nil ctx"}` + "\n"
	writeTranscriptFile(t, tempDir, sessionID, tContent, time.Now().Add(-1*time.Hour))

	store := db.NewFakeStore()
	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      tempDir,
	}

	// 1. nil ctx should fallback to context.Background() without error
	stats, err := SyncTranscripts(nil, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("expected nil error with nil ctx, got: %v", err)
	}
	if stats.Synced != 1 {
		t.Errorf("expected 1 synced, got %d", stats.Synced)
	}

	// 2. error from GetSessionSyncStates
	mockErrStore := &mockTranscriptStore{
		TranscriptStore: db.NewFakeStore(),
		getSessionSyncStatesFn: func(ctx context.Context) (map[string]db.SessionSyncState, error) {
			return nil, errors.New("simulated sync states query failure")
		},
	}
	_, err = SyncTranscripts(context.Background(), mockErrStore, tempDir, nil, nil, opts)
	if err == nil || !strings.Contains(err.Error(), "load session sync states") {
		t.Fatalf("expected load session sync states error, got %v", err)
	}

	// 3. os.Open failure on transcript path / error from syncSingleSession
	sockPath := filepath.Join(os.TempDir(), fmt.Sprintf("ae_t_%d.sock", time.Now().UnixNano()))
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("net.Listen unix socket failed: %v", err)
	}
	defer l.Close()
	defer os.Remove(sockPath)

	sockTempDir := t.TempDir()
	sockSessionID := "sess-sock-err"
	sockDir := filepath.Join(sockTempDir, sockSessionID, ".system_generated", "logs")
	if err := os.MkdirAll(sockDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.Symlink(sockPath, filepath.Join(sockDir, "transcript.jsonl")); err != nil {
		t.Fatalf("symlink failed: %v", err)
	}

	sockOpts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      sockTempDir,
	}
	sockStats, err := SyncTranscripts(context.Background(), db.NewFakeStore(), sockTempDir, nil, nil, sockOpts)
	if err != nil {
		t.Fatalf("expected nil sweep error, got %v", err)
	}
	if sockStats.Errors != 1 {
		t.Errorf("expected 1 error for failed socket os.Open, got %d", sockStats.Errors)
	}
}

func TestSyncSingleSession_TimestampsAndWatermarks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tempDir := t.TempDir()
	store := db.NewFakeStore()

	// 1. Test RFC3339 timestamp parsing, RFC3339Nano, fallback to step0Prompt when step 0 is planner response,
	// and blank line / non-substantive step filtering.
	sessionID := "sess-timestamps-001"
	lines := []string{
		"", // Blank line
		`{"step_index":0,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Initial thinking before user"}`,
		`{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:01.123456789Z","content":"User follow-up prompt"}`,
		`{"step_index":2,"source":"MODEL","type":"HEARTBEAT","status":"DONE","created_at":"invalid-timestamp","content":"heartbeat ping"}`, // non-substantive
		"", // Another blank line
	}
	tContent := strings.Join(lines, "\n") + "\n"
	writeTranscriptFile(t, tempDir, sessionID, tContent, time.Now().Add(-1*time.Hour))

	opts := SyncOptions{
		BatchLimit:    10,
		IdleThreshold: 10 * time.Minute,
		BrainDir:      tempDir,
	}

	stats, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts failed: %v", err)
	}
	if stats.StepsInserted != 2 {
		t.Errorf("expected 2 substantive steps inserted, got %d", stats.StepsInserted)
	}

	states, err := store.GetSessionSyncStates(ctx)
	if err != nil {
		t.Fatalf("GetSessionSyncStates failed: %v", err)
	}
	if states[sessionID].LastIndexedStep != 2 {
		t.Errorf("expected LastIndexedStep=2, got %d", states[sessionID].LastIndexedStep)
	}

	// 2. Test raw.StepIndex <= watermark skip when session exists and incremental steps arrive
	// Here firstParsedStep (2) >= state.LastIndexedStep (2), so watermark = 2.
	// Step 2 is skipped (2 <= 2), step 3 is inserted.
	newLines := []string{
		`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","created_at":"2026-10-01T12:00:02Z","content":"Old step 2"}`,
		`{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T12:00:03Z","content":"New step 3"}`,
	}
	writeTranscriptFile(t, tempDir, sessionID, strings.Join(newLines, "\n")+"\n", time.Now().Add(-30*time.Minute))

	stats2, err := SyncTranscripts(ctx, store, tempDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts second pass failed: %v", err)
	}
	if stats2.StepsInserted != 1 {
		t.Errorf("expected exactly 1 new step inserted (step 3), got %d", stats2.StepsInserted)
	}
}

func TestSyncSingleSession_BatchInsertAndUpsertErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. BatchInsertTranscriptSteps error
	t.Run("batch insert error", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		sessID := "sess-batch-err"
		tContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Batch err test"}` + "\n"
		writeTranscriptFile(t, tempDir, sessID, tContent, time.Now().Add(-1*time.Hour))

		mock := &mockTranscriptStore{
			TranscriptStore: db.NewFakeStore(),
			batchInsertTranscriptStepsFn: func(ctx context.Context, steps []db.TranscriptStep) error {
				return errors.New("simulated batch insert error")
			},
		}

		stats, err := SyncTranscripts(ctx, mock, tempDir, nil, nil, SyncOptions{BrainDir: tempDir})
		if err != nil {
			t.Fatalf("SyncTranscripts failed: %v", err)
		}
		if stats.Errors != 1 {
			t.Errorf("expected 1 error for batch insert failure, got %d", stats.Errors)
		}
		if stats.StepsInserted != 0 {
			t.Errorf("expected 0 steps inserted, got %d", stats.StepsInserted)
		}
	})

	// 2. UpsertSessionSummary error on settled session
	t.Run("upsert settled session error", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		sessID := "sess-upsert-settled-err"
		tContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Upsert err test"}` + "\n"
		writeTranscriptFile(t, tempDir, sessID, tContent, time.Now().Add(-1*time.Hour))

		mock := &mockTranscriptStore{
			TranscriptStore: db.NewFakeStore(),
			upsertSessionSummaryFn: func(ctx context.Context, summary db.SessionSummary) error {
				return errors.New("simulated upsert summary error")
			},
		}

		stats, err := SyncTranscripts(ctx, mock, tempDir, nil, nil, SyncOptions{BrainDir: tempDir})
		if err != nil {
			t.Fatalf("SyncTranscripts failed: %v", err)
		}
		if stats.Errors != 1 {
			t.Errorf("expected 1 error for upsert summary failure, got %d", stats.Errors)
		}
		if stats.SummariesGenerated != 0 {
			t.Errorf("expected 0 summaries generated, got %d", stats.SummariesGenerated)
		}
	})

	// 3. UpsertSessionSummary on empty session (empty file: watermark=-1 && maxStepIndex=-1)
	t.Run("empty session upsert success and error", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		sessID := "sess-empty-file"
		// Write completely empty transcript.jsonl
		writeTranscriptFile(t, tempDir, sessID, "", time.Now().Add(-1*time.Hour))

		fakeStore := db.NewFakeStore()
		stats, err := SyncTranscripts(ctx, fakeStore, tempDir, nil, nil, SyncOptions{BrainDir: tempDir})
		if err != nil {
			t.Fatalf("SyncTranscripts failed on empty session: %v", err)
		}
		if stats.SummariesGenerated != 1 {
			t.Errorf("expected 1 summary generated for empty session, got %d", stats.SummariesGenerated)
		}

		states, err := fakeStore.GetSessionSyncStates(ctx)
		if err != nil {
			t.Fatalf("GetSessionSyncStates failed: %v", err)
		}
		// When watermark == -1 and maxStepIndex == -1, lastIndexedStepToSave is -2
		if states[sessID].LastIndexedStep != -2 {
			t.Errorf("expected LastIndexedStep = -2 for empty session, got %d", states[sessID].LastIndexedStep)
		}

		// Now test with upsert error on empty session
		mockErr := &mockTranscriptStore{
			TranscriptStore: db.NewFakeStore(),
			upsertSessionSummaryFn: func(ctx context.Context, summary db.SessionSummary) error {
				return errors.New("simulated upsert error on empty session")
			},
		}
		statsErr, err := SyncTranscripts(ctx, mockErr, tempDir, nil, nil, SyncOptions{BrainDir: tempDir})
		if err != nil {
			t.Fatalf("SyncTranscripts failed: %v", err)
		}
		if statsErr.Errors != 1 {
			t.Errorf("expected 1 error on empty session upsert failure, got %d", statsErr.Errors)
		}
	})

	// 4. UpsertSessionSummary error on active session progress (else if exists)
	t.Run("upsert active session progress error", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		sessID := "sess-active-err"

		// Pre-populate DB state as existing active session
		baseStore := db.NewFakeStore()
		_ = baseStore.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID:       sessID,
			Summary:         "Initial active session",
			LastIndexedStep: 0,
			LastMtime:       time.Now().Add(-5 * time.Minute),
			IsSettled:       false,
		})

		// Write new active step (modified 5 seconds ago, < 10m idle threshold)
		newContent := strings.Join([]string{
			`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-10-01T12:00:00Z","content":"Step 0"}`,
			`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-10-01T12:00:05Z","content":"Step 1"}`,
		}, "\n") + "\n"
		writeTranscriptFile(t, tempDir, sessID, newContent, time.Now().Add(-5*time.Second))

		mockActiveErr := &mockTranscriptStore{
			TranscriptStore: baseStore,
			upsertSessionSummaryFn: func(ctx context.Context, summary db.SessionSummary) error {
				return errors.New("simulated active progress upsert failure")
			},
		}

		stats, err := SyncTranscripts(ctx, mockActiveErr, tempDir, nil, nil, SyncOptions{
			BrainDir:      tempDir,
			IdleThreshold: 10 * time.Minute,
		})
		if err != nil {
			t.Fatalf("SyncTranscripts failed: %v", err)
		}
		if stats.Errors != 1 {
			t.Errorf("expected 1 error on active progress upsert error, got %d", stats.Errors)
		}
	})

	// 5. Settled session re-summarization when new steps > 10
	t.Run("settled session re-summarization when new steps > 10", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		sessID := "sess-resummarize"

		baseStore := db.NewFakeStore()
		_ = baseStore.UpsertSessionSummary(ctx, db.SessionSummary{
			SessionID:            sessID,
			Summary:              "Initial settled summary",
			LastIndexedStep:      2,
			SummaryStepWatermark: 2,
			LastMtime:            time.Now().Add(-2 * time.Hour),
			IsSettled:            true,
		})

		var stepLines []string
		for i := 0; i <= 15; i++ {
			stepLines = append(stepLines, fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","content":"Step %d"}`, i, i))
		}
		writeTranscriptFile(t, tempDir, sessID, strings.Join(stepLines, "\n")+"\n", time.Now().Add(-1*time.Hour))

		stats, err := SyncTranscripts(ctx, baseStore, tempDir, nil, nil, SyncOptions{
			BrainDir:      tempDir,
			IdleThreshold: 10 * time.Minute,
		})
		if err != nil {
			t.Fatalf("SyncTranscripts failed: %v", err)
		}
		if stats.SummariesGenerated != 1 {
			t.Errorf("expected 1 re-summarization when delta > 10, got %d", stats.SummariesGenerated)
		}
	})
}

