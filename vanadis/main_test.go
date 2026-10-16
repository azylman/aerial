package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// --- Mock pgx implementations for hermetic unit testing of PostgresVanadisStore ---

type mockRow struct {
	scanFn func(dest ...any) error
}

func (r *mockRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type mockRows struct {
	pgx.Rows
	items   []VanadisChunk
	idx     int
	scanErr error
	rowsErr error
}

func (r *mockRows) Close() {}

func (r *mockRows) Err() error { return r.rowsErr }

func (r *mockRows) Next() bool {
	if r.idx < len(r.items) {
		r.idx++
		return true
	}
	return false
}

func (r *mockRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	c := r.items[r.idx-1]
	*(dest[0].(*string)) = c.ID
	*(dest[1].(*string)) = c.BuildID
	*(dest[2].(*int)) = c.ChunkIndex
	*(dest[3].(*string)) = c.Repo
	*(dest[4].(*string)) = c.Title
	*(dest[5].(*string)) = c.Instructions
	*(dest[6].(*[]string)) = c.TargetFiles
	*(dest[7].(*[]string)) = c.TargetSymbols
	*(dest[8].(*string)) = c.Effort
	*(dest[9].(*[]string)) = c.AcceptanceCriteria
	*(dest[10].(*string)) = c.Status
	*(dest[11].(*int)) = c.PRNumber
	*(dest[12].(*string)) = c.DiffStat
	if !c.StartedAt.IsZero() {
		*(dest[13].(*sql.NullTime)) = sql.NullTime{Time: c.StartedAt, Valid: true}
	} else {
		*(dest[13].(*sql.NullTime)) = sql.NullTime{}
	}
	*(dest[14].(*time.Time)) = c.CreatedAt
	*(dest[15].(*time.Time)) = c.UpdatedAt
	return nil
}

type mockTx struct {
	pgx.Tx
	execFn     func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	commitErr  error
	rollbackFn func(ctx context.Context) error
}

func (t *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, arguments...)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (t *mockTx) Commit(_ context.Context) error {
	return t.commitErr
}

func (t *mockTx) Rollback(ctx context.Context) error {
	if t.rollbackFn != nil {
		return t.rollbackFn(ctx)
	}
	return nil
}

type mockDB struct {
	execFn     func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	beginFn    func(ctx context.Context) (pgx.Tx, error)
}

func (db *mockDB) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if db.execFn != nil {
		return db.execFn(ctx, sql, arguments...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (db *mockDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if db.queryFn != nil {
		return db.queryFn(ctx, sql, args...)
	}
	return &mockRows{}, nil
}

func (db *mockDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if db.queryRowFn != nil {
		return db.queryRowFn(ctx, sql, args...)
	}
	return &mockRow{}
}

func (db *mockDB) Begin(ctx context.Context) (pgx.Tx, error) {
	if db.beginFn != nil {
		return db.beginFn(ctx)
	}
	return &mockTx{}, nil
}

// --- In-memory VanadisStore & RiverInserter for HTTP and Worker tests ---

type memoryVanadisStore struct {
	builds         map[string]VanadisBuild
	chunks         map[string][]VanadisChunk
	createErr      error
	getErr         error
	updateChunkErr error
	updateBuildErr error
}

func newMemoryVanadisStore() *memoryVanadisStore {
	return &memoryVanadisStore{
		builds: make(map[string]VanadisBuild),
		chunks: make(map[string][]VanadisChunk),
	}
}

func (m *memoryVanadisStore) CreateBuild(_ context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.builds[build.ID] = build
	copied := make([]VanadisChunk, len(chunks))
	copy(copied, chunks)
	m.chunks[build.ID] = copied
	return nil
}

func (m *memoryVanadisStore) GetBuild(_ context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	b, ok := m.builds[buildID]
	if !ok {
		return nil, nil, ErrBuildNotFound
	}
	copied := make([]VanadisChunk, len(m.chunks[buildID]))
	copy(copied, m.chunks[buildID])
	return &b, copied, nil
}

func (m *memoryVanadisStore) UpdateChunkStatus(_ context.Context, chunkID, status string) error {
	if m.updateChunkErr != nil {
		return m.updateChunkErr
	}
	if status == "failed" && m.updateBuildErr != nil {
		return m.updateBuildErr
	}
	for bID, list := range m.chunks {
		for i := range list {
			if list[i].ID == chunkID {
				list[i].Status = status
				if (status == "running" || status == "coding") && list[i].StartedAt.IsZero() {
					list[i].StartedAt = time.Now().UTC()
				}
				list[i].UpdatedAt = time.Now().UTC()
				m.chunks[bID] = list
				return nil
			}
		}
	}
	return ErrChunkNotFound
}

func (m *memoryVanadisStore) UpdateBuildStatus(_ context.Context, buildID, status string) error {
	if m.updateBuildErr != nil {
		return m.updateBuildErr
	}
	b, ok := m.builds[buildID]
	if !ok {
		return ErrBuildNotFound
	}
	b.Status = status
	m.builds[buildID] = b
	return nil
}

func (m *memoryVanadisStore) UpdateBuildStatusMessageID(_ context.Context, buildID, messageID string) error {
	if m.updateBuildErr != nil {
		return m.updateBuildErr
	}
	b, ok := m.builds[buildID]
	if !ok {
		return ErrBuildNotFound
	}
	b.StatusMessageID = messageID
	m.builds[buildID] = b
	return nil
}

type mockRiverInserter struct {
	inserted []river.JobArgs
	opts     []*river.InsertOpts
	err      error
}

func (m *mockRiverInserter) Insert(_ context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.inserted = append(m.inserted, args)
	m.opts = append(m.opts, opts)
	return &rivertype.JobInsertResult{}, nil
}

type mockRiverLifecycle struct {
	mockRiverInserter
	startErr error
	stopErr  error
}

func (m *mockRiverLifecycle) Start(_ context.Context) error { return m.startErr }
func (m *mockRiverLifecycle) Stop(_ context.Context) error  { return m.stopErr }

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("simulated read error")
}

// --- Tests ---

func TestLoadConfigAndEnv(t *testing.T) {
	origFallback := vanadisFallbackConfigPath
	origLookup := lookupHostFn
	defer func() {
		vanadisFallbackConfigPath = origFallback
		lookupHostFn = origLookup
	}()

	for _, k := range []string{"PORT", "POSTGRES_URL", "DATABASE_URL", "BRAIN_INTERNAL_URL", "BRAIN_URL", "CONFIG_PATH"} {
		t.Setenv(k, "")
	}

	tmpDir := t.TempDir()
	vanadisFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")

	t.Run("DNSFallback_AerialPostgres", func(t *testing.T) {
		lookupHostFn = func(_ context.Context, host string) ([]string, error) {
			if host == "aerial-postgres" {
				return []string{"10.0.0.2"}, nil
			}
			return nil, errors.New("not found")
		}
		cfg := LoadConfig("")
		if cfg.Port != "4025" || cfg.BrainURL != "http://127.0.0.1:8088" {
			t.Fatalf("unexpected defaults: %+v", cfg)
		}
		if !strings.Contains(cfg.PostgresURL, "@aerial-postgres:5432/") {
			t.Fatalf("expected aerial-postgres URL, got %s", cfg.PostgresURL)
		}
	})

	t.Run("DNSFallback_Postgres", func(t *testing.T) {
		lookupHostFn = func(_ context.Context, host string) ([]string, error) {
			if host == "postgres" {
				return []string{"10.0.0.3"}, nil
			}
			return nil, errors.New("not found")
		}
		cfg := LoadConfig("")
		if !strings.Contains(cfg.PostgresURL, "@postgres:5432/") {
			t.Fatalf("expected postgres URL, got %s", cfg.PostgresURL)
		}
	})

	t.Run("DNSFallback_Localhost", func(t *testing.T) {
		lookupHostFn = func(_ context.Context, _ string) ([]string, error) {
			return nil, errors.New("not found")
		}
		cfg := LoadConfig("   ")
		if !strings.Contains(cfg.PostgresURL, "@127.0.0.1:5432/") {
			t.Fatalf("expected 127.0.0.1 URL, got %s", cfg.PostgresURL)
		}
	})

	t.Run("YAMLFileAndFallbackPath", func(t *testing.T) {
		yamlPath := filepath.Join(tmpDir, "vanadis.yaml")
		yamlContent := `
port: "4099"
postgres_url: "postgres://u:p@db:5432/vanadis"
brain_url: "http://brain.aerial:8088/"
`
		if err := os.WriteFile(yamlPath, []byte(yamlContent), 0o600); err != nil {
			t.Fatalf("write yaml: %v", err)
		}
		vanadisFallbackConfigPath = yamlPath
		cfg := LoadConfig(filepath.Join(tmpDir, "missing.yaml"))
		if cfg.Port != "4099" || cfg.PostgresURL != "postgres://u:p@db:5432/vanadis" || cfg.BrainURL != "http://brain.aerial:8088" {
			t.Fatalf("unexpected yaml config: %+v", cfg)
		}

		badPath := filepath.Join(tmpDir, "bad.yaml")
		_ = os.WriteFile(badPath, []byte(":::invalid_yaml"), 0o600)
		t.Setenv("CONFIG_PATH", badPath)
		t.Setenv("PORT", "4030")
		t.Setenv("POSTGRES_URL", "postgres://env:5432/db")
		t.Setenv("BRAIN_INTERNAL_URL", "http://internal-brain:8088/")

		envCfg := LoadConfigFromEnv()
		if envCfg.Port != "4030" || envCfg.PostgresURL != "postgres://env:5432/db" || envCfg.BrainURL != "http://internal-brain:8088" {
			t.Fatalf("unexpected env override config: %+v", envCfg)
		}
	})
}

func TestExecuteChunkArgsAndNormalizeRepo(t *testing.T) {
	t.Parallel()
	args := ExecuteChunkArgs{BuildID: "b-1", ChunkIndex: 2}
	if args.Kind() != "vanadis_execute_chunk" {
		t.Fatalf("unexpected kind: %s", args.Kind())
	}
	opts := args.InsertOpts()
	if opts.Queue != VanadisQueueName || opts.MaxAttempts != 3 || !opts.UniqueOpts.ByArgs {
		t.Fatalf("unexpected insert opts: %+v", opts)
	}

	if got := normalizeRepo(""); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
	if got := normalizeRepo("aerial"); got != "azylman/aerial" {
		t.Fatalf("expected azylman/aerial, got %q", got)
	}
	if got := normalizeRepo("azylman/aerial-config"); got != "azylman/aerial-config" {
		t.Fatalf("expected azylman/aerial-config, got %q", got)
	}
}

func TestPostgresVanadisStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var nilStore *PostgresVanadisStore
	if err := nilStore.CreateBuild(ctx, VanadisBuild{}, nil); err == nil {
		t.Fatal("expected error on nil store CreateBuild")
	}
	if _, _, err := nilStore.GetBuild(ctx, "b1"); err == nil {
		t.Fatal("expected error on nil store GetBuild")
	}
	if err := nilStore.UpdateChunkStatus(ctx, "c1", "running"); err == nil {
		t.Fatal("expected error on nil store UpdateChunkStatus")
	}
	if err := nilStore.UpdateBuildStatus(ctx, "b1", "completed"); err == nil {
		t.Fatal("expected error on nil store UpdateBuildStatus")
	}

	validBuild := VanadisBuild{
		ID:            "b-1",
		Repo:          "aerial",
		ThreadID:      "12345",
		Objective:     "Ship Vanadis",
		SharedContext: "Shared ctx test",
	}
	validChunks := []VanadisChunk{
		{
			ID:                 "c-1",
			ChunkIndex:         0,
			Title:              "T1",
			Instructions:       "I1",
			TargetFiles:        []string{"main.go"},
			TargetSymbols:      []string{"Foo"},
			Effort:             "medium",
			AcceptanceCriteria: []string{"Must pass"},
			PRNumber:           42,
		},
	}

	store := NewPostgresVanadisStore(&mockDB{})

	// Validation error cases for CreateBuild
	invalidCases := []struct {
		name   string
		build  VanadisBuild
		chunks []VanadisChunk
	}{
		{"empty build id", VanadisBuild{Repo: "aerial", ThreadID: "1", Objective: "o"}, nil},
		{"empty repo", VanadisBuild{ID: "b", ThreadID: "1", Objective: "o"}, nil},
		{"empty thread", VanadisBuild{ID: "b", Repo: "aerial", Objective: "o"}, nil},
		{"empty objective", VanadisBuild{ID: "b", Repo: "aerial", ThreadID: "1"}, nil},
		{"empty chunk id", validBuild, []VanadisChunk{{Title: "t", Instructions: "i"}}},
		{"mismatched chunk build_id", validBuild, []VanadisChunk{{ID: "c", BuildID: "other", Title: "t", Instructions: "i"}}},
		{"empty chunk title", validBuild, []VanadisChunk{{ID: "c", Instructions: "i"}}},
		{"empty chunk instructions", validBuild, []VanadisChunk{{ID: "c", Title: "t"}}},
		{"invalid chunk effort", validBuild, []VanadisChunk{{ID: "c", Title: "t", Instructions: "i", Effort: "ultra"}}},
	}
	for _, tc := range invalidCases {
		if err := store.CreateBuild(ctx, tc.build, tc.chunks); err == nil {
			t.Fatalf("expected error for case %s", tc.name)
		}
	}

	// CreateBuild success (including StartedAt variants: pending/zero, explicit StartedAt, and running status)
	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	createChunks := []VanadisChunk{
		validChunks[0],
		{ID: "c-2", ChunkIndex: 1, Title: "T2", Instructions: "I2", StartedAt: explicitStart},
		{ID: "c-3", ChunkIndex: 2, Title: "T3", Instructions: "I3", Status: "running"},
	}
	if err := store.CreateBuild(ctx, validBuild, createChunks); err != nil {
		t.Fatalf("CreateBuild failed: %v", err)
	}

	// CreateBuild tx error paths
	beginErrStore := NewPostgresVanadisStore(&mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return nil, errors.New("begin error")
		},
	})
	if err := beginErrStore.CreateBuild(ctx, validBuild, validChunks); err == nil {
		t.Fatal("expected begin tx error")
	}

	buildExecErrStore := NewPostgresVanadisStore(&mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				execFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
					return pgconn.CommandTag{}, errors.New("insert build error")
				},
			}, nil
		},
	})
	if err := buildExecErrStore.CreateBuild(ctx, validBuild, validChunks); err == nil {
		t.Fatal("expected build insert error")
	}

	chunkExecCalls := 0
	chunkExecErrStore := NewPostgresVanadisStore(&mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				execFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
					chunkExecCalls++
					if chunkExecCalls == 2 {
						return pgconn.CommandTag{}, errors.New("insert chunk error")
					}
					return pgconn.NewCommandTag("INSERT 0 1"), nil
				},
			}, nil
		},
	})
	if err := chunkExecErrStore.CreateBuild(ctx, validBuild, validChunks); err == nil {
		t.Fatal("expected chunk insert error")
	}

	commitErrStore := NewPostgresVanadisStore(&mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				commitErr:  errors.New("commit failed"),
				rollbackFn: func(context.Context) error { return errors.New("rollback failed") },
			}, nil
		},
	})
	if err := commitErrStore.CreateBuild(ctx, validBuild, validChunks); err == nil {
		t.Fatal("expected commit error")
	}

	// GetBuild tests
	if _, _, err := store.GetBuild(ctx, "   "); err == nil {
		t.Fatal("expected empty build id error")
	}

	notFoundStore := NewPostgresVanadisStore(&mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{scanFn: func(...any) error { return pgx.ErrNoRows }}
		},
	})
	if _, _, err := notFoundStore.GetBuild(ctx, "b-404"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}

	queryRowErrStore := NewPostgresVanadisStore(&mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{scanFn: func(...any) error { return errors.New("db error") }}
		},
	})
	if _, _, err := queryRowErrStore.GetBuild(ctx, "b-err"); err == nil {
		t.Fatal("expected queryRow error")
	}

	populateBuildRow := func(dest ...any) error {
		*(dest[0].(*string)) = "b-1"
		*(dest[1].(*string)) = "azylman/aerial"
		*(dest[2].(*string)) = "thread-1"
		*(dest[3].(*int)) = 2
		*(dest[4].(*string)) = "Build Title"
		*(dest[5].(*string)) = "Objective"
		*(dest[6].(*string)) = "Shared Context"
		*(dest[7].(*string)) = "running"
		*(dest[8].(*string)) = "msg-1"
		*(dest[9].(*time.Time)) = time.Now()
		*(dest[10].(*time.Time)) = time.Now()
		return nil
	}

	queryChunksErrStore := NewPostgresVanadisStore(&mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{scanFn: populateBuildRow}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return nil, errors.New("chunks query error")
		},
	})
	if _, _, err := queryChunksErrStore.GetBuild(ctx, "b-1"); err == nil {
		t.Fatal("expected chunks query error")
	}

	scanErrStore := NewPostgresVanadisStore(&mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{scanFn: populateBuildRow}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: validChunks, scanErr: errors.New("scan failed")}, nil
		},
	})
	if _, _, err := scanErrStore.GetBuild(ctx, "b-1"); err == nil {
		t.Fatal("expected scan error")
	}

	rowsErrStore := NewPostgresVanadisStore(&mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{scanFn: populateBuildRow}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: validChunks, rowsErr: errors.New("rows iteration error")}, nil
		},
	})
	if _, _, err := rowsErrStore.GetBuild(ctx, "b-1"); err == nil {
		t.Fatal("expected rows iteration error")
	}

	okGetStore := NewPostgresVanadisStore(&mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return &mockRow{scanFn: populateBuildRow}
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: []VanadisChunk{
				{
					ID:           "c-1",
					BuildID:      "b-1",
					Title:        "T1",
					Instructions: "I1",
					TargetFiles:  nil,
					Effort:       "",
					StartedAt:    explicitStart,
				},
				{
					ID:           "c-2",
					BuildID:      "b-1",
					Title:        "T2",
					Instructions: "I2",
				},
			}}, nil
		},
	})
	gotB, gotC, err := okGetStore.GetBuild(ctx, "b-1")
	if err != nil || gotB.ID != "b-1" || gotB.Title != "Build Title" || gotB.SharedContext != "Shared Context" || len(gotC) != 2 || gotC[0].TargetFiles == nil || gotC[0].TargetSymbols == nil || gotC[0].AcceptanceCriteria == nil || gotC[0].Effort != "low" || !gotC[0].StartedAt.Equal(explicitStart) || !gotC[1].StartedAt.IsZero() {
		t.Fatalf("unexpected GetBuild result: b=%+v c=%+v err=%v", gotB, gotC, err)
	}

	// UpdateChunkStatus & UpdateBuildStatus tests
	if err := store.UpdateChunkStatus(ctx, "", "running"); err == nil {
		t.Fatal("expected empty chunk id error")
	}
	if err := store.UpdateChunkStatus(ctx, "c-1", ""); err == nil {
		t.Fatal("expected empty status error")
	}
	if err := store.UpdateChunkStatus(ctx, "c-1", "running"); err != nil {
		t.Fatalf("unexpected UpdateChunkStatus error: %v", err)
	}

	if err := store.UpdateBuildStatus(ctx, "", "completed"); err == nil {
		t.Fatal("expected empty build id error")
	}
	if err := store.UpdateBuildStatus(ctx, "b-1", ""); err == nil {
		t.Fatal("expected empty status error")
	}
	if err := store.UpdateBuildStatus(ctx, "b-1", "completed"); err != nil {
		t.Fatalf("unexpected UpdateBuildStatus error: %v", err)
	}

	zeroRowsStore := NewPostgresVanadisStore(&mockDB{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		},
	})
	if err := zeroRowsStore.UpdateChunkStatus(ctx, "c-404", "running"); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("expected ErrChunkNotFound, got %v", err)
	}
	if err := zeroRowsStore.UpdateBuildStatus(ctx, "b-404", "completed"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}

	execErrStore := NewPostgresVanadisStore(&mockDB{
		execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, errors.New("exec failed")
		},
	})
	if err := execErrStore.UpdateChunkStatus(ctx, "c-1", "running"); err == nil {
		t.Fatal("expected exec error on UpdateChunkStatus")
	}
	if err := execErrStore.UpdateBuildStatus(ctx, "b-1", "completed"); err == nil {
		t.Fatal("expected exec error on UpdateBuildStatus")
	}

	// UpdateBuildStatusMessageID tests
	if err := store.UpdateBuildStatusMessageID(ctx, "", "msg-1"); err == nil {
		t.Fatal("expected empty build id error")
	}
	if err := store.UpdateBuildStatusMessageID(ctx, "b-1", "msg-1"); err != nil {
		t.Fatalf("unexpected UpdateBuildStatusMessageID error: %v", err)
	}
	if err := zeroRowsStore.UpdateBuildStatusMessageID(ctx, "b-404", "msg-1"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}
	if err := execErrStore.UpdateBuildStatusMessageID(ctx, "b-1", "msg-1"); err == nil {
		t.Fatal("expected exec error on UpdateBuildStatusMessageID")
	}
}

func TestFormatChunkPrompt(t *testing.T) {
	t.Parallel()
	build := VanadisBuild{
		ID:            "build-999",
		Repo:          "azylman/aerial",
		ThreadID:      "1555000111",
		Tier:          2,
		Title:         "Build Vanadis Queue Execution Pipeline",
		Objective:     "Build Vanadis Queue",
		SharedContext: "Architecture: River queue backed by PostgreSQL.",
	}
	chunks := []VanadisChunk{
		{
			ID:           "build-999-chunk-1",
			ChunkIndex:   0,
			Title:        "Task 1: Schema",
			Instructions: "Create SQL tables",
			TargetFiles:  []string{"brain/schema.sql"},
			Status:       "merged",
			PRNumber:     101,
			DiffStat:     "1 file changed, 25 insertions(+)",
		},
		{
			ID:                 "build-999-chunk-2",
			ChunkIndex:         1,
			Title:              "Task 2: Service",
			Instructions:       "Create vanadis service",
			TargetFiles:        []string{"vanadis/main.go", "vanadis/main_test.go"},
			TargetSymbols:      []string{"VanadisServer", "FormatChunkPrompt"},
			Effort:             "high",
			AcceptanceCriteria: []string{"Coverage >= 95.0%", "  ", "Table driven tests pass"},
			Status:             "running",
		},
		{
			ID:           "build-999-chunk-3",
			ChunkIndex:   2,
			Title:        "Task 3: Webhook Advancement",
			Instructions: "Advance chunks on merge",
			TargetFiles:  nil,
			Status:       "pending",
		},
	}

	if _, err := FormatChunkPrompt(build, nil, 0); err == nil {
		t.Fatal("expected error for empty chunks")
	}
	if _, err := FormatChunkPrompt(build, chunks, -1); err == nil {
		t.Fatal("expected error for negative activeIdx")
	}
	if _, err := FormatChunkPrompt(build, chunks, 3); err == nil {
		t.Fatal("expected error for out-of-range activeIdx")
	}

	p0, err := FormatChunkPrompt(build, chunks, 0)
	if err != nil {
		t.Fatalf("unexpected error for chunk 0: %v", err)
	}
	if !strings.Contains(p0, "(none — this is the first chunk)") || !strings.Contains(p0, "DO NOT implement these remaining chunks in this turn") {
		t.Fatalf("unexpected prompt for chunk 0:\n%s", p0)
	}
	if !strings.Contains(p0, "=== Shared Build Context ===\nArchitecture: River queue backed by PostgreSQL.") {
		t.Fatalf("expected Shared Build Context in prompt 0:\n%s", p0)
	}

	p1, err := FormatChunkPrompt(build, chunks, 1)
	if err != nil {
		t.Fatalf("unexpected error for chunk 1: %v", err)
	}
	for _, want := range []string{
		"[VANADIS BUILD CHUNK EXECUTION]",
		"Build ID: build-999",
		"Build Title: Build Vanadis Queue Execution Pipeline",
		"=== Shared Build Context ===\nArchitecture: River queue backed by PostgreSQL.",
		"[Chunk 1/3] Task 1: Schema (azylman/aerial, PR #101)",
		"DiffStat: 1 file changed, 25 insertions(+)",
		"=== Active Chunk (Chunk 2 of 3) ===",
		"Repository: azylman/aerial",
		"Target Files: vanadis/main.go, vanadis/main_test.go",
		"Target Symbols: VanadisServer, FormatChunkPrompt",
		"=== Acceptance Criteria ===\n- Coverage >= 95.0%\n- Table driven tests pass\n",
		"- [Chunk 3/3] Task 3: Webhook Advancement (azylman/aerial)",
		"Inspect `Target Files` and `Target Symbols` directly rather than running broad repo-wide searches.",
		"--vanadis-chunk build-999-chunk-2",
	} {
		if !strings.Contains(p1, want) {
			t.Fatalf("prompt for chunk 1 missing %q:\n%s", want, p1)
		}
	}

	// Verify build with empty shared context does not render header
	emptySharedBuild := build
	emptySharedBuild.SharedContext = "   "
	pEmpty, err := FormatChunkPrompt(emptySharedBuild, chunks, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(pEmpty, "=== Shared Build Context ===") {
		t.Fatalf("unexpected Shared Build Context header when empty:\n%s", pEmpty)
	}

	p2, err := FormatChunkPrompt(build, chunks, 2)
	if err != nil {
		t.Fatalf("unexpected error for chunk 2: %v", err)
	}
	if !strings.Contains(p2, "Target Files: (none specified)") || !strings.Contains(p2, "(none — this is the final chunk)") {
		t.Fatalf("unexpected prompt for chunk 2:\n%s", p2)
	}
}

func TestExecuteChunkWorkerAndProcessExecuteChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var emptyWorker ExecuteChunkWorker
	if err := emptyWorker.Work(ctx, &river.Job[ExecuteChunkArgs]{}); err == nil {
		t.Fatal("expected error when worker server is nil")
	}

	srvNoStore := NewVanadisServer(Config{}, nil, nil, nil)
	if err := srvNoStore.ProcessExecuteChunk(ctx, "b1", 0, false); err == nil {
		t.Fatal("expected error when store is nil")
	}

	var gotPrompts []BrainPromptRequest
	brainStatus := http.StatusAccepted
	brainSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prompt" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req BrainPromptRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		gotPrompts = append(gotPrompts, req)
		w.WriteHeader(brainStatus)
		_, _ = w.Write([]byte(`{"status":"queued"}`))
	}))
	defer brainSrv.Close()

	memStore := newMemoryVanadisStore()
	srv := NewVanadisServer(Config{BrainURL: brainSrv.URL}, memStore, &mockRiverInserter{}, brainSrv.Client())

	// 1. Build not found -> nil
	if err := srv.ProcessExecuteChunk(ctx, "nonexistent", 0, false); err != nil {
		t.Fatalf("expected nil on missing build, got %v", err)
	}

	// 2. GetBuild error -> returns error
	memStore.getErr = errors.New("db outage")
	if err := srv.ProcessExecuteChunk(ctx, "b1", 0, false); err == nil {
		t.Fatal("expected error when GetBuild fails")
	}
	memStore.getErr = nil

	// 3. Build not running -> nil
	_ = memStore.CreateBuild(ctx, VanadisBuild{ID: "b-cancelled", Status: "cancelled"}, []VanadisChunk{{ID: "c0", ChunkIndex: 0}})
	if err := srv.ProcessExecuteChunk(ctx, "b-cancelled", 0, false); err != nil {
		t.Fatalf("expected nil on cancelled build, got %v", err)
	}

	// 4. Active build tests
	_ = memStore.CreateBuild(ctx, VanadisBuild{
		ID:        "b-run",
		Repo:      "azylman/aerial",
		ThreadID:  "thread-777",
		Tier:      2,
		Objective: "Test execution",
		Status:    "running",
	}, []VanadisChunk{
		{ID: "c-0", BuildID: "b-run", ChunkIndex: 0, Title: "C0", Instructions: "I0", Status: "merged", PRNumber: 10},
		{ID: "c-1", BuildID: "b-run", ChunkIndex: 1, Title: "C1", Instructions: "I1", Effort: "high", Status: "pending"},
	})

	// Chunk index not found
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 99, false); err == nil {
		t.Fatal("expected error for missing chunk index 99")
	}

	// Chunk already merged -> idempotent nil
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 0, false); err != nil {
		t.Fatalf("expected nil on already merged chunk, got %v", err)
	}

	// UpdateChunkStatus error
	memStore.updateChunkErr = errors.New("update chunk failed")
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 1, false); err == nil {
		t.Fatal("expected error when UpdateChunkStatus fails")
	}
	memStore.updateChunkErr = nil

	// Successful execution via Worker with Effort propagation
	worker := &ExecuteChunkWorker{server: srv}
	job := &river.Job[ExecuteChunkArgs]{
		JobRow: &rivertype.JobRow{Attempt: 1, MaxAttempts: 3},
		Args:   ExecuteChunkArgs{BuildID: "b-run", ChunkIndex: 1},
	}
	if err := worker.Work(ctx, job); err != nil {
		t.Fatalf("unexpected worker error: %v", err)
	}
	if len(gotPrompts) != 1 || !gotPrompts[0].Silent || gotPrompts[0].Effort != "high" || gotPrompts[0].ChannelID != "thread-777" || gotPrompts[0].VanadisChunkID != "c-1" {
		t.Fatalf("unexpected dispatched prompts: %+v", gotPrompts)
	}

	// Non-final failure vs Final attempt failure
	brainStatus = http.StatusBadGateway
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 1, false); err == nil {
		t.Fatal("expected error on 502 from brain")
	}
	bState, _, _ := memStore.GetBuild(ctx, "b-run")
	if bState.Status != "running" {
		t.Fatalf("expected build to remain running on non-final attempt, got %s", bState.Status)
	}

	finalJob := &river.Job[ExecuteChunkArgs]{
		JobRow: &rivertype.JobRow{Attempt: 3, MaxAttempts: 3},
		Args:   ExecuteChunkArgs{BuildID: "b-run", ChunkIndex: 1},
	}
	if err := worker.Work(ctx, finalJob); err == nil {
		t.Fatal("expected error on final attempt 502 from brain")
	}
	bState, cState, _ := memStore.GetBuild(ctx, "b-run")
	if bState.Status != "failed" || cState[1].Status != "failed" {
		t.Fatalf("expected build and chunk marked failed on final attempt, got build=%s chunk=%s", bState.Status, cState[1].Status)
	}

	// Also cover final attempt when store updates fail during failure marking and network error on closed server
	_ = memStore.UpdateBuildStatus(ctx, "b-run", "running")
	_ = memStore.UpdateChunkStatus(ctx, "c-1", "pending")
	badSrv := NewVanadisServer(Config{BrainURL: "http://[::1]:namedport"}, memStore, nil, nil)
	_ = badSrv.ProcessExecuteChunk(ctx, "b-run", 1, true)

	closedSrv := httptest.NewServer(http.NotFoundHandler())
	closedURL := closedSrv.URL
	closedSrv.Close()
	netErrSrv := NewVanadisServer(Config{BrainURL: closedURL}, memStore, nil, nil)
	_ = memStore.UpdateBuildStatus(ctx, "b-run", "running")
	_ = memStore.UpdateChunkStatus(ctx, "c-1", "pending")
	memStore.updateBuildErr = errors.New("build fail update err")
	_ = netErrSrv.ProcessExecuteChunk(ctx, "b-run", 1, true)
	memStore.updateBuildErr = nil

	writeJSON(httptest.NewRecorder(), http.StatusOK, make(chan int))
}

func TestHTTPEndpoints(t *testing.T) {
	memStore := newMemoryVanadisStore()
	inserter := &mockRiverInserter{}
	srv := NewVanadisServer(Config{Port: "0", BrainURL: "http://127.0.0.1:8088"}, memStore, inserter, nil)
	mux := srv.Routes()

	doReq := func(method, path string, body io.Reader) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, body)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	// Health & Metrics
	if rec := doReq(http.MethodGet, "/health", nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /health, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/healthz", nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /healthz, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/metrics", nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /metrics, got %d", rec.Code)
	}

	// Coverage Flush
	t.Setenv("GOCOVERDIR", t.TempDir())
	origMeta, origCounters := writeMetaDirFn, writeCountersDirFn
	defer func() {
		writeMetaDirFn = origMeta
		writeCountersDirFn = origCounters
	}()
	writeMetaDirFn = func(string) error { return errors.New("meta err") }
	writeCountersDirFn = func(string) error { return nil }
	if rec := doReq(http.MethodPost, "/debug/coverage/flush", nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on coverage flush, got %d", rec.Code)
	}
	writeCountersDirFn = func(string) error { return errors.New("counters err") }
	if rec := doReq(http.MethodGet, "/debug/coverage/flush", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on coverage flush error, got %d", rec.Code)
	}

	// Nil store checks
	srv.SetStore(nil)
	if rec := doReq(http.MethodPost, "/builds", strings.NewReader(`{}`)); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when store nil on POST /builds, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/builds/b1", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when store nil on GET /builds/b1, got %d", rec.Code)
	}
	srv.SetStore(memStore)

	// POST /builds validation cases
	if rec := doReq(http.MethodPost, "/builds", errorReader{}); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on body read error, got %d", rec.Code)
	}
	badPayloads := []string{
		`{invalid`,
		`{"repo":""}`,
		`{"repo":"aerial","thread_id":""}`,
		`{"repo":"aerial","thread_id":"not-numeric","objective":"obj","chunks":[{"title":"t","instructions":"i"}]}`,
		`{"repo":"aerial","thread_id":"123","objective":""}`,
		`{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[]}`,
		`{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[{"title":"","instructions":"i"}]}`,
		`{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[{"title":"t","instructions":""}]}`,
		`{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[{"title":"t","instructions":"i","effort":"unsupported"}]}`,
	}
	for _, p := range badPayloads {
		if rec := doReq(http.MethodPost, "/builds", strings.NewReader(p)); rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for payload %s, got %d", p, rec.Code)
		}
	}

	validPayload := `{
		"repo": "aerial",
		"thread_id": "1555999",
		"title": "Implement Feature X Build Title",
		"objective": "Implement feature X",
		"shared_context": "Targeting Nomad cluster",
		"chunks": [
			{"title": "Part 1", "instructions": "Do part 1", "effort": "low"},
			{"id": "custom-chunk-2", "title": "Part 2", "instructions": "Do part 2", "target_files": ["a.go"], "target_symbols": ["A"], "effort": "medium", "acceptance_criteria": ["All green"]}
		]
	}`

	// Store error on CreateBuild
	memStore.createErr = errors.New("create failed")
	if rec := doReq(http.MethodPost, "/builds", strings.NewReader(validPayload)); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on store create error, got %d", rec.Code)
	}
	memStore.createErr = nil

	// River insert error (nil client and error client)
	srv.SetRiverClient(nil)
	if rec := doReq(http.MethodPost, "/builds", strings.NewReader(validPayload)); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when river client nil, got %d", rec.Code)
	}
	inserter.err = errors.New("river insert failed")
	srv.SetRiverClient(inserter)
	if rec := doReq(http.MethodPost, "/builds", strings.NewReader(validPayload)); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when river insert fails, got %d", rec.Code)
	}
	inserter.err = nil

	// Success POST /builds
	rec := doReq(http.MethodPost, "/builds", strings.NewReader(validPayload))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	buildID, _ := createResp["build_id"].(string)
	if buildID == "" || createResp["status"] != "accepted" || len(inserter.inserted) != 1 {
		t.Fatalf("unexpected create response: %+v", createResp)
	}
	storedBuild, storedChunks, _ := memStore.GetBuild(context.Background(), buildID)
	if storedBuild.Title != "Implement Feature X Build Title" || storedBuild.SharedContext != "Targeting Nomad cluster" || len(storedChunks) != 2 || storedChunks[1].Effort != "medium" || len(storedChunks[1].TargetSymbols) != 1 || len(storedChunks[1].AcceptanceCriteria) != 1 {
		t.Fatalf("stored build mismatch: build=%+v chunks=%+v", storedBuild, storedChunks)
	}

	// GET /builds/{id} and POST /builds/{id}/cancel
	if rec := doReq(http.MethodGet, "/builds/", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on empty build id, got %d", rec.Code)
	}
	if rec := doReq(http.MethodDelete, "/builds/"+buildID, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on DELETE, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/builds/unknown-id", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on unknown build id, got %d", rec.Code)
	}
	memStore.getErr = errors.New("get error")
	if rec := doReq(http.MethodGet, "/builds/"+buildID, nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on get error, got %d", rec.Code)
	}
	memStore.getErr = nil

	if rec := doReq(http.MethodGet, "/builds/"+buildID, nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET /builds/%s, got %d", buildID, rec.Code)
	}

	// Cancel routes
	if rec := doReq(http.MethodGet, "/builds/"+buildID+"/cancel", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /cancel, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/builds/%20/cancel", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on blank build id cancel, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/builds/unknown-id/cancel", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on unknown build cancel, got %d", rec.Code)
	}
	memStore.updateBuildErr = errors.New("cancel db err")
	if rec := doReq(http.MethodPost, "/builds/"+buildID+"/cancel", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on cancel db err, got %d", rec.Code)
	}
	memStore.updateBuildErr = nil

	if rec := doReq(http.MethodPost, "/builds/"+buildID+"/cancel", bytes.NewReader(nil)); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on POST /cancel, got %d", rec.Code)
	}
	_ = memStore.UpdateBuildStatus(context.Background(), buildID, "completed")
	if rec := doReq(http.MethodPost, "/builds/"+buildID+"/cancel", bytes.NewReader(nil)); rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict cancelling completed build, got %d", rec.Code)
	}
}

func TestInitPostgresAndRiverAndRunServer(t *testing.T) {
	origPoolFn := newPgxPoolFn
	origRiverFn := newRiverClientFn
	defer func() {
		newPgxPoolFn = origPoolFn
		newRiverClientFn = origRiverFn
	}()

	ctx := context.Background()
	srv := NewVanadisServer(Config{}, nil, nil, nil)

	// 1. ParseConfig error
	if _, err := initPostgresAndRiver(ctx, srv, "postgres://user:pass@invalid%xx/db"); err == nil {
		t.Fatal("expected parse error")
	}

	// 2. newPgxPoolFn error
	newPgxPoolFn = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, errors.New("pool creation failed")
	}
	if _, err := initPostgresAndRiver(ctx, srv, "postgres://u:p@127.0.0.1:5432/db"); err == nil {
		t.Fatal("expected pool error")
	}

	// Create a real lazy pool object (does not connect until used)
	cfg, _ := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db")
	newPgxPoolFn = origPoolFn

	// 3. newRiverClientFn error
	newRiverClientFn = func(*pgxpool.Pool, *river.Workers) (riverLifecycleClient, error) {
		return nil, errors.New("river client error")
	}
	if _, err := initPostgresAndRiver(ctx, srv, cfg.ConnString()); err == nil {
		t.Fatal("expected river client error")
	}

	// 4. rc.Start error
	newRiverClientFn = func(*pgxpool.Pool, *river.Workers) (riverLifecycleClient, error) {
		return &mockRiverLifecycle{startErr: errors.New("start failed")}, nil
	}
	if _, err := initPostgresAndRiver(ctx, srv, cfg.ConnString()); err == nil {
		t.Fatal("expected river start error")
	}

	// 5. Success and cleanup
	newRiverClientFn = func(*pgxpool.Pool, *river.Workers) (riverLifecycleClient, error) {
		return &mockRiverLifecycle{stopErr: errors.New("stop warning")}, nil
	}
	cleanup, err := initPostgresAndRiver(ctx, srv, cfg.ConnString())
	if err != nil || cleanup == nil {
		t.Fatalf("expected success from initPostgresAndRiver, got err=%v", err)
	}
	cleanup(ctx)

	// 6. runServer with mocked River + Postgres lifecycle and invalid port
	if err := runServer(ctx, Config{Port: "-1", PostgresURL: cfg.ConnString()}, nil); err == nil {
		t.Fatal("expected listen error on port -1")
	}
	if err := runServer(ctx, Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"}, nil); err == nil {
		t.Fatal("expected error on invalid PostgresURL in runServer")
	}

	// 7. Full runServer + RunApp lifecycle
	runCtx, runCancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- runServer(runCtx, Config{Port: "0", PostgresURL: cfg.ConnString()}, func(addr string) {
			ready <- addr
		})
	}()

	select {
	case addr := <-ready:
		if addr == "" {
			t.Fatal("expected non-empty listen addr")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server ready")
	}
	runCancel()
	if err := <-done; err != nil {
		t.Fatalf("unexpected runServer exit error: %v", err)
	}

	appCtx, appCancel := context.WithCancel(context.Background())
	appCancel()
	if err := RunApp(appCtx, Config{Port: "0"}); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("unexpected RunApp error: %v", err)
	}
}

func TestSyncBuildWidget(t *testing.T) {
	ctx := context.Background()

	// 1. Nil server or client
	var nilSrv *VanadisServer
	nilSrv.syncBuildWidget(ctx, VanadisBuild{}, nil, "")

	store := newMemoryVanadisStore()
	store.builds["b1"] = VanadisBuild{ID: "b1", ThreadID: "1555405874565091380"}

	// 2. Successful widget post and persistence of new message_id
	var postedReq StatusWidgetRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/discord/widget" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&postedReq)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":     "delivered",
			"message_id": "msg-new-widget-42",
		})
	}))
	defer ts.Close()

	srv := &VanadisServer{
		cfg:        Config{BrainURL: ts.URL},
		httpClient: ts.Client(),
		store:      store,
	}

	build := VanadisBuild{
		ID:        "b1",
		ThreadID:  "1555405874565091380",
		Title:     "Test Build Title",
		Objective: "Test Obj",
		Status:    "running",
		Tier:      2,
	}
	startedAt := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	updatedAt := startedAt.Add(3 * time.Minute)
	chunks := []VanadisChunk{
		{ChunkIndex: 0, Title: "Chunk 0", Status: "running", Repo: "aerial", StartedAt: startedAt, UpdatedAt: updatedAt},
	}

	srv.syncBuildWidget(ctx, build, chunks, "test banner")
	if postedReq.ID != "b1" || postedReq.Title != "Test Build Title" || postedReq.Banner != "test banner" || len(postedReq.Chunks) != 1 || !postedReq.Chunks[0].StartedAt.Equal(startedAt) || !postedReq.Chunks[0].UpdatedAt.Equal(updatedAt) {
		t.Errorf("Unexpected posted widget payload: %+v", postedReq)
	}
	if store.builds["b1"].StatusMessageID != "msg-new-widget-42" {
		t.Errorf("Expected store status_message_id msg-new-widget-42, got %q", store.builds["b1"].StatusMessageID)
	}

	// 3. Error decoding response
	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer tsBadJSON.Close()

	srvBadJSON := &VanadisServer{
		cfg:        Config{BrainURL: tsBadJSON.URL},
		httpClient: tsBadJSON.Client(),
		store:      store,
	}
	srvBadJSON.syncBuildWidget(ctx, build, chunks, "")

	// 4. Persistence failure in store
	store.updateBuildErr = errors.New("simulated store failure")
	srv.syncBuildWidget(ctx, build, chunks, "")
}
