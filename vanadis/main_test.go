package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

type mockRow struct {
	b   *VanadisBuild
	err error
}

func fillBuildDest(b VanadisBuild, dest []any) {
	*dest[0].(*string), *dest[1].(*string), *dest[2].(*int), *dest[3].(*string), *dest[4].(*string), *dest[5].(*string), *dest[6].(*string), *dest[7].(*string), *dest[8].(*time.Time), *dest[9].(*time.Time) = b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.CreatedAt, b.UpdatedAt
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.b != nil && len(dest) == 10 {
		fillBuildDest(*r.b, dest)
	}
	return nil
}

type mockRows struct {
	pgx.Rows
	items            []VanadisChunk
	builds           []VanadisBuild
	idx              int
	scanErr, rowsErr error
}

func (r *mockRows) Close()     {}
func (r *mockRows) Err() error { return r.rowsErr }
func (r *mockRows) Next() bool {
	if len(r.builds) > 0 {
		r.idx++
		return r.idx <= len(r.builds)
	}
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
	if len(r.builds) > 0 || len(dest) == 10 {
		fillBuildDest(r.builds[r.idx-1], dest)
		return nil
	}
	c := r.items[r.idx-1]
	*dest[0].(*string), *dest[1].(*string), *dest[2].(*int), *dest[3].(*string), *dest[4].(*string), *dest[5].(*string), *dest[6].(*[]string), *dest[7].(*[]string), *dest[8].(*string), *dest[9].(*[]string), *dest[10].(*string), *dest[11].(*int), *dest[12].(*sql.NullTime), *dest[13].(*time.Time), *dest[14].(*time.Time) = c.ID, c.BuildID, c.ChunkIndex, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, c.CreatedAt, c.UpdatedAt
	return nil
}

type mockTx struct {
	pgx.Tx
	execFn                 func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	commitErr, rollbackErr error
}

func (t *mockTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}
func (t *mockTx) Commit(_ context.Context) error   { return t.commitErr }
func (t *mockTx) Rollback(_ context.Context) error { return t.rollbackErr }

type mockDB struct {
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	beginFn    func(ctx context.Context) (pgx.Tx, error)
}

func (db *mockDB) Exec(ctx context.Context, s string, a ...any) (pgconn.CommandTag, error) {
	if db.execFn != nil {
		return db.execFn(ctx, s, a...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (db *mockDB) Query(ctx context.Context, s string, a ...any) (pgx.Rows, error) {
	if db.queryFn != nil {
		return db.queryFn(ctx, s, a...)
	}
	return &mockRows{}, nil
}

func (db *mockDB) QueryRow(ctx context.Context, s string, a ...any) pgx.Row {
	if db.queryRowFn != nil {
		return db.queryRowFn(ctx, s, a...)
	}
	return &mockRow{}
}

func (db *mockDB) Begin(ctx context.Context) (pgx.Tx, error) {
	if db.beginFn != nil {
		return db.beginFn(ctx)
	}
	return &mockTx{}, nil
}

type memoryVanadisStore struct {
	builds                                                     map[string]VanadisBuild
	chunks                                                     map[string][]VanadisChunk
	createErr, getErr, listErr, updateChunkErr, updateBuildErr error
}

func newMemoryVanadisStore() *memoryVanadisStore {
	return &memoryVanadisStore{builds: make(map[string]VanadisBuild), chunks: make(map[string][]VanadisChunk)}
}

func (m *memoryVanadisStore) CreateBuild(_ context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.builds[build.ID], m.chunks[build.ID] = build, chunks
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
	return &b, m.chunks[buildID], nil
}

func (m *memoryVanadisStore) ListBuilds(_ context.Context, limit int) ([]VanadisBuildEntry, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	bList := make([]VanadisBuild, 0, len(m.builds))
	for _, b := range m.builds {
		bList = append(bList, b)
	}
	sort.Slice(bList, func(i, j int) bool {
		return bList[i].CreatedAt.After(bList[j].CreatedAt) || (bList[i].CreatedAt.Equal(bList[j].CreatedAt) && bList[i].ID > bList[j].ID)
	})
	if len(bList) > limit {
		bList = bList[:limit]
	}
	entries := make([]VanadisBuildEntry, len(bList))
	for i, b := range bList {
		entries[i] = SummarizeBuildEntry(b, m.chunks[b.ID])
	}
	return entries, nil
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

func (m *memoryVanadisStore) setBuild(id string, fn func(*VanadisBuild)) error {
	if m.updateBuildErr != nil {
		return m.updateBuildErr
	}
	b, ok := m.builds[id]
	if !ok {
		return ErrBuildNotFound
	}
	fn(&b)
	m.builds[id] = b
	return nil
}

func (m *memoryVanadisStore) UpdateBuildStatus(_ context.Context, id, status string) error {
	return m.setBuild(id, func(b *VanadisBuild) { b.Status = status })
}

func (m *memoryVanadisStore) UpdateBuildStatusMessageID(_ context.Context, id, msgID string) error {
	return m.setBuild(id, func(b *VanadisBuild) { b.StatusMessageID = msgID })
}

type mockRiverInserter struct {
	inserted []ExecuteChunkArgs
	err      error
}

func (m *mockRiverInserter) Insert(_ context.Context, args river.JobArgs, _ *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	if a, ok := args.(ExecuteChunkArgs); ok {
		m.inserted = append(m.inserted, a)
	}
	return &rivertype.JobInsertResult{}, nil
}

type mockRiverLifecycle struct {
	mockRiverInserter
	startErr, stopErr error
}

func (m *mockRiverLifecycle) Start(_ context.Context) error { return m.startErr }
func (m *mockRiverLifecycle) Stop(_ context.Context) error  { return m.stopErr }

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func newMockStore(db mockDB) *PostgresVanadisStore {
	return NewPostgresVanadisStore(&db)
}

func mkBuild(id, threadID, title, objective, status string) VanadisBuild {
	return VanadisBuild{ID: id, ThreadID: threadID, Title: title, Objective: objective, Status: status, Tier: 2}
}

func mkChunk(id, buildID string, idx int, title, instr, effort, status string) VanadisChunk {
	if effort == "" {
		effort = "medium"
	}
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}}
}

func TestLoadConfigAndEnv(t *testing.T) {
	origFallback, origLookup := vanadisFallbackConfigPath, lookupHostFn
	defer func() {
		vanadisFallbackConfigPath, lookupHostFn = origFallback, origLookup
	}()
	for _, k := range []string{"PORT", "POSTGRES_URL", "DATABASE_URL", "BRAIN_INTERNAL_URL", "BRAIN_URL", "CONFIG_PATH"} {
		t.Setenv(k, "")
	}
	tmpDir := t.TempDir()
	vanadisFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
	for _, tc := range []struct {
		name, lookupHost, wantHost string
	}{
		{"DNSFallback_AerialPostgres", "aerial-postgres", "@aerial-postgres:5432/"},
		{"DNSFallback_Postgres", "postgres", "@postgres:5432/"},
		{"DNSFallback_Localhost", "other", "@127.0.0.1:5432/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookupHostFn = func(_ context.Context, host string) ([]string, error) {
				if host == tc.lookupHost {
					return []string{"10.0.0.2"}, nil
				}
				return nil, errors.New("not found")
			}
			cfg := LoadConfig("")
			if !strings.Contains(cfg.PostgresURL, tc.wantHost) {
				t.Fatalf("expected postgres URL to contain %s, got %s", tc.wantHost, cfg.PostgresURL)
			}
			if tc.name == "DNSFallback_AerialPostgres" && (cfg.Port != "4025" || cfg.BrainURL != "http://127.0.0.1:8088") {
				t.Fatalf("unexpected defaults: %+v", cfg)
			}
		})
	}
	yamlPath := filepath.Join(tmpDir, "vanadis.yaml")
	_ = os.WriteFile(yamlPath, []byte("port: \"4099\"\npostgres_url: \"postgres://u:p@db:5432/vanadis\"\nbrain_url: \"http://brain.aerial:8088/\"\n"), 0o600)
	vanadisFallbackConfigPath = yamlPath
	if cfg := LoadConfig(filepath.Join(tmpDir, "missing.yaml")); cfg.Port != "4099" || cfg.PostgresURL != "postgres://u:p@db:5432/vanadis" || cfg.BrainURL != "http://brain.aerial:8088" {
		t.Fatalf("unexpected yaml config: %+v", cfg)
	}
	badPath := filepath.Join(tmpDir, "bad.yaml")
	_ = os.WriteFile(badPath, []byte(":::invalid_yaml"), 0o600)
	t.Setenv("CONFIG_PATH", badPath)
	t.Setenv("PORT", "4030")
	t.Setenv("POSTGRES_URL", "postgres://env:5432/db")
	t.Setenv("BRAIN_INTERNAL_URL", "http://internal-brain:8088/")
	if envCfg := LoadConfigFromEnv(); envCfg.Port != "4030" || envCfg.PostgresURL != "postgres://env:5432/db" || envCfg.BrainURL != "http://internal-brain:8088" {
		t.Fatalf("unexpected env override config: %+v", envCfg)
	}
}

func TestExecuteChunkArgsAndNormalizeRepo(t *testing.T) {
	t.Parallel()
	if args := (ExecuteChunkArgs{BuildID: "b1", ChunkIndex: 2}); args.Kind() != "vanadis_execute_chunk" || args.InsertOpts().Queue != VanadisQueueName || args.InsertOpts().MaxAttempts != 3 || !args.InsertOpts().UniqueOpts.ByArgs {
		t.Fatalf("unexpected ExecuteChunkArgs metadata: %+v", args)
	}
	repoTests := []struct {
		name, input, want string
	}{
		{"empty repo", "", ""},
		{"whitespace repo", "   ", ""},
		{"already qualified", "azylman/aerial", "azylman/aerial"},
		{"unqualified repo", "aerial", "azylman/aerial"},
		{"unqualified with spaces", "  mirrormere  ", "azylman/mirrormere"},
	}
	for _, tc := range repoTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeRepo(tc.input); got != tc.want {
				t.Fatalf("normalizeRepo(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var nilStore *PostgresVanadisStore
	_, _, getErr := nilStore.GetBuild(ctx, "b1")
	_, listErr := nilStore.ListBuilds(ctx, 10)
	_, emptyDbListErr := (&PostgresVanadisStore{}).ListBuilds(ctx, 10)
	if nilStore.CreateBuild(ctx, VanadisBuild{}, nil) == nil || nilStore.UpdateChunkStatus(ctx, "c1", "running") == nil || nilStore.UpdateBuildStatus(ctx, "b1", "completed") == nil || getErr == nil || listErr == nil || emptyDbListErr == nil {
		t.Fatal("expected error on nil store operations")
	}
	validBuild := mkBuild("b-1", "12345", "", "Ship Vanadis", "")
	validBuild.SharedContext = "Shared ctx test"
	cValid := mkChunk("c-1", "", 0, "T1", "I1", "medium", "")
	cValid.TargetFiles, cValid.TargetSymbols, cValid.AcceptanceCriteria, cValid.PRNumber = []string{"main.go"}, []string{"Foo"}, []string{"Must pass"}, 42
	validChunks := []VanadisChunk{cValid}
	store := newMockStore(mockDB{})
	for _, tc := range []struct {
		name   string
		build  VanadisBuild
		chunks []VanadisChunk
	}{
		{"empty build id", mkBuild("", "1", "", "o", ""), nil},
		{"empty thread", mkBuild("b", "", "", "o", ""), nil},
		{"empty objective", mkBuild("b", "1", "", "", ""), nil},
		{"empty chunk id", validBuild, []VanadisChunk{mkChunk("", "", 0, "t", "i", "", "")}},
		{"mismatched chunk build_id", validBuild, []VanadisChunk{mkChunk("c", "other", 0, "t", "i", "", "")}},
		{"empty chunk title", validBuild, []VanadisChunk{mkChunk("c", "", 0, "", "i", "", "")}},
		{"empty chunk instructions", validBuild, []VanadisChunk{mkChunk("c", "", 0, "t", "", "", "")}},
		{"invalid chunk effort", validBuild, []VanadisChunk{mkChunk("c", "", 0, "t", "i", "ultra", "")}},
	} {
		if err := store.CreateBuild(ctx, tc.build, tc.chunks); err == nil {
			t.Fatalf("expected error for case %s", tc.name)
		}
	}
	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	cStart := mkChunk("c-2", "", 1, "T2", "I2", "", "")
	cStart.StartedAt = explicitStart
	createChunks := []VanadisChunk{validChunks[0], cStart, mkChunk("c-3", "", 2, "T3", "I3", "", "running")}
	if err := store.CreateBuild(ctx, validBuild, createChunks); err != nil {
		t.Fatalf("CreateBuild failed: %v", err)
	}
	callCount := 0
	errExecTx := &mockTx{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, errors.New("err")
	}}
	for _, begin := range []func(context.Context) (pgx.Tx, error){
		func(context.Context) (pgx.Tx, error) { return nil, errors.New("err") },
		func(context.Context) (pgx.Tx, error) { return errExecTx, nil },
		func(context.Context) (pgx.Tx, error) {
			return &mockTx{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
				callCount++
				if callCount == 2 {
					return pgconn.CommandTag{}, errors.New("err")
				}
				return pgconn.NewCommandTag("INSERT 0 1"), nil
			}}, nil
		},
		func(context.Context) (pgx.Tx, error) {
			return &mockTx{commitErr: errors.New("err"), rollbackErr: errors.New("rb")}, nil
		},
	} {
		if err := newMockStore(mockDB{beginFn: begin}).CreateBuild(ctx, validBuild, validChunks); err == nil {
			t.Fatal("expected CreateBuild error")
		}
	}
	if _, _, err := store.GetBuild(ctx, "   "); err == nil {
		t.Fatal("expected empty build id error")
	}
	if _, _, err := newMockStore(mockDB{queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{err: pgx.ErrNoRows} }}).GetBuild(ctx, "b-404"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}
	if _, _, err := newMockStore(mockDB{queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{err: errors.New("err")} }}).GetBuild(ctx, "b-err"); err == nil {
		t.Fatal("expected queryRow error")
	}
	popBuild := &VanadisBuild{ID: "b-1", ThreadID: "thread-1", Tier: 2, Title: "Build Title", Objective: "Objective", SharedContext: "Shared Context", Status: "running", StatusMessageID: "msg-1", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	for _, r := range []*mockRows{nil, {items: validChunks, scanErr: errors.New("err")}, {items: validChunks, rowsErr: errors.New("err")}} {
		if _, _, err := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{b: popBuild} },
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				if r == nil {
					return nil, errors.New("err")
				}
				return r, nil
			},
		}).GetBuild(ctx, "b-1"); err == nil {
			t.Fatal("expected GetBuild error")
		}
	}
	cGet0 := mkChunk("c-1", "b-1", 0, "T1", "I1", "", "")
	cGet0.StartedAt = explicitStart
	okGetStore := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{b: popBuild} },
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: []VanadisChunk{cGet0, mkChunk("c-2", "b-1", 1, "T2", "I2", "", "")}}, nil
		},
	})
	gotB, gotC, err := okGetStore.GetBuild(ctx, "b-1")
	if err != nil || gotB.ID != "b-1" || gotB.Title != "Build Title" || len(gotC) != 2 || !gotC[0].StartedAt.Equal(explicitStart) || !gotC[1].StartedAt.IsZero() {
		t.Fatalf("unexpected GetBuild result: b=%+v c=%+v err=%v", gotB, gotC, err)
	}
	for _, err := range []error{store.UpdateChunkStatus(ctx, "", "running"), store.UpdateChunkStatus(ctx, "c-1", ""), store.UpdateBuildStatus(ctx, "", "completed"), store.UpdateBuildStatus(ctx, "b-1", ""), store.UpdateBuildStatusMessageID(ctx, "", "msg-1")} {
		if err == nil {
			t.Fatal("expected validation error")
		}
	}
	for _, err := range []error{store.UpdateChunkStatus(ctx, "c-1", "running"), store.UpdateBuildStatus(ctx, "b-1", "completed"), store.UpdateBuildStatusMessageID(ctx, "b-1", "msg-1")} {
		if err != nil {
			t.Fatal(err)
		}
	}
	zeroStore := newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}})
	if !errors.Is(zeroStore.UpdateChunkStatus(ctx, "c-404", "running"), ErrChunkNotFound) || !errors.Is(zeroStore.UpdateBuildStatus(ctx, "b-404", "completed"), ErrBuildNotFound) || !errors.Is(zeroStore.UpdateBuildStatusMessageID(ctx, "b-404", "msg-1"), ErrBuildNotFound) {
		t.Fatal("expected not found errors")
	}
	errExecStore := newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, errors.New("err")
	}})
	if errExecStore.UpdateChunkStatus(ctx, "c-1", "running") == nil || errExecStore.UpdateBuildStatus(ctx, "b-1", "completed") == nil || errExecStore.UpdateBuildStatusMessageID(ctx, "b-1", "msg-1") == nil {
		t.Fatal("expected exec errors")
	}
	failQueries := []struct {
		buildRows *mockRows
		buildErr  error
		chunkRows *mockRows
		chunkErr  error
	}{
		{nil, errors.New("err"), nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "")}, scanErr: errors.New("err")}, nil, nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "")}, rowsErr: errors.New("err")}, nil, nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, nil, errors.New("err")},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, &mockRows{items: []VanadisChunk{mkChunk("c-1", "b-1", 0, "", "", "", "")}, scanErr: errors.New("err")}, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, &mockRows{items: []VanadisChunk{mkChunk("c-1", "b-1", 0, "", "", "", "")}, rowsErr: errors.New("err")}, nil},
	}
	for _, tc := range failQueries {
		s := newMockStore(mockDB{queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "vanadis_builds") {
				return tc.buildRows, tc.buildErr
			}
			return tc.chunkRows, tc.chunkErr
		}})
		if _, err := s.ListBuilds(ctx, 10); err == nil {
			t.Fatal("expected ListBuilds error")
		}
	}
	entries, err := newMockStore(mockDB{queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{}, nil }}).ListBuilds(ctx, 0)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected 0 entries on 0 builds, got %d, err=%v", len(entries), err)
	}
	now := time.Now().UTC()
	b1, b2 := mkBuild("b-1", "100", "", "", "running"), mkBuild("b-2", "101", "", "", "completed")
	b1.CreatedAt, b1.UpdatedAt, b2.CreatedAt, b2.UpdatedAt = now.Add(-time.Hour), now, now.Add(-2*time.Hour), now
	c1, c2, c3 := mkChunk("c-1", "b-1", 0, "chunk 1", "", "", "running"), mkChunk("c-2", "b-1", 1, "chunk 2", "", "", "pending"), mkChunk("c-3", "b-2", 0, "chunk 3", "", "", "merged")
	c1.CreatedAt, c1.UpdatedAt, c2.CreatedAt, c2.UpdatedAt, c3.CreatedAt, c3.UpdatedAt = now, now, now, now, now, now
	var capturedLimit any
	okStore := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "vanadis_builds") {
				if len(args) > 0 {
					capturedLimit = args[0]
				}
				return &mockRows{builds: []VanadisBuild{b1, b2}}, nil
			}
			return &mockRows{items: []VanadisChunk{c1, c2, c3}}, nil
		},
	})
	gotEntries, err := okStore.ListBuilds(ctx, 25)
	if err != nil || capturedLimit != 25 || len(gotEntries) != 2 || gotEntries[0].ID != "b-1" || gotEntries[0].TotalChunks != 2 || gotEntries[0].QueueState != "running" || gotEntries[1].ID != "b-2" || gotEntries[1].QueueState != "completed" {
		t.Fatalf("unexpected ListBuilds result: entries=%+v limit=%v err=%v", gotEntries, capturedLimit, err)
	}
}

func TestFormatChunkPrompt(t *testing.T) {
	t.Parallel()
	build := mkBuild("build-999", "1555000111", "Build Vanadis Queue Execution Pipeline", "Build Vanadis Queue", "")
	build.SharedContext = "Architecture: River queue backed by PostgreSQL."
	c1 := mkChunk("build-999-chunk-1", "", 0, "Task 1: Schema", "Create SQL tables", "", "merged")
	c1.TargetFiles, c1.PRNumber = []string{"brain/schema.sql"}, 101
	c2 := mkChunk("build-999-chunk-2", "", 1, "Task 2: Service", "Create vanadis service", "high", "running")
	c2.TargetFiles, c2.TargetSymbols, c2.AcceptanceCriteria = []string{"vanadis/main.go", "vanadis/main_test.go"}, []string{"VanadisServer", "FormatChunkPrompt"}, []string{"Coverage >= 95.0%", "  ", "Table driven tests pass"}
	c3 := mkChunk("build-999-chunk-3", "", 2, "Task 3: Webhook Advancement", "Advance chunks on merge", "", "pending")
	chunks := []VanadisChunk{c1, c2, c3}
	for _, tc := range []struct {
		b VanadisBuild
		c []VanadisChunk
		i int
	}{
		{build, nil, 0}, {build, chunks, -1}, {build, chunks, 3},
	} {
		if _, err := FormatChunkPrompt(tc.b, tc.c, tc.i); err == nil {
			t.Fatal("expected error")
		}
	}
	p0, err := FormatChunkPrompt(build, chunks, 0)
	if err != nil || !strings.Contains(p0, "(none — this is the first chunk)") || !strings.Contains(p0, "DO NOT implement these remaining chunks in this turn") || !strings.Contains(p0, "=== Shared Build Context ===\nArchitecture: River queue backed by PostgreSQL.") {
		t.Fatalf("unexpected prompt for chunk 0:\n%s (err=%v)", p0, err)
	}
	p1, err := FormatChunkPrompt(build, chunks, 1)
	if err != nil {
		t.Fatalf("unexpected error for chunk 1: %v", err)
	}
	for _, want := range []string{
		"[VANADIS BUILD CHUNK EXECUTION]", "Build ID: build-999", "Build Title: Build Vanadis Queue Execution Pipeline",
		"=== Build Objective ===\nBuild Vanadis Queue", "=== Shared Build Context ===\nArchitecture: River queue backed by PostgreSQL.",
		"[Chunk 1/3] Task 1: Schema (azylman/aerial, PR #101)", "=== Active Chunk (Chunk 2 of 3) ===", "Repository: azylman/aerial",
		"Target Files: vanadis/main.go, vanadis/main_test.go", "Target Symbols: VanadisServer, FormatChunkPrompt",
		"- [Chunk 3/3] Task 3: Webhook Advancement (azylman/aerial)", "optimizing all design and implementation choices for the `Build Objective`",
		"Run after init: `grep -n -E \"VanadisServer|FormatChunkPrompt\" \"$SCRATCH_DIR/vanadis/main.go\" \"$SCRATCH_DIR/vanadis/main_test.go\"`",
		"5. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk build-999-chunk-2`",
	} {
		if !strings.Contains(p1, want) {
			t.Fatalf("prompt for chunk 1 missing %q:\n%s", want, p1)
		}
	}
	if strings.Contains(p1, "=== Acceptance Criteria ===") || strings.Contains(p0, "Run after init:") {
		t.Fatalf("unexpected prompt headers: p1=%s p0=%s", p1, p0)
	}
	if got := formatSymbolGrepCommand([]string{"  ", "./pkg/foo.go"}, []string{" ", "Bar"}); got != `grep -n -E "Bar" "$SCRATCH_DIR/pkg/foo.go"` {
		t.Fatalf("unexpected formatSymbolGrepCommand output: %s", got)
	}
	if got := formatSymbolGrepCommand([]string{"   "}, []string{"Bar"}); got != "" {
		t.Fatalf("expected empty command for blank files, got %q", got)
	}
	emptySharedBuild := build
	emptySharedBuild.SharedContext = "   "
	pEmpty, err := FormatChunkPrompt(emptySharedBuild, chunks, 1)
	if err != nil || strings.Contains(pEmpty, "=== Shared Build Context ===") {
		t.Fatalf("unexpected Shared Build Context header when empty: err=%v prompt=%s", err, pEmpty)
	}
	p2, err := FormatChunkPrompt(build, chunks, 2)
	if err != nil || !strings.Contains(p2, "Target Files: (none specified)") || !strings.Contains(p2, "(none — this is the final chunk)") {
		t.Fatalf("unexpected prompt for chunk 2:\n%s (err=%v)", p2, err)
	}
}

func TestExecuteChunkWorkerAndProcessExecuteChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if (&ExecuteChunkWorker{}).Work(ctx, &river.Job[ExecuteChunkArgs]{}) == nil {
		t.Fatal("expected error when worker server is nil")
	}
	if NewVanadisServer(Config{}, nil, nil, nil).ProcessExecuteChunk(ctx, "b1", 0, false) == nil {
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
	if err := srv.ProcessExecuteChunk(ctx, "nonexistent", 0, false); err != nil {
		t.Fatalf("expected nil on missing build, got %v", err)
	}
	memStore.getErr = errors.New("db outage")
	if err := srv.ProcessExecuteChunk(ctx, "b1", 0, false); err == nil {
		t.Fatal("expected error when GetBuild fails")
	}
	memStore.getErr = nil
	_ = memStore.CreateBuild(ctx, mkBuild("b-cancelled", "", "", "", "cancelled"), []VanadisChunk{mkChunk("c0", "", 0, "", "", "", "")})
	if err := srv.ProcessExecuteChunk(ctx, "b-cancelled", 0, false); err != nil {
		t.Fatalf("expected nil on cancelled build, got %v", err)
	}
	c0 := mkChunk("c-0", "b-run", 0, "C0", "I0", "", "merged")
	c0.PRNumber = 10
	_ = memStore.CreateBuild(ctx, mkBuild("b-run", "thread-777", "", "", "running"), []VanadisChunk{c0, mkChunk("c-1", "b-run", 1, "C1", "I1", "high", "pending")})
	if srv.ProcessExecuteChunk(ctx, "b-run", 99, false) == nil || srv.ProcessExecuteChunk(ctx, "b-run", 0, false) != nil {
		t.Fatal("unexpected chunk status handling")
	}
	memStore.updateChunkErr = errors.New("update chunk failed")
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 1, false); err == nil {
		t.Fatal("expected error when UpdateChunkStatus fails")
	}
	memStore.updateChunkErr = nil
	worker := &ExecuteChunkWorker{server: srv}
	job := &river.Job[ExecuteChunkArgs]{JobRow: &rivertype.JobRow{Attempt: 1, MaxAttempts: 3}, Args: ExecuteChunkArgs{BuildID: "b-run", ChunkIndex: 1}}
	if err := worker.Work(ctx, job); err != nil {
		t.Fatalf("unexpected worker error: %v", err)
	}
	if len(gotPrompts) != 1 || !gotPrompts[0].Silent || gotPrompts[0].Effort != "high" || gotPrompts[0].ChannelID != "thread-777" || gotPrompts[0].VanadisChunkID != "c-1" {
		t.Fatalf("unexpected dispatched prompts: %+v", gotPrompts)
	}
	brainStatus = http.StatusBadGateway
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 1, false); err == nil {
		t.Fatal("expected error on 502 from brain")
	}
	if bState, _, _ := memStore.GetBuild(ctx, "b-run"); bState.Status != "running" {
		t.Fatalf("expected build to remain running on non-final attempt, got %s", bState.Status)
	}
	finalJob := &river.Job[ExecuteChunkArgs]{JobRow: &rivertype.JobRow{Attempt: 3, MaxAttempts: 3}, Args: ExecuteChunkArgs{BuildID: "b-run", ChunkIndex: 1}}
	if err := worker.Work(ctx, finalJob); err == nil {
		t.Fatal("expected error on final attempt 502 from brain")
	}
	bState, cState, _ := memStore.GetBuild(ctx, "b-run")
	if bState.Status != "failed" || cState[1].Status != "failed" {
		t.Fatalf("expected build and chunk marked failed on final attempt, got build=%s chunk=%s", bState.Status, cState[1].Status)
	}
	_ = memStore.UpdateBuildStatus(ctx, "b-run", "running")
	_ = memStore.UpdateChunkStatus(ctx, "c-1", "pending")
	_ = NewVanadisServer(Config{BrainURL: "http://[::1]:namedport"}, memStore, nil, nil).ProcessExecuteChunk(ctx, "b-run", 1, true)
	closedSrv := httptest.NewServer(http.NotFoundHandler())
	closedURL := closedSrv.URL
	closedSrv.Close()
	memStore.updateBuildErr = errors.New("build fail update err")
	_ = NewVanadisServer(Config{BrainURL: closedURL}, memStore, nil, nil).ProcessExecuteChunk(ctx, "b-run", 1, true)
	memStore.updateBuildErr = nil
	writeJSON(httptest.NewRecorder(), http.StatusOK, make(chan int))
}

func TestHTTPEndpoints(t *testing.T) {
	memStore := newMemoryVanadisStore()
	inserter := &mockRiverInserter{}
	srv := NewVanadisServer(Config{Port: "0", BrainURL: "http://127.0.0.1:8088"}, memStore, inserter, nil)
	mux := srv.Routes()
	doReq := func(method, path string, body io.Reader) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, path, body))
		return rec
	}
	checkCode := func(method, path string, body io.Reader, want int) {
		if rec := doReq(method, path, body); rec.Code != want {
			t.Fatalf("%s %s got %d, want %d", method, path, rec.Code, want)
		}
	}
	for _, p := range []string{"/health", "/healthz", "/metrics"} {
		checkCode(http.MethodGet, p, nil, http.StatusOK)
	}
	t.Setenv("GOCOVERDIR", t.TempDir())
	origMeta, origCounters := writeMetaDirFn, writeCountersDirFn
	defer func() { writeMetaDirFn, writeCountersDirFn = origMeta, origCounters }()
	writeMetaDirFn, writeCountersDirFn = func(string) error { return errors.New("meta err") }, func(string) error { return nil }
	checkCode(http.MethodPost, "/debug/coverage/flush", nil, http.StatusOK)
	writeCountersDirFn = func(string) error { return errors.New("counters err") }
	checkCode(http.MethodGet, "/debug/coverage/flush", nil, http.StatusInternalServerError)
	srv.SetStore(nil)
	checkCode(http.MethodPost, "/builds", strings.NewReader(`{}`), http.StatusInternalServerError)
	checkCode(http.MethodGet, "/builds/b1", nil, http.StatusInternalServerError)
	checkCode(http.MethodGet, "/builds", nil, http.StatusInternalServerError)
	srv.SetStore(memStore)
	checkCode(http.MethodPost, "/builds", errorReader{}, http.StatusBadRequest)
	for _, p := range []string{
		`{invalid`, `{"repo":""}`, `{"repo":"aerial","thread_id":""}`,
		`{"repo":"aerial","thread_id":"not-numeric","objective":"obj","chunks":[{"title":"t","instructions":"i"}]}`,
		`{"repo":"aerial","thread_id":"123","objective":""}`, `{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[]}`,
		`{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[{"title":"","instructions":"i"}]}`,
		`{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[{"title":"t","instructions":""}]}`,
		`{"repo":"aerial","thread_id":"123","objective":"obj","chunks":[{"title":"t","instructions":"i","effort":"unsupported"}]}`,
	} {
		checkCode(http.MethodPost, "/builds", strings.NewReader(p), http.StatusBadRequest)
	}
	validPayload := `{"repo":"aerial","thread_id":"1555999","title":"Implement Feature X Build Title","objective":"Implement feature X","shared_context":"Targeting Nomad cluster","chunks":[{"title":"Part 1","instructions":"Do part 1","effort":"low"},{"id":"custom-chunk-2","title":"Part 2","instructions":"Do part 2","target_files":["a.go"],"target_symbols":["A"],"effort":"medium","acceptance_criteria":["All green"]}]}`
	memStore.createErr = errors.New("create failed")
	checkCode(http.MethodPost, "/builds", strings.NewReader(validPayload), http.StatusInternalServerError)
	memStore.createErr = nil
	srv.SetRiverClient(nil)
	checkCode(http.MethodPost, "/builds", strings.NewReader(validPayload), http.StatusInternalServerError)
	inserter.err = errors.New("river insert failed")
	srv.SetRiverClient(inserter)
	checkCode(http.MethodPost, "/builds", strings.NewReader(validPayload), http.StatusInternalServerError)
	inserter.err = nil
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
	for _, tc := range []struct {
		m, p string
		w    int
	}{
		{http.MethodGet, "/builds/", http.StatusBadRequest},
		{http.MethodDelete, "/builds/" + buildID, http.StatusMethodNotAllowed},
		{http.MethodGet, "/builds/unknown-id", http.StatusNotFound},
		{http.MethodGet, "/builds/" + buildID, http.StatusOK},
		{http.MethodGet, "/builds/" + buildID + "/cancel", http.StatusMethodNotAllowed},
		{http.MethodPost, "/builds/%20/cancel", http.StatusBadRequest},
		{http.MethodPost, "/builds/unknown-id/cancel", http.StatusNotFound},
	} {
		checkCode(tc.m, tc.p, nil, tc.w)
	}
	memStore.getErr = errors.New("get error")
	checkCode(http.MethodGet, "/builds/"+buildID, nil, http.StatusInternalServerError)
	memStore.getErr = nil
	memStore.updateBuildErr = errors.New("cancel db err")
	checkCode(http.MethodPost, "/builds/"+buildID+"/cancel", nil, http.StatusInternalServerError)
	memStore.updateBuildErr = nil
	checkCode(http.MethodPost, "/builds/"+buildID+"/cancel", bytes.NewReader(nil), http.StatusOK)
	_ = memStore.UpdateBuildStatus(context.Background(), buildID, "completed")
	checkCode(http.MethodPost, "/builds/"+buildID+"/cancel", bytes.NewReader(nil), http.StatusConflict)
	wPost := httptest.NewRecorder()
	srv.handleListBuilds(wPost, httptest.NewRequest(http.MethodPost, "/builds", nil))
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on handleListBuilds POST, got %d", wPost.Code)
	}
	now := time.Now().UTC()
	bRunning, bCompleted, bQueued := mkBuild("b-run", "111", "", "", "running"), mkBuild("b-comp", "222", "", "", "completed"), mkBuild("b-q", "333", "", "", "running")
	bRunning.CreatedAt, bCompleted.CreatedAt, bQueued.CreatedAt = now.Add(-time.Minute), now.Add(-5*time.Minute), now.Add(-10*time.Minute)
	listStore := newMemoryVanadisStore()
	_ = listStore.CreateBuild(context.Background(), bRunning, []VanadisChunk{mkChunk("c-run-1", "b-run", 0, "C1", "", "", "running")})
	_ = listStore.CreateBuild(context.Background(), bCompleted, []VanadisChunk{mkChunk("c-comp-1", "b-comp", 0, "C2", "", "", "completed")})
	_ = listStore.CreateBuild(context.Background(), bQueued, []VanadisChunk{mkChunk("c-q-1", "b-q", 0, "C3", "", "", "pending")})
	srv.SetStore(listStore)
	listStore.listErr = errors.New("simulated list failure")
	checkCode(http.MethodGet, "/builds", nil, http.StatusInternalServerError)
	listStore.listErr = nil
	recList := doReq(http.MethodGet, "/builds", nil)
	var resp struct {
		Total          int                 `json:"total"`
		RunningCount   int                 `json:"running_count"`
		QueuedCount    int                 `json:"queued_count"`
		CompletedCount int                 `json:"completed_count"`
		Builds         []VanadisBuildEntry `json:"builds"`
	}
	if recList.Code != http.StatusOK || json.Unmarshal(recList.Body.Bytes(), &resp) != nil || resp.Total != 3 || resp.RunningCount != 1 || resp.CompletedCount != 1 || resp.QueuedCount != 1 {
		t.Fatalf("unexpected list response: code=%d resp=%+v", recList.Code, resp)
	}
	checkCode(http.MethodGet, "/runs", nil, http.StatusOK)
	fetchList := func(query string) (int, string) {
		rec := doReq(http.MethodGet, query, nil)
		var out struct {
			Total  int                 `json:"total"`
			Builds []VanadisBuildEntry `json:"builds"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		id := ""
		if len(out.Builds) > 0 {
			id = out.Builds[0].ID
		}
		return out.Total, id
	}
	for _, tc := range []struct {
		query, wantID string
		wantTot       int
	}{
		{"/builds?queue_state=running", "b-run", 1},
		{"/builds?status=completed", "b-comp", 1},
		{"/builds?limit=1", "", 1},
	} {
		tot, id := fetchList(tc.query)
		if tot != tc.wantTot || (tc.wantID != "" && id != tc.wantID) {
			t.Fatalf("query %s got total=%d id=%s", tc.query, tot, id)
		}
	}
	recDetail := doReq(http.MethodGet, "/builds/b-run", nil)
	var detailResp struct {
		Entry VanadisBuildEntry `json:"entry"`
	}
	if err := json.Unmarshal(recDetail.Body.Bytes(), &detailResp); err != nil || detailResp.Entry.ID != "b-run" || detailResp.Entry.QueueState != "running" || detailResp.Entry.TotalChunks != 1 {
		t.Fatalf("unexpected Entry: %+v", detailResp.Entry)
	}
}

func TestInitPostgresAndRiverAndRunServer(t *testing.T) {
	origPoolFn, origRiverFn := newPgxPoolFn, newRiverClientFn
	defer func() { newPgxPoolFn, newRiverClientFn = origPoolFn, origRiverFn }()
	ctx := context.Background()
	srv := NewVanadisServer(Config{}, nil, nil, nil)
	if _, err := initPostgresAndRiver(ctx, srv, "postgres://user:pass@invalid%xx/db"); err == nil {
		t.Fatal("expected parse error")
	}
	newPgxPoolFn = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, errors.New("pool creation failed")
	}
	if _, err := initPostgresAndRiver(ctx, srv, "postgres://u:p@127.0.0.1:5432/db"); err == nil {
		t.Fatal("expected pool error")
	}
	cfg, _ := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db")
	newPgxPoolFn = origPoolFn
	for _, clientErr := range []error{errors.New("client error"), nil} {
		startErr := clientErr
		if startErr == nil {
			startErr = errors.New("start failed")
		}
		newRiverClientFn = func(*pgxpool.Pool, *river.Workers) (riverLifecycleClient, error) {
			if clientErr != nil {
				return nil, clientErr
			}
			return &mockRiverLifecycle{startErr: startErr}, nil
		}
		if _, err := initPostgresAndRiver(ctx, srv, cfg.ConnString()); err == nil {
			t.Fatal("expected error from initPostgresAndRiver")
		}
	}
	newRiverClientFn = func(*pgxpool.Pool, *river.Workers) (riverLifecycleClient, error) {
		return &mockRiverLifecycle{stopErr: errors.New("stop warning")}, nil
	}
	cleanup, err := initPostgresAndRiver(ctx, srv, cfg.ConnString())
	if err != nil || cleanup == nil {
		t.Fatalf("expected success from initPostgresAndRiver, got err=%v", err)
	}
	cleanup(ctx)
	if runServer(ctx, Config{Port: "-1", PostgresURL: cfg.ConnString()}, nil) == nil || runServer(ctx, Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"}, nil) == nil {
		t.Fatal("expected error on invalid runServer configs")
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- runServer(runCtx, Config{Port: "0", PostgresURL: cfg.ConnString()}, func(addr string) { ready <- addr })
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
	var nilSrv *VanadisServer
	nilSrv.syncBuildWidget(ctx, VanadisBuild{}, nil, "")
	store := newMemoryVanadisStore()
	store.builds["b1"] = mkBuild("b1", "1555405874565091380", "", "", "")
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
	build := mkBuild("b1", "1555405874565091380", "Test Build Title", "Test Obj", "running")
	startedAt, updatedAt := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC), time.Date(2026, 10, 16, 18, 3, 0, 0, time.UTC)
	chunk := mkChunk("", "", 0, "Chunk 0", "", "", "running")
	chunk.Repo, chunk.StartedAt, chunk.UpdatedAt = "aerial", startedAt, updatedAt
	chunks := []VanadisChunk{chunk}
	srv.syncBuildWidget(ctx, build, chunks, "test banner")
	if postedReq.ID != "b1" || postedReq.Title != "Test Build Title" || postedReq.Banner != "test banner" || len(postedReq.Chunks) != 1 || !postedReq.Chunks[0].StartedAt.Equal(startedAt) || !postedReq.Chunks[0].UpdatedAt.Equal(updatedAt) {
		t.Errorf("Unexpected posted widget payload: %+v", postedReq)
	}
	if store.builds["b1"].StatusMessageID != "msg-new-widget-42" {
		t.Errorf("Expected store status_message_id msg-new-widget-42, got %q", store.builds["b1"].StatusMessageID)
	}
	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer tsBadJSON.Close()
	NewVanadisServer(Config{BrainURL: tsBadJSON.URL}, store, nil, tsBadJSON.Client()).syncBuildWidget(ctx, build, chunks, "")
	store.updateBuildErr = errors.New("simulated store failure")
	srv.syncBuildWidget(ctx, build, chunks, "")
}

func TestClassifyAndSummarizeBuildEntry(t *testing.T) {
	t.Parallel()
	classifyTests := []struct {
		name        string
		buildStatus string
		chunks      []VanadisChunk
		expected    string
	}{
		{"completed status", "completed", nil, "completed"},
		{"merged status", "merged", nil, "completed"},
		{"failed status", "failed", nil, "completed"},
		{"cancelled status", "cancelled", nil, "completed"},
		{"canceled status uppercase", "CANCELED", nil, "completed"},
		{"queued status", "queued", nil, "queued"},
		{"pending status", "pending", nil, "queued"},
		{"running with empty chunks", "running", []VanadisChunk{}, "running"},
		{"running with all pending chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "pending")}, "queued"},
		{"running with mixed chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "running")}, "running"},
		{"running with completed chunk", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "completed")}, "running"},
		{"unknown status defaults to queued", "unknown_state", nil, "queued"},
		{"whitespace padded status", "  completed  ", nil, "completed"},
	}
	for _, tt := range classifyTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyBuildQueueState(tt.buildStatus, tt.chunks); got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
	build := mkBuild("b-summary-1", "", "", "", "running")
	entryNil := SummarizeBuildEntry(build, nil)
	if entryNil.Chunks == nil || len(entryNil.Chunks) != 0 || entryNil.TotalChunks != 0 || entryNil.QueueState != "running" {
		t.Fatalf("unexpected entryNil: %+v", entryNil)
	}
	chunkStatuses := []string{"completed", "merged", "running", "coding", "pr_opened", "deploying", "pending", "failed", "unknown"}
	chunks := make([]VanadisChunk, len(chunkStatuses))
	for i, st := range chunkStatuses {
		chunks[i] = mkChunk(fmt.Sprintf("c%d", i+1), "", i, "", "", "", st)
	}
	entry := SummarizeBuildEntry(build, chunks)
	if entry.TotalChunks != 9 || entry.CompletedChunks != 2 || entry.RunningChunks != 4 || entry.PendingChunks != 1 || entry.FailedChunks != 1 || entry.QueueState != "running" {
		t.Fatalf("unexpected summarized entry: %+v", entry)
	}
}
