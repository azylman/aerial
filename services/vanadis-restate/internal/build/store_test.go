package build

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type (
	bareStore struct{ store *store.Store }
	dbScript  struct {
		bare                        *bareStore
		routes                      []sqlRoute
		row, buildRow, countRow     pgx.Row
		chunks                      []model.VanadisChunk
		builds                      []model.VanadisBuild
		beginErr, queryErr, scanErr error
		rowsErr, execErr, commitErr error
		rollbackErr                 error
		failExec, execTag           string
	}
)

type sqlRoute struct {
	match string
	row   pgx.Row
}

func (s dbScript) resolveQueryRow(query string, args []any, candidates *[]string) pgx.Row {
	for _, arg := range args {
		if cands, ok := arg.([]string); ok {
			*candidates = cands
			break
		}
	}
	if strings.Contains(query, "RETURNING ci_fail_count") {
		for _, r := range s.routes {
			if strings.Contains(query, r.match) {
				return r.row
			}
		}
		if s.row != nil {
			return s.row
		}
		if s.execTag == "UPDATE 0" {
			return errRow(pgx.ErrNoRows)
		}
		return valuesRow(1)
	}
	for _, r := range s.routes {
		if strings.Contains(query, r.match) {
			return r.row
		}
	}
	if strings.Contains(query, "COALESCE(pr_number, 0), stall_nudges FROM vanadis_chunks") && len(s.chunks) > 0 && !isActionRow(s.row) {
		c := s.chunks[0]
		return valuesRow(c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Title, c.Status, c.PRNumber, c.StallNudges)
	}
	if strings.HasPrefix(query, "SELECT id FROM vanadis_builds WHERE id = (SELECT build_id") {
		return valuesRow("b1")
	}
	if s.countRow != nil && strings.Contains(query, "COUNT(*)") {
		return s.countRow
	}
	if s.buildRow != nil && strings.Contains(query, "FROM vanadis_builds") {
		return s.buildRow
	}
	return s.row
}

func (s dbScript) open() (*store.Store, *[]string) {
	if s.bare != nil {
		return s.bare.store, new([]string)
	}
	db, candidates := s.openRaw()
	return store.New(db), candidates
}

func (s dbScript) openRaw() (mockDB, *[]string) {
	candidates := new([]string)
	tag := store.FirstNonEmpty(s.execTag, "UPDATE 1")
	queryRow := func(_ context.Context, query string, args ...any) pgx.Row {
		return s.resolveQueryRow(query, args, candidates)
	}
	query := func(context.Context, string, ...any) (pgx.Rows, error) {
		if s.queryErr != nil {
			return nil, s.queryErr
		}
		return &mockRows{items: s.chunks, builds: s.builds, scanErr: s.scanErr, rowsErr: s.rowsErr}, nil
	}
	exec := func(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
		if s.execErr != nil && strings.Contains(query, s.failExec) {
			return pgconn.CommandTag{}, s.execErr
		}
		return pgconn.NewCommandTag(tag), nil
	}
	tx := &mockTx{commitErr: s.commitErr, rollbackErr: s.rollbackErr, queryRowFn: queryRow, queryFn: query, execFn: exec}
	return mockDB{
		queryRowFn: queryRow,
		queryFn:    query,
		execFn:     exec,
		beginFn: func(context.Context) (pgx.Tx, error) {
			if s.beginErr != nil {
				return nil, s.beginErr
			}
			return tx, nil
		},
	}, candidates
}

var nilDB = &bareStore{store: &store.Store{}}

func errRow(err error) *mockRow { return &mockRow{err: err} }
func assertStoreErr(t *testing.T, err, wantErrIs error, wantErr bool) bool {
	t.Helper()
	switch {
	case wantErrIs != nil:
		if !errors.Is(err, wantErrIs) {
			t.Fatalf("err = %v, want %v", err, wantErrIs)
		}
		return true
	case wantErr:
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		return true
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	}
	return false
}

type genericMockRow struct {
	scanFn func(dest ...any) error
}

func valuesRow(vals ...any) *genericMockRow {
	return &genericMockRow{scanFn: func(dest ...any) error { return scanValues(dest, vals...) }}
}

func scanValues(dest []any, vals ...any) error {
	if len(vals) > len(dest) {
		return fmt.Errorf("scan: %d values for %d destinations", len(vals), len(dest))
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		ptr, val := reflect.ValueOf(dest[i]), reflect.ValueOf(v)
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Elem().Type() != val.Type() {
			return fmt.Errorf("scan column %d: destination %T cannot hold %T", i, dest[i], v)
		}
		ptr.Elem().Set(val)
	}
	return nil
}

func (r *genericMockRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type mockRow struct {
	b   *model.VanadisBuild
	err error
}

func fillBuildDest(b model.VanadisBuild, dest []any) error {
	if len(dest) == 7 {
		return scanValues(dest, b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.Status, b.StatusMessageID)
	}
	if len(dest) != 11 {
		return fmt.Errorf("scan build: %d destinations, want 11", len(dest))
	}
	return scanValues(dest, b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet, b.CreatedAt, b.UpdatedAt)
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) == 1 && r.b != nil {
		return scanValues(dest, r.b.Status)
	}
	if r.b != nil {
		return fillBuildDest(*r.b, dest)
	}
	return nil
}

type mockRows struct {
	pgx.Rows

	items            []model.VanadisChunk
	builds           []model.VanadisBuild
	idx              int
	scanErr, rowsErr error
}

func (*mockRows) Close()       {}
func (r *mockRows) Err() error { return r.rowsErr }
func (r *mockRows) Next() bool {
	if len(r.builds) > 0 && len(r.items) == 0 {
		r.idx++
		return r.idx <= len(r.builds)
	}
	if r.idx < len(r.items) {
		r.idx++
		return true
	}
	return false
}

func (r *mockRows) scan11(dest []any) error {
	if len(r.builds) > 0 && r.idx > 0 && r.idx <= len(r.builds) {
		return fillBuildDest(r.builds[r.idx-1], dest)
	}
	if len(r.items) > 0 && r.idx > 0 && r.idx <= len(r.items) {
		c := r.items[r.idx-1]
		b := model.VanadisBuild{ID: c.BuildID, Title: c.Title}
		return fillBuildDest(b, dest)
	}
	return nil
}

func (r *mockRows) scan10(dest []any) error {
	if len(r.items) > 0 && r.idx > 0 && r.idx <= len(r.items) {
		c := r.items[r.idx-1]
		return scanValues(dest, c.ID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Status, c.PRNumber, c.StartedAt, c.CompletedAt, c.UpdatedAt)
	}
	return nil
}

func (r *mockRows) scan18(dest []any) error {
	if len(r.items) > 0 && r.idx > 0 && r.idx <= len(r.items) {
		c := r.items[r.idx-1]
		return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
	}
	return nil
}

func (r *mockRows) scan25(dest []any) error {
	if len(r.items) > 0 && r.idx > 0 && r.idx <= len(r.items) {
		c := r.items[r.idx-1]
		rawCIChecks := recoveryFixtureChecks(c)
		rawRolloutState := []byte(`{"expected_jobs":["eri"]}`)
		return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, rawCIChecks, rawRolloutState, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
	}
	return scanValues(dest, "c1", "b1", 0, "pr", "aerial", "c1", "inst", []string{}, []string{}, "medium", []string{}, "pending", 0, 0, []string{}, 0, []byte("{}"), []byte("{}"), "", "", "", sql.NullTime{}, sql.NullTime{}, time.Now(), time.Now())
}

func (r *mockRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	switch len(dest) {
	case 2:
		if len(r.items) > 0 && r.idx > 0 && r.idx <= len(r.items) {
			return scanValues(dest, r.items[r.idx-1].BuildID, r.items[r.idx-1].ID)
		}
		return nil
	case 11:
		return r.scan11(dest)
	case 10:
		return r.scan10(dest)
	case 18:
		return r.scan18(dest)
	case 25:
		return r.scan25(dest)
	default:
		return nil
	}
}

type mockTx struct {
	pgx.Tx

	execFn                 func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	queryFn                func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn             func(ctx context.Context, sql string, args ...any) pgx.Row
	commitErr, rollbackErr error
}

func (t *mockTx) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, query, args...)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (t *mockTx) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	if t.queryFn != nil {
		return t.queryFn(ctx, query, args...)
	}
	return &mockRows{}, nil
}

func (t *mockTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if t.queryRowFn != nil {
		return t.queryRowFn(ctx, query, args...)
	}
	return &mockRow{}
}
func (t *mockTx) Commit(_ context.Context) error   { return t.commitErr }
func (t *mockTx) Rollback(_ context.Context) error { return t.rollbackErr }

type mockDB struct {
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	beginFn    func(ctx context.Context) (pgx.Tx, error)
}

func (db mockDB) Exec(ctx context.Context, s string, a ...any) (pgconn.CommandTag, error) {
	if db.execFn != nil {
		return db.execFn(ctx, s, a...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (db mockDB) Query(ctx context.Context, s string, a ...any) (pgx.Rows, error) {
	if db.queryFn != nil {
		return db.queryFn(ctx, s, a...)
	}
	return &mockRows{}, nil
}

func (db mockDB) QueryRow(ctx context.Context, s string, a ...any) pgx.Row {
	if db.queryRowFn != nil {
		return db.queryRowFn(ctx, s, a...)
	}
	return &mockRow{}
}

func (db mockDB) Begin(ctx context.Context) (pgx.Tx, error) {
	if db.beginFn != nil {
		return db.beginFn(ctx)
	}
	return &mockTx{}, nil
}

func newMockStore(db mockDB) *store.Store {
	return store.New(db)
}

func attachRoutes(mode string, exists, cancelled bool) []sqlRoute {
	return []sqlRoute{
		{match: "SELECT mode", row: valuesRow(mode)},
		{match: "c.status = 'cancelled'", row: valuesRow(cancelled)},
		{match: "SELECT EXISTS", row: valuesRow(exists)},
	}
}

func attachRequest(chunkID, repo string) store.AttachChunkPRRequest {
	return store.AttachChunkPRRequest{ChunkID: chunkID, PRNumber: 101, Repo: repo}
}

func TestPostgresVanadisStore_UpdatesAndStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "update build status success", db: dbScript{execTag: "UPDATE 1"}},
		{name: "update build status not found", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: store.ErrBuildNotFound},
		{name: "update build status message id success", db: dbScript{execTag: "UPDATE 1"}},
		{name: "update build status message id not found", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: store.ErrBuildNotFound},
		{name: "update chunk status success", db: dbScript{execTag: "UPDATE 1"}},
		{name: "update chunk status not found", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "update chunk banner and status success", db: dbScript{execTag: "UPDATE 1"}},
		{name: "update chunk banner and status not found", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: errPRTransitionSkipped},
		{name: "record rollout state success", db: dbScript{execTag: "UPDATE 1"}},
		{name: "record rollout state not found", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			switch tc.name {
			case "update build status success", "update build status not found":
				err := st.UpdateBuildStatus(context.Background(), "b1", "running")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "update build status message id success", "update build status message id not found":
				err := st.UpdateBuildStatusMessageID(context.Background(), "b1", "msg-1")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "update chunk status success", "update chunk status not found":
				err := st.UpdateChunkStatus(context.Background(), "c1", "completed")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "update chunk banner and status success", "update chunk banner and status not found":
				err := st.UpdateChunkBannerAndStatus(context.Background(), "c1", "banner", "running")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "record rollout state success", "record rollout state not found":
				err := st.RecordPRRolloutState(context.Background(), "c1", "deploying", &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}})
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			}
		})
	}
}

func TestStoreHelpers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		thread     string
		wantThread string
		repo       string
		wantRepo   string
	}{
		{name: "clean thread with discord prefix", thread: "discord-12345", wantThread: "12345", repo: "azylman/aerial", wantRepo: "aerial"},
		{name: "clean thread with colon", thread: "chan:9876", wantThread: "9876", repo: "aerial", wantRepo: "aerial"},
		{name: "empty snowflake", repo: "", wantRepo: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.thread != "" && normalizeDiscordThreadID(tc.thread) != tc.wantThread {
				t.Errorf("normalizeDiscordThreadID(%q) = %q, want %q", tc.thread, normalizeDiscordThreadID(tc.thread), tc.wantThread)
			}
			if cleanRepoName(tc.repo) != tc.wantRepo {
				t.Errorf("cleanRepoName(%q) = %q, want %q", tc.repo, cleanRepoName(tc.repo), tc.wantRepo)
			}
		})
	}
	if id := GenerateBuildID(); !strings.HasPrefix(id, "build-") {
		t.Errorf("GenerateBuildID() = %q, want prefix 'build-'", id)
	}
}

func isActionRow(row pgx.Row) bool { _, ok := row.(*genericMockRow); return ok }
