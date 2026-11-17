package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
)

type (
	bareStore struct{ store *PostgresVanadisStore }
	dbScript  struct {
		bare                    *bareStore
		beginErr                error
		routes                  []sqlRoute
		row, buildRow, countRow pgx.Row
		chunks                  []VanadisChunk
		builds                  []VanadisBuild
		queryErr                error
		scanErr                 error
		rowsErr                 error
		failExec                string
		execErr                 error
		execTag                 string
		commitErr               error
		rollbackErr             error
	}
)

type sqlRoute struct {
	match string
	row   pgx.Row
}

func (s dbScript) open() (*PostgresVanadisStore, *[]string) {
	candidates := new([]string)
	if s.bare != nil {
		return s.bare.store, candidates
	}
	tag := s.execTag
	if tag == "" {
		tag = "UPDATE 1"
	}
	queryRow := func(_ context.Context, query string, args ...any) pgx.Row {
		for _, arg := range args {
			if cands, ok := arg.([]string); ok {
				*candidates = cands
				break
			}
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
	return newMockStore(mockDB{
		queryRowFn: queryRow,
		queryFn:    query,
		execFn:     exec,
		beginFn: func(context.Context) (pgx.Tx, error) {
			if s.beginErr != nil {
				return nil, s.beginErr
			}
			return tx, nil
		},
	}), candidates
}

var (
	nilReceiver   = &bareStore{}
	nilDB         = &bareStore{store: &PostgresVanadisStore{}}
	aerialRepoSet = []string{"aerial", "azylman/aerial"}
)

func errRow(err error) *mockRow { return &mockRow{err: err} }
func actionChunkRow(mode, cStatus string) *genericMockRow {
	return valuesRow("c1", "b1", 0, mode, "Action Chunk", cStatus, 0)
}

func actionBuildRow(bStatus string) *genericMockRow {
	return valuesRow("b1", "thread-1", 2, "Build 1", "Objective 1", bStatus, "msg-1")
}

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

func assertCandidates(t *testing.T, got *[]string, want []string) {
	t.Helper()
	if want != nil && !slices.Equal(*got, want) {
		t.Errorf("repo candidates = %v, want %v", *got, want)
	}
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
	b   *VanadisBuild
	err error
}

func fillBuildDest(b VanadisBuild, dest []any) error {
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

	items            []VanadisChunk
	builds           []VanadisBuild
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
		b := VanadisBuild{ID: c.BuildID, Title: c.Title}
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

func newMockStore(db mockDB) *PostgresVanadisStore {
	return NewPostgresVanadisStore(db)
}

type advanceOutcome struct {
	Matched, Advanced, BuildCompleted bool
	NextChunkIndex                    int
	BuildStatus                       string
	Chunks                            int
}

func outcomeOf(res *VanadisAdvancementResult) advanceOutcome {
	return advanceOutcome{Matched: res.Matched, Advanced: res.Advanced, BuildCompleted: res.BuildCompleted, NextChunkIndex: res.NextChunkIndex, BuildStatus: res.BuildStatus, Chunks: len(res.Chunks)}
}

func attachRoutes(mode string, exists, cancelled bool) []sqlRoute {
	return []sqlRoute{
		{match: "SELECT mode", row: valuesRow(mode)},
		{match: "c.status = 'cancelled'", row: valuesRow(cancelled)},
		{match: "SELECT EXISTS", row: valuesRow(exists)},
	}
}

type attachResult struct{ BuildID, ChunkID, ThreadID string }

func TestPostgresVanadisStore_AttachChunkPR(t *testing.T) {
	t.Parallel()
	var (
		errModeQuery = errors.New("attach db err")
		errLookup    = errors.New("lookup err")
		errBegin     = errors.New("begin standalone tx err")
		errBuildIns  = errors.New("insert build err")
		errChunkIns  = errors.New("insert chunk err")
		errCommit    = errors.New("commit standalone err")
	)
	noRows := errRow(pgx.ErrNoRows)
	attached := func(mode string) *genericMockRow { return valuesRow("b-"+mode, "c-"+mode, "thread-"+mode) }
	existing := attachRequest("c1", "aerial")
	standalone := attachRequest("", "aerial")
	tests := []struct {
		name            string
		req             AttachChunkPRRequest
		db              dbScript
		wantErrIs       error
		wantErr         bool
		wantErrContains string
		want            attachResult
		wantNewBuild    bool
		wantCandidates  []string
	}{
		{name: "nil receiver", req: existing, db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", req: existing, db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id and zero pr number", req: AttachChunkPRRequest{Repo: "aerial"}, wantErrIs: ErrInvalidBuildInput},
		{name: "chunk not found", req: existing, db: dbScript{row: noRows}, wantErrIs: ErrChunkNotFound},
		{name: "mode query error", req: existing, db: dbScript{row: errRow(errModeQuery)}, wantErrIs: errModeQuery},
		{name: "action chunk requires repo", req: attachRequest("c1", ""), db: dbScript{routes: attachRoutes("action", true, false), row: noRows}, wantErrIs: ErrInvalidBuildInput},
		{name: "action blank repo", req: attachRequest("c1", " \t"), db: dbScript{routes: attachRoutes("action", true, false)}, wantErrIs: ErrInvalidBuildInput},
		{name: "negative pr number", req: AttachChunkPRRequest{ChunkID: "c1", PRNumber: -1}, wantErrIs: ErrInvalidBuildInput},
		{name: "mismatched repo", req: existing, db: attachmentScript("c1", "b1", "", "running", "other"), wantErrIs: ErrBuildConflict},
		{name: "cancelled chunk or build with repo conflicts", req: existing, db: attachmentScript("c1", "b1", "", "cancelled", "aerial"), wantErrIs: ErrBuildConflict},
		{name: "cancelled chunk or build without repo conflicts", req: attachRequest("c1", ""), db: attachmentScript("c1", "b1", "", "cancelled", "aerial"), wantErrIs: ErrBuildConflict},
		{name: "attaches pr chunk without repo", req: attachRequest("c-pr", ""), db: attachmentScript("c-pr", "b-pr", "thread-pr", "running", "aerial"), want: attachResult{"b-pr", "c-pr", "thread-pr"}},
		{name: "attaches pr chunk matching repo candidates", req: attachRequest("c-pr", "aerial"), db: attachmentScript("c-pr", "b-pr", "thread-pr", "running", "aerial"), want: attachResult{"b-pr", "c-pr", "thread-pr"}},
		{name: "attaches action chunk", req: attachRequest("c-act", "aerial"), db: dbScript{routes: attachRoutes("action", true, false), row: attached("action")}, want: attachResult{"b-action", "c-action", "thread-action"}, wantCandidates: aerialRepoSet},
		{name: "action attachment removed chunk", req: existing, db: dbScript{routes: attachRoutes("action", false, false), row: noRows}, wantErrIs: ErrChunkNotFound},
		{name: "action attachment terminal parent", req: existing, db: dbScript{routes: attachRoutes("action", true, true), row: noRows}, wantErrIs: ErrBuildConflict},
		{name: "action attachment mismatching repository", req: existing, db: dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("action")}, {match: "c.status = 'cancelled'", row: valuesRow(false)}, {match: "repo != ''", row: valuesRow(true)}, {match: "SELECT EXISTS", row: valuesRow(true)}}, row: noRows}, wantErrIs: ErrBuildConflict},
		{name: "action attachment unknown rejection", req: existing, db: dbScript{routes: attachRoutes("action", true, false), row: noRows}, wantErr: true},
		{name: "pr transaction begin error", req: existing, db: dbScript{routes: attachRoutes("pr", true, false), beginErr: errBegin}, wantErrIs: errBegin},
		{name: "pr transaction chunk removed", req: existing, db: dbScript{routes: attachRoutes("pr", true, false), row: noRows}, wantErrIs: ErrChunkNotFound},
		{name: "pr transaction lookup failure", req: existing, db: dbScript{routes: attachRoutes("pr", true, false), row: errRow(errLookup)}, wantErrIs: errLookup},
		{name: "pr parent lock failure", req: existing, db: dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, buildRow: errRow(errLookup)}, wantErrIs: errLookup},
		{name: "pr locked chunk absent", req: existing, db: dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, buildRow: &mockRow{b: &VanadisBuild{ID: "b1", Status: "running"}}}, wantErrIs: ErrChunkNotFound},
		{name: "standalone requires repo", req: attachRequest("", ""), wantErrIs: ErrInvalidBuildInput},
		{name: "standalone begin error", req: standalone, db: dbScript{beginErr: errBegin}, wantErrIs: errBegin},
		{name: "standalone existing pr lookup error", req: standalone, db: dbScript{row: errRow(errLookup)}, wantErrIs: errLookup},
		{name: "standalone reuses existing pr chunk", req: standalone, db: dbScript{row: valuesRow("b-existing", "c-existing", "thread-existing")}, want: attachResult{"b-existing", "c-existing", "thread-existing"}, wantCandidates: aerialRepoSet},
		{name: "standalone insert build error", req: standalone, db: dbScript{row: noRows, failExec: "INSERT INTO vanadis_builds", execErr: errBuildIns}, wantErrIs: errBuildIns},
		{name: "standalone insert chunk error", req: standalone, db: dbScript{row: noRows, failExec: "INSERT INTO vanadis_chunks", execErr: errChunkIns}, wantErrIs: errChunkIns},
		{name: "standalone commit error", req: standalone, db: dbScript{row: noRows, commitErr: errCommit}, wantErrIs: errCommit},
		{name: "standalone creates build in target thread", req: AttachChunkPRRequest{Repo: "aerial", PRNumber: 101, TargetID: "123456789012345678", Branch: "dependabot/npm_and_yarn/foo", Title: "Bump foo"}, db: dbScript{row: noRows}, want: attachResult{ThreadID: "123456789012345678"}, wantNewBuild: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, candidates := tc.db.open()
			bID, cID, threadID, err := store.AttachChunkPR(context.Background(), tc.req)
			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("err = %v, want error containing %q", err, tc.wantErrContains)
				}
				return
			}
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			got := attachResult{bID, cID, threadID}
			if tc.wantNewBuild {
				if !strings.HasPrefix(bID, "build-") || cID != bID+"-chunk-1" {
					t.Errorf("AttachChunkPR minted build %q chunk %q, want build-* with chunk <build>-chunk-1", bID, cID)
				}
				got.BuildID, got.ChunkID = "", ""
			}
			if got != tc.want {
				t.Errorf("AttachChunkPR = %+v, want %+v", got, tc.want)
			}
			assertCandidates(t, candidates, tc.wantCandidates)
		})
	}
}

func attachRequest(chunkID, repo string) AttachChunkPRRequest {
	return AttachChunkPRRequest{ChunkID: chunkID, PRNumber: 101, Repo: repo}
}

func TestPostgresVanadisStore_CompleteActionChunk(t *testing.T) {
	t.Parallel()
	running := actionChunkRow("action", "running")
	runningBuild := actionBuildRow("running")
	tests := []struct {
		name       string
		chunkID    string
		dispatchID string
		db         dbScript
		wantErrIs  error
		wantErr    bool
		want       advanceOutcome
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{beginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{row: errRow(errors.New("select chunk err"))}, wantErr: true},
		{name: "build not found", chunkID: "c1", db: dbScript{row: running, buildRow: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select build error", chunkID: "c1", db: dbScript{row: running, buildRow: errRow(errors.New("db build error"))}, wantErr: true},
		{name: "pr mode chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("pr", "running"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "completed action chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("action", "completed"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "failed action chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("action", "failed"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "cancelled parent rejects success", chunkID: "c1", db: dbScript{row: running, buildRow: actionBuildRow("cancelled")}, want: advanceOutcome{Matched: true, NextChunkIndex: 1, BuildStatus: "cancelled"}},
		{name: "missing retry identity", chunkID: "c1", dispatchID: "", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "stale retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "matching retry identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "mismatched first identity", chunkID: "c1", dispatchID: "wrong", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 0), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: false, NextChunkIndex: 1, BuildStatus: "running"}},
		{name: "remaining count failure", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: errRow(errors.New("count unavailable"))}, wantErr: true},
		{name: "completion commit failure", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(1), commitErr: errors.New("commit unavailable")}, wantErr: true},
		{name: "rejected completion commit failure", chunkID: "c1", db: dbScript{row: actionChunkRow("action", "completed"), buildRow: runningBuild, commitErr: errors.New("commit unavailable")}, wantErr: true},

		{name: "update chunk error", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, failExec: "UPDATE vanadis_chunks", execErr: errors.New("update action chunk err")}, wantErr: true},
		{name: "completes action chunk and marks build completed when no remaining chunks", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0), chunks: []VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "action", Repo: "aerial", Title: "T", Status: "completed"}}}, want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: 1, BuildStatus: "completed", Chunks: 1}},
		{name: "complete build error in finishActionBuild", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0), failExec: "UPDATE vanadis_builds", execErr: errors.New("upd build err")}, wantErr: true},
		{name: "query chunks error in finishActionBuild", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0), queryErr: errors.New("query err")}, wantErr: true},
		{name: "completes action chunk", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: 1, BuildStatus: "running"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.db.buildRow == nil {
				tc.db.buildRow = runningBuild
			}
			store, _ := tc.db.open()
			res, err := store.CompleteActionChunk(context.Background(), tc.chunkID, tc.dispatchID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if got := outcomeOf(res); got != tc.want {
				t.Errorf("CompleteActionChunk = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore_FailChunk(t *testing.T) {
	t.Parallel()
	running := failRow("running", "running")
	tests := []struct {
		name       string
		chunkID    string
		db         dbScript
		wantErrIs  error
		wantErr    bool
		wantFailed bool
		dispatchID string
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "", wantErr: true},
		{name: "whitespace chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{beginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{row: errRow(errors.New("select chunk fail err"))}, wantErr: true},
		{name: "already terminal chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("failed", "failed")}},
		{name: "completed chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("completed", "running")}},
		{name: "cancelled chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("cancelled", "running")}},
		{name: "update chunk error", chunkID: "c1", db: dbScript{row: running, failExec: "UPDATE vanadis_chunks", execErr: errors.New("update chunk fail err")}, wantErr: true},
		{name: "update build error", chunkID: "c1", db: dbScript{row: running, failExec: "UPDATE vanadis_builds", execErr: errors.New("update build fail err")}, wantErr: true},
		{name: "commit error", chunkID: "c1", db: dbScript{row: running, commitErr: errors.New("commit fail err")}, wantErr: true},
		{name: "retry missing identity", chunkID: "c1", db: dbScript{row: failRow("running", "running", 1)}},
		{name: "retry old identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: failRow("running", "running", 1)}},
		{name: "retry current identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{row: failRow("running", "running", 1)}, wantFailed: true},
		{name: "parent lock error", chunkID: "c1", db: dbScript{routes: []sqlRoute{{match: "SELECT id FROM vanadis_builds", row: errRow(context.Canceled)}}}, wantErrIs: context.Canceled},
		{name: "stale identity", chunkID: "c1", dispatchID: "stale", db: dbScript{row: running}},
		{name: "fails running chunk and build", chunkID: "c1", db: dbScript{row: running}, wantFailed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			b, c, failed, err := store.FailChunk(context.Background(), tc.chunkID, "unrecoverable failure", tc.dispatchID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if failed != tc.wantFailed {
				t.Fatalf("FailChunk failed = %v, want %v", failed, tc.wantFailed)
			}
			if tc.wantFailed && (b.Status != "failed" || c.Status != "failed") {
				t.Errorf("FailChunk statuses = build %q chunk %q, want failed/failed", b.Status, c.Status)
			}
		})
	}
}

func TestPostgresVanadisStore_ReserveChunkForLaunch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		chunkID   string
		db        dbScript
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "exec error", chunkID: "c1", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("exec error")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "reserves chunk successfully", chunkID: "c1", db: dbScript{execTag: "UPDATE 1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			err := store.ReserveChunkForLaunch(context.Background(), tc.chunkID)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestPostgresVanadisStore_BuildCRUD(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1", Objective: "Obj 1"}
	c1 := []VanadisChunk{{ID: "c1", BuildID: "b1", Repo: "aerial", Title: "Chunk 1", Instructions: "Inst 1", DependsOn: []string{}}}

	tests := []struct {
		name      string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "create build success", db: dbScript{}},
		{name: "create build begin error", db: dbScript{beginErr: errors.New("begin error")}, wantErr: true},
		{name: "create build exec build error", db: dbScript{failExec: "INSERT INTO vanadis_builds", execErr: errors.New("exec build error")}, wantErr: true},
		{name: "create build exec chunk error", db: dbScript{failExec: "INSERT INTO vanadis_chunks", execErr: errors.New("exec chunk error")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			err := store.CreateBuild(context.Background(), b1, c1)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestPostgresVanadisStore_GetBuild(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1"}
	tests := []struct {
		name      string
		buildID   string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "get build not found", buildID: "b1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "get build query error", buildID: "b1", db: dbScript{row: errRow(errors.New("db error"))}, wantErr: true},
		{name: "get build chunks query error", buildID: "b1", db: dbScript{row: &mockRow{b: &b1}, queryErr: errors.New("chunks err")}, wantErr: true},
		{name: "get build success", buildID: "b1", db: dbScript{
			row: &mockRow{b: &b1},
			chunks: []VanadisChunk{{
				ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "pr", Repo: "aerial",
				StartedAt: time.Now(), CompletedAt: time.Now(),
				TargetFiles: []string{"f.go"}, TargetSymbols: []string{"S"}, AcceptanceCriteria: []string{"AC"},
				DependsOn: []string{"dep"}, Effort: "medium",
			}},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			_, _, err := store.GetBuild(context.Background(), tc.buildID)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
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
		{name: "update build status not found", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrBuildNotFound},
		{name: "update build status message id success", db: dbScript{execTag: "UPDATE 1"}},
		{name: "update build status message id not found", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrBuildNotFound},
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
			store, _ := tc.db.open()
			switch tc.name {
			case "update build status success", "update build status not found":
				err := store.UpdateBuildStatus(context.Background(), "b1", "running")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "update build status message id success", "update build status message id not found":
				err := store.UpdateBuildStatusMessageID(context.Background(), "b1", "msg-1")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "update chunk status success", "update chunk status not found":
				err := store.UpdateChunkStatus(context.Background(), "c1", "completed")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "update chunk banner and status success", "update chunk banner and status not found":
				err := store.UpdateChunkBannerAndStatus(context.Background(), "c1", "banner", "running")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "record rollout state success", "record rollout state not found":
				err := store.RecordPRRolloutState(context.Background(), "c1", "deploying", &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}})
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			}
		})
	}
}

func TestPostgresVanadisStore_LookupsAndAppend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		db        dbScript
		wantErr   bool
		wantErrIs error
		wantBuild string
	}{
		{name: "lookup pr chunk not found", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "lookup pr chunk success", db: dbScript{row: valuesRow("c1", "b1", 0, "medium", false)}, wantBuild: "b1"},
		{name: "lookup chunk build id not found", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "lookup chunk build id success", db: dbScript{row: valuesRow("b1")}, wantBuild: "b1"},
		{name: "append chunks success", db: dbScript{
			row:     valuesRow("running"),
			chunks:  []VanadisChunk{{ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "action"}},
			execTag: "INSERT 0 1",
		}},
		{name: "append chunks build not found", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "append chunks build not running", db: dbScript{row: valuesRow("completed")}, wantErrIs: ErrBuildConflict},
		{name: "append chunks duplicate id", db: dbScript{
			row:    valuesRow("running"),
			chunks: []VanadisChunk{{ID: "c-dup", BuildID: "b1", Title: "C1", Status: "running", Mode: "action"}},
		}, wantErrIs: ErrBuildConflict},
		{name: "append chunks begin error", db: dbScript{beginErr: errors.New("begin fail")}, wantErr: true},
		{name: "append chunks commit error", db: dbScript{
			row:       valuesRow("running"),
			chunks:    []VanadisChunk{{ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "action"}},
			execTag:   "INSERT 0 1",
			commitErr: errors.New("commit fail"),
		}, wantErr: true},
		{name: "remove chunk success", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{{ID: "c1", Status: "pending", Mode: "action"}, {ID: "c2", Status: "pending", Mode: "action"}}, execTag: "DELETE 1"}},
		{name: "remove chunk only chunk", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{{ID: "c1", Status: "pending", Mode: "action"}}}, wantErrIs: ErrBuildConflict},
		{name: "remove chunk not found", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "remove chunk build not running", db: dbScript{row: valuesRow("completed")}, wantErrIs: ErrBuildConflict},
		{name: "remove chunk not pending", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{{ID: "c1", Status: "running"}}}, wantErrIs: ErrBuildConflict},
		{name: "remove chunk begin error", db: dbScript{beginErr: errors.New("begin fail")}, wantErr: true},
		{name: "remove chunk commit error", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{{ID: "c1", Status: "pending", Mode: "action"}, {ID: "c2", Status: "pending", Mode: "action"}}, execTag: "DELETE 1", commitErr: errors.New("commit fail")}, wantErr: true},
		{name: "update chunk success", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{{ID: "c1", Status: "pending", Repo: "aerial", Mode: "pr"}}, execTag: "UPDATE 1"}},
		{name: "update chunk not found", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "update chunk not pending", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{{ID: "c1", Status: "running", Repo: "aerial", Mode: "pr"}}}, wantErrIs: ErrBuildConflict},
		{name: "update chunk begin error", db: dbScript{beginErr: errors.New("begin fail")}, wantErr: true},
		{name: "update chunk commit error", db: dbScript{row: valuesRow("running"), chunks: []VanadisChunk{{ID: "c1", Status: "pending", Repo: "aerial", Mode: "pr"}}, execTag: "UPDATE 1", commitErr: errors.New("commit fail")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			switch tc.name {
			case "lookup pr chunk not found", "lookup pr chunk success":
				bID, _, err := store.LookupPRChunk(context.Background(), "aerial", 123)
				if !assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) && bID != tc.wantBuild {
					t.Errorf("LookupPRChunk = %q, want %q", bID, tc.wantBuild)
				}
			case "lookup chunk build id not found", "lookup chunk build id success":
				bID, err := store.LookupChunkBuildID(context.Background(), "c1")
				if !assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) && bID != tc.wantBuild {
					t.Errorf("LookupChunkBuildID = %q, want %q", bID, tc.wantBuild)
				}
			case "append chunks success", "append chunks build not found", "append chunks build not running", "append chunks duplicate id", "append chunks begin error", "append chunks commit error":
				inputID := ""
				if tc.name == "append chunks duplicate id" {
					inputID = "c-dup"
				}
				_, err := store.AppendChunks(context.Background(), "b1", []CreateChunkInput{{ID: inputID, Mode: "action", Title: "New Chunk", Instructions: "Inst", DependsOn: []string{}}})
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "remove chunk success", "remove chunk only chunk", "remove chunk not found", "remove chunk build not running", "remove chunk not pending", "remove chunk begin error", "remove chunk commit error":
				_, _, err := store.RemoveChunk(context.Background(), "b1", "c1")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case "update chunk success", "update chunk not found", "update chunk not pending", "update chunk begin error", "update chunk commit error":
				_, err := store.UpdateChunk(context.Background(), "b1", "c1", UpdateChunkRequest{Title: "Updated"})
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
	if id := generateBuildID(); !strings.HasPrefix(id, "build-") {
		t.Errorf("generateBuildID() = %q, want prefix 'build-'", id)
	}
}

func TestInitPostgres(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		url        string
		mockFail   bool
		pingFail   bool
		wantErr    bool
		wantSubstr string
	}{
		{name: "invalid url", url: "://invalid-url", wantErr: true, wantSubstr: "parse postgres url"},
		{name: "pool creation error", url: "postgres://user:pass@localhost:5432/db", mockFail: true, wantErr: true, wantSubstr: "create pgxpool"},
		{name: "ping error closes pool", url: "postgres://user:pass@localhost:5432/db", pingFail: true, wantErr: true, wantSubstr: "ping postgres"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closed := false
			origNew, origPing, origClose := newPgxPoolFn, pingPgxPoolFn, closePgxPoolFn
			defer func() { newPgxPoolFn, pingPgxPoolFn, closePgxPoolFn = origNew, origPing, origClose }()
			closePgxPoolFn = func(pool *pgxpool.Pool) {
				closed = true
				origClose(pool)
			}
			if tc.mockFail {
				newPgxPoolFn = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
					return nil, errors.New("mock pool fail")
				}
			}
			if tc.pingFail {
				pingPgxPoolFn = func(context.Context, *pgxpool.Pool) error {
					return errors.New("ping refused")
				}
			}
			_, _, err := initPostgres(ctx, tc.url)
			if (err != nil) != tc.wantErr {
				t.Fatalf("initPostgres() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
			}
			if tc.pingFail && !closed {
				t.Fatal("expected pool to be closed on ping failure")
			}
		})
	}
}

func TestValidateChunkModeAndRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		mode          string
		repo          string
		targetFiles   []string
		targetSymbols []string
		wantErr       bool
		wantSubstr    string
	}{
		{name: "invalid mode", mode: "invalid", repo: "aerial", wantErr: true, wantSubstr: "invalid chunk mode"},
		{name: "pr mode empty repo", mode: "pr", repo: "", wantErr: true, wantSubstr: "repo is required"},
		{name: "pr mode valid", mode: "pr", repo: "aerial", wantErr: false},
		{name: "action mode with repo", mode: "action", repo: "aerial", wantErr: true, wantSubstr: "action chunk must have empty repo"},
		{name: "action mode with target files", mode: "action", repo: "", targetFiles: []string{"a.go"}, wantErr: true, wantSubstr: "action chunk must not specify target_files"},
		{name: "action mode with target symbols", mode: "action", repo: "", targetSymbols: []string{"Sym"}, wantErr: true, wantSubstr: "action chunk must not specify target_symbols"},
		{name: "action mode valid", mode: "action", repo: "", wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateChunkModeAndRepo(0, tc.mode, tc.repo, tc.targetFiles, tc.targetSymbols)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestPostgresVanadisStore_StatusUpdates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		action    string
		id        string
		val       string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "chunk status nil db", action: "chunk", id: "c1", val: "running", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "chunk status empty id", action: "chunk", id: "", val: "running", wantErr: true},
		{name: "chunk status empty val", action: "chunk", id: "c1", val: "", wantErr: true},
		{name: "chunk status not found", action: "chunk", id: "c1", val: "running", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "chunk status db error", action: "chunk", id: "c1", val: "running", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("db error")}, wantErr: true},
		{name: "chunk status success", action: "chunk", id: "c1", val: "running", db: dbScript{execTag: "UPDATE 1"}},

		{name: "build status nil db", action: "build", id: "b1", val: "running", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "build status empty id", action: "build", id: "", val: "running", wantErr: true},
		{name: "build status not found", action: "build", id: "b1", val: "running", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrBuildNotFound},
		{name: "build status success", action: "build", id: "b1", val: "running", db: dbScript{execTag: "UPDATE 1"}},

		{name: "msg id nil db", action: "msg", id: "b1", val: "m1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "msg id empty id", action: "msg", id: "", val: "m1", wantErr: true},
		{name: "msg id not found", action: "msg", id: "b1", val: "m1", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrBuildNotFound},
		{name: "msg id success", action: "msg", id: "b1", val: "m1", db: dbScript{execTag: "UPDATE 1"}},

		{name: "reserve nil db", action: "reserve", id: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "reserve empty id", action: "reserve", id: "", wantErr: true},
		{name: "reserve db error", action: "reserve", id: "c1", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("db error")}, wantErr: true},
		{name: "reserve not found", action: "reserve", id: "c1", db: dbScript{execTag: "UPDATE 0"}, wantErrIs: ErrChunkNotFound},
		{name: "reserve success", action: "reserve", id: "c1", db: dbScript{execTag: "UPDATE 1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			var err error
			ctx := context.Background()
			switch tc.action {
			case "chunk":
				err = store.UpdateChunkStatus(ctx, tc.id, tc.val)
			case "build":
				err = store.UpdateBuildStatus(ctx, tc.id, tc.val)
			case "msg":
				err = store.UpdateBuildStatusMessageID(ctx, tc.id, tc.val)
			case "reserve":
				err = store.ReserveChunkForLaunch(ctx, tc.id)
			}
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, wantErrIs %v", err, tc.wantErrIs)
				}
				return
			}
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestPostgresVanadisStore_BannerAndLookups(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store, _ := dbScript{bare: nilDB}.open()
	if err := store.UpdateChunkBannerAndStatus(ctx, "c1", "status", "banner"); err == nil {
		t.Error("expected error with nilDB")
	}
	storeActive, _ := dbScript{execTag: "UPDATE 0"}.open()
	if err := storeActive.UpdateChunkBannerAndStatus(ctx, "", "status", "banner"); err == nil {
		t.Error("expected error with empty chunkID")
	}
	if _, err := store.GetChunkByPR(ctx, "aerial", 101); err == nil {
		t.Error("expected error with nil db")
	}
	if lookup, err := storeActive.GetChunkByPR(ctx, "", 101); err != nil || lookup.Matched {
		t.Errorf("expected unmatched with empty repo: %+v, %v", lookup, err)
	}
	if lookup, err := storeActive.GetChunkByPR(ctx, "aerial", 0); err != nil || lookup.Matched {
		t.Errorf("expected unmatched with 0 pr: %+v, %v", lookup, err)
	}
	storeFindErr, _ := dbScript{row: errRow(errors.New("db find err"))}.open()
	if _, err := storeFindErr.GetChunkByPR(ctx, "aerial", 101); err == nil {
		t.Error("expected error when findChunkPRMatch fails")
	}

	storeFailCancelledBuild, _ := dbScript{row: failRow("running", "cancelled"), execTag: "UPDATE 1"}.open()
	if _, _, failed, err := storeFailCancelledBuild.FailChunk(ctx, "c1", "reason", ""); err != nil || failed {
		t.Errorf("FailChunk on cancelled build: failed=%v, err=%v", failed, err)
	}
}

func TestPostgresVanadisStore_ParsingHelpers(t *testing.T) {
	t.Parallel()

	if eff, effErr := normalizeChunkEffort("low"); effErr != nil || eff != "low" {
		t.Errorf("normalizeChunkEffort(low) = (%q, %v)", eff, effErr)
	}
	if eff, effErr := normalizeChunkEffort("high"); effErr != nil || eff != "medium" {
		t.Errorf("normalizeChunkEffort(high) = (%q, %v)", eff, effErr)
	}
	if _, effErr := normalizeChunkEffort("invalid"); effErr == nil {
		t.Error("expected error for invalid effort")
	}
	if eff := capChunkEffort("invalid"); eff != "medium" {
		t.Errorf("capChunkEffort(invalid) = %q, want medium", eff)
	}
	if eff := capChunkEffort("low"); eff != "low" {
		t.Errorf("capChunkEffort(low) = %q, want low", eff)
	}
}

func TestPostgresVanadisStore_StatusAndRolloutUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storeNil, _ := dbScript{bare: nilDB}.open()
	if err := storeNil.RecordPRRolloutState(ctx, "c1", "open", nil); err == nil {
		t.Error("expected error with nil db")
	}
	storeActive, _ := dbScript{execTag: "UPDATE 0"}.open()
	if err := storeActive.RecordPRRolloutState(ctx, "", "open", nil); err == nil {
		t.Error("expected error with empty chunkID")
	}
	if err := storeActive.RecordPRRolloutState(ctx, "c1", "open", &pubsub.PRRolloutState{}); !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("expected ErrChunkNotFound, got %v", err)
	}
	storeRolloutOK, _ := dbScript{execTag: "UPDATE 1"}.open()
	if err := storeRolloutOK.RecordPRRolloutState(ctx, "c1", "open", &pubsub.PRRolloutState{}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if _, err := storeNil.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "New Title"}); err == nil {
		t.Error("expected error with nil db")
	}

	if _, err := storeNil.LookupChunkBuildID(ctx, "c1"); err == nil {
		t.Error("expected error with nil db")
	}
	if _, err := storeActive.LookupChunkBuildID(ctx, ""); err == nil {
		t.Error("expected error with empty chunkID")
	}

	storeExecErr, _ := dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("exec error")}.open()
	if err := storeExecErr.RecordPRRolloutState(ctx, "c1", "open", nil); err == nil {
		t.Error("expected error on exec failure")
	}
	if err := storeExecErr.UpdateChunkBannerAndStatus(ctx, "c1", "status", "banner"); err == nil {
		t.Error("expected error on exec failure")
	}
	if _, err := storeActive.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{}); err == nil {
		t.Error("expected error with empty request")
	}
	storeUpdateChunkNotFound, _ := dbScript{row: errRow(pgx.ErrNoRows)}.open()
	if _, err := storeUpdateChunkNotFound.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "New Title"}); !errors.Is(err, ErrBuildNotFound) {
		t.Errorf("expected ErrBuildNotFound, got %v", err)
	}
	storeUpdateChunkNotRunning, _ := dbScript{row: valuesRow("completed")}.open()
	if _, err := storeUpdateChunkNotRunning.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "New Title"}); !errors.Is(err, ErrBuildConflict) {
		t.Errorf("expected ErrBuildConflict, got %v", err)
	}

	if _, _, err := storeNil.RemoveChunk(ctx, "b1", "c1"); err == nil {
		t.Error("expected error with nil db")
	}
	if _, _, err := storeActive.RemoveChunk(ctx, "", "c1"); err == nil {
		t.Error("expected error with empty buildID")
	}
	if _, _, err := storeUpdateChunkNotFound.RemoveChunk(ctx, "b1", "c1"); !errors.Is(err, ErrBuildNotFound) {
		t.Errorf("expected ErrBuildNotFound, got %v", err)
	}
	if _, _, err := storeUpdateChunkNotRunning.RemoveChunk(ctx, "b1", "c1"); !errors.Is(err, ErrBuildConflict) {
		t.Errorf("expected ErrBuildConflict, got %v", err)
	}
}

func TestValidateAppendChunksInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		buildID    string
		inputs     []CreateChunkInput
		wantErr    bool
		wantSubstr string
	}{
		{
			name:       "empty build id",
			buildID:    "",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}},
			wantErr:    true,
			wantSubstr: "build id and at least one chunk are required",
		},
		{
			name:       "empty inputs",
			buildID:    "b1",
			inputs:     nil,
			wantErr:    true,
			wantSubstr: "build id and at least one chunk are required",
		},
		{
			name:       "invalid chunk mode",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Mode: "invalid"}},
			wantErr:    true,
			wantSubstr: "invalid chunk mode",
		},
		{
			name:       "empty chunk effort",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Effort: "gigantic"}},
			wantErr:    true,
			wantSubstr: "invalid chunk effort",
		},
		{
			name:       "empty chunk title",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "", Instructions: "I", Repo: "aerial"}},
			wantErr:    true,
			wantSubstr: "title and instructions cannot be empty",
		},
		{
			name:       "empty chunk instructions",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "", Repo: "aerial"}},
			wantErr:    true,
			wantSubstr: "title and instructions cannot be empty",
		},
		{
			name:    "valid inputs",
			buildID: "b1",
			inputs:  []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateAppendChunksInputs(tc.buildID, tc.inputs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestResolveChunkRef(t *testing.T) {
	t.Parallel()
	chunks := []VanadisChunk{
		{ID: "c-first", ChunkIndex: 0},
		{ID: "c-second", ChunkIndex: 1},
	}
	tests := []struct {
		ref  string
		want int
	}{
		{ref: "", want: -1},
		{ref: "c-first", want: 0},
		{ref: "c-second", want: 1},
		{ref: "0", want: 0},
		{ref: "1", want: 1},
		{ref: "5", want: -1},
		{ref: "c-unknown", want: -1},
	}
	for _, tc := range tests {
		t.Run(tc.ref, func(t *testing.T) {
			t.Parallel()
			if got := resolveChunkRef(chunks, tc.ref); got != tc.want {
				t.Errorf("resolveChunkRef(%q) = %d, want %d", tc.ref, got, tc.want)
			}
		})
	}
}

func TestRepoMatchCandidates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		repo string
		want []string
	}{
		{repo: "", want: nil},
		{repo: "aerial", want: []string{"aerial", "azylman/aerial"}},
		{repo: "azylman/aerial", want: []string{"azylman/aerial", "aerial"}},
		{repo: "other/repo", want: []string{"other/repo"}},
	}
	for _, tc := range tests {
		t.Run(tc.repo, func(t *testing.T) {
			t.Parallel()
			got := repoMatchCandidates(tc.repo)
			if !slices.Equal(got, tc.want) {
				t.Errorf("repoMatchCandidates(%q) = %v, want %v", tc.repo, got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore_UpdateBuild_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		buildID    string
		req        UpdateBuildRequest
		db         dbScript
		wantErr    bool
		wantErrIs  error
		wantSubstr string
	}{
		{
			name:       "empty input",
			buildID:    "b1",
			req:        UpdateBuildRequest{},
			wantErr:    true,
			wantSubstr: "build id and at least one non-empty field",
		},
		{
			name:       "empty build id",
			buildID:    "",
			req:        UpdateBuildRequest{Title: "Title"},
			wantErr:    true,
			wantSubstr: "build id and at least one non-empty field",
		},
		{
			name:      "build not found",
			buildID:   "b1",
			req:       UpdateBuildRequest{Title: "Title"},
			db:        dbScript{row: errRow(pgx.ErrNoRows)},
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build not running",
			buildID:    "b1",
			req:        UpdateBuildRequest{Title: "Title"},
			db:         dbScript{row: valuesRow("completed")},
			wantErrIs:  ErrBuildConflict,
			wantSubstr: "must be running",
		},
		{
			name:       "nil db",
			buildID:    "b1",
			req:        UpdateBuildRequest{Title: "Title"},
			db:         dbScript{bare: nilDB},
			wantErr:    true,
			wantSubstr: "database connection is nil",
		},
		{
			name:       "begin error",
			buildID:    "b1",
			req:        UpdateBuildRequest{Title: "Title"},
			db:         dbScript{beginErr: errors.New("begin fail")},
			wantErr:    true,
			wantSubstr: "begin update build tx",
		},
		{
			name:    "exec error",
			buildID: "b1",
			req:     UpdateBuildRequest{Title: "Title"},
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
				},
				failExec: "UPDATE vanadis_builds",
				execErr:  errors.New("exec fail"),
			},
			wantErr:    true,
			wantSubstr: "update build",
		},
		{
			name:    "commit error",
			buildID: "b1",
			req:     UpdateBuildRequest{Title: "Title"},
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
					{match: "SELECT id, thread_id", row: &mockRow{b: &VanadisBuild{ID: "b1", Title: "Updated Title", Status: "running"}}},
				},
				commitErr: errors.New("commit fail"),
			},
			wantErr:    true,
			wantSubstr: "commit update build tx",
		},
		{
			name:    "update build success",
			buildID: "b1",
			req:     UpdateBuildRequest{Title: "Updated Title", Objective: "Updated Obj", SharedContext: "Shared"},
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
					{match: "SELECT id, thread_id", row: &mockRow{b: &VanadisBuild{ID: "b1", Title: "Updated Title", Status: "running"}}},
				},
			},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			_, err := store.UpdateBuild(context.Background(), tc.buildID, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("UpdateBuild err = %v, wantErrIs %v", err, tc.wantErrIs)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestInsertBuildChunk_ModesAndValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		chunk      VanadisChunk
		failExec   string
		wantErr    bool
		wantErrSub string
	}{
		{
			name:       "pr mode missing repo",
			chunk:      VanadisChunk{ID: "c1", Mode: "pr", Repo: ""},
			wantErr:    true,
			wantErrSub: "repo is required for pr mode",
		},
		{
			name:       "action mode with non-empty repo",
			chunk:      VanadisChunk{ID: "c1", Mode: "action", Repo: "aerial"},
			wantErr:    true,
			wantErrSub: "action chunk must have empty repo",
		},
		{
			name:       "action mode with target files",
			chunk:      VanadisChunk{ID: "c1", Mode: "action", TargetFiles: []string{"a.go"}},
			wantErr:    true,
			wantErrSub: "action chunk must not specify target files or symbols",
		},
		{
			name:       "invalid effort",
			chunk:      VanadisChunk{ID: "c1", Mode: "action", Effort: "gigantic"},
			wantErr:    true,
			wantErrSub: "invalid chunk effort",
		},
		{
			name:     "insert exec error",
			chunk:    VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Status: "running", PRNumber: 123},
			failExec: "INSERT INTO vanadis_chunks",
			wantErr:  true,
		},
		{
			name:    "insert success with completed status and action mode",
			chunk:   VanadisChunk{ID: "c1", Mode: "action", Status: "completed"},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &mockTx{
				execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
					if tc.failExec != "" && strings.Contains(sql, tc.failExec) {
						return pgconn.CommandTag{}, errors.New("exec error")
					}
					return pgconn.NewCommandTag("INSERT 1"), nil
				},
			}
			err := insertBuildChunk(context.Background(), tx, "b1", tc.chunk, "INSERT INTO vanadis_chunks VALUES (...)")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
				}
				if tc.wantErrSub != "" && !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCompletePRDeployment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, chunkID                string
		nilDB, evidence, execFailure bool
		wantErr                      bool
		rejected                     bool
	}{
		{"nil database", "c1", true, false, false, true, false},
		{"empty id", "", false, false, false, true, false},
		{"rejected update", "c1", false, false, false, true, true},
		{"nil evidence", "c1", false, false, false, false, false},
		{"supplied evidence", "c1", false, true, false, false, false},
		{"persistence failure", "c1", false, true, true, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotArgs []any
			store := newMockStore(mockDB{execFn: func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				gotArgs = args
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				if tc.rejected {
					return pgconn.NewCommandTag("UPDATE 0"), nil
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}})
			if tc.nilDB {
				store = &PostgresVanadisStore{}
			}
			var evidence *pubsub.PRRolloutState
			if tc.evidence {
				evidence = &pubsub.PRRolloutState{ExpectedJobs: []string{"service"}}
			}
			err := store.CompletePRDeployment(context.Background(), tc.chunkID, evidence)
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.wantErr {
				return
			}

			if tc.evidence {
				data, ok := gotArgs[0].([]byte)
				assert.True(t, ok)
				assert.Contains(t, string(data), "service")
			} else {
				assert.Nil(t, gotArgs[0])
			}
		})
	}
}

func TestUpdatePRLifecycleStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, id, tag      string
		nilDB, execFailure bool
		wantErr            bool
	}{
		{"nil database", "c1", "UPDATE 1", true, false, true},
		{"empty id", "", "UPDATE 1", false, false, true},
		{"eligible merge", "c1", "UPDATE 1", false, false, false},
		{"terminal chunk", "c1", "UPDATE 0", false, false, true},
		{"persistence failure", "c1", "", false, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
				if tc.execFailure {
					return pgconn.CommandTag{}, errors.New("database unavailable")
				}
				return pgconn.NewCommandTag(tc.tag), nil
			}})
			if tc.nilDB {
				store = &PostgresVanadisStore{}
			}
			err := store.UpdatePRLifecycleStatus(context.Background(), tc.id, "merged")
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.tag == "UPDATE 0" {
				assert.ErrorIs(t, err, errPRTransitionSkipped)
			}
		})
	}
}

func isActionRow(row pgx.Row) bool { _, ok := row.(*genericMockRow); return ok }
