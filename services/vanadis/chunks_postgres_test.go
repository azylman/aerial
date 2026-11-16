package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// bareStore pins a store value with no usable database handle (nil receiver or nil db).
type bareStore struct{ store *PostgresVanadisStore }

// dbScript declaratively describes how a mocked Postgres connection answers a store call.
// The same answers are served on the pool and inside transactions opened by Begin.
type dbScript struct {
	bare        *bareStore // when set, the test calls bare.store instead of a scripted store
	beginErr    error
	routes      []sqlRoute // checked in order before countRow/buildRow/row; the first matching route answers QueryRow
	row         pgx.Row    // answers QueryRow (all of them when buildRow is nil)
	buildRow    pgx.Row    // when set, answers QueryRow whose SQL does not select FROM vanadis_chunks
	countRow    pgx.Row    // when set, answers QueryRow whose SQL contains COUNT(*)
	chunks      []VanadisChunk
	queryErr    error
	scanErr     error
	rowsErr     error
	failExec    string // Exec whose SQL contains this substring returns execErr ("" matches every Exec)
	execErr     error
	execTag     string // command tag for successful Exec; defaults to "UPDATE 1"
	commitErr   error
	rollbackErr error
}

// sqlRoute answers every QueryRow whose SQL contains match with row.
type sqlRoute struct {
	match string
	row   pgx.Row
}

// open builds the store under test and returns the repo candidates recorded from QueryRow's first []string argument.
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
		return &mockRows{items: s.chunks, scanErr: s.scanErr, rowsErr: s.rowsErr}, nil
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

func buildRow(id, status string) *mockRow { return &mockRow{b: &VanadisBuild{ID: id, Status: status}} }

func errRow(err error) *mockRow { return &mockRow{err: err} }

// actionChunkRow answers CompleteActionChunk's chunk select for chunk c1 of build b1.
func actionChunkRow(mode, cStatus string) *genericMockRow {
	return valuesRow("c1", "b1", 0, mode, "Action Chunk", cStatus, 0, 0)
}

// actionBuildRow answers CompleteActionChunk's build select for build b1.
func actionBuildRow(bStatus string) *genericMockRow {
	return valuesRow("b1", "thread-1", 2, "Build 1", "Objective 1", bStatus, "msg-1")
}

// failRow answers FailChunk's joined chunk+build select for action chunk c1 of build b1.
func failRow(cStatus, bStatus string, generation ...int) *genericMockRow {
	nudges, prNumber := 0, 0
	if len(generation) > 0 {
		nudges = generation[0]
	}
	if len(generation) > 1 {
		prNumber = generation[1]
	}
	return &genericMockRow{scanFn: func(dest ...any) error {
		now := time.Now()
		return scanValues(dest, "c1", "b1", 0, "action", "", "Fail Chunk Title", "Instructions", []string{}, []string{}, "medium", []string{}, cStatus, prNumber, sql.NullTime{Time: now, Valid: true}, sql.NullTime{Time: now, Valid: true}, now, now, nudges, "b1", "thread-1", 2, "Build 1", "Objective 1", "Context", bStatus, "msg-1", now, now)
	}}
}

// idPairRow answers FailChunkByPR's RETURNING build_id, id.
func idPairRow(buildID, chunkID string) *genericMockRow {
	return valuesRow(buildID, chunkID)
}

// assertStoreErr checks err against a row's expectation and reports whether an error was expected.
// wantErrIs implies an error; wantErr accepts any non-nil error.
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

// assertCandidates compares recorded repo candidates; it is a no-op when want is nil.
func assertCandidates(t *testing.T, got *[]string, want []string) {
	t.Helper()
	if want != nil && !slices.Equal(*got, want) {
		t.Errorf("repo candidates = %v, want %v", *got, want)
	}
}

// advanceOutcome is the observable projection of a VanadisAdvancementResult.
type advanceOutcome struct {
	Matched, Advanced, BuildCompleted bool
	NextChunkIndex                    int
	BuildStatus                       string
	Chunks                            int
}

func outcomeOf(res *VanadisAdvancementResult) advanceOutcome {
	return advanceOutcome{Matched: res.Matched, Advanced: res.Advanced, BuildCompleted: res.BuildCompleted, NextChunkIndex: res.NextChunkIndex, BuildStatus: res.BuildStatus, Chunks: len(res.Chunks)}
}

func TestPostgresVanadisStore_GetChunk(t *testing.T) {
	t.Parallel()
	c1 := []VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk Title"}}
	tests := []struct {
		name        string
		chunkID     string
		db          dbScript
		wantErrIs   error
		wantErr     bool
		wantChunkID string
		wantBuildID string
	}{
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "query error", chunkID: "c1", db: dbScript{queryErr: errors.New("query err")}, wantErr: true},
		{name: "rows error", chunkID: "c1", db: dbScript{chunks: c1, rowsErr: errors.New("rows err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{chunks: []VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "build not found", chunkID: "c1", db: dbScript{chunks: c1, row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build query error", chunkID: "c1", db: dbScript{chunks: c1, row: errRow(errors.New("build query err"))}, wantErr: true},
		{name: "success", chunkID: "c1", db: dbScript{chunks: c1, row: &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build Title"}}}, wantChunkID: "c1", wantBuildID: "b1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			c, b, err := store.GetChunk(context.Background(), tc.chunkID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if c.ID != tc.wantChunkID || b.ID != tc.wantBuildID {
				t.Errorf("GetChunk = chunk %q build %q, want chunk %q build %q", c.ID, b.ID, tc.wantChunkID, tc.wantBuildID)
			}
		})
	}
}

// attachRoutes answers attachToExistingChunk's mode select and resolveAttachChunkPRError's
// existence and cancelled/completed probes; the RETURNING update falls through to dbScript.row.
func attachRoutes(mode string, exists, cancelled bool) []sqlRoute {
	return []sqlRoute{
		{match: "SELECT mode", row: valuesRow(mode)},
		{match: "c.status = 'cancelled'", row: valuesRow(cancelled)},
		{match: "SELECT EXISTS", row: valuesRow(exists)},
	}
}

// attachResult is the observable (buildID, chunkID, threadID) triple AttachChunkPR returns.
type attachResult struct{ BuildID, ChunkID, ThreadID string }

func TestPostgresVanadisStore_AttachChunkPR(t *testing.T) {
	t.Parallel()
	var (
		errModeQuery = errors.New("attach db err")
		errLookup    = errors.New("lookup err")
		errBegin     = errors.New("begin standalone tx err")
		errLock      = errors.New("advisory lock boom")
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
		wantNewBuild    bool // standalone build was minted: ChunkID is BuildID + "-chunk-1" and BuildID is a generated build-* id
		wantCandidates  []string
	}{
		{name: "nil receiver", req: existing, db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", req: existing, db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id and zero pr number", req: AttachChunkPRRequest{Repo: "aerial"}, wantErr: true},
		{name: "chunk not found", req: existing, db: dbScript{row: noRows}, wantErrIs: ErrChunkNotFound},
		{name: "mode query error", req: existing, db: dbScript{row: errRow(errModeQuery)}, wantErrIs: errModeQuery},
		{name: "action chunk requires repo", req: attachRequest("c1", ""), db: dbScript{routes: attachRoutes("action", true, false), row: noRows}, wantErrContains: "repo is required"},
		{name: "mismatched repo", req: existing, db: dbScript{routes: attachRoutes("pr", true, false), row: noRows}, wantErrContains: "mismatched repo"},
		{name: "cancelled chunk or build with repo conflicts", req: existing, db: dbScript{routes: attachRoutes("pr", true, true), row: noRows}, wantErrIs: ErrBuildConflict},
		{name: "cancelled chunk or build without repo conflicts", req: attachRequest("c1", ""), db: dbScript{routes: attachRoutes("pr", true, true), row: noRows}, wantErrIs: ErrBuildConflict},
		{name: "attaches pr chunk without repo", req: attachRequest("c-pr", ""), db: dbScript{routes: attachRoutes("pr", true, false), row: attached("pr")}, want: attachResult{"b-pr", "c-pr", "thread-pr"}},
		{name: "attaches pr chunk matching repo candidates", req: attachRequest("c-pr", "aerial"), db: dbScript{routes: attachRoutes("pr", true, false), row: attached("pr")}, want: attachResult{"b-pr", "c-pr", "thread-pr"}, wantCandidates: aerialRepoSet},
		{name: "attaches action chunk", req: attachRequest("c-act", "aerial"), db: dbScript{routes: attachRoutes("action", true, false), row: attached("action")}, want: attachResult{"b-action", "c-action", "thread-action"}},
		{name: "standalone requires repo", req: attachRequest("", ""), wantErrContains: "repo is required"},
		{name: "standalone begin error", req: standalone, db: dbScript{beginErr: errBegin}, wantErrIs: errBegin},
		{name: "standalone advisory lock error", req: standalone, db: dbScript{row: noRows, failExec: "pg_advisory_xact_lock", execErr: errLock}, wantErrIs: errLock},
		{name: "standalone existing pr lookup error", req: standalone, db: dbScript{row: errRow(errLookup)}, wantErrIs: errLookup},
		{name: "standalone reuses existing pr chunk", req: standalone, db: dbScript{row: valuesRow("b-existing", "c-existing", "thread-existing")}, want: attachResult{"b-existing", "c-existing", "thread-existing"}, wantCandidates: aerialRepoSet},
		{name: "standalone insert build error", req: standalone, db: dbScript{row: noRows, failExec: "INSERT INTO vanadis_builds", execErr: errBuildIns}, wantErrIs: errBuildIns},
		{name: "standalone insert chunk error", req: standalone, db: dbScript{row: noRows, failExec: "INSERT INTO vanadis_chunks", execErr: errChunkIns}, wantErrIs: errChunkIns},
		{name: "standalone commit error", req: standalone, db: dbScript{row: noRows, commitErr: errCommit}, wantErrIs: errCommit},
		{name: "standalone creates build in target thread", req: AttachChunkPRRequest{PRNumber: 101, Repo: "aerial", TargetID: "123456789012345678", Branch: "dependabot/npm_and_yarn/foo", Title: "Bump foo"}, db: dbScript{row: noRows}, want: attachResult{ThreadID: "123456789012345678"}, wantNewBuild: true},
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

// attachRequest builds an AttachChunkPR request for PR #101.
func attachRequest(chunkID, repo string) AttachChunkPRRequest {
	return AttachChunkPRRequest{ChunkID: chunkID, PRNumber: 101, Repo: repo}
}

func TestPostgresVanadisStore_FailChunkByPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		repo           string
		prNumber       int
		db             dbScript
		wantErr        bool
		wantFailed     bool
		wantBuildID    string
		wantChunkID    string
		wantCandidates []string
	}{
		{name: "nil receiver", repo: "aerial", prNumber: 1, db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", repo: "aerial", prNumber: 1, db: dbScript{bare: nilDB}, wantErr: true},
		{name: "invalid args do not fail", repo: "", prNumber: 0},
		{name: "no rows do not fail", repo: "aerial", prNumber: 101, db: dbScript{row: errRow(pgx.ErrNoRows)}},
		{name: "query row error", repo: "aerial", prNumber: 101, db: dbScript{row: errRow(errors.New("fail query err"))}, wantErr: true},
		{name: "update build error", repo: "aerial", prNumber: 101, db: dbScript{row: idPairRow("b1", "c1"), failExec: "UPDATE vanadis_builds", execErr: errors.New("update build err")}, wantErr: true},
		{name: "fails matched chunk", repo: "aerial", prNumber: 101, db: dbScript{row: idPairRow("b1", "c1")}, wantFailed: true, wantBuildID: "b1", wantChunkID: "c1", wantCandidates: aerialRepoSet},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, candidates := tc.db.open()
			bID, cID, failed, err := store.FailChunkByPR(context.Background(), tc.repo, tc.prNumber)
			if assertStoreErr(t, err, nil, tc.wantErr) {
				return
			}
			if failed != tc.wantFailed || bID != tc.wantBuildID || cID != tc.wantChunkID {
				t.Errorf("FailChunkByPR = (%q, %q, %v), want (%q, %q, %v)", bID, cID, failed, tc.wantBuildID, tc.wantChunkID, tc.wantFailed)
			}
			assertCandidates(t, candidates, tc.wantCandidates)
		})
	}
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
		wantPRNum  int
	}{
		{name: "matching retried identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: -1, BuildStatus: "running"}},
		{name: "missing retried identity", chunkID: "c1", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: -1, BuildStatus: "running"}},
		{name: "stale retried identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: valuesRow("c1", "b1", 0, "action", "Action Chunk", "running", 0, 1), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: -1, BuildStatus: "running"}},

		{name: "remaining count error", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: errRow(errors.New("count failed"))}, wantErr: true},
		{name: "build persistence error", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0), failExec: "UPDATE vanadis_builds", execErr: errors.New("write failed")}, wantErr: true},
		{name: "commit error", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(1), commitErr: errors.New("commit failed")}, wantErr: true},
		{name: "cancelled parent", chunkID: "c1", db: dbScript{row: running, buildRow: actionBuildRow("cancelled")}, want: advanceOutcome{Matched: true, NextChunkIndex: -1, BuildStatus: "cancelled"}},

		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{beginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{row: errRow(errors.New("select chunk err"))}, wantErr: true},
		{name: "select build error", chunkID: "c1", db: dbScript{row: running, buildRow: errRow(errors.New("db build error"))}, wantErr: true},
		{name: "pr mode chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("pr", "running"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: -1, BuildStatus: "running"}},
		{name: "completed action chunk is a matched no-op", chunkID: "c1", db: dbScript{row: actionChunkRow("action", "completed"), buildRow: runningBuild}, want: advanceOutcome{Matched: true, NextChunkIndex: -1, BuildStatus: "running"}},
		{name: "update chunk error", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, failExec: "UPDATE vanadis_chunks", execErr: errors.New("update action chunk err")}, wantErr: true},
		{name: "update chunk error logs rollback error", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, failExec: "UPDATE vanadis_chunks", execErr: errors.New("update action chunk err"), rollbackErr: errors.New("rb err")}, wantErr: true},
		{name: "completes action chunk and marks build completed when no remaining chunks", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(0)}, want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: -1, BuildStatus: "completed"}},
		{
			name:    "completes action chunk and populates chunk summaries when build completed",
			chunkID: "c1",
			db: dbScript{
				row:      running,
				buildRow: runningBuild,
				countRow: valuesRow(0),
				chunks: []VanadisChunk{
					{ID: "c0", ChunkIndex: 0, Mode: "pr", Repo: "azylman/aerial", Title: "PR Chunk", Status: "completed", PRNumber: 101, StartedAt: time.Now(), CompletedAt: time.Now(), UpdatedAt: time.Now()},
					{ID: "c1", ChunkIndex: 1, Mode: "action", Title: "Action Chunk", Status: "completed", StartedAt: time.Now(), CompletedAt: time.Now(), UpdatedAt: time.Now()},
				},
			},
			want:      advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: -1, BuildStatus: "completed", Chunks: 2},
			wantPRNum: 101,
		},
		{
			name:    "completes action chunk with summary query error falls back to nil chunks",
			chunkID: "c1",
			db: dbScript{
				row:      running,
				buildRow: runningBuild,
				countRow: valuesRow(0),
				queryErr: errors.New("chunks query fail"),
			},
			want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: -1, BuildStatus: "completed", Chunks: 0},
		},
		{
			name:    "completes action chunk with summary scan error falls back to nil chunks",
			chunkID: "c1",
			db: dbScript{
				row:      running,
				buildRow: runningBuild,
				countRow: valuesRow(0),
				chunks:   []VanadisChunk{{ID: "c1"}},
				scanErr:  errors.New("scan fail"),
			},
			want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: -1, BuildStatus: "completed", Chunks: 0},
		},
		{
			name:    "completes action chunk with summary rows error falls back to nil chunks",
			chunkID: "c1",
			db: dbScript{
				row:      running,
				buildRow: runningBuild,
				countRow: valuesRow(0),
				chunks:   []VanadisChunk{{ID: "c1"}},
				rowsErr:  errors.New("rows fail"),
			},
			want: advanceOutcome{Matched: true, Advanced: true, BuildCompleted: true, NextChunkIndex: -1, BuildStatus: "completed", Chunks: 0},
		},
		{name: "completes action chunk", chunkID: "c1", db: dbScript{row: running, buildRow: runningBuild, countRow: valuesRow(1)}, want: advanceOutcome{Matched: true, Advanced: true, NextChunkIndex: -1, BuildStatus: "running"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			res, err := store.CompleteActionChunk(context.Background(), tc.chunkID, tc.dispatchID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if got := outcomeOf(res); got != tc.want {
				t.Errorf("CompleteActionChunk = %+v, want %+v", got, tc.want)
			}
			if tc.wantPRNum > 0 && (len(res.Chunks) == 0 || res.Chunks[0].PRNumber != tc.wantPRNum) {
				t.Errorf("res.Chunks[0].PRNumber = %v, want %d", res.Chunks, tc.wantPRNum)
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
		dispatchID string
		db         dbScript
		wantErrIs  error
		wantErr    bool
		wantFailed bool
	}{
		{name: "attached PR CI remediation failure", chunkID: "c1", dispatchID: "vanadis-c1-cifail-head-1", db: dbScript{row: failRow("ci_failed", "running", 0, 101)}, wantFailed: true},
		{name: "attached PR conflict failure", chunkID: "c1", dispatchID: "vanadis-conflict-c1-head", db: dbScript{row: failRow("conflict", "running", 1, 101)}, wantFailed: true},
		{name: "attached PR direct failure compatibility", chunkID: "c1", db: dbScript{row: failRow("ci_failed", "running", 1, 101)}, wantFailed: true},
		{name: "attached PR stale action turn", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: failRow("ci_failed", "running", 1, 101)}},
		{name: "unattached PR rejects remediation", chunkID: "c1", dispatchID: "vanadis-c1-cifail-head-1", db: dbScript{row: failRow("running", "running")}},
		{name: "retry missing identity", chunkID: "c1", db: dbScript{row: failRow("running", "running", 1)}},
		{name: "retry old identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: failRow("running", "running", 1)}},
		{name: "retry current identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{row: failRow("running", "running", 1)}, wantFailed: true},
		{name: "cancelled parent", chunkID: "c1", dispatchID: "vanadis-c1-turn-1", db: dbScript{row: failRow("running", "cancelled", 1)}},
		{name: "parent lock error", chunkID: "c1", db: dbScript{routes: []sqlRoute{{match: "SELECT id FROM vanadis_builds", row: errRow(errors.New("lock error"))}}}, wantErr: true},
		{name: "stale identity", chunkID: "c1", dispatchID: "old", db: dbScript{row: running}},
		{name: "matching identity", chunkID: "c1", dispatchID: "vanadis-c1-turn-0", db: dbScript{row: running}, wantFailed: true},
		{name: "nil receiver", chunkID: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", chunkID: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty chunk id", chunkID: "", wantErr: true},
		{name: "whitespace chunk id", chunkID: "  ", wantErr: true},
		{name: "begin error", chunkID: "c1", db: dbScript{beginErr: errors.New("begin tx err")}, wantErr: true},
		{name: "chunk not found", chunkID: "c1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrChunkNotFound},
		{name: "select chunk error", chunkID: "c1", db: dbScript{row: errRow(errors.New("select chunk fail err"))}, wantErr: true},
		{name: "already terminal chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("failed", "failed")}},
		{name: "deploying chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("deploying", "running")}},
		{name: "merged chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("merged", "running")}},
		{name: "completed chunk is a no-op", chunkID: "c1", db: dbScript{row: failRow("completed", "running")}},
		{name: "update chunk error", chunkID: "c1", db: dbScript{row: running, failExec: "UPDATE vanadis_chunks SET status = 'failed'", execErr: errors.New("update chunk fail err")}, wantErr: true},
		{name: "update build error", chunkID: "c1", db: dbScript{row: running, failExec: "UPDATE vanadis_builds SET status = 'failed'", execErr: errors.New("update build fail err")}, wantErr: true},
		{name: "commit error", chunkID: "c1", db: dbScript{row: running, commitErr: errors.New("commit fail err")}, wantErr: true},
		{name: "fails running chunk and build", chunkID: "c1", db: dbScript{row: running}, wantFailed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.db.routes = append(tc.db.routes, sqlRoute{match: "SELECT id FROM vanadis_builds", row: valuesRow("b1")})
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

type genericMockRow struct {
	scanFn func(dest ...any) error
}

// valuesRow is a pgx.Row whose Scan assigns vals positionally via scanValues.
func valuesRow(vals ...any) *genericMockRow {
	return &genericMockRow{scanFn: func(dest ...any) error { return scanValues(dest, vals...) }}
}

// scanValues assigns vals positionally into Scan destinations and, like a real
// pgx Scan, returns an error when a destination cannot hold its value instead
// of panicking. A nil val leaves that destination untouched.
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

func TestPostgresVanadisStore_UpdateBuild(t *testing.T) {
	t.Parallel()
	title := UpdateBuildRequest{Title: "T"}
	running := buildRow("b1", "running")
	tests := []struct {
		name      string
		buildID   string
		req       UpdateBuildRequest
		db        dbScript
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil receiver", buildID: "b1", req: title, db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", buildID: "b1", req: title, db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty_build_id", buildID: "", req: title, wantErrIs: ErrInvalidBuildInput},
		{name: "all_empty_fields", buildID: "b1", req: UpdateBuildRequest{Title: "   "}, wantErrIs: ErrInvalidBuildInput},
		{name: "begin_error", buildID: "b1", req: title, db: dbScript{beginErr: errors.New("begin err")}, wantErr: true},
		{name: "build_not_found", buildID: "b1", req: title, db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build_query_error", buildID: "b1", req: title, db: dbScript{row: errRow(errors.New("query err"))}, wantErr: true},
		{name: "build_conflict_status", buildID: "b1", req: title, db: dbScript{row: buildRow("b1", "completed")}, wantErrIs: ErrBuildConflict},
		{name: "exec_error", buildID: "b1", req: title, db: dbScript{row: running, execErr: errors.New("exec err")}, wantErr: true},
		{name: "exec_error_with_rollback_err", buildID: "b1", req: title, db: dbScript{row: running, execErr: errors.New("exec err"), rollbackErr: errors.New("rb err")}, wantErr: true},
		{name: "commit_error", buildID: "b1", req: title, db: dbScript{row: running, commitErr: errors.New("commit err")}, wantErr: true},
		{name: "success", buildID: "b1", req: UpdateBuildRequest{Title: "New Title", Objective: "New Obj", SharedContext: "New Ctx"}, db: dbScript{row: running}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			got, err := store.UpdateBuild(context.Background(), tc.buildID, tc.req)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if got == nil || got.Title != tc.req.Title || got.Objective != tc.req.Objective || got.SharedContext != tc.req.SharedContext {
				t.Fatalf("updated build = %+v, want title/objective/shared_context from %+v", got, tc.req)
			}
		})
	}
}

// pendingChunks is a one-line fixture of chunks c1..cN at indexes 0..N-1 with the given statuses.
func pendingChunks(statuses ...string) []VanadisChunk {
	chunks := make([]VanadisChunk, 0, len(statuses))
	for i, status := range statuses {
		chunks = append(chunks, VanadisChunk{ID: "c" + string(rune('1'+i)), ChunkIndex: i, Status: status})
	}
	return chunks
}

// updatedChunkView is the observable projection of an updated chunk.
type updatedChunkView struct {
	Mode, Repo, Title string
	Files, Symbols    int
}

func TestPostgresVanadisStore_UpdateChunk(t *testing.T) {
	t.Parallel()
	modeBad, modePR, modeAction := "bad-mode", "pr", "action"
	title := UpdateChunkRequest{Title: "T"}
	running := buildRow("b1", "running")
	pendingC1 := pendingChunks("pending")
	actionC1 := []VanadisChunk{{ID: "c1", Mode: "action", Status: "pending"}}
	tests := []struct {
		name      string
		buildID   string
		chunkRef  string
		req       UpdateChunkRequest
		db        dbScript
		wantErrIs error
		wantErr   bool
		want      updatedChunkView
	}{
		{name: "nil receiver", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty_build_id", buildID: "", chunkRef: "c1", req: title, wantErrIs: ErrInvalidBuildInput},
		{name: "empty_chunk_ref", buildID: "b1", chunkRef: "", req: title, wantErrIs: ErrInvalidBuildInput},
		{name: "no_fields", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{}, wantErrIs: ErrInvalidBuildInput},
		{name: "invalid_effort", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{Effort: "extreme"}, wantErrIs: ErrInvalidBuildInput},
		{name: "invalid_mode", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{Mode: &modeBad}, wantErrIs: ErrInvalidBuildInput},
		{name: "begin_error", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{beginErr: errors.New("begin err")}, wantErr: true},
		{name: "build_not_found", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build_query_error", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: errRow(errors.New("query err"))}, wantErr: true},
		{name: "build_conflict_status", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: buildRow("b1", "completed")}, wantErrIs: ErrBuildConflict},
		{name: "chunks_query_error", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: running, queryErr: errors.New("chunks query err")}, wantErr: true},
		{name: "chunks_scan_error", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1"}}, scanErr: errors.New("chunks scan err")}, wantErr: true},
		{name: "chunk_not_found", buildID: "b1", chunkRef: "c999", req: title, db: dbScript{row: running, chunks: pendingC1}, wantErrIs: ErrChunkNotFound},
		{name: "chunk_conflict_status", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: running, chunks: pendingChunks("running")}, wantErrIs: ErrBuildConflict},
		{name: "exec_error", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: running, chunks: pendingC1, execErr: errors.New("exec err")}, wantErr: true},
		{name: "exec_error_with_rollback_err", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: running, chunks: pendingC1, execErr: errors.New("exec err"), rollbackErr: errors.New("rb err")}, wantErr: true},
		{name: "commit_error", buildID: "b1", chunkRef: "c1", req: title, db: dbScript{row: running, chunks: pendingC1, commitErr: errors.New("commit err")}, wantErr: true},
		{name: "pr_mode_empty_repo", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{Mode: &modePR}, db: dbScript{row: running, chunks: actionC1}, wantErrIs: ErrInvalidBuildInput},
		{name: "action_mode_with_repo", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{Mode: &modeAction, Repo: "aerial"}, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Status: "pending"}}}, wantErrIs: ErrInvalidBuildInput},
		{name: "action_mode_with_files", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{TargetFiles: []string{"a.go"}}, db: dbScript{row: running, chunks: actionC1}, wantErrIs: ErrInvalidBuildInput},
		{name: "action_mode_with_symbols", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{TargetSymbols: []string{"A"}}, db: dbScript{row: running, chunks: actionC1}, wantErrIs: ErrInvalidBuildInput},
		{name: "dag_cycle_error", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{DependsOn: &[]string{"c2"}}, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1", Mode: "action", Status: "pending"}, {ID: "c2", Mode: "action", DependsOn: []string{"c1"}, Status: "pending"}}}, wantErr: true},
		{name: "transition_pr_to_action_mode_clears_repo_files_symbols", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{Mode: &modeAction}, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", TargetFiles: []string{"a.go"}, TargetSymbols: []string{"A"}, Status: "pending"}}}, want: updatedChunkView{Mode: "action"}},
		{name: "success", buildID: "b1", chunkRef: "c1", req: UpdateChunkRequest{Repo: "mirrormere", Title: "New Title", Instructions: "New Instr", Effort: "low", TargetFiles: []string{"a.go"}, TargetSymbols: []string{"A"}}, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Status: "pending"}}}, want: updatedChunkView{Mode: "pr", Repo: "mirrormere", Title: "New Title", Files: 1, Symbols: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			got, err := store.UpdateChunk(context.Background(), tc.buildID, tc.chunkRef, tc.req)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			view := updatedChunkView{Mode: got.Mode, Repo: got.Repo, Title: got.Title, Files: len(got.TargetFiles), Symbols: len(got.TargetSymbols)}
			if view != tc.want {
				t.Errorf("UpdateChunk = %+v, want %+v", view, tc.want)
			}
		})
	}
}

// chunkInput is a one-line CreateChunkInput fixture.
func chunkInput(id, mode, repo, title, instructions string) CreateChunkInput {
	return CreateChunkInput{ID: id, Mode: mode, Repo: repo, Title: title, Instructions: instructions}
}

func TestPostgresVanadisStore_AppendChunks(t *testing.T) {
	t.Parallel()
	one := []CreateChunkInput{chunkInput("", "", "aerial", "T", "I")}
	running := buildRow("b1", "running")
	cycleA, cycleB := chunkInput("c2", "", "aerial", "T", "I"), chunkInput("c3", "", "aerial", "T", "I")
	cycleA.DependsOn, cycleB.DependsOn = []string{"c3"}, []string{"c2"}
	badEffort := chunkInput("", "", "aerial", "T", "I")
	badEffort.Effort = "super"
	withFiles, withSymbols := chunkInput("", "action", "", "T", "I"), chunkInput("", "action", "", "T", "I")
	withFiles.TargetFiles, withSymbols.TargetSymbols = []string{"a.go"}, []string{"A"}
	tests := []struct {
		name      string
		buildID   string
		inputs    []CreateChunkInput
		db        dbScript
		wantErrIs error
		wantErr   bool
		wantCount int
	}{
		{name: "nil receiver", buildID: "b1", inputs: one, db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", buildID: "b1", inputs: one, db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty_build_id", buildID: "", inputs: []CreateChunkInput{chunkInput("", "", "", "T", "I")}, wantErrIs: ErrInvalidBuildInput},
		{name: "empty_inputs", buildID: "b1", inputs: nil, wantErrIs: ErrInvalidBuildInput},
		{name: "empty_title", buildID: "b1", inputs: []CreateChunkInput{chunkInput("", "", "", "", "I")}, wantErrIs: ErrInvalidBuildInput},
		{name: "empty_instructions", buildID: "b1", inputs: []CreateChunkInput{chunkInput("", "", "", "T", "")}, wantErrIs: ErrInvalidBuildInput},
		{name: "invalid_id", buildID: "b1", inputs: []CreateChunkInput{chunkInput("bad:id", "", "", "T", "I")}, wantErrIs: ErrInvalidBuildInput},
		{name: "invalid_effort", buildID: "b1", inputs: []CreateChunkInput{badEffort}, wantErrIs: ErrInvalidBuildInput},
		{name: "invalid_mode", buildID: "b1", inputs: []CreateChunkInput{chunkInput("", "bad", "", "T", "I")}, wantErrIs: ErrInvalidBuildInput},
		{name: "pr_mode_empty_repo", buildID: "b1", inputs: []CreateChunkInput{chunkInput("", "pr", "", "T", "I")}, wantErrIs: ErrInvalidBuildInput},
		{name: "action_mode_with_repo", buildID: "b1", inputs: []CreateChunkInput{chunkInput("", "action", "aerial", "T", "I")}, wantErrIs: ErrInvalidBuildInput},
		{name: "action_mode_with_files", buildID: "b1", inputs: []CreateChunkInput{withFiles}, wantErrIs: ErrInvalidBuildInput},
		{name: "action_mode_with_symbols", buildID: "b1", inputs: []CreateChunkInput{withSymbols}, wantErrIs: ErrInvalidBuildInput},
		{name: "begin_error", buildID: "b1", inputs: one, db: dbScript{beginErr: errors.New("begin err")}, wantErr: true},
		{name: "build_not_found", buildID: "b1", inputs: one, db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build_query_error", buildID: "b1", inputs: one, db: dbScript{row: errRow(errors.New("query err"))}, wantErr: true},
		{name: "build_conflict_status", buildID: "b1", inputs: one, db: dbScript{row: buildRow("b1", "completed")}, wantErrIs: ErrBuildConflict},
		{name: "chunks_query_error", buildID: "b1", inputs: one, db: dbScript{row: running, queryErr: errors.New("chunks err")}, wantErr: true},
		{name: "chunks_scan_error", buildID: "b1", inputs: one, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1"}}, scanErr: errors.New("scan err")}, wantErr: true},
		{name: "duplicate_id_conflict", buildID: "b1", inputs: []CreateChunkInput{chunkInput("c1", "", "aerial", "T", "I")}, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1"}}}, wantErrIs: ErrBuildConflict},
		{name: "exec_insert_error", buildID: "b1", inputs: one, db: dbScript{row: running, execErr: errors.New("insert err")}, wantErr: true},
		{name: "exec_insert_error_with_rollback_err", buildID: "b1", inputs: one, db: dbScript{row: running, execErr: errors.New("insert err"), rollbackErr: errors.New("rb err")}, wantErr: true},
		{name: "commit_error", buildID: "b1", inputs: one, db: dbScript{row: running, commitErr: errors.New("commit err")}, wantErr: true},
		{name: "dag_validation_cycle_error", buildID: "b1", inputs: []CreateChunkInput{cycleA, cycleB}, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1", Repo: "aerial"}}}, wantErr: true},
		{name: "success_with_id_collision_resolution", buildID: "b1", inputs: one, db: dbScript{row: running, chunks: []VanadisChunk{{ID: "b1-chunk-1", ChunkIndex: 0, Repo: "mirrormere"}}, execTag: "INSERT 0 1"}, wantCount: 1},
		{name: "success_append_action_chunk", buildID: "b1", inputs: []CreateChunkInput{chunkInput("", "action", "", "Act", "Do Act")}, db: dbScript{row: running, execTag: "INSERT 0 1"}, wantCount: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			got, err := store.AppendChunks(context.Background(), tc.buildID, tc.inputs)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if len(got) != tc.wantCount {
				t.Fatalf("got %d chunks, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestPostgresVanadisStore_RemoveChunk(t *testing.T) {
	t.Parallel()
	running := buildRow("b1", "running")
	runningPending := pendingChunks("running", "pending")
	tests := []struct {
		name          string
		buildID       string
		chunkRef      string
		db            dbScript
		wantErrIs     error
		wantErr       bool
		wantRemovedID string
		wantRemainLen int
	}{
		{name: "nil receiver", buildID: "b1", chunkRef: "c1", db: dbScript{bare: nilReceiver}, wantErr: true},
		{name: "nil db", buildID: "b1", chunkRef: "c1", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty_build_id", buildID: "", chunkRef: "c1", wantErrIs: ErrInvalidBuildInput},
		{name: "empty_chunk_ref", buildID: "b1", chunkRef: "", wantErrIs: ErrInvalidBuildInput},
		{name: "begin_error", buildID: "b1", chunkRef: "c1", db: dbScript{beginErr: errors.New("begin err")}, wantErr: true},
		{name: "build_not_found", buildID: "b1", chunkRef: "c1", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build_query_error", buildID: "b1", chunkRef: "c1", db: dbScript{row: errRow(errors.New("query err"))}, wantErr: true},
		{name: "build_conflict_status", buildID: "b1", chunkRef: "c1", db: dbScript{row: buildRow("b1", "completed")}, wantErrIs: ErrBuildConflict},
		{name: "chunks_query_error", buildID: "b1", chunkRef: "c1", db: dbScript{row: running, queryErr: errors.New("chunks err")}, wantErr: true},
		{name: "chunks_scan_error", buildID: "b1", chunkRef: "c1", db: dbScript{row: running, chunks: []VanadisChunk{{ID: "c1"}}, scanErr: errors.New("scan err")}, wantErr: true},
		{name: "chunk_not_found", buildID: "b1", chunkRef: "c999", db: dbScript{row: running, chunks: pendingChunks("pending")}, wantErrIs: ErrChunkNotFound},
		{name: "chunk_conflict_status", buildID: "b1", chunkRef: "c1", db: dbScript{row: running, chunks: runningPending}, wantErrIs: ErrBuildConflict},
		{name: "cannot_remove_only_chunk", buildID: "b1", chunkRef: "c1", db: dbScript{row: running, chunks: pendingChunks("pending")}, wantErrIs: ErrBuildConflict},
		{name: "delete_exec_error", buildID: "b1", chunkRef: "c2", db: dbScript{row: running, chunks: runningPending, execErr: errors.New("del err")}, wantErr: true},
		{name: "delete_exec_error_with_rollback_err", buildID: "b1", chunkRef: "c2", db: dbScript{row: running, chunks: runningPending, execErr: errors.New("del err"), rollbackErr: errors.New("rb err")}, wantErr: true},
		{name: "commit_error", buildID: "b1", chunkRef: "c2", db: dbScript{row: running, chunks: runningPending, commitErr: errors.New("commit err")}, wantErr: true},
		{name: "success", buildID: "b1", chunkRef: "1", db: dbScript{row: running, chunks: pendingChunks("running", "pending", "pending"), execTag: "DELETE 1"}, wantRemovedID: "c2", wantRemainLen: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			removed, remaining, err := store.RemoveChunk(context.Background(), tc.buildID, tc.chunkRef)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if removed.ID != tc.wantRemovedID || len(remaining) != tc.wantRemainLen {
				t.Fatalf("RemoveChunk = removed %q, %d remaining; want %q, %d", removed.ID, len(remaining), tc.wantRemovedID, tc.wantRemainLen)
			}
		})
	}
}

func TestPostgresVanadisStore_ClaimStalledChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                                  string
		chunkID, mode, status, buildStatus    string
		prNumber, nudges                      int
		dispatchID                            string
		beginErr, scanErr, execErr, commitErr error
		wantVerdict                           StalledChunkVerdict
		wantErr                               error
		wantAnyErr, wantNudgeExec             bool
	}{
		{name: "pr chunk without pr first stall nudges", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", wantVerdict: StalledChunkVerdictNudge, wantNudgeExec: true},
		{name: "pr chunk without pr second stall fails", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", nudges: 1, wantVerdict: StalledChunkVerdictFail},
		{name: "matching dispatch id proceeds with verdict", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", nudges: 0, dispatchID: "vanadis-c1-turn-0", wantVerdict: StalledChunkVerdictNudge, wantNudgeExec: true},
		{name: "mismatched dispatch id returns noop verdict", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", nudges: 1, dispatchID: "vanadis-c1-turn-0", wantVerdict: StalledChunkVerdictNoop},
		{name: "pr opened chunk is not stalled", chunkID: "c1", mode: "pr", status: "pr_opened", buildStatus: "running", prNumber: 42, wantVerdict: StalledChunkVerdictNone},
		{name: "running chunk with linked pr is not stalled", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", prNumber: 42, wantVerdict: StalledChunkVerdictNone},
		{name: "action chunk is never stalled", chunkID: "c1", mode: "action", status: "running", buildStatus: "running", wantVerdict: StalledChunkVerdictNone},
		{name: "build not running is not stalled", chunkID: "c1", mode: "pr", status: "running", buildStatus: "failed", wantVerdict: StalledChunkVerdictNone},
		{name: "empty chunk id rejected", chunkID: "  ", wantAnyErr: true},
		{name: "begin error", chunkID: "c1", beginErr: errors.New("begin boom"), wantAnyErr: true},
		{name: "missing chunk", chunkID: "c1", scanErr: pgx.ErrNoRows, wantErr: ErrChunkNotFound},
		{name: "select error", chunkID: "c1", scanErr: errors.New("select boom"), wantAnyErr: true},
		{name: "nudge update error", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", execErr: errors.New("exec boom"), wantAnyErr: true, wantNudgeExec: true},
		{name: "nudge commit error", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", commitErr: errors.New("commit boom"), wantAnyErr: true, wantNudgeExec: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var nudgeExecs []any
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					return &genericMockRow{scanFn: func(dest ...any) error {
						if tc.scanErr != nil {
							return tc.scanErr
						}
						return scanValues(dest, tc.chunkID, "b1", nil, tc.mode, "Stalled", tc.status, tc.prNumber, tc.nudges, "b1", "thread-1", "Objective", tc.buildStatus)
					}}
				},
				execFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
					if strings.Contains(sql, "stall_nudges = stall_nudges + 1") {
						nudgeExecs = append(nudgeExecs, args...)
					}
					return pgconn.NewCommandTag("UPDATE 1"), tc.execErr
				},
			}
			store := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return tx, tc.beginErr }})
			verdict, build, chunk, err := store.ClaimStalledChunk(context.Background(), tc.chunkID, tc.dispatchID)
			if gotNudge := len(nudgeExecs) == 1 && nudgeExecs[0] == "c1"; gotNudge != tc.wantNudgeExec {
				t.Fatalf("nudge exec = %v (args %v), want %v", gotNudge, nudgeExecs, tc.wantNudgeExec)
			}
			if tc.wantErr != nil || tc.wantAnyErr {
				if err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
					t.Fatalf("err = %v, want %v (any=%v)", err, tc.wantErr, tc.wantAnyErr)
				}
				return
			}
			if err != nil || verdict != tc.wantVerdict || build == nil || build.ID != "b1" || build.ThreadID != "thread-1" || chunk == nil || chunk.ID != "c1" {
				t.Fatalf("got verdict=%q build=%+v chunk=%+v err=%v, want verdict %q", verdict, build, chunk, err, tc.wantVerdict)
			}
		})
	}
	var nilStore *PostgresVanadisStore
	if _, _, _, err := nilStore.ClaimStalledChunk(context.Background(), "c1"); err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestPostgresVanadisStore_RetryActionChunk(t *testing.T) {
	t.Parallel()
	injectedErr := errors.New("injected database failure")
	tests := []struct {
		name        string
		buildStatus string
		chunk       VanadisChunk
		siblings    []VanadisChunk
		expected    string
		db          dbScript
		wantErr     bool
		wantErrIs   error
	}{
		{name: "generation overflow", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2147483647}, expected: "vanadis-c1-turn-2147483647", wantErr: true},
		{name: "transitive completed descendant", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c3", Status: "completed", DependsOn: []string{"c2"}}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil receiver", db: dbScript{bare: nilReceiver}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "nil database", db: dbScript{bare: nilDB}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "parent not found", db: dbScript{routes: []sqlRoute{{match: "FROM vanadis_builds", row: errRow(pgx.ErrNoRows)}}}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrBuildNotFound},
		{name: "parent query error", db: dbScript{routes: []sqlRoute{{match: "FROM vanadis_builds", row: errRow(injectedErr)}}}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "selected absent", buildStatus: "failed", chunk: VanadisChunk{ID: "other", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: ErrChunkNotFound},
		{name: "failed action", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0"},
		{name: "committed reset replay pending", buildStatus: "running", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "pending", StallNudges: 1}, expected: "vanadis-c1-turn-0"},
		{name: "replay after independent sibling launch", buildStatus: "running", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, siblings: []VanadisChunk{{ID: "c2", Status: "running"}}, expected: "vanadis-c1-turn-0"},
		{name: "committed reset replay running", buildStatus: "running", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}, expected: "vanadis-c1-turn-0"},
		{name: "terminal replay", buildStatus: "completed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "completed", StallNudges: 1}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "missing identity", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, wantErr: true},
		{name: "stale identity", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", StallNudges: 2}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled parent", buildStatus: "cancelled", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "pr action", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", PRNumber: 12}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "pr mode", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled selected", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "cancelled"}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "failed sibling", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "failed"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "cancelled sibling", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "cancelled"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "active sibling", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "deploying"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "unready dependency", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []VanadisChunk{{ID: "c0", Mode: "pr", Status: "merged"}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "completed descendant", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, siblings: []VanadisChunk{{ID: "c2", Status: "completed", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0", wantErr: true},
		{name: "safe graph", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, siblings: []VanadisChunk{{ID: "c0", Status: "completed"}, {ID: "c2", Status: "pending", DependsOn: []string{"c1"}}}, expected: "vanadis-c1-turn-0"},
		{name: "begin failure", db: dbScript{beginErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "query failure", buildStatus: "failed", db: dbScript{queryErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "scan failure", buildStatus: "failed", chunk: VanadisChunk{ID: "c1"}, db: dbScript{scanErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "rows failure", buildStatus: "failed", db: dbScript{rowsErr: injectedErr}, expected: "vanadis-c1-turn-0", wantErr: true, wantErrIs: injectedErr},
		{name: "chunk write rollback", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: errors.New("write")}, wantErr: true},
		{name: "commit failure", buildStatus: "failed", chunk: VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}, expected: "vanadis-c1-turn-0", db: dbScript{commitErr: errors.New("commit")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.db.buildRow = buildRow("b1", tc.buildStatus)
			tc.db.chunks = append([]VanadisChunk{tc.chunk}, tc.siblings...)
			store, _ := tc.db.open()
			got, err := store.RetryActionChunk(context.Background(), RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: tc.expected, Reason: "operator retry"})
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("err=%v want=%v", err, tc.wantErrIs)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("result=%+v err=%v wantErr=%v", got, err, tc.wantErr)
			}
			if !tc.wantErr && (got == nil || *got != (RetryActionChunkResult{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-1", Accepted: true})) {
				t.Fatalf("result=%+v", got)
			}
		})
	}
}

// retryTransaction models the external database's atomic commit and parent lock.
// State changes remain private until Commit; Rollback discards them.
type retryTransaction struct {
	*mockTx

	finish func(bool) error
}

func (tx *retryTransaction) Commit(context.Context) error   { return tx.finish(true) }
func (tx *retryTransaction) Rollback(context.Context) error { return tx.finish(false) }

func TestPostgresVanadisStore_RetryActionChunkTransactions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		concurrent     int
		failBuild      bool
		lostCommit     bool
		wantErr        bool
		wantStatus     string
		wantGeneration int
	}{
		{name: "duplicate concurrent requests", concurrent: 8, wantStatus: "pending", wantGeneration: 1},
		{name: "build write rolls back reset", concurrent: 1, failBuild: true, wantErr: true, wantStatus: "failed"},
		{name: "committed before journal replay", concurrent: 1, lostCommit: true, wantErr: true, wantStatus: "pending", wantGeneration: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			buildStatus := "failed"
			original := VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}, StallNudges: 0}
			current := original
			sibling := VanadisChunk{ID: "c0", BuildID: "b1", Status: "completed"}
			lost := tc.lostCommit
			store := retryFixtureStore(&mu, &current, &buildStatus, sibling, tc.failBuild, &lost)
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}
			for err := range retryConcurrentRequests(store, req, tc.concurrent) {
				if (err != nil) != tc.wantErr {
					t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
				}
			}
			if tc.lostCommit {
				result, err := store.RetryActionChunk(context.Background(), req)
				if err != nil || !validRetryResult(result) {
					t.Fatalf("replay=%+v err=%v", result, err)
				}
			}
			if current.Status != tc.wantStatus || current.StallNudges != tc.wantGeneration {
				t.Fatalf("status=%s generation=%d", current.Status, current.StallNudges)
			}
			wantParent := "running"
			if tc.failBuild {
				wantParent = "failed"
			}
			if buildStatus != wantParent {
				t.Fatalf("parent=%s", buildStatus)
			}
		})
	}
}

func retryFixtureStore(mu *sync.Mutex, current *VanadisChunk, buildStatus *string, sibling VanadisChunk, failBuild bool, lost *bool) *PostgresVanadisStore {
	return newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) {
		var local VanadisChunk
		var parent string
		locked := false
		closed := false
		base := &mockTx{
			queryRowFn: func(_ context.Context, query string, _ ...any) pgx.Row {
				if !strings.Contains(query, "FROM vanadis_builds") || !strings.Contains(query, "FOR UPDATE") {
					return errRow(errors.New("missing parent lock"))
				}
				mu.Lock()
				locked = true
				local = *current
				parent = *buildStatus
				return buildRow("b1", parent)
			},
			queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
				if !locked || !strings.Contains(query, "ORDER BY chunk_index ASC FOR UPDATE") {
					return nil, errors.New("missing ordered chunk locks")
				}
				return &mockRows{items: []VanadisChunk{sibling, local}}, nil
			},
			execFn: func(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
				return retryFixtureExec(query, &local, &parent, failBuild)
			},
		}
		return &retryTransaction{mockTx: base, finish: func(commit bool) error {
			if closed {
				return pgx.ErrTxClosed
			}
			closed = true
			if !locked {
				return errors.New("transaction never locked parent")
			}
			defer mu.Unlock()
			if commit {
				*current = local
				*buildStatus = parent
				if *lost {
					*lost = false
					return errors.New("commit response lost")
				}
			}
			return nil
		}}, nil
	}})
}

func validRetryResult(result *RetryActionChunkResult) bool {
	return result != nil && result.DispatchID == "vanadis-c1-turn-1" && result.Accepted
}

func retryFixtureExec(query string, local *VanadisChunk, parent *string, failBuild bool) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(query, "UPDATE vanadis_chunks"):
		for _, fragment := range []string{"status = 'pending'", "stall_nudges = stall_nudges + 1", "started_at = NULL", "completed_at = NULL", "ci_fail_count = 0", "ci_checks = '{}'::jsonb", "rollout_state = '{}'::jsonb", "banner = ''", "merge_awakeable_id = ''", "deploy_awakeable_id = ''"} {
			if !strings.Contains(query, fragment) {
				return pgconn.CommandTag{}, fmt.Errorf("missing reset %s", fragment)
			}
		}
		local.Status = "pending"
		local.StallNudges++
	case strings.Contains(query, "UPDATE vanadis_builds SET status = 'running'"):
		if failBuild {
			return pgconn.CommandTag{}, errors.New("build write failed")
		}
		*parent = "running"
	default:
		return pgconn.CommandTag{}, errors.New("unexpected mutation")
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func retryConcurrentRequests(store *PostgresVanadisStore, req RetryActionChunkRequest, concurrent int) <-chan error {
	results := make(chan error, concurrent)
	var wg sync.WaitGroup
	for range concurrent {
		wg.Go(func() {
			result, err := store.RetryActionChunk(context.Background(), req)
			if err == nil && (!validRetryResult(result)) {
				err = fmt.Errorf("unexpected result %+v", result)
			}
			results <- err
		})
	}
	wg.Wait()
	close(results)
	return results
}
