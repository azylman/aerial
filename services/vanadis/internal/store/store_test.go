package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis/internal/model"
)

type scriptRow struct {
	vals []any
	err  error
}

func (r *scriptRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanValues(dest, r.vals...)
}

func scanValues(dest []any, vals ...any) error {
	if len(vals) > len(dest) {
		return errors.New("too many values for scan destination")
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		switch d := dest[i].(type) {
		case *string:
			if s, ok := v.(string); ok {
				*d = s
			}
		case *int:
			if n, ok := v.(int); ok {
				*d = n
			}
		case *bool:
			if b, ok := v.(bool); ok {
				*d = b
			}
		case *sql.NullTime:
			if nt, ok := v.(sql.NullTime); ok {
				*d = nt
			}
		case *time.Time:
			if t, ok := v.(time.Time); ok {
				*d = t
			}
		case *[]string:
			if sl, ok := v.([]string); ok {
				*d = sl
			}
		case *[]byte:
			if bs, ok := v.([]byte); ok {
				*d = bs
			}
		}
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
	if len(r.builds) > 0 {
		b := r.builds[r.idx-1]
		return scanValues(dest, b.ID, b.Agent, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet, b.CreatedAt, b.UpdatedAt)
	}
	if len(r.items) > 0 {
		c := r.items[r.idx-1]
		if len(dest) == 5 {
			return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Effort, true)
		}
		rawCIChecks := []byte(`{"lint":"passed"}`)
		rawRolloutState := []byte(`{"expected_jobs":["eri"]}`)
		if c.Title == "bad-json" {
			rawCIChecks = []byte(`{invalid`)
			rawRolloutState = []byte(`{invalid`)
		}
		if c.Title == "null-json" {
			rawCIChecks = []byte(`null`)
			rawRolloutState = []byte(`{}`)
		}
		return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, rawCIChecks, rawRolloutState, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
	}
	return nil
}

type mockDB struct {
	queryRowFn func(ctx context.Context, query string, args ...any) pgx.Row
	queryFn    func(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	beginFn    func(ctx context.Context) (pgx.Tx, error)
}

func (*mockDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (db *mockDB) Query(ctx context.Context, s string, a ...any) (pgx.Rows, error) {
	if db.queryFn != nil {
		return checkedQuery(s)(db.queryFn(ctx, s, a...))
	}
	return &mockRows{}, nil
}

func (db *mockDB) QueryRow(ctx context.Context, s string, a ...any) pgx.Row {
	if db.queryRowFn != nil {
		return checkedQueryRow(s, db.queryRowFn(ctx, s, a...))
	}
	return &scriptRow{}
}

// sqlTopLevel walks sql and calls visit for every byte outside string literals
// and parentheses, stopping early when visit returns false.
func sqlTopLevel(query string, visit func(i int) bool) {
	depth, inStr := 0, false
	for i := range len(query) {
		switch c := query[i]; {
		case c == '\'':
			inStr = !inStr
		case inStr:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && !visit(i):
			return
		}
	}
}

// sqlKeywordAt reports whether upper holds the keyword w as a whole word at i.
func sqlKeywordAt(upper string, i int, w string) bool {
	ident := func(j int) bool {
		if j < 0 || j >= len(upper) {
			return false
		}
		c := upper[j]
		return c == '_' || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}
	return strings.HasPrefix(upper[i:], w) && !ident(i-1) && !ident(i+len(w))
}

// selectedColumnCount returns how many columns sql yields: the top-level
// RETURNING list when present, else the top-level SELECT list. Commas nested in
// parentheses or string literals do not split columns. It returns 0 when sql
// yields no column list (plain INSERT/UPDATE/DELETE).
func selectedColumnCount(query string) int {
	upper := strings.ToUpper(query)
	var start int
	end := len(query)
	selectEnd, fromAt, returningEnd := -1, -1, -1
	sqlTopLevel(upper, func(i int) bool {
		switch {
		case selectEnd < 0 && sqlKeywordAt(upper, i, "SELECT"):
			selectEnd = i + len("SELECT")
		case selectEnd >= 0 && fromAt < 0 && sqlKeywordAt(upper, i, "FROM"):
			fromAt = i
		case sqlKeywordAt(upper, i, "RETURNING"):
			returningEnd = i + len("RETURNING")
		}
		return true
	})
	switch {
	case returningEnd >= 0:
		start = returningEnd
	case selectEnd >= 0:
		start = selectEnd
		if fromAt >= 0 {
			end = fromAt
		}
	default:
		return 0
	}
	list := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query[start:end]), ";"))
	if list == "" {
		return 0
	}
	cols := 1
	sqlTopLevel(list, func(i int) bool {
		if list[i] == ',' {
			cols++
		}
		return true
	})
	return cols
}

// checkColumnCount rejects a Scan whose destination count differs from the
// columns the SQL selects, mirroring pgx's column-count mismatch error.
func checkColumnCount(cols int, dest []any) error {
	if cols > 0 && cols != len(dest) {
		return fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", cols, len(dest))
	}
	return nil
}

// columnCheckedRow wraps a scripted pgx.Row with its SQL's selected column count.
type columnCheckedRow struct {
	pgx.Row

	cols int
}

func (r columnCheckedRow) Scan(dest ...any) error {
	if err := checkColumnCount(r.cols, dest); err != nil {
		return err
	}
	return r.Row.Scan(dest...)
}

// columnCheckedRows wraps scripted pgx.Rows with their SQL's selected column count.
type columnCheckedRows struct {
	pgx.Rows

	cols int
}

func (r columnCheckedRows) Scan(dest ...any) error {
	if err := checkColumnCount(r.cols, dest); err != nil {
		return err
	}
	return r.Rows.Scan(dest...)
}

// checkedQuery and checkedQueryRow apply the column-count check to whatever a
// scripted mock returns, so every store test catches SQL/Scan drift.
func checkedQuery(query string) func(pgx.Rows, error) (pgx.Rows, error) {
	return func(rows pgx.Rows, err error) (pgx.Rows, error) {
		if err != nil || rows == nil {
			return rows, err
		}
		return columnCheckedRows{Rows: rows, cols: selectedColumnCount(query)}, nil
	}
}

func checkedQueryRow(query string, row pgx.Row) pgx.Row {
	if row == nil {
		return nil
	}
	return columnCheckedRow{Row: row, cols: selectedColumnCount(query)}
}

func (db *mockDB) Begin(ctx context.Context) (pgx.Tx, error) {
	if db.beginFn != nil {
		return db.beginFn(ctx)
	}
	return nil, errors.New("begin not implemented")
}

type bareStore struct{ store *PostgresVanadisStore }

var (
	nilReceiver = &bareStore{}
	nilDB       = &bareStore{store: &PostgresVanadisStore{}}
)

type dbScript struct {
	bare         *bareStore
	buildVals    []any
	buildErr     error
	builds       []model.VanadisBuild
	buildsErr    error
	buildScanErr error
	buildRowsErr error
	chunks       []model.VanadisChunk
	chunksErr    error
	chunkScanErr error
	chunkRowsErr error
}

func (s dbScript) open() (*PostgresVanadisStore, *[]string) {
	candidates := new([]string)
	if s.bare != nil {
		return s.bare.store, candidates
	}
	queryRow := func(_ context.Context, _ string, args ...any) pgx.Row {
		if len(args) > 0 {
			if c, ok := args[0].([]string); ok {
				*candidates = c
			}
		}
		if s.buildErr != nil {
			return &scriptRow{err: s.buildErr}
		}
		return &scriptRow{vals: s.buildVals}
	}
	query := func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
		if strings.Contains(query, "vanadis_builds") {
			if s.buildsErr != nil {
				return nil, s.buildsErr
			}
			return &mockRows{builds: s.builds, scanErr: s.buildScanErr, rowsErr: s.buildRowsErr}, nil
		}
		if s.chunksErr != nil {
			return nil, s.chunksErr
		}
		return &mockRows{items: s.chunks, scanErr: s.chunkScanErr, rowsErr: s.chunkRowsErr}, nil
	}
	return &PostgresVanadisStore{db: &mockDB{queryRowFn: queryRow, queryFn: query}}, candidates
}

func mkBuild(id, threadID, title, objective, status string) model.VanadisBuild {
	return model.VanadisBuild{ID: id, ThreadID: threadID, Title: title, Objective: objective, Status: status, Tier: 2}
}

func mkChunk(id, buildID string, idx int, title, instr, effort, status string) model.VanadisChunk {
	if effort == "" {
		effort = "medium"
	}
	return model.VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: "aerial", Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}, DependsOn: []string{}}
}

func buildVals(b model.VanadisBuild) []any {
	return []any{b.ID, b.Agent, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet, b.CreatedAt, b.UpdatedAt}
}

type getBuildCase struct {
	name        string
	bare        *bareStore
	buildID     string
	build       model.VanadisBuild
	buildErr    error
	chunks      []model.VanadisChunk
	chunksErr   error
	scanErr     error
	rowsErr     error
	wantErr     bool
	wantErrIs   error
	wantChunks  int
	wantNilMaps bool
}

func TestGetBuild(t *testing.T) {
	t.Parallel()
	pop := mkBuild("b-1", "thread-1", "Build Title", "Objective", "running")
	tests := []getBuildCase{
		{name: "nil receiver errors", bare: nilReceiver, buildID: "b-1", wantErr: true},
		{name: "nil db errors", bare: nilDB, buildID: "b-1", wantErr: true},
		{name: "empty build id errors", buildID: "   ", wantErr: true},
		{name: "missing build maps to not found", buildID: "b-404", buildErr: pgx.ErrNoRows, wantErrIs: model.ErrBuildNotFound},
		{name: "build query error propagates", buildID: "b-err", buildErr: errors.New("db down"), wantErr: true},
		{name: "chunks query error propagates", buildID: "b-1", build: pop, chunksErr: errors.New("chunks down"), wantErr: true},
		{name: "chunk scan error propagates", buildID: "b-1", build: pop, chunks: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "T", "I", "", "")}, scanErr: errors.New("scan down"), wantErr: true},
		{name: "chunk rows error propagates", buildID: "b-1", build: pop, chunks: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "T", "I", "", "")}, rowsErr: errors.New("rows down"), wantErr: true},
		{name: "success scans build and chunks", buildID: "b-1", build: pop, chunks: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "T1", "I1", "medium", ""), mkChunk("c-2", "b-1", 1, "T2", "I2", "low", "")}, wantChunks: 2},
		{name: "null json leaves maps nil", buildID: "b-1", build: pop, chunks: []model.VanadisChunk{mkChunk("c-9", "b-1", 0, "null-json", "I", "medium", "")}, wantChunks: 1, wantNilMaps: true},
		{name: "invalid json leaves maps nil", buildID: "b-1", build: pop, chunks: []model.VanadisChunk{mkChunk("c-10", "b-1", 0, "bad-json", "I", "medium", "")}, wantChunks: 1, wantNilMaps: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, _ := dbScript{bare: tc.bare, buildVals: buildVals(tc.build), buildErr: tc.buildErr, chunks: tc.chunks, chunksErr: tc.chunksErr, chunkScanErr: tc.scanErr, chunkRowsErr: tc.rowsErr}.open()
			gotB, gotC, err := s.GetBuild(context.Background(), tc.buildID)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want %v", err, tc.wantErrIs)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotB.ID != tc.build.ID || len(gotC) != tc.wantChunks {
				t.Errorf("GetBuild = build %q chunks %d, want build %q chunks %d", gotB.ID, len(gotC), tc.build.ID, tc.wantChunks)
			}
			if tc.wantNilMaps && len(gotC) > 0 && (gotC[0].CIChecks != nil || gotC[0].RolloutState != nil) {
				t.Errorf("GetBuild chunk maps = %+v / %+v, want nil", gotC[0].CIChecks, gotC[0].RolloutState)
			}
		})
	}
}

type listBuildsCase struct {
	name       string
	bare       *bareStore
	limit      int
	builds     []model.VanadisBuild
	buildsErr  error
	scanErr    error
	rowsErr    error
	chunks     []model.VanadisChunk
	chunksErr  error
	wantErr    bool
	wantTotal  int
	wantLimit  int
	wantStates []string
}

func TestListBuilds(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	b1, b2 := mkBuild("b-1", "100", "", "", "running"), mkBuild("b-2", "101", "", "", "completed")
	b1.CreatedAt, b2.CreatedAt = now.Add(-time.Hour), now.Add(-2*time.Hour)
	tests := []listBuildsCase{
		{name: "nil receiver errors", bare: nilReceiver, wantErr: true},
		{name: "nil db errors", bare: nilDB, wantErr: true},
		{name: "builds query error propagates", buildsErr: errors.New("builds down"), wantErr: true},
		{name: "build scan error propagates", builds: []model.VanadisBuild{b1}, scanErr: errors.New("scan down"), wantErr: true},
		{name: "build rows error propagates", builds: []model.VanadisBuild{b1}, rowsErr: errors.New("rows down"), wantErr: true},
		{name: "empty builds returns empty entries", wantTotal: 0},
		{name: "chunks query error propagates", builds: []model.VanadisBuild{b1}, chunksErr: errors.New("chunks down"), wantErr: true},
		{name: "success groups chunks by build", builds: []model.VanadisBuild{b1, b2}, chunks: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "C1", "", "", "running"), mkChunk("c-3", "b-2", 0, "C3", "", "", "merged")}, wantTotal: 2, wantStates: []string{"running", "completed"}},
		{name: "nonpositive limit clamps to default", limit: 0, builds: []model.VanadisBuild{b1}, chunks: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "C1", "", "", "running")}, wantTotal: 1, wantLimit: 50},
		{name: "oversize limit clamps to default", limit: 500, builds: []model.VanadisBuild{b1}, chunks: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "C1", "", "", "running")}, wantTotal: 1, wantLimit: 50},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runListBuildsCase(t, tc)
		})
	}
}

func captureBuildLimit(s *PostgresVanadisStore, wantLimit int, gotLimit *any) {
	if wantLimit == 0 || s == nil {
		return
	}
	db, ok := s.db.(*mockDB)
	if !ok {
		return
	}
	inner := db.queryFn
	db.queryFn = func(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
		if strings.Contains(query, "vanadis_builds") && len(args) > 0 {
			*gotLimit = args[0]
		}
		return inner(ctx, query, args...)
	}
}

func runListBuildsCase(t *testing.T, tc listBuildsCase) {
	t.Helper()
	s, _ := dbScript{bare: tc.bare, builds: tc.builds, buildsErr: tc.buildsErr, buildScanErr: tc.scanErr, buildRowsErr: tc.rowsErr, chunks: tc.chunks, chunksErr: tc.chunksErr}.open()
	var gotLimit any
	captureBuildLimit(s, tc.wantLimit, &gotLimit)
	got, err := s.ListBuilds(context.Background(), tc.limit)
	if tc.wantErr {
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != tc.wantTotal {
		t.Fatalf("ListBuilds entries = %d, want %d", len(got), tc.wantTotal)
	}
	if tc.wantLimit != 0 && gotLimit != tc.wantLimit {
		t.Errorf("ListBuilds limit = %v, want %d", gotLimit, tc.wantLimit)
	}
	checkQueueStates(t, got, tc.wantStates)
}

func checkQueueStates(t *testing.T, got []model.VanadisBuildEntry, want []string) {
	t.Helper()
	for i := range want {
		if got[i].QueueState != want[i] {
			t.Errorf("entries[%d].QueueState = %q, want %q", i, got[i].QueueState, want[i])
		}
	}
}

type getChunkCase struct {
	name      string
	bare      *bareStore
	chunkID   string
	chunks    []model.VanadisChunk
	queryErr  error
	scanErr   error
	rowsErr   error
	build     model.VanadisBuild
	buildErr  error
	wantErr   bool
	wantErrIs error
	wantChunk string
	wantBuild string
}

func TestGetChunk(t *testing.T) {
	t.Parallel()
	c1 := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk Title"}}
	tests := []getChunkCase{
		{name: "nil receiver errors", bare: nilReceiver, chunkID: "c1", wantErr: true},
		{name: "nil db errors", bare: nilDB, chunkID: "c1", wantErr: true},
		{name: "empty chunk id errors", chunkID: "  ", wantErr: true},
		{name: "query error propagates", chunkID: "c1", queryErr: errors.New("query down"), wantErr: true},
		{name: "scan error propagates", chunkID: "c1", chunks: c1, scanErr: errors.New("scan down"), wantErr: true},
		{name: "rows error propagates", chunkID: "c1", chunks: c1, rowsErr: errors.New("rows down"), wantErr: true},
		{name: "missing chunk maps to not found", chunkID: "c1", wantErrIs: model.ErrChunkNotFound},
		{name: "missing build maps to not found", chunkID: "c1", chunks: c1, buildErr: pgx.ErrNoRows, wantErrIs: model.ErrBuildNotFound},
		{name: "build query error propagates", chunkID: "c1", chunks: c1, buildErr: errors.New("build down"), wantErr: true},
		{name: "success returns chunk and build", chunkID: "c1", chunks: c1, build: mkBuild("b1", "", "Build Title", "", ""), wantChunk: "c1", wantBuild: "b1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, _ := dbScript{bare: tc.bare, buildVals: buildVals(tc.build), buildErr: tc.buildErr, chunks: tc.chunks, chunksErr: tc.queryErr, chunkScanErr: tc.scanErr, chunkRowsErr: tc.rowsErr}.open()
			c, b, err := s.GetChunk(context.Background(), tc.chunkID)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want %v", err, tc.wantErrIs)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.ID != tc.wantChunk || b.ID != tc.wantBuild {
				t.Errorf("GetChunk = chunk %q build %q, want chunk %q build %q", c.ID, b.ID, tc.wantChunk, tc.wantBuild)
			}
		})
	}
}

type getChunkByPRCase struct {
	name           string
	bare           *bareStore
	repo           string
	prNumber       int
	rowErr         error
	rowEffort      string
	wantErr        bool
	wantLookup     VanadisChunkPRLookup
	wantCandidates []string
}

func TestGetChunkByPR(t *testing.T) {
	t.Parallel()
	tests := []getChunkByPRCase{
		{name: "nil receiver errors", bare: nilReceiver, repo: "aerial", prNumber: 1, wantErr: true},
		{name: "nil db errors", bare: nilDB, repo: "aerial", prNumber: 1, wantErr: true},
		{name: "invalid args return unmatched", repo: "", prNumber: 0, wantLookup: VanadisChunkPRLookup{}},
		{name: "no rows returns unmatched", repo: "aerial", prNumber: 101, rowErr: pgx.ErrNoRows, wantLookup: VanadisChunkPRLookup{}, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "chunk query error propagates", repo: "aerial", prNumber: 101, rowErr: errors.New("db down"), wantErr: true, wantCandidates: []string{"aerial", "azylman/aerial"}},
		{name: "successful match returns lookup", repo: "aerial", prNumber: 101, rowEffort: "low", wantLookup: VanadisChunkPRLookup{Matched: true, ChunkID: "c1", BuildID: "b1", Effort: "low"}, wantCandidates: []string{"aerial", "azylman/aerial"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runGetChunkByPRCase(t, tc)
		})
	}
}

func stubPRRow(s *PostgresVanadisStore, gotCandidates *[]string, rowErr error, rowEffort string) {
	if s == nil {
		return
	}
	db, ok := s.db.(*mockDB)
	if !ok {
		return
	}
	db.queryRowFn = func(_ context.Context, query string, args ...any) pgx.Row {
		if len(args) > 0 {
			captureCandidates(args[0], gotCandidates)
		}
		if rowErr != nil {
			return checkedQueryRow(query, &scriptRow{err: rowErr})
		}
		return checkedQueryRow(query, &scriptRow{vals: []any{"c1", "b1", 0, rowEffort, true}})
	}
}

func captureCandidates(arg any, gotCandidates *[]string) {
	c, ok := arg.([]string)
	if !ok {
		return
	}
	*gotCandidates = c
}

func checkCandidates(t *testing.T, gotCandidates *[]string, want []string) {
	t.Helper()
	if len(*gotCandidates) != len(want) {
		t.Errorf("GetChunkByPR candidates = %v, want %v", *gotCandidates, want)
		return
	}
	for i := range want {
		if (*gotCandidates)[i] != want[i] {
			t.Errorf("GetChunkByPR candidates = %v, want %v", *gotCandidates, want)
			return
		}
	}
}

func runGetChunkByPRCase(t *testing.T, tc getChunkByPRCase) {
	t.Helper()
	s, gotCandidates := dbScript{bare: tc.bare}.open()
	stubPRRow(s, gotCandidates, tc.rowErr, tc.rowEffort)
	got, err := s.GetChunkByPR(context.Background(), tc.repo, tc.prNumber)
	if tc.wantErr {
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *got != tc.wantLookup {
		t.Errorf("GetChunkByPR() = %+v, want %+v", *got, tc.wantLookup)
	}
	if len(tc.wantCandidates) > 0 {
		checkCandidates(t, gotCandidates, tc.wantCandidates)
	}
}

type repoCandidatesCase struct {
	name     string
	input    string
	expected []string
}

func TestRepoMatchCandidates(t *testing.T) {
	t.Parallel()
	tests := []repoCandidatesCase{
		{name: "empty repo returns nil", input: "", expected: nil},
		{name: "bare repo adds default owner prefix", input: "aerial", expected: []string{"aerial", "azylman/aerial"}},
		{name: "whitespace trims then prefixes", input: "  aerial  ", expected: []string{"aerial", "azylman/aerial"}},
		{name: "prefixed repo adds bare candidate", input: "azylman/aerial", expected: []string{"azylman/aerial", "aerial"}},
		{name: "other org repo returns single candidate", input: "other-org/aerial", expected: []string{"other-org/aerial"}},
		{name: "nested slash returns single candidate", input: "azylman/sub/repo", expected: []string{"azylman/sub/repo"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := RepoMatchCandidates(tc.input)
			if len(got) != len(tc.expected) {
				t.Fatalf("RepoMatchCandidates(%q) = %v, want %v", tc.input, got, tc.expected)
			}
			for i := range tc.expected {
				if got[i] != tc.expected[i] {
					t.Fatalf("RepoMatchCandidates(%q) = %v, want %v", tc.input, got, tc.expected)
				}
			}
		})
	}
}

type scanChunksCase struct {
	name       string
	items      []model.VanadisChunk
	scanErr    error
	rowsErr    error
	wantTotal  int
	wantEffort string
	wantChecks bool
}

func TestScanChunks(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	complete := time.Date(2026, 10, 16, 18, 15, 0, 0, time.UTC)
	timed := mkChunk("c-1", "b-1", 0, "T1", "I1", "", "")
	timed.StartedAt, timed.CompletedAt = start, complete
	tests := []scanChunksCase{
		{name: "empty rows scan to empty", wantTotal: 0},
		{name: "scan error propagates", items: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "T", "I", "", "")}, scanErr: errors.New("scan down")},
		{name: "rows error propagates", items: []model.VanadisChunk{mkChunk("c-1", "b-1", 0, "T", "I", "", "")}, rowsErr: errors.New("rows down")},
		{name: "times and checks decode", items: []model.VanadisChunk{timed}, wantTotal: 1, wantEffort: "medium", wantChecks: true},
		{name: "low effort preserved", items: []model.VanadisChunk{mkChunk("c-2", "b-1", 1, "T2", "I2", "low", "")}, wantTotal: 1, wantEffort: "low"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ScanChunks(&mockRows{items: tc.items, scanErr: tc.scanErr, rowsErr: tc.rowsErr})
			if tc.scanErr != nil || tc.rowsErr != nil {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tc.wantTotal {
				t.Fatalf("ScanChunks total = %d, want %d", len(got), tc.wantTotal)
			}
			if tc.wantTotal == 0 {
				return
			}
			if got[0].Effort != tc.wantEffort {
				t.Errorf("ScanChunks effort = %q, want %q", got[0].Effort, tc.wantEffort)
			}
			if tc.wantChecks && (got[0].CIChecks == nil || got[0].RolloutState == nil || !got[0].StartedAt.Equal(start) || !got[0].CompletedAt.Equal(complete)) {
				t.Errorf("ScanChunks decoded fields mismatch: %+v", got[0])
			}
		})
	}
}
