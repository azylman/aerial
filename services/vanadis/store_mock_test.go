package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type mockRow struct {
	b   *VanadisBuild
	err error
}

// fillBuildDest answers scanBuild's 11 destinations; any other shape is a drifted query.
func fillBuildDest(b VanadisBuild, dest []any) error {
	if len(dest) != 11 {
		return fmt.Errorf("scan build: %d destinations, want 11", len(dest))
	}
	return scanValues(dest, b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet, b.CreatedAt, b.UpdatedAt)
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.b != nil {
		return fillBuildDest(*r.b, dest)
	}
	return nil
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
		return fillBuildDest(r.builds[r.idx-1], dest)
	}
	if len(r.items) > 0 {
		c := r.items[r.idx-1]
		if len(dest) == 10 {
			return scanValues(dest, c.ID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Status, c.PRNumber, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.UpdatedAt)
		}
		if len(dest) == 18 {
			return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
		}
		if len(dest) == 25 {
			rawCIChecks := []byte(`{"lint":"passed"}`)
			rawRolloutState := []byte(`{"expected_jobs":["eri"]}`)
			return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, rawCIChecks, rawRolloutState, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
		}
	}
	return nil
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
		return checkedQuery(query)(t.queryFn(ctx, query, args...))
	}
	return &mockRows{}, nil
}

func (t *mockTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if t.queryRowFn != nil {
		return checkedQueryRow(query, t.queryRowFn(ctx, query, args...))
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

func (db *mockDB) Exec(ctx context.Context, s string, a ...any) (pgconn.CommandTag, error) {
	if db.execFn != nil {
		return db.execFn(ctx, s, a...)
	}
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
	return &mockRow{}
}

func (db *mockDB) Begin(ctx context.Context) (pgx.Tx, error) {
	if db.beginFn != nil {
		return db.beginFn(ctx)
	}
	return &mockTx{}, nil
}

type memoryVanadisStore struct {
	builds                     map[string]VanadisBuild
	chunks                     map[string][]VanadisChunk
	createErr, getErr, listErr error
}

func newMemoryVanadisStore() *memoryVanadisStore {
	return &memoryVanadisStore{builds: make(map[string]VanadisBuild), chunks: make(map[string][]VanadisChunk)}
}

func (m *memoryVanadisStore) CreateBuild(_ context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if m.createErr != nil {
		return m.createErr
	}
	normChunks, err := normalizeAndValidateBuildDAG(chunks)
	if err != nil {
		return err
	}
	m.builds[build.ID], m.chunks[build.ID] = build, normChunks
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

func (m *memoryVanadisStore) GetChunk(_ context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	for bID, list := range m.chunks {
		for _, c := range list {
			if c.ID == chunkID {
				b, ok := m.builds[bID]
				if !ok {
					return nil, nil, ErrBuildNotFound
				}
				return &c, &b, nil
			}
		}
	}
	return nil, nil, ErrChunkNotFound
}

func reposMatch(a, b string) bool {
	if b = normalizeRepo(b); b == "" {
		return false
	}
	return slices.Contains(repoMatchCandidates(a), b)
}

func (m *memoryVanadisStore) GetChunkByPR(_ context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return &VanadisChunkPRLookup{Matched: false}, nil
	}
	for bID, list := range m.chunks {
		b := m.builds[bID]
		if b.Status == "cancelled" {
			continue
		}
		for _, c := range list {
			if reposMatch(c.Repo, repo) && c.PRNumber == prNumber {
				return &VanadisChunkPRLookup{
					Matched: true,
					ChunkID: c.ID,
					BuildID: bID,
					Effort:  capChunkEffort(c.Effort),
				}, nil
			}
		}
	}
	return &VanadisChunkPRLookup{Matched: false}, nil
}

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
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: normalizeRepo("aerial"), Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}, DependsOn: []string{}}
}
