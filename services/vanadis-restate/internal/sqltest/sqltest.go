// Package sqltest is the scripted pgx fake shared by vanadis-restate's store,
// build and router tests. A Script declares the rows, routes and errors a
// test wants; Script.DB turns it into a DB that satisfies
// store.PgxQueryExecer structurally. The package never imports
// internal/store so store's in-package tests can use it without a cycle.
package sqltest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Route answers every QueryRow whose SQL contains Match with Row.
type Route struct {
	Match string
	Row   pgx.Row
}

// Script declares how a fake database answers. Zero fields mean "succeed".
type Script struct {
	Routes                      []Route
	Row, BuildRow, CountRow     pgx.Row
	Chunks                      []model.VanadisChunk
	Builds                      []model.VanadisBuild
	BeginErr, QueryErr, ScanErr error
	RowsErr, ExecErr, CommitErr error
	RollbackErr                 error
	// ExecErr fires only for statements containing FailExec ("" matches all).
	FailExec string
	// ExecTag is the command tag every Exec returns (default "UPDATE 1").
	ExecTag string
}

func (s Script) route(query string) pgx.Row {
	for _, r := range s.Routes {
		if strings.Contains(query, r.Match) {
			return r.Row
		}
	}
	return nil
}

func (s Script) queryRow(query string, args []any, candidates *[]string) pgx.Row {
	for _, arg := range args {
		if cands, ok := arg.([]string); ok {
			*candidates = cands
			break
		}
	}
	if strings.Contains(query, "RETURNING ci_fail_count") {
		switch {
		case s.route(query) != nil:
			return s.route(query)
		case s.Row != nil:
			return s.Row
		case s.ExecTag == "UPDATE 0":
			return ErrRow(pgx.ErrNoRows)
		}
		return ValuesRow(1)
	}
	if row := s.route(query); row != nil {
		return row
	}
	if _, values := s.Row.(Values); strings.Contains(query, "COALESCE(pr_number, 0), stall_nudges FROM vanadis_chunks") && len(s.Chunks) > 0 && !values {
		c := s.Chunks[0]
		return ValuesRow(c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Title, c.Status, c.PRNumber, c.StallNudges)
	}
	if strings.HasPrefix(query, "SELECT id FROM vanadis_builds WHERE id = (SELECT build_id") {
		return ValuesRow("b1")
	}
	if s.CountRow != nil && strings.Contains(query, "COUNT(*)") {
		return s.CountRow
	}
	if s.BuildRow != nil && strings.Contains(query, "FROM vanadis_builds") {
		return s.BuildRow
	}
	return s.Row
}

// DB builds the fake database. The returned slice records the last []string
// argument (repo candidates) any QueryRow received.
func (s Script) DB() (*DB, *[]string) {
	candidates := new([]string)
	tag := s.ExecTag
	if tag == "" {
		tag = "UPDATE 1"
	}
	queryRow := func(_ context.Context, query string, args ...any) pgx.Row {
		return s.queryRow(query, args, candidates)
	}
	query := func(context.Context, string, ...any) (pgx.Rows, error) {
		if s.QueryErr != nil {
			return nil, s.QueryErr
		}
		return &Rows{Items: s.Chunks, Builds: s.Builds, ScanErr: s.ScanErr, RowsErr: s.RowsErr}, nil
	}
	exec := func(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
		if s.ExecErr != nil && strings.Contains(query, s.FailExec) {
			return pgconn.CommandTag{}, s.ExecErr
		}
		return pgconn.NewCommandTag(tag), nil
	}
	tx := &Tx{CommitErr: s.CommitErr, RollbackErr: s.RollbackErr, QueryRowFn: queryRow, QueryFn: query, ExecFn: exec}
	return &DB{
		QueryRowFn: queryRow,
		QueryFn:    query,
		ExecFn:     exec,
		BeginFn: func(context.Context) (pgx.Tx, error) {
			if s.BeginErr != nil {
				return nil, s.BeginErr
			}
			return tx, nil
		},
	}, candidates
}

// Values is a row that scans its values positionally into the destinations.
type Values []any

// ValuesRow returns a row scanning vals in order.
func ValuesRow(vals ...any) Values { return Values(vals) }

// Scan implements pgx.Row.
func (v Values) Scan(dest ...any) error { return ScanValues(dest, v...) }

// ScanValues assigns vals to the pointer destinations in order. Nil values
// leave their destination untouched; extra destinations are ignored.
func ScanValues(dest []any, vals ...any) error {
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

// Row scans Build (status-only, 7- or 12-column shape) or returns Err.
type Row struct {
	Build *model.VanadisBuild
	Err   error
}

// ErrRow returns a row whose Scan fails with err.
func ErrRow(err error) *Row { return &Row{Err: err} }

// Scan implements pgx.Row.
func (r *Row) Scan(dest ...any) error {
	switch {
	case r.Err != nil:
		return r.Err
	case r.Build == nil:
		return nil
	case len(dest) == 1:
		return ScanValues(dest, r.Build.Status)
	}
	return FillBuildDest(*r.Build, dest)
}

// FillBuildDest scans b into the 7-column summary or 12-column build shape.
func FillBuildDest(b model.VanadisBuild, dest []any) error {
	if len(dest) == 7 {
		return ScanValues(dest, b.ID, b.ThreadID, b.Tier, b.Title, b.Objective, b.Status, b.StatusMessageID)
	}
	if len(dest) != 12 {
		return fmt.Errorf("scan build: %d destinations, want 12", len(dest))
	}
	agent := b.Agent
	if agent == "" {
		agent = "aerial"
	}
	return ScanValues(dest, b.ID, agent, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet, b.CreatedAt, b.UpdatedAt)
}

// BuildRow is a 12-column build row with the given id and status.
func BuildRow(id, status string) pgx.Row {
	return ValuesRow(id, "aerial", "thread", 3, "title", "objective", "", status, "", false, time.Time{}, time.Time{})
}

// FailRow is the joined chunk+build row FailActionChunk reads. generation
// optionally carries stall_nudges then pr_number.
func FailRow(cStatus, bStatus string, generation ...int) Values {
	now := time.Now()
	nudges, prNumber := 0, 0
	if len(generation) > 0 {
		nudges = generation[0]
	}
	if len(generation) > 1 {
		prNumber = generation[1]
	}
	return ValuesRow(
		"c1", "b1", 0, "action", "", "Fail Chunk Title", "Instructions", []string{}, []string{}, "medium", []string{}, cStatus, prNumber, sql.NullTime{Time: now, Valid: true}, sql.NullTime{Time: now, Valid: true}, now, now, nudges,
		"b1", "thread-1", 2, "Build 1", "Objective 1", "Context", bStatus, "msg-1", now, now,
	)
}

// AttachRoutes answers AttachChunkPR's mode, cancelled-sibling and existence probes.
func AttachRoutes(mode string, exists, cancelled bool) []Route {
	return []Route{
		{Match: "SELECT mode", Row: ValuesRow(mode)},
		{Match: "c.status = 'cancelled'", Row: ValuesRow(cancelled)},
		{Match: "SELECT EXISTS", Row: ValuesRow(exists)},
	}
}

// RecoveryChecks renders the chunk's ci_checks JSON: CIChecks plus any set
// _head_sha/_merge_sha markers; without CIChecks, both markers, or
// {"lint":"passed"} when neither SHA is set.
func RecoveryChecks(c model.VanadisChunk) []byte {
	if len(c.CIChecks) > 0 {
		checks := maps.Clone(c.CIChecks)
		if c.HeadSHA != "" {
			checks["_head_sha"] = c.HeadSHA
		}
		if c.MergeSHA != "" {
			checks["_merge_sha"] = c.MergeSHA
		}
		return marshal(checks)
	}
	if c.HeadSHA == "" && c.MergeSHA == "" {
		return []byte(`{"lint":"passed"}`)
	}
	return marshal(map[string]string{"_head_sha": c.HeadSHA, "_merge_sha": c.MergeSHA})
}

// marshal renders v as JSON, or the error text for unencodable values.
func marshal(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return []byte(err.Error())
	}
	return raw
}

// Rows iterates Builds (when Items is empty) or Items, scanning by column count.
type Rows struct {
	pgx.Rows

	Items            []model.VanadisChunk
	Builds           []model.VanadisBuild
	ScanErr, RowsErr error
	idx              int
}

// Close implements pgx.Rows.
func (*Rows) Close() {}

// Err implements pgx.Rows.
func (r *Rows) Err() error { return r.RowsErr }

// Next implements pgx.Rows.
func (r *Rows) Next() bool {
	if len(r.Builds) > 0 && len(r.Items) == 0 {
		r.idx++
		return r.idx <= len(r.Builds)
	}
	if r.idx < len(r.Items) {
		r.idx++
		return true
	}
	return false
}

func (r *Rows) current() (model.VanadisChunk, bool) {
	if r.idx > 0 && r.idx <= len(r.Items) {
		return r.Items[r.idx-1], true
	}
	return model.VanadisChunk{}, false
}

func nullTime(t time.Time) sql.NullTime { return sql.NullTime{Time: t, Valid: !t.IsZero()} }

// Scan implements pgx.Rows for the 2/3-column id, 10-column summary,
// 12-column build, 18-column chunk and 25/26-column full chunk shapes.
func (r *Rows) Scan(dest ...any) error {
	if r.ScanErr != nil {
		return r.ScanErr
	}
	c, ok := r.current()
	switch len(dest) {
	case 2, 3:
		if ok {
			return ScanValues(dest, c.BuildID, c.ID)
		}
	case 10:
		if ok {
			return ScanValues(dest, c.ID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Status, c.PRNumber, c.StartedAt, c.CompletedAt, c.UpdatedAt)
		}
	case 12:
		if len(r.Builds) > 0 && r.idx > 0 && r.idx <= len(r.Builds) {
			return FillBuildDest(r.Builds[r.idx-1], dest)
		}
		if ok {
			return FillBuildDest(model.VanadisBuild{ID: c.BuildID, Title: c.Title}, dest)
		}
	case 18:
		if ok {
			return ScanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, nullTime(c.StartedAt), nullTime(c.CompletedAt), c.CreatedAt, c.UpdatedAt)
		}
	case 25, 26:
		return scanFull(dest, c, ok)
	}
	return nil
}

func scanFull(dest []any, c model.VanadisChunk, ok bool) error {
	if !ok {
		return ScanValues(dest, "c1", "b1", 0, "pr", "aerial", "c1", "inst", []string{}, []string{}, "medium", []string{}, "pending", 0, 0, []string{}, 0, []byte("{}"), []byte("{}"), "", "", "", sql.NullTime{}, sql.NullTime{}, time.Now(), time.Now())
	}
	rollout := []byte(`{"expected_jobs":["eri"]}`)
	if c.RolloutState != nil {
		rollout = marshal(c.RolloutState)
	}
	return ScanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, RecoveryChecks(c), rollout, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, nullTime(c.StartedAt), nullTime(c.CompletedAt), c.CreatedAt, c.UpdatedAt)
}

// Tx is a pgx.Tx whose statements go to the Fn fields (defaults succeed).
type Tx struct {
	pgx.Tx

	ExecFn                 func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryFn                func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRowFn             func(ctx context.Context, sql string, args ...any) pgx.Row
	CommitErr, RollbackErr error
}

// Exec implements pgx.Tx.
func (t *Tx) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if t.ExecFn != nil {
		return t.ExecFn(ctx, query, args...)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

// Query implements pgx.Tx.
func (t *Tx) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	if t.QueryFn != nil {
		return t.QueryFn(ctx, query, args...)
	}
	return &Rows{}, nil
}

// QueryRow implements pgx.Tx.
func (t *Tx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if t.QueryRowFn != nil {
		return t.QueryRowFn(ctx, query, args...)
	}
	return &Row{}
}

// Commit implements pgx.Tx.
func (t *Tx) Commit(context.Context) error { return t.CommitErr }

// Rollback implements pgx.Tx.
func (t *Tx) Rollback(context.Context) error { return t.RollbackErr }

// DB is a store.PgxQueryExecer whose calls go to the Fn fields (defaults succeed).
type DB struct {
	ExecFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	BeginFn    func(ctx context.Context) (pgx.Tx, error)
}

// Exec implements store.PgxQueryExecer.
func (db DB) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if db.ExecFn != nil {
		return db.ExecFn(ctx, query, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

// Query implements store.PgxQueryExecer.
func (db DB) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	if db.QueryFn != nil {
		return db.QueryFn(ctx, query, args...)
	}
	return &Rows{}, nil
}

// QueryRow implements store.PgxQueryExecer.
func (db DB) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if db.QueryRowFn != nil {
		return db.QueryRowFn(ctx, query, args...)
	}
	return &Row{}
}

// Begin implements store.PgxQueryExecer.
func (db DB) Begin(ctx context.Context) (pgx.Tx, error) {
	if db.BeginFn != nil {
		return db.BeginFn(ctx)
	}
	return &Tx{}, nil
}

// AtomicTx models the external database's atomic commit: Finish(true) on
// Commit, Finish(false) on Rollback.
type AtomicTx struct {
	*Tx

	Finish func(commit bool) error
}

// Commit implements pgx.Tx.
func (tx *AtomicTx) Commit(context.Context) error { return tx.Finish(true) }

// Rollback implements pgx.Tx.
func (tx *AtomicTx) Rollback(context.Context) error { return tx.Finish(false) }

// RetryFixtureDB models RetryActionChunk's transaction over one action chunk
// (current), its parent build status and a sibling: it requires the parent
// row lock before the ordered chunk locks, keeps writes private until commit
// and holds mu from the parent lock until the transaction finishes. When
// *lost is true the next commit applies but reports a lost response.
func RetryFixtureDB(mu *sync.Mutex, current *model.VanadisChunk, buildStatus *string, sibling model.VanadisChunk, failBuild bool, lost *bool) DB {
	return DB{BeginFn: func(context.Context) (pgx.Tx, error) {
		var local model.VanadisChunk
		var parent string
		locked, closed := false, false
		base := &Tx{
			QueryRowFn: func(_ context.Context, query string, _ ...any) pgx.Row {
				if !strings.Contains(query, "FROM vanadis_builds") || !strings.Contains(query, "FOR UPDATE") {
					return ErrRow(errors.New("missing parent lock"))
				}
				mu.Lock()
				locked = true
				local, parent = *current, *buildStatus
				return BuildRow("b1", parent)
			},
			QueryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
				if !locked || !strings.Contains(query, "ORDER BY chunk_index ASC FOR UPDATE") {
					return nil, errors.New("missing ordered chunk locks")
				}
				return &Rows{Items: []model.VanadisChunk{sibling, local}}, nil
			},
			ExecFn: func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				return retryExec(query, &local, &parent, failBuild, args...)
			},
		}
		return &AtomicTx{Tx: base, Finish: func(commit bool) error {
			if closed {
				return pgx.ErrTxClosed
			}
			closed = true
			if !locked {
				return errors.New("transaction never locked parent")
			}
			defer mu.Unlock()
			if commit {
				*current, *buildStatus = local, parent
				if *lost {
					*lost = false
					return errors.New("commit response lost")
				}
			}
			return nil
		}}, nil
	}}
}

var retryResets = []string{"status = 'pending'", "stall_nudges = stall_nudges + 1", "repo = ''", "pr_number = NULL", "started_at = NULL", "completed_at = NULL", "ci_fail_count = 0", "ci_checks = '{}'::jsonb", "rollout_state = '{}'::jsonb", "banner = ''", "merge_awakeable_id = ''", "deploy_awakeable_id = ''"}

func retryExec(query string, local *model.VanadisChunk, parent *string, failBuild bool, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(query, "UPDATE vanadis_chunks"):
		for _, fragment := range retryResets {
			if !strings.Contains(query, fragment) {
				return pgconn.CommandTag{}, fmt.Errorf("missing reset %s", fragment)
			}
		}
		if strings.Contains(query, "instructions = COALESCE($2, instructions)") && len(args) == 2 {
			if replacement, ok := args[1].(*string); ok && replacement != nil {
				local.Instructions = *replacement
			}
		}
		local.Status, local.Repo, local.PRNumber = "pending", "", 0
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
