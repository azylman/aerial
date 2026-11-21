package build

// Shared BuildObject test harness retained while orphaned suites consolidate
// here (chunk 6): dbScript/mockRows family (store_test.go), retry fixtures
// (store_action_retry_test.go), recovery fixtures (store_pr_recovery_test.go),
// mockEriClient/launch helpers (executor_test.go) and the unattached-PR spy
// (router_test.go). Only helpers needed by main_test.go,
// main_journal_test.go, pr_attachment_test.go, pr_lifecycle_test.go,
// reconcile_test.go, reconcile_lifecycle_test.go and action_retry_test.go live
// here; relocated Test functions moved into pr_lifecycle_test.go and
// reconcile_lifecycle_test.go.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
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
	tag := strutil.FirstNonEmpty(s.execTag, "UPDATE 1")
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
	if len(dest) != 13 {
		return fmt.Errorf("scan build: %d destinations, want 13", len(dest))
	}
	agent := b.Agent
	if agent == "" {
		agent = "aerial"
	}
	return scanValues(dest, b.ID, agent, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet, b.CreatedAt, b.UpdatedAt, b.RunnerAgent)
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

func (r *mockRows) scan12(dest []any) error {
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
		if c.RolloutState != nil {
			rawRolloutState, _ = json.Marshal(c.RolloutState)
		}
		return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, rawCIChecks, rawRolloutState, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
	}
	return scanValues(dest, "c1", "b1", 0, "pr", "aerial", "c1", "inst", []string{}, []string{}, "medium", []string{}, "pending", 0, 0, []string{}, 0, []byte("{}"), []byte("{}"), "", "", "", sql.NullTime{}, time.Now(), time.Now())
}

func (r *mockRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	switch len(dest) {
	case 2, 3:
		if len(r.items) > 0 && r.idx > 0 && r.idx <= len(r.items) {
			return scanValues(dest, r.items[r.idx-1].BuildID, r.items[r.idx-1].ID)
		}
		return nil
	case 13:
		return r.scan12(dest)
	case 10:
		return r.scan10(dest)
	case 18:
		return r.scan18(dest)
	case 25, 26:
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

func isActionRow(row pgx.Row) bool { _, ok := row.(*genericMockRow); return ok }

// retryTransaction models the external database's atomic commit and parent lock.
// State changes remain private until Commit; Rollback discards them.
type retryTransaction struct {
	*mockTx

	finish func(bool) error
}

func (tx *retryTransaction) Commit(context.Context) error   { return tx.finish(true) }
func (tx *retryTransaction) Rollback(context.Context) error { return tx.finish(false) }

func retryFixtureStore(mu *sync.Mutex, current *model.VanadisChunk, buildStatus *string, sibling model.VanadisChunk, failBuild bool, lost *bool) *store.Store {
	return newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) {
		var local model.VanadisChunk
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
				return &mockRows{items: []model.VanadisChunk{sibling, local}}, nil
			},
			execFn: func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				return retryFixtureExec(query, &local, &parent, failBuild, args...)
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

func retryFixtureExec(query string, local *model.VanadisChunk, parent *string, failBuild bool, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(query, "UPDATE vanadis_chunks"):
		for _, fragment := range []string{"status = 'pending'", "stall_nudges = stall_nudges + 1", "started_at = NULL", "completed_at = NULL", "ci_fail_count = 0", "ci_checks = '{}'::jsonb", "rollout_state = '{}'::jsonb", "banner = ''", "merge_awakeable_id = ''", "deploy_awakeable_id = ''"} {
			if !strings.Contains(query, fragment) {
				return pgconn.CommandTag{}, fmt.Errorf("missing reset %s", fragment)
			}
		}
		if strings.Contains(query, "instructions = COALESCE($2, instructions)") && len(args) == 2 {
			if replacement, ok := args[1].(*string); ok && replacement != nil {
				local.Instructions = *replacement
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

func buildRow(id, status string) pgx.Row {
	return valuesRow(id, "aerial", "thread", 3, "title", "objective", "", status, "", false, time.Time{}, time.Time{})
}

// The database boundary keeps transaction writes private until commit.
type recoveryReadFailureStore struct{ *store.Store }

func (recoveryReadFailureStore) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return nil, nil, errors.New("read unavailable")
}

type recoveryLifecycleStore struct {
	*store.Store

	parent *string
	chunks *[]model.VanadisChunk
}

func (s recoveryLifecycleStore) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: "b1", Status: *s.parent}, *s.chunks, nil
}

func (s recoveryLifecycleStore) FailChunk(context.Context, string, string, string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	*s.parent = "failed"
	(*s.chunks)[0].Status = "failed"
	return &model.VanadisBuild{ID: "b1", Status: "failed"}, &(*s.chunks)[0], true, nil
}

func recoveryFixtureStore(mu *sync.Mutex, parent *string, chunks *[]model.VanadisChunk, failParent bool) *store.Store {
	return newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}, beginFn: func(context.Context) (pgx.Tx, error) {
		var local []model.VanadisChunk
		var state string
		locked, closed := false, false
		tx := &mockTx{
			queryRowFn: func(_ context.Context, q string, _ ...any) pgx.Row {
				if strings.Contains(q, "SELECT build_id") {
					return valuesRow("b1")
				}
				mu.Lock()
				locked = true
				state = *parent
				local = append([]model.VanadisChunk(nil), (*chunks)...)
				return buildRow("b1", state)
			}, queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{items: local}, nil },
			execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
				if !locked {
					return pgconn.CommandTag{}, errors.New("write without lock")
				}
				if strings.Contains(q, "UPDATE vanadis_chunks") {
					applyRecoveryFixtureHead(local, args, strings.Contains(q, "SET ci_checks = COALESCE"))
				} else {
					if failParent {
						return pgconn.CommandTag{}, errors.New("parent write failed")
					}
					state = "running"
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		}
		return &retryTransaction{mockTx: tx, finish: func(commit bool) error {
			if closed {
				return pgx.ErrTxClosed
			}
			closed = true
			if locked {
				defer mu.Unlock()
			}
			if commit {
				*chunks = local
				*parent = state
			}
			return nil
		}}, nil
	}})
}

func recoveryFixtureChecks(c model.VanadisChunk) []byte {
	if len(c.CIChecks) > 0 {
		checks := maps.Clone(c.CIChecks)
		if c.HeadSHA != "" {
			checks["_head_sha"] = c.HeadSHA
		}
		if c.MergeSHA != "" {
			checks["_merge_sha"] = c.MergeSHA
		}
		raw, _ := json.Marshal(checks)
		return raw
	}
	if c.HeadSHA == "" && c.MergeSHA == "" {
		return []byte(`{"lint":"passed"}`)
	}
	checks := map[string]string{"_head_sha": c.HeadSHA, "_merge_sha": c.MergeSHA}
	raw, _ := json.Marshal(checks)
	return raw
}

func applyRecoveryFixtureHead(chunks []model.VanadisChunk, args []any, baseline bool) {
	for i := range chunks {
		if chunks[i].ID == args[0] {
			if !baseline {
				chunks[i].Status = "pr_opened"
			}
			chunks[i].HeadSHA, _ = args[1].(string)
		}
	}
}

type mockEriClient struct {
	dispatchFn func(ctx context.Context, req executor.BrainDispatchRequest) error
	cancelFn   func(ctx context.Context, chunkID string) error
	promptFn   func(ctx context.Context, threadID, prompt string) error
	widgetFn   func(ctx context.Context, req executor.StatusWidgetRequest) (string, error)
	messageFn  func(ctx context.Context, channelID, content string) error

	lastDispatch *executor.BrainDispatchRequest
	lastCancel   string
	cancelAgent  string
	lastThreadID string
	lastPrompt   string
	lastWidget   *executor.StatusWidgetRequest
	lastMsgChan  string
	lastMsgCont  string
}

func (m *mockEriClient) DispatchAccepted(ctx context.Context, req executor.BrainDispatchRequest) error {
	return m.Dispatch(ctx, req)
}

func (m *mockEriClient) Dispatch(ctx context.Context, req executor.BrainDispatchRequest) error {
	m.lastDispatch = &req
	if m.dispatchFn != nil {
		return m.dispatchFn(ctx, req)
	}
	return nil
}

func (m *mockEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	m.lastCancel = chunkID
	m.cancelAgent = ericlient.AgentFromContext(ctx)
	if m.cancelFn != nil {
		return m.cancelFn(ctx, chunkID)
	}
	return nil
}

func (m *mockEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	m.lastThreadID = threadID
	m.lastPrompt = prompt
	if m.promptFn != nil {
		return m.promptFn(ctx, threadID, prompt)
	}
	return nil
}

func (m *mockEriClient) Widget(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
	m.lastWidget = &req
	if m.widgetFn != nil {
		return m.widgetFn(ctx, req)
	}
	return "msg-123", nil
}

func (m *mockEriClient) Message(ctx context.Context, channelID, content string) error {
	m.lastMsgChan = channelID
	m.lastMsgCont = content
	if m.messageFn != nil {
		return m.messageFn(ctx, channelID, content)
	}
	return nil
}

type mockStoreForLaunch struct {
	store.VanadisStore

	reserveErr error
}

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockStoreForLaunch) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	return m.reserveErr
}

func (m *mockBusExecutor) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.lastSubj = subject
	return nil
}

func launchRecoveryServer(t *testing.T, id, first string, ready *atomic.Bool) (*httptest.Server, chan string) {
	t.Helper()
	ids := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dispatch" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var req executor.BrainDispatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case ids <- req.ID:
		default:
			t.Error("unexpected repeated dispatch")
		}
		if req.ID == id && !ready.Load() {
			switch first {
			case "active":
				_, _ = w.Write([]byte(`{"status":"skipped_active"}`))
			case "lost":
				w.Header().Set("Content-Length", "100")
				_, _ = w.Write([]byte(`{"status":"accepted"`))
			default:
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		status := "accepted"
		if first == "lost" {
			status = "already_exists"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "dispatch_id": req.ID})
	}))
	return srv, ids
}

type unattachedPREventSpyStore struct {
	store.VanadisStore

	chunks          []model.VanadisChunk
	getBuildErr     error
	attachErr       error
	attachedChunkID string
	attachedPR      int
	attachedRepo    string
	prStatusUpdated string
	cStatus         string
	banner          string
	failedChunkID   string
	syncWidgetCalls int
}

func (*unattachedPREventSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*store.VanadisChunkPRLookup, error) {
	return &store.VanadisChunkPRLookup{Matched: false}, nil
}

func (s *unattachedPREventSpyStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	return &model.VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, s.chunks, nil
}

func (s *unattachedPREventSpyStore) AttachChunkPR(_ context.Context, req store.AttachChunkPRRequest) (string, string, string, error) {
	if s.attachErr != nil {
		return "", "", "", s.attachErr
	}
	s.attachedChunkID = req.ChunkID
	s.attachedPR = req.PRNumber
	s.attachedRepo = req.Repo
	return "b1", req.ChunkID, "th-1", nil
}

func (s *unattachedPREventSpyStore) UpdatePRLifecycleStatus(_ context.Context, chunkID, status string) error {
	s.prStatusUpdated = chunkID + ":" + status
	return nil
}

func (*unattachedPREventSpyStore) RecordPRCheckStatus(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ResetPRCheckState(_ context.Context, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) RecordPRRolloutState(_ context.Context, _, _ string, _ *pubsub.PRRolloutState) error {
	return nil
}

func (s *unattachedPREventSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.cStatus = chunkID + ":" + status
	s.banner = banner
	return nil
}

func (s *unattachedPREventSpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	s.syncWidgetCalls++
	return nil
}

func (*unattachedPREventSpyStore) UpdateBuildStatus(_ context.Context, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ReserveChunkForLaunch(_ context.Context, _ string) error {
	return nil
}

func (s *unattachedPREventSpyStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string, _ ...string) (int, error) {
	s.cStatus = chunkID + ":ci_failed"
	s.banner = banner
	return 1, nil
}

func (s *unattachedPREventSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failedChunkID = chunkID + ":" + reason
	return &model.VanadisBuild{ID: "b1", Status: statusFailed, ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, Status: statusFailed}, true, nil
}
