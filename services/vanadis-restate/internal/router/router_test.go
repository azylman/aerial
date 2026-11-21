package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return nil }

func runMockRunContext(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
	res, err := fn(mockRunContext{Context: context.Background()})
	if err != nil {
		return restate.TerminalErrorf("%v", err)
	}
	if output != nil && res != nil {
		outVal := reflect.ValueOf(output)
		resVal := reflect.ValueOf(res)
		if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
			outVal.Elem().Set(resVal)
		}
	}
	return nil
}

type metadataMutationStore struct {
	store.VanadisStore

	err error
}

func (s *metadataMutationStore) UpdateBuild(_ context.Context, id string, req store.UpdateBuildRequest) (*model.VanadisBuild, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &model.VanadisBuild{ID: id, Title: req.Title}, nil
}

func (s *metadataMutationStore) LookupChunkBuildID(context.Context, string) (store.BuildRoute, error) {
	return store.BuildRoute{}, s.err
}

func (s *metadataMutationStore) LookupPRChunk(context.Context, string, int) (store.BuildRoute, string, error) {
	return store.BuildRoute{}, "", s.err
}

func observeJournalCallback(t *testing.T, mc *mocks.MockContext, want error, code restate.Code) {
	t.Helper()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if code != 0 {
			var terminal restate.TerminalError
			require.ErrorAs(t, err, &terminal)
			assert.Equal(t, code, terminal.Code())
		} else {
			assert.False(t, restate.IsTerminalError(err))
		}
		switch {
		case want != nil && code != 0:
			assert.EqualError(t, err, want.Error())
		case want != nil:
			assert.ErrorIs(t, err, want)
		default:
			assert.NoError(t, err)
		}
		// Only adapt after observing the raw callback error above.
		return runMockRunContext(func(restate.RunContext) (any, error) { return res, err }, output, opts...)
	})
}

func TestRouterJournalLookupErrors(t *testing.T) {
	t.Parallel()
	transient := errors.New("storage unavailable")
	tests := []struct {
		name        string
		storageErr  error
		callbackErr error
	}{
		{"missing build", store.ErrBuildNotFound, nil},
		{"missing chunk", store.ErrChunkNotFound, nil},
		{"storage retry", transient, transient},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			router := &BuildRouterService{
				store:          &metadataMutationStore{err: tc.storageErr},
				invokerFactory: func(restate.Context) BuildObjectInvoker { return inv },
			}
			prContext := mocks.NewMockContext(t)
			observeJournalCallback(t, prContext, tc.callbackErr, 0)
			prErr := router.RoutePREvent(restate.WithMockContext(prContext), pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1})
			dispatchContext := mocks.NewMockContext(t)
			observeJournalCallback(t, dispatchContext, tc.callbackErr, 0)
			dispatchErr := router.RouteDispatchOutcome(restate.WithMockContext(dispatchContext), pubsub.DispatchOutcomeEvent{VanadisChunkID: "chunk"})
			attachContext := mocks.NewMockContext(t)
			observeJournalCallback(t, attachContext, tc.callbackErr, 0)
			_, attachErr := router.RouteAttachPR(restate.WithMockContext(attachContext), store.AttachChunkPRRequest{ChunkID: "chunk", Repo: "owner/repo", PRNumber: 1})
			if tc.callbackErr == nil {
				assert.NoError(t, prErr)
				assert.NoError(t, dispatchErr)
				assert.NoError(t, attachErr)
			} else {
				assert.Error(t, prErr)
				assert.Error(t, dispatchErr)
				assert.Error(t, attachErr)
			}
		})
	}
}

// Exercise the database classification through the actual persistence callback,
// before the SDK adapter can turn retryable errors into terminal mock results.

func TestBuildRouterService_ReflectionAndInterface(t *testing.T) {
	t.Parallel()
	r := NewBuildRouterService(nil)
	assert.Equal(t, "BuildRouter", r.ServiceName())
	assert.Equal(t, "BuildRouter", (*BuildRouterService)(nil).ServiceName())

	require.NotPanics(t, func() {
		def := restate.Reflect(r)
		require.NotNil(t, def)
	})
}

func TestRestateBuildObjectInvoker(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)

	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Maybe()

	inv := restateBuildObjectInvoker{ctx: restate.WithMockContext(mc)}

	ctx := context.Background()
	_ = inv.RoutePREvent(ctx, "", "b1", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 1})
	_ = inv.RouteDispatchOutcome(ctx, "", "b1", pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"})
	_, _ = inv.RouteAttachPR(ctx, "", "b1", store.AttachChunkPRRequest{PRNumber: 1})
	_, _ = inv.RouteFailChunk(ctx, "", "b1", model.FailChunkRequest{ChunkID: "c1"})
	_ = inv.RouteReconcile(ctx, "", "b1", build.ReconcileBuildRequest{Reason: build.ReconcileReasonWidgetSync})
}

func TestBuildRouterService_InvokerWiring(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		routerAgent string
		runner      string
		wantService string
	}{
		{name: "empty runner targets legacy Build", routerAgent: "", runner: "", wantService: "Build"},
		{name: "default runner targets legacy Build", routerAgent: "", runner: "aerial", wantService: "Build"},
		{name: "sibling runner targets scoped Build", routerAgent: "", runner: "agent-alpha", wantService: "Build-agent-alpha"},
		{name: "mixed case runner normalized", routerAgent: "", runner: " Agent-Alpha ", wantService: "Build-agent-alpha"},
		{name: "router agent never overrides runner", routerAgent: "agent-beta", runner: "aerial", wantService: "Build"},
		{name: "scoped router targets other runner", routerAgent: "agent-beta", runner: "agent-alpha", wantService: "Build-agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			mClient := mocks.NewMockClient(t)
			mInv := mocks.NewMockInvocation(t)
			for _, handler := range []string{"OnPREvent", "OnDispatchOutcome", "AttachPR", "FailChunk", "Reconcile"} {
				mc.EXPECT().Object(tc.wantService, "b1", handler).Return(mClient).Once()
			}
			mClient.EXPECT().Send(mock.Anything).Return(mInv).Times(3)
			mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Times(2)
			r := NewBuildRouterServiceWithAgent(nil, tc.routerAgent)
			inv, ok := r.getInvoker(restate.WithMockContext(mc)).(restateBuildObjectInvoker)
			require.True(t, ok)
			ctx := context.Background()
			require.NoError(t, inv.RoutePREvent(ctx, tc.runner, "b1", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 1}))
			require.NoError(t, inv.RouteDispatchOutcome(ctx, tc.runner, "b1", pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"}))
			_, _ = inv.RouteAttachPR(ctx, tc.runner, "b1", store.AttachChunkPRRequest{PRNumber: 1})
			_, _ = inv.RouteFailChunk(ctx, tc.runner, "b1", model.FailChunkRequest{ChunkID: "c1"})
			require.NoError(t, inv.RouteReconcile(ctx, tc.runner, "b1", build.ReconcileBuildRequest{Reason: build.ReconcileReasonWidgetSync}))
		})
	}
}

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
		return scanValues(dest, c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, rawCIChecks, rawRolloutState, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt)
	}
	return scanValues(dest, "c1", "b1", 0, "pr", "aerial", "c1", "inst", []string{}, []string{}, "medium", []string{}, "pending", 0, 0, []string{}, 0, []byte("{}"), []byte("{}"), "", "", "", sql.NullTime{}, sql.NullTime{}, time.Now(), time.Now())
}

func (r *mockRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	switch len(dest) {
	case 3:
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

func recoveryFixtureChecks(c model.VanadisChunk) []byte {
	if c.HeadSHA == "" && c.MergeSHA == "" {
		return []byte(`{"lint":"passed"}`)
	}
	checks := map[string]string{"_head_sha": c.HeadSHA, "_merge_sha": c.MergeSHA}
	raw, _ := json.Marshal(checks)
	return raw
}

func isActionRow(row pgx.Row) bool { _, ok := row.(*genericMockRow); return ok }

func routerTestMetricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func routerErrorMetricValue(t *testing.T, metric, handler string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	routerTestMetricsMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	prefix := fmt.Sprintf(`%s{handler=%q} `, metric, handler)
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if valStr, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			val, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
			require.NoError(t, err)
			return val
		}
	}
	return 0
}

func routerReconcileErrorsCount(t *testing.T, handler string) float64 {
	t.Helper()
	return routerErrorMetricValue(t, "vanadis_restate_reconcile_errors_total", handler)
}

type mockInvoker struct {
	prEventFn         func(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error
	dispatchOutcomeFn func(ctx context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) error
	attachPRFn        func(ctx context.Context, buildID string, req store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error)
	failChunkFn       func(ctx context.Context, buildID string, req model.FailChunkRequest) (model.FailChunkResponse, error)

	lastPRBuildID        string
	lastDispatchBuildID  string
	lastAttachBuildID    string
	lastFailBuildID      string
	lastReconcileBuildID string
	lastRunner           string
	lastReconcileReason  string
}

func (m *mockInvoker) RoutePREvent(ctx context.Context, runner, buildID string, ev pubsub.PRLifecycleEvent) error {
	m.lastPRBuildID, m.lastRunner = buildID, runner
	if m.prEventFn != nil {
		return m.prEventFn(ctx, buildID, ev)
	}
	return nil
}

func (m *mockInvoker) RouteDispatchOutcome(ctx context.Context, runner, buildID string, ev pubsub.DispatchOutcomeEvent) error {
	m.lastDispatchBuildID, m.lastRunner = buildID, runner
	if m.dispatchOutcomeFn != nil {
		return m.dispatchOutcomeFn(ctx, buildID, ev)
	}
	return nil
}

func (m *mockInvoker) RouteAttachPR(ctx context.Context, runner, buildID string, req store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error) {
	m.lastAttachBuildID, m.lastRunner = buildID, runner
	if m.attachPRFn != nil {
		return m.attachPRFn(ctx, buildID, req)
	}
	return model.AttachChunkPRResponse{BuildID: buildID, ChunkID: "c1", Status: "attached"}, nil
}

func (m *mockInvoker) RouteFailChunk(ctx context.Context, runner, buildID string, req model.FailChunkRequest) (model.FailChunkResponse, error) {
	m.lastFailBuildID, m.lastRunner = buildID, runner
	if m.failChunkFn != nil {
		return m.failChunkFn(ctx, buildID, req)
	}
	return model.FailChunkResponse{BuildID: buildID, ChunkID: req.ChunkID, Failed: true}, nil
}

func (m *mockInvoker) RouteReconcile(_ context.Context, runner, buildID string, req build.ReconcileBuildRequest) error {
	m.lastReconcileBuildID, m.lastRunner, m.lastReconcileReason = buildID, runner, req.Reason
	return nil
}

type routerMockRunContext struct {
	context.Context
}

func (routerMockRunContext) Log() *slog.Logger         { return slog.Default() }
func (routerMockRunContext) Request() *restate.Request { return nil }

func runRouterMockRunContext(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
	res, err := fn(routerMockRunContext{Context: context.Background()})
	if err != nil {
		return restate.TerminalErrorf("%v", err)
	}
	if output != nil && res != nil {
		outVal, resVal := reflect.ValueOf(output), reflect.ValueOf(res)
		if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
			outVal.Elem().Set(resVal)
		}
	}
	return nil
}

func makeRouterMockContext(t *testing.T) restate.Context {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe() // slog handlers look up the span context.
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runRouterMockRunContext).Maybe()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Maybe()
	return restate.WithMockContext(mc)
}

func TestBuildRouterService_RoutePREvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		ev          pubsub.PRLifecycleEvent
		db          dbScript
		wantBuildID string
		wantRunner  string
		wantErr     bool
	}{
		{
			name:    "empty repo",
			ev:      pubsub.PRLifecycleEvent{Repo: "", PRNumber: 101},
			wantErr: true,
		},
		{
			name:    "zero pr number",
			ev:      pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 0},
			wantErr: true,
		},
		{
			name:        "primary lookup matched",
			ev:          pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"},
			db:          dbScript{row: valuesRow("c1", "b-matched", 0, "medium", false)},
			wantBuildID: "b-matched",
			wantRunner:  "aerial",
		},
		{name: "primary lookup routes to sibling runner", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"}, db: dbScript{row: valuesRow("c1", "b-alpha", 0, "medium", false, "agent-alpha")}, wantBuildID: "b-alpha", wantRunner: "agent-alpha"},
		{
			name: "branch-based fallback resolution",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 102,
				Event:    "opened",
				Branch:   "feat/aerial-pr-vanadis-chunk-1-router-a1b2c3",
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				routes: []sqlRoute{{match: "FROM vanadis_chunks c JOIN vanadis_builds b ON b.id = c.build_id WHERE c.id = $1", row: valuesRow("b-branch", "agent-alpha")}},
			},
			wantBuildID: "b-branch",
			wantRunner:  "agent-alpha",
		},
		{
			name: "thread and repo fallback resolution",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 103,
				Event:    "opened",
				Branch:   "feat/aerial-pr-20261116-123456",
				TargetID: "thread-123",
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				chunks: []model.VanadisChunk{{BuildID: "b-thread", ID: "c1"}},
			},
			wantBuildID: "b-thread",
			wantRunner:  "aerial",
		},
		{
			name: "dependabot guard exclusion",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 104,
				Event:    "opened",
				Branch:   "refs/heads/dependabot/npm_and_yarn/foo",
				TargetID: "thread-123",
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				chunks: []model.VanadisChunk{{BuildID: "b-thread", ID: "c1"}},
			},
			wantBuildID: "",
		},
		{
			name: "quiet guard exclusion",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 105,
				Event:    "opened",
				Branch:   "feat/something",
				TargetID: "thread-123",
				Quiet:    true,
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				chunks: []model.VanadisChunk{{BuildID: "b-thread", ID: "c1"}},
			},
			wantBuildID: "",
		},
		{
			name: "unmatched no-op routing",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 106,
				Event:    "opened",
				Branch:   "feat/something",
			},
			db:          dbScript{row: errRow(pgx.ErrNoRows)},
			wantBuildID: "",
		},
		{
			name: "branch-based lookup error propagates",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 107,
				Event:    "opened",
				Branch:   "feat/aerial-pr-vanadis-chunk-1-router-a1b2c3",
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				routes: []sqlRoute{{match: "FROM vanadis_chunks c JOIN vanadis_builds b ON b.id = c.build_id WHERE c.id = $1", row: errRow(errors.New("lookup err"))}},
			},
			wantErr: true,
		},
		{
			name: "thread-based lookup error propagates",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 108,
				Event:    "opened",
				Branch:   "feat/aerial-pr-20261116-123456",
				TargetID: "thread-123",
			},
			db: dbScript{
				row:      errRow(pgx.ErrNoRows),
				queryErr: errors.New("lookup err"),
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			st, _ := tc.db.open()
			router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })
			ctx := makeRouterMockContext(t)
			err := router.RoutePREvent(ctx, tc.ev)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantBuildID, inv.lastPRBuildID)
				assert.Equal(t, tc.wantRunner, inv.lastRunner)
			}
		})
	}
}

func TestBuildRouterService_RouteDispatchOutcome(t *testing.T) {
	t.Parallel()
	inv := &mockInvoker{}
	st, _ := dbScript{row: valuesRow("b-dispatch")}.open()
	router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })

	ctx := makeRouterMockContext(t)

	// Empty chunk id -> no-op
	err := router.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{})
	assert.NoError(t, err)

	// Matched chunk
	err = router.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"})
	assert.NoError(t, err)
	assert.Equal(t, "b-dispatch", inv.lastDispatchBuildID)

	// Unmatched chunk -> no-op
	storeNotFound, _ := dbScript{row: errRow(store.ErrChunkNotFound)}.open()
	routerNotFound := NewBuildRouterService(storeNotFound, func(restate.Context) BuildObjectInvoker { return inv })
	inv.lastDispatchBuildID = ""
	err = routerNotFound.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c-unknown"})
	assert.NoError(t, err)
	assert.Empty(t, inv.lastDispatchBuildID)
}

func TestBuildRouterService_RouteAttachPR(t *testing.T) {
	t.Parallel()
	inv := &mockInvoker{}
	st, _ := dbScript{row: valuesRow("b-existing")}.open()
	router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })

	ctx := makeRouterMockContext(t)

	// Validation failure
	_, err := router.RouteAttachPR(ctx, store.AttachChunkPRRequest{})
	assert.Error(t, err)

	// Route attach with chunk id
	res, err := router.RouteAttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "b-existing", res.BuildID)

	// Route attach without chunk id, but with repo lookup returning build id
	storeRepo, _ := dbScript{row: valuesRow("c1", "b-repo", 0, "medium", false)}.open()
	routerRepo := NewBuildRouterService(storeRepo, func(restate.Context) BuildObjectInvoker { return inv })
	resRepo, err := routerRepo.RouteAttachPR(ctx, store.AttachChunkPRRequest{PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "b-repo", resRepo.BuildID)

	// Route attach standalone new build id
	storeUnmatched, _ := dbScript{row: errRow(store.ErrChunkNotFound)}.open()
	routerUnmatched := NewBuildRouterService(storeUnmatched, func(restate.Context) BuildObjectInvoker { return inv })
	resNew, err := routerUnmatched.RouteAttachPR(ctx, store.AttachChunkPRRequest{PRNumber: 999, Repo: "aerial"})
	assert.NoError(t, err)
	assert.True(t, resNew.BuildID != "")
}

func TestBuildRouterService_RouteFailChunk(t *testing.T) {
	t.Parallel()
	inv := &mockInvoker{}
	st, _ := dbScript{row: valuesRow("b-fail")}.open()
	router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })

	ctx := makeRouterMockContext(t)

	// Validation failure
	_, err := router.RouteFailChunk(ctx, model.FailChunkRequest{})
	assert.Error(t, err)

	// Chunk not found
	storeNotFound, _ := dbScript{row: errRow(store.ErrChunkNotFound)}.open()
	routerNotFound := NewBuildRouterService(storeNotFound, func(restate.Context) BuildObjectInvoker { return inv })
	_, err = routerNotFound.RouteFailChunk(ctx, model.FailChunkRequest{ChunkID: "c-missing", Reason: "boom"})
	assert.Error(t, err)

	tests := []struct {
		name, buildID, dispatchID string
		wantErr                   bool
	}{
		{"legacy", "", "", false},
		{"current generation", "b-fail", "vanadis-c1-turn-1", false},
		{"wrong owner", "other", "vanadis-c1-turn-1", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })
			inv.failChunkFn = func(_ context.Context, buildID string, req model.FailChunkRequest) (model.FailChunkResponse, error) {
				assert.Equal(t, tc.dispatchID, req.DispatchID)
				return model.FailChunkResponse{BuildID: buildID, Failed: true}, nil
			}
			res, err := router.RouteFailChunk(makeRouterMockContext(t), model.FailChunkRequest{BuildID: tc.buildID, ChunkID: "c1", Reason: "boom", DispatchID: tc.dispatchID})
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, "b-fail", res.BuildID)
				assert.True(t, res.Failed)
			}
		})
	}
}

func TestBuildRouterService_NilStoreAndErrorPaths(t *testing.T) {
	rNil := NewBuildRouterService(nil)
	ctx := makeRouterMockContext(t)

	// RoutePREvent nil store
	assert.NoError(t, rNil.RoutePREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "opened"}))

	// RoutePREvent query error records metric inside journaled callback
	storeErr, _ := dbScript{row: errRow(errors.New("db query error"))}.open()
	rErr := NewBuildRouterService(storeErr)
	beforePR := routerReconcileErrorsCount(t, "RoutePREvent")
	assert.Error(t, rErr.RoutePREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"}))
	assert.Equal(t, beforePR+1, routerReconcileErrorsCount(t, "RoutePREvent"))

	// RouteDispatchOutcome nil store
	assert.NoError(t, rNil.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"}))

	// RouteDispatchOutcome query error records metric inside journaled callback
	beforeDisp := routerReconcileErrorsCount(t, "RouteDispatchOutcome")
	assert.Error(t, rErr.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"}))
	assert.Equal(t, beforeDisp+1, routerReconcileErrorsCount(t, "RouteDispatchOutcome"))

	// RouteAttachPR nil store generates build ID
	_, err := rNil.RouteAttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)

	// RouteAttachPR standalone matching existing PR
	storeAttach, _ := dbScript{row: valuesRow("c-new", "b-new", 0, "medium", false)}.open()
	inv := &mockInvoker{}
	rStandalone := NewBuildRouterService(storeAttach, func(restate.Context) BuildObjectInvoker { return inv })
	resp, err := rStandalone.RouteAttachPR(ctx, store.AttachChunkPRRequest{PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "b-new", resp.BuildID)

	// RouteFailChunk nil store
	_, err = rNil.RouteFailChunk(ctx, model.FailChunkRequest{ChunkID: "c1", Reason: "error"})
	assert.Error(t, err)

	// RouteFailChunk query error records metric inside journaled callback
	beforeFail := routerReconcileErrorsCount(t, "RouteFailChunk")
	_, err = rErr.RouteFailChunk(ctx, model.FailChunkRequest{ChunkID: "c1", Reason: "error"})
	assert.Error(t, err)
	assert.Equal(t, beforeFail+1, routerReconcileErrorsCount(t, "RouteFailChunk"))
}

func TestBuildRouterService_TypedNilStoreNormalized(t *testing.T) {
	t.Parallel()
	var typedNilStore *store.Store
	router := NewBuildRouterService(typedNilStore)
	// Strict interface-nil check to ensure typed-nil pointer was normalized to untyped nil interface;
	// Testify assert.Nil uses reflection and would consider a non-normalized (*store.Store)(nil) interface value nil.
	assert.True(t, router.store == nil)
}

func TestBuildRouterServiceName_Derivation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		agent      string
		wantRouter string
	}{
		{name: "empty defaults to legacy", agent: "", wantRouter: "BuildRouter"},
		{name: "aerial defaults to legacy", agent: "aerial", wantRouter: "BuildRouter"},
		{name: "aerial case insensitive", agent: "Aerial", wantRouter: "BuildRouter"},
		{name: "agent-alpha scoped", agent: "agent-alpha", wantRouter: "BuildRouter-agent-alpha"},
		{name: "agent-beta scoped", agent: "agent-beta", wantRouter: "BuildRouter-agent-beta"},
		{name: "whitespace trimmed", agent: "  agent-alpha  ", wantRouter: "BuildRouter-agent-alpha"},
		{name: "mixed case scoped normalized", agent: "Agent-Alpha", wantRouter: "BuildRouter-agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantRouter, model.BuildRouterServiceName(tc.agent))
			assert.Equal(t, tc.wantRouter, NewBuildRouterServiceWithAgent(nil, tc.agent).ServiceName())
		})
	}
}

func TestBuildRouterService_RouteDispatchOutcomeRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		row         pgx.Row
		wantBuildID string
		wantRunner  string
	}{
		{name: "default runner", row: valuesRow("b1", "aerial"), wantBuildID: "b1", wantRunner: "aerial"},
		{name: "sibling runner", row: valuesRow("b2", "agent-alpha"), wantBuildID: "b2", wantRunner: "agent-alpha"},
		{name: "unknown chunk not routed", row: errRow(pgx.ErrNoRows)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			st, _ := dbScript{row: tc.row}.open()
			router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })
			require.NoError(t, router.RouteDispatchOutcome(makeRouterMockContext(t), pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"}))
			assert.Equal(t, tc.wantBuildID, inv.lastDispatchBuildID)
			assert.Equal(t, tc.wantRunner, inv.lastRunner)
		})
	}
}

func TestBuildRouterService_RouteFailChunkRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		row        pgx.Row
		wantRunner string
	}{
		{name: "default runner", row: valuesRow("b1", "aerial"), wantRunner: "aerial"},
		{name: "sibling runner", row: valuesRow("b1", "agent-alpha"), wantRunner: "agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			st, _ := dbScript{row: tc.row}.open()
			router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })
			_, err := router.RouteFailChunk(makeRouterMockContext(t), model.FailChunkRequest{ChunkID: "c1", Reason: "boom"})
			require.NoError(t, err)
			assert.Equal(t, "b1", inv.lastFailBuildID)
			assert.Equal(t, tc.wantRunner, inv.lastRunner)
		})
	}
}

func TestBuildRouterService_RouteAttachPRRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		routerAgent string
		row         pgx.Row
		req         store.AttachChunkPRRequest
		wantBuildID string
		wantRunner  string
	}{
		{name: "existing chunk on sibling runner", row: valuesRow("b1", "agent-alpha"), req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 7}, wantBuildID: "b1", wantRunner: "agent-alpha"},
		{name: "existing pr on default runner", routerAgent: "agent-beta", row: valuesRow("c1", "b2", 0, "medium", false, "aerial"), req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}, wantBuildID: "b2", wantRunner: "aerial"},
		{name: "new build without caller runs on default agent", row: errRow(pgx.ErrNoRows), req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}, wantRunner: "aerial"},
		{name: "new build without caller ignores scoped router", routerAgent: "agent-beta", row: errRow(pgx.ErrNoRows), req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}, wantRunner: "aerial"},
		{name: "new build runs on caller runner", row: errRow(pgx.ErrNoRows), req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: "agent-alpha"}, wantRunner: "agent-alpha"},
		{name: "new build caller agent normalized", row: errRow(pgx.ErrNoRows), req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: " Agent-Alpha "}, wantRunner: "agent-alpha"},
		{name: "existing build ignores caller agent", row: valuesRow("b1", "aerial"), req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 7, Agent: "agent-alpha"}, wantBuildID: "b1", wantRunner: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			st, _ := dbScript{row: tc.row}.open()
			router := NewBuildRouterServiceWithAgent(st, tc.routerAgent, func(restate.Context) BuildObjectInvoker { return inv })
			res, err := router.RouteAttachPR(makeRouterMockContext(t), tc.req)
			require.NoError(t, err)
			assert.NotEmpty(t, res.BuildID)
			if tc.wantBuildID != "" {
				assert.Equal(t, tc.wantBuildID, inv.lastAttachBuildID)
			}
			assert.Equal(t, tc.wantRunner, inv.lastRunner)
		})
	}
}

func TestBuildRouterService_RouteBuildReconcile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		buildID     string
		db          dbScript
		wantErr     bool
		wantBuildID string
		wantRunner  string
		wantReason  string
	}{
		{name: "empty build id rejected", buildID: " ", wantErr: true},
		{name: "default runner", buildID: "b1", db: dbScript{buildRow: &mockRow{b: &model.VanadisBuild{ID: "b1", RunnerAgent: "aerial"}}}, wantBuildID: "b1", wantRunner: "aerial", wantReason: "widget_sync"},
		{name: "sibling runner", buildID: "b2", db: dbScript{buildRow: &mockRow{b: &model.VanadisBuild{ID: "b2", RunnerAgent: "agent-alpha"}}}, wantBuildID: "b2", wantRunner: "agent-alpha", wantReason: "widget_sync"},
		{name: "unknown build dropped", buildID: "b3", db: dbScript{buildRow: errRow(pgx.ErrNoRows)}},
		{name: "storage error propagates", buildID: "b4", db: dbScript{buildRow: errRow(errors.New("db down"))}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			st, _ := tc.db.open()
			router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })
			err := router.RouteBuildReconcile(makeRouterMockContext(t), tc.buildID)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantBuildID, inv.lastReconcileBuildID)
			assert.Equal(t, tc.wantRunner, inv.lastRunner)
			assert.Equal(t, tc.wantReason, inv.lastReconcileReason)
		})
	}
}
