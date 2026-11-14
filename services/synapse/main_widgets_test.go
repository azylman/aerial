package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func syncPRWidgetGuardCases() []syncPRWidgetCase {
	return []syncPRWidgetCase{
		{
			name:         "nil server returns clean nil",
			useNilServer: true,
			repo:         "azylman/aerial",
			prNumber:     1,
		},
		{
			name:     "empty repo returns clean nil",
			repo:     "",
			prNumber: 1,
		},
		{
			name:     "prNumber <= 0 returns clean nil",
			repo:     "azylman/aerial",
			prNumber: 0,
		},
		{
			name: "riverClient configured queues WidgetSyncArgs without inline HTTP dispatch", useMockReg: true, useInserter: true,
			mockTargetID: "1555405874565091380", mockBranch: "feat/queued", repo: "aerial", prNumber: 101, wantQueued: 1, wantQueuedStatus: "",
		},
		{
			name: "Postgres registry queues WidgetSyncArgs with status from pr_registry", useInserter: true,
			repo: "azylman/aerial", prNumber: 101, prDBStatus: "merged", wantQueued: 1, wantQueuedStatus: "merged",
		},
		{
			name: "Postgres registry queues WidgetSyncArgs with empty status on query error", useInserter: true,
			repo: "azylman/aerial", prNumber: 101, prQueryErr: errors.New("status lookup failed"), wantQueued: 1, wantQueuedStatus: "",
		},
		{
			name: "riverClient insert error returns error", useMockReg: true, useInserter: true,
			inserterErr: errors.New("db insert failure"), repo: "azylman/aerial", prNumber: 101, wantErr: true,
		},
	}
}

func syncPRWidgetMockCases() []syncPRWidgetCase {
	return []syncPRWidgetCase{
		{
			name:           "non-Postgres registry fallback path",
			useMockReg:     true,
			mockTargetID:   "1555405874565091380",
			mockBranch:     "feat/fallback",
			repo:           "azylman/aerial",
			prNumber:       101,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "open",
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:           "non-Postgres registry empty targetID returns clean nil",
			useMockReg:     true,
			mockTargetID:   "",
			repo:           "azylman/aerial",
			prNumber:       101,
			wantDispatched: false,
		},
		{
			name:           "non-Postgres registry dependabot branch suppresses widget",
			useMockReg:     true,
			mockTargetID:   "1555405874565091380",
			mockBranch:     "dependabot/go_modules/foo-1.0.0",
			repo:           "azylman/aerial",
			prNumber:       101,
			wantDispatched: false,
		},
		{
			name:           "Postgres standalone PR dependabot branch suppresses widget",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "open",
			prDBBranch:     "dependabot/go_modules/foo-1.0.0",
			wantDispatched: false,
		},
		{
			name:           "Postgres standalone PR merged status from DB",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "merged",
			prDBBanner:     "stale banner ignored on merged",
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "merged",
			wantBanner:     "",
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:           "Postgres standalone PR synced status from DB",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "synced",
			prDBBanner:     "stale banner ignored on synced",
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "synced",
			wantBanner:     "",
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name: "Postgres standalone PR ci_failed normalizes to failed and includes banner", prDBStatus: "ci_failed", prDBBanner: "CI check failed",
			repo: "azylman/aerial", prNumber: 101, execErr: errors.New("exec warning"),
			wantDispatched: true, wantWidgetID: "pr-azylman/aerial-101", wantStatus: "failed", wantBanner: "CI check failed", wantChannel: "1555405874565091380", wantChunks: 1,
		},
		{
			name: "Postgres standalone PR deploying status from DB", prDBStatus: "deploying",
			repo: "azylman/aerial", prNumber: 101,
			wantDispatched: true, wantWidgetID: "pr-azylman/aerial-101", wantStatus: "deploying", wantChannel: "1555405874565091380", wantChunks: 1,
		},
	}
}

func syncPRWidgetPgxBasicCases(prStart, prUpdate, chunk0Start, chunk0Update time.Time) []syncPRWidgetCase {
	return []syncPRWidgetCase{
		{
			name:           "Postgres Vanadis chunk match with deployed status and older PR sweep",
			repo:           "azylman/aerial",
			prNumber:       102,
			prDBStatus:     "deployed",
			olderPRs:       []int{101},
			vanadisMatch:   true,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "deployed",
			wantChunks:     1,
		},
		{
			name: "Postgres deployed sweep logs warning when older PR widget dispatch fails", repo: "azylman/aerial",
			prNumber: 102, prDBStatus: "deployed", olderPRs: []int{101}, vanadisMatch: true, dispErr: errors.New("dispatch error"), wantErr: true,
		},
		{
			name:           "Postgres Vanadis chunk completed build status from DB",
			repo:           "azylman/aerial",
			prNumber:       102,
			prDBStatus:     "merged",
			buildDBStatus:  "completed",
			vanadisMatch:   true,
			wantDispatched: false,
		},
		{
			name:           "Postgres Vanadis chunk deploy_failed sets build status failed and banner",
			repo:           "azylman/aerial",
			prNumber:       102,
			prDBStatus:     "deploy_failed",
			prDBBanner:     "Deploy failed",
			vanadisMatch:   true,
			wantDispatched: false,
		},
		{
			name:           "Postgres Vanadis chunk conflict and closed resolution variants",
			repo:           "azylman/aerial",
			prNumber:       102,
			prDBStatus:     "closed",
			chunk1DBStatus: "pr_opened",
			vanadisMatch:   true,
			wantDispatched: false,
		},
		{
			name:           "Postgres superseded Vanadis chunk PR with chunk id in metadata suppresses standalone widget",
			repo:           "azylman/aerial",
			prNumber:       103,
			prDBStatus:     "open",
			prDBMetadata:   `{"vanadis_chunk_id":"chunk-superseded"}`,
			vanadisMatch:   false,
			wantDispatched: false,
		},
		{
			name:         "Postgres Vanadis chunk dispatch error",
			repo:         "azylman/aerial",
			prNumber:     102,
			vanadisMatch: true,
			dispErr:      errors.New("dispatch error"),
			wantErr:      true,
		},
		{
			name:     "Postgres standalone PR dispatch error",
			repo:     "azylman/aerial",
			prNumber: 101,
			dispErr:  errors.New("dispatch error"),
			wantErr:  true,
		},
	}
}

func syncPRWidgetPgxAdvancedCases(prStart, prUpdate, chunk0Start, chunk0Update time.Time) []syncPRWidgetCase {
	return []syncPRWidgetCase{
		{
			name:       "Postgres standalone PR query error",
			repo:       "azylman/aerial",
			prNumber:   101,
			prQueryErr: errors.New("pr query error"),
			wantErr:    true,
		},
		{
			name:         "Postgres Vanadis chunk query error",
			repo:         "azylman/aerial",
			prNumber:     102,
			vanadisMatch: true,
			queryErr:     errors.New("query chunks error"),
			wantErr:      true,
		},
		{
			name:           "Postgres standalone PR open with pending and passed CI checks sets active step and skip sink",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "open",
			prDBCIChecks:   `{"test": "passed", "lint": "pending"}`,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "open",
			wantActiveStep: "Waiting on CI (1/2 passed)",
			wantSkipSink:   true,
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:           "Postgres standalone PR open with all passed CI checks sets active step and skip sink",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "open",
			prDBCIChecks:   `{"test": "passed", "lint": "passed"}`,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "open",
			wantActiveStep: "Waiting on CI (2/2 passed)",
			wantSkipSink:   true,
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:           "Postgres standalone PR ci_failed clears active step and disables skip sink",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "ci_failed",
			prDBBanner:     "CI failed",
			prDBCIChecks:   `{"test": "failed", "lint": "passed"}`,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "failed",
			wantBanner:     "CI failed",
			wantActiveStep: "",
			wantSkipSink:   false,
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:            "Postgres Vanadis chunk match passes active step to vanadis",
			repo:            "azylman/aerial",
			prNumber:        102,
			prDBStatus:      "open",
			prDBCIChecks:    `{"build": "passed", "test": "pending"}`,
			vanadisMatch:    true,
			wantVanadisStep: "Waiting on CI (1/2 passed)",
			wantDispatched:  false,
		},
		{
			name:           "Postgres standalone PR merged status with candidate jobs sets per-job active step and skip sink",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "merged",
			prDBMetadata:   `{"jobs": ["eri", "haro"]}`,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "merged",
			wantActiveStep: "`eri`: ⏳ building image\n`haro`: ⏳ building image",
			wantSkipSink:   true,
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:           "Postgres standalone PR synced status with deployed jobs and ci_checks sets active step and skip sink",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "synced",
			prDBMetadata:   `{"jobs": ["eri", "haro"], "deployed_jobs": ["eri"]}`,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "synced",
			wantActiveStep: "`eri`: ✅ healthy\n`haro`: ⏳ building image",
			wantSkipSink:   true,
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:           "Postgres standalone PR deploying status with started and deployed jobs sets active step and skip sink",
			repo:           "azylman/aerial",
			prNumber:       101,
			prDBStatus:     "deploying",
			prDBMetadata:   `{"jobs": ["coven", "eri", "haro"], "started_jobs": ["haro"], "job_states": {"haro": "verifying_health"}, "deployed_jobs": ["eri"]}`,
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "deploying",
			wantActiveStep: "`coven`: ⏳ building image\n`eri`: ✅ healthy\n`haro`: 🔄 verifying health",
			wantSkipSink:   true,
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
	}
}

func syncPRWidgetPgxCases(prStart, prUpdate, chunk0Start, chunk0Update time.Time) []syncPRWidgetCase {
	return append(syncPRWidgetPgxBasicCases(prStart, prUpdate, chunk0Start, chunk0Update), syncPRWidgetPgxAdvancedCases(prStart, prUpdate, chunk0Start, chunk0Update)...)
}

func syncPRWidgetCases(prStart, prUpdate, chunk0Start, chunk0Update time.Time) []syncPRWidgetCase {
	guards := syncPRWidgetGuardCases()
	mocks := syncPRWidgetMockCases()
	pgx := syncPRWidgetPgxCases(prStart, prUpdate, chunk0Start, chunk0Update)
	all := make([]syncPRWidgetCase, 0, len(guards)+len(mocks)+len(pgx))
	all = append(all, guards...)
	all = append(all, mocks...)
	all = append(all, pgx...)
	return all
}

func TestSyncPRWidget_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	prStart := time.Date(2026, 10, 16, 17, 30, 0, 0, time.UTC)
	prUpdate := prStart.Add(4 * time.Minute)
	chunk0Start := time.Date(2026, 10, 16, 17, 0, 0, 0, time.UTC)
	chunk0Update := chunk0Start.Add(5 * time.Minute)

	for _, tc := range syncPRWidgetCases(prStart, prUpdate, chunk0Start, chunk0Update) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := &mockOutboundDispatcher{widgetReturnMsgID: "msg-widget-101", widgetErr: tc.dispErr}
			if tc.useNilServer {
				runSyncPRWidgetNilServerCase(ctx, t, tc)
				return
			}
			if tc.useMockReg {
				runSyncPRWidgetMockRegCase(ctx, t, tc, mockDisp)
				return
			}
			runSyncPRWidgetPgxCase(ctx, t, tc, mockDisp, prStart, prUpdate, chunk0Start, chunk0Update)
		})
	}
}

type syncPRWidgetCase struct {
	name             string
	useNilServer     bool
	useMockReg       bool
	useInserter      bool
	inserterErr      error
	mockTargetID     string
	mockBranch       string
	prDBStatus       string
	prDBBranch       string
	prDBBanner       string
	prDBCIChecks     string
	prDBMetadata     string
	buildDBStatus    string
	chunk1DBStatus   string
	repo             string
	prNumber         int
	olderPRs         []int
	vanadisMatch     bool
	dispErr          error
	queryErr         error
	prQueryErr       error
	execErr          error
	wantErr          bool
	wantDispatched   bool
	wantQueued       int
	wantQueuedStatus string
	wantWidgetID     string
	wantStatus       string
	wantBanner       string
	wantActiveStep   string
	wantSkipSink     bool
	wantVanadisStep  string
	wantChannel      string
	wantChunks       int
}

func runSyncPRWidgetNilServerCase(ctx context.Context, t *testing.T, tc syncPRWidgetCase) {
	t.Helper()
	var nilSrv *RouterServer
	nilSrv.setPRWidgetState(ctx, tc.repo, tc.prNumber, "conflict", "banner")
	if err := nilSrv.SyncPRWidget(ctx, tc.repo, tc.prNumber); err != nil {
		t.Fatalf("SyncPRWidget nil server err = %v", err)
	}
	if err := nilSrv.ProcessWidgetSync(ctx, WidgetSyncArgs{Repo: tc.repo, PRNumber: tc.prNumber}); err != nil {
		t.Fatalf("ProcessWidgetSync nil server err = %v", err)
	}
}

func runSyncPRWidgetMockRegCase(ctx context.Context, t *testing.T, tc syncPRWidgetCase, mockDisp *mockOutboundDispatcher) {
	t.Helper()
	reg := &mockPRRegistry{
		resolveTargetID: tc.mockTargetID,
		resolveBranch:   tc.mockBranch,
	}
	srv := newTestRouter(Config{}, nil, reg, mockDisp)
	srv.setPRWidgetState(ctx, tc.repo, tc.prNumber, "conflict", "banner")
	var inserter *mockRiverInserter
	if tc.useInserter {
		inserter = &mockRiverInserter{err: tc.inserterErr}
		srv.SetRiverClient(inserter)
	}
	err := srv.SyncPRWidget(ctx, tc.repo, tc.prNumber)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SyncPRWidget fallback err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantQueued > 0 {
		assertSyncPRWidgetQueued(t, tc, inserter)
	}
	if tc.wantDispatched {
		calls := mockDisp.WidgetCalls()
		if len(calls) == 0 {
			t.Fatalf("expected widget call dispatched")
		}
		last := calls[len(calls)-1]
		if last.ID != tc.wantWidgetID || last.Status != tc.wantStatus || last.ChannelID != tc.wantChannel {
			t.Errorf("unexpected widget request: %+v", last)
		}
	} else if len(mockDisp.WidgetCalls()) != 0 {
		t.Errorf("expected 0 dispatched widget calls, got %d", len(mockDisp.WidgetCalls()))
	}
}

func runSyncPRWidgetPgxCase(ctx context.Context, t *testing.T, tc syncPRWidgetCase, mockDisp *mockOutboundDispatcher, prStart, prUpdate, chunk0Start, chunk0Update time.Time) {
	t.Helper()
	pool := newSyncPRWidgetPgxPool(tc, prStart, prUpdate, chunk0Start, chunk0Update)
	var receivedVanadisActiveStep string
	tsVanadis := newSyncPRWidgetServer(tc, &receivedVanadisActiveStep)
	t.Cleanup(tsVanadis.Close)

	pgReg := NewPostgresPRRegistry(pool, WithVanadisClient(tsVanadis.URL, tsVanadis.Client()))
	srv := newTestRouter(Config{VanadisURL: tsVanadis.URL}, tsVanadis.Client(), pgReg, mockDisp)
	var inserter *mockRiverInserter
	if tc.useInserter {
		inserter = &mockRiverInserter{err: tc.inserterErr}
		srv.SetRiverClient(inserter)
	}
	if tc.prDBBanner != "" {
		srv.setPRWidgetState(ctx, tc.repo, tc.prNumber, tc.prDBStatus, tc.prDBBanner)
	}
	err := srv.SyncPRWidget(ctx, tc.repo, tc.prNumber)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SyncPRWidget err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantQueued > 0 {
		assertSyncPRWidgetQueued(t, tc, inserter)
	}
	assertSyncPRWidgetDispatched(t, tc, mockDisp)
	if tc.wantVanadisStep != "" && receivedVanadisActiveStep != tc.wantVanadisStep {
		t.Errorf("got receivedVanadisActiveStep = %q, want %q", receivedVanadisActiveStep, tc.wantVanadisStep)
	}
}

func newSyncPRWidgetServer(tc syncPRWidgetCase, receivedStep *string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chunks/sync-pr-widget" {
			http.NotFound(w, r)
			return
		}
		if tc.queryErr != nil || (tc.vanadisMatch && tc.dispErr != nil) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"vanadis failure"}`))
			return
		}
		var req struct {
			Repo       string `json:"repo"`
			PRNumber   int    `json:"pr_number"`
			ActiveStep string `json:"active_step"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		*receivedStep = req.ActiveStep
		if tc.vanadisMatch && req.PRNumber == tc.prNumber {
			_ = json.NewEncoder(w).Encode(map[string]any{"matched": true, "build_id": "build-abc"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"matched": false})
	}))
}

func newSyncPRWidgetPgxPool(tc syncPRWidgetCase, prStart, prUpdate, chunk0Start, chunk0Update time.Time) *mockVanadisPgxPool {
	return &mockVanadisPgxPool{
		execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), tc.execErr
		},
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "FROM vanadis_chunks") {
				return scanVanadisChunksRow(tc)
			}
			if strings.Contains(sql, "FROM pr_registry") {
				return scanPRRegistryFullRow(tc, prStart, prUpdate)
			}
			return &mockVanadisPgxRow{}
		},
		queryFn: func(qCtx context.Context, sql string, args ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "UPDATE pr_registry") {
				return &mockVanadisPgxRows{intItems: tc.olderPRs}, nil
			}
			if tc.queryErr != nil {
				return nil, tc.queryErr
			}
			c1Stat := tc.chunk1DBStatus
			if c1Stat == "" {
				c1Stat = "running"
			}
			chunks := []VanadisChunkSummary{
				{ID: "chunk-0", ChunkIndex: 0, Repo: "aerial", Title: "Chunk 0", Status: "merged", PRNumber: 101, StartedAt: chunk0Start, UpdatedAt: chunk0Update},
				{ID: "chunk-1", ChunkIndex: 1, Repo: "", Title: "Chunk 1", Status: c1Stat, PRNumber: 102},
			}
			return &mockVanadisPgxRows{items: chunks}, nil
		},
	}
}

func scanVanadisChunksRow(tc syncPRWidgetCase) pgx.Row {
	if !tc.vanadisMatch {
		return &mockVanadisPgxRow{scanFn: func(dest ...any) error { return pgx.ErrNoRows }}
	}
	return &mockVanadisPgxRow{scanFn: func(dest ...any) error {
		*dest[0].(*string) = "chunk-1"
		*dest[1].(*string) = "build-abc"
		*dest[2].(*string) = "1555405874565091380"
		*dest[3].(*int) = 2
		*dest[4].(*string) = "Vanadis Build Title"
		*dest[5].(*string) = "Vanadis Objective"
		bStat := tc.buildDBStatus
		if bStat == "" {
			bStat = "running"
		}
		*dest[6].(*string) = bStat
		*dest[7].(*string) = "old-build-msg"
		return nil
	}}
}

func scanPRRegistryFullRow(tc syncPRWidgetCase, prStart, prUpdate time.Time) pgx.Row {
	return &mockVanadisPgxRow{scanFn: func(dest ...any) error {
		if len(dest) == 1 {
			if tc.prQueryErr != nil {
				return tc.prQueryErr
			}
			dbStat := tc.prDBStatus
			if dbStat == "" {
				dbStat = "open"
			}
			*dest[0].(*string) = dbStat
			return nil
		}
		if tc.prQueryErr != nil {
			*dest[0].(*string) = "1555405874565091380"
			return tc.prQueryErr
		}
		*dest[0].(*string) = "1555405874565091380"
		*dest[1].(*string) = "feat: awesome feature"
		dbStat := tc.prDBStatus
		if dbStat == "" {
			dbStat = "open"
		}
		*dest[2].(*string) = dbStat
		*dest[3].(*string) = "old-msg-id"
		if len(dest) > 4 {
			*dest[4].(*time.Time) = prStart
		}
		if len(dest) > 5 {
			*dest[5].(*time.Time) = prUpdate
		}
		if len(dest) > 6 {
			*dest[6].(*string) = tc.prDBBanner
		}
		ciChecks := tc.prDBCIChecks
		if ciChecks == "" {
			ciChecks = "{}"
		}
		if len(dest) > 7 {
			*dest[7].(*string) = ciChecks
		}
		meta := tc.prDBMetadata
		if meta == "" {
			meta = "{}"
		}
		if len(dest) > 8 {
			*dest[8].(*string) = meta
		}
		if len(dest) > 9 {
			*dest[9].(*string) = tc.prDBBranch
		}
		return nil
	}}
}

func assertSyncPRWidgetQueued(t *testing.T, tc syncPRWidgetCase, inserter *mockRiverInserter) {
	t.Helper()
	if inserter == nil || len(inserter.insertedWidgetSyncJobs) != tc.wantQueued {
		t.Fatalf("expected %d queued widget sync jobs, got %+v", tc.wantQueued, inserter)
	}
	if lastJob := inserter.insertedWidgetSyncJobs[len(inserter.insertedWidgetSyncJobs)-1]; lastJob.Status != tc.wantQueuedStatus {
		t.Errorf("got queued job Status = %q, want %q", lastJob.Status, tc.wantQueuedStatus)
	}
}

func assertSyncPRWidgetDispatched(t *testing.T, tc syncPRWidgetCase, mockDisp *mockOutboundDispatcher) {
	t.Helper()
	if tc.wantDispatched {
		calls := mockDisp.WidgetCalls()
		if len(calls) == 0 {
			t.Fatalf("expected widget call dispatched")
		}
		last := calls[len(calls)-1]
		if last.ID != tc.wantWidgetID || last.Status != tc.wantStatus || last.Banner != tc.wantBanner || len(last.Chunks) != tc.wantChunks {
			t.Errorf("unexpected widget request: %+v, want ID=%q Status=%q Banner=%q Chunks=%d", last, tc.wantWidgetID, tc.wantStatus, tc.wantBanner, tc.wantChunks)
		}
		if len(last.Chunks) > 0 && last.Chunks[0].ActiveStep != tc.wantActiveStep {
			t.Errorf("got chunk[0].ActiveStep = %q, want %q", last.Chunks[0].ActiveStep, tc.wantActiveStep)
		}
		if last.SkipSink != tc.wantSkipSink {
			t.Errorf("got last.SkipSink = %v, want %v", last.SkipSink, tc.wantSkipSink)
		}
		return
	}
	if !tc.wantErr && len(mockDisp.WidgetCalls()) != 0 {
		t.Errorf("expected 0 dispatched widget calls, got %d", len(mockDisp.WidgetCalls()))
	}
}

func TestReconcileStuckDeployingPRWidget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []reconcileStuckDeployingCase{
		{name: "nil server guard", nilServer: true, repo: "azylman/aerial", prNum: 42},
		{name: "nil registry guard", nilRegistry: true, repo: "azylman/aerial", prNum: 42},
		{name: "non-postgres registry guard", useMockReg: true, repo: "azylman/aerial", prNum: 42},
		{name: "nil pool guard", nilPool: true, repo: "azylman/aerial", prNum: 42},
		{name: "empty repo guard", repo: "", prNum: 42},
		{name: "zero prNum guard", repo: "azylman/aerial", prNum: 0},
		{name: "db error scanning status does not reconcile", repo: "azylman/aerial", prNum: 42, queryErr: errors.New("db scan error"), queryStatus: ""},
		{name: "pr status already deployed is a no-op", repo: "azylman/aerial", prNum: 42, queryStatus: "deployed"},
		{name: "pr status deploying reconciles to deployed and dispatches widget", repo: "azylman/aerial", prNum: 42, queryStatus: "deploying", wantExecCount: 1, wantWidgetCall: true, wantWidgetStat: "deployed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runReconcileStuckDeployingCase(ctx, t, tc)
		})
	}
}

type reconcileStuckDeployingCase struct {
	name           string
	nilServer      bool
	nilRegistry    bool
	useMockReg     bool
	nilPool        bool
	repo           string
	prNum          int
	queryStatus    string
	queryErr       error
	wantExecCount  int
	wantWidgetCall bool
	wantWidgetStat string
}

func runReconcileStuckDeployingCase(ctx context.Context, t *testing.T, tc reconcileStuckDeployingCase) {
	t.Helper()
	if tc.nilServer {
		var nilSrv *RouterServer
		nilSrv.reconcileStuckDeployingPRWidget(ctx, tc.repo, tc.prNum)
		return
	}
	mockDisp := &mockOutboundDispatcher{}
	reg, getExecCalls := setupReconcileStuckRegistry(tc)
	srv := newTestRouter(Config{}, nil, reg, mockDisp)
	srv.reconcileStuckDeployingPRWidget(ctx, tc.repo, tc.prNum)

	if getExecCalls() != tc.wantExecCount {
		t.Errorf("setPRWidgetState execCalls = %d, want %d", getExecCalls(), tc.wantExecCount)
	}
	assertReconcileWidgetCalls(t, mockDisp.WidgetCalls(), tc.wantWidgetCall, tc.wantWidgetStat)
}

func scanReconcileStatusRow(dest []any, currentStatus string, queryErr error) error {
	if queryErr != nil {
		return queryErr
	}
	if len(dest) > 0 {
		if s, ok := dest[0].(*string); ok {
			*s = currentStatus
		}
	}
	return nil
}

func scanReconcileDetailsRow(dest []any, currentStatus string) error {
	if len(dest) < 10 {
		return nil
	}
	*dest[0].(*string) = "1555405874565091380"
	*dest[1].(*string) = "Fix Widget Race"
	*dest[2].(*string) = currentStatus
	*dest[3].(*string) = "status-msg-1"
	*dest[4].(*time.Time) = time.Now()
	*dest[5].(*time.Time) = time.Now()
	*dest[6].(*string) = ""
	*dest[7].(*string) = "{}"
	*dest[8].(*string) = "{}"
	*dest[9].(*string) = "feat/fix-race"
	return nil
}

func newReconcilePgxPool(tc reconcileStuckDeployingCase, currentStatus *string, execCalls *int) *mockVanadisPgxPool {
	return &mockVanadisPgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT status FROM pr_registry") {
				return &mockVanadisPgxRow{scanFn: func(dest ...any) error {
					return scanReconcileStatusRow(dest, *currentStatus, tc.queryErr)
				}}
			}
			if strings.Contains(sql, "SELECT target_id, title, status") {
				return &mockVanadisPgxRow{scanFn: func(dest ...any) error {
					return scanReconcileDetailsRow(dest, *currentStatus)
				}}
			}
			return &mockVanadisPgxRow{}
		},
		execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "UPDATE pr_registry") && strings.Contains(sql, "SET status = CASE") {
				*execCalls++
				if len(args) >= 3 {
					if st, ok := args[2].(string); ok && st != "" {
						*currentStatus = st
					}
				}
			}
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
}

func setupReconcileStuckRegistry(tc reconcileStuckDeployingCase) (PRRegistryUpdater, func() int) {
	if tc.nilRegistry {
		return nil, func() int { return 0 }
	}
	if tc.useMockReg {
		return &mockPRRegistry{}, func() int { return 0 }
	}
	if tc.nilPool {
		return NewPostgresPRRegistry(nil), func() int { return 0 }
	}
	var execCalls int
	currentStatus := tc.queryStatus
	pool := newReconcilePgxPool(tc, &currentStatus, &execCalls)
	return NewPostgresPRRegistry(pool), func() int { return execCalls }
}

func assertReconcileWidgetCalls(t *testing.T, calls []StatusWidgetRequest, wantWidgetCall bool, wantWidgetStat string) {
	t.Helper()
	if wantWidgetCall {
		if len(calls) == 0 {
			t.Fatalf("expected widget call dispatched, got 0")
		}
		last := calls[len(calls)-1]
		if last.Status != wantWidgetStat {
			t.Errorf("got widget status = %q, want %q", last.Status, wantWidgetStat)
		}
		return
	}
	if len(calls) != 0 {
		t.Errorf("expected 0 widget calls, got %d", len(calls))
	}
}

type handlePRRegisterCase struct {
	name           string
	nilRegistry    bool
	noExistingPR   bool
	sysChanID      string
	regErr         error
	regTargetID    string
	resolveTID     string
	body           string
	dispWidgetErr  error
	wantStatus     int
	wantSubstr     string
	wantRegistered bool
	wantTargetID   string
	wantWidgetCall bool
}

func routerServerHandlePRRegisterValidationCases() []handlePRRegisterCase {
	return []handlePRRegisterCase{
		{
			name:        "NilRegistry",
			nilRegistry: true,
			body:        `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":"1555405874565091380"}`,
			wantStatus:  http.StatusInternalServerError,
			wantSubstr:  "registry not initialized",
		},
		{name: "InvalidJSON", body: `{not-valid-json`, wantStatus: http.StatusBadRequest, wantSubstr: "invalid json payload"},
		{
			name:       "EmptyRepo",
			body:       `{"repo":"","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":"1555405874565091380"}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "repo cannot be empty",
		},
		{
			name:       "ZeroPRNumber",
			body:       `{"repo":"azylman/aerial","pr_number":0,"branch":"feat/test","head_sha":"sha123","target_id":"1555405874565091380"}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "pr_number must be greater than 0",
		},
		{
			name:       "EmptyBranch",
			body:       `{"repo":"azylman/aerial","pr_number":101,"branch":"","head_sha":"sha123","target_id":"1555405874565091380"}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "branch cannot be empty",
		},
		{
			name:       "EmptyHeadSHA",
			body:       `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"","target_id":"1555405874565091380"}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "head_sha cannot be empty",
		},
		{
			name:         "EmptyTargetIDWithoutExistingPROrSystemChannelReturns400",
			noExistingPR: true,
			body:         `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":""}`,
			wantStatus:   http.StatusBadRequest,
			wantSubstr:   "target_id must be a valid Discord snowflake (17-20 digits)",
		},
		{
			name:           "SuccessEmptyTargetIDPreservesExistingTargetID",
			body:           `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":""}`,
			resolveTID:     "1555405874565091380",
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1555405874565091380",
			wantWidgetCall: true,
		},
		{
			name:           "SuccessEmptyTargetIDFallsBackToSystemChannelID",
			noExistingPR:   true,
			sysChanID:      "1542423172400291873",
			body:           `{"repo":"azylman/aerial","pr_number":108,"branch":"feat/empty-target","head_sha":"sha555","target_id":""}`,
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1542423172400291873",
			wantWidgetCall: true,
		},
		{
			name:       "InvalidTargetIDWithoutChunk",
			body:       `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":"not-a-snowflake"}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "target_id must be a valid Discord snowflake (17-20 digits)",
		},
		{
			name:       "MissingVanadisChunkReturns404",
			body:       `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","vanadis_chunk_id":"missing-chunk-id"}`,
			regErr:     ErrVanadisChunkNotFound,
			wantStatus: http.StatusNotFound,
			wantSubstr: "vanadis chunk not found",
		},
	}
}

func routerServerHandlePRRegisterSuccessCases() []handlePRRegisterCase {
	return []handlePRRegisterCase{
		{
			name:       "CancelledVanadisChunkReturns409",
			body:       `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","vanadis_chunk_id":"cancelled-chunk-id"}`,
			regErr:     ErrVanadisChunkCancelled,
			wantStatus: http.StatusConflict,
			wantSubstr: "vanadis chunk or build is cancelled",
		},
		{
			name:       "InvalidTargetIDFromChunkRegistrationReturns400",
			body:       `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","vanadis_chunk_id":"chunk-no-thread"}`,
			regErr:     ErrInvalidTargetID,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "target_id must be a valid Discord snowflake (17-20 digits)",
		},
		{
			name:       "InternalRegistrationErrorReturns500",
			body:       `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":"1555405874565091380"}`,
			regErr:     errors.New("db disk failure"),
			wantStatus: http.StatusInternalServerError,
			wantSubstr: "Failed to register PR",
		},
		{
			name:           "SuccessDirectTargetID",
			body:           `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":"1555405874565091380","title":"feat: test"}`,
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1555405874565091380",
			wantWidgetCall: true,
		},
		{
			name:           "SuccessWithChunkIDResolvingThreadID",
			body:           `{"repo":"azylman/aerial","pr_number":102,"branch":"feat/chunk","head_sha":"sha456","vanadis_chunk_id":"chunk-xyz"}`,
			regTargetID:    "1555405874565091399",
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1555405874565091399",
			wantWidgetCall: true,
		},
		{
			name:           "SuccessDiscordPrefixedTargetID",
			body:           `{"repo":"azylman/aerial","pr_number":104,"branch":"feat/discord-prefix","head_sha":"sha111","target_id":"stateful_medium:discord-1555405874565091380"}`,
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1555405874565091380",
			wantWidgetCall: true,
		},
		{
			name:           "SuccessHeadlessDispatchPreservesExistingTargetID",
			body:           `{"repo":"azylman/aerial","pr_number":105,"branch":"feat/dispatch","head_sha":"sha222","target_id":"dispatch-8d23879c-1111"}`,
			resolveTID:     "1555405874565091380",
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1555405874565091380",
			wantWidgetCall: true,
		},
		{
			name:           "SuccessHeadlessHeartbeatFallsBackToSystemChannelID",
			noExistingPR:   true,
			sysChanID:      "discord-1542423172400291873",
			body:           `{"repo":"azylman/aerial","pr_number":106,"branch":"feat/heartbeat","head_sha":"sha333","target_id":"heartbeat-daily-check"}`,
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1542423172400291873",
			wantWidgetCall: true,
		},
		{
			name:           "SuccessPooledWorkerKeyPreservesExistingTargetID",
			body:           `{"repo":"azylman/aerial","pr_number":108,"branch":"feat/worker-remediate","head_sha":"sha555","target_id":"discord:medium-worker-0"}`,
			resolveTID:     "1555405874565091380",
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1555405874565091380",
			wantWidgetCall: true,
		},
		{
			name:         "HeadlessDispatchWithoutExistingTargetOrSystemChannelReturns400",
			noExistingPR: true,
			body:         `{"repo":"azylman/aerial","pr_number":107,"branch":"feat/headless-unresolvable","head_sha":"sha444","target_id":"stateful_low:dispatch-8d23879c"}`,
			wantStatus:   http.StatusBadRequest,
			wantSubstr:   "target_id must be a valid Discord snowflake (17-20 digits)",
		},
		{
			name:           "SuccessEvenIfWidgetSyncFails",
			body:           `{"repo":"azylman/aerial","pr_number":103,"branch":"feat/widget-fail","head_sha":"sha789","target_id":"1555405874565091380"}`,
			dispWidgetErr:  errors.New("discord rate limited"),
			wantStatus:     http.StatusOK,
			wantSubstr:     `"status":"registered"`,
			wantRegistered: true,
			wantTargetID:   "1555405874565091380",
			wantWidgetCall: true,
		},
	}
}

func routerServerHandlePRRegisterCases() []handlePRRegisterCase {
	return append(routerServerHandlePRRegisterValidationCases(), routerServerHandlePRRegisterSuccessCases()...)
}

func TestRouterServer_HandlePRRegister(t *testing.T) {
	t.Parallel()

	for _, tc := range routerServerHandlePRRegisterCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandlePRRegisterCase(t, tc)
		})
	}
}

func runHandlePRRegisterCase(t *testing.T, tc handlePRRegisterCase) {
	t.Helper()
	var reg PRRegistryUpdater
	var mockReg *mockPRRegistry
	if !tc.nilRegistry {
		resolveTarget := firstNonEmpty(tc.resolveTID, tc.regTargetID, "1555405874565091380")
		if tc.noExistingPR {
			resolveTarget = ""
		}
		mockReg = &mockPRRegistry{
			registerErr:      tc.regErr,
			registerTargetID: tc.regTargetID,
			resolveTargetID:  resolveTarget,
			resolveBranch:    "feat/test",
		}
		reg = mockReg
	}

	disp := &mockOutboundDispatcher{
		widgetErr: tc.dispWidgetErr,
	}

	srv := newTestRouter(Config{SystemChannelID: tc.sysChanID}, nil, reg, disp)
	routes := srv.Routes()

	rec := doHTTPRequest(routes, http.MethodPost, "/internal/pr/register", strings.NewReader(tc.body), nil)

	if rec.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
	}
	if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
		t.Errorf("body = %q, want substr %q", rec.Body.String(), tc.wantSubstr)
	}
	if tc.wantRegistered && mockReg != nil {
		if len(mockReg.registerCalls) != 1 {
			t.Errorf("registerCalls = %d, want 1", len(mockReg.registerCalls))
		}
	}
	if tc.wantWidgetCall {
		calls := disp.WidgetCalls()
		if len(calls) != 1 {
			t.Errorf("WidgetCalls = %d, want 1", len(calls))
		}
	}
}

func TestSyncBuildWidget_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []syncBuildWidgetCase{
		{name: "nil server returns clean nil", useNilServer: true, buildID: "build-123"},
		{name: "empty buildID returns clean nil", buildID: ""},
		{name: "riverClient configured queues WidgetSyncArgs without inline HTTP dispatch", useMockReg: true, useInserter: true, buildID: "build-101", banner: "Build running", wantQueued: 1},
		{name: "riverClient insert error returns error", useMockReg: true, useInserter: true, inserterErr: errors.New("db insert failure"), buildID: "build-101", wantErr: true},
		{name: "non-Postgres registry fallback returns clean nil", useMockReg: true, buildID: "build-102", wantErr: false},
		{name: "Postgres registry calls vanadis build widget sync successfully", buildID: "build-103", banner: "Chunk 1 running", vanadisStatus: http.StatusOK, wantErr: false},
		{
			name:            "Postgres registry retries transient 502 then 200 succeeds",
			buildID:         "build-103-retry",
			banner:          "Chunk 1 retried",
			vanadisStatuses: []int{http.StatusBadGateway, http.StatusOK},
			wantErr:         false,
		},
		{name: "Postgres registry returns error when vanadis fails", buildID: "build-104", vanadisStatus: http.StatusInternalServerError, wantErr: true},
		{name: "Postgres registry non-transient 400 error", buildID: "build-106", vanadisStatus: http.StatusBadRequest, wantErr: true},
		{name: "Postgres registry invalid json decode error", buildID: "build-105", vanadisStatus: http.StatusOK, vanadisBody: `{invalid-json`, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSyncBuildWidgetCase(ctx, t, tc)
		})
	}
}

type syncBuildWidgetCase struct {
	name            string
	useNilServer    bool
	useMockReg      bool
	useInserter     bool
	inserterErr     error
	buildID         string
	banner          string
	vanadisStatus   int
	vanadisStatuses []int
	vanadisBody     string
	wantErr         bool
	wantQueued      int
}

func runSyncBuildWidgetCase(ctx context.Context, t *testing.T, tc syncBuildWidgetCase) {
	t.Helper()
	if tc.useNilServer {
		var nilSrv *RouterServer
		if err := nilSrv.SyncBuildWidget(ctx, tc.buildID, tc.banner); err != nil {
			t.Fatalf("SyncBuildWidget nil server err = %v, want nil", err)
		}
		return
	}
	tsVanadis := newSyncBuildWidgetServer(tc)
	t.Cleanup(tsVanadis.Close)

	srv, inserter := setupSyncBuildWidgetRouter(tc, tsVanadis)
	err := srv.SyncBuildWidget(ctx, tc.buildID, tc.banner)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SyncBuildWidget err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertSyncBuildWidgetQueue(t, tc, inserter)
}

func newSyncBuildWidgetServer(tc syncBuildWidgetCase) *httptest.Server {
	var reqIdx int
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chunks/sync-pr-widget" {
			http.NotFound(w, r)
			return
		}
		status := tc.vanadisStatus
		if len(tc.vanadisStatuses) > 0 {
			if reqIdx < len(tc.vanadisStatuses) {
				status = tc.vanadisStatuses[reqIdx]
			} else {
				status = tc.vanadisStatuses[len(tc.vanadisStatuses)-1]
			}
		}
		reqIdx++
		if status != 0 && status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"vanadis build error"}`))
			return
		}
		if tc.vanadisBody != "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(tc.vanadisBody))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"matched": true, "build_id": tc.buildID})
	}))
}

func setupSyncBuildWidgetRouter(tc syncBuildWidgetCase, tsVanadis *httptest.Server) (*RouterServer, *mockRiverInserter) {
	mockDisp := &mockOutboundDispatcher{}
	var reg PRRegistryUpdater
	if tc.useMockReg {
		reg = &mockPRRegistry{resolveBranch: "feat/mock"}
	} else {
		pool := &mockVanadisPgxPool{}
		pgReg := NewPostgresPRRegistry(pool, WithVanadisClient(tsVanadis.URL, tsVanadis.Client()))
		pgReg.vanadisRetryDelay = 0
		reg = pgReg
	}

	srv := newTestRouter(Config{VanadisURL: tsVanadis.URL}, tsVanadis.Client(), reg, mockDisp)
	var inserter *mockRiverInserter
	if tc.useInserter {
		inserter = &mockRiverInserter{err: tc.inserterErr}
		srv.SetRiverClient(inserter)
	}
	return srv, inserter
}

func assertSyncBuildWidgetQueue(t *testing.T, tc syncBuildWidgetCase, inserter *mockRiverInserter) {
	t.Helper()
	if tc.wantQueued > 0 {
		if inserter == nil || len(inserter.insertedWidgetSyncJobs) != tc.wantQueued {
			t.Fatalf("expected %d queued widget sync jobs, got %+v", tc.wantQueued, inserter)
		}
		lastJob := inserter.insertedWidgetSyncJobs[len(inserter.insertedWidgetSyncJobs)-1]
		if lastJob.BuildID != tc.buildID || lastJob.Banner != tc.banner {
			t.Errorf("queued job mismatch: got %+v, want BuildID=%q Banner=%q", lastJob, tc.buildID, tc.banner)
		}
	}
}

func TestRouterServer_HandleInternalWidgetSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          string
		useInserter   bool
		inserterErr   error
		vanadisStatus int
		wantStatus    int
		wantSubstr    string
		wantQueued    int
	}{
		{name: "InvalidJSON", body: `{not-valid-json`, wantStatus: http.StatusBadRequest, wantSubstr: "invalid json payload"},
		{name: "MissingAllFieldsReturns400", body: `{}`, wantStatus: http.StatusBadRequest, wantSubstr: "build_id or repo and pr_number are required"},
		{name: "EmptyBuildIDAndEmptyRepoReturns400", body: `{"build_id":"","repo":"","pr_number":0}`, wantStatus: http.StatusBadRequest, wantSubstr: "build_id or repo and pr_number are required"},
		{name: "RepoWithoutPRNumberReturns400", body: `{"repo":"azylman/aerial","pr_number":0}`, wantStatus: http.StatusBadRequest, wantSubstr: "build_id or repo and pr_number are required"},
		{name: "ValidBuildIDWithRiver", body: `{"build_id":"build-777","banner":"Building chunk 0"}`, useInserter: true, wantStatus: http.StatusAccepted, wantSubstr: `"status":"accepted"`, wantQueued: 1},
		{name: "ValidRepoAndPRNumberWithRiver", body: `{"repo":"azylman/aerial","pr_number":202}`, useInserter: true, wantStatus: http.StatusAccepted, wantSubstr: `"status":"accepted"`, wantQueued: 1},
		{name: "RiverInsertErrorOnBuildIDReturns500", body: `{"build_id":"build-888"}`, useInserter: true, inserterErr: errors.New("river pool full"), wantStatus: http.StatusInternalServerError, wantSubstr: "failed to sync build widget"},
		{name: "RiverInsertErrorOnPRReturns500", body: `{"repo":"azylman/aerial","pr_number":203}`, useInserter: true, inserterErr: errors.New("river pool full"), wantStatus: http.StatusInternalServerError, wantSubstr: "failed to sync pr widget"},
		{name: "ValidBuildIDDirectWithoutRiver", body: `{"build_id":"build-999","banner":"Direct sync"}`, vanadisStatus: http.StatusOK, wantStatus: http.StatusAccepted, wantSubstr: `"build_id":"build-999"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tsVanadis := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/chunks/sync-pr-widget" {
					http.NotFound(w, r)
					return
				}
				if tc.vanadisStatus != 0 && tc.vanadisStatus != http.StatusOK {
					w.WriteHeader(tc.vanadisStatus)
					_, _ = w.Write([]byte(`{"error":"vanadis error"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"matched": true, "build_id": "build-999"})
			}))
			t.Cleanup(tsVanadis.Close)

			pool := &mockVanadisPgxPool{}
			reg := NewPostgresPRRegistry(pool, WithVanadisClient(tsVanadis.URL, tsVanadis.Client()))
			reg.vanadisRetryDelay = 0
			disp := &mockOutboundDispatcher{}

			srv := newTestRouter(Config{VanadisURL: tsVanadis.URL}, tsVanadis.Client(), reg, disp)
			var inserter *mockRiverInserter
			if tc.useInserter {
				inserter = &mockRiverInserter{err: tc.inserterErr}
				srv.SetRiverClient(inserter)
			}

			routes := srv.Routes()
			rec := doHTTPRequest(routes, http.MethodPost, "/internal/widget/sync", strings.NewReader(tc.body), nil)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Errorf("body = %q, want substr %q", rec.Body.String(), tc.wantSubstr)
			}
			if tc.wantQueued > 0 && (inserter == nil || len(inserter.insertedWidgetSyncJobs) != tc.wantQueued) {
				t.Errorf("expected %d queued jobs, got %+v", tc.wantQueued, inserter)
			}
		})
	}
}

func TestProcessMergeConflict_Table(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		nilServer     bool
		isEnqueue     bool
		hasRiver      bool
		hasDispatcher bool
		args          MergeConflictArgs
		wantEnqueued  int
		wantPrompts   int
	}{
		{name: "ProcessMergeConflict nil server", nilServer: true},
		{name: "enqueueMergeConflict nil server", nilServer: true, isEnqueue: true},
		{
			name: "ProcessMergeConflict valid dispatch", hasDispatcher: true,
			args:        MergeConflictArgs{Repo: "azylman/aerial", Branch: "feat/conflict", HeadSHA: "sha-conflict-1", PRNumber: 543, TargetID: "1555405874565091380"},
			wantPrompts: 1,
		},
		{
			name: "enqueueMergeConflict with river client enqueues", isEnqueue: true, hasRiver: true,
			args:         MergeConflictArgs{Repo: "aerial", Branch: "feat/conflict", HeadSHA: "sha-conflict-2", PRNumber: 544, TargetID: "1555405874565091380"},
			wantEnqueued: 1,
		},
		{
			name: "enqueueMergeConflict without river client dispatches directly", isEnqueue: true, hasDispatcher: true,
			args:        MergeConflictArgs{Repo: "azylman/aerial", Branch: "feat/conflict", HeadSHA: "sha-conflict-3", PRNumber: 545, TargetID: "1555405874565091380"},
			wantPrompts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp := &mockOutboundDispatcher{}
			river := &mockRiverInserter{}
			var srv *RouterServer
			if !tc.nilServer {
				srv = newTestRouter(Config{}, nil, nil, disp)
				if tc.hasRiver {
					srv.SetRiverClient(river)
				}
			}

			var err error
			if tc.isEnqueue {
				err = srv.enqueueMergeConflict(ctx, tc.args)
			} else {
				err = srv.ProcessMergeConflict(ctx, tc.args)
			}

			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if tc.hasRiver {
				if len(river.insertedConflictJobs) != tc.wantEnqueued {
					t.Errorf("insertedConflictJobs = %d, want %d", len(river.insertedConflictJobs), tc.wantEnqueued)
				}
			}
			if tc.hasDispatcher {
				if len(disp.DispatchCalls()) != tc.wantPrompts {
					t.Errorf("DispatchCalls = %d, want %d", len(disp.DispatchCalls()), tc.wantPrompts)
				}
			}
		})
	}
}

func TestSetupRiverWorkerPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name        string
		postgresURL string
		wantErr     bool
		wantNilPool bool
	}{
		{
			name:        "empty postgres URL returns nil pool without error",
			postgresURL: "",
			wantNilPool: true,
		},
		{
			name:        "invalid postgres URL returns error",
			postgresURL: "postgres://%gh:invalid",
			wantErr:     true,
			wantNilPool: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			pool, rc, err := setupRiverWorkerPool(ctx, srv, Config{PostgresURL: tc.postgresURL})
			if (err != nil) != tc.wantErr {
				t.Fatalf("setupRiverWorkerPool() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantNilPool && pool != nil {
				t.Errorf("expected nil pool, got %v", pool)
			}
			if pool != nil {
				defer pool.Close()
			}
			if rc != nil {
				defer rc.Stop(context.Background())
			}
		})
	}
}

func TestHasMetadataVanadisChunkID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{name: "empty string", input: "", expected: false},
		{name: "whitespace only", input: "   ", expected: false},
		{name: "invalid json", input: "{invalid", expected: false},
		{name: "valid json without field", input: `{"foo":"bar"}`, expected: false},
		{name: "valid json with empty chunk id", input: `{"vanadis_chunk_id":""}`, expected: false},
		{name: "valid json with whitespace chunk id", input: `{"vanadis_chunk_id":"   "}`, expected: false},
		{name: "valid json with valid chunk id", input: `{"vanadis_chunk_id":"chunk-superseded"}`, expected: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := hasMetadataVanadisChunkID(tc.input)
			if got != tc.expected {
				t.Errorf("hasMetadataVanadisChunkID(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}
