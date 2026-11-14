package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/jackc/pgx/v5"
)

func TestIsWaitingForImageBuild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []isWaitingForImageBuildCase{
		{
			name:       "nil server returns false",
			serverNil:  true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "nil registry returns false",
			nilReg:     true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "empty repo returns false",
			repo:       "",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "zero prNumber returns false",
			repo:       "azylman/aerial",
			prNumber:   0,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "non-postgres registry returns false",
			mockReg:    true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "nil pool returns false",
			nilPool:    true,
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "query error returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			queryErr:   errors.New("db error"),
			wantResult: false,
		},
		{
			name:       "unsupported status returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "deploying",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "invalid metadata unmarshals to nil and returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{invalid json`,
			wantResult: false,
		},
		{
			name:       "started_jobs non-empty returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"started_jobs":["aerial-synapse"],"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "no build checks in metadata returns false",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Go Unit Tests":"passed"}}`,
			wantResult: false,
		},
		{
			name:       "open status with build checks returns true",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "open",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: true,
		},
		{
			name:       "merged status with build checks returns true",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "merged",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: true,
		},
		{
			name:       "synced status with build checks returns true",
			repo:       "azylman/aerial",
			prNumber:   42,
			status:     "synced",
			metadata:   `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
			wantResult: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runIsWaitingForImageBuildCase(ctx, t, tc)
		})
	}
}

type isWaitingForImageBuildCase struct {
	name       string
	serverNil  bool
	nilReg     bool
	mockReg    bool
	nilPool    bool
	repo       string
	prNumber   int
	status     string
	metadata   string
	queryErr   error
	wantResult bool
}

func newWaitingImageMockPool(status, metadata string, queryErr error) *mockVanadisPgxPool {
	return &mockVanadisPgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			if queryErr != nil {
				return mockVanadisPgxRow{scanFn: func(dest ...any) error {
					return queryErr
				}}
			}
			return mockVanadisPgxRow{scanFn: func(dest ...any) error {
				if len(dest) >= 2 {
					if s, ok := dest[0].(*string); ok {
						*s = status
					}
					if b, ok := dest[1].(*[]byte); ok && metadata != "" {
						*b = []byte(metadata)
					}
				}
				return nil
			}}
		},
	}
}

func runIsWaitingForImageBuildCase(ctx context.Context, t *testing.T, tc isWaitingForImageBuildCase) {
	t.Helper()
	if tc.serverNil {
		var nilSrv *RouterServer
		if got := nilSrv.isWaitingForImageBuild(ctx, tc.repo, tc.prNumber); got != tc.wantResult {
			t.Errorf("isWaitingForImageBuild() = %v, want %v", got, tc.wantResult)
		}
		return
	}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	switch {
	case tc.mockReg:
		srv.SetRegistry(&mockPRRegistry{})
	case tc.nilPool:
		srv.SetRegistry(NewPostgresPRRegistry(nil))
	case !tc.nilReg:
		srv.SetRegistry(NewPostgresPRRegistry(newWaitingImageMockPool(tc.status, tc.metadata, tc.queryErr)))
	}
	got := srv.isWaitingForImageBuild(ctx, tc.repo, tc.prNumber)
	if got != tc.wantResult {
		t.Errorf("isWaitingForImageBuild() = %v, want %v", got, tc.wantResult)
	}
}

// --- 5. Outbound Dispatcher ---

func TestResolveSHA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name               string
		repo               string
		sha                string
		regErr             error
		regResolvePRNum    int
		regResolveTargetID string
		regResolveBranch   string
		regEmptySHAID      bool
		byNumTargetID      string
		ghPRNumber         int
		ghBranch           string
		wantPRNum          int
		wantTargetID       string
		wantBranch         string
		wantBackfillCalls  int
		wantErr            bool
	}{
		{
			name:               "local fast-path with targetID",
			repo:               "azylman/aerial",
			sha:                "sha-fast-1",
			regResolvePRNum:    42,
			regResolveTargetID: "1555405874565091380",
			regResolveBranch:   "feat/fast-path",
			wantPRNum:          42,
			wantTargetID:       "1555405874565091380",
			wantBranch:         "feat/fast-path",
		},
		{
			name:             "local fast-path with empty targetID resolved via ResolvePRByNumber",
			repo:             "azylman/aerial",
			sha:              "sha-fast-empty-tid",
			regResolvePRNum:  43,
			regResolveBranch: "feat/resolved-by-num",
			regEmptySHAID:    true,
			byNumTargetID:    "1555405874565091380",
			wantPRNum:        43,
			wantTargetID:     "1555405874565091380",
			wantBranch:       "feat/resolved-by-num",
		},
		{
			name:              "GitHub API fallback with BackfillPushMergeSHA",
			repo:              "azylman/aerial",
			sha:               "sha-gh-3",
			ghPRNumber:        99,
			ghBranch:          "feat/gh-fallback",
			byNumTargetID:     "1555405874565091380",
			wantPRNum:         99,
			wantTargetID:      "1555405874565091380",
			wantBranch:        "feat/gh-fallback",
			wantBackfillCalls: 1,
		},
		{
			name:       "GitHub API fallback with BackfillPushMergeSHA error",
			repo:       "azylman/aerial",
			sha:        "sha-gh-bf-err",
			ghPRNumber: 99,
			ghBranch:   "feat/gh-fallback-err",
			regErr:     errors.New("bf err"),
			wantPRNum:  99,
			wantBranch: "feat/gh-fallback-err",
		},
		{
			name:      "zero SHA returns zero results",
			repo:      "azylman/aerial",
			sha:       "0000000000000000000000000000000000000000",
			wantPRNum: 0,
		},
		{
			name:      "empty repo and sha returns zero results",
			repo:      "",
			sha:       "",
			wantPRNum: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.ghPRNumber > 0 {
					w.Header().Set("Content-Type", "application/json")
					item := GitHubCommitPRItem{Number: tc.ghPRNumber, State: "closed"}
					item.Head.Ref = tc.ghBranch
					_ = json.NewEncoder(w).Encode([]GitHubCommitPRItem{item})
					return
				}
				http.NotFound(w, r)
			}))
			defer ghServer.Close()

			targetID := tc.regResolveTargetID
			if tc.byNumTargetID != "" {
				targetID = tc.byNumTargetID
			}
			mockReg := &mockPRRegistry{
				err:                     tc.regErr,
				resolvePRNum:            tc.regResolvePRNum,
				resolveTargetID:         targetID,
				resolveBranch:           tc.regResolveBranch,
				resolveBySHATargetEmpty: tc.regEmptySHAID,
				backfillTargetID:        tc.byNumTargetID,
			}
			srv := newTestRouter(Config{GitHubAPIURL: ghServer.URL}, ghServer.Client(), mockReg, nil)

			prNum, targetID, branch, err := srv.ResolveSHA(ctx, tc.repo, tc.sha)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveSHA() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if prNum != tc.wantPRNum {
				t.Errorf("got prNum = %d, want %d", prNum, tc.wantPRNum)
			}
			if targetID != tc.wantTargetID {
				t.Errorf("got targetID = %q, want %q", targetID, tc.wantTargetID)
			}
			if branch != tc.wantBranch {
				t.Errorf("got branch = %q, want %q", branch, tc.wantBranch)
			}
			if len(mockReg.backfillCalls) != tc.wantBackfillCalls {
				t.Errorf("got %d backfill calls, want %d", len(mockReg.backfillCalls), tc.wantBackfillCalls)
			}
		})
	}
}

func TestResolveThreadForPRAndDeploymentSuccessPrompt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validSnowflake := "1555405874565091380"

	tests := []resolveThreadCase{
		{
			name:           "ResolveThreadForPR nil registry returns empty",
			useNilRegistry: true,
			prNumber:       123,
			wantTargetID:   "",
		},
		{
			name:         "ResolveThreadForPR zero prNumber returns empty",
			prNumber:     0,
			wantTargetID: "",
		},
		{
			name:       "ResolveThreadForPR registry error returns error",
			prNumber:   456,
			resolveErr: errors.New("db error"),
			wantErr:    true,
		},
		{
			name:         "ResolveThreadForPR valid prNumber returns targetID",
			prNumber:     456,
			targetID:     validSnowflake,
			wantTargetID: validSnowflake,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt normal",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			wantPrompts:         1,
			wantWidgets:         1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job brain on azylman/aerial",
				"Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely.",
			},
		},
		{
			name:                "dispatchDeploymentSuccessPrompt dependabot branch suppresses wake and widget",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			branch:              "dependabot/github_actions/fetch-metadata-3",
			wantPrompts:         0,
			wantWidgets:         0,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt invalid snowflake is no-op",
			isDeploymentSuccess: true,
			targetID:            "invalid-snowflake",
			wantPrompts:         0,
			wantWidgets:         0,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt Vanadis wake suppression true",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			suppressVanadis:     true,
			wantPrompts:         0,
			wantDMs:             0,
			wantWidgets:         1,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt final Vanadis chunk advances build and dispatches completion prompt",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			suppressVanadis:     true,
			advanceVanadisRes: &VanadisAdvancementResult{
				Matched:        true,
				Advanced:       true,
				BuildID:        "build-done",
				ThreadID:       validSnowflake,
				BuildCompleted: true,
			},
			wantPrompts:    1,
			wantPromptSubs: []string{"Vanadis build build-done completed"},
			wantDMs:        0,
			wantWidgets:    1,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt final Vanadis chunk already advanced on merge syncs widget",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			suppressVanadis:     true,
			advanceVanadisRes: &VanadisAdvancementResult{
				Matched:  true,
				Advanced: false,
			},
			wantPrompts: 0,
			wantDMs:     0,
			wantWidgets: 1,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt Vanadis wake suppression DB error propagates",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			suppressVanadisErr:  errors.New("db check err"),
			wantErr:             true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runResolveThreadCase(ctx, t, tc)
		})
	}

	testConflictPromptStandalone(ctx, t, validSnowflake)
	testPostgresPRRegistrySyncs(ctx, t, validSnowflake)
	testDispatchAlertWithPrompt(ctx, t, validSnowflake)
	testVanadisSilentAutoFix(ctx, t, validSnowflake)
}

type resolveThreadCase struct {
	name                string
	isDeploymentSuccess bool
	useNilRegistry      bool
	prNumber            int
	targetID            string
	branch              string
	suppressVanadis     bool
	suppressVanadisErr  error
	advanceVanadisRes   *VanadisAdvancementResult
	wantTargetID        string
	wantErr             bool
	wantPrompts         int
	wantPromptSubs      []string
	wantDMs             int
	wantWidgets         int
	resolveErr          error
	widgetErr           error
}

func runResolveThreadCase(ctx context.Context, t *testing.T, tc resolveThreadCase) {
	t.Helper()
	var mockReg PRRegistryUpdater
	if !tc.useNilRegistry {
		mockReg = &mockPRRegistry{
			resolveTargetID:    tc.targetID,
			resolveBranch:      tc.branch,
			resolveErr:         tc.resolveErr,
			suppressVanadisRes: tc.suppressVanadis,
			suppressVanadisErr: tc.suppressVanadisErr,
			advanceVanadisRes:  tc.advanceVanadisRes,
		}
	}
	mockDisp := &mockOutboundDispatcher{widgetErr: tc.widgetErr}
	srv := newTestRouter(Config{}, nil, mockReg, mockDisp)

	if !tc.isDeploymentSuccess {
		gotTarget, err := srv.ResolveThreadForPR(ctx, "azylman/aerial", tc.prNumber)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ResolveThreadForPR() err = %v, wantErr = %v", err, tc.wantErr)
		}
		if gotTarget != tc.wantTargetID {
			t.Errorf("got targetID = %q, want %q", gotTarget, tc.wantTargetID)
		}
		return
	}

	err := srv.dispatchDeploymentSuccessPrompt(ctx, tc.targetID, "brain", "azylman/aerial", "sha123", 558, "alloc-1")
	if (err != nil) != tc.wantErr {
		t.Fatalf("dispatchDeploymentSuccessPrompt() err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertResolveThreadDispatcher(t, mockDisp, tc)
}

func assertResolveThreadDispatcher(t *testing.T, mockDisp *mockOutboundDispatcher, tc resolveThreadCase) {
	t.Helper()
	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != tc.wantPrompts {
		t.Errorf("got %d prompts, want %d", len(pCalls), tc.wantPrompts)
	}
	for _, sub := range tc.wantPromptSubs {
		if len(pCalls) == 0 || !strings.Contains(pCalls[0].Prompt, sub) {
			t.Errorf("expected prompt containing %q, got %+v", sub, pCalls)
		}
	}
	if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
		t.Errorf("got %d DMs, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
	}
	wCalls := mockDisp.WidgetCalls()
	if len(wCalls) != tc.wantWidgets {
		t.Errorf("got %d widgets, want %d", len(wCalls), tc.wantWidgets)
	} else if tc.wantWidgets > 0 && wCalls[0].Status != "open" {
		t.Errorf("got widget status %q, want \"open\"", wCalls[0].Status)
	}
}

func testConflictPromptStandalone(ctx context.Context, t *testing.T, validSnowflake string) {
	t.Helper()
	srv := newTestRouter(Config{}, nil, nil, &mockOutboundDispatcher{})
	if err := srv.dispatchConflictPrompt(ctx, validSnowflake, "azylman/aerial", "feat/x", "sha1", 0); err != nil {
		t.Errorf("dispatchConflictPrompt(pr=0): %v", err)
	}
}

func testPostgresPRRegistrySyncs(ctx context.Context, t *testing.T, validSnowflake string) {
	t.Helper()
	pool := &mockVanadisPgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			return mockVanadisPgxRow{scanFn: func(dest ...any) error {
				if len(dest) >= 1 {
					if s, ok := dest[0].(*string); ok {
						*s = validSnowflake
					}
				}
				return nil
			}}
		},
	}
	dispPG := &mockOutboundDispatcher{}
	srvPG := newTestRouter(Config{}, nil, NewPostgresPRRegistry(pool), dispPG)
	if err := srvPG.dispatchConflictPrompt(ctx, validSnowflake, "azylman/aerial", "main", "sha1", 42); err != nil {
		t.Errorf("dispatchConflictPrompt with pgReg: %v", err)
	}
	if err := srvPG.dispatchCIFailurePrompt(ctx, validSnowflake, "run", "azylman/aerial", "sha1", 42, "fail", "http://ci.test"); err != nil {
		t.Errorf("dispatchCIFailurePrompt with pgReg: %v", err)
	}
	if err := srvPG.dispatchCIFailurePrompt(ctx, validSnowflake, "RoboRev Code Review", "azylman/aerial", "sha1", 42, "findings", "http://ci.test"); err != nil {
		t.Errorf("dispatchCIFailurePrompt(roborev) with pgReg: %v", err)
	}
	pgDispatches := dispPG.DispatchCalls()
	if len(pgDispatches) < 3 {
		t.Fatalf("expected at least 3 dispatches from srvPG, got %d", len(pgDispatches))
	}
	if !strings.Contains(pgDispatches[0].Prompt, "Merge conflict detected on PR #42") || pgDispatches[0].TargetID != validSnowflake || pgDispatches[0].Source != "webhook" || pgDispatches[0].Effort != "medium" {
		t.Errorf("expected silent medium-effort conflict dispatch with target_id for standalone PR, got %+v", pgDispatches[0])
	}
	if !strings.Contains(pgDispatches[1].Prompt, "Please investigate and fix the failure by inspecting the failing logs") || pgDispatches[1].TargetID != validSnowflake || pgDispatches[1].Source != "webhook" || pgDispatches[1].Effort != "medium" {
		t.Errorf("expected silent medium-effort standard CI dispatch with target_id, got %+v", pgDispatches[1])
	}
	if !strings.Contains(pgDispatches[2].Prompt, "override-roborev 42") || pgDispatches[2].TargetID != validSnowflake || pgDispatches[2].Source != "webhook" || pgDispatches[2].Effort != "medium" {
		t.Errorf("expected silent medium-effort roborev override dispatch with target_id, got %+v", pgDispatches[2])
	}
}

func testDispatchAlertWithPrompt(ctx context.Context, t *testing.T, validSnowflake string) {
	t.Helper()
	dispAlert := &mockOutboundDispatcher{directMessageErr: errors.New("dm failed")}
	srvAlert := newTestRouter(Config{}, nil, nil, dispAlert)
	if err := srvAlert.dispatchAlertWithPrompt(ctx, validSnowflake, "alert", "dm text", "prompt text"); err != nil {
		t.Errorf("dispatchAlertWithPrompt with dm err: %v", err)
	}
	if err := srvAlert.dispatchAlertWithPrompt(ctx, "stateful_medium:discord-"+validSnowflake, "silent_alert", "", "prompt text", vanadisPromptOpts{silent: true, chunkID: "c-alert"}); err != nil {
		t.Errorf("dispatchAlertWithPrompt silent prefixed: %v", err)
	}
	if got := dispAlert.DispatchCalls(); len(got) != 1 || got[0].SessionID != "vanadis-c-alert" || got[0].TargetID != validSnowflake || got[0].VanadisChunkID != "c-alert" {
		t.Errorf("expected single structured target_id, vanadis_chunk_id, and session_id in silent dispatch, got %+v", got)
	}
	// invalid chunkID should not populate SessionID or VanadisChunkID
	if err := srvAlert.dispatchAlertWithPrompt(ctx, "stateful_medium:discord-"+validSnowflake, "silent_alert", "", "prompt text", vanadisPromptOpts{silent: true, chunkID: "bad:chunk:id"}); err != nil {
		t.Errorf("dispatchAlertWithPrompt bad chunk id: %v", err)
	}
	if got := dispAlert.DispatchCalls(); len(got) != 2 || got[1].SessionID != "" || got[1].VanadisChunkID != "" {
		t.Errorf("expected empty SessionID and VanadisChunkID on invalid chunk id, got %+v", got)
	}
}

func testVanadisSilentAutoFix(ctx context.Context, t *testing.T, validSnowflake string) {
	t.Helper()
	dispVanadis := &mockOutboundDispatcher{}
	srvVanadis := newTestRouter(Config{}, nil, &mockPRRegistry{suppressVanadisRes: true}, dispVanadis)
	if err := srvVanadis.dispatchCIFailurePrompt(ctx, validSnowflake, "unit", "azylman/aerial", "sha1", 885, "err", "http://ci.test"); err != nil {
		t.Errorf("dispatchCIFailurePrompt(vanadis): %v", err)
	}
	if err := srvVanadis.dispatchConflictPrompt(ctx, validSnowflake, "azylman/aerial", "feat/x", "sha1", 885); err != nil {
		t.Errorf("dispatchConflictPrompt(vanadis): %v", err)
	}
	if len(dispVanadis.DirectMessageCalls()) != 0 {
		t.Errorf("expected 0 DMs for Vanadis auto-fix, got %d", len(dispVanadis.DirectMessageCalls()))
	}
	vDispatches := dispVanadis.DispatchCalls()
	if len(vDispatches) != 2 || vDispatches[0].Source != "webhook" || vDispatches[0].Effort != "medium" || vDispatches[1].Source != "webhook" || vDispatches[1].Effort != "medium" {
		t.Errorf("expected 2 silent medium-effort dispatches for Vanadis auto-fix, got %+v", vDispatches)
	}

	tsVanadis := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/chunks/by-pr") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"matched":  true,
				"chunk_id": "chunk-885",
				"effort":   "low",
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(tsVanadis.Close)

	dispVanadisPG := &mockOutboundDispatcher{}
	srvVanadisPG := newTestRouter(Config{}, nil, NewPostgresPRRegistry(nil, WithVanadisClient(tsVanadis.URL, tsVanadis.Client())), dispVanadisPG)
	if err := srvVanadisPG.dispatchCIFailurePrompt(ctx, validSnowflake, "unit", "azylman/aerial", "sha1", 885, "err", "http://ci.test"); err != nil {
		t.Errorf("dispatchCIFailurePrompt(vanadis pg): %v", err)
	}
	vpgDispatches := dispVanadisPG.DispatchCalls()
	if len(vpgDispatches) != 1 || vpgDispatches[0].Source != "webhook" || vpgDispatches[0].SessionID != "vanadis-chunk-885" || vpgDispatches[0].Effort != "low" || vpgDispatches[0].VanadisChunkID != "chunk-885" || vpgDispatches[0].TargetID != validSnowflake {
		t.Errorf("expected silent low-effort dispatch with chunk-885 and target_id, got %+v", vpgDispatches)
	}
}

// --- 8. Server Endpoints ---

func TestServerEndpoints_HealthMetricsAndCoverage(t *testing.T) {
	oldCountersFn := httpkit.WriteCountersDirFn
	oldMetaFn := httpkit.WriteMetaDirFn
	t.Cleanup(func() {
		httpkit.WriteCountersDirFn = oldCountersFn
		httpkit.WriteMetaDirFn = oldMetaFn
	})

	RecordUniqueSkipped("")
	RecordUniqueSkipped("test_kind_metric")
	RecordJobEnqueued("", "", "")
	RecordJobRetry("", "", "")
	RecordJobDropped("", "", "")
	SetDiscordGatewayLatency(0.042)
	registerCollector(JobsEnqueuedTotal)

	tests := []struct {
		name          string
		method        string
		path          string
		mockMetaErr   error
		mockCountErr  error
		queueStatsErr bool
		wantCode      int
		wantBodySubs  []string
	}{
		{
			name:         "/healthz returns 200 and json",
			method:       http.MethodGet,
			path:         "/healthz",
			wantCode:     http.StatusOK,
			wantBodySubs: []string{`"status":"ok"`, `"service":"synapse"`},
		},
		{
			name:     "/metrics returns 200 and prometheus metrics with queue stats and gateway latency",
			method:   http.MethodGet,
			path:     "/metrics",
			wantCode: http.StatusOK,
			wantBodySubs: []string{
				"synapse_unique_skipped_total",
				`kind="test_kind_metric"`,
				"synapse_queue_depth",
				"synapse_queue_oldest_age_seconds",
				"synapse_discord_gateway_latency_seconds 0.042",
			},
		},
		{
			name:          "/metrics with queue stats error logs and still returns 200",
			method:        http.MethodGet,
			path:          "/metrics",
			queueStatsErr: true,
			wantCode:      http.StatusOK,
			wantBodySubs:  []string{"synapse_discord_gateway_latency_seconds 0.042"},
		},
		{
			name:     "/debug/coverage/flush calls flush and returns 200",
			method:   http.MethodGet,
			path:     "/debug/coverage/flush",
			wantCode: http.StatusOK,
		},
		{
			name:        "/debug/coverage/flush meta error still returns 200",
			method:      http.MethodGet,
			path:        "/debug/coverage/flush",
			mockMetaErr: errors.New("simulated meta flush error"),
			wantCode:    http.StatusOK,
		},
		{
			name:         "/debug/coverage/flush counters error returns 500",
			method:       http.MethodGet,
			path:         "/debug/coverage/flush",
			mockCountErr: errors.New("simulated counters flush error"),
			wantCode:     http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			httpkit.WriteMetaDirFn = func(dir string) error {
				return tc.mockMetaErr
			}
			httpkit.WriteCountersDirFn = func(dir string) error {
				return tc.mockCountErr
			}

			srv := NewRouterServer(fixedConfig(Config{Port: "4020"}), nil)
			mockQ := &mockQueueStatsQuerier{
				rows: &mockPgxRows{
					rows: []mockRowData{
						{queue: "default", state: "available", count: 4, oldestAge: 33.5},
					},
				},
			}
			if tc.queueStatsErr {
				mockQ.queryErr = errors.New("simulated queue query error")
			}
			srv.SetQueueStats(mockQ)
			srv.SetGatewayLatencyFn(func() time.Duration {
				return 42 * time.Millisecond
			})

			rec := doHTTPRequest(srv.Routes(), tc.method, tc.path, nil, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			for _, sub := range tc.wantBodySubs {
				if !strings.Contains(rec.Body.String(), sub) {
					t.Errorf("body missing %q: %s", sub, rec.Body.String())
				}
			}
		})
	}

	rec := httptest.NewRecorder()
	httpkit.WriteJSON(rec, http.StatusOK, make(chan int))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("writeJSON code = %d, want 500", rec.Code)
	}
}

func newTestReloadable(initial, next Config) *httpkit.Reloadable[Config] {
	return httpkit.NewReloadable(&initial, func() (*Config, error) {
		reloaded := next
		return &reloaded, nil
	}, synapseRestartRequired)
}

func TestRunServerLifecycle(t *testing.T) {
	tests := []runServerLifecycleCase{
		{
			name:             "runServer start, loopback reload, and graceful cancel shutdown",
			config:           Config{Port: "0"},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadUnchanged,
		},
		{
			name:             "runServer live reload applies non-restart field",
			config:           Config{Port: "0"},
			reloadConfig:     &Config{Port: "0", SystemChannelID: "123456789012345678"},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadApplied,
		},
		{
			name:    "runServer invalid port -1 returns error",
			config:  Config{Port: "-1"},
			wantErr: true,
		},
		{
			name:      "runApp pre-canceled context returns clean or server closed",
			config:    Config{Port: "0"},
			preCancel: true,
			isRunApp:  true,
		},
		{
			name:         "runApp exits cleanly when reload requires restart",
			config:       Config{Port: "0"},
			reloadConfig: &Config{Port: "4021"},
			isRunApp:     true,
		},
		{
			name:    "runServer invalid postgres url returns error",
			config:  Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"},
			wantErr: true,
		},
		{
			name:    "runServer river start connection error",
			config:  Config{Port: "0", PostgresURL: "postgres://fake:fake@127.0.0.1:54329/fakedb?sslmode=disable"},
			wantErr: true,
		},
		{
			name:     "runApp invalid port returns error",
			config:   Config{Port: "-1"},
			isRunApp: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next := tc.config
			if tc.reloadConfig != nil {
				next = *tc.reloadConfig
			}
			cfgSrc := newTestReloadable(tc.config, next)
			if tc.wantGraceful {
				runServerGracefulCase(t, tc, cfgSrc, next)
				return
			}
			if tc.isRunApp {
				runServerAppCase(t, tc, cfgSrc)
				return
			}
			err := runServer(context.Background(), cfgSrc, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("runServer() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

type runServerLifecycleCase struct {
	name             string
	config           Config
	reloadConfig     *Config
	preCancel        bool
	isRunApp         bool
	wantErr          bool
	wantGraceful     bool
	wantReloadStatus httpkit.ReloadStatus
}

func runServerGracefulCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config], next Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		done <- runServer(ctx, cfgSrc, func(addr string) {
			ready <- addr
		})
	}()

	var addr string
	select {
	case addr = <-ready:
		if addr == "" {
			t.Errorf("expected non-empty listening address")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server to start")
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	resp, err := http.Post("http://"+net.JoinHostPort("127.0.0.1", port)+"/-/reload", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /-/reload: %v", err)
	}
	var got struct {
		Status httpkit.ReloadStatus `json:"status"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&got)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || decodeErr != nil || got.Status != tc.wantReloadStatus {
		t.Errorf("POST /-/reload = %d %q (decode err %v), want 200 %q", resp.StatusCode, got.Status, decodeErr, tc.wantReloadStatus)
	}
	if gotSys := cfgSrc.Current().SystemChannelID; gotSys != next.SystemChannelID {
		t.Errorf("SystemChannelID after reload = %q, want %q", gotSys, next.SystemChannelID)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected clean shutdown, got error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server shutdown")
	}
}

func runServerAppCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config]) {
	t.Helper()
	ctxC, cancelC := context.WithCancel(context.Background())
	if tc.preCancel {
		cancelC()
	} else {
		defer cancelC()
	}
	if tc.reloadConfig != nil {
		if status, err := cfgSrc.Reload(); status != httpkit.ReloadRestarting || err != nil {
			t.Fatalf("Reload() = %q, %v; want %q", status, err, httpkit.ReloadRestarting)
		}
	}
	err := runApp(ctxC, cfgSrc)
	if (err != nil) != tc.wantErr && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("unexpected error from runApp: %v", err)
	}
}

// --- 9. Vanadis Build Queue Advancement & Wake Suppression Tests ---

func TestAdvanceVanadisChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validThread := "123456789012345678"

	tests := []advanceVanadisCase{
		{
			name:           "nil registry is clean no-op",
			useNilRegistry: true,
			prNumber:       10,
			wantErr:        false,
		},
		{
			name:     "prNumber <= 0 is clean no-op",
			prNumber: 0,
			wantErr:  false,
		},
		{
			name:       "unmatched result returns nil",
			prNumber:   10,
			advanceRes: &VanadisAdvancementResult{Matched: false},
			wantErr:    false,
		},
		{
			name:          "matched but already advanced result syncs widget and returns nil",
			prNumber:      10,
			advanceRes:    &VanadisAdvancementResult{Matched: true, Advanced: false},
			dispWidgetErr: errors.New("transient widget error"),
			wantErr:       false,
		},
		{
			name:       "registry error returns error",
			prNumber:   10,
			advanceErr: errors.New("db failure"),
			wantErr:    true,
		},
		{
			name:     "mid-run chunk advancement syncs widget and succeeds",
			prNumber: 42,
			advanceRes: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         true,
				BuildID:          "build-mid",
				ThreadID:         validThread,
				MergedChunkIndex: 0,
				MergedChunkTitle: "Chunk One",
				NextChunkIndex:   1,
				BuildCompleted:   false,
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, Title: "Chunk One", Status: "merged"},
					{ChunkIndex: 1, Title: "Chunk Two", Status: "pending"},
				},
			},
			dispDirectErr: errors.New("transient dm error warning"),
			wantErr:       false,
		},
		{
			name:     "mid-run chunk advancement with widget error logs warning",
			prNumber: 42,
			advanceRes: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         true,
				BuildID:          "build-mid",
				ThreadID:         validThread,
				MergedChunkIndex: 0,
				MergedChunkTitle: "Chunk One",
				NextChunkIndex:   1,
				BuildCompleted:   false,
			},
			dispWidgetErr: errors.New("transient widget error"),
			wantErr:       false,
		},
		{
			name:     "final build completion dispatches widget sync and completion prompt",
			prNumber: 42,
			advanceRes: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         true,
				BuildID:          "build-done",
				ThreadID:         validThread,
				Tier:             2,
				Objective:        "Complete all chunks",
				MergedChunkIndex: 1,
				MergedChunkTitle: "Chunk Two",
				NextChunkIndex:   -1,
				BuildCompleted:   true,
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, Title: "Chunk One", Status: "merged", PRNumber: 41},
					{ChunkIndex: 1, Title: "Chunk Two", Status: "merged", PRNumber: 42},
				},
			},
			dispWidgetErr:  errors.New("widget err"),
			repeatCall:     true,
			wantErr:        false,
			wantPrompts:    1,
			wantPromptSubs: []string{"Vanadis build build-done completed"},
			wantDMs:        0,
		},
		{
			name:     "retry of already-completed build with Advanced false still dispatches completion prompt once",
			prNumber: 42,
			advanceRes: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         false,
				BuildID:          "build-retry-done",
				ThreadID:         validThread,
				Tier:             2,
				Objective:        "Complete all chunks on retry",
				MergedChunkIndex: 1,
				MergedChunkTitle: "Chunk Two",
				NextChunkIndex:   -1,
				BuildCompleted:   true,
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, Title: "Chunk One", Status: "merged", PRNumber: 41},
					{ChunkIndex: 1, Title: "Chunk Two", Status: "merged", PRNumber: 42},
				},
			},
			repeatCall:     true,
			wantErr:        false,
			wantPrompts:    1,
			wantPromptSubs: []string{"Vanadis build build-retry-done completed", "Chunk 2/2: Chunk Two"},
			wantDMs:        0,
		},
		{
			name:     "final build completion prompt error propagates without marking dispatched",
			prNumber: 42,
			advanceRes: &VanadisAdvancementResult{
				Matched:        true,
				Advanced:       true,
				BuildID:        "",
				ThreadID:       validThread,
				BuildCompleted: true,
			},
			dispPromptErr: errors.New("502 Bad Gateway"),
			wantErr:       true,
			wantPrompts:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runAdvanceVanadisCase(ctx, t, tc)
		})
	}
}

type advanceVanadisCase struct {
	name           string
	useNilRegistry bool
	prNumber       int
	advanceRes     *VanadisAdvancementResult
	advanceErr     error
	dispDirectErr  error
	dispWidgetErr  error
	dispPromptErr  error
	repeatCall     bool
	wantErr        bool
	wantPrompts    int
	wantPromptSubs []string
	wantDMs        int
}

func runAdvanceVanadisCase(ctx context.Context, t *testing.T, tc advanceVanadisCase) {
	t.Helper()
	var reg PRRegistryUpdater
	if !tc.useNilRegistry {
		reg = &mockPRRegistry{
			advanceVanadisRes: tc.advanceRes,
			advanceVanadisErr: tc.advanceErr,
		}
	}
	disp := &mockOutboundDispatcher{
		directMessageErr: tc.dispDirectErr,
		widgetErr:        tc.dispWidgetErr,
		promptErr:        tc.dispPromptErr,
	}
	srv := newTestRouter(Config{}, nil, reg, disp)

	err := srv.advanceVanadisChunk(ctx, "azylman/aerial", tc.prNumber)
	if (err != nil) != tc.wantErr {
		t.Fatalf("advanceVanadisChunk() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.repeatCall {
		if err2 := srv.advanceVanadisChunk(ctx, "azylman/aerial", tc.prNumber); err2 != nil {
			t.Fatalf("repeat advanceVanadisChunk() err = %v, want nil", err2)
		}
	}
	if tc.useNilRegistry || tc.prNumber <= 0 {
		assertAdvanceVanadisGuards(ctx, t, srv, tc.prNumber)
	}
	assertAdvanceVanadisDispatcher(t, disp, tc.wantPrompts, tc.wantPromptSubs, tc.wantDMs)
}

func assertAdvanceVanadisGuards(ctx context.Context, t *testing.T, srv *RouterServer, prNumber int) {
	t.Helper()
	if err := srv.advanceVanadisChunkOnMerge(ctx, "azylman/aerial", prNumber); err != nil {
		t.Errorf("expected advanceVanadisChunkOnMerge nil error for guard case, got %v", err)
	}
	vanadisPR, err := srv.isVanadisPR(ctx, "azylman/aerial", prNumber)
	if err != nil || vanadisPR {
		t.Errorf("expected isVanadisPR false for guard case")
	}
	if err := srv.advanceVanadisChunkOnSync(ctx, "azylman/aerial", prNumber, true); err != nil {
		t.Errorf("expected advanceVanadisChunkOnSync nil error for guard case, got %v", err)
	}
}

func assertAdvanceVanadisDispatcher(t *testing.T, disp *mockOutboundDispatcher, wantPrompts int, wantPromptSubs []string, wantDMs int) {
	t.Helper()
	if len(disp.PromptCalls()) != wantPrompts {
		t.Errorf("got %d prompts, want %d", len(disp.PromptCalls()), wantPrompts)
	}
	for _, sub := range wantPromptSubs {
		if len(disp.PromptCalls()) == 0 || !strings.Contains(disp.PromptCalls()[0].Prompt, sub) {
			t.Errorf("prompt missing %q: %+v", sub, disp.PromptCalls())
		}
	}
	if len(disp.DirectMessageCalls()) != wantDMs {
		t.Errorf("got %d DMs, want %d", len(disp.DirectMessageCalls()), wantDMs)
	}
}
