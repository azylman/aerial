package router

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func mkDeployEvt(event, job, repo, sha string, pr int, targetID, status, details string) DeployEvent {
	return DeployEvent{Event: event, JobName: job, Repo: repo, CommitSHA: sha, PRNumber: pr, TargetID: targetID, Status: status, Details: details}
}

func mkKustomizationEvt(event, job, repo, sha string, pr int, targetID, status string) DeployEvent {
	return DeployEvent{Event: event, SourceKind: "Kustomization", JobName: job, Repo: repo, CommitSHA: sha, PRNumber: pr, TargetID: targetID, Status: status}
}

func scanBuildChecksRow(dest []any, status, checksJSON string) error {
	if len(dest) < 2 {
		return nil
	}
	if s, ok := dest[0].(*string); ok {
		*s = status
	}
	if b, ok := dest[1].(*[]byte); ok {
		*b = []byte(checksJSON)
	}
	return nil
}

func newMockBuildChecksPgRegistry(status string, checksJSON string, extraRowFns ...func(sql string) pgx.Row) *PostgresPRRegistry {
	return NewPostgresPRRegistry(&mockSynapsePgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT status, COALESCE(metadata, '{}'::jsonb)") {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					return scanBuildChecksRow(dest, status, checksJSON)
				}}
			}
			for _, fn := range extraRowFns {
				if row := fn(sql); row != nil {
					return row
				}
			}
			return mockSynapsePgxRow{}
		},
		execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	})
}

type processDeployEventCase struct {
	name               string
	evt                DeployEvent
	extraEvts          []DeployEvent
	sysChan            string
	reg                *mockPRRegistry
	pgReg              *PostgresPRRegistry
	disp               *mockOutboundDispatcher
	bus                *mockEventBus
	wantErr            bool
	wantPR             int
	wantTargetID       string
	wantDeployingCalls int
	wantAdvanceCalls   int
	wantPrompts        int
	wantDispatches     int
	wantDMs            int
	wantDMSubstr       string
	wantPromptSubs     []string
	wantRequested      []string
	wantReconciles     []fluxKustomizationRef
	wantPublished      []wantPREvent
}

func processDeployEventBasicCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name: "deploy_started ignored without PR number or targetID",
			evt:  mkDeployEvt("deploy_started", "brain", "azylman/aerial", "sha-test", 0, "", "started", ""),
		},
		{
			name: "deploy_success ignored without PR number or targetID",
			evt:  mkDeployEvt("deploy_success", "brain", "azylman/aerial", "sha-test", 0, "", "success", ""),
		},
		{
			name: "deploy_failed ignored without PR number or targetID",
			evt:  mkDeployEvt("deploy_failed", "brain", "azylman/aerial", "sha-test", 0, "", "failed", ""),
		},
		{
			name: "deploy_rollback ignored without PR number or targetID",
			evt:  mkDeployEvt("deploy_rollback", "brain", "azylman/aerial", "sha-test", 0, "", "rollback", ""),
		},
		{
			name:               "deploy_started resolves SHA and transitions deploying",
			evt:                mkDeployEvt("deploy_started", "webhooks-router", "azylman/aerial", "sha111", 0, "", "started", ""),
			reg:                &mockPRRegistry{resolvePRNum: 99, resolveTargetID: "target-99"},
			wantPR:             99,
			wantTargetID:       "target-99",
			wantDeployingCalls: 1,
		},
		{
			name:         "deploy_started tolerates registry error",
			evt:          mkDeployEvt("deploy_started", "webhooks-router", "azylman/aerial", "sha111", 0, "", "started", ""),
			reg:          &mockPRRegistry{resolvePRNum: 99, resolveTargetID: "target-99", deployingErr: errors.New("db error")},
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_success transitions state",
			evt:          mkDeployEvt("deploy_success", "webhooks-router", "azylman/aerial", "", 99, "target-99", "success", "deployment completed"),
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_rollback transitions state",
			evt:          mkDeployEvt("deploy_rollback", "webhooks-router", "azylman/aerial", "", 99, "target-99", "rollback", "rolled back"),
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_failed transitions state",
			evt:          mkDeployEvt("deploy_failed", "webhooks-router", "azylman/aerial", "", 99, "target-99", "failed", "deploy unhealthy"),
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_started valid snowflake",
			evt:          mkDeployEvt("deploy_started", "brain", "azylman/aerial", "sha1234", 527, "100000000000000001", "started", ""),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       527,
			wantTargetID: "100000000000000001",
		},
		{
			name:         "deploy_success valid snowflake",
			evt:          mkDeployEvt("deploy_success", "brain", "azylman/aerial", "sha3d2a", 528, "100000000000000001", "success", ""),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       528,
			wantTargetID: "100000000000000001",
			wantPrompts:  0,
		},
		{
			name:          "deploy_started then deploy_success publish lifecycle events to the bus",
			evt:           mkDeployEvt("deploy_started", "eri", "azylman/aerial", "sha-dep", 42, "123456789012345678", "", ""),
			extraEvts:     []DeployEvent{mkDeployEvt("deploy_success", "eri", "azylman/aerial", "sha-dep", 42, "123456789012345678", "", "")},
			sysChan:       "123456789012345678",
			reg:           &mockPRRegistry{resolveTargetID: "123456789012345678"},
			disp:          &mockOutboundDispatcher{},
			bus:           &mockEventBus{},
			wantPR:        42,
			wantTargetID:  "123456789012345678",
			wantPublished: []wantPREvent{{Event: "deploy_started", PRNumber: 42, MergeSHA: "sha-dep", Status: "deploying"}, {Event: "deploy_success", PRNumber: 42, MergeSHA: "sha-dep", Status: "deployed"}},
		},
		{
			name:           "deploy_rollback valid snowflake",
			evt:            mkDeployEvt("deploy_rollback", "webhooks-router", "azylman/aerial", "shafail", 529, "100000000000000001", "rollback", strings.Repeat("error detail line\n", 50)),
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         529,
			wantTargetID:   "100000000000000001",
			wantDispatches: 0,
		},
		{
			name:           "deploy_failed valid snowflake",
			evt:            mkDeployEvt("deploy_failed", "hangar", "azylman/aerial", "shabroken", 530, "100000000000000001", "failed", "exit status 1"),
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         530,
			wantTargetID:   "100000000000000001",
			wantDispatches: 0,
		},
		{
			name:    "deploy_failed prompt error reverts state and returns error",
			evt:     mkDeployEvt("deploy_failed", "eri", "azylman/aerial", "", 0, "100000000000000001", "failed", "exit status 1"),
			reg:     &mockPRRegistry{deployFailedUpdated: true},
			disp:    &mockOutboundDispatcher{promptErr: errors.New("eri 502 bad gateway")},
			wantErr: true,
		},
		{
			name:           "deploy_rollback without targetID falls back to system_channel name",
			evt:            mkDeployEvt("deploy_rollback", "eri", "azylman/aerial", "shafail", 532, "", "rollback", "dotenv parse error"),
			sysChan:        "aerial-dev",
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         532,
			wantTargetID:   "aerial-dev",
			wantDispatches: 0,
		},
	}
}

func processDeployEventSyncCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name: "sync_success without PR number",
			evt:  mkDeployEvt("sync_success", "", "azylman/aerial-config", "sha999", 0, "100000000000000001", "", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{},
		},
		{
			name:         "sync_failed with details",
			evt:          mkDeployEvt("sync_failed", "", "azylman/aerial-config", "sha888", 0, "100000000000000001", "", "merge conflict in rules/persona"),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantDMs:      1,
			wantPrompts:  1,
			wantDMSubstr: "⚠️ **(repo: aerial-config)** Git file sync failed. Following up...",
			wantPromptSubs: []string{
				"Git sync failed",
				"merge conflict in rules/persona",
				"Please investigate and fix",
			},
		},
		{
			name: "deploy_success invalid snowflake ignored",
			evt:  mkDeployEvt("deploy_success", "hangar", "", "", 0, "not-a-snowflake", "", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{},
		},
		{
			name: "deploy_started invalid snowflake ignored",
			evt:  mkDeployEvt("deploy_started", "hangar", "", "", 0, "not-a-snowflake", "", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{},
		},
		{
			name:             "sync_success late resolution via ResolvePRBySHA",
			evt:              mkDeployEvt("sync_success", "", "azylman/aerial-config", "dccc646186d7bf7d17efa6cac6f730d17f66125b", 0, "", "", ""),
			reg:              &mockPRRegistry{resolvePRNum: 241, resolveTargetID: "100000000000000001"},
			disp:             &mockOutboundDispatcher{},
			wantPR:           241,
			wantTargetID:     "100000000000000001",
			wantAdvanceCalls: 1,
			wantPrompts:      0,
		},
		{
			name:         "deploy_success on aerial-config",
			evt:          mkDeployEvt("deploy_success", "homepage", "azylman/aerial-config", "sha_cfg_succ", 242, "100000000000000001", "success", ""),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       242,
			wantTargetID: "100000000000000001",
			wantPrompts:  0,
		},
		{
			name:         "sync_failed with PR number",
			evt:          mkDeployEvt("sync_failed", "", "azylman/aerial", "merge542", 542, "100000000000000001", "failed", "merge conflict in rules/foo.md"),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       542,
			wantTargetID: "100000000000000001",
		},
		{
			name:               "deploy_started skipped when job already deployed",
			evt:                mkDeployEvt("deploy_started", "coven", "azylman/aerial", "bc1d3ef", 971, "100000000000000001", "started", ""),
			reg:                &mockPRRegistry{skipDeployingTransition: true},
			disp:               &mockOutboundDispatcher{},
			wantPR:             971,
			wantTargetID:       "100000000000000001",
			wantDeployingCalls: 1,
		},
		{
			name: "deploy_started with expected_jobs records expected sibling jobs before transition",
			evt: DeployEvent{
				Event: "deploy_started", JobName: "db-migrate", ExpectedJobs: []string{"coven", "db-migrate"},
				Repo: "azylman/aerial", CommitSHA: "bc1d3ef", PRNumber: 972, TargetID: "100000000000000001", Status: "started",
			},
			reg:                &mockPRRegistry{recordExpectedErr: errors.New("record expected warn")},
			disp:               &mockOutboundDispatcher{},
			wantPR:             972,
			wantTargetID:       "100000000000000001",
			wantDeployingCalls: 1,
		},
	}
}

func processDeployEventFluxWatcherCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name:         "deploy_success on aerial-core with build checks in metadata and merged status handles sync success",
			evt:          mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "sha-aerial-sync", 893, "100000000000000001", "success"),
			pgReg:        newMockBuildChecksPgRegistry("merged", `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`),
			disp:         &mockOutboundDispatcher{},
			wantPR:       893,
			wantTargetID: "100000000000000001",
			wantPrompts:  0,
		},
		{
			name: "deploy_started on aerial-core skipped when waiting for image build",
			evt:  mkKustomizationEvt("deploy_started", "aerial-core", "azylman/aerial", "sha-aerial-started", 89894, "100000000000000001", "started"),
			pgReg: newMockBuildChecksPgRegistry("merged", `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
				func(sql string) pgx.Row {
					if strings.Contains(sql, "SET status = 'deploying'") || strings.Contains(sql, "SELECT target_id, title, status") {
						return mockSynapsePgxRow{scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if s, ok := dest[0].(*string); ok {
									*s = "100000000000000001"
								}
							}
							return nil
						}}
					}
					return nil
				},
			),
			wantPR:       89894,
			wantTargetID: "100000000000000001",
			wantPrompts:  0,
		},
		{
			name:         "sync_success on git-sync with ci_checks build jobs stays in synced without firing deploy prompt",
			evt:          mkDeployEvt("sync_success", "git-sync", "azylman/aerial", "sha-aerial-sync-pending", 89895, "100000000000000001", "success", ""),
			pgReg:        newMockBuildChecksPgRegistry("merged", `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`),
			disp:         &mockOutboundDispatcher{},
			wantPR:       89895,
			wantTargetID: "100000000000000001",
			wantPrompts:  0,
		},
		{
			name:         "Kustomization deploy_success never completes a watcher-tracked PR",
			evt:          mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "sha-pr-1485", 1485, "100000000000000001", "success"),
			reg:          &mockPRRegistry{hasKustomizationReqs: true, resolveTargetID: "100000000000000001"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1485,
			wantTargetID: "100000000000000001",
		},
		{
			name:         "CI job deploy_success never completes a watcher-tracked PR",
			evt:          mkDeployEvt("deploy_success", "eri", "azylman/aerial", "sha-pr-1485", 1485, "100000000000000001", "success", ""),
			reg:          &mockPRRegistry{hasKustomizationReqs: true, resolveTargetID: "100000000000000001"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1485,
			wantTargetID: "100000000000000001",
		},
		{
			name:         "Kustomization deploy_failed alert defers to the watcher for a tracked PR",
			evt:          mkKustomizationEvt("deploy_failed", "aerial-core", "azylman/aerial", "sha-pr-1485", 1485, "100000000000000001", "failed"),
			reg:          &mockPRRegistry{hasKustomizationReqs: true, deployFailedUpdated: true, resolveTargetID: "100000000000000001"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1485,
			wantTargetID: "100000000000000001",
		},
		{
			name:         "untracked PR keeps legacy Kustomization deploy_success completion",
			evt:          mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "sha-legacy", 1300, "100000000000000001", "success"),
			reg:          &mockPRRegistry{resolveTargetID: "100000000000000001"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1300,
			wantTargetID: "100000000000000001",
			wantPrompts:  0,
		},
		{
			name:         "tracked-PR lookup error falls back to legacy completion",
			evt:          mkDeployEvt("deploy_success", "eri", "azylman/aerial", "sha-legacy", 1301, "100000000000000001", "success", ""),
			reg:          &mockPRRegistry{hasKustomizationReqsErr: errors.New("db down"), resolveTargetID: "100000000000000001"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1301,
			wantTargetID: "100000000000000001",
			wantPrompts:  0,
		},
	}
}

func processDeployEventFluxDiscoveryCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name: "sync_success on a Flux-sourced repo defers completion to the kustomization watcher",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-config-only", 1640, "100000000000000001", "success", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}, {Namespace: "flux-system", Name: "aerial-config-apps"}},
			}},
			wantPR:             1640,
			wantTargetID:       "100000000000000001",
			wantDeployingCalls: 1,
			wantRequested:      []string{"aerial-config", "aerial-config-apps"},
			wantReconciles:     []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-config"}, {Namespace: "flux-system", Name: "aerial-config-apps"}},
		},
		{
			name: "sync_success with empty targetID on a Flux-sourced repo defers kustomization watcher",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-config-only-2", 1641, "", "success", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}},
			}},
			wantPR:             1641,
			wantDeployingCalls: 1,
			wantRequested:      []string{"aerial-config"},
			wantReconciles:     []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-config"}},
		},
		{
			name: "sync_success on a repo with no Flux source completes as deployed",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/mirrormere", "sha-no-flux", 1642, "100000000000000001", "success", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}},
			}},
			wantPR:           1642,
			wantTargetID:     "100000000000000001",
			wantAdvanceCalls: 1,
			wantPrompts:      0,
		},
		{
			name:         "sync_success with image build pending stays synced without discovering kustomizations",
			evt:          DeployEvent{Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"eri"}, Repo: "azylman/aerial", CommitSHA: "sha-image-pending", PRNumber: 1643, TargetID: "100000000000000001", Status: "success"},
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{repoDiscoverErr: errors.New("unexpected discovery while images are pending")},
			wantPR:       1643,
			wantTargetID: "100000000000000001",
		},
		{
			name:    "sync_success kustomization discovery error propagates",
			evt:     mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-discover-err", 1644, "100000000000000001", "success", ""),
			reg:     &mockPRRegistry{},
			disp:    &mockOutboundDispatcher{repoDiscoverErr: errors.New("kube api down")},
			wantErr: true,
		},
		{
			name: "sync_success kustomization request record error propagates",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-record-err", 1645, "100000000000000001", "success", ""),
			reg:  &mockPRRegistry{kustomizationRequestErr: errors.New("db down")},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}},
			}},
			wantErr: true,
		},
		{
			name: "sync_success deploying transition error propagates",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-deploying-err", 1646, "100000000000000001", "success", ""),
			reg:  &mockPRRegistry{deployingErr: errors.New("db down")},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}},
			}},
			wantErr: true,
		},
		{
			name: "sync_success reconcile patch error propagates after requests are recorded",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-patch-err", 1647, "100000000000000001", "success", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{
				repoKustomizations: map[string][]fluxKustomizationRef{"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}}},
				reconcileErrs:      map[string]error{"aerial-config": errors.New("patch forbidden")},
			},
			wantErr: true,
		},
	}
}

func processDeployEventFluxCases() []processDeployEventCase {
	return append(processDeployEventFluxWatcherCases(), processDeployEventFluxDiscoveryCases()...)
}

func processDeployEventCases() []processDeployEventCase {
	basic := processDeployEventBasicCases()
	syncCases := processDeployEventSyncCases()
	flux := processDeployEventFluxCases()
	all := make([]processDeployEventCase, 0, len(basic)+len(syncCases)+len(flux))
	all = append(all, basic...)
	all = append(all, syncCases...)
	all = append(all, flux...)
	return all
}

func TestProcessDeployEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range processDeployEventCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProcessDeployEventCase(ctx, t, tc)
		})
	}
}

func runProcessDeployEventCase(ctx context.Context, t *testing.T, tc processDeployEventCase) {
	t.Helper()
	var reg PRRegistryUpdater
	switch {
	case tc.pgReg != nil:
		reg = tc.pgReg
	case tc.reg != nil:
		reg = tc.reg
	default:
		reg = &mockPRRegistry{}
	}
	var disp OutboundDispatcher
	if tc.disp != nil {
		disp = tc.disp
	}
	cfg := Config{SystemChannelID: tc.sysChan}
	client := &http.Client{Transport: &zeroCallTransport{}}
	srv := newTestRouter(cfg, client, reg, disp)
	if tc.bus != nil {
		srv.SetEventBus(tc.bus)
	}
	res, err := srv.ProcessDeployEvent(ctx, tc.evt)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ProcessDeployEvent() err = %v, wantErr %v", err, tc.wantErr)
	}
	for _, extra := range tc.extraEvts {
		if _, err := srv.ProcessDeployEvent(ctx, extra); err != nil {
			t.Fatalf("ProcessDeployEvent(extra) err = %v", err)
		}
	}
	if tc.wantErr {
		return
	}
	if tc.wantPR != 0 && res.PRNumber != tc.wantPR {
		t.Errorf("PRNumber = %d, want %d", res.PRNumber, tc.wantPR)
	}
	if tc.wantTargetID != "" && res.TargetID != tc.wantTargetID {
		t.Errorf("TargetID = %q, want %q", res.TargetID, tc.wantTargetID)
	}
	assertDeployEventRegResults(t, tc)
	assertDeployEventDispResults(t, tc)
	if tc.bus != nil {
		assertPREvents(t, tc.bus, tc.wantPublished)
	}
}

func assertDeployEventRegResults(t *testing.T, tc processDeployEventCase) {
	t.Helper()
	if tc.reg == nil {
		return
	}
	if tc.wantDeployingCalls > 0 && len(tc.reg.deployingCalls) != tc.wantDeployingCalls {
		t.Errorf("deployingCalls = %d, want %d", len(tc.reg.deployingCalls), tc.wantDeployingCalls)
	}
	var requested []string
	for _, call := range tc.reg.kustomizationRequestCalls {
		for name := range call {
			requested = append(requested, name)
		}
	}
	slices.Sort(requested)
	if !slices.Equal(requested, tc.wantRequested) {
		t.Errorf("recorded kustomization requests = %v, want %v", requested, tc.wantRequested)
	}
}

func assertDeployEventDispResults(t *testing.T, tc processDeployEventCase) {
	t.Helper()
	if tc.disp == nil {
		return
	}
	if got := tc.disp.ReconcileCalls(); !slices.Equal(got, tc.wantReconciles) {
		t.Errorf("reconcile calls = %v, want %v", got, tc.wantReconciles)
	}
	dms := tc.disp.DirectMessageCalls()
	if len(dms) != tc.wantDMs {
		t.Errorf("DMs = %d, want %d", len(dms), tc.wantDMs)
	}
	if tc.wantDMSubstr != "" && (len(dms) == 0 || !strings.Contains(dms[len(dms)-1].Content, tc.wantDMSubstr)) {
		t.Errorf("DM = %v, want substr %q", dms, tc.wantDMSubstr)
	}
	dispatches := tc.disp.DispatchCalls()
	if len(dispatches) != tc.wantDispatches {
		t.Errorf("dispatches = %d, want %d", len(dispatches), tc.wantDispatches)
	}
	if tc.wantDispatches > 0 && len(dispatches) > 0 {
		last := dispatches[len(dispatches)-1].Prompt
		for _, sub := range tc.wantPromptSubs {
			if !strings.Contains(last, sub) {
				t.Errorf("dispatch = %v, want substr %q", dispatches, sub)
			}
		}
	}
	prompts := tc.disp.PromptCalls()
	if len(prompts) != tc.wantPrompts {
		t.Errorf("prompts = %d, want %d", len(prompts), tc.wantPrompts)
	}
	if tc.wantPrompts > 0 && len(prompts) > 0 {
		last := prompts[len(prompts)-1].Prompt
		for _, sub := range tc.wantPromptSubs {
			if !strings.Contains(last, sub) {
				t.Errorf("prompt = %v, want substr %q", prompts, sub)
			}
		}
	}
}
