package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
)

// --- Shared Test Mocks & Helpers ---

type mockEventBus struct {
	pubsub.Bus

	mu                   sync.Mutex
	insertedJobs         []GitHubWebhookArgs
	insertedDeployJobs   []DeployWebhookArgs
	insertedDiscordJobs  []DiscordIngressArgs
	insertedInteractJobs []DiscordInteractionArgs
	publishedPREvents    []pubsub.PRLifecycleEvent
	err                  error
}

func (m *mockEventBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.recordPublishedEvents(data)
	m.recordPublishedJobs(data)
	return nil
}

func (m *mockEventBus) recordPublishedEvents(data any) {
	switch a := data.(type) {
	case pubsub.PRLifecycleEvent:
		m.publishedPREvents = append(m.publishedPREvents, a)
	case *pubsub.PRLifecycleEvent:
		if a != nil {
			m.publishedPREvents = append(m.publishedPREvents, *a)
		}
	}
}

func (m *mockEventBus) recordPublishedJobs(data any) {
	switch a := data.(type) {
	case GitHubWebhookArgs:
		m.insertedJobs = append(m.insertedJobs, a)
	case *GitHubWebhookArgs:
		if a != nil {
			m.insertedJobs = append(m.insertedJobs, *a)
		}
	case DeployWebhookArgs:
		m.insertedDeployJobs = append(m.insertedDeployJobs, a)
	case *DeployWebhookArgs:
		if a != nil {
			m.insertedDeployJobs = append(m.insertedDeployJobs, *a)
		}
	case DiscordIngressArgs:
		m.insertedDiscordJobs = append(m.insertedDiscordJobs, a)
	case *DiscordIngressArgs:
		if a != nil {
			m.insertedDiscordJobs = append(m.insertedDiscordJobs, *a)
		}
	case DiscordInteractionArgs:
		m.insertedInteractJobs = append(m.insertedInteractJobs, a)
	case *DiscordInteractionArgs:
		if a != nil {
			m.insertedInteractJobs = append(m.insertedInteractJobs, *a)
		}
	}
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

type zeroCallTransport struct {
	calls atomic.Int32
}

func (z *zeroCallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	z.calls.Add(1)
	return nil, errors.New("unexpected outbound HTTP call")
}

type loopbackOnlyTransport struct{ base http.RoundTripper }

func (l *loopbackOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if h := req.URL.Hostname(); h != "127.0.0.1" && h != "::1" && h != "localhost" {
		return nil, errors.New("hermetic test guard: blocked non-loopback HTTP call to " + req.URL.Host)
	}
	return l.base.RoundTrip(req)
}

func init() {
	http.DefaultTransport = &loopbackOnlyTransport{base: http.DefaultTransport}
}

type prCall struct {
	repo     string
	prNumber int
	mergeSHA string
	headSHA  string
	sha      string
	jobs     []string
}

type prStatusCall struct {
	repo     string
	prNumber int
	status   string
	metadata map[string]any
}

type deployedResult struct {
	targetID string
	prNumber int
	mergeSHA string
	repo     string
	updated  bool
	err      error
}

type mockPRRegistry struct {
	mergedCalls         []prCall
	syncCalls           []prCall
	closedUnmergedCalls []prCall
	ciFailedCalls       []prCall
	ciFailedBySHACalls  []prCall
	backfillCalls       []prCall
	resolveBySHACalls   []prCall

	mergedTargetID   string
	syncTargetID     string
	closedTargetID   string
	backfillTargetID string

	resolvePRNum            int
	resolveBranch           string
	resolveHeadSHA          string
	resolveTargetID         string
	resolveBySHATargetEmpty bool
	resolveErr              error

	ciFailedTargetID     string
	ciFailedUpdated      bool
	ciFailedErr          error
	ciFailedResults      []bool
	ciFailedBySHAPRNum   int
	ciFailedBySHATarget  string
	ciFailedBySHAUpdated bool
	ciFailedBySHAErr     error

	closedErr error

	conflictCalls    []prCall
	conflictTargetID string
	conflictUpdated  bool
	conflictErr      error

	deployingCalls          []prCall
	deployingTargetID       string
	deployingErr            error
	skipDeployingTransition bool

	deployedJobCalls []string
	deployedTargetID string
	deployedPRNum    int
	deployedMergeSHA string
	deployedRepo     string
	deployedUpdated  bool
	deployedErr      error
	deployedResults  []deployedResult

	deployFailedCalls    []prCall
	deployFailedTargetID string
	deployFailedUpdated  bool
	deployFailedErr      error

	deployFailedJobCalls    []string
	deployFailedJobTargetID string
	deployFailedJobPRNum    int
	deployFailedJobMergeSHA string
	deployFailedJobRepo     string
	deployFailedJobUpdated  bool
	deployFailedJobErr      error

	listDeployingPRs []DeployingPR
	listDeployingErr error

	recordCheckCalls   []recordCheckStatusCall
	recordCheckUpdated bool
	recordCheckErr     error

	registerCalls    []PRRegisterRequest
	registerTargetID string
	registerErr      error

	ensureCalls    []prCall
	ensureTargetID string
	ensureErr      error

	recordExpectedCalls []prCall
	recordExpectedRes   bool
	recordExpectedErr   error
	hasExpectedCalls    []prCall
	hasExpectedRes      bool
	hasExpectedErr      error

	recordJobStateCalls   []recordJobStateCall
	recordJobStateRepo    string
	recordJobStatePRNum   int
	recordJobStateUpdated bool
	recordJobStateErr     error

	appendPRJobsCalls   []prCall
	appendPRJobsErr     error
	updatePRStatusCalls []prStatusCall
	updatePRStatusErr   error

	kustomizationRequestCalls []map[string]kustomizationRequest
	kustomizationRequestErr   error

	markKustomizationsCalls []prCall
	markKustomizationsRes   bool
	markKustomizationsErr   error
	hasKustomizationReqs    bool
	hasKustomizationReqsErr error

	err error
}

func (m *mockPRRegistry) checkErr(specific error) error {
	if specific != nil {
		return specific
	}
	return m.err
}

func (m *mockPRRegistry) TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (string, error) {
	if err := m.checkErr(m.deployingErr); err != nil {
		return "", err
	}
	m.deployingCalls = append(m.deployingCalls, prCall{repo: repo, prNumber: prNumber, jobs: jobs})
	if m.skipDeployingTransition {
		return "", nil
	}
	return firstNonEmpty(m.deployingTargetID, m.resolveTargetID, "100000000000000001"), nil
}

func (m *mockPRRegistry) AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (JobTransition, error) {
	m.deployedJobCalls = append(m.deployedJobCalls, jobName)
	if len(m.deployedResults) > 0 {
		res := m.deployedResults[0]
		m.deployedResults = m.deployedResults[1:]
		if res.err != nil {
			return JobTransition{}, res.err
		}
		return JobTransition{TargetID: res.targetID, PRNumber: res.prNumber, MergeSHA: res.mergeSHA, Repo: res.repo, Updated: res.updated}, nil
	}
	if err := m.checkErr(m.deployedErr); err != nil {
		return JobTransition{}, err
	}
	return JobTransition{TargetID: m.deployedTargetID, PRNumber: m.deployedPRNum, MergeSHA: m.deployedMergeSHA, Repo: m.deployedRepo, Updated: m.deployedUpdated}, nil
}

func (m *mockPRRegistry) AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if err := m.checkErr(m.deployFailedErr); err != nil {
		return "", false, err
	}
	m.deployFailedCalls = append(m.deployFailedCalls, prCall{repo: repo, prNumber: prNumber})
	return m.deployFailedTargetID, m.deployFailedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (JobTransition, error) {
	if err := m.checkErr(m.deployFailedJobErr); err != nil {
		return JobTransition{}, err
	}
	m.deployFailedJobCalls = append(m.deployFailedJobCalls, jobName)
	return JobTransition{TargetID: m.deployFailedJobTargetID, PRNumber: m.deployFailedJobPRNum, MergeSHA: m.deployFailedJobMergeSHA, Repo: m.deployFailedJobRepo, Updated: m.deployFailedJobUpdated}, nil
}

func (m *mockPRRegistry) ListDeployingPRs(ctx context.Context) ([]DeployingPR, error) {
	if err := m.checkErr(m.listDeployingErr); err != nil {
		return nil, err
	}
	return m.listDeployingPRs, nil
}

func (m *mockPRRegistry) AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if err := m.checkErr(m.conflictErr); err != nil {
		return "", false, err
	}
	m.conflictCalls = append(m.conflictCalls, prCall{repo: repo, prNumber: prNumber})
	return m.conflictTargetID, m.conflictUpdated, nil
}

func (m *mockPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.mergedCalls = append(m.mergedCalls, prCall{repo: repo, prNumber: prNumber, mergeSHA: mergeSHA})
	return m.mergedTargetID, nil
}

func (m *mockPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.syncCalls = append(m.syncCalls, prCall{repo: repo, prNumber: prNumber, headSHA: headSHA})
	return m.syncTargetID, nil
}

func (m *mockPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	if err := m.checkErr(m.closedErr); err != nil {
		return "", err
	}
	m.closedUnmergedCalls = append(m.closedUnmergedCalls, prCall{repo: repo, prNumber: prNumber})
	return m.closedTargetID, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if err := m.checkErr(m.ciFailedErr); err != nil {
		return "", false, err
	}
	m.ciFailedCalls = append(m.ciFailedCalls, prCall{repo: repo, prNumber: prNumber})
	if len(m.ciFailedResults) > 0 {
		res := m.ciFailedResults[0]
		m.ciFailedResults = m.ciFailedResults[1:]
		return m.ciFailedTargetID, res, nil
	}
	return m.ciFailedTargetID, m.ciFailedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	if err := m.checkErr(m.ciFailedBySHAErr); err != nil {
		return "", 0, false, err
	}
	m.ciFailedBySHACalls = append(m.ciFailedBySHACalls, prCall{repo: repo, headSHA: headSHA})
	return m.ciFailedBySHATarget, m.ciFailedBySHAPRNum, m.ciFailedBySHAUpdated, nil
}

func (m *mockPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.backfillCalls = append(m.backfillCalls, prCall{repo: repo, prNumber: prNumber, mergeSHA: mergeSHA})
	return m.backfillTargetID, nil
}

func (m *mockPRRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	if err := m.checkErr(m.resolveErr); err != nil {
		return 0, "", "", err
	}
	m.resolveBySHACalls = append(m.resolveBySHACalls, prCall{repo: repo, sha: sha})
	targetID := m.resolveTargetID
	if m.resolveBySHATargetEmpty {
		targetID = ""
	}
	return m.resolvePRNum, m.resolveBranch, targetID, nil
}

func (m *mockPRRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	if m == nil {
		return "", "", "", "", errors.New("nil mockPRRegistry")
	}
	if err := m.checkErr(m.resolveErr); err != nil {
		return "", "", "", "", err
	}
	return m.resolveTargetID, m.resolveBranch, m.resolveHeadSHA, "", nil
}

func (m *mockPRRegistry) AppendPRJobs(ctx context.Context, repo string, prNumber int, jobs []string) error {
	m.appendPRJobsCalls = append(m.appendPRJobsCalls, prCall{repo: repo, prNumber: prNumber, jobs: jobs})
	return m.checkErr(m.appendPRJobsErr)
}

func (m *mockPRRegistry) RecordKustomizationRequests(ctx context.Context, repo string, prNumber int, requests map[string]kustomizationRequest) error {
	if err := m.checkErr(m.kustomizationRequestErr); err != nil {
		return err
	}
	m.kustomizationRequestCalls = append(m.kustomizationRequestCalls, requests)
	return nil
}

func (m *mockPRRegistry) MarkKustomizationsDeployed(ctx context.Context, repo string, prNumber int, names []string) (bool, error) {
	m.markKustomizationsCalls = append(m.markKustomizationsCalls, prCall{repo: repo, prNumber: prNumber, jobs: names})
	if err := m.checkErr(m.markKustomizationsErr); err != nil {
		return false, err
	}
	return m.markKustomizationsRes, nil
}

func (m *mockPRRegistry) HasKustomizationRequests(ctx context.Context, repo string, prNumber int) (bool, error) {
	if err := m.checkErr(m.hasKustomizationReqsErr); err != nil {
		return false, err
	}
	return m.hasKustomizationReqs, nil
}

func (m *mockPRRegistry) UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]any) error {
	m.updatePRStatusCalls = append(m.updatePRStatusCalls, prStatusCall{repo: repo, prNumber: prNumber, status: status, metadata: metadata})
	return m.checkErr(m.updatePRStatusErr)
}

type recordCheckStatusCall struct {
	repo       string
	prNumber   int
	headSHA    string
	checkName  string
	checkState string
}

func (m *mockPRRegistry) RecordPRCheckStatus(ctx context.Context, repo string, prNumber int, headSHA, checkName, checkState string) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	m.recordCheckCalls = append(m.recordCheckCalls, recordCheckStatusCall{
		repo:       repo,
		prNumber:   prNumber,
		headSHA:    headSHA,
		checkName:  checkName,
		checkState: checkState,
	})
	if m.recordCheckErr != nil {
		return false, m.recordCheckErr
	}
	return m.recordCheckUpdated, nil
}

func (m *mockPRRegistry) RegisterPR(ctx context.Context, req PRRegisterRequest) (string, error) {
	if m == nil {
		return "", errors.New("nil mockPRRegistry")
	}
	m.registerCalls = append(m.registerCalls, req)
	if err := m.checkErr(m.registerErr); err != nil {
		return "", err
	}
	resolved := firstNonEmpty(m.registerTargetID, req.TargetID)
	if m.resolveTargetID == "" {
		m.resolveTargetID = resolved
	}
	return resolved, nil
}

func (m *mockPRRegistry) EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID string) (string, error) {
	m.ensureCalls = append(m.ensureCalls, prCall{repo: repo, prNumber: prNumber, headSHA: headSHA})
	if err := m.checkErr(m.ensureErr); err != nil {
		return "", err
	}
	return firstNonEmpty(m.ensureTargetID, targetID), nil
}

func (m *mockPRRegistry) RecordExpectedJobs(ctx context.Context, repo string, prNumber int, jobs []string) (bool, error) {
	m.recordExpectedCalls = append(m.recordExpectedCalls, prCall{repo: repo, prNumber: prNumber, jobs: jobs})
	if err := m.checkErr(m.recordExpectedErr); err != nil {
		return false, err
	}
	return m.recordExpectedRes || len(dedupeTrimmedStrings(jobs)) > 0, nil
}

func (m *mockPRRegistry) HasExpectedDeployJobs(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.hasExpectedCalls = append(m.hasExpectedCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.hasExpectedErr); err != nil {
		return false, err
	}
	return m.hasExpectedRes, nil
}

type recordJobStateCall struct {
	jobName  string
	jobState string
}

func (m *mockPRRegistry) RecordPRJobState(ctx context.Context, jobName, jobState string) (string, int, bool, error) {
	if m.err != nil {
		return "", 0, false, m.err
	}
	m.recordJobStateCalls = append(m.recordJobStateCalls, recordJobStateCall{
		jobName:  jobName,
		jobState: jobState,
	})
	if m.recordJobStateErr != nil {
		return "", 0, false, m.recordJobStateErr
	}
	return m.recordJobStateRepo, m.recordJobStatePRNum, m.recordJobStateUpdated, nil
}

func newTestRouter(cfg Config, client *http.Client, reg PRRegistryUpdater, disp OutboundDispatcher, bus ...pubsub.Bus) *RouterServer {
	if client == nil && cfg.GitHubAPIURL == "" && cfg.KubeAPIServer == "" {
		client = &http.Client{Transport: &zeroCallTransport{}}
	}
	srv := NewRouterServer(fixedConfig(cfg), client)
	if reg != nil {
		srv.SetRegistry(reg)
	}
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	if len(bus) > 0 && bus[0] != nil {
		srv.SetEventBus(bus[0])
	}
	return srv
}

func doHTTPRequest(handler http.Handler, method, path string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func mkDeployEvt(event, job, repo, sha string, pr int, targetID, status, details string) DeployEvent {
	return DeployEvent{Event: event, JobName: job, Repo: repo, CommitSHA: sha, PRNumber: pr, TargetID: targetID, Status: status, Details: details}
}

func mkKustomizationEvt(event, job, repo, sha string, pr int, targetID, status string) DeployEvent {
	return DeployEvent{Event: event, SourceKind: "Kustomization", JobName: job, Repo: repo, CommitSHA: sha, PRNumber: pr, TargetID: targetID, Status: status}
}

// --- 4. Deploy Event Processing ---

type fixedConfigSource struct{ cfg Config }

func (f *fixedConfigSource) Current() *Config { return &f.cfg }

func fixedConfig(cfg Config) ConfigSource { return &fixedConfigSource{cfg: cfg} }

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
	wantErr            bool
	wantPR             int
	wantTargetID       string
	wantDeployingCalls int
	wantDeployedJobs   []string
	wantAdvanceCalls   int
	wantPrompts        int
	wantDispatches     int
	wantDMs            int
	wantDMSubstr       string
	wantPromptSubs     []string
	wantRequested      []string
	wantReconciles     []fluxKustomizationRef
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
}

func assertDeployEventRegResults(t *testing.T, tc processDeployEventCase) {
	t.Helper()
	if tc.reg == nil {
		return
	}
	if tc.wantDeployingCalls > 0 && len(tc.reg.deployingCalls) != tc.wantDeployingCalls {
		t.Errorf("deployingCalls = %d, want %d", len(tc.reg.deployingCalls), tc.wantDeployingCalls)
	}
	if len(tc.wantDeployedJobs) > 0 && !reflect.DeepEqual(tc.reg.deployedJobCalls, tc.wantDeployedJobs) {
		t.Errorf("deployedJobCalls = %v, want %v", tc.reg.deployedJobCalls, tc.wantDeployedJobs)
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

func TestHandlePRRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useNilReg  bool
		regErr     error
		body       string
		wantStatus int
	}{
		{
			name:       "nil registry returns 500",
			useNilReg:  true,
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "invalid json returns 400",
			body:       `{malformed`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty repo returns 400",
			body:       `{"repo":"","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero pr_number returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":0,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty branch returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty head_sha returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"","target_id":"100000000000000001"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid target_id returns 400",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"bad"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "headless session key resolves system channel fallback and succeeds",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"dispatch-uuid-1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid request succeeds",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "registry register error returns 500",
			regErr:     errors.New("db insert fail"),
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg PRRegistryUpdater
			if !tc.useNilReg {
				reg = &mockPRRegistry{registerErr: tc.regErr}
			}
			srv := newTestRouter(Config{SystemChannelID: "100000000000000001"}, nil, reg, &mockOutboundDispatcher{})
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/internal/pr/register", strings.NewReader(tc.body), nil)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestSetupPostgres(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		postgresURL string
		wantErr     bool
		errSubstr   string
	}{
		{
			name:        "invalid postgres URL parse error",
			postgresURL: "::not a valid postgres url::",
			wantErr:     true,
			errSubstr:   "parse postgres url",
		},
		{
			name:        "valid postgres url configures runtime search_path",
			postgresURL: "postgres://user:pass@127.0.0.1:5432/testdb",
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := &RouterServer{}
			pool, err := setupPostgres(context.Background(), srv, tc.postgresURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("setupPostgres() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("setupPostgres() err = %v, want substr %q", err, tc.errSubstr)
			}
			if pool != nil {
				pool.Close()
			}
		})
	}
}
