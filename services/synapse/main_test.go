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
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// --- Shared Test Mocks & Helpers ---

type mockRiverInserter struct {
	mu                     sync.Mutex
	insertedJobs           []GitHubWebhookArgs
	insertedDeployJobs     []DeployWebhookArgs
	insertedWidgetSyncJobs []WidgetSyncArgs
	insertedEventJobs      []EventTriggerWebhookArgs
	insertedConflictJobs   []MergeConflictArgs
	insertedDiscordJobs    []DiscordIngressArgs
	insertedInteractJobs   []DiscordInteractionArgs
	insertedOpts           []*river.InsertOpts
	skipDuplicate          bool
	err                    error
}

func (m *mockRiverInserter) Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	m.insertedOpts = append(m.insertedOpts, opts)
	switch a := args.(type) {
	case GitHubWebhookArgs:
		m.insertedJobs = append(m.insertedJobs, a)
	case DeployWebhookArgs:
		m.insertedDeployJobs = append(m.insertedDeployJobs, a)
	case WidgetSyncArgs:
		m.insertedWidgetSyncJobs = append(m.insertedWidgetSyncJobs, a)
	case EventTriggerWebhookArgs:
		m.insertedEventJobs = append(m.insertedEventJobs, a)
	case MergeConflictArgs:
		m.insertedConflictJobs = append(m.insertedConflictJobs, a)
	case DiscordIngressArgs:
		m.insertedDiscordJobs = append(m.insertedDiscordJobs, a)
	case DiscordInteractionArgs:
		m.insertedInteractJobs = append(m.insertedInteractJobs, a)
	}
	return &rivertype.JobInsertResult{
		Job:                      &rivertype.JobRow{ID: 1},
		UniqueSkippedAsDuplicate: m.skipDuplicate,
	}, nil
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

type noopClusterTransport struct{}

func (noopClusterTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"matched":false,"advanced":false,"fired":0,"triggers":[]}`))}, nil
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

	advanceVanadisCalls []prCall
	advanceVanadisRes   *VanadisAdvancementResult
	advanceVanadisErr   error

	suppressVanadisCalls []prCall
	suppressVanadisRes   bool
	suppressVanadisErr   error
	mergeAdvanceRes      bool
	mergeAdvanceErr      error

	failVanadisCalls   []prCall
	failVanadisRes     bool
	failVanadisErr     error
	failVanadisBuildID string
	failVanadisChunkID string

	claimTriggersCalls []claimTriggersCall
	claimTriggersRes   []ClaimedEventTrigger
	claimTriggersErr   error
	checkTriggersRes   bool
	checkTriggersErr   error

	ciFailCount    int
	ciFailCountErr error

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
	return firstNonEmpty(m.deployingTargetID, m.resolveTargetID, "1555405874565091380"), nil
}

func (m *mockPRRegistry) AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	m.deployedJobCalls = append(m.deployedJobCalls, jobName)
	if len(m.deployedResults) > 0 {
		res := m.deployedResults[0]
		m.deployedResults = m.deployedResults[1:]
		if res.err != nil {
			return "", 0, "", "", false, res.err
		}
		return res.targetID, res.prNumber, res.mergeSHA, res.repo, res.updated, nil
	}
	if err := m.checkErr(m.deployedErr); err != nil {
		return "", 0, "", "", false, err
	}
	return m.deployedTargetID, m.deployedPRNum, m.deployedMergeSHA, m.deployedRepo, m.deployedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if err := m.checkErr(m.deployFailedErr); err != nil {
		return "", false, err
	}
	m.deployFailedCalls = append(m.deployFailedCalls, prCall{repo: repo, prNumber: prNumber})
	return m.deployFailedTargetID, m.deployFailedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	if err := m.checkErr(m.deployFailedJobErr); err != nil {
		return "", 0, "", "", false, err
	}
	m.deployFailedJobCalls = append(m.deployFailedJobCalls, jobName)
	return m.deployFailedJobTargetID, m.deployFailedJobPRNum, m.deployFailedJobMergeSHA, m.deployFailedJobRepo, m.deployFailedJobUpdated, nil
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

func (m *mockPRRegistry) AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	m.advanceVanadisCalls = append(m.advanceVanadisCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.advanceVanadisErr); err != nil {
		return nil, err
	}
	if m.advanceVanadisRes != nil {
		return m.advanceVanadisRes, nil
	}
	if m.suppressVanadisRes {
		return &VanadisAdvancementResult{Matched: true, Advanced: false}, nil
	}
	return &VanadisAdvancementResult{}, nil
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

func (m *mockPRRegistry) ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.suppressVanadisCalls = append(m.suppressVanadisCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.suppressVanadisErr); err != nil {
		return false, err
	}
	return m.suppressVanadisRes || (m.advanceVanadisRes != nil && m.advanceVanadisRes.Matched), nil
}

func (m *mockPRRegistry) AdvancesVanadisChunkOnMerge(ctx context.Context, repo string, prNumber int) (bool, error) {
	if err := m.checkErr(m.mergeAdvanceErr); err != nil {
		return false, err
	}
	return m.mergeAdvanceRes, nil
}

func (m *mockPRRegistry) FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.failVanadisCalls = append(m.failVanadisCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.failVanadisErr); err != nil {
		return false, err
	}
	return m.failVanadisRes, nil
}

func (m *mockPRRegistry) FailVanadisChunkDetailsByPR(ctx context.Context, repo string, prNumber int) (string, string, bool, error) {
	failed, err := m.FailVanadisChunkByPR(ctx, repo, prNumber)
	return m.failVanadisBuildID, m.failVanadisChunkID, failed, err
}

type claimTriggersCall struct {
	source        string
	targets       []string
	eventType     string
	contextHeader string
}

func (m *mockPRRegistry) CheckEventTriggers(ctx context.Context, source string, targets []string, eventType string) (bool, error) {
	if err := m.checkErr(m.checkTriggersErr); err != nil {
		return false, err
	}
	return m.checkTriggersRes, nil
}

func (m *mockPRRegistry) FireEventTriggers(ctx context.Context, source string, targets []string, eventType, contextHeader string) ([]ClaimedEventTrigger, error) {
	m.claimTriggersCalls = append(m.claimTriggersCalls, claimTriggersCall{
		source:        source,
		targets:       targets,
		eventType:     eventType,
		contextHeader: contextHeader,
	})
	if err := m.checkErr(m.claimTriggersErr); err != nil {
		return nil, err
	}
	return m.claimTriggersRes, nil
}

func (m *mockPRRegistry) IncrementCIFailCount(ctx context.Context, repo string, prNumber int) (int, error) {
	return m.ciFailCount, m.checkErr(m.ciFailCountErr)
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

func (m *mockPRRegistry) EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID, vanadisChunkID string) (string, error) {
	m.ensureCalls = append(m.ensureCalls, prCall{repo: repo, prNumber: prNumber, headSHA: headSHA, sha: vanadisChunkID})
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

func newTestRouter(cfg Config, client *http.Client, reg PRRegistryUpdater, disp OutboundDispatcher, riverInserter ...RiverInserter) *RouterServer {
	if client == nil && cfg.GitHubAPIURL == "" && cfg.KubeAPIServer == "" {
		client = &http.Client{Transport: &zeroCallTransport{}}
	}
	srv := NewRouterServer(fixedConfig(cfg), client, riverInserter...)
	if reg != nil {
		if pgReg, ok := reg.(*PostgresPRRegistry); ok && pgReg != nil {
			if pgReg.vanadisURL == "http://vanadis.aerial" && pgReg.haroURL == "http://haro.aerial" {
				pgReg.httpClient = &http.Client{Transport: noopClusterTransport{}}
				pgReg.vanadisRetryDelay = 0
			}
		}
		srv.SetRegistry(reg)
	}
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	if len(riverInserter) > 0 && riverInserter[0] != nil {
		srv.SetRiverClient(riverInserter[0])
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
	return NewPostgresPRRegistry(&mockVanadisPgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT status, COALESCE(metadata, '{}'::jsonb)") {
				return mockVanadisPgxRow{scanFn: func(dest ...any) error {
					return scanBuildChecksRow(dest, status, checksJSON)
				}}
			}
			for _, fn := range extraRowFns {
				if row := fn(sql); row != nil {
					return row
				}
			}
			return mockVanadisPgxRow{}
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
			evt:          mkDeployEvt("deploy_started", "brain", "azylman/aerial", "sha1234", 527, "1555405874565091380", "started", ""),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       527,
			wantTargetID: "1555405874565091380",
		},
		{
			name:         "deploy_success valid snowflake",
			evt:          mkDeployEvt("deploy_success", "brain", "azylman/aerial", "sha3d2a", 528, "1555405874565091380", "success", ""),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       528,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job brain",
				"Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely.",
			},
		},
		{
			name:           "deploy_rollback valid snowflake",
			evt:            mkDeployEvt("deploy_rollback", "webhooks-router", "azylman/aerial", "shafail", 529, "1555405874565091380", "rollback", strings.Repeat("error detail line\n", 50)),
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         529,
			wantTargetID:   "1555405874565091380",
			wantDispatches: 1,
			wantPromptSubs: []string{
				"Deployment failed and rolled back",
				"Please investigate and fix",
				"```",
			},
		},
		{
			name:           "deploy_failed valid snowflake",
			evt:            mkDeployEvt("deploy_failed", "hangar", "azylman/aerial", "shabroken", 530, "1555405874565091380", "failed", "exit status 1"),
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         530,
			wantTargetID:   "1555405874565091380",
			wantDispatches: 1,
			wantPromptSubs: []string{
				"Deployment failed for job hangar",
				"Please investigate and fix",
			},
		},
		{
			name:    "deploy_failed prompt error reverts state and returns error",
			evt:     mkDeployEvt("deploy_failed", "eri", "azylman/aerial", "shabroken", 531, "1555405874565091380", "failed", "exit status 1"),
			reg:     &mockPRRegistry{deployFailedUpdated: true},
			disp:    &mockOutboundDispatcher{dispatchErr: errors.New("eri 502 bad gateway")},
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
			wantDispatches: 1,
			wantPromptSubs: []string{"Deployment failed and rolled back"},
		},
	}
}

func processDeployEventSyncCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name: "sync_success without PR number",
			evt:  mkDeployEvt("sync_success", "", "azylman/aerial-config", "sha999", 0, "1555405874565091380", "", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{},
		},
		{
			name:         "sync_failed with details",
			evt:          mkDeployEvt("sync_failed", "", "azylman/aerial-config", "sha888", 0, "1555405874565091380", "", "merge conflict in rules/persona"),
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
			reg:              &mockPRRegistry{resolvePRNum: 241, resolveTargetID: "1555405874565091380"},
			disp:             &mockOutboundDispatcher{},
			wantPR:           241,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
			wantPrompts:      1,
			wantPromptSubs:   []string{"Continuous Delivery deployment completed for job git-sync on azylman/aerial-config"},
		},
		{
			name:         "deploy_success on aerial-config",
			evt:          mkDeployEvt("deploy_success", "homepage", "azylman/aerial-config", "sha_cfg_succ", 242, "1555405874565091380", "success", ""),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       242,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job homepage on azylman/aerial-config",
			},
		},
		{
			name:           "sync_failed with PR number",
			evt:            mkDeployEvt("sync_failed", "", "azylman/aerial", "merge542", 542, "1555405874565091380", "failed", "merge conflict in rules/foo.md"),
			reg:            &mockPRRegistry{},
			disp:           &mockOutboundDispatcher{},
			wantPR:         542,
			wantTargetID:   "1555405874565091380",
			wantDispatches: 1,
			wantPromptSubs: []string{
				"Git sync failed for repo azylman/aerial",
				"merge conflict in rules/foo.md",
				"Please investigate and fix the git sync failure.",
			},
		},
		{
			name:    "deploy_started widget error propagation",
			evt:     mkDeployEvt("deploy_started", "brain", "azylman/aerial", "sha_start_test", 603, "1555405874565091380", "started", ""),
			reg:     &mockPRRegistry{resolveTargetID: "1555405874565091380"},
			disp:    &mockOutboundDispatcher{widgetErr: errors.New("brain offline")},
			wantErr: true,
		},
		{
			name:             "sync_success advances Vanadis with valid targetID",
			evt:              mkDeployEvt("sync_success", "", "azylman/aerial-config", "sha_vanadis_1", 301, "1555405874565091380", "success", ""),
			reg:              &mockPRRegistry{suppressVanadisRes: true},
			disp:             &mockOutboundDispatcher{},
			wantPR:           301,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
		},
		{
			name:             "sync_success advances Vanadis with empty targetID",
			evt:              mkDeployEvt("sync_success", "", "azylman/aerial-config", "sha_vanadis_2", 302, "", "success", ""),
			reg:              &mockPRRegistry{},
			wantPR:           302,
			wantAdvanceCalls: 1,
		},
		{
			name:    "sync_success Vanadis advance error propagates",
			evt:     mkDeployEvt("sync_success", "", "azylman/aerial-config", "sha_vanadis_3", 303, "1555405874565091380", "success", ""),
			reg:     &mockPRRegistry{advanceVanadisErr: errors.New("vanadis err")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
		{
			name:               "deploy_started skipped when job already deployed",
			evt:                mkDeployEvt("deploy_started", "coven", "azylman/aerial", "bc1d3ef", 971, "1555405874565091380", "started", ""),
			reg:                &mockPRRegistry{skipDeployingTransition: true},
			disp:               &mockOutboundDispatcher{},
			wantPR:             971,
			wantTargetID:       "1555405874565091380",
			wantDeployingCalls: 1,
		},
		{
			name: "deploy_started with expected_jobs records expected sibling jobs before transition",
			evt: DeployEvent{
				Event: "deploy_started", JobName: "db-migrate", ExpectedJobs: []string{"coven", "db-migrate"},
				Repo: "azylman/aerial", CommitSHA: "bc1d3ef", PRNumber: 972, TargetID: "1555405874565091380", Status: "started",
			},
			reg:                &mockPRRegistry{recordExpectedErr: errors.New("record expected warn")},
			disp:               &mockOutboundDispatcher{},
			wantPR:             972,
			wantTargetID:       "1555405874565091380",
			wantDeployingCalls: 1,
		},
	}
}

func processDeployEventVanadisActionCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name: "sync_success with expected_jobs advances Vanadis when next chunk is a pr",
			evt: DeployEvent{
				Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"coven", "db-migrate"},
				Repo: "azylman/aerial", CommitSHA: "sha_sync_exp", PRNumber: 973, TargetID: "1555405874565091380", Status: "success",
			},
			reg:              &mockPRRegistry{mergeAdvanceRes: true},
			disp:             &mockOutboundDispatcher{},
			wantPR:           973,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
		},
		{
			name:             "sync_success with registry expected jobs and empty targetID advances Vanadis when next chunk is a pr",
			evt:              mkDeployEvt("sync_success", "git-sync", "azylman/aerial", "sha_sync_reg_exp", 974, "", "success", ""),
			reg:              &mockPRRegistry{recordExpectedRes: true, mergeAdvanceRes: true},
			wantPR:           974,
			wantAdvanceCalls: 1,
		},
		{
			name: "sync_success with expected_jobs defers Vanadis advancement when next chunk is an action", evt: DeployEvent{Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"eri"}, Repo: "azylman/aerial", CommitSHA: "sha_sync_action_next", PRNumber: 981, TargetID: "1555405874565091380", Status: "success"},
			reg: &mockPRRegistry{advanceVanadisRes: &VanadisAdvancementResult{Matched: true, Advanced: true, BuildID: "build-action-next", ThreadID: "1555405874565091380", NextChunkIndex: 1}}, disp: &mockOutboundDispatcher{}, wantPR: 981, wantTargetID: "1555405874565091380",
		},
		{
			name: "deploy_success advances Vanadis when next chunk is an action", evt: mkDeployEvt("deploy_success", "eri", "azylman/aerial", "sha_deploy_action_next", 982, "1555405874565091380", "success", ""),
			reg: &mockPRRegistry{suppressVanadisRes: true, advanceVanadisRes: &VanadisAdvancementResult{Matched: true, Advanced: true, BuildID: "build-action-next", ThreadID: "1555405874565091380", NextChunkIndex: 1}}, disp: &mockOutboundDispatcher{}, wantPR: 982, wantTargetID: "1555405874565091380", wantAdvanceCalls: 1,
		},
		{
			name:             "sync_success tolerates RecordExpectedJobs error and advances when no expected jobs",
			evt:              mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha_sync_err", 975, "1555405874565091380", "success", ""),
			reg:              &mockPRRegistry{recordExpectedErr: errors.New("record expected db error")},
			disp:             &mockOutboundDispatcher{},
			wantPR:           975,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
			wantPrompts:      1,
			wantPromptSubs:   []string{"Continuous Delivery deployment completed for job git-sync on azylman/aerial-config"},
		},
		{
			name: "sync_success with expected_jobs on final Vanadis chunk defers advancement until deploy_success",
			evt:  DeployEvent{Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"eri"}, Repo: "azylman/aerial", CommitSHA: "sha_final_defer", PRNumber: 976, TargetID: "1555405874565091380", Status: "success"},
			reg: &mockPRRegistry{
				advanceVanadisRes: &VanadisAdvancementResult{Matched: true, Advanced: true, BuildID: "build-final", ThreadID: "1555405874565091380", BuildCompleted: true},
			},
			disp:             &mockOutboundDispatcher{},
			wantPR:           976,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 0,
			wantPrompts:      0,
		},
		{
			name: "sync_success with expected_jobs and empty targetID on final Vanadis chunk defers advancement",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial", "sha_final_empty_tid", 977, "", "success", ""),
			reg: &mockPRRegistry{
				recordExpectedRes: true,
			},
			wantPR:           977,
			wantAdvanceCalls: 0,
		},
	}
}

func processDeployEventVanadisChunkCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name: "sync_success without expected_jobs on final Vanadis chunk advances and dispatches build completed prompt",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha_final_sync_only", 978, "1555405874565091380", "success", ""),
			reg: &mockPRRegistry{
				advanceVanadisRes: &VanadisAdvancementResult{Matched: true, Advanced: true, BuildID: "build-skill-sync", ThreadID: "1555405874565091380", BuildCompleted: true},
			},
			disp:             &mockOutboundDispatcher{},
			wantPR:           978,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
			wantPrompts:      1,
			wantPromptSubs:   []string{"Vanadis build build-skill-sync completed"},
		},
		{
			name: "deploy_success on final Vanadis chunk suppresses deploy prompt and dispatches build completed prompt",
			evt:  mkDeployEvt("deploy_success", "eri", "azylman/aerial", "sha_final_deploy", 979, "1555405874565091380", "success", ""),
			reg: &mockPRRegistry{
				suppressVanadisRes: true,
				advanceVanadisRes:  &VanadisAdvancementResult{Matched: true, Advanced: true, BuildID: "build-final-deploy", ThreadID: "1555405874565091380", BuildCompleted: true},
			},
			disp:             &mockOutboundDispatcher{},
			wantPR:           979,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
			wantPrompts:      1,
			wantPromptSubs:   []string{"Vanadis build build-final-deploy completed"},
		},
		{
			name: "sync_success with expected_jobs AdvancesVanadisChunkOnMerge error propagates",
			evt: DeployEvent{
				Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"eri"},
				Repo: "azylman/aerial", CommitSHA: "sha_final_err", PRNumber: 980, TargetID: "1555405874565091380", Status: "success",
			},
			reg: &mockPRRegistry{
				mergeAdvanceErr: errors.New("vanadis merge gate err"),
			},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
		{
			name: "duplicate deploy_success for same PR suppressed",
			evt:  mkDeployEvt("deploy_success", "brain", "azylman/aerial", "sha-dup-pr", 890, "1555405874565091380", "success", ""),
			extraEvts: []DeployEvent{
				mkDeployEvt("deploy_success", "brain", "azylman/aerial", "sha-dup-pr", 890, "1555405874565091380", "success", ""),
			},
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       890,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job brain",
			},
		},
		{
			name: "duplicate deploy_success for same repo job and commitSHA suppressed",
			evt:  mkDeployEvt("deploy_success", "cadvisor", "azylman/aerial", "sha-dup-sha", 0, "1555405874565091380", "success", ""),
			extraEvts: []DeployEvent{
				mkDeployEvt("deploy_success", "cadvisor", "azylman/aerial", "sha-dup-sha", 0, "1555405874565091380", "success", ""),
			},
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job cadvisor",
			},
		},
		{
			name: "deploy_success following sync_success without expected_jobs is suppressed",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-sync-no-exp", 891, "1555405874565091380", "success", ""),
			extraEvts: []DeployEvent{
				mkDeployEvt("deploy_success", "homepage", "azylman/aerial-config", "sha-sync-no-exp", 891, "1555405874565091380", "success", ""),
			},
			reg:              &mockPRRegistry{},
			disp:             &mockOutboundDispatcher{},
			wantPR:           891,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
			wantPrompts:      1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job git-sync on azylman/aerial-config",
			},
		},
		{
			name: "deploy_success suppressed when postgres pr_registry already has status deployed",
			evt:  mkDeployEvt("deploy_success", "brain", "azylman/aerial", "sha-db-deployed", 892, "1555405874565091380", "success", ""),
			pgReg: NewPostgresPRRegistry(&mockVanadisPgxPool{
				queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
					if strings.Contains(sql, "SELECT status FROM pr_registry") {
						return mockVanadisPgxRow{scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if s, ok := dest[0].(*string); ok {
									*s = "deployed"
								}
							}
							return nil
						}}
					}
					return mockVanadisPgxRow{}
				},
			}),
			disp:         &mockOutboundDispatcher{},
			wantPR:       892,
			wantTargetID: "1555405874565091380",
			wantPrompts:  0,
		},
	}
}

func processDeployEventVanadisCases() []processDeployEventCase {
	return append(processDeployEventVanadisActionCases(), processDeployEventVanadisChunkCases()...)
}

func processDeployEventFluxWatcherCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name:         "deploy_success on aerial-core with build checks in metadata and merged status handles sync success",
			evt:          mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "sha-aerial-sync", 893, "1555405874565091380", "success"),
			pgReg:        newMockBuildChecksPgRegistry("merged", `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`),
			disp:         &mockOutboundDispatcher{},
			wantPR:       893,
			wantTargetID: "1555405874565091380",
			wantPrompts:  0,
		},
		{
			name: "deploy_started on aerial-core skipped when waiting for image build",
			evt:  mkKustomizationEvt("deploy_started", "aerial-core", "azylman/aerial", "sha-aerial-started", 89894, "1555405874565091380", "started"),
			pgReg: newMockBuildChecksPgRegistry("merged", `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`,
				func(sql string) pgx.Row {
					if strings.Contains(sql, "SET status = 'deploying'") || strings.Contains(sql, "SELECT target_id, title, status") {
						return mockVanadisPgxRow{scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if s, ok := dest[0].(*string); ok {
									*s = "1555405874565091380"
								}
							}
							return nil
						}}
					}
					return nil
				},
			),
			disp:         &mockOutboundDispatcher{widgetErr: errors.New("unexpected widget sync for skipped deploy_started")},
			wantPR:       89894,
			wantTargetID: "1555405874565091380",
			wantPrompts:  0,
		},
		{
			name:         "sync_success on git-sync with ci_checks build jobs stays in synced without firing deploy prompt",
			evt:          mkDeployEvt("sync_success", "git-sync", "azylman/aerial", "sha-aerial-sync-pending", 89895, "1555405874565091380", "success", ""),
			pgReg:        newMockBuildChecksPgRegistry("merged", `{"ci_checks":{"Build & Push (aerial-synapse)":"passed"}}`),
			disp:         &mockOutboundDispatcher{},
			wantPR:       89895,
			wantTargetID: "1555405874565091380",
			wantPrompts:  0,
		},
		{
			name:         "Kustomization deploy_success never completes a watcher-tracked PR",
			evt:          mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "sha-pr-1485", 1485, "1555405874565091380", "success"),
			reg:          &mockPRRegistry{hasKustomizationReqs: true, resolveTargetID: "1555405874565091380"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1485,
			wantTargetID: "1555405874565091380",
		},
		{
			name:         "CI job deploy_success never completes a watcher-tracked PR",
			evt:          mkDeployEvt("deploy_success", "eri", "azylman/aerial", "sha-pr-1485", 1485, "1555405874565091380", "success", ""),
			reg:          &mockPRRegistry{hasKustomizationReqs: true, resolveTargetID: "1555405874565091380"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1485,
			wantTargetID: "1555405874565091380",
		},
		{
			name:         "Kustomization deploy_failed alert defers to the watcher for a tracked PR",
			evt:          mkKustomizationEvt("deploy_failed", "aerial-core", "azylman/aerial", "sha-pr-1485", 1485, "1555405874565091380", "failed"),
			reg:          &mockPRRegistry{hasKustomizationReqs: true, deployFailedUpdated: true, resolveTargetID: "1555405874565091380"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1485,
			wantTargetID: "1555405874565091380",
		},
		{
			name:         "untracked PR keeps legacy Kustomization deploy_success completion",
			evt:          mkKustomizationEvt("deploy_success", "aerial-core", "azylman/aerial", "sha-legacy", 1300, "1555405874565091380", "success"),
			reg:          &mockPRRegistry{resolveTargetID: "1555405874565091380"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1300,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job aerial-core on azylman/aerial (PR #1300, commit: sha-legacy).",
			},
		},
		{
			name:         "tracked-PR lookup error falls back to legacy completion",
			evt:          mkDeployEvt("deploy_success", "eri", "azylman/aerial", "sha-legacy", 1301, "1555405874565091380", "success", ""),
			reg:          &mockPRRegistry{hasKustomizationReqsErr: errors.New("db down"), resolveTargetID: "1555405874565091380"},
			disp:         &mockOutboundDispatcher{},
			wantPR:       1301,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
		},
	}
}

func processDeployEventFluxDiscoveryCases() []processDeployEventCase {
	return []processDeployEventCase{
		{
			name: "sync_success on a Flux-sourced repo defers completion to the kustomization watcher",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-config-only", 1640, "1555405874565091380", "success", ""),
			reg:  &mockPRRegistry{suppressVanadisRes: true},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}, {Namespace: "flux-system", Name: "aerial-config-apps"}},
			}},
			wantPR:             1640,
			wantTargetID:       "1555405874565091380",
			wantDeployingCalls: 1,
			wantRequested:      []string{"aerial-config", "aerial-config-apps"},
			wantReconciles:     []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-config"}, {Namespace: "flux-system", Name: "aerial-config-apps"}},
		},
		{
			name: "sync_success with empty targetID on a Flux-sourced repo defers instead of advancing Vanadis",
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
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/mirrormere", "sha-no-flux", 1642, "1555405874565091380", "success", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}},
			}},
			wantPR:           1642,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
			wantPrompts:      1,
			wantPromptSubs:   []string{"Continuous Delivery deployment completed for job git-sync on azylman/mirrormere (PR #1642, commit: sha-no-flux)."},
		},
		{
			name:         "sync_success with image build pending stays synced without discovering kustomizations",
			evt:          DeployEvent{Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"eri"}, Repo: "azylman/aerial", CommitSHA: "sha-image-pending", PRNumber: 1643, TargetID: "1555405874565091380", Status: "success"},
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{repoDiscoverErr: errors.New("unexpected discovery while images are pending")},
			wantPR:       1643,
			wantTargetID: "1555405874565091380",
		},
		{
			name:    "sync_success kustomization discovery error propagates",
			evt:     mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-discover-err", 1644, "1555405874565091380", "success", ""),
			reg:     &mockPRRegistry{},
			disp:    &mockOutboundDispatcher{repoDiscoverErr: errors.New("kube api down")},
			wantErr: true,
		},
		{
			name: "sync_success kustomization request record error propagates",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-record-err", 1645, "1555405874565091380", "success", ""),
			reg:  &mockPRRegistry{kustomizationRequestErr: errors.New("db down")},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}},
			}},
			wantErr: true,
		},
		{
			name: "sync_success deploying transition error propagates",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-deploying-err", 1646, "1555405874565091380", "success", ""),
			reg:  &mockPRRegistry{deployingErr: errors.New("db down")},
			disp: &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{
				"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}},
			}},
			wantErr: true,
		},
		{
			name: "sync_success reconcile patch error propagates after requests are recorded",
			evt:  mkDeployEvt("sync_success", "git-sync", "azylman/aerial-config", "sha-patch-err", 1647, "1555405874565091380", "success", ""),
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
	sync := processDeployEventSyncCases()
	vanadis := processDeployEventVanadisCases()
	flux := processDeployEventFluxCases()
	all := make([]processDeployEventCase, 0, len(basic)+len(sync)+len(vanadis)+len(flux))
	all = append(all, basic...)
	all = append(all, sync...)
	all = append(all, vanadis...)
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
	if len(tc.reg.advanceVanadisCalls) != tc.wantAdvanceCalls {
		t.Errorf("advanceVanadisCalls = %d, want %d", len(tc.reg.advanceVanadisCalls), tc.wantAdvanceCalls)
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
