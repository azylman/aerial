package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/bwmarrin/discordgo"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// --- Shared Test Mocks & Helpers ---

type mockRiverInserter struct {
	mu                     sync.Mutex
	insertedJobs           []GitHubWebhookArgs
	insertedHangarJobs     []HangarWebhookArgs
	insertedNomadJobs      []NomadEventArgs
	insertedCrashAlertJobs []NomadCrashAlertArgs
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
	case HangarWebhookArgs:
		m.insertedHangarJobs = append(m.insertedHangarJobs, a)
	case NomadEventArgs:
		m.insertedNomadJobs = append(m.insertedNomadJobs, a)
	case NomadCrashAlertArgs:
		m.insertedCrashAlertJobs = append(m.insertedCrashAlertJobs, a)
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
	metadata map[string]interface{}
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

	listOpenPRsCalls []string
	openPRs          []RegisteredPR
	listOpenPRsErr   error
	closedErr        error

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
	finalVanadisCalls    []prCall
	finalVanadisRes      bool
	finalVanadisErr      error

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
	if err := m.checkErr(m.deployedErr); err != nil {
		return "", 0, "", "", false, err
	}
	m.deployedJobCalls = append(m.deployedJobCalls, jobName)
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

func (m *mockPRRegistry) ListOpenPRs(ctx context.Context, repo string) ([]RegisteredPR, error) {
	if err := m.checkErr(m.listOpenPRsErr); err != nil {
		return nil, err
	}
	m.listOpenPRsCalls = append(m.listOpenPRsCalls, repo)
	return m.openPRs, nil
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

func (m *mockPRRegistry) UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]interface{}) error {
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

func (m *mockPRRegistry) IsFinalVanadisChunk(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.finalVanadisCalls = append(m.finalVanadisCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.finalVanadisErr); err != nil {
		return false, err
	}
	return m.finalVanadisRes || (m.advanceVanadisRes != nil && m.advanceVanadisRes.Matched && m.advanceVanadisRes.BuildCompleted), nil
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

type mockOutboundDispatcher struct {
	mu                      sync.Mutex
	gitPushCalls            []GitPushEventRequest
	imageReadyCalls         []ImageReadyEventRequest
	promptCalls             []PromptRequest
	dispatchCalls           []DispatchRequest
	directMessageCalls      []DirectMessageRequest
	discordMessageCalls     []DiscordMessageIngressRequest
	discordInteractionCalls []*discordgo.Interaction
	gitPushErr              error
	imageReadyErr           error
	promptErr               error
	dispatchErr             error
	directMessageErr        error
	discordMessageErr       error
	discordMessageFailCount int
	discordMessageAttempts  int
	discordInteractionErr   error
	widgetErr               error
	widgetReturnMsgID       string
	widgetCalls             []StatusWidgetRequest
	matchedJobs             map[string][]string
}

func (m *mockOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	m.mu.Lock()
	m.gitPushCalls = append(m.gitPushCalls, req)
	m.mu.Unlock()
	return m.gitPushErr
}

func (m *mockOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error) {
	m.mu.Lock()
	m.imageReadyCalls = append(m.imageReadyCalls, req)
	var jobs []string
	if m.matchedJobs != nil {
		jobs = m.matchedJobs[req.Image]
	}
	m.mu.Unlock()
	return jobs, m.imageReadyErr
}

func (m *mockOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	m.mu.Lock()
	m.promptCalls = append(m.promptCalls, req)
	m.mu.Unlock()
	return m.promptErr
}

func (m *mockOutboundDispatcher) DispatchBackground(ctx context.Context, req DispatchRequest) error {
	m.mu.Lock()
	m.dispatchCalls = append(m.dispatchCalls, req)
	m.mu.Unlock()
	return m.dispatchErr
}

func (m *mockOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	m.mu.Lock()
	m.directMessageCalls = append(m.directMessageCalls, req)
	m.mu.Unlock()
	return m.directMessageErr
}

func (m *mockOutboundDispatcher) GitPushCalls() []GitPushEventRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]GitPushEventRequest(nil), m.gitPushCalls...)
}

func (m *mockOutboundDispatcher) ImageReadyCalls() []ImageReadyEventRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ImageReadyEventRequest(nil), m.imageReadyCalls...)
}

func (m *mockOutboundDispatcher) PromptCalls() []PromptRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]PromptRequest(nil), m.promptCalls...)
}

func (m *mockOutboundDispatcher) DispatchCalls() []DispatchRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DispatchRequest(nil), m.dispatchCalls...)
}

func (m *mockOutboundDispatcher) DirectMessageCalls() []DirectMessageRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DirectMessageRequest(nil), m.directMessageCalls...)
}

func (m *mockOutboundDispatcher) DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error) {
	m.mu.Lock()
	m.widgetCalls = append(m.widgetCalls, payload)
	msgID := m.widgetReturnMsgID
	err := m.widgetErr
	m.mu.Unlock()
	return msgID, err
}

func (m *mockOutboundDispatcher) WidgetCalls() []StatusWidgetRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]StatusWidgetRequest(nil), m.widgetCalls...)
}

func (m *mockOutboundDispatcher) DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error {
	m.mu.Lock()
	m.discordMessageCalls = append(m.discordMessageCalls, req)
	m.discordMessageAttempts++
	if m.discordMessageFailCount > 0 {
		if m.discordMessageAttempts <= m.discordMessageFailCount {
			err := m.discordMessageErr
			if err == nil {
				err = errors.New("transient error")
			}
			m.mu.Unlock()
			return err
		}
		m.mu.Unlock()
		return nil
	}
	err := m.discordMessageErr
	m.mu.Unlock()
	return err
}

func (m *mockOutboundDispatcher) DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error {
	m.mu.Lock()
	m.discordInteractionCalls = append(m.discordInteractionCalls, interaction)
	m.mu.Unlock()
	return m.discordInteractionErr
}

func (m *mockOutboundDispatcher) DiscordMessageCalls() []DiscordMessageIngressRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DiscordMessageIngressRequest(nil), m.discordMessageCalls...)
}

func (m *mockOutboundDispatcher) DiscordInteractionCalls() []*discordgo.Interaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*discordgo.Interaction(nil), m.discordInteractionCalls...)
}

func newTestRouter(cfg Config, client *http.Client, reg PRRegistryUpdater, disp OutboundDispatcher, riverInserter ...RiverInserter) *RouterServer {
	srv := NewRouterServer(cfg, client, riverInserter...)
	if reg != nil {
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

func mkHangarEvt(event, job, repo, sha string, pr int, targetID, status, details string) HangarDeployEvent {
	return HangarDeployEvent{Event: event, JobName: job, Repo: repo, CommitSHA: sha, PRNumber: pr, TargetID: targetID, Status: status, Details: details}
}

func mkDeployedReg(targetID string, pr int, sha, repo string, updated bool) *mockPRRegistry {
	return &mockPRRegistry{resolveTargetID: targetID, deployedTargetID: targetID, deployedPRNum: pr, deployedMergeSHA: sha, deployedRepo: repo, deployedUpdated: updated}
}

func mkDeployFailedReg(targetID string, pr int, sha, repo string, updated bool) *mockPRRegistry {
	return &mockPRRegistry{deployFailedJobTargetID: targetID, deployFailedJobPRNum: pr, deployFailedJobMergeSHA: sha, deployFailedJobRepo: repo, deployFailedJobUpdated: updated}
}

// --- 4. Hangar Webhook HTTP Handler & Event Processing ---

func TestHandleHangarWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, path, wantJobName, wantTargetID, wantMetricSubstr string
		body                                                    io.Reader
		reg                                                     *mockPRRegistry
		river                                                   *mockRiverInserter
		wantStatus, wantPR, wantResolveSHACalls, wantRiverJobs  int
	}{
		{name: "malformed JSON api path", path: "/api/webhooks/hangar", body: strings.NewReader(`{invalid json`), wantStatus: http.StatusBadRequest},
		{name: "malformed JSON legacy path", path: "/webhooks/hangar", body: strings.NewReader(`{invalid json`), wantStatus: http.StatusBadRequest},
		{name: "read error in body", path: "/api/webhooks/hangar", body: errReader{}, wantStatus: http.StatusBadRequest},
		{name: "missing event field", path: "/api/webhooks/hangar", body: strings.NewReader(`{"job_name":"brain"}`), wantStatus: http.StatusBadRequest},
		{name: "missing job_name for deploy event", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_started"}`), wantStatus: http.StatusBadRequest},
		{name: "empty object payload", path: "/api/webhooks/hangar", body: strings.NewReader(`{}`), wantStatus: http.StatusBadRequest},
		{name: "empty job_name deploy_success", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"","repo":"azylman/aerial","status":"success"}`), wantStatus: http.StatusBadRequest},
		{name: "sync_success empty job_name defaults to git-sync", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"sync_success","job_name":"","repo":"azylman/aerial","commit_sha":"abc1234","status":"success"}`), wantStatus: http.StatusOK, wantJobName: "git-sync"},
		{name: "sync_failed empty job_name defaults to git-sync", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"sync_failed","job_name":"","repo":"azylman/aerial","commit_sha":"abc1234","status":"success"}`), wantStatus: http.StatusOK, wantJobName: "git-sync"},
		{name: "valid deploy_started api path", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_started","job_name":"brain","repo":"azylman/aerial","pr_number":420,"target_id":"thread-999","status":"started","details":"Nomad job evaluation placed"}`), wantStatus: http.StatusOK, wantJobName: "brain", wantPR: 420, wantTargetID: "thread-999"},
		{name: "valid deploy_started legacy path", path: "/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_started","job_name":"brain","repo":"azylman/aerial","pr_number":420,"target_id":"thread-999","status":"started","details":"Nomad job evaluation placed"}`), wantStatus: http.StatusOK, wantJobName: "brain", wantPR: 420, wantTargetID: "thread-999"},
		{name: "valid deploy_success", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"brain","repo":"azylman/aerial","pr_number":420,"target_id":"thread-999","status":"success","details":"Healthy deployment completed"}`), wantStatus: http.StatusOK, wantJobName: "brain", wantPR: 420, wantTargetID: "thread-999"},
		{name: "valid deploy_failed", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_failed","job_name":"brain","repo":"azylman/aerial","pr_number":420,"target_id":"thread-999","status":"failed","details":"Allocation failed healthcheck"}`), wantStatus: http.StatusOK, wantJobName: "brain", wantPR: 420, wantTargetID: "thread-999"},
		{name: "valid deploy_rollback", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_rollback","job_name":"brain","repo":"azylman/aerial","pr_number":420,"target_id":"thread-999","status":"rollback","details":"Reverted to previous healthy release"}`), wantStatus: http.StatusOK, wantJobName: "brain", wantPR: 420, wantTargetID: "thread-999"},
		{name: "resolve PR and TargetID by SHA", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"webhooks-router","repo":"azylman/aerial","commit_sha":"deploycommit123","status":"success","details":"Deployed successfully"}`), reg: &mockPRRegistry{resolvePRNum: 789, resolveTargetID: "thread-resolved-789"}, wantStatus: http.StatusOK, wantPR: 789, wantTargetID: "thread-resolved-789", wantResolveSHACalls: 1},
		{name: "resolve PR by SHA preserves existing target_id", path: "/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_rollback","job_name":"webhooks-router","repo":"azylman/aerial","commit_sha":"deploycommit123","target_id":"existing-target-snowflake","status":"rollback"}`), reg: &mockPRRegistry{resolvePRNum: 789, resolveTargetID: "thread-resolved-789"}, wantStatus: http.StatusOK, wantPR: 789, wantTargetID: "existing-target-snowflake", wantResolveSHACalls: 1},
		{name: "provided PR number skips resolve PR by SHA", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_started","job_name":"brain","repo":"azylman/aerial","commit_sha":"deploycommit123","pr_number":123,"status":"started"}`), reg: &mockPRRegistry{resolvePRNum: 789, resolveTargetID: "thread-resolved-789"}, wantStatus: http.StatusOK, wantPR: 123, wantResolveSHACalls: 0},
		{name: "registry resolve error still accepts event", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"webhooks-router","repo":"azylman/aerial","commit_sha":"deploycommit123","status":"success"}`), reg: &mockPRRegistry{resolveErr: errors.New("db error")}, wantStatus: http.StatusOK},
		{name: "nil registry still accepts event", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"webhooks-router","repo":"azylman/aerial","commit_sha":"deploycommit123","status":"success"}`), reg: nil, wantStatus: http.StatusOK},
		{name: "river accepts on api path", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"brain","repo":"azylman/aerial","pr_number":558,"target_id":"1555405874565091380","status":"success","details":"Deployment healthy"}`), river: &mockRiverInserter{}, wantStatus: http.StatusAccepted, wantJobName: "brain", wantRiverJobs: 1},
		{name: "river accepts on legacy path", path: "/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"brain","repo":"azylman/aerial","pr_number":558,"target_id":"1555405874565091380","status":"success","details":"Deployment healthy"}`), river: &mockRiverInserter{}, wantStatus: http.StatusAccepted, wantJobName: "brain", wantRiverJobs: 1},
		{name: "river insertion error", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"brain","repo":"azylman/aerial","pr_number":558,"target_id":"1555405874565091380","status":"success","details":"Deployment healthy"}`), river: &mockRiverInserter{err: errors.New("simulated river insert error")}, wantStatus: http.StatusInternalServerError},
		{name: "river duplicate skipped increments metric", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"brain","commit_sha":"abc1234"}`), river: &mockRiverInserter{skipDuplicate: true}, wantStatus: http.StatusAccepted, wantMetricSubstr: `synapse_unique_skipped_total{kind="hangar_webhook"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var inserters []RiverInserter
			if tc.river != nil {
				inserters = append(inserters, tc.river)
			}
			var reg PRRegistryUpdater
			if tc.reg != nil {
				reg = tc.reg
			}
			srv := newTestRouter(Config{}, nil, reg, nil, inserters...)
			routes := srv.Routes()

			rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, nil)
			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.wantStatus, rec.Code, rec.Body.String())
			}
			if rec.Code == http.StatusBadRequest && tc.body != nil {
				var errResp map[string]string
				_ = json.Unmarshal(rec.Body.Bytes(), &errResp)
			}
			if rec.Code == http.StatusOK || rec.Code == http.StatusAccepted {
				var resp map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err == nil {
					if tc.wantJobName != "" && resp["job_name"] != tc.wantJobName {
						t.Errorf("job_name = %v, want %s", resp["job_name"], tc.wantJobName)
					}
					if tc.wantTargetID != "" && resp["target_id"] != tc.wantTargetID {
						t.Errorf("target_id = %v, want %s", resp["target_id"], tc.wantTargetID)
					}
					if tc.wantPR > 0 {
						if prVal, ok := resp["pr_number"].(float64); !ok || int(prVal) != tc.wantPR {
							t.Errorf("pr_number = %v, want %d", resp["pr_number"], tc.wantPR)
						}
					}
				}
			}
			if tc.reg != nil && tc.wantResolveSHACalls > 0 {
				if len(tc.reg.resolveBySHACalls) != tc.wantResolveSHACalls {
					t.Errorf("resolveBySHACalls = %d, want %d", len(tc.reg.resolveBySHACalls), tc.wantResolveSHACalls)
				}
			}
			if tc.river != nil && tc.wantRiverJobs > 0 {
				if len(tc.river.insertedHangarJobs) != tc.wantRiverJobs {
					t.Errorf("insertedHangarJobs = %d, want %d", len(tc.river.insertedHangarJobs), tc.wantRiverJobs)
				}
				if len(tc.river.insertedOpts) > 0 {
					opts := tc.river.insertedOpts[0]
					if opts.MaxAttempts != 5 || !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
						t.Errorf("unexpected insert opts: %+v", opts)
					}
				}
			}
			if tc.wantMetricSubstr != "" {
				recM := doHTTPRequest(routes, http.MethodGet, "/metrics", nil, nil)
				if !strings.Contains(recM.Body.String(), tc.wantMetricSubstr) {
					t.Errorf("expected /metrics to include %q, got:\n%s", tc.wantMetricSubstr, recM.Body.String())
				}
			}
		})
	}
}

func TestProcessHangarEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name               string
		evt                HangarDeployEvent
		sysChan            string
		nomadJobJSON       string
		reg                *mockPRRegistry
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
	}{
		{
			name: "deploy_started ignored without PR number or targetID",
			evt:  mkHangarEvt("deploy_started", "brain", "azylman/aerial", "sha-test", 0, "", "started", ""),
		},
		{
			name: "deploy_success ignored without PR number or targetID",
			evt:  mkHangarEvt("deploy_success", "brain", "azylman/aerial", "sha-test", 0, "", "success", ""),
		},
		{
			name: "deploy_failed ignored without PR number or targetID",
			evt:  mkHangarEvt("deploy_failed", "brain", "azylman/aerial", "sha-test", 0, "", "failed", ""),
		},
		{
			name: "deploy_rollback ignored without PR number or targetID",
			evt:  mkHangarEvt("deploy_rollback", "brain", "azylman/aerial", "sha-test", 0, "", "rollback", ""),
		},
		{
			name:               "deploy_started resolves SHA and transitions deploying",
			evt:                mkHangarEvt("deploy_started", "webhooks-router", "azylman/aerial", "sha111", 0, "", "started", ""),
			reg:                &mockPRRegistry{resolvePRNum: 99, resolveTargetID: "target-99"},
			wantPR:             99,
			wantTargetID:       "target-99",
			wantDeployingCalls: 1,
		},
		{
			name:         "deploy_started tolerates registry error",
			evt:          mkHangarEvt("deploy_started", "webhooks-router", "azylman/aerial", "sha111", 0, "", "started", ""),
			reg:          &mockPRRegistry{resolvePRNum: 99, resolveTargetID: "target-99", deployingErr: errors.New("db error")},
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_success transitions state",
			evt:          mkHangarEvt("deploy_success", "webhooks-router", "azylman/aerial", "", 99, "target-99", "success", "deployment completed"),
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_rollback transitions state",
			evt:          mkHangarEvt("deploy_rollback", "webhooks-router", "azylman/aerial", "", 99, "target-99", "rollback", "rolled back"),
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_failed transitions state",
			evt:          mkHangarEvt("deploy_failed", "webhooks-router", "azylman/aerial", "", 99, "target-99", "failed", "deploy unhealthy"),
			wantPR:       99,
			wantTargetID: "target-99",
		},
		{
			name:         "deploy_started valid snowflake",
			evt:          mkHangarEvt("deploy_started", "brain", "azylman/aerial", "sha1234", 527, "1555405874565091380", "started", ""),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       527,
			wantTargetID: "1555405874565091380",
		},
		{
			name:         "deploy_success valid snowflake",
			evt:          mkHangarEvt("deploy_success", "brain", "azylman/aerial", "sha3d2a", 528, "1555405874565091380", "success", ""),
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
			evt:            mkHangarEvt("deploy_rollback", "webhooks-router", "azylman/aerial", "shafail", 529, "1555405874565091380", "rollback", strings.Repeat("error detail line\n", 50)),
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         529,
			wantTargetID:   "1555405874565091380",
			wantDispatches: 1,
			wantPromptSubs: []string{
				"Deployment failed and rolled back",
				"Please investigate and fix",
				"```",
				"thread_id: 1555405874565091380",
			},
		},
		{
			name:           "deploy_failed valid snowflake",
			evt:            mkHangarEvt("deploy_failed", "hangar", "azylman/aerial", "shabroken", 530, "1555405874565091380", "failed", "exit status 1"),
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         530,
			wantTargetID:   "1555405874565091380",
			wantDispatches: 1,
			wantPromptSubs: []string{
				"Deployment failed for job hangar",
				"Please investigate and fix",
				"thread_id: 1555405874565091380",
			},
		},
		{
			name:    "deploy_failed prompt error reverts state and returns error",
			evt:     mkHangarEvt("deploy_failed", "eri", "azylman/aerial", "shabroken", 531, "1555405874565091380", "failed", "exit status 1"),
			reg:     &mockPRRegistry{deployFailedUpdated: true},
			disp:    &mockOutboundDispatcher{dispatchErr: errors.New("eri 502 bad gateway")},
			wantErr: true,
		},
		{
			name:           "deploy_rollback without targetID falls back to system_channel name",
			evt:            mkHangarEvt("deploy_rollback", "eri", "azylman/aerial", "shafail", 532, "", "rollback", "dotenv parse error"),
			sysChan:        "aerial-dev",
			reg:            &mockPRRegistry{deployFailedUpdated: true},
			disp:           &mockOutboundDispatcher{},
			wantPR:         532,
			wantTargetID:   "aerial-dev",
			wantDispatches: 1,
			wantPromptSubs: []string{"Deployment failed and rolled back"},
		},
		{
			name: "sync_success without PR number",
			evt:  mkHangarEvt("sync_success", "", "azylman/aerial-config", "sha999", 0, "1555405874565091380", "", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{},
		},
		{
			name:         "sync_failed with details",
			evt:          mkHangarEvt("sync_failed", "", "azylman/aerial-config", "sha888", 0, "1555405874565091380", "", "merge conflict in rules/persona"),
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
			evt:  mkHangarEvt("deploy_success", "hangar", "", "", 0, "not-a-snowflake", "", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{},
		},
		{
			name: "deploy_started invalid snowflake ignored",
			evt:  mkHangarEvt("deploy_started", "hangar", "", "", 0, "not-a-snowflake", "", ""),
			reg:  &mockPRRegistry{},
			disp: &mockOutboundDispatcher{},
		},
		{
			name:             "sync_success late resolution via ResolvePRBySHA",
			evt:              mkHangarEvt("sync_success", "", "azylman/aerial-config", "dccc646186d7bf7d17efa6cac6f730d17f66125b", 0, "", "", ""),
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
			evt:          mkHangarEvt("deploy_success", "homepage", "azylman/aerial-config", "sha_cfg_succ", 242, "1555405874565091380", "success", ""),
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
			evt:            mkHangarEvt("sync_failed", "", "azylman/aerial", "merge542", 542, "1555405874565091380", "failed", "merge conflict in rules/foo.md"),
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
			evt:     mkHangarEvt("deploy_started", "brain", "azylman/aerial", "sha_start_test", 603, "1555405874565091380", "started", ""),
			reg:     &mockPRRegistry{resolveTargetID: "1555405874565091380"},
			disp:    &mockOutboundDispatcher{widgetErr: errors.New("brain offline")},
			wantErr: true,
		},
		{
			name:             "sync_success advances Vanadis with valid targetID",
			evt:              mkHangarEvt("sync_success", "", "azylman/aerial-config", "sha_vanadis_1", 301, "1555405874565091380", "success", ""),
			reg:              &mockPRRegistry{suppressVanadisRes: true},
			disp:             &mockOutboundDispatcher{},
			wantPR:           301,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
		},
		{
			name:             "sync_success advances Vanadis with empty targetID",
			evt:              mkHangarEvt("sync_success", "", "azylman/aerial-config", "sha_vanadis_2", 302, "", "success", ""),
			reg:              &mockPRRegistry{},
			wantPR:           302,
			wantAdvanceCalls: 1,
		},
		{
			name:    "sync_success Vanadis advance error propagates",
			evt:     mkHangarEvt("sync_success", "", "azylman/aerial-config", "sha_vanadis_3", 303, "1555405874565091380", "success", ""),
			reg:     &mockPRRegistry{advanceVanadisErr: errors.New("vanadis err")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
		{
			name:               "deploy_started periodic batch job transitions to deployed immediately",
			evt:                mkHangarEvt("deploy_started", "coverage-ingest", "azylman/aerial", "1fc4aaf", 851, "1555405874565091380", "started", ""),
			nomadJobJSON:       `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`,
			reg:                mkDeployedReg("1555405874565091380", 851, "1fc4aaf", "azylman/aerial", true),
			disp:               &mockOutboundDispatcher{},
			wantPR:             851,
			wantTargetID:       "1555405874565091380",
			wantDeployingCalls: 1,
			wantDeployedJobs:   []string{"coverage-ingest"},
			wantPrompts:        1,
			wantPromptSubs:     []string{"Continuous Delivery deployment completed for job coverage-ingest on azylman/aerial (PR #851, commit: 1fc4aaf, ref: batch-registered)"},
		},
		{
			name:               "deploy_started periodic batch job tolerates HandleNomadJobSuccess error",
			evt:                mkHangarEvt("deploy_started", "coverage-ingest", "azylman/aerial", "1fc4aaf", 851, "1555405874565091380", "started", ""),
			nomadJobJSON:       `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`,
			reg:                &mockPRRegistry{deployedErr: errors.New("db transition err")},
			disp:               &mockOutboundDispatcher{},
			wantPR:             851,
			wantTargetID:       "1555405874565091380",
			wantDeployingCalls: 1,
		},
		{
			name:               "deploy_started skipped when job already deployed",
			evt:                mkHangarEvt("deploy_started", "coven", "azylman/aerial", "bc1d3ef", 971, "1555405874565091380", "started", ""),
			reg:                &mockPRRegistry{skipDeployingTransition: true},
			disp:               &mockOutboundDispatcher{},
			wantPR:             971,
			wantTargetID:       "1555405874565091380",
			wantDeployingCalls: 1,
		},
		{
			name: "deploy_started with expected_jobs records expected sibling jobs before transition",
			evt: HangarDeployEvent{
				Event: "deploy_started", JobName: "db-migrate", ExpectedJobs: []string{"coven", "db-migrate"},
				Repo: "azylman/aerial", CommitSHA: "bc1d3ef", PRNumber: 972, TargetID: "1555405874565091380", Status: "started",
			},
			reg:                &mockPRRegistry{recordExpectedErr: errors.New("record expected warn")},
			disp:               &mockOutboundDispatcher{},
			wantPR:             972,
			wantTargetID:       "1555405874565091380",
			wantDeployingCalls: 1,
		},
		{
			name: "sync_success with expected_jobs advances Vanadis while suppressing merged widget",
			evt: HangarDeployEvent{
				Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"coven", "db-migrate"},
				Repo: "azylman/aerial", CommitSHA: "sha_sync_exp", PRNumber: 973, TargetID: "1555405874565091380", Status: "success",
			},
			reg:              &mockPRRegistry{},
			disp:             &mockOutboundDispatcher{},
			wantPR:           973,
			wantTargetID:     "1555405874565091380",
			wantAdvanceCalls: 1,
		},
		{
			name:             "sync_success with registry expected jobs and empty targetID advances Vanadis",
			evt:              mkHangarEvt("sync_success", "git-sync", "azylman/aerial", "sha_sync_reg_exp", 974, "", "success", ""),
			reg:              &mockPRRegistry{recordExpectedRes: true},
			wantPR:           974,
			wantAdvanceCalls: 1,
		},
		{
			name:             "sync_success tolerates RecordExpectedJobs error and advances when no expected jobs",
			evt:              mkHangarEvt("sync_success", "git-sync", "azylman/aerial-config", "sha_sync_err", 975, "1555405874565091380", "success", ""),
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
			evt: HangarDeployEvent{
				Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"eri"},
				Repo: "azylman/aerial", CommitSHA: "sha_final_defer", PRNumber: 976, TargetID: "1555405874565091380", Status: "success",
			},
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
			evt:  mkHangarEvt("sync_success", "git-sync", "azylman/aerial", "sha_final_empty_tid", 977, "", "success", ""),
			reg: &mockPRRegistry{
				recordExpectedRes: true,
				finalVanadisRes:   true,
			},
			wantPR:           977,
			wantAdvanceCalls: 0,
		},
		{
			name: "sync_success without expected_jobs on final Vanadis chunk advances and dispatches build completed prompt",
			evt:  mkHangarEvt("sync_success", "git-sync", "azylman/aerial-config", "sha_final_sync_only", 978, "1555405874565091380", "success", ""),
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
			evt:  mkHangarEvt("deploy_success", "eri", "azylman/aerial", "sha_final_deploy", 979, "1555405874565091380", "success", ""),
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
			name: "sync_success with expected_jobs IsFinalVanadisChunk error propagates",
			evt: HangarDeployEvent{
				Event: "sync_success", JobName: "git-sync", ExpectedJobs: []string{"eri"},
				Repo: "azylman/aerial", CommitSHA: "sha_final_err", PRNumber: 980, TargetID: "1555405874565091380", Status: "success",
			},
			reg: &mockPRRegistry{
				finalVanadisErr: errors.New("vanadis final check err"),
			},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := tc.reg
			if reg == nil {
				reg = &mockPRRegistry{}
			}
			var disp OutboundDispatcher
			if tc.disp != nil {
				disp = tc.disp
			}
			cfg := Config{SystemChannelID: tc.sysChan}
			client := &http.Client{Transport: &zeroCallTransport{}}
			if tc.nomadJobJSON != "" {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(tc.nomadJobJSON))
				}))
				defer ts.Close()
				cfg.NomadAddr = ts.URL
				client = ts.Client()
			}
			srv := newTestRouter(cfg, client, reg, disp)
			res, err := srv.ProcessHangarEvent(ctx, tc.evt)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessHangarEvent() err = %v, wantErr %v", err, tc.wantErr)
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
			if tc.reg != nil {
				if tc.wantDeployingCalls > 0 && len(tc.reg.deployingCalls) != tc.wantDeployingCalls {
					t.Errorf("deployingCalls = %d, want %d", len(tc.reg.deployingCalls), tc.wantDeployingCalls)
				}
				if len(tc.wantDeployedJobs) > 0 && !reflect.DeepEqual(tc.reg.deployedJobCalls, tc.wantDeployedJobs) {
					t.Errorf("deployedJobCalls = %v, want %v", tc.reg.deployedJobCalls, tc.wantDeployedJobs)
				}
				if len(tc.reg.advanceVanadisCalls) != tc.wantAdvanceCalls {
					t.Errorf("advanceVanadisCalls = %d, want %d", len(tc.reg.advanceVanadisCalls), tc.wantAdvanceCalls)
				}
			}
			if tc.disp != nil {
				dms := tc.disp.DirectMessageCalls()
				if len(dms) != tc.wantDMs {
					t.Errorf("DMs = %d, want %d", len(dms), tc.wantDMs)
				}
				if tc.wantDMSubstr != "" {
					if len(dms) == 0 || !strings.Contains(dms[len(dms)-1].Content, tc.wantDMSubstr) {
						t.Errorf("DM = %v, want substr %q", dms, tc.wantDMSubstr)
					}
				}
				dispatches := tc.disp.DispatchCalls()
				if len(dispatches) != tc.wantDispatches {
					t.Errorf("dispatches = %d, want %d", len(dispatches), tc.wantDispatches)
				}
				for _, sub := range tc.wantPromptSubs {
					if tc.wantDispatches > 0 {
						if len(dispatches) == 0 || !strings.Contains(dispatches[len(dispatches)-1].Prompt, sub) {
							t.Errorf("dispatch = %v, want substr %q", dispatches, sub)
						}
					}
				}
				prompts := tc.disp.PromptCalls()
				if len(prompts) != tc.wantPrompts {
					t.Errorf("prompts = %d, want %d", len(prompts), tc.wantPrompts)
				}
				for _, sub := range tc.wantPromptSubs {
					if tc.wantPrompts > 0 {
						if len(prompts) == 0 || !strings.Contains(prompts[len(prompts)-1].Prompt, sub) {
							t.Errorf("prompt = %v, want substr %q", prompts, sub)
						}
					}
				}
			}
		})
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

	tests := []struct {
		name                string
		isDeploymentSuccess bool
		useNilRegistry      bool
		prNumber            int
		targetID            string
		suppressVanadis     bool
		suppressVanadisErr  error
		finalVanadis        bool
		advanceVanadisRes   *VanadisAdvancementResult
		wantTargetID        string
		wantErr             bool
		wantPrompts         int
		wantPromptSubs      []string
		wantDMs             int
		wantWidgets         int
		widgetErr           error
	}{
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
			finalVanadis:        true,
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
			var mockReg PRRegistryUpdater
			if !tc.useNilRegistry {
				mockReg = &mockPRRegistry{
					resolveTargetID:    tc.targetID,
					suppressVanadisRes: tc.suppressVanadis,
					suppressVanadisErr: tc.suppressVanadisErr,
					finalVanadisRes:    tc.finalVanadis,
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
		})
	}

	// PR 0 with mock dispatcher
	srv := newTestRouter(Config{}, nil, nil, &mockOutboundDispatcher{})
	if err := srv.dispatchConflictPrompt(ctx, validSnowflake, "azylman/aerial", "feat/x", "sha1", 0); err != nil {
		t.Errorf("dispatchConflictPrompt(pr=0): %v", err)
	}

	// PostgresPRRegistry syncs widget in conflict prompt and CI failure prompt
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
	if !strings.Contains(pgDispatches[0].Prompt, "Merge conflict detected on PR #42") || !strings.Contains(pgDispatches[0].Prompt, "thread_id: "+validSnowflake) || pgDispatches[0].Source != "webhook" || pgDispatches[0].Effort != "medium" {
		t.Errorf("expected silent medium-effort conflict dispatch with thread_id for standalone PR, got %+v", pgDispatches[0])
	}
	if !strings.Contains(pgDispatches[1].Prompt, "Please investigate and fix the failure by inspecting the failing logs") || !strings.Contains(pgDispatches[1].Prompt, "thread_id: "+validSnowflake) || pgDispatches[1].Source != "webhook" || pgDispatches[1].Effort != "medium" {
		t.Errorf("expected silent medium-effort standard CI dispatch with thread_id, got %+v", pgDispatches[1])
	}
	if !strings.Contains(pgDispatches[2].Prompt, "override-roborev 42") || !strings.Contains(pgDispatches[2].Prompt, "thread_id: "+validSnowflake) || pgDispatches[2].Source != "webhook" || pgDispatches[2].Effort != "medium" {
		t.Errorf("expected silent medium-effort roborev override dispatch with thread_id, got %+v", pgDispatches[2])
	}

	// dispatchAlertWithPrompt with DM error and silent prefixed targetID deduplication
	dispAlert := &mockOutboundDispatcher{directMessageErr: errors.New("dm failed")}
	srvAlert := newTestRouter(Config{}, nil, nil, dispAlert)
	if err := srvAlert.dispatchAlertWithPrompt(ctx, validSnowflake, "alert", "dm text", "prompt text"); err != nil {
		t.Errorf("dispatchAlertWithPrompt with dm err: %v", err)
	}
	if err := srvAlert.dispatchAlertWithPrompt(ctx, "stateful_medium:discord-"+validSnowflake, "silent_alert", "", "prompt text\nthread_id: "+validSnowflake, vanadisPromptOpts{silent: true, chunkID: "c-alert"}); err != nil {
		t.Errorf("dispatchAlertWithPrompt silent prefixed: %v", err)
	}
	if got := dispAlert.DispatchCalls(); len(got) != 1 || got[0].SessionID != "vanadis-c-alert" || strings.Count(got[0].Prompt, "thread_id: "+validSnowflake) != 1 {
		t.Errorf("expected single deduplicated thread_id and session_id in silent dispatch, got %+v", got)
	}
	// invalid chunkID should not populate SessionID
	if err := srvAlert.dispatchAlertWithPrompt(ctx, "stateful_medium:discord-"+validSnowflake, "silent_alert", "", "prompt text", vanadisPromptOpts{silent: true, chunkID: "bad:chunk:id"}); err != nil {
		t.Errorf("dispatchAlertWithPrompt bad chunk id: %v", err)
	}
	if got := dispAlert.DispatchCalls(); len(got) != 2 || got[1].SessionID != "" {
		t.Errorf("expected empty SessionID on invalid chunk id, got %+v", got)
	}

	// Vanadis silent auto-fix prompts suppress DM and set Silent: true with Vanadis effort/chunk routing
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
	if len(vpgDispatches) != 1 || vpgDispatches[0].Source != "webhook" || vpgDispatches[0].SessionID != "vanadis-chunk-885" || vpgDispatches[0].Effort != "low" || !strings.Contains(vpgDispatches[0].Prompt, "Chunk ID: chunk-885") {
		t.Errorf("expected silent low-effort dispatch with chunk-885, got %+v", vpgDispatches)
	}
}

// --- 8. River Workers, Crash Alerts & Server Endpoints ---

func TestCrashAlerts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	args := NomadCrashAlertArgs{
		JobID:      "openobserve",
		AllocID:    "alloc-oom",
		Status:     "failed",
		StatusDesc: "OOMKilled exit code 137",
	}

	tests := []struct {
		name        string
		channel     string
		disp        *mockOutboundDispatcher
		wantErr     bool
		wantDMs     int
		wantPrompts int
	}{
		{
			name:        "valid system channel dispatches DM and prompt",
			channel:     "1555405874565091380",
			disp:        &mockOutboundDispatcher{},
			wantDMs:     1,
			wantPrompts: 1,
		},
		{
			name:        "named system channel dispatches DM and prompt",
			channel:     "aerial-dev",
			disp:        &mockOutboundDispatcher{},
			wantDMs:     1,
			wantPrompts: 1,
		},
		{
			name:    "invalid channel is clean no-op",
			channel: "invalid channel!",
			disp:    &mockOutboundDispatcher{},
		},
		{
			name:    "nil dispatcher is clean no-op",
			channel: "1555405874565091380",
			disp:    nil,
		},
		{
			name:        "4xx on prompt is ignored",
			channel:     "1555405874565091380",
			disp:        &mockOutboundDispatcher{promptErr: errors.New("brain prompt returned status 404: channel not found")},
			wantDMs:     1,
			wantPrompts: 1,
		},
		{
			name:        "4xx on DM is ignored",
			channel:     "1555405874565091380",
			disp:        &mockOutboundDispatcher{directMessageErr: errors.New("brain internal message returned status 400: bad request")},
			wantDMs:     1,
			wantPrompts: 0,
		},
		{
			name:        "5xx on prompt returns error",
			channel:     "1555405874565091380",
			disp:        &mockOutboundDispatcher{promptErr: errors.New("brain prompt returned status 500: internal server error")},
			wantErr:     true,
			wantDMs:     1,
			wantPrompts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var disp OutboundDispatcher
			if tc.disp != nil {
				disp = tc.disp
			}
			s := newTestRouter(Config{SystemChannelID: tc.channel}, nil, nil, disp)
			err := s.ProcessCrashAlert(ctx, args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessCrashAlert() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.disp != nil {
				if len(tc.disp.DirectMessageCalls()) != tc.wantDMs {
					t.Errorf("got %d DMs, want %d", len(tc.disp.DirectMessageCalls()), tc.wantDMs)
				}
				if len(tc.disp.PromptCalls()) != tc.wantPrompts {
					t.Errorf("got %d prompts, want %d", len(tc.disp.PromptCalls()), tc.wantPrompts)
				}
			}
		})
	}
}

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

			srv := NewRouterServer(Config{Port: "4020"}, nil)
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

	done := make(chan struct{})
	runBackgroundSync("test_panic", func(ctx context.Context) error {
		defer close(done)
		panic("simulated panic")
	})
	<-done
}

func TestRunServerLifecycle(t *testing.T) {
	tests := []struct {
		name         string
		config       Config
		preCancel    bool
		isRunApp     bool
		wantErr      bool
		wantGraceful bool
	}{
		{
			name:         "runServer start and graceful cancel shutdown",
			config:       Config{Port: "0"},
			wantGraceful: true,
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
			if tc.wantGraceful {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ready := make(chan string, 1)
				done := make(chan error, 1)

				go func() {
					done <- runServer(ctx, tc.config, func(addr string) {
						ready <- addr
					})
				}()

				select {
				case addr := <-ready:
					if addr == "" {
						t.Errorf("expected non-empty listening address")
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("timeout waiting for server to start")
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
				return
			}

			if tc.isRunApp {
				ctxC, cancelC := context.WithCancel(context.Background())
				if tc.preCancel {
					cancelC()
				} else {
					defer cancelC()
				}
				err := runApp(ctxC, tc.config)
				if (err != nil) != tc.wantErr && !errors.Is(err, http.ErrServerClosed) {
					t.Errorf("unexpected error from runApp: %v", err)
				}
				return
			}

			err := runServer(context.Background(), tc.config, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("runServer() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// --- 9. Vanadis Build Queue Advancement & Wake Suppression Tests ---

func TestAdvanceVanadisChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validThread := "123456789012345678"

	tests := []struct {
		name           string
		useNilRegistry bool
		prNumber       int
		advanceRes     *VanadisAdvancementResult
		advanceErr     error
		dispDirectErr  error
		dispWidgetErr  error
		wantErr        bool
		wantPrompts    int
		wantPromptSubs []string
		wantDMs        int
	}{
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
			wantErr:        false,
			wantPrompts:    1,
			wantPromptSubs: []string{"Vanadis build build-done completed"},
			wantDMs:        0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			}
			srv := newTestRouter(Config{}, nil, reg, disp)

			err := srv.advanceVanadisChunk(ctx, "azylman/aerial", tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("advanceVanadisChunk() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.useNilRegistry || tc.prNumber <= 0 {
				finalChunk, err := srv.isFinalVanadisChunk(ctx, "azylman/aerial", tc.prNumber)
				if err != nil || finalChunk {
					t.Errorf("expected isFinalVanadisChunk false for guard case")
				}
				vanadisPR, err := srv.isVanadisPR(ctx, "azylman/aerial", tc.prNumber)
				if err != nil || vanadisPR {
					t.Errorf("expected isVanadisPR false for guard case")
				}
				if err := srv.advanceVanadisChunkOnSync(ctx, "azylman/aerial", tc.prNumber, true); err != nil {
					t.Errorf("expected advanceVanadisChunkOnSync nil error for guard case, got %v", err)
				}
			}
			if len(disp.PromptCalls()) != tc.wantPrompts {
				t.Errorf("got %d prompts, want %d", len(disp.PromptCalls()), tc.wantPrompts)
			}
			for _, sub := range tc.wantPromptSubs {
				if len(disp.PromptCalls()) == 0 || !strings.Contains(disp.PromptCalls()[0].Prompt, sub) {
					t.Errorf("prompt missing %q: %+v", sub, disp.PromptCalls())
				}
			}
			if len(disp.DirectMessageCalls()) != tc.wantDMs {
				t.Errorf("got %d DMs, want %d", len(disp.DirectMessageCalls()), tc.wantDMs)
			}
		})
	}
}

func TestSyncPRWidget_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	prStart := time.Date(2026, 10, 16, 17, 30, 0, 0, time.UTC)
	prUpdate := prStart.Add(4 * time.Minute)
	chunk0Start := time.Date(2026, 10, 16, 17, 0, 0, 0, time.UTC)
	chunk0Update := chunk0Start.Add(5 * time.Minute)

	tests := []struct {
		name            string
		useNilServer    bool
		useMockReg      bool
		useInserter     bool
		inserterErr     error
		mockTargetID    string
		mockBranch      string
		prDBStatus      string
		prDBBanner      string
		prDBCIChecks    string
		prDBMetadata    string
		buildDBStatus   string
		chunk1DBStatus  string
		repo            string
		prNumber        int
		olderPRs        []int
		vanadisMatch    bool
		dispErr         error
		queryErr        error
		prQueryErr      error
		execErr         error
		wantErr         bool
		wantDispatched  bool
		wantQueued      int
		wantWidgetID    string
		wantStatus      string
		wantBanner      string
		wantActiveStep  string
		wantSkipSink    bool
		wantVanadisStep string
		wantChannel     string
		wantChunks      int
	}{
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
			mockTargetID: "1555405874565091380", mockBranch: "feat/queued", repo: "aerial", prNumber: 101, wantQueued: 1,
		},
		{
			name: "riverClient insert error returns error", useMockReg: true, useInserter: true,
			inserterErr: errors.New("db insert failure"), repo: "azylman/aerial", prNumber: 101, wantErr: true,
		},
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := &mockOutboundDispatcher{widgetReturnMsgID: "msg-widget-101", widgetErr: tc.dispErr}

			if tc.useNilServer {
				var nilSrv *RouterServer
				nilSrv.setPRWidgetState(ctx, tc.repo, tc.prNumber, "conflict", "banner")
				if err := nilSrv.SyncPRWidget(ctx, tc.repo, tc.prNumber); err != nil {
					t.Fatalf("SyncPRWidget nil server err = %v", err)
				}
				if err := nilSrv.ProcessWidgetSync(ctx, WidgetSyncArgs{Repo: tc.repo, PRNumber: tc.prNumber}); err != nil {
					t.Fatalf("ProcessWidgetSync nil server err = %v", err)
				}
				return
			}

			if tc.useMockReg {
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
				if tc.wantQueued > 0 && (inserter == nil || len(inserter.insertedWidgetSyncJobs) != tc.wantQueued) {
					t.Fatalf("expected %d queued widget sync jobs, got %+v", tc.wantQueued, inserter)
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
				return
			}

			pool := &mockVanadisPgxPool{
				execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
					return pgconn.NewCommandTag("UPDATE 1"), tc.execErr
				},
				queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
					if strings.Contains(sql, "FROM vanadis_chunks") {
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
					if strings.Contains(sql, "FROM pr_registry") {
						if tc.prQueryErr != nil {
							return &mockVanadisPgxRow{scanFn: func(dest ...any) error {
								*dest[0].(*string) = "1555405874565091380"
								return tc.prQueryErr
							}}
						}
						return &mockVanadisPgxRow{scanFn: func(dest ...any) error {
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
							if len(dest) > 7 {
								ciChecks := tc.prDBCIChecks
								if ciChecks == "" {
									ciChecks = "{}"
								}
								*dest[7].(*string) = ciChecks
							}
							if len(dest) > 8 {
								meta := tc.prDBMetadata
								if meta == "" {
									meta = "{}"
								}
								*dest[8].(*string) = meta
							}
							return nil
						}}
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

			var receivedVanadisActiveStep string
			tsVanadis := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				receivedVanadisActiveStep = req.ActiveStep
				if tc.vanadisMatch && req.PRNumber == tc.prNumber {
					_ = json.NewEncoder(w).Encode(map[string]any{"matched": true, "build_id": "build-abc"})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"matched": false})
			}))
			t.Cleanup(tsVanadis.Close)

			srv := newTestRouter(Config{VanadisURL: tsVanadis.URL}, tsVanadis.Client(), NewPostgresPRRegistry(pool, WithVanadisClient(tsVanadis.URL, tsVanadis.Client())), mockDisp)
			if tc.prDBBanner != "" {
				srv.setPRWidgetState(ctx, tc.repo, tc.prNumber, tc.prDBStatus, tc.prDBBanner)
			}
			err := srv.SyncPRWidget(ctx, tc.repo, tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncPRWidget err = %v, wantErr = %v", err, tc.wantErr)
			}
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
			}
			if tc.wantVanadisStep != "" && receivedVanadisActiveStep != tc.wantVanadisStep {
				t.Errorf("got receivedVanadisActiveStep = %q, want %q", receivedVanadisActiveStep, tc.wantVanadisStep)
			}
		})
	}
}

func TestRouterServer_HandlePRRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
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
	}{
		{
			name:        "NilRegistry",
			nilRegistry: true,
			body:        `{"repo":"azylman/aerial","pr_number":101,"branch":"feat/test","head_sha":"sha123","target_id":"1555405874565091380"}`,
			wantStatus:  http.StatusInternalServerError,
			wantSubstr:  "registry not initialized",
		},
		{
			name:       "InvalidJSON",
			body:       `{not-valid-json`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "invalid json payload",
		},
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

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
			if tc.wantWidgetCall && disp != nil {
				calls := disp.WidgetCalls()
				if len(calls) != 1 {
					t.Errorf("WidgetCalls = %d, want 1", len(calls))
				}
			}
		})
	}
}

func TestSyncBuildWidget_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
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
	}{
		{
			name:         "nil server returns clean nil",
			useNilServer: true,
			buildID:      "build-123",
		},
		{
			name:    "empty buildID returns clean nil",
			buildID: "",
		},
		{
			name:        "riverClient configured queues WidgetSyncArgs without inline HTTP dispatch",
			useMockReg:  true,
			useInserter: true,
			buildID:     "build-101",
			banner:      "Build running",
			wantQueued:  1,
		},
		{
			name:        "riverClient insert error returns error",
			useMockReg:  true,
			useInserter: true,
			inserterErr: errors.New("db insert failure"),
			buildID:     "build-101",
			wantErr:     true,
		},
		{
			name:       "non-Postgres registry fallback returns clean nil",
			useMockReg: true,
			buildID:    "build-102",
			wantErr:    false,
		},
		{
			name:          "Postgres registry calls vanadis build widget sync successfully",
			buildID:       "build-103",
			banner:        "Chunk 1 running",
			vanadisStatus: http.StatusOK,
			wantErr:       false,
		},
		{
			name:            "Postgres registry retries transient 502 then 200 succeeds",
			buildID:         "build-103-retry",
			banner:          "Chunk 1 retried",
			vanadisStatuses: []int{http.StatusBadGateway, http.StatusOK},
			wantErr:         false,
		},
		{
			name:          "Postgres registry returns error when vanadis fails",
			buildID:       "build-104",
			vanadisStatus: http.StatusInternalServerError,
			wantErr:       true,
		},
		{
			name:          "Postgres registry non-transient 400 error",
			buildID:       "build-106",
			vanadisStatus: http.StatusBadRequest,
			wantErr:       true,
		},
		{
			name:          "Postgres registry invalid json decode error",
			buildID:       "build-105",
			vanadisStatus: http.StatusOK,
			vanadisBody:   `{invalid-json`,
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.useNilServer {
				var nilSrv *RouterServer
				if err := nilSrv.SyncBuildWidget(ctx, tc.buildID, tc.banner); err != nil {
					t.Fatalf("SyncBuildWidget nil server err = %v, want nil", err)
				}
				return
			}

			var reqIdx int
			tsVanadis := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			t.Cleanup(tsVanadis.Close)

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

			err := srv.SyncBuildWidget(ctx, tc.buildID, tc.banner)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncBuildWidget err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantQueued > 0 {
				if inserter == nil || len(inserter.insertedWidgetSyncJobs) != tc.wantQueued {
					t.Fatalf("expected %d queued widget sync jobs, got %+v", tc.wantQueued, inserter)
				}
				lastJob := inserter.insertedWidgetSyncJobs[len(inserter.insertedWidgetSyncJobs)-1]
				if lastJob.BuildID != tc.buildID || lastJob.Banner != tc.banner {
					t.Errorf("queued job mismatch: got %+v, want BuildID=%q Banner=%q", lastJob, tc.buildID, tc.banner)
				}
			}
		})
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
		{
			name:       "InvalidJSON",
			body:       `{not-valid-json`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "invalid json payload",
		},
		{
			name:       "MissingAllFieldsReturns400",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "build_id or repo and pr_number are required",
		},
		{
			name:       "EmptyBuildIDAndEmptyRepoReturns400",
			body:       `{"build_id":"","repo":"","pr_number":0}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "build_id or repo and pr_number are required",
		},
		{
			name:       "RepoWithoutPRNumberReturns400",
			body:       `{"repo":"azylman/aerial","pr_number":0}`,
			wantStatus: http.StatusBadRequest,
			wantSubstr: "build_id or repo and pr_number are required",
		},
		{
			name:        "ValidBuildIDWithRiver",
			body:        `{"build_id":"build-777","banner":"Building chunk 0"}`,
			useInserter: true,
			wantStatus:  http.StatusAccepted,
			wantSubstr:  `"status":"accepted"`,
			wantQueued:  1,
		},
		{
			name:        "ValidRepoAndPRNumberWithRiver",
			body:        `{"repo":"azylman/aerial","pr_number":202}`,
			useInserter: true,
			wantStatus:  http.StatusAccepted,
			wantSubstr:  `"status":"accepted"`,
			wantQueued:  1,
		},
		{
			name:        "RiverInsertErrorOnBuildIDReturns500",
			body:        `{"build_id":"build-888"}`,
			useInserter: true,
			inserterErr: errors.New("river pool full"),
			wantStatus:  http.StatusInternalServerError,
			wantSubstr:  "failed to sync build widget",
		},
		{
			name:        "RiverInsertErrorOnPRReturns500",
			body:        `{"repo":"azylman/aerial","pr_number":203}`,
			useInserter: true,
			inserterErr: errors.New("river pool full"),
			wantStatus:  http.StatusInternalServerError,
			wantSubstr:  "failed to sync pr widget",
		},
		{
			name:          "ValidBuildIDDirectWithoutRiver",
			body:          `{"build_id":"build-999","banner":"Direct sync"}`,
			vanadisStatus: http.StatusOK,
			wantStatus:    http.StatusAccepted,
			wantSubstr:    `"build_id":"build-999"`,
		},
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
		{
			name:      "ProcessMergeConflict nil server",
			nilServer: true,
		},
		{
			name:      "enqueueMergeConflict nil server",
			nilServer: true,
			isEnqueue: true,
		},
		{
			name:          "ProcessMergeConflict valid dispatch",
			hasDispatcher: true,
			args: MergeConflictArgs{
				Repo:     "azylman/aerial",
				Branch:   "feat/conflict",
				HeadSHA:  "sha-conflict-1",
				PRNumber: 543,
				TargetID: "1555405874565091380",
			},
			wantPrompts: 1,
		},
		{
			name:      "enqueueMergeConflict with river client enqueues",
			isEnqueue: true,
			hasRiver:  true,
			args: MergeConflictArgs{
				Repo:     "aerial",
				Branch:   "feat/conflict",
				HeadSHA:  "sha-conflict-2",
				PRNumber: 544,
				TargetID: "1555405874565091380",
			},
			wantEnqueued: 1,
		},
		{
			name:          "enqueueMergeConflict without river client dispatches directly",
			isEnqueue:     true,
			hasDispatcher: true,
			args: MergeConflictArgs{
				Repo:     "azylman/aerial",
				Branch:   "feat/conflict",
				HeadSHA:  "sha-conflict-3",
				PRNumber: 545,
				TargetID: "1555405874565091380",
			},
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

func TestRecordNomadJobProgress_Cases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name   string
		server *RouterServer
		jobID  string
		state  string
	}{
		{
			name:   "nil server",
			server: nil,
			jobID:  "job-1",
			state:  "running",
		},
		{
			name:   "empty jobID",
			server: newTestRouter(Config{}, nil, &mockPRRegistry{}, nil),
			jobID:  "",
			state:  "running",
		},
		{
			name:   "registry error",
			server: newTestRouter(Config{}, nil, &mockPRRegistry{recordJobStateErr: errors.New("db err")}, nil),
			jobID:  "job-1",
			state:  "running",
		},
		{
			name: "updated with pr and repo",
			server: newTestRouter(Config{}, nil, &mockPRRegistry{
				recordJobStateRepo:    "azylman/aerial",
				recordJobStatePRNum:   123,
				recordJobStateUpdated: true,
			}, nil),
			jobID: "job-1",
			state: "running",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.server.recordNomadJobProgress(ctx, tc.jobID, tc.state)
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
			srv := NewRouterServer(Config{}, nil)
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
