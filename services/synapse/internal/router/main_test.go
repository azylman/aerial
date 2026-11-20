package router

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
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
	// publishedTraceparents records the W3C traceparent the real bus injects into
	// each published message's NATS headers.
	publishedTraceparents []string
	err                   error
}

func (m *mockEventBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.publishedTraceparents = append(m.publishedTraceparents, tracing.Traceparent(ctx))
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

type mockPRRegistry struct {
	rawMetadata            map[string]any
	recordedBuildMetadata  []BuildMetadata
	recordBuildMetadataErr error
	buildLifecycle         PRBuildLifecycle
	buildMetadata          BuildMetadata
	buildMetadataFound     bool
	buildMetadataErr       error
	buildLifecycleErr      error
	mergedCalls            []prCall
	syncCalls              []prCall
	closedUnmergedCalls    []prCall
	ciFailedCalls          []prCall
	ciFailedBySHACalls     []prCall
	backfillCalls          []prCall
	resolveBySHACalls      []prCall

	mergedTargetID   string
	syncTargetID     string
	closedTargetID   string
	backfillTargetID string

	resolvePRNum            int
	resolveBranch           string
	resolveHeadSHA          string
	resolveMergeSHA         string
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

	deployFailedCalls    []prCall
	deployFailedTargetID string
	deployFailedUpdated  bool
	deployFailedResults  []bool
	deployFailedErr      error

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

	ackDeployFailedPublishedCalls []prCall
	ackDeployFailedPublishedErr   error

	kustomizationRequestCalls []map[string]kustomizationRequest
	kustomizationRequestErr   error

	markKustomizationsCalls []prCall
	markKustomizationsRes   bool
	markKustomizationsErr   error
	markJobsCalls           []prCall
	markJobsRes             bool
	markJobsErr             error
	hasKustomizationReqs    bool
	hasKustomizationReqsErr error

	deployedRes bool
	deployedErr error

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

func popBoolResult(results *[]bool, fallback bool) bool {
	if len(*results) > 0 {
		res := (*results)[0]
		*results = (*results)[1:]
		return res
	}
	return fallback
}

func (m *mockPRRegistry) AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if err := m.checkErr(m.deployFailedErr); err != nil {
		return "", false, err
	}
	m.deployFailedCalls = append(m.deployFailedCalls, prCall{repo: repo, prNumber: prNumber})
	return m.deployFailedTargetID, popBoolResult(&m.deployFailedResults, m.deployFailedUpdated), nil
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
	// Mirror the projection: a successful sync leaves the PR open on this head SHA.
	m.buildLifecycle = PRBuildLifecycle{Status: prOpen, HeadSHA: headSHA}
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
	updated := popBoolResult(&m.ciFailedResults, m.ciFailedUpdated)
	if !updated {
		// Mirror the projection: a non-matching UPDATE returns no row, so
		// no target. Callers fall back to registry/headless resolution.
		return "", false, nil
	}
	m.buildLifecycle.Status = prCIFailed
	return m.ciFailedTargetID, true, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	if err := m.checkErr(m.ciFailedBySHAErr); err != nil {
		return "", 0, false, err
	}
	m.ciFailedBySHACalls = append(m.ciFailedBySHACalls, prCall{repo: repo, headSHA: headSHA})
	if m.ciFailedBySHAUpdated {
		m.buildLifecycle = PRBuildLifecycle{Status: prCIFailed, HeadSHA: headSHA}
	}
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
	return m.resolveTargetID, m.resolveBranch, m.resolveHeadSHA, m.resolveMergeSHA, nil
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

func (m *mockPRRegistry) MarkJobsDeployed(ctx context.Context, repo string, prNumber int, jobs []string) (bool, error) {
	m.markJobsCalls = append(m.markJobsCalls, prCall{repo: repo, prNumber: prNumber, jobs: jobs})
	if err := m.checkErr(m.markJobsErr); err != nil {
		return false, err
	}
	return m.markJobsRes, nil
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

func (m *mockPRRegistry) AcknowledgeDeployFailedPublished(ctx context.Context, repo string, prNumber int) error {
	m.ackDeployFailedPublishedCalls = append(m.ackDeployFailedPublishedCalls, prCall{repo: repo, prNumber: prNumber})
	return m.checkErr(m.ackDeployFailedPublishedErr)
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

type fixedConfigSource struct{ cfg Config }

func (f *fixedConfigSource) Current() *Config { return &f.cfg }

func fixedConfig(cfg Config) ConfigSource { return &fixedConfigSource{cfg: cfg} }

type handlePRRegisterCase struct {
	name            string
	cfgBotID        string
	cfgBots         map[string]config.BotConfig
	useNilReg       bool
	authEnabled     bool
	authHeader      string
	tokenReviewResp string
	sarResp         string
	sarPrsAllDeny   bool
	sarUpdateDeny   bool
	rawMetadata     map[string]any
	regErr          error
	body            string
	wantStatus      int
}

func handlePRRegisterCases() []handlePRRegisterCase {
	return append([]handlePRRegisterCase{
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
		{
			name:       "foreign bot_id returns 409 conflict",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"suletta"}`,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "foreign bot_id with cfg.Bots returns 409 conflict",
			cfgBots:    map[string]config.BotConfig{"aerial": {}},
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"foreign"}`,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "matching bot_id succeeds",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"aerial"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "case-insensitive matching bot_id succeeds",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"AERIAL"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty bot_id in request succeeds when config has bot_id",
			cfgBotID:   "aerial",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "bot_id in request succeeds when config has empty bot_id",
			cfgBotID:   "",
			body:       `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","bot_id":"suletta"}`,
			wantStatus: http.StatusOK,
		},
	}, handlePRRegisterAuthCases()...)
}

func handlePRRegisterAuthCases() []handlePRRegisterCase {
	return []handlePRRegisterCase{
		{
			name:        "k8sauth missing bearer token returns 401",
			authEnabled: true,
			body:        `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus:  http.StatusUnauthorized,
		},
		{
			name:            "k8sauth invalid bearer token returns 401",
			authEnabled:     true,
			authHeader:      "Bearer invalid-token",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`,
			body:            `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001"}`,
			wantStatus:      http.StatusUnauthorized,
		},
		{
			name:            "k8sauth forbidden target agent returns 403",
			authEnabled:     true,
			authHeader:      "Bearer tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-miorine"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false}}`,
			body:            `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"suletta"}`,
			wantStatus:      http.StatusForbidden,
		},
		{
			name:            "k8sauth mismatched bot_id denied on prs/all returns 403",
			authEnabled:     true,
			authHeader:      "Bearer tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			sarPrsAllDeny:   true,
			body:            `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial","bot_id":"suletta"}`,
			wantStatus:      http.StatusForbidden,
		},
		{
			name:            "k8sauth mismatched existing agent denied on prs/update returns 403",
			authEnabled:     true,
			authHeader:      "Bearer tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			sarUpdateDeny:   true,
			rawMetadata:     map[string]any{"agent": "suletta"},
			body:            `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial"}`,
			wantStatus:      http.StatusForbidden,
		},
		{
			name:            "k8sauth authorized matching agent succeeds",
			authEnabled:     true,
			authHeader:      "Bearer tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			body:            `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"aerial"}`,
			wantStatus:      http.StatusOK,
		},
		{
			name:            "k8sauth non-default agent registering brand-new PR succeeds without prs:update on default agent",
			authEnabled:     true,
			authHeader:      "Bearer tok",
			tokenReviewResp: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-miorine"}}}`,
			sarResp:         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`,
			sarUpdateDeny:   true,
			rawMetadata:     nil,
			body:            `{"repo":"azylman/aerial","pr_number":42,"branch":"feat/test","head_sha":"sha123","target_id":"100000000000000001","agent":"miorine"}`,
			wantStatus:      http.StatusOK,
		},
	}
}

func TestHandlePRRegister(t *testing.T) {
	t.Parallel()

	for _, tc := range handlePRRegisterCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg PRRegistryUpdater
			var mockReg *mockPRRegistry
			if !tc.useNilReg {
				mockReg = &mockPRRegistry{registerErr: tc.regErr, rawMetadata: tc.rawMetadata}
				reg = mockReg
			}
			srv := newTestRouter(Config{
				SystemChannelID: "100000000000000001",
				BotID:           tc.cfgBotID,
				Bots:            tc.cfgBots,
			}, nil, reg, &mockOutboundDispatcher{})
			if tc.authEnabled {
				tokenFile := filepath.Join(t.TempDir(), "token")
				_ = os.WriteFile(tokenFile, []byte("sa-token"), 0o600)
				rt := &cannedAuthRoundTripper{
					tokenReviewResp: tc.tokenReviewResp,
					sarResp:         tc.sarResp,
					sarPrsAllDeny:   tc.sarPrsAllDeny,
					sarUpdateDeny:   tc.sarUpdateDeny,
				}
				auth := k8sauth.NewAuthenticator(k8sauth.Config{
					Enabled:      true,
					APIServerURL: "https://k8s.local",
					HTTPClient:   &http.Client{Transport: rt},
					TokenPath:    tokenFile,
				})
				srv.SetAuthenticator(auth)
			}
			var headers map[string]string
			if tc.authHeader != "" {
				headers = map[string]string{"Authorization": tc.authHeader}
			}
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/internal/pr/register", strings.NewReader(tc.body), headers)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusConflict {
				if !strings.Contains(rec.Body.String(), "bot_id mismatch for local pr_registry") {
					t.Errorf("body = %s, want substr 'bot_id mismatch for local pr_registry'", rec.Body.String())
				}
				if mockReg != nil && len(mockReg.registerCalls) > 0 {
					t.Errorf("registerCalls = %d, want 0 when bot_id mismatches", len(mockReg.registerCalls))
				}
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

func (m *mockPRRegistry) RecordPRBuildMetadata(_ context.Context, _ string, _ int, metadata BuildMetadata) (bool, error) {
	m.recordedBuildMetadata = append(m.recordedBuildMetadata, metadata)
	return m.recordBuildMetadataErr == nil, m.recordBuildMetadataErr
}

func (m *mockPRRegistry) LoadPRBuildMetadata(context.Context, string, int) (BuildMetadata, bool, error) {
	return m.buildMetadata, m.buildMetadataFound, m.buildMetadataErr
}

func (m *mockPRRegistry) LoadPRBuildLifecycle(context.Context, string, int) (PRBuildLifecycle, error) {
	return m.buildLifecycle, m.buildLifecycleErr
}

func (m *mockPRRegistry) LoadRawPRMetadata(context.Context, string, int) (map[string]any, error) {
	if m != nil && m.rawMetadata != nil {
		return m.rawMetadata, nil
	}
	return map[string]any{}, nil
}

func (m *mockPRRegistry) IsPRDeployed(context.Context, string, int) (bool, error) {
	if m == nil {
		return false, nil
	}
	return m.deployedRes, m.deployedErr
}

type cannedAuthRoundTripper struct {
	tokenReviewResp    string
	sarResp            string
	sarPrsAllDeny      bool
	sarUpdateDeny      bool
	sarUpdateDenyAgent string
}

func (c *cannedAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var body string
	switch {
	case strings.Contains(req.URL.Path, "tokenreviews"):
		body = c.tokenReviewResp
	case strings.Contains(req.URL.Path, "subjectaccessreviews"):
		var reqBytes []byte
		if req.Body != nil {
			reqBytes, _ = io.ReadAll(req.Body)
			req.Body = io.NopCloser(bytes.NewReader(reqBytes))
		}
		reqStr := string(reqBytes)
		switch {
		case c.sarPrsAllDeny && strings.Contains(reqStr, `"resource":"prs/all"`):
			body = `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false,"reason":"prs/all denied"}}`
		case c.sarUpdateDeny && strings.Contains(reqStr, `"verb":"update"`):
			if c.sarUpdateDenyAgent == "" || strings.Contains(reqStr, `"name":"`+c.sarUpdateDenyAgent+`"`) {
				body = `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false,"reason":"update denied"}}`
			} else {
				body = c.sarResp
			}
		default:
			body = c.sarResp
		}
	default:
		body = `{}`
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": {"application/json"}},
		Request:    req,
	}, nil
}

func TestWriteAuthError_And_ServerAccessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name:       "validation error returns 400",
			err:        &k8sauth.ValidationError{Message: "invalid token format"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unauthorized error returns 401",
			err:        &k8sauth.UnauthorizedError{Message: "invalid bearer token"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "forbidden error returns 403",
			err:        &k8sauth.ForbiddenError{Message: "caller not allowed"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "generic error returns 500",
			err:        errors.New("unexpected internal error"),
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			writeAuthError(w, tc.err)
			if w.Code != tc.wantStatus {
				t.Errorf("writeAuthError status = %d, want %d", w.Code, tc.wantStatus)
			}
		})
	}

	srv := &RouterServer{}
	reg := &mockPRRegistry{}
	disp := &mockOutboundDispatcher{}
	srv.SetRegistry(reg)
	srv.SetDispatcher(disp)
	if srv.Registry() != reg {
		t.Errorf("srv.Registry() mismatch")
	}
	if srv.Dispatcher() != disp {
		t.Errorf("srv.Dispatcher() mismatch")
	}
	var called bool
	srv.SetOnProcessed(func(event, delivery string) { called = true })
	if srv.onProcessed != nil {
		srv.onProcessed("test", "deliv")
		if !called {
			t.Errorf("onProcessed was not invoked")
		}
	}
}

func TestDefaultOutboundDispatcher_DiscordTo(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	disp := NewDefaultOutboundDispatcher(ts.URL, ts.Client())
	ctx := context.Background()
	if err := disp.DispatchDiscordMessageTo(ctx, ts.URL, DiscordMessageIngressRequest{
		BotUserID: "b1",
		Message:   &discordgo.Message{ID: "m1", Content: "hi"},
	}); err != nil {
		t.Errorf("DispatchDiscordMessageTo err = %v", err)
	}
	if err := disp.DispatchDiscordInteractionTo(ctx, ts.URL, &discordgo.Interaction{
		ID: "ix1",
	}); err != nil {
		t.Errorf("DispatchDiscordInteractionTo err = %v", err)
	}
}

func TestProcessDiscordIngress_Routing(t *testing.T) {
	t.Parallel()
	disp := &mockOutboundDispatcher{}
	cfg := Config{
		Bots: map[string]config.BotConfig{
			"miorine": {EriURL: "http://eri-miorine:8080"},
		},
	}
	srv := newTestRouter(cfg, nil, nil, disp)
	ctx := context.Background()

	// Dispatch with matching agent in bots
	err := srv.ProcessDiscordIngress(ctx, DiscordIngressArgs{
		Agent:   "miorine",
		Message: &discordgo.Message{ID: "m1", Content: "hello"},
	})
	if err != nil {
		t.Fatalf("ProcessDiscordIngress err = %v", err)
	}

	// Dispatch without matching agent (falls back to default dispatch)
	err = srv.ProcessDiscordIngress(ctx, DiscordIngressArgs{
		Agent:   "unknown",
		Message: &discordgo.Message{ID: "m2", Content: "hello2"},
	})
	if err != nil {
		t.Fatalf("ProcessDiscordIngress err = %v", err)
	}
}

func TestProcessDiscordInteraction_Routing(t *testing.T) {
	t.Parallel()
	disp := &mockOutboundDispatcher{}
	cfg := Config{
		Bots: map[string]config.BotConfig{
			"miorine": {EriURL: "http://eri-miorine:8080"},
		},
	}
	srv := newTestRouter(cfg, nil, nil, disp)
	ctx := context.Background()

	// Dispatch with matching agent in bots
	err := srv.ProcessDiscordInteraction(ctx, DiscordInteractionArgs{
		Agent:       "miorine",
		Interaction: &discordgo.Interaction{ID: "ix1"},
	})
	if err != nil {
		t.Fatalf("ProcessDiscordInteraction err = %v", err)
	}

	// Dispatch without matching agent
	err = srv.ProcessDiscordInteraction(ctx, DiscordInteractionArgs{
		Agent:       "unknown",
		Interaction: &discordgo.Interaction{ID: "ix2"},
	})
	if err != nil {
		t.Fatalf("ProcessDiscordInteraction err = %v", err)
	}
}

func TestStartDiscordGatewayWrapper(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// len(Bots) > 0
	srv := newTestRouter(Config{
		Bots: map[string]config.BotConfig{
			"aerial": {DiscordToken: ""},
		},
	}, nil, nil, nil)
	stopFn, err := startDiscordGateway(ctx, srv, "")
	if err != nil {
		t.Fatalf("startDiscordGateway err = %v", err)
	}
	if stopFn != nil {
		stopFn()
	}

	// len(Bots) == 0
	srvLegacy := newTestRouter(Config{}, nil, nil, nil)
	stopFn, err = startDiscordGateway(ctx, srvLegacy, "")
	if err != nil {
		t.Fatalf("startDiscordGateway legacy err = %v", err)
	}
	if stopFn != nil {
		stopFn()
	}
}

func TestRouter_EdgeCasesCoverage(t *testing.T) {
	t.Parallel()

	// Authenticator() accessor
	srv := &RouterServer{}
	if srv.Authenticator() != nil {
		t.Errorf("expected nil authenticator")
	}
	srv.SetAuthenticator(&k8sauth.Authenticator{})
	if srv.Authenticator() == nil {
		t.Errorf("expected non-nil authenticator")
	}

	// NewRouterServer with Auth.Enabled == true
	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "token")
	_ = os.WriteFile(tokenFile, []byte("tok"), 0o600)
	cfgSrc := fixedConfig(Config{
		Auth: k8sauth.Config{
			Enabled:   true,
			TokenPath: tokenFile,
		},
	})
	server := NewRouterServer(cfgSrc, nil)
	if server.Authenticator() == nil {
		t.Errorf("expected initialized authenticator")
	}

	// AgentName() coverage
	if got := (AerialPRMetadata{Agent: "Suletta"}).AgentName(); got != "suletta" {
		t.Errorf("AgentName() = %q, want suletta", got)
	}
	if got := (AerialPRMetadata{BotID: "Miorine"}).AgentName(); got != "miorine" {
		t.Errorf("AgentName() = %q, want miorine", got)
	}
	if got := (AerialPRMetadata{}).AgentName(); got != "" {
		t.Errorf("AgentName() = %q, want empty", got)
	}

	// resolvePRAgent with LoadRawPRMetadata returning agent or bot_id
	mockReg := &mockPRRegistry{
		rawMetadata: map[string]any{"agent": "suletta"},
	}
	srv = &RouterServer{registry: mockReg}
	if got := srv.resolvePRAgent(context.Background(), "repo", 42); got != "suletta" {
		t.Errorf("resolvePRAgent() = %q, want suletta", got)
	}
	mockReg.rawMetadata = map[string]any{"bot_id": "miorine"}
	if got := srv.resolvePRAgent(context.Background(), "repo", 42); got != "miorine" {
		t.Errorf("resolvePRAgent() = %q, want miorine", got)
	}

	// routeRepoEvent fallback branches
	srvNilCfg := &RouterServer{}
	_ = srvNilCfg.routeRepoEvent(context.Background(), "test", restateclient.RepoEvent{})

	// isForeignBotEvent coverage
	var nilSrv *RouterServer
	if nilSrv.isForeignBotEvent(AerialPRMetadata{}) {
		t.Errorf("expected false for nil server")
	}
	srvNoCfg := &RouterServer{}
	if srvNoCfg.isForeignBotEvent(AerialPRMetadata{}) {
		t.Errorf("expected false for nil cfgSrc")
	}
	srvEmptyCfg := &RouterServer{cfgSrc: fixedConfig(Config{})}
	if srvEmptyCfg.isForeignBotEvent(AerialPRMetadata{}) {
		t.Errorf("expected false for empty meta")
	}
	srvBots := &RouterServer{cfgSrc: fixedConfig(Config{
		Bots: map[string]config.BotConfig{
			"aerial": {},
		},
	})}
	if srvBots.isForeignBotEvent(AerialPRMetadata{Agent: "aerial"}) {
		t.Errorf("expected false for matching bot in cfg.Bots")
	}
	if !srvBots.isForeignBotEvent(AerialPRMetadata{Agent: "unknown"}) {
		t.Errorf("expected true for foreign bot in cfg.Bots")
	}

	// resolveJobQueueAndKind and normalizeJobArgs coverage
	q, k := resolveJobQueueAndKind(DeployWebhookArgs{})
	if q != queueDefault || k != "deploy_webhook" {
		t.Errorf("resolveJobQueueAndKind(DeployWebhookArgs) = %s, %s", q, k)
	}
	deployPtr := &DeployWebhookArgs{}
	q, k = resolveJobQueueAndKind(deployPtr)
	if q != queueDefault || k != "deploy_webhook" {
		t.Errorf("resolveJobQueueAndKind(&DeployWebhookArgs) = %s, %s", q, k)
	}
	ghPtr := &GitHubWebhookArgs{}
	_ = normalizeJobArgs(ghPtr)
	var nilGH *GitHubWebhookArgs
	_ = normalizeJobArgs(nilGH)
	discPtr := &DiscordIngressArgs{}
	_ = normalizeJobArgs(discPtr)
	var nilDisc *DiscordIngressArgs
	_ = normalizeJobArgs(nilDisc)
	actPtr := &DiscordInteractionArgs{}
	_ = normalizeJobArgs(actPtr)
	var nilAct *DiscordInteractionArgs
	_ = normalizeJobArgs(nilAct)
}
