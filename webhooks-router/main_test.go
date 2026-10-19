package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
	insertedVanadisJobs    []VanadisExecuteChunkArgs
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
	case VanadisExecuteChunkArgs:
		m.insertedVanadisJobs = append(m.insertedVanadisJobs, a)
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

	conflictCalls    []prCall
	conflictTargetID string
	conflictUpdated  bool
	conflictErr      error

	deployingCalls    []prCall
	deployingTargetID string
	deployingErr      error

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

	channelContextMap map[string][]ChannelMessageContext
	channelContextErr error

	advanceVanadisCalls []prCall
	advanceVanadisRes   *VanadisAdvancementResult
	advanceVanadisErr   error

	suppressVanadisCalls []prCall
	suppressVanadisRes   bool
	suppressVanadisErr   error

	failVanadisCalls []prCall
	failVanadisRes   bool
	failVanadisErr   error

	ciFailCount    int
	ciFailCountErr error

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
	return m.deployingTargetID, nil
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

func (m *mockPRRegistry) GetRecentChannelContext(ctx context.Context, targetID string, limit int) ([]ChannelMessageContext, error) {
	if m.channelContextErr != nil {
		return nil, m.channelContextErr
	}
	if m.channelContextMap != nil {
		return m.channelContextMap[targetID], nil
	}
	return nil, nil
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
	if m.err != nil {
		return "", m.err
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
	return m.resolveTargetID, m.resolveBranch, "", "", nil
}

func (m *mockPRRegistry) AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	m.advanceVanadisCalls = append(m.advanceVanadisCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.advanceVanadisErr); err != nil {
		return nil, err
	}
	if m.advanceVanadisRes != nil {
		return m.advanceVanadisRes, nil
	}
	return &VanadisAdvancementResult{}, nil
}

func (m *mockPRRegistry) ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.suppressVanadisCalls = append(m.suppressVanadisCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.suppressVanadisErr); err != nil {
		return false, err
	}
	return m.suppressVanadisRes, nil
}

func (m *mockPRRegistry) FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.failVanadisCalls = append(m.failVanadisCalls, prCall{repo: repo, prNumber: prNumber})
	if err := m.checkErr(m.failVanadisErr); err != nil {
		return false, err
	}
	return m.failVanadisRes, nil
}

func (m *mockPRRegistry) IncrementCIFailCount(ctx context.Context, repo string, prNumber int) (int, error) {
	return m.ciFailCount, m.checkErr(m.ciFailCountErr)
}

type mockOutboundDispatcher struct {
	mu                 sync.Mutex
	gitPushCalls       []GitPushEventRequest
	imageReadyCalls    []ImageReadyEventRequest
	promptCalls        []PromptRequest
	directMessageCalls []DirectMessageRequest
	gitPushErr         error
	imageReadyErr      error
	promptErr          error
	directMessageErr   error
	widgetErr          error
	widgetReturnMsgID  string
	widgetCalls        []StatusWidgetRequest
	matchedJobs        map[string][]string
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
	return &mockPRRegistry{deployedTargetID: targetID, deployedPRNum: pr, deployedMergeSHA: sha, deployedRepo: repo, deployedUpdated: updated}
}

func mkDeployFailedReg(targetID string, pr int, sha, repo string, updated bool) *mockPRRegistry {
	return &mockPRRegistry{deployFailedJobTargetID: targetID, deployFailedJobPRNum: pr, deployFailedJobMergeSHA: sha, deployFailedJobRepo: repo, deployFailedJobUpdated: updated}
}

// --- 1. Config & Pure Helpers ---

func TestLoadConfig_EnvAndFallbacks(t *testing.T) {
	tests := []struct {
		name          string
		env           map[string]string
		wantPort      string
		wantInfURL    string
		wantCID       string
		wantCSec      string
		wantPID       string
		wantEnv       string
		wantNomadAddr string
		wantNomadTok  string
		wantGHTok     string
		wantGHURL     string
		wantPGURL     string
		wantSysChan   string
	}{
		{
			name: "primary infisical and nomad envs",
			env: map[string]string{
				"PORT":                    "9999",
				"INFISICAL_URL":           "http://infisical:8085/",
				"INFISICAL_CLIENT_ID":     "cid",
				"INFISICAL_CLIENT_SECRET": "csec",
				"INFISICAL_PROJECT_ID":    "pid",
				"INFISICAL_ENVIRONMENT":   "staging",
				"NOMAD_ADDR":              "http://nomad:4646/",
				"NOMAD_TOKEN":             "ntok",
			},
			wantPort:      "9999",
			wantInfURL:    "http://infisical:8085",
			wantCID:       "cid",
			wantCSec:      "csec",
			wantPID:       "pid",
			wantEnv:       "staging",
			wantNomadAddr: "http://nomad:4646",
			wantNomadTok:  "ntok",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
		},
		{
			name: "alternate environment variable keys",
			env: map[string]string{
				"INFISICAL_HOST_URL":                     "http://alternate:8085",
				"INFISICAL_UNIVERSAL_AUTH_CLIENT_ID":     "u-cid",
				"INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET": "u-csec",
				"INFISICAL_WORKSPACE_ID":                 "w-id",
			},
			wantInfURL: "http://alternate:8085",
			wantCID:    "u-cid",
			wantCSec:   "u-csec",
			wantPID:    "w-id",
		},
		{
			name:          "default fallback loading with empty envs",
			env:           map[string]string{},
			wantPort:      "4020",
			wantInfURL:    "http://infisical.aerial",
			wantEnv:       "prod",
			wantNomadAddr: "http://127.0.0.1:4646",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
		},
		{
			name: "github and postgres primary envs",
			env: map[string]string{
				"GITHUB_PAT":     "pat123",
				"GITHUB_API_URL": "https://api.github.test/",
				"POSTGRES_URL":   "postgres://custom:pass@db:5432/customdb",
			},
			wantGHTok: "pat123",
			wantGHURL: "https://api.github.test",
			wantPGURL: "postgres://custom:pass@db:5432/customdb",
		},
		{
			name: "github token and database url fallback envs",
			env: map[string]string{
				"GITHUB_TOKEN": "tok456",
				"DATABASE_URL": "postgres://dbuser:pass@db:5432/db",
			},
			wantGHTok: "tok456",
			wantPGURL: "postgres://dbuser:pass@db:5432/db",
		},
		{
			name:        "system channel id primary env",
			env:         map[string]string{"SYSTEM_CHANNEL_ID": "123456789012345678"},
			wantSysChan: "123456789012345678",
		},
		{
			name:        "discord channel id fallback env",
			env:         map[string]string{"DISCORD_CHANNEL_ID": "987654321098765432"},
			wantSysChan: "987654321098765432",
		},
	}

	keysToClear := []string{
		"PORT", "INFISICAL_URL", "INFISICAL_HOST_URL", "INFISICAL_CLIENT_ID",
		"INFISICAL_UNIVERSAL_AUTH_CLIENT_ID", "INFISICAL_CLIENT_SECRET",
		"INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET", "INFISICAL_PROJECT_ID",
		"INFISICAL_WORKSPACE_ID", "INFISICAL_ENVIRONMENT", "NOMAD_ADDR",
		"NOMAD_TOKEN", "GITHUB_PAT", "GITHUB_TOKEN", "GITHUB_API_URL",
		"POSTGRES_URL", "DATABASE_URL", "HANGAR_URL", "BRAIN_INTERNAL_URL",
		"BRAIN_URL", "SYSTEM_CHANNEL_ID", "DISCORD_CHANNEL_ID", "CONFIG_PATH",
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range keysToClear {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg := LoadConfigFromEnv()
			for _, check := range []struct{ got, want, field string }{
				{cfg.Port, tc.wantPort, "Port"},
				{cfg.InfisicalURL, tc.wantInfURL, "InfisicalURL"},
				{cfg.InfisicalClientID, tc.wantCID, "InfisicalClientID"},
				{cfg.InfisicalClientSecret, tc.wantCSec, "InfisicalClientSecret"},
				{cfg.InfisicalProjectID, tc.wantPID, "InfisicalProjectID"},
				{cfg.InfisicalEnvironment, tc.wantEnv, "InfisicalEnvironment"},
				{cfg.NomadAddr, tc.wantNomadAddr, "NomadAddr"},
				{cfg.NomadToken, tc.wantNomadTok, "NomadToken"},
				{cfg.GitHubToken, tc.wantGHTok, "GitHubToken"},
				{cfg.GitHubAPIURL, tc.wantGHURL, "GitHubAPIURL"},
				{cfg.PostgresURL, tc.wantPGURL, "PostgresURL"},
				{cfg.SystemChannelID, tc.wantSysChan, "SystemChannelID"},
			} {
				if check.want != "" && check.got != check.want {
					t.Errorf("%s = %q, want %q", check.field, check.got, check.want)
				}
			}
		})
	}
}

func TestLoadConfig_YAMLAndOverrides(t *testing.T) {
	tests := []struct {
		name            string
		localYAML       string
		fallbackYAML    string
		useEmptyPath    bool
		missingFallback bool
		env             map[string]string
		wantPort        string
		wantInfURL      string
		wantEnv         string
		wantNomadAddr   string
		wantHangarURL   string
		wantBrainURL    string
		wantSysChan     string
	}{
		{
			name: "yaml config without overrides",
			localYAML: `port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
nomad_addr: "http://nomad.custom:4646"
hangar_url: "http://hangar.custom:8087"
brain_url: "http://brain.custom:8088"
`,
			wantPort:      "4099",
			wantInfURL:    "http://infisical.custom:8085",
			wantEnv:       "staging",
			wantNomadAddr: "http://nomad.custom:4646",
			wantHangarURL: "http://hangar.custom:8087",
			wantBrainURL:  "http://brain.custom:8088",
		},
		{
			name: "env overrides yaml values",
			localYAML: `port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
nomad_addr: "http://nomad.custom:4646"
hangar_url: "http://hangar.custom:8087"
brain_url: "http://brain.custom:8088"
`,
			env: map[string]string{
				"PORT":                  "5000",
				"INFISICAL_ENVIRONMENT": "dev",
			},
			wantPort:      "5000",
			wantInfURL:    "http://infisical.custom:8085",
			wantEnv:       "dev",
			wantNomadAddr: "http://nomad.custom:4646",
			wantHangarURL: "http://hangar.custom:8087",
			wantBrainURL:  "http://brain.custom:8088",
		},
		{
			name:          "zero-byte local config with existing fallback",
			localYAML:     "",
			fallbackYAML:  "port: \"4088\"\ninfisical_url: \"http://infisical.fallback:8085\"\nnomad_addr: \"http://nomad.fallback:4646\"\n",
			wantPort:      "4088",
			wantInfURL:    "http://infisical.fallback:8085",
			wantNomadAddr: "http://nomad.fallback:4646",
		},
		{
			name:            "zero-byte local config with missing fallback uses defaults",
			localYAML:       "",
			missingFallback: true,
			wantPort:        "4020",
			wantInfURL:      "http://infisical.aerial",
		},
		{
			name:         "empty config path with valid fallback",
			useEmptyPath: true,
			fallbackYAML: "port: \"4088\"\ninfisical_url: \"http://infisical.fallback:8085\"\nnomad_addr: \"http://nomad.fallback:4646\"\n",
			wantPort:     "4088",
		},
		{
			name:         "non-empty local config takes precedence over fallback",
			localYAML:    "port: \"4077\"\n",
			fallbackYAML: "port: \"4088\"\n",
			wantPort:     "4077",
		},
		{
			name:        "system channel id loaded from yaml",
			localYAML:   "system_channel_id: \"112233445566778899\"\n",
			wantSysChan: "112233445566778899",
		},
		{
			name:      "invalid yaml logs warning and uses defaults",
			localYAML: "invalid: [yaml",
			wantPort:  "4020",
		},
	}

	keysToClear := []string{
		"PORT", "INFISICAL_URL", "INFISICAL_HOST_URL", "INFISICAL_ENVIRONMENT",
		"NOMAD_ADDR", "HANGAR_URL", "BRAIN_URL", "SYSTEM_CHANNEL_ID", "DISCORD_CHANNEL_ID",
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			var targetPath string
			if !tc.useEmptyPath {
				targetPath = filepath.Join(tmpDir, "local.yaml")
				if err := os.WriteFile(targetPath, []byte(tc.localYAML), 0644); err != nil {
					t.Fatalf("failed to write local config: %v", err)
				}
			}

			if tc.fallbackYAML != "" || tc.missingFallback {
				oldFallback := webhooksRouterFallbackConfigPath
				if tc.missingFallback {
					webhooksRouterFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
				} else {
					fbPath := filepath.Join(tmpDir, "fallback.yaml")
					if err := os.WriteFile(fbPath, []byte(tc.fallbackYAML), 0644); err != nil {
						t.Fatalf("failed to write fallback config: %v", err)
					}
					webhooksRouterFallbackConfigPath = fbPath
				}
				t.Cleanup(func() { webhooksRouterFallbackConfigPath = oldFallback })
			}

			for _, k := range keysToClear {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg := LoadConfig(targetPath)
			for _, check := range []struct{ got, want, field string }{
				{cfg.Port, tc.wantPort, "Port"},
				{cfg.InfisicalURL, tc.wantInfURL, "InfisicalURL"},
				{cfg.InfisicalEnvironment, tc.wantEnv, "InfisicalEnvironment"},
				{cfg.NomadAddr, tc.wantNomadAddr, "NomadAddr"},
				{cfg.HangarURL, tc.wantHangarURL, "HangarURL"},
				{cfg.BrainURL, tc.wantBrainURL, "BrainURL"},
				{cfg.SystemChannelID, tc.wantSysChan, "SystemChannelID"},
			} {
				if check.want != "" && check.got != check.want {
					t.Errorf("%s = %q, want %q", check.field, check.got, check.want)
				}
			}
		})
	}
}

func TestInfisicalWebhookPayload_Getters(t *testing.T) {
	t.Parallel()
	type dataStruct = struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
	}
	tests := []struct {
		name     string
		payload  InfisicalWebhookPayload
		fallback string
		wantWS   string
		wantEnv  string
	}{
		{name: "workspace id from payload field", payload: InfisicalWebhookPayload{WorkspaceID: "ws-1"}, fallback: "fb", wantWS: "ws-1", wantEnv: "fb"},
		{name: "project id from payload field", payload: InfisicalWebhookPayload{ProjectID: "proj-1"}, fallback: "fb", wantWS: "proj-1", wantEnv: "fb"},
		{name: "workspace id from nested data", payload: InfisicalWebhookPayload{Data: &dataStruct{WorkspaceID: "ws-data"}}, fallback: "fb", wantWS: "ws-data", wantEnv: "fb"},
		{name: "project id from nested data", payload: InfisicalWebhookPayload{Data: &dataStruct{ProjectID: "proj-data"}}, fallback: "fb", wantWS: "proj-data", wantEnv: "fb"},
		{name: "environment from payload field", payload: InfisicalWebhookPayload{Environment: "prod"}, fallback: "fb", wantWS: "fb", wantEnv: "prod"},
		{name: "environment from nested data", payload: InfisicalWebhookPayload{Data: &dataStruct{Environment: "staging"}}, fallback: "fb", wantWS: "fb", wantEnv: "staging"},
		{name: "fallback when fields empty", payload: InfisicalWebhookPayload{}, fallback: "fb", wantWS: "fb", wantEnv: "fb"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if gotWS := tc.payload.GetWorkspaceID(tc.fallback); gotWS != tc.wantWS {
				t.Errorf("GetWorkspaceID() = %q, want %q", gotWS, tc.wantWS)
			}
			if gotEnv := tc.payload.GetEnvironment(tc.fallback); gotEnv != tc.wantEnv {
				t.Errorf("GetEnvironment() = %q, want %q", gotEnv, tc.wantEnv)
			}
		})
	}
}

func TestStringAndRepoHelpers(t *testing.T) {
	t.Parallel()

	zeroSHATests := []struct {
		name string
		sha  string
		want bool
	}{
		{name: "isZeroSHA empty", sha: "", want: true},
		{name: "isZeroSHA 40 zeroes", sha: "0000000000000000000000000000000000000000", want: true},
		{name: "isZeroSHA single zero", sha: "0", want: true},
		{name: "isZeroSHA three zeroes", sha: "000", want: true},
		{name: "isZeroSHA non-zero ending with 1", sha: "0000000000000000000000000000000000000001", want: false},
		{name: "isZeroSHA valid commit sha", sha: "ed197571a381920f540e7a3c55b68008a45912a0", want: false},
	}
	for _, tc := range zeroSHATests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isZeroSHA(tc.sha); got != tc.want {
				t.Errorf("isZeroSHA(%q) = %v, want %v", tc.sha, got, tc.want)
			}
		})
	}

	normalizeTests := []struct {
		name, input, want string
	}{
		{name: "normalizeRepo empty", input: "", want: ""},
		{name: "normalizeRepo whitespace", input: "   ", want: ""},
		{name: "normalizeRepo aerial prefix", input: "aerial", want: "azylman/aerial"},
		{name: "normalizeRepo fully qualified aerial", input: "azylman/aerial", want: "azylman/aerial"},
		{name: "normalizeRepo trimmed mirrormere", input: "  mirrormere  ", want: "azylman/mirrormere"},
		{name: "normalizeRepo other org repo", input: "otherorg/custom", want: "otherorg/custom"},
	}
	for _, tc := range normalizeTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeRepo(tc.input); got != tc.want {
				t.Errorf("normalizeRepo(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}

	cleanRepoTests := []struct {
		name, input, want string
	}{
		{name: "cleanRepoName azylman prefix", input: "azylman/aerial", want: "aerial"},
		{name: "cleanRepoName with whitespace", input: "  azylman/mirrormere  ", want: "mirrormere"},
		{name: "cleanRepoName already clean", input: "aerial", want: "aerial"},
		{name: "cleanRepoName foreign repo", input: "otherorg/custom", want: "otherorg/custom"},
		{name: "cleanRepoName empty", input: "", want: ""},
	}
	for _, tc := range cleanRepoTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := cleanRepoName(tc.input); got != tc.want {
				t.Errorf("cleanRepoName(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}

	prefixTests := []struct {
		name, repo, want string
		prNumber         int
	}{
		{name: "formatDirectMessagePrefix pr and repo", prNumber: 42, repo: "azylman/aerial", want: "**(PR: #42, repo: aerial)** "},
		{name: "formatDirectMessagePrefix repo only", prNumber: 0, repo: "azylman/aerial", want: "**(repo: aerial)** "},
		{name: "formatDirectMessagePrefix pr only with empty repo", prNumber: 99, repo: "", want: "**(PR: #99)** "},
		{name: "formatDirectMessagePrefix neither", prNumber: 0, repo: "", want: ""},
	}
	for _, tc := range prefixTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatDirectMessagePrefix(tc.prNumber, tc.repo); got != tc.want {
				t.Errorf("formatDirectMessagePrefix(%d, %q) = %q, want %q", tc.prNumber, tc.repo, got, tc.want)
			}
		})
	}

	dedupeTests := []struct {
		name        string
		input, want []string
	}{
		{name: "dedupeTrimmedStrings duplicates and blanks", input: []string{"foo", "  foo  ", "bar", "", "   ", "baz", "bar"}, want: []string{"foo", "bar", "baz"}},
		{name: "dedupeTrimmedStrings empty slice", input: []string{}, want: nil},
	}
	for _, tc := range dedupeTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dedupeTrimmedStrings(tc.input)
			if len(got) != len(tc.want) {
				t.Fatalf("dedupeTrimmedStrings() len = %d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("dedupeTrimmedStrings()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}

	hangarSyncedTests := []struct {
		name, repo string
		want       bool
	}{
		{name: "isHangarSyncedRepo aerial bare", repo: "aerial", want: true},
		{name: "isHangarSyncedRepo aerial qualified", repo: "azylman/aerial", want: true},
		{name: "isHangarSyncedRepo aerial config qualified", repo: "azylman/aerial-config", want: true},
		{name: "isHangarSyncedRepo uppercase aerial-config", repo: "AZYLMAN/AERIAL-CONFIG", want: true},
		{name: "isHangarSyncedRepo mirrormere", repo: "mirrormere", want: false},
		{name: "isHangarSyncedRepo other", repo: "otherorg/custom", want: false},
		{name: "isHangarSyncedRepo empty", repo: "", want: false},
	}
	for _, tc := range hangarSyncedTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isHangarSyncedRepo(tc.repo); got != tc.want {
				t.Errorf("isHangarSyncedRepo(%q) = %v, want %v", tc.repo, got, tc.want)
			}
		})
	}

	if got := readBodyString(errReader{}); got != "" {
		t.Errorf("readBodyString(errReader) = %q, want empty", got)
	}
	if got := readBodyString(strings.NewReader("")); got != "" {
		t.Errorf("readBodyString(empty) = %q, want empty", got)
	}
	if n := len(randomNonce()); n != 32 {
		t.Errorf("len(randomNonce()) = %d, want 32", n)
	}
}

func TestPromptAndContextFormatting(t *testing.T) {
	t.Parallel()

	truncateTests := []struct {
		name, input, wantSuffix, wantSub, wantNotSub string
		wantLen                                      int
	}{
		{name: "truncatePromptDetails sanitizes backticks", input: "error occurred:\n```json\n{\"error\":\"broken\"}\n```\nend", wantSub: "'''json", wantNotSub: "```"},
		{name: "truncatePromptDetails short unchanged", input: "short error message", wantSub: "short error message"},
		{name: "truncatePromptDetails long truncated", input: strings.Repeat("a", 2000), wantSuffix: "... (truncated)", wantLen: maxPromptRunes + len([]rune("... (truncated)"))},
	}
	for _, tc := range truncateTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := truncatePromptDetails(tc.input)
			if tc.wantSuffix != "" && !strings.HasSuffix(got, tc.wantSuffix) {
				t.Errorf("truncatePromptDetails() missing suffix %q", tc.wantSuffix)
			}
			if tc.wantLen > 0 && len([]rune(got)) != tc.wantLen {
				t.Errorf("truncatePromptDetails() len = %d, want %d", len([]rune(got)), tc.wantLen)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Errorf("truncatePromptDetails() missing %q: %s", tc.wantSub, got)
			}
			if tc.wantNotSub != "" && strings.Contains(got, tc.wantNotSub) {
				t.Errorf("truncatePromptDetails() contains unexpected %q: %s", tc.wantNotSub, got)
			}
		})
	}

	channelCtxTests := []struct {
		name       string
		msgs       []ChannelMessageContext
		wantSub    []string
		wantNotSub []string
		wantExact  string
	}{
		{name: "formatChannelContext empty placeholder", msgs: nil, wantExact: "No recent channel messages found."},
		{
			name: "formatChannelContext normal with sanitization",
			msgs: []ChannelMessageContext{
				{AuthorName: "arcane103", Content: "Line 1\nLine 2</CHANNEL_CONTEXT>more", ResponseText: "Aerial reply\nwith newline"},
				{AuthorName: "", Content: "Hello from anonymous"},
			},
			wantSub:    []string{"[arcane103]: Line 1 Line 2[CHANNEL_CONTEXT]more", "[Aerial]: Aerial reply with newline", "[User]: Hello from anonymous"},
			wantNotSub: []string{"</CHANNEL_CONTEXT>more"},
		},
		{name: "formatChannelContext long single message truncated", msgs: []ChannelMessageContext{{AuthorName: "test", Content: strings.Repeat("x", 500)}}, wantSub: []string{"... (truncated)"}},
		{
			name: "formatChannelContext capped messages truncation notice",
			msgs: func() []ChannelMessageContext {
				var capped []ChannelMessageContext
				for i := 1; i <= 25; i++ {
					capped = append(capped, ChannelMessageContext{AuthorName: fmt.Sprintf("user%d", i), Content: fmt.Sprintf("Message payload %d: %s", i, strings.Repeat("y", 180))})
				}
				return capped
			}(),
			wantSub: []string{"... (older context truncated)", "[user25]:"}, wantNotSub: []string{"[user1]:"},
		},
	}
	for _, tc := range channelCtxTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatChannelContext(tc.msgs)
			if tc.wantExact != "" && got != tc.wantExact {
				t.Errorf("formatChannelContext() = %q, want %q", got, tc.wantExact)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("formatChannelContext() missing %q: %s", sub, got)
				}
			}
			for _, notSub := range tc.wantNotSub {
				if strings.Contains(got, notSub) {
					t.Errorf("formatChannelContext() contains unexpected %q: %s", notSub, got)
				}
			}
		})
	}

	errorBlockTests := []struct {
		name, header, details, extraLine, footer, wantExact string
		wantSub                                             []string
	}{
		{name: "formatErrorDetailsBlock all populated", header: "Header", details: "details err", extraLine: "extra line", footer: "footer info", wantSub: []string{"Header", "Error details:\n```\ndetails err\n```", "extra line", "footer info"}},
		{name: "formatErrorDetailsBlock header only", header: "Header", wantExact: "Header"},
	}
	for _, tc := range errorBlockTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatErrorDetailsBlock(tc.header, tc.details, tc.extraLine, tc.footer)
			if tc.wantExact != "" && got != tc.wantExact {
				t.Errorf("formatErrorDetailsBlock() = %q, want %q", got, tc.wantExact)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("formatErrorDetailsBlock() missing %q: %s", sub, got)
				}
			}
		})
	}

	vanadisPromptTests := []struct {
		name    string
		res     *VanadisAdvancementResult
		wantSub []string
		wantLen int
	}{
		{name: "formatVanadisBuildCompletedPrompt nil result", res: nil, wantLen: 0},
		{
			name: "formatVanadisBuildCompletedPrompt with chunks",
			res: &VanadisAdvancementResult{
				BuildID: "build-42", Tier: 2, Objective: "Complete feature migration",
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, Title: "Chunk One", PRNumber: 101},
					{ChunkIndex: 1, Title: "Chunk Two", Repo: "", PRNumber: 0},
				},
			},
			wantSub: []string{
				"Vanadis build build-42 completed (Tier 2, 2 chunks merged).",
				"Objective: Complete feature migration",
				"Merged Chunks Ledger:",
				"- Chunk 1/2: Chunk One (azylman/aerial, PR #101)",
				"- Chunk 2/2: Chunk Two (azylman/aerial)",
				"All Vanadis build chunks have merged into main.",
			},
		},
	}
	for _, tc := range vanadisPromptTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatVanadisBuildCompletedPrompt(tc.res)
			if tc.wantLen == 0 && tc.res == nil && got != "" {
				t.Errorf("formatVanadisBuildCompletedPrompt(nil) = %q, want empty", got)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("formatVanadisBuildCompletedPrompt() missing %q: %s", sub, got)
				}
			}
		})
	}
}

func TestIs4xxClientError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: false},
		{name: "status 400", err: errors.New("brain prompt returned status 400: bad request"), want: true},
		{name: "status 404", err: errors.New("brain prompt returned status 404: not found"), want: true},
		{name: "status 422", err: errors.New("brain internal message returned status 422: unprocessable"), want: true},
		{name: "status 403", err: errors.New("brain prompt returned status 403: forbidden"), want: true},
		{name: "status 500", err: errors.New("brain prompt returned status 500: internal server error"), want: false},
		{name: "status 502", err: errors.New("brain prompt returned status 502: bad gateway"), want: false},
		{name: "network timeout", err: errors.New("dial tcp 127.0.0.1:8088: i/o timeout"), want: false},
		{name: "invalid snowflake", err: errors.New("invalid discord snowflake channel id: \"abc\""), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := is4xxClientError(tc.err); got != tc.want {
				t.Errorf("%s: is4xxClientError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
			}
		})
	}
}

// --- 2. Infisical / Generic Secret Webhooks & Sync ---

func TestHandleInfisicalAndGenericWebhooks(t *testing.T) {
	t.Parallel()
	routes := NewRouterServer(Config{}, nil).Routes()

	cases := []struct {
		name, path, wantTargetVar, wantEnv, wantMsg string
		body                                        io.Reader
		wantStatus                                  int
	}{
		{name: "infisical valid payload", path: "/webhooks/infisical", body: strings.NewReader(`{"event":"secrets.modified"}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable},
		{name: "infisical malformed json", path: "/webhooks/infisical", body: strings.NewReader(`{not valid json}`), wantStatus: http.StatusBadRequest},
		{name: "infisical body read error", path: "/webhooks/infisical", body: errReader{}, wantStatus: http.StatusBadRequest},
		{name: "generic standard sync", path: "/webhooks/generic", body: strings.NewReader(`{"environment":"prod"}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable},
		{name: "generic empty body allowed", path: "/webhooks/generic", body: strings.NewReader(""), wantStatus: http.StatusAccepted},
		{name: "generic direct secrets map", path: "/webhooks/generic", body: strings.NewReader(`{"secrets":{"KEY1":"VAL1"}}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable, wantMsg: "Direct secret sync scheduled"},
		{name: "generic explicit workspace and env", path: "/webhooks/generic", body: strings.NewReader(`{"workspaceId":"custom-ws","environment":"dev"}`), wantStatus: http.StatusAccepted, wantTargetVar: TargetNomadVariable, wantEnv: "dev"},
		{name: "generic bad json", path: "/webhooks/generic", body: strings.NewReader(`{invalid`), wantStatus: http.StatusBadRequest},
		{name: "generic body read error", path: "/webhooks/generic", body: errReader{}, wantStatus: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, nil)
			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
			if tc.wantTargetVar != "" || tc.wantEnv != "" || tc.wantMsg != "" {
				var resp map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse response: %v", err)
				}
				if tc.wantTargetVar != "" && resp["targetVariable"] != tc.wantTargetVar {
					t.Errorf("expected targetVariable %q, got %v", tc.wantTargetVar, resp["targetVariable"])
				}
				if tc.wantEnv != "" && resp["environment"] != tc.wantEnv {
					t.Errorf("expected environment %q, got %v", tc.wantEnv, resp["environment"])
				}
				if tc.wantMsg != "" && resp["message"] != tc.wantMsg {
					t.Errorf("expected message %q, got %v", tc.wantMsg, resp["message"])
				}
			}
		})
	}
}

func TestSyncSecrets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		cfgOverride   *Config
		loginStatus   int
		loginBody     string
		secretsStatus int
		secretsBody   string
		nomadStatus   int
		unreachable   bool
		wantErr       bool
		wantNomadPuts int32
	}{
		{name: "successful full sync flow", loginStatus: http.StatusOK, loginBody: `{"accessToken":"mock-jwt-token"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"API_KEY","secretValue":"secret123"}]}`, nomadStatus: http.StatusOK, wantNomadPuts: 1},
		{name: "missing infisical credentials", cfgOverride: &Config{}, wantErr: true},
		{name: "login 500 server error", loginStatus: http.StatusInternalServerError, loginBody: "server error", wantErr: true},
		{name: "login invalid json response", loginStatus: http.StatusOK, loginBody: "{invalid-json", wantErr: true},
		{name: "login empty token string", loginStatus: http.StatusOK, loginBody: `{"accessToken":""}`, wantErr: true},
		{name: "login fallback token field", loginStatus: http.StatusOK, loginBody: `{"token":"fallback-token"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`, nomadStatus: http.StatusOK, wantNomadPuts: 1},
		{name: "secrets fetch 502 bad gateway", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusBadGateway, wantErr: true},
		{name: "secrets fetch invalid json", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: "{invalid-secrets", wantErr: true},
		{name: "nomad put 201 created", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`, nomadStatus: http.StatusCreated, wantNomadPuts: 1},
		{name: "nomad put 204 no content", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`, nomadStatus: http.StatusNoContent, wantNomadPuts: 1},
		{name: "nomad put 500 internal server error", loginStatus: http.StatusOK, loginBody: `{"accessToken":"tok"}`, secretsStatus: http.StatusOK, secretsBody: `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`, nomadStatus: http.StatusInternalServerError, wantErr: true, wantNomadPuts: 1},
		{name: "unreachable client connections", unreachable: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.unreachable {
				badClientServer := NewRouterServer(Config{
					InfisicalURL:          "http://127.0.0.1:1",
					InfisicalClientID:     "cid",
					InfisicalClientSecret: "csec",
					NomadAddr:             "http://127.0.0.1:1",
				}, &http.Client{Timeout: 50 * time.Millisecond})

				if err := badClientServer.SyncSecrets(ctx, "ws", "env"); err == nil {
					t.Errorf("SyncSecrets() expected error on unreachable, got nil")
				}
				if _, err := badClientServer.fetchInfisicalSecrets(ctx, "tok", "ws", "env"); err == nil {
					t.Errorf("fetchInfisicalSecrets() expected error on unreachable, got nil")
				}
				if err := badClientServer.putNomadVariable(ctx, map[string]string{"A": "B"}); err == nil {
					t.Errorf("putNomadVariable() expected error on unreachable, got nil")
				}
				return
			}

			var nomadPutCount atomic.Int32
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/auth/universal-auth/login":
					w.WriteHeader(tc.loginStatus)
					_, _ = w.Write([]byte(tc.loginBody))
				case r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw":
					w.WriteHeader(tc.secretsStatus)
					_, _ = w.Write([]byte(tc.secretsBody))
				case r.Method == http.MethodPut && r.URL.Path == "/v1/var/"+TargetNomadVariable:
					nomadPutCount.Add(1)
					w.WriteHeader(tc.nomadStatus)
				default:
					http.NotFound(w, r)
				}
			}))
			defer mockServer.Close()

			cfg := Config{
				InfisicalURL:          mockServer.URL,
				InfisicalClientID:     "client-id",
				InfisicalClientSecret: "client-secret",
				InfisicalProjectID:    "proj-123",
				InfisicalEnvironment:  "prod",
				NomadAddr:             mockServer.URL,
				NomadToken:            "test-nomad-token",
			}
			if tc.cfgOverride != nil {
				cfg = *tc.cfgOverride
			}

			srv := NewRouterServer(cfg, mockServer.Client())
			err := srv.SyncSecrets(ctx, "proj-123", "prod")
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncSecrets() err = %v, wantErr %v", err, tc.wantErr)
			}
			if gotPuts := nomadPutCount.Load(); gotPuts != tc.wantNomadPuts {
				t.Errorf("nomad PUT count = %d, want %d", gotPuts, tc.wantNomadPuts)
			}
		})
	}
}

// --- 3. GitHub Webhook HTTP Handler & Event Processing ---

func TestHandleGitHubWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, path, wantMetricSubstr string
		body                         io.Reader
		headers                      map[string]string
		river                        *mockRiverInserter
		wantStatus, wantInserted     int
	}{
		{name: "missing X-GitHub-Event header", path: "/api/webhooks/github", body: strings.NewReader(`{}`), wantStatus: http.StatusBadRequest},
		{name: "read error in body", path: "/api/webhooks/github", body: errReader{}, headers: map[string]string{"X-GitHub-Event": "ping"}, wantStatus: http.StatusBadRequest},
		{name: "river client nil", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping"}, wantStatus: http.StatusInternalServerError},
		{name: "river client insert error", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping"}, river: &mockRiverInserter{err: errors.New("simulated database failure")}, wantStatus: http.StatusInternalServerError},
		{name: "api path success", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234"}, river: &mockRiverInserter{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "legacy path success", path: "/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234"}, river: &mockRiverInserter{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "duplicate skipped", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Responsive is better than fast.","hook_id":9999,"repository":{"full_name":"azylman/aerial"}}`), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "dup-gh-delivery-1"}, river: &mockRiverInserter{skipDuplicate: true}, wantStatus: http.StatusOK, wantMetricSubstr: `webhooks_router_unique_skipped_total{kind="github_webhook"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if tc.river != nil {
				srv = NewRouterServer(Config{}, nil, tc.river)
			} else {
				srv = NewRouterServer(Config{}, nil)
			}
			routes := srv.Routes()

			rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, tc.headers)
			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
			if tc.wantInserted > 0 && tc.river != nil {
				if len(tc.river.insertedJobs) != tc.wantInserted {
					t.Fatalf("expected %d inserted jobs, got %d", tc.wantInserted, len(tc.river.insertedJobs))
				}
				var resp map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse json response: %v", err)
				}
				if resp["status"] != "accepted" || resp["event"] != tc.headers["X-GitHub-Event"] || resp["delivery"] != tc.headers["X-GitHub-Delivery"] {
					t.Errorf("unexpected response body: %+v", resp)
				}
				if tc.river.insertedJobs[0].Event != tc.headers["X-GitHub-Event"] || tc.river.insertedJobs[0].Delivery != tc.headers["X-GitHub-Delivery"] {
					t.Errorf("unexpected inserted jobs: %+v", tc.river.insertedJobs)
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

	opts := (GitHubWebhookArgs{Delivery: "test-delivery"}).InsertOpts()
	if !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
		t.Errorf("unexpected GitHubWebhookArgs InsertOpts: %+v", opts)
	}
}

type ghEventCase struct {
	name              string
	event             string
	delivery          string
	payload           string
	reg               *mockPRRegistry
	disp              *mockOutboundDispatcher
	river             *mockRiverInserter
	spyNoGitHubHit    bool
	wantErr           bool
	wantRepo          string
	wantAction        string
	wantPR            int
	wantBranch        string
	wantHeadSHA       string
	wantMergeSHA      string
	wantTargetID      string
	wantSource        string
	wantConclusion    string
	wantSender        string
	wantMergedCalls   int
	wantClosedCalls   int
	wantSyncCalls     int
	wantCIFailedCalls int
	wantCIFailedSHAs  int
	wantBackfillCalls int
	wantConflictCalls int
	wantAdvanceCalls  int
	wantFailVanadis   int
	wantGitPushes     int
	wantPrompts       int
	wantDMs           int
	wantPromptSubstr  string
	wantDMSubstr      string
}

func TestProcessGitHubEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var ghPaths sync.Map
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ghPaths.Store(r.URL.Path, true)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/commits/commit-fallback/pulls"):
			_, _ = w.Write([]byte(`[{"number": 777, "state": "open", "head": {"ref": "feat/fallback", "sha": "commit-fallback"}}]`))
		case strings.Contains(r.URL.Path, "/commits/wf-fallback/pulls"):
			_, _ = w.Write([]byte(`[{"number": 888, "state": "open", "head": {"ref": "feat/wf-branch", "sha": "wf-fallback"}}]`))
		case strings.Contains(r.URL.Path, "/commits/squash123/pulls"):
			_, _ = w.Write([]byte(`[{"number": 999, "state": "closed", "head": {"ref": "feat/squash", "sha": "squash123"}}]`))
		case strings.Contains(r.URL.Path, "/commits/pushsha999/pulls"):
			_, _ = w.Write([]byte(`[{"number": 522, "state": "closed"}]`))
		case strings.Contains(r.URL.Path, "/pulls/543"):
			_, _ = w.Write([]byte(`{"number": 543, "mergeable": false, "mergeable_state": "dirty"}`))
		case strings.Contains(r.URL.Path, "/issues/525/comments"):
			_, _ = w.Write([]byte(`[{"id": 0, "body": "Non-roborev comment"}, {"id": 1, "body": "## roborev: Review Complete (` + "`old0000`" + `)\nOld bug"}, {"id": 2, "body": "## roborev: Combined Review (` + "`robo525`" + `)\nFound SQL injection in query"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(mockServer.Close)

	cases := []ghEventCase{
		{
			name: "ping valid", event: "ping", delivery: "del-ping-1",
			payload:  `{"zen":"Responsive is better than fast.","hook_id":98765,"repository":{"full_name":"azylman/aerial"}}`,
			wantRepo: "azylman/aerial",
		},
		{
			name: "ping malformed", event: "ping", delivery: "del-ping-2",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "pull_request closed and merged", event: "pull_request", delivery: "del-pr-1",
			payload:    `{"action":"closed","number":512,"pull_request":{"number":512,"title":"feat: test pr","head":{"ref":"feat/test","sha":"headsha123"},"base":{"ref":"main","sha":"basesha456"},"merged":true,"merge_commit_sha":"mergesha789"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			wantAction: "closed", wantPR: 512, wantBranch: "feat/test", wantHeadSHA: "headsha123", wantMergeSHA: "mergesha789", wantSource: "pull_request_payload", wantSender: "arcane103",
		},
		{
			name: "pull_request malformed", event: "pull_request", delivery: "del-pr-2",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "check_run with pull_requests array", event: "check_run", delivery: "del-cr-1",
			payload: `{"action":"completed","check_run":{"id":111,"name":"Unit Tests","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  515, wantBranch: "feat/awesome", wantSource: "check_run_payload", wantConclusion: "success",
		},
		{
			name: "check_run fallback via commit SHA lookup", event: "check_run", delivery: "del-cr-2",
			payload: `{"action":"completed","check_run":{"id":222,"name":"Integration Tests","head_sha":"commit-fallback","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  777, wantBranch: "feat/fallback", wantSource: "commit_sha_lookup", wantConclusion: "failure",
		},
		{
			name: "check_run malformed", event: "check_run", delivery: "del-cr-3",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "check_run empty head sha and no prs", event: "check_run", delivery: "del-cr-empty-sha",
			payload: `{"action":"completed","check_run":{"id":991,"name":"Lint","head_sha":"","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
		},
		{
			name: "check_run with no prs and registry resolve error", event: "check_run", delivery: "del-cr-reg-err",
			payload: `{"action":"completed","check_run":{"id":992,"name":"Build","head_sha":"unknown-sha","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolveErr: errors.New("reg err")},
		},
		{
			name: "check_run ci failure transition error", event: "check_run", delivery: "del-cr-fail-err",
			payload: `{"action":"completed","check_run":{"id":993,"name":"Test","head_sha":"headsha111","status":"completed","conclusion":"failure","pull_requests":[{"number":515,"head":{"ref":"feat/fail","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedErr: errors.New("ci err")},
			wantErr: true,
		},
		{
			name: "check_run ci failure by sha transition error", event: "check_run", delivery: "del-cr-fail-sha-err",
			payload: `{"action":"completed","check_run":{"id":994,"name":"Test","head_sha":"headsha994","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedBySHAErr: errors.New("sha err")},
			wantErr: true,
		},
		{
			name: "workflow_run with PR array", event: "workflow_run", delivery: "del-wf-1",
			payload: `{"action":"completed","workflow_run":{"id":333,"name":"Continuous Delivery","head_sha":"headsha333","head_branch":"feat/cd","status":"completed","conclusion":"success","pull_requests":[{"number":516,"head":{"ref":"feat/cd","sha":"headsha333"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  516, wantBranch: "feat/cd", wantSource: "workflow_run_payload",
		},
		{
			name: "workflow_run fallback to commit lookup", event: "workflow_run", delivery: "del-wf-2",
			payload: `{"action":"completed","workflow_run":{"id":444,"name":"Continuous Delivery","head_sha":"wf-fallback","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  888, wantBranch: "feat/wf-branch", wantSource: "commit_sha_lookup",
		},
		{
			name: "workflow_run malformed", event: "workflow_run", delivery: "del-wf-3",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "push to main with commit resolution", event: "push", delivery: "del-push-1",
			payload: `{"ref":"refs/heads/main","before":"prev111","after":"squash123","deleted":false,"head_commit":{"id":"squash123","message":"feat: merged feature (#999)"},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  999, wantMergeSHA: "squash123", wantSource: "commit_sha_lookup",
		},
		{
			name: "push branch deletion", event: "push", delivery: "del-push-2",
			payload:    `{"ref":"refs/heads/feat/temp","before":"temp111","after":"0000000000000000000000000000000000000000","deleted":true,"repository":{"full_name":"azylman/aerial"}}`,
			wantSource: "branch_deleted",
		},
		{
			name: "push tag non-branch ref", event: "push", delivery: "del-push-3",
			payload:    `{"ref":"refs/tags/v1.0.0","after":"tagsha111","repository":{"full_name":"azylman/aerial"}}`,
			wantSource: "non_branch_ref",
		},
		{
			name: "push feature branch", event: "push", delivery: "del-push-feat",
			payload:    `{"ref":"refs/heads/feat/new-feature","before":"featprev","after":"featsha222","head_commit":{"id":"featsha222","message":"feat: new feature"},"repository":{"full_name":"azylman/aerial"}}`,
			wantSource: "non_main_branch",
		},
		{
			name: "push git push dispatcher error", event: "push", delivery: "del-push-disp-err",
			payload: `{"ref":"refs/heads/main","before":"prev111","after":"pushsha999","head_commit":{"id":"pushsha999","message":"feat: push"},"repository":{"full_name":"azylman/aerial"}}`,
			disp:    &mockOutboundDispatcher{gitPushErr: errors.New("hangar down")},
			wantErr: true,
		},
		{
			name: "push malformed", event: "push", delivery: "del-push-4",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "unhandled event", event: "star", delivery: "del-star-1",
			payload: `{"action":"created"}`, wantSource: "unhandled_event",
		},
		{
			name: "pull_request closed and merged transitions registry", event: "pull_request", delivery: "del-pr-merged",
			payload: `{"action":"closed","number":515,"pull_request":{"number":515,"head":{"ref":"feat/decouple","sha":"head111"},"merged":true,"merge_commit_sha":"merge111"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergedTargetID: "target-merged-1"},
			wantPR:  515, wantMergeSHA: "merge111", wantTargetID: "target-merged-1", wantMergedCalls: 1,
		},
		{
			name: "pull_request closed unmerged transitions registry", event: "pull_request", delivery: "del-pr-closed",
			payload: `{"action":"closed","number":516,"pull_request":{"number":516,"head":{"ref":"feat/discard","sha":"head222"},"merged":false},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{closedTargetID: "target-closed-1", failVanadisErr: errors.New("fail err")},
			wantPR:  516, wantTargetID: "target-closed-1", wantClosedCalls: 1, wantFailVanadis: 1,
		},
		{
			name: "pull_request synchronize transitions registry", event: "pull_request", delivery: "del-pr-sync",
			payload: `{"action":"synchronize","number":517,"pull_request":{"number":517,"head":{"ref":"feat/update","sha":"newhead333"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{syncTargetID: "target-sync-1"},
			wantPR:  517, wantHeadSHA: "newhead333", wantTargetID: "target-sync-1", wantSyncCalls: 1,
		},
		{
			name: "check_run failure with PR in payload transitions registry", event: "check_run", delivery: "del-cr-fail",
			payload: `{"action":"completed","check_run":{"name":"Tests","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  518, wantCIFailedCalls: 1,
		},
		{
			name: "check_run failure for roborev enriches prompt with latest comment", event: "check_run", delivery: "del-cr-roborev",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Failed","summary":"Found issues"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 1, wantDMSubstr: "roborev", wantPrompts: 1, wantPromptSubstr: "Found SQL injection in query",
		},
		{
			name: "check_run failure with ciFailCount exceeding cap suppresses prompt and dispatches cap DM", event: "check_run", delivery: "del-cr-cap",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Failed"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true, ciFailCount: 4},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 1, wantDMSubstr: "remediation cap 3/3 reached", wantPrompts: 0,
		},
		{
			name: "check_run failure with ciFailCount error still dispatches prompt", event: "check_run", delivery: "del-cr-failcount-err",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Failed"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true, ciFailCountErr: errors.New("db error")},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 1, wantDMSubstr: "Lint", wantPrompts: 1, wantPromptSubstr: "Lint",
		},
		{
			name: "check_run timed_out with PR in payload transitions registry", event: "check_run", delivery: "del-cr-timeout",
			payload: `{"action":"completed","check_run":{"name":"Tests","head_sha":"checkhead555","status":"completed","conclusion":"timed_out","pull_requests":[{"number":519,"head":{"ref":"feat/test","sha":"checkhead555"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  519, wantCIFailedCalls: 1,
		},
		{
			name: "check_run failure without PR falls back to headSHA transition", event: "check_run", delivery: "del-cr-fallback",
			payload:     `{"action":"completed","check_run":{"name":"Lint","head_sha":"orphansha666","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:         &mockPRRegistry{ciFailedBySHAPRNum: 520, ciFailedBySHATarget: "target-222", ciFailedBySHAUpdated: true},
			wantHeadSHA: "orphansha666", wantCIFailedSHAs: 1,
		},
		{
			name: "workflow_run failure with PR transitions registry", event: "workflow_run", delivery: "del-wf-fail",
			payload: `{"action":"completed","workflow_run":{"name":"CI","head_sha":"wfhead777","status":"completed","conclusion":"failure","pull_requests":[{"number":521,"head":{"ref":"feat/wf","sha":"wfhead777"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  521, wantCIFailedCalls: 1,
		},
		{
			name: "workflow_run timed_out without PR falls back to headSHA transition", event: "workflow_run", delivery: "del-wf-timeout",
			payload:     `{"action":"completed","workflow_run":{"name":"Build Docker","head_sha":"wfhead888","status":"completed","conclusion":"timed_out","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:         &mockPRRegistry{ciFailedBySHAPRNum: 520, ciFailedBySHATarget: "target-222", ciFailedBySHAUpdated: true},
			wantHeadSHA: "wfhead888", wantCIFailedSHAs: 1,
		},
		{
			name: "push to main with resolved PR backfills merge SHA", event: "push", delivery: "del-push-main",
			payload: `{"ref":"refs/heads/main","after":"pushsha999","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"pushsha999","message":"feat: merge pr"}}`,
			reg:     &mockPRRegistry{backfillTargetID: "target-backfill"},
			wantPR:  522, wantMergeSHA: "pushsha999", wantBackfillCalls: 1,
		},
		{
			name: "feature branch push does not backfill", event: "push", delivery: "del-push-feat-reg",
			payload:    `{"ref":"refs/heads/feat/branch-1","after":"featsha888","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"featsha888","message":"feat: wip"}}`,
			reg:        &mockPRRegistry{},
			wantSource: "non_main_branch",
		},
		{
			name: "registry error propagation", event: "pull_request", delivery: "del-err",
			payload: `{"action":"closed","number":515,"pull_request":{"number":515,"head":{"ref":"feat/decouple","sha":"head111"},"merged":true,"merge_commit_sha":"merge111"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("simulated database failure")},
			wantErr: true,
		},
		{
			name: "check_run local PR resolution priority avoids GitHub API", event: "check_run", delivery: "del-cr-local",
			payload:        `{"action":"completed","check_run":{"name":"Integration Tests","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:            &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantSource: "local_pr_registry",
		},
		{
			name: "workflow_run local PR resolution priority avoids GitHub API", event: "workflow_run", delivery: "del-wf-local",
			payload:        `{"action":"completed","workflow_run":{"name":"Build Pipeline","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:            &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantSource: "local_pr_registry",
		},
		{
			name: "push local PR resolution priority avoids GitHub API", event: "push", delivery: "del-push-local",
			payload:        `{"ref":"refs/heads/main","after":"localsha123","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"localsha123","message":"feat: merged commit"}}`,
			reg:            &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantMergeSHA: "localsha123", wantSource: "local_pr_registry", wantBackfillCalls: 1,
		},
		{
			name: "push to main triggers DispatchGitPush", event: "push", delivery: "del-push-main-disp",
			payload: `{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 542, resolveBranch: "main", resolveTargetID: "1555405874565091380"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  542, wantTargetID: "1555405874565091380", wantGitPushes: 1, wantBackfillCalls: 1,
		},
		{
			name: "push to feature branch does not trigger DispatchGitPush", event: "push", delivery: "del-push-feat-disp",
			payload:       `{"ref":"refs/heads/feat/test-branch","after":"featcommit456","repository":{"full_name":"azylman/aerial"}}`,
			reg:           &mockPRRegistry{resolvePRNum: 542, resolveBranch: "main", resolveTargetID: "1555405874565091380"},
			disp:          &mockOutboundDispatcher{},
			wantGitPushes: 0,
		},
		{
			name: "check_run failure with valid snowflake triggers DispatchPrompt", event: "check_run", delivery: "del-cr-fail-prompt",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Build Failed","summary":"Compilation error in brain/main.go:42"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 1, wantDMSubstr: "Service Unit Tests", wantPrompts: 1, wantPromptSubstr: "Service Unit Tests",
		},
		{
			name: "check_run failure duplicate does not trigger prompt", event: "check_run", delivery: "del-cr-dup-prompt",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Build Failed","summary":"Compilation error in brain/main.go:42"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: false},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantPrompts: 0,
		},
		{
			name: "pull_request closed and merged suppresses direct message", event: "pull_request", delivery: "del-pr-merge-disp",
			payload: `{"action":"closed","number":542,"pull_request":{"number":542,"merged":true,"head":{"ref":"feat/batch1","sha":"head542"},"merge_commit_sha":"merge542"},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{mergedTargetID: "1555405874565091380"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  542, wantAction: "closed", wantMergedCalls: 1, wantDMs: 0,
		},
		{
			name: "pull_request auto_merge_disabled dispatches conflict DM and prompt", event: "pull_request", delivery: "del-amd",
			payload:    `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"}},"repository":{"full_name":"azylman/aerial"}}`,
			reg:        &mockPRRegistry{conflictTargetID: "1555405874565091380", conflictUpdated: true},
			disp:       &mockOutboundDispatcher{},
			wantAction: "auto_merge_disabled", wantPR: 543, wantConflictCalls: 1, wantDMs: 1, wantDMSubstr: "Merge conflict detected", wantPrompts: 1, wantPromptSubstr: "Merge conflict detected on PR #543",
		},
		{
			name: "push to main triggers conflict check on open PRs", event: "push", delivery: "del-push-conflict",
			payload:           `{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`,
			reg:               &mockPRRegistry{openPRs: []RegisteredPR{{PRNumber: 543, Branch: "feat/conflict-check", TargetID: "1555405874565091380", HeadSHA: "head543"}}, conflictTargetID: "1555405874565091380", conflictUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantConflictCalls: 1, wantGitPushes: 1, wantDMs: 1, wantDMSubstr: "Merge conflict detected", wantPrompts: 1, wantPromptSubstr: "Merge conflict detected on PR #543",
		},
		{
			name: "merged PR on azylman/mirrormere advances Vanadis chunk", event: "pull_request", delivery: "del-pr-mirrormere",
			payload: `{"action":"closed","number":210,"pull_request":{"number":210,"head":{"ref":"feat/ui","sha":"head210"},"merged":true,"merge_commit_sha":"merge210"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergedTargetID: "target-merged-m"},
			wantPR:  210, wantMergedCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "merged PR advance error", event: "pull_request", delivery: "del-pr-mirrormere-err",
			payload: `{"action":"closed","number":211,"pull_request":{"number":211,"head":{"ref":"feat/ui2","sha":"head211"},"merged":true,"merge_commit_sha":"merge211"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{advanceVanadisErr: errors.New("simulated advance error")},
			wantErr: true,
		},
		{
			name: "closed unmerged PR calls FailVanadisChunkByPR", event: "pull_request", delivery: "del-pr-discard",
			payload: `{"action":"closed","number":212,"pull_request":{"number":212,"head":{"ref":"feat/discard","sha":"head212"},"merged":false},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{closedTargetID: "target-closed-m"},
			wantPR:  212, wantClosedCalls: 1, wantFailVanadis: 1,
		},
		{
			name: "push to azylman/aerial-sidecars advances Vanadis chunk", event: "push", delivery: "del-push-sidecars",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha1","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha1","message":"feat: merged sidecar (#310)"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 310, resolveBranch: "feat/sidecar", resolveTargetID: "target-sidecars", backfillTargetID: "target-sidecars"},
			wantPR:  310, wantMergeSHA: "sidecarsha1", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "push advance error", event: "push", delivery: "del-push-sidecars-err",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha2","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha2","message":"feat: merged sidecar 2 (#311)"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 311, resolveBranch: "feat/sidecar2", resolveTargetID: "target-sidecars-2", advanceVanadisErr: errors.New("simulated push advance error")},
			wantErr: true,
		},
		{
			name: "pull_request closed merged UpdatePRMerged error", event: "pull_request", delivery: "del-pr-merged-err",
			payload: `{"action":"closed","number":215,"pull_request":{"number":215,"head":{"ref":"feat/ui3","sha":"head215"},"merged":true,"merge_commit_sha":"merge215"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("merged db error")},
			wantErr: true,
		},
		{
			name: "pull_request closed unmerged UpdatePRClosedUnmerged error", event: "pull_request", delivery: "del-pr-closed-err",
			payload: `{"action":"closed","number":216,"pull_request":{"number":216,"head":{"ref":"feat/discard2","sha":"head216"},"merged":false},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("closed db error")},
			wantErr: true,
		},
		{
			name: "pull_request synchronize UpdatePRSync error", event: "pull_request", delivery: "del-pr-sync-err",
			payload: `{"action":"synchronize","number":217,"pull_request":{"number":217,"head":{"ref":"feat/update2","sha":"head217"},"merged":false},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("sync db error")},
			wantErr: true,
		},
	}

	for _, tc := range cases {
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
			var disp OutboundDispatcher
			if tc.disp != nil {
				disp = tc.disp
			}
			srv := newTestRouter(Config{GitHubAPIURL: mockServer.URL}, mockServer.Client(), reg, disp, inserters...)
			srv.SetConflictCheckDelay(1 * time.Millisecond)
			srv.onProcessed = func(string, string) {}

			res, err := srv.ProcessGitHubEvent(ctx, tc.event, tc.delivery, []byte(tc.payload))
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessGitHubEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if tc.spyNoGitHubHit {
				if _, hit := ghPaths.Load("/repos/azylman/aerial/commits/localsha123/pulls"); hit {
					t.Fatalf("expected 0 GitHub API hits for localsha123, but endpoint was called")
				}
			}
			if tc.wantRepo != "" && res.Repo != tc.wantRepo {
				t.Errorf("Repo = %q, want %q", res.Repo, tc.wantRepo)
			}
			if tc.wantAction != "" && res.Action != tc.wantAction {
				t.Errorf("Action = %q, want %q", res.Action, tc.wantAction)
			}
			if tc.wantPR != 0 && res.PRNumber != tc.wantPR {
				t.Errorf("PRNumber = %d, want %d", res.PRNumber, tc.wantPR)
			}
			if tc.wantBranch != "" && res.Branch != tc.wantBranch {
				t.Errorf("Branch = %q, want %q", res.Branch, tc.wantBranch)
			}
			if tc.wantHeadSHA != "" && res.HeadSHA != tc.wantHeadSHA {
				t.Errorf("HeadSHA = %q, want %q", res.HeadSHA, tc.wantHeadSHA)
			}
			if tc.wantMergeSHA != "" && res.MergeSHA != tc.wantMergeSHA {
				t.Errorf("MergeSHA = %q, want %q", res.MergeSHA, tc.wantMergeSHA)
			}
			if tc.wantTargetID != "" && res.TargetID != tc.wantTargetID {
				t.Errorf("TargetID = %q, want %q", res.TargetID, tc.wantTargetID)
			}
			if tc.wantSource != "" && res.ResolutionSource != tc.wantSource {
				t.Errorf("ResolutionSource = %q, want %q", res.ResolutionSource, tc.wantSource)
			}
			if tc.wantConclusion != "" && res.Conclusion != tc.wantConclusion {
				t.Errorf("Conclusion = %q, want %q", res.Conclusion, tc.wantConclusion)
			}
			if tc.wantSender != "" && res.Sender != tc.wantSender {
				t.Errorf("Sender = %q, want %q", res.Sender, tc.wantSender)
			}
			if tc.reg != nil {
				if len(tc.reg.mergedCalls) != tc.wantMergedCalls {
					t.Errorf("mergedCalls = %d, want %d", len(tc.reg.mergedCalls), tc.wantMergedCalls)
				}
				if tc.wantMergeSHA != "" && len(tc.reg.mergedCalls) > 0 && tc.reg.mergedCalls[0].mergeSHA != tc.wantMergeSHA {
					t.Errorf("mergedCalls mergeSHA = %q, want %q", tc.reg.mergedCalls[0].mergeSHA, tc.wantMergeSHA)
				}
				if len(tc.reg.closedUnmergedCalls) != tc.wantClosedCalls {
					t.Errorf("closedUnmergedCalls = %d, want %d", len(tc.reg.closedUnmergedCalls), tc.wantClosedCalls)
				}
				if len(tc.reg.syncCalls) != tc.wantSyncCalls {
					t.Errorf("syncCalls = %d, want %d", len(tc.reg.syncCalls), tc.wantSyncCalls)
				}
				if len(tc.reg.ciFailedCalls) != tc.wantCIFailedCalls {
					t.Errorf("ciFailedCalls = %d, want %d", len(tc.reg.ciFailedCalls), tc.wantCIFailedCalls)
				}
				if len(tc.reg.ciFailedBySHACalls) != tc.wantCIFailedSHAs {
					t.Errorf("ciFailedBySHACalls = %d, want %d", len(tc.reg.ciFailedBySHACalls), tc.wantCIFailedSHAs)
				}
				if tc.wantHeadSHA != "" && len(tc.reg.ciFailedBySHACalls) > 0 && tc.reg.ciFailedBySHACalls[0].headSHA != tc.wantHeadSHA {
					t.Errorf("ciFailedBySHACalls headSHA = %q, want %q", tc.reg.ciFailedBySHACalls[0].headSHA, tc.wantHeadSHA)
				}
				if len(tc.reg.backfillCalls) != tc.wantBackfillCalls {
					t.Errorf("backfillCalls = %d, want %d", len(tc.reg.backfillCalls), tc.wantBackfillCalls)
				}
				if tc.wantMergeSHA != "" && len(tc.reg.backfillCalls) > 0 && tc.reg.backfillCalls[0].mergeSHA != tc.wantMergeSHA {
					t.Errorf("backfillCalls mergeSHA = %q, want %q", tc.reg.backfillCalls[0].mergeSHA, tc.wantMergeSHA)
				}
				if len(tc.reg.conflictCalls) != tc.wantConflictCalls {
					t.Errorf("conflictCalls = %d, want %d", len(tc.reg.conflictCalls), tc.wantConflictCalls)
				}
				if len(tc.reg.advanceVanadisCalls) != tc.wantAdvanceCalls {
					t.Errorf("advanceVanadisCalls = %d, want %d", len(tc.reg.advanceVanadisCalls), tc.wantAdvanceCalls)
				}
				if len(tc.reg.failVanadisCalls) != tc.wantFailVanadis {
					t.Errorf("failVanadisCalls = %d, want %d", len(tc.reg.failVanadisCalls), tc.wantFailVanadis)
				}
			}
			if tc.disp != nil {
				gitPushes := tc.disp.GitPushCalls()
				if len(gitPushes) != tc.wantGitPushes {
					t.Errorf("gitPushes = %d, want %d", len(gitPushes), tc.wantGitPushes)
				}
				if tc.wantGitPushes > 0 && len(gitPushes) > 0 {
					if tc.wantTargetID != "" && gitPushes[0].TargetID != tc.wantTargetID {
						t.Errorf("git push TargetID = %q, want %q", gitPushes[0].TargetID, tc.wantTargetID)
					}
					if tc.wantPR != 0 && gitPushes[0].PRNumber != tc.wantPR {
						t.Errorf("git push PRNumber = %d, want %d", gitPushes[0].PRNumber, tc.wantPR)
					}
				}
				prompts := tc.disp.PromptCalls()
				if len(prompts) != tc.wantPrompts {
					t.Errorf("prompts = %d, want %d", len(prompts), tc.wantPrompts)
				}
				if tc.wantPromptSubstr != "" {
					if len(prompts) == 0 || !strings.Contains(prompts[len(prompts)-1].Prompt, tc.wantPromptSubstr) {
						t.Errorf("expected prompt to contain %q, got %+v", tc.wantPromptSubstr, prompts)
					}
				}
				dms := tc.disp.DirectMessageCalls()
				if len(dms) != tc.wantDMs {
					t.Errorf("dms = %d, want %d", len(dms), tc.wantDMs)
				}
				if tc.wantDMSubstr != "" {
					if len(dms) == 0 || !strings.Contains(dms[len(dms)-1].Content, tc.wantDMSubstr) {
						t.Errorf("expected DM to contain %q, got %+v", tc.wantDMSubstr, dms)
					}
				}
			}
		})
	}
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
		{name: "river duplicate skipped increments metric", path: "/api/webhooks/hangar", body: strings.NewReader(`{"event":"deploy_success","job_name":"brain","commit_sha":"abc1234"}`), river: &mockRiverInserter{skipDuplicate: true}, wantStatus: http.StatusAccepted, wantMetricSubstr: `webhooks_router_unique_skipped_total{kind="hangar_webhook"}`},
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
		reg                *mockPRRegistry
		disp               *mockOutboundDispatcher
		wantErr            bool
		wantPR             int
		wantTargetID       string
		wantDeployingCalls int
		wantAdvanceCalls   int
		wantPrompts        int
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
			name:         "deploy_success valid snowflake with channel context",
			evt:          mkHangarEvt("deploy_success", "brain", "azylman/aerial", "sha3d2a", 528, "1555405874565091380", "success", ""),
			reg:          &mockPRRegistry{channelContextMap: map[string][]ChannelMessageContext{"1555405874565091380": {{AuthorName: "arcane103", Content: "Deploy the new router", ResponseText: "Working on it!"}}}},
			disp:         &mockOutboundDispatcher{},
			wantPR:       528,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job brain",
				"[arcane103]: Deploy the new router",
				"[Aerial]: Working on it!",
				"Directive: Based on the recent conversation context above",
			},
		},
		{
			name:         "deploy_rollback valid snowflake",
			evt:          mkHangarEvt("deploy_rollback", "webhooks-router", "azylman/aerial", "shafail", 529, "1555405874565091380", "rollback", strings.Repeat("error detail line\n", 50)),
			reg:          &mockPRRegistry{deployFailedUpdated: true},
			disp:         &mockOutboundDispatcher{},
			wantPR:       529,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Deployment failed and rolled back",
				"Please investigate and fix",
				"```",
			},
		},
		{
			name:         "deploy_failed valid snowflake",
			evt:          mkHangarEvt("deploy_failed", "hangar", "azylman/aerial", "shabroken", 530, "1555405874565091380", "failed", "exit status 1"),
			reg:          &mockPRRegistry{deployFailedUpdated: true},
			disp:         &mockOutboundDispatcher{},
			wantPR:       530,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
			wantPromptSubs: []string{
				"Deployment failed for job hangar",
				"Please investigate and fix",
			},
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
			name:         "sync_failed with PR number",
			evt:          mkHangarEvt("sync_failed", "", "azylman/aerial", "merge542", 542, "1555405874565091380", "failed", "merge conflict in rules/foo.md"),
			reg:          &mockPRRegistry{},
			disp:         &mockOutboundDispatcher{},
			wantPR:       542,
			wantTargetID: "1555405874565091380",
			wantPrompts:  1,
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
			reg:              &mockPRRegistry{},
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
			srv := newTestRouter(Config{}, &http.Client{Transport: &zeroCallTransport{}}, reg, disp)
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
				prompts := tc.disp.PromptCalls()
				if len(prompts) != tc.wantPrompts {
					t.Errorf("prompts = %d, want %d", len(prompts), tc.wantPrompts)
				}
				for _, sub := range tc.wantPromptSubs {
					if len(prompts) == 0 || !strings.Contains(prompts[len(prompts)-1].Prompt, sub) {
						t.Errorf("prompt = %v, want substr %q", prompts, sub)
					}
				}
			}
		})
	}
}

// --- 5. Outbound Dispatcher ---

func TestDefaultOutboundDispatcher(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validSnowflake := "1555405874565091380"

	tests := []struct {
		name               string
		callType           string
		snowflake          string
		statusCode         int
		responseBody       string
		responseHeaderType string
		failCount          int
		maxRetries         int
		cancelDuringRetry  bool
		wantErr            bool
		wantSubstr         string
		wantMatchedJobs    int
	}{
		{
			name:         "DispatchGitPush success 200",
			callType:     "git_push",
			statusCode:   http.StatusOK,
			responseBody: `{"status":"accepted"}`,
		},
		{
			name:            "DispatchImageReady success 200",
			callType:        "image_ready",
			statusCode:      http.StatusOK,
			responseBody:    `{"status":"accepted","matched_jobs":["brain"]}`,
			wantMatchedJobs: 1,
		},
		{
			name:         "DispatchImageReady invalid JSON decode warning",
			callType:     "image_ready",
			statusCode:   http.StatusOK,
			responseBody: `{not json`,
		},
		{
			name:         "DispatchGitPush hangar 404 unmanaged tolerated",
			callType:     "git_push",
			statusCode:   http.StatusNotFound,
			responseBody: `{"status":"error","message":"repository not managed by hangar"}`,
		},
		{
			name:         "DispatchGitPush hangar 404 other returns error",
			callType:     "git_push",
			statusCode:   http.StatusNotFound,
			responseBody: "endpoint not found",
			wantErr:      true,
			wantSubstr:   "hangar git push returned 404",
		},
		{
			name:         "DispatchImageReady hangar 404 not found tolerated",
			callType:     "image_ready",
			statusCode:   http.StatusNotFound,
			responseBody: `{"status":"not_found","message":"no nomad jobs found using image"}`,
		},
		{
			name:         "DispatchGitPush 500 error not tolerated",
			callType:     "git_push",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"internal"}`,
			wantErr:      true,
		},
		{
			name:         "DispatchImageReady 500 error not tolerated",
			callType:     "image_ready",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"internal"}`,
			wantErr:      true,
		},
		{
			name:               "DispatchGitPush HTML 404 rejected",
			callType:           "git_push",
			statusCode:         http.StatusNotFound,
			responseHeaderType: "text/html",
			responseBody:       `<html><title>404 Not Found</title><body>Proxy Error</body></html>`,
			wantErr:            true,
		},
		{
			name:               "DispatchImageReady HTML 404 rejected",
			callType:           "image_ready",
			statusCode:         http.StatusNotFound,
			responseHeaderType: "text/html",
			responseBody:       `<html><title>404 Not Found</title><body>Proxy Error</body></html>`,
			wantErr:            true,
		},
		{
			name:       "DispatchPrompt invalid snowflake",
			callType:   "prompt",
			snowflake:  "not-a-snowflake",
			wantErr:    true,
			wantSubstr: "invalid discord snowflake",
		},
		{
			name:         "DispatchPrompt retry on 500 then success",
			callType:     "prompt",
			snowflake:    validSnowflake,
			failCount:    1,
			maxRetries:   4,
			responseBody: `{"status":"queued"}`,
		},
		{
			name:       "DispatchPrompt exhausting retries on 500",
			callType:   "prompt",
			snowflake:  validSnowflake,
			failCount:  5,
			maxRetries: 2,
			wantErr:    true,
			wantSubstr: "returned status 500",
		},
		{
			name:              "DispatchPrompt context cancellation during retry backoff",
			callType:          "prompt",
			snowflake:         validSnowflake,
			failCount:         5,
			maxRetries:        4,
			cancelDuringRetry: true,
			wantErr:           true,
		},
		{
			name:       "DispatchDirectMessage invalid snowflake",
			callType:   "direct_message",
			snowflake:  "not-a-snowflake",
			wantErr:    true,
			wantSubstr: "invalid discord snowflake",
		},
		{
			name:         "DispatchDirectMessage retry on 500 then success",
			callType:     "direct_message",
			snowflake:    validSnowflake,
			failCount:    1,
			maxRetries:   4,
			responseBody: `{"status":"sent"}`,
		},
		{
			name:         "DispatchDirectMessage 400 client error no retry",
			callType:     "direct_message",
			snowflake:    validSnowflake,
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":"bad request"}`,
			maxRetries:   3,
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				att := attempts.Add(1)
				if int(att) <= tc.failCount {
					http.Error(w, "transient error", http.StatusInternalServerError)
					return
				}
				if tc.responseHeaderType != "" {
					w.Header().Set("Content-Type", tc.responseHeaderType)
				} else {
					w.Header().Set("Content-Type", "application/json")
				}
				status := tc.statusCode
				if status == 0 {
					status = http.StatusOK
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte(tc.responseBody))
			}))
			defer ts.Close()

			disp := NewDefaultOutboundDispatcher(ts.URL, ts.URL, nil)
			delay := 1 * time.Millisecond
			if tc.cancelDuringRetry {
				delay = 30 * time.Millisecond
			}
			retries := tc.maxRetries
			if retries <= 0 {
				retries = 2
			}
			disp.SetRetryConfig(delay, retries)

			testCtx := ctx
			if tc.cancelDuringRetry {
				var cancel context.CancelFunc
				testCtx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}

			var err error
			switch tc.callType {
			case "git_push":
				err = disp.DispatchGitPush(testCtx, GitPushEventRequest{Ref: "refs/heads/main", Commit: "testcommit123"})
			case "image_ready":
				jobs, e := disp.DispatchImageReady(testCtx, ImageReadyEventRequest{Image: "ghcr.io/azylman/aerial-webhooks-router:latest", Digest: "sha256:abcd"})
				err = e
				if tc.wantMatchedJobs > 0 && len(jobs) != tc.wantMatchedJobs {
					t.Errorf("got %d matched jobs, want %d", len(jobs), tc.wantMatchedJobs)
				}
			case "prompt":
				err = disp.DispatchPrompt(testCtx, PromptRequest{ChannelID: tc.snowflake, Prompt: "test prompt message"})
			case "direct_message":
				err = disp.DispatchDirectMessage(testCtx, DirectMessageRequest{ChannelID: tc.snowflake, Content: "test content"})
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("Dispatch (%s) err = %v, wantErr = %v", tc.callType, err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
		})
	}
}

// --- 6. Workflow Run Image Resolution & Dispatch ---

func TestResolveWorkflowRunImages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 7,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 1, Name: "Detect Changed Microservices", Status: "completed", Conclusion: "success"},
			{ID: 2, Name: "Run Service Unit Tests", Status: "completed", Conclusion: "success"},
			{ID: 3, Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"},
			{ID: 4, Name: "Build & Push Images to GHCR (webhooks-router)", Status: "completed", Conclusion: "success"},
			{ID: 5, Name: "Build & Push Images to GHCR (custom-microservice)", Status: "completed", Conclusion: "success"},
			{ID: 6, Name: "Build & Push Images to GHCR (failed-service)", Status: "completed", Conclusion: "failure"},
			{ID: 7, Name: "Build & Push Images to GHCR (running-service)", Status: "in_progress", Conclusion: ""},
		},
	}
	mockSidecarJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 10, Name: "Build & Push Sidecar Images to GHCR (banana)", Status: "completed", Conclusion: "success"},
			{ID: 11, Name: "Build & Push Sidecar Images to GHCR (orin-voice)", Status: "completed", Conclusion: "success"},
		},
	}
	mockMirrormereJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 20, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success"},
			{ID: 21, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success"},
		},
	}
	mockSkippedStepJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{
				ID: 101, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "skipped", Number: 2},
				},
			},
			{
				ID: 102, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "success", Number: 2},
				},
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/actions/runs/12345/jobs"):
			_ = json.NewEncoder(w).Encode(mockJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/54321/jobs"):
			_ = json.NewEncoder(w).Encode(mockSidecarJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/67890/jobs"):
			_ = json.NewEncoder(w).Encode(mockMirrormereJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/77777/jobs"):
			_ = json.NewEncoder(w).Encode(mockSkippedStepJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/403/jobs"):
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
		case strings.Contains(r.URL.Path, "/actions/runs/500/jobs"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/actions/runs/999/jobs"):
			_, _ = w.Write([]byte("malformed json"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)

	srv := NewRouterServer(Config{GitHubAPIURL: ts.URL}, ts.Client())

	cases := []struct {
		name    string
		repo    string
		runID   int64
		want    []string
		wantErr bool
	}{
		{
			name:  "aerial monorepo images",
			repo:  "azylman/aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/aerial-brain:latest",
				"ghcr.io/azylman/aerial-webhooks-router:latest",
				"ghcr.io/azylman/aerial-custom-microservice:latest",
			},
		},
		{
			name:  "unprefixed repo normalization",
			repo:  "aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/aerial-brain:latest",
				"ghcr.io/azylman/aerial-webhooks-router:latest",
				"ghcr.io/azylman/aerial-custom-microservice:latest",
			},
		},
		{
			name:  "aerial-sidecars images",
			repo:  "azylman/aerial-sidecars",
			runID: 54321,
			want: []string{
				"ghcr.io/azylman/aerial-sidecar-banana:latest",
				"ghcr.io/azylman/orin-voice:latest",
			},
		},
		{
			name:  "mirrormere images",
			repo:  "azylman/mirrormere",
			runID: 67890,
			want: []string{
				"ghcr.io/azylman/mirrormere:latest",
				"ghcr.io/azylman/mirrormere-cast-watcher:latest",
			},
		},
		{
			name:  "skipped build step filtering",
			repo:  "azylman/mirrormere",
			runID: 77777,
			want:  []string{"ghcr.io/azylman/mirrormere-cast-watcher:latest"},
		},
		{name: "empty repo", repo: "", runID: 12345, wantErr: true},
		{name: "zero runID", repo: "aerial", runID: 0, wantErr: true},
		{name: "403 rate limit", repo: "aerial", runID: 403, wantErr: true},
		{name: "500 server error", repo: "aerial", runID: 500, wantErr: true},
		{name: "malformed json", repo: "aerial", runID: 999, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			images, err := srv.resolveWorkflowRunImages(ctx, tc.repo, tc.runID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveWorkflowRunImages() err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if len(images) != len(tc.want) {
					t.Fatalf("expected %v, got %v", tc.want, images)
				}
				for i := range tc.want {
					if images[i] != tc.want[i] {
						t.Errorf("image[%d] = %q, want %q", i, images[i], tc.want[i])
					}
				}
			}
		})
	}
}

func TestDispatchWorkflowRunImages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name            string
		isGitPush       bool
		ref             string
		commit          string
		headBranch      string
		conclusion      string
		jobsStatus      int
		jobsBody        string
		noDisp          bool
		gitPushErr      error
		imageReadyErr   error
		deployingErr    error
		wantImageCalls  int
		wantDeployCalls int
		wantErr         bool
	}{
		{
			name:            "main success with matched jobs and transition deploying",
			headBranch:      "main",
			conclusion:      "success",
			jobsStatus:      http.StatusOK,
			jobsBody:        `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`,
			wantImageCalls:  1,
			wantDeployCalls: 1,
		},
		{
			name:       "jobs API 500 error",
			headBranch: "main",
			conclusion: "success",
			jobsStatus: http.StatusInternalServerError,
			jobsBody:   `api error`,
			wantErr:    true,
		},
		{
			name:       "0 resolved images returns nil",
			headBranch: "main",
			conclusion: "success",
			jobsStatus: http.StatusOK,
			jobsBody:   `{"jobs":[]}`,
			wantErr:    false,
		},
		{
			name:           "imageReadyErr dispatch failure returns error",
			headBranch:     "main",
			conclusion:     "success",
			jobsStatus:     http.StatusOK,
			jobsBody:       `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`,
			imageReadyErr:  errors.New("hangar image failed"),
			wantImageCalls: 1,
			wantErr:        true,
		},
		{
			name:            "deployingErr registry error returns error",
			headBranch:      "main",
			conclusion:      "success",
			jobsStatus:      http.StatusOK,
			jobsBody:        `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`,
			deployingErr:    errors.New("db fail"),
			wantImageCalls:  1,
			wantErr:         true,
		},
		{
			name:       "non-main branch is no-op",
			headBranch: "feat/my-feature",
			conclusion: "success",
		},
		{
			name:       "non-success conclusion is no-op",
			headBranch: "main",
			conclusion: "failure",
		},
		{
			name:      "git push nil dispatcher is no-op",
			isGitPush: true,
			noDisp:    true,
			ref:       "refs/heads/main",
			commit:    "sha123",
		},
		{
			name:      "git push non-main ref is no-op",
			isGitPush: true,
			ref:       "refs/heads/feat",
			commit:    "sha123",
		},
		{
			name:      "git push zero sha is no-op",
			isGitPush: true,
			ref:       "refs/heads/main",
			commit:    "0000000000000000000000000000000000000000",
		},
		{
			name:       "git push dispatcher error returns error",
			isGitPush:  true,
			ref:        "refs/heads/main",
			commit:     "sha123",
			gitPushErr: errors.New("hangar down"),
			wantErr:    true,
		},
		{
			name:      "git push successful dispatch",
			isGitPush: true,
			ref:       "refs/heads/main",
			commit:    "sha123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				status := tc.jobsStatus
				if status == 0 {
					status = http.StatusOK
				}
				w.WriteHeader(status)
				body := tc.jobsBody
				if body == "" {
					body = `{"jobs":[]}`
				}
				_, _ = w.Write([]byte(body))
			}))
			defer ts.Close()

			var disp *mockOutboundDispatcher
			if !tc.noDisp {
				disp = &mockOutboundDispatcher{
					gitPushErr:    tc.gitPushErr,
					imageReadyErr: tc.imageReadyErr,
					matchedJobs: map[string][]string{
						"ghcr.io/azylman/aerial-brain:latest": {"brain"},
					},
				}
			}
			reg := &mockPRRegistry{
				deployingErr: tc.deployingErr,
			}
			srv := NewRouterServer(Config{GitHubAPIURL: ts.URL}, ts.Client())
			if disp != nil {
				srv.SetDispatcher(disp)
			}
			srv.SetRegistry(reg)

			var err error
			if tc.isGitPush {
				err = srv.dispatchGitPush(ctx, "azylman/aerial", tc.ref, tc.commit, "1555405874565091380", 1)
			} else {
				err = srv.dispatchWorkflowRunImages(ctx, "azylman/aerial", 12345, tc.headBranch, tc.conclusion, "sha123", "1555405874565091380", 42)
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if disp != nil && len(disp.ImageReadyCalls()) != tc.wantImageCalls {
				t.Errorf("imageReadyCalls = %d, want %d", len(disp.ImageReadyCalls()), tc.wantImageCalls)
			}
			if len(reg.deployingCalls) != tc.wantDeployCalls {
				t.Errorf("deployingCalls = %d, want %d", len(reg.deployingCalls), tc.wantDeployCalls)
			}
		})
	}
}

// --- 7. PR Conflict Checks & SHA/Thread Resolution ---

func TestCheckOpenPRConflicts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name           string
		useNilRegistry bool
		listOpenPRsErr error
		conflictErr    error
		openPRs        []RegisteredPR
		prNumber       int
		prResponses    []string
		prStatusCodes  []int
		wantErr        bool
		wantErrSubstr  string
		wantConflict   bool
		wantConflictPR int
		wantPrompts    int
		wantPromptSubs []string
		wantDMs        int
		wantDMSubs     []string
	}{
		{
			name:           "nil registry is clean no-op",
			useNilRegistry: true,
			wantErr:        false,
		},
		{
			name:           "ListOpenPRs error returns error",
			listOpenPRsErr: errors.New("db list open prs failed"),
			wantErr:        true,
			wantErrSubstr:  "list open prs",
		},
		{
			name:           "conflict detected transitions and dispatches DM and prompt",
			openPRs:        []RegisteredPR{{PRNumber: 543, Branch: "feat/my-feature", TargetID: "1555405874565091380", HeadSHA: "head543"}},
			prNumber:       543,
			prResponses:    []string{`{"number":543,"mergeable":false,"mergeable_state":"dirty"}`},
			wantConflict:   true,
			wantConflictPR: 543,
			wantDMs:        1,
			wantDMSubs:     []string{"Merge conflict detected. Following up..."},
			wantPrompts:    1,
			wantPromptSubs: []string{"Merge conflict detected on PR #543"},
		},
		{
			name:        "clean PR no action taken",
			openPRs:     []RegisteredPR{{PRNumber: 544, Branch: "feat/clean", TargetID: "1555405874565091380", HeadSHA: "head544"}},
			prNumber:    544,
			prResponses: []string{`{"number":544,"mergeable":true,"mergeable_state":"clean"}`},
		},
		{
			name:     "retry null or unknown mergeable state",
			openPRs:  []RegisteredPR{{PRNumber: 545, Branch: "feat/retry", TargetID: "1555405874565091380", HeadSHA: "head545"}},
			prNumber: 545,
			prResponses: []string{
				`{"number":545,"mergeable":null,"mergeable_state":"unknown"}`,
				`{"number":545,"mergeable":false,"mergeable_state":"dirty"}`,
			},
			wantConflict:   true,
			wantConflictPR: 545,
			wantDMs:        1,
			wantPrompts:    1,
		},
		{
			name:          "403 rate-limit error return",
			openPRs:       []RegisteredPR{{PRNumber: 546, Branch: "feat/rate-limit", TargetID: "1555405874565091380", HeadSHA: "head546"}},
			prNumber:      546,
			prStatusCodes: []int{http.StatusForbidden},
			prResponses:   []string{`{"message":"API rate limit exceeded"}`},
			wantErr:       true,
			wantErrSubstr: "rate limit",
		},
		{
			name:          "non-403 fetch error skip PR and return nil",
			openPRs:       []RegisteredPR{{PRNumber: 547, Branch: "feat/not-found", TargetID: "1555405874565091380", HeadSHA: "head547"}},
			prNumber:      547,
			prStatusCodes: []int{http.StatusNotFound},
			prResponses:   []string{`{"message":"Not Found"}`},
			wantErr:       false,
		},
		{
			name:          "500 fetch error skip PR and return nil",
			openPRs:       []RegisteredPR{{PRNumber: 548, Branch: "feat/fetch-500", TargetID: "1555405874565091380", HeadSHA: "head548"}},
			prNumber:      548,
			prStatusCodes: []int{http.StatusInternalServerError},
			prResponses:   []string{`{"message":"Internal Server Error"}`},
			wantErr:       false,
		},
		{
			name:        "conflict transition error logged cleanly without returning error",
			openPRs:     []RegisteredPR{{PRNumber: 549, Branch: "feat/trans-err", TargetID: "1555405874565091380", HeadSHA: "head549"}},
			prNumber:    549,
			prResponses: []string{`{"number":549,"mergeable":false,"mergeable_state":"dirty"}`},
			conflictErr: errors.New("conflict transition err"),
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reqCount atomic.Int32
			apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				idx := int(reqCount.Add(1)) - 1
				code := http.StatusOK
				if idx < len(tc.prStatusCodes) && tc.prStatusCodes[idx] != 0 {
					code = tc.prStatusCodes[idx]
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				body := `{"number":0}`
				if idx < len(tc.prResponses) {
					body = tc.prResponses[idx]
				} else if len(tc.prResponses) > 0 {
					body = tc.prResponses[len(tc.prResponses)-1]
				}
				_, _ = w.Write([]byte(body))
			}))
			defer apiServer.Close()

			var mockReg PRRegistryUpdater
			if !tc.useNilRegistry {
				mockReg = &mockPRRegistry{
					openPRs:          tc.openPRs,
					listOpenPRsErr:   tc.listOpenPRsErr,
					conflictErr:      tc.conflictErr,
					conflictTargetID: "1555405874565091380",
					conflictUpdated:  true,
				}
			}
			mockDisp := &mockOutboundDispatcher{}
			srv := newTestRouter(Config{GitHubAPIURL: apiServer.URL}, nil, mockReg, mockDisp)
			srv.SetConflictCheckDelay(1 * time.Millisecond)

			err := srv.checkOpenPRConflicts(ctx, "azylman/aerial")
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkOpenPRConflicts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantErrSubstr)
			}
			if mReg, ok := mockReg.(*mockPRRegistry); ok && mReg != nil {
				if tc.wantConflict {
					if len(mReg.conflictCalls) != 1 || mReg.conflictCalls[0].prNumber != tc.wantConflictPR {
						t.Errorf("conflictCalls = %+v, want PR %d", mReg.conflictCalls, tc.wantConflictPR)
					}
				} else if len(mReg.conflictCalls) != 0 {
					t.Errorf("unexpected conflictCalls: %+v", mReg.conflictCalls)
				}
			}
			if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
				t.Errorf("got %d DMs, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
			}
			for _, sub := range tc.wantDMSubs {
				if len(mockDisp.DirectMessageCalls()) == 0 || !strings.Contains(mockDisp.DirectMessageCalls()[0].Content, sub) {
					t.Errorf("expected DM containing %q", sub)
				}
			}
			if len(mockDisp.PromptCalls()) != tc.wantPrompts {
				t.Errorf("got %d prompts, want %d", len(mockDisp.PromptCalls()), tc.wantPrompts)
			}
			for _, sub := range tc.wantPromptSubs {
				if len(mockDisp.PromptCalls()) == 0 || !strings.Contains(mockDisp.PromptCalls()[0].Prompt, sub) {
					t.Errorf("expected prompt containing %q", sub)
				}
			}
		})
	}

	srv := NewRouterServer(Config{}, nil)
	if _, err := srv.fetchPRDetails(ctx, "", 0); err == nil {
		t.Error("fetchPRDetails('', 0) expected error, got nil")
	}
}

func TestResolvePRFromCommitSHA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		repo          string
		sha           string
		apiURL        string
		statusCode    int
		responseBody  string
		wantErr       bool
		wantErrSubstr string
		wantPRNum     int
		wantBranch    string
	}{
		{
			name:          "empty repo",
			repo:          "",
			sha:           "sha123",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:    "invalid api url",
			repo:    "azylman/aerial",
			sha:     "sha123",
			apiURL:  "::bad-url::",
			wantErr: true,
		},
		{
			name:          "empty sha",
			repo:          "azylman/aerial",
			sha:           "",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "zero SHA",
			repo:          "azylman/aerial",
			sha:           "0000000000000000000000000000000000000000",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "403 rate limit",
			repo:          "azylman/aerial",
			sha:           "sha403",
			statusCode:    http.StatusForbidden,
			responseBody:  `{"message":"API rate limit exceeded"}`,
			wantErr:       true,
			wantErrSubstr: "rate limit",
		},
		{
			name:          "404 not found",
			repo:          "azylman/aerial",
			sha:           "sha404",
			statusCode:    http.StatusNotFound,
			responseBody:  `{"message":"Not Found"}`,
			wantErr:       true,
			wantErrSubstr: "404",
		},
		{
			name:         "malformed JSON",
			repo:         "azylman/aerial",
			sha:          "shamalformed",
			statusCode:   http.StatusOK,
			responseBody: `{not-an-array`,
			wantErr:      true,
		},
		{
			name:          "empty PR list",
			repo:          "azylman/aerial",
			sha:           "shaempty",
			statusCode:    http.StatusOK,
			responseBody:  `[]`,
			wantErr:       true,
			wantErrSubstr: "no pull requests associated",
		},
		{
			name:         "closed-PR over open-PR priority",
			repo:         "azylman/aerial",
			sha:          "shaclosed",
			statusCode:   http.StatusOK,
			responseBody: `[{"number":10,"state":"open","head":{"ref":"feat/open"}},{"number":20,"state":"closed","head":{"ref":"feat/closed"}}]`,
			wantPRNum:    20,
			wantBranch:   "feat/closed",
		},
		{
			name:         "higher-number selection priority when states match",
			repo:         "azylman/aerial",
			sha:          "shahigher",
			statusCode:   http.StatusOK,
			responseBody: `[{"number":10,"state":"closed","head":{"ref":"feat/ten"}},{"number":30,"state":"closed","head":{"ref":"feat/thirty"}},{"number":20,"state":"closed","head":{"ref":"feat/twenty"}}]`,
			wantPRNum:    30,
			wantBranch:   "feat/thirty",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				code := tc.statusCode
				if code == 0 {
					code = http.StatusOK
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				_, _ = w.Write([]byte(tc.responseBody))
			}))
			defer ts.Close()

			apiURL := ts.URL
			if tc.apiURL != "" {
				apiURL = tc.apiURL
			}
			srv := NewRouterServer(Config{GitHubAPIURL: apiURL, GitHubToken: "secret"}, ts.Client())
			prNum, branch, err := srv.resolvePRFromCommitSHA(ctx, tc.repo, tc.sha)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolvePRFromCommitSHA() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantErrSubstr)
			}
			if prNum != tc.wantPRNum {
				t.Errorf("got prNum = %d, want %d", prNum, tc.wantPRNum)
			}
			if branch != tc.wantBranch {
				t.Errorf("got branch = %q, want %q", branch, tc.wantBranch)
			}
		})
	}
}

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
		channelContextErr   error
		channelContext      []ChannelMessageContext
		wantTargetID        string
		wantErr             bool
		wantPrompts         int
		wantPromptSubs      []string
		wantDMs             int
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
			name:                "dispatchDeploymentSuccessPrompt normal with channel context",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			channelContext: []ChannelMessageContext{
				{AuthorName: "arcane103", Content: "Wrap up the deployment", ResponseText: "Done!"},
			},
			wantPrompts: 1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job brain on azylman/aerial",
				"[arcane103]: Wrap up the deployment",
				"Directive: Based on the recent conversation context above",
			},
		},
		{
			name:                "dispatchDeploymentSuccessPrompt invalid snowflake is no-op",
			isDeploymentSuccess: true,
			targetID:            "invalid-snowflake",
			wantPrompts:         0,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt Vanadis wake suppression true",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			suppressVanadis:     true,
			wantPrompts:         0,
			wantDMs:             0,
		},
		{
			name:                "dispatchDeploymentSuccessPrompt Vanadis wake suppression DB error fallback",
			isDeploymentSuccess: true,
			targetID:            validSnowflake,
			suppressVanadisErr:  errors.New("db check err"),
			channelContextErr:   errors.New("ctx err"),
			wantPrompts:         1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mockReg PRRegistryUpdater
			if !tc.useNilRegistry {
				mReg := &mockPRRegistry{
					resolveTargetID:    tc.targetID,
					suppressVanadisRes: tc.suppressVanadis,
					suppressVanadisErr: tc.suppressVanadisErr,
					channelContextErr:  tc.channelContextErr,
				}
				if len(tc.channelContext) > 0 {
					mReg.channelContextMap = map[string][]ChannelMessageContext{
						tc.targetID: tc.channelContext,
					}
				}
				mockReg = mReg
			}
			mockDisp := &mockOutboundDispatcher{}
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
	srvPG := newTestRouter(Config{}, nil, NewPostgresPRRegistry(pool), &mockOutboundDispatcher{})
	if err := srvPG.dispatchConflictPrompt(ctx, validSnowflake, "azylman/aerial", "main", "sha1", 42); err != nil {
		t.Errorf("dispatchConflictPrompt with pgReg: %v", err)
	}
	if err := srvPG.dispatchCIFailurePrompt(ctx, validSnowflake, "run", "azylman/aerial", "sha1", 42, "fail", "http://ci.test"); err != nil {
		t.Errorf("dispatchCIFailurePrompt with pgReg: %v", err)
	}

	// dispatchAlertWithPrompt with DM error
	srvAlert := newTestRouter(Config{}, nil, nil, &mockOutboundDispatcher{directMessageErr: errors.New("dm failed")})
	if err := srvAlert.dispatchAlertWithPrompt(ctx, validSnowflake, "alert", "dm text", "prompt text"); err != nil {
		t.Errorf("dispatchAlertWithPrompt with dm err: %v", err)
	}
}

// --- 8. River Workers, Crash Alerts & Server Endpoints ---

func TestRiverWorkers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/deployments"):
			_, _ = fmt.Fprint(w, `[{"ID":"dep-1","Status":"successful"}]`)
		case strings.Contains(r.URL.Path, "/allocations"):
			_, _ = w.Write([]byte(`[{"ID":"alloc-1","DesiredStatus":"run","ClientStatus":"running"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(nomadServer.Close)

	tests := []struct {
		name          string
		workerType    string
		nilServer     bool
		downstreamErr bool
		wantErr       bool
	}{
		{name: "GitHubWebhookWorker valid job", workerType: "github"},
		{name: "GitHubWebhookWorker nil server error", workerType: "github", nilServer: true, wantErr: true},
		{name: "GitHubWebhookWorker downstream processing error", workerType: "github", downstreamErr: true, wantErr: true},
		{name: "HangarWebhookWorker valid job", workerType: "hangar"},
		{name: "HangarWebhookWorker nil server error", workerType: "hangar", nilServer: true, wantErr: true},
		{name: "HangarWebhookWorker downstream dispatch error", workerType: "hangar", downstreamErr: true, wantErr: true},
		{name: "NomadEventWorker valid job", workerType: "nomad"},
		{name: "NomadEventWorker nil server error", workerType: "nomad", nilServer: true, wantErr: true},
		{name: "NomadEventWorker downstream prompt error", workerType: "nomad", downstreamErr: true, wantErr: true},
		{name: "NomadCrashAlertWorker valid job", workerType: "crash"},
		{name: "NomadCrashAlertWorker nil server error", workerType: "crash", nilServer: true, wantErr: true},
		{name: "NomadCrashAlertWorker downstream prompt error", workerType: "crash", downstreamErr: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockReg := &mockPRRegistry{
				deployedTargetID: "1555405874565091380",
				deployedPRNum:    552,
				deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				deployedRepo:     "azylman/aerial",
				deployedUpdated:  true,
			}
			mockDisp := &mockOutboundDispatcher{}
			if tc.downstreamErr {
				mockDisp.promptErr = errors.New("503 downstream failure")
			}
			srv := newTestRouter(Config{
				NomadAddr:       nomadServer.URL,
				SystemChannelID: "1555405874565091380",
			}, nil, mockReg, mockDisp)

			var err error
			switch tc.workerType {
			case "github":
				w := &GitHubWebhookWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[GitHubWebhookArgs]{
					JobRow: &rivertype.JobRow{ID: 101},
					Args: GitHubWebhookArgs{
						Event:    "ping",
						Delivery: "del-worker-1",
						Body:     []byte(`{"zen":"Responsive is better than fast.","hook_id":123,"repository":{"full_name":"azylman/aerial"}}`),
					},
				}
				if tc.downstreamErr {
					job.Args.Event = "pull_request"
					job.Args.Body = []byte(`{malformed`)
				}
				err = w.Work(ctx, job)
			case "hangar":
				w := &HangarWebhookWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[HangarWebhookArgs]{
					JobRow: &rivertype.JobRow{ID: 201},
					Args: HangarWebhookArgs{
						Event: HangarDeployEvent{
							Event: "deploy_success", JobName: "brain",
							PRNumber: 558, TargetID: "1555405874565091380", Status: "success",
						},
					},
				}
				err = w.Work(ctx, job)
			case "nomad":
				w := &NomadEventWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[NomadEventArgs]{
					JobRow: &rivertype.JobRow{ID: 301},
					Args: NomadEventArgs{
						Topic:   "Deployment",
						Type:    "DeploymentStatusUpdate",
						Payload: []byte(`{"Deployment":{"ID":"dep-101","JobID":"brain","Status":"successful"}}`),
					},
				}
				err = w.Work(ctx, job)
			case "crash":
				w := &NomadCrashAlertWorker{server: srv}
				if tc.nilServer {
					w.server = nil
				}
				job := &river.Job[NomadCrashAlertArgs]{
					JobRow: &rivertype.JobRow{ID: 401},
					Args: NomadCrashAlertArgs{
						JobID:      "openobserve",
						AllocID:    "alloc-oom",
						Status:     "failed",
						StatusDesc: "OOMKilled exit code 137",
					},
				}
				err = w.Work(ctx, job)
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("Work() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestRiverJobArgsAndUniqueOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         river.JobArgs
		wantKind     string
		wantQueue    string
		wantByArgs   bool
		wantByPeriod time.Duration
		wantAttempts int
	}{
		{
			name:     "GitHubWebhookArgs",
			args:     GitHubWebhookArgs{Delivery: "del-1"},
			wantKind: "github_webhook",
		},
		{
			name:         "HangarWebhookArgs",
			args:         HangarWebhookArgs{IdempotencyKey: "key-1"},
			wantKind:     "hangar_webhook",
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
		},
		{
			name:         "NomadEventArgs",
			args:         NomadEventArgs{Topic: "Deployment"},
			wantKind:     "nomad_event",
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
		},
		{
			name:         "NomadCrashAlertArgs",
			args:         NomadCrashAlertArgs{JobID: "brain"},
			wantKind:     "nomad_crash_alert",
			wantByArgs:   true,
			wantByPeriod: 15 * time.Minute,
		},
		{
			name:         "VanadisExecuteChunkArgs",
			args:         VanadisExecuteChunkArgs{BuildID: "build-01", ChunkIndex: 2},
			wantKind:     "vanadis_execute_chunk",
			wantQueue:    VanadisQueueName,
			wantByArgs:   true,
			wantAttempts: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.args.Kind(); got != tc.wantKind {
				t.Errorf("Kind() = %q, want %q", got, tc.wantKind)
			}
			optsProvider, ok := tc.args.(interface{ InsertOpts() river.InsertOpts })
			if !ok {
				return
			}
			opts := optsProvider.InsertOpts()
			if tc.wantQueue != "" && opts.Queue != tc.wantQueue {
				t.Errorf("Queue = %q, want %q", opts.Queue, tc.wantQueue)
			}
			if tc.wantByArgs && !opts.UniqueOpts.ByArgs {
				t.Errorf("UniqueOpts.ByArgs = false, want true")
			}
			if tc.wantByPeriod > 0 && opts.UniqueOpts.ByPeriod != tc.wantByPeriod {
				t.Errorf("UniqueOpts.ByPeriod = %v, want %v", opts.UniqueOpts.ByPeriod, tc.wantByPeriod)
			}
			if tc.wantAttempts > 0 && opts.MaxAttempts != tc.wantAttempts {
				t.Errorf("MaxAttempts = %d, want %d", opts.MaxAttempts, tc.wantAttempts)
			}
		})
	}
}

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
			name:    "invalid snowflake is clean no-op",
			channel: "invalid-snowflake",
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
	oldCountersFn := writeCountersDirFn
	oldMetaFn := writeMetaDirFn
	t.Cleanup(func() {
		writeCountersDirFn = oldCountersFn
		writeMetaDirFn = oldMetaFn
	})

	RecordUniqueSkipped("")
	RecordUniqueSkipped("test_kind_metric")

	routes := NewRouterServer(Config{Port: "4020"}, nil).Routes()

	tests := []struct {
		name         string
		method       string
		path         string
		gocoverdir   string
		useTempDir   bool
		mockMetaErr  error
		mockCountErr error
		wantCode     int
		wantBodySubs []string
	}{
		{
			name:         "/healthz returns 200 and json",
			method:       http.MethodGet,
			path:         "/healthz",
			wantCode:     http.StatusOK,
			wantBodySubs: []string{`"status":"ok"`, `"service":"webhooks-router"`},
		},
		{
			name:         "/metrics returns 200 and prometheus metrics",
			method:       http.MethodGet,
			path:         "/metrics",
			wantCode:     http.StatusOK,
			wantBodySubs: []string{"webhooks_router_unique_skipped_total", `kind="test_kind_metric"`},
		},
		{
			name:       "/debug/coverage/flush with empty GOCOVERDIR returns 200",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			gocoverdir: "",
			wantCode:   http.StatusOK,
		},
		{
			name:       "/debug/coverage/flush with valid GOCOVERDIR calls flush and returns 200",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			useTempDir: true,
			wantCode:   http.StatusOK,
		},
		{
			name:        "/debug/coverage/flush meta error still returns 200",
			method:      http.MethodGet,
			path:        "/debug/coverage/flush",
			useTempDir:  true,
			mockMetaErr: errors.New("simulated meta flush error"),
			wantCode:    http.StatusOK,
		},
		{
			name:         "/debug/coverage/flush counters error returns 500",
			method:       http.MethodGet,
			path:         "/debug/coverage/flush",
			useTempDir:   true,
			mockCountErr: errors.New("simulated counters flush error"),
			wantCode:     http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.useTempDir {
				t.Setenv("GOCOVERDIR", t.TempDir())
			} else {
				t.Setenv("GOCOVERDIR", tc.gocoverdir)
			}
			writeMetaDirFn = func(dir string) error {
				return tc.mockMetaErr
			}
			writeCountersDirFn = func(dir string) error {
				return tc.mockCountErr
			}

			rec := doHTTPRequest(routes, tc.method, tc.path, nil, nil)
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
	writeJSON(rec, http.StatusOK, make(chan int))
	if rec.Code != http.StatusOK {
		t.Errorf("writeJSON code = %d, want 200", rec.Code)
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
		name             string
		useNilRegistry   bool
		prNumber         int
		advanceRes       *VanadisAdvancementResult
		advanceErr       error
		inserterErr      error
		dispDirectErr    error
		dispWidgetErr    error
		wantErr          bool
		wantInsertedJobs int
		wantPrompts      int
		wantPromptSubs   []string
		wantDMs          int
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
			name:       "registry error returns error",
			prNumber:   10,
			advanceErr: errors.New("db failure"),
			wantErr:    true,
		},
		{
			name:     "mid-run chunk advancement enqueues next chunk in River",
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
			dispDirectErr:    errors.New("transient dm error warning"),
			wantInsertedJobs: 1,
			wantErr:          false,
		},
		{
			name:     "River insert error during mid-run advancement returns error",
			prNumber: 42,
			advanceRes: &VanadisAdvancementResult{
				Matched:        true,
				Advanced:       true,
				BuildID:        "build-mid",
				ThreadID:       validThread,
				NextChunkIndex: 1,
				BuildCompleted: false,
			},
			inserterErr: errors.New("river insert failed"),
			wantErr:     true,
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
			inserter := &mockRiverInserter{
				err: tc.inserterErr,
			}
			srv := newTestRouter(Config{}, nil, reg, disp, inserter)

			err := srv.advanceVanadisChunk(ctx, "azylman/aerial", tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("advanceVanadisChunk() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(inserter.insertedVanadisJobs) != tc.wantInsertedJobs {
				t.Errorf("got %d inserted jobs, want %d", len(inserter.insertedVanadisJobs), tc.wantInsertedJobs)
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

type mockVanadisPgxRow struct {
	scanFn func(dest ...any) error
}

func (r mockVanadisPgxRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type mockVanadisPgxRows struct {
	pgx.Rows
	items     []VanadisChunkSummary
	idx       int
	scanErr   error
	rowsErr   error
	totalRows int
	scanRowFn func(idx int, dest ...any) error
}

func (r *mockVanadisPgxRows) Close()     {}
func (r *mockVanadisPgxRows) Err() error { return r.rowsErr }
func (r *mockVanadisPgxRows) Next() bool {
	limit := len(r.items)
	if r.totalRows > limit {
		limit = r.totalRows
	}
	if r.idx < limit {
		r.idx++
		return true
	}
	return false
}

func (r *mockVanadisPgxRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.scanRowFn != nil {
		return r.scanRowFn(r.idx-1, dest...)
	}
	it := r.items[r.idx-1]
	*(dest[0].(*string)) = it.ID
	*(dest[1].(*int)) = it.ChunkIndex
	*(dest[2].(*string)) = it.Repo
	*(dest[3].(*string)) = it.Title
	*(dest[4].(*string)) = it.Status
	*(dest[5].(*int)) = it.PRNumber
	if len(dest) > 6 {
		if !it.StartedAt.IsZero() {
			*(dest[6].(*sql.NullTime)) = sql.NullTime{Time: it.StartedAt, Valid: true}
		} else {
			*(dest[6].(*sql.NullTime)) = sql.NullTime{}
		}
	}
	if len(dest) > 7 {
		*(dest[7].(*time.Time)) = it.UpdatedAt
	}
	return nil
}

type mockVanadisPgxTx struct {
	pgx.Tx
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	commitErr  error
	rbErr      error
}

func (t *mockVanadisPgxTx) Commit(ctx context.Context) error { return t.commitErr }
func (t *mockVanadisPgxTx) Rollback(ctx context.Context) error {
	if t.rbErr != nil {
		return t.rbErr
	}
	return pgx.ErrTxClosed
}
func (t *mockVanadisPgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if t.queryRowFn != nil {
		return t.queryRowFn(ctx, sql, args...)
	}
	return mockVanadisPgxRow{}
}
func (t *mockVanadisPgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (t *mockVanadisPgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if t.queryFn != nil {
		return t.queryFn(ctx, sql, args...)
	}
	return &mockVanadisPgxRows{}, nil
}

type mockVanadisPgxPool struct {
	beginFn    func(ctx context.Context) (pgx.Tx, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (p *mockVanadisPgxPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if p.beginFn != nil {
		return p.beginFn(ctx)
	}
	return &mockVanadisPgxTx{}, nil
}
func (p *mockVanadisPgxPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if p.queryRowFn != nil {
		return p.queryRowFn(ctx, sql, args...)
	}
	return mockVanadisPgxRow{}
}
func (p *mockVanadisPgxPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if p.execFn != nil {
		return p.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (p *mockVanadisPgxPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.queryFn != nil {
		return p.queryFn(ctx, sql, args...)
	}
	return &mockVanadisPgxRows{}, nil
}

func TestPostgresPRRegistry_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name           string
		op             string
		nilPool        bool
		repo           string
		prNumber       int
		sha            string
		jobName        string
		jobs           []string
		targetID       string
		limit          int
		mockSelectErr  error
		mockScanErr    error
		mockExecErr    error
		mockBeginErr   error
		mockCommitErr  error
		mockRowsErr    error
		rowsAffected   int64
		targetIDReturn string
		branchReturn   string
		mergeSHAReturn string
		metaJSON       string
		totalRows      int
		wantErr        bool
		wantUpdated    bool
		wantTarget     string
		wantPRNum      int
		wantCount      int
	}{
		// Single-row transitions: UpdatePRMerged
		{name: "UpdatePRMerged success", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "UpdatePRMerged not found", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRMerged query error", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "UpdatePRMerged nil pool guard", op: "UpdatePRMerged", nilPool: true, repo: "azylman/aerial", prNumber: 42, sha: "merge123"},
		{name: "UpdatePRMerged empty repo guard", op: "UpdatePRMerged", repo: "", prNumber: 42},
		{name: "UpdatePRMerged prNumber zero guard", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 0},

		// UpdatePRClosedUnmerged
		{name: "UpdatePRClosedUnmerged success", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "UpdatePRClosedUnmerged not found", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRClosedUnmerged db error", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},

		// UpdatePRSync
		{name: "UpdatePRSync success", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "UpdatePRSync not found", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRSync db error", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", mockSelectErr: errors.New("db error"), wantErr: true},

		// BackfillPushMergeSHA
		{name: "BackfillPushMergeSHA success", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "BackfillPushMergeSHA not found", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "BackfillPushMergeSHA db error", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: errors.New("db error"), wantErr: true},

		// IncrementCIFailCount
		{name: "IncrementCIFailCount success", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 42, wantPRNum: 2},
		{name: "IncrementCIFailCount not found", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantPRNum: 0},
		{name: "IncrementCIFailCount db error", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "IncrementCIFailCount nil pool guard", op: "IncrementCIFailCount", nilPool: true, repo: "azylman/aerial", prNumber: 42},
		{name: "IncrementCIFailCount empty repo guard", op: "IncrementCIFailCount", repo: "", prNumber: 42},
		{name: "IncrementCIFailCount prNumber zero guard", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionCIFailed
		{name: "AtomicTransitionCIFailed success", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantUpdated: true},
		{name: "AtomicTransitionCIFailed not found", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionCIFailed db error", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionCIFailed empty repo guard", op: "AtomicTransitionCIFailed", repo: "", prNumber: 42},
		{name: "AtomicTransitionCIFailed prNumber zero guard", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionConflict
		{name: "AtomicTransitionConflict success", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantUpdated: true},
		{name: "AtomicTransitionConflict not found", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionConflict db error", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionConflict empty repo guard", op: "AtomicTransitionConflict", repo: "", prNumber: 42},
		{name: "AtomicTransitionConflict prNumber zero guard", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionDeployFailed
		{name: "AtomicTransitionDeployFailed success", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantUpdated: true},
		{name: "AtomicTransitionDeployFailed not found", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionDeployFailed db error", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionDeployFailed empty repo guard", op: "AtomicTransitionDeployFailed", repo: "", prNumber: 42},
		{name: "AtomicTransitionDeployFailed prNumber zero guard", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionCIFailedByHeadSHA
		{name: "AtomicTransitionCIFailedByHeadSHA success", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantPRNum: 42, wantUpdated: true},
		{name: "AtomicTransitionCIFailedByHeadSHA not found", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "AtomicTransitionCIFailedByHeadSHA db error", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionCIFailedByHeadSHA empty sha guard", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: ""},
		{name: "AtomicTransitionCIFailedByHeadSHA zero sha guard", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "0000000000000000000000000000000000000000"},

		// ResolvePRBySHA
		{name: "ResolvePRBySHA success", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", wantPRNum: 42, branchReturn: "feat/sha", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "ResolvePRBySHA not found", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: pgx.ErrNoRows, wantPRNum: 0},
		{name: "ResolvePRBySHA db error", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "ResolvePRBySHA zero sha guard", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "0000000000000000000000000000000000000000", wantPRNum: 0},

		// ResolvePRByNumber
		{name: "ResolvePRByNumber success", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", branchReturn: "feat/by-num", mergeSHAReturn: "merge123", wantTarget: "1555405874565091380"},
		{name: "ResolvePRByNumber not found", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "ResolvePRByNumber db error", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "ResolvePRByNumber zero pr guard", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 0, wantTarget: ""},

		// TransitionDeploying
		{name: "TransitionDeploying empty jobs guard", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: nil, wantTarget: ""},
		{name: "TransitionDeploying empty repo guard", op: "TransitionDeploying", repo: "", prNumber: 42, jobs: []string{"brain"}},
		{name: "TransitionDeploying zero prNumber guard", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 0, jobs: []string{"brain"}},
		{name: "TransitionDeploying success", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain", "webhooks-router"}, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "TransitionDeploying db error", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// ListOpenPRs
		{name: "ListOpenPRs success", op: "ListOpenPRs", repo: "azylman/aerial", totalRows: 2, wantCount: 2},
		{name: "ListOpenPRs query error", op: "ListOpenPRs", repo: "azylman/aerial", mockSelectErr: errors.New("query err"), wantErr: true},
		{name: "ListOpenPRs scan error", op: "ListOpenPRs", repo: "azylman/aerial", totalRows: 1, mockScanErr: errors.New("scan err"), wantErr: true},
		{name: "ListOpenPRs empty repo guard", op: "ListOpenPRs", repo: ""},

		// ListDeployingPRs
		{name: "ListDeployingPRs success", op: "ListDeployingPRs", totalRows: 2, metaJSON: `{"jobs":["brain"]}`, wantCount: 2},
		{name: "ListDeployingPRs invalid metadata JSON warning still succeeds", op: "ListDeployingPRs", totalRows: 1, metaJSON: `{invalid json`, wantCount: 1},
		{name: "ListDeployingPRs query error", op: "ListDeployingPRs", mockSelectErr: errors.New("query err"), wantErr: true},

		// GetRecentChannelContext
		{name: "GetRecentChannelContext empty targetID guard", op: "GetRecentChannelContext", targetID: "", wantCount: 0},
		{name: "GetRecentChannelContext default limit <= 0", op: "GetRecentChannelContext", targetID: "1555405874565091380", limit: -1, totalRows: 1, wantCount: 1},
		{name: "GetRecentChannelContext query error", op: "GetRecentChannelContext", targetID: "1555405874565091380", limit: 5, mockSelectErr: errors.New("query err"), wantErr: true},

		// AtomicTransitionDeployedByJob
		{name: "AtomicTransitionDeployedByJob empty job guard", op: "AtomicTransitionDeployedByJob", jobName: "", wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob begin tx error", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockBeginErr: errors.New("begin err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob select ErrNoRows", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob select error", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockSelectErr: errors.New("select err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob remaining jobs update success", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob remaining jobs update exec error", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob remaining jobs update commit error", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, mockCommitErr: errors.New("commit err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob final job deployed RowsAffected 1", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", repo: "azylman/aerial", metaJSON: `{"jobs":["node-exporter"]}`, targetIDReturn: "1555405874565091380", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 1, wantUpdated: true, wantTarget: "1555405874565091380"},
		{name: "AtomicTransitionDeployedByJob final job deployed RowsAffected 0", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, rowsAffected: 0, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob final job exec error", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob final job commit error", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, rowsAffected: 1, mockCommitErr: errors.New("commit err"), wantErr: true},

		// AtomicTransitionDeployFailedByJob
		{name: "AtomicTransitionDeployFailedByJob empty job guard", op: "AtomicTransitionDeployFailedByJob", jobName: "", wantUpdated: false},
		{name: "AtomicTransitionDeployFailedByJob select ErrNoRows", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "AtomicTransitionDeployFailedByJob select error", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockSelectErr: errors.New("select err"), wantErr: true},
		{name: "AtomicTransitionDeployFailedByJob exec error", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployFailedByJob RowsAffected 0", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", rowsAffected: 0, wantUpdated: false},
		{name: "AtomicTransitionDeployFailedByJob success", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", repo: "azylman/aerial", targetIDReturn: "1555405874565091380", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 1, wantUpdated: true, wantTarget: "1555405874565091380"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var pool *mockVanadisPgxPool
			if !tc.nilPool {
				pool = &mockVanadisPgxPool{
					queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
						return mockVanadisPgxRow{scanFn: func(dest ...any) error {
							if tc.mockSelectErr != nil {
								return tc.mockSelectErr
							}
							if tc.mockScanErr != nil {
								return tc.mockScanErr
							}
							for _, d := range dest {
								switch ptr := d.(type) {
								case *string:
									*ptr = tc.targetIDReturn
								case *int:
									*ptr = tc.wantPRNum
								case *int64:
									*ptr = 100
								case *[]byte:
									if tc.metaJSON != "" {
										*ptr = []byte(tc.metaJSON)
									}
								}
							}
							if len(dest) >= 2 {
								if sPtr, ok := dest[0].(*string); ok && tc.targetIDReturn != "" {
									*sPtr = tc.targetIDReturn
								}
								if iPtr, ok := dest[1].(*int); ok && tc.wantPRNum != 0 {
									*iPtr = tc.wantPRNum
								}
								if bPtr, ok := dest[1].(*string); ok && tc.branchReturn != "" {
									*bPtr = tc.branchReturn
								}
							}
							if len(dest) >= 3 {
								if tPtr, ok := dest[2].(*string); ok && tc.targetIDReturn != "" {
									*tPtr = tc.targetIDReturn
								}
							}
							if len(dest) >= 4 {
								if mPtr, ok := dest[3].(*string); ok && tc.mergeSHAReturn != "" {
									*mPtr = tc.mergeSHAReturn
								}
							}
							if len(dest) == 5 {
								*(dest[0].(*int64)) = 100
								*(dest[1].(*string)) = tc.targetIDReturn
								*(dest[2].(*int)) = tc.wantPRNum
								*(dest[3].(*string)) = tc.mergeSHAReturn
								*(dest[4].(*string)) = tc.repo
							}
							return nil
						}}
					},
					execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
						if tc.mockExecErr != nil {
							return pgconn.CommandTag{}, tc.mockExecErr
						}
						rowsAff := tc.rowsAffected
						if rowsAff == 0 && tc.wantUpdated {
							rowsAff = 1
						}
						return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAff)), nil
					},
					beginFn: func(qCtx context.Context) (pgx.Tx, error) {
						if tc.mockBeginErr != nil {
							return nil, tc.mockBeginErr
						}
						return &mockVanadisPgxTx{
							commitErr: tc.mockCommitErr,
							execFn: func(txCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
								if tc.mockExecErr != nil {
									return pgconn.CommandTag{}, tc.mockExecErr
								}
								rowsAff := tc.rowsAffected
								if rowsAff == 0 && tc.wantUpdated {
									rowsAff = 1
								}
								return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAff)), nil
							},
							queryRowFn: func(txCtx context.Context, sql string, args ...any) pgx.Row {
								return mockVanadisPgxRow{scanFn: func(dest ...any) error {
									if tc.mockSelectErr != nil {
										return tc.mockSelectErr
									}
									if len(dest) == 6 {
										*(dest[0].(*int64)) = 101
										*(dest[1].(*string)) = tc.targetIDReturn
										*(dest[2].(*int)) = tc.wantPRNum
										*(dest[3].(*string)) = tc.mergeSHAReturn
										*(dest[4].(*string)) = tc.repo
										if tc.metaJSON != "" {
											*(dest[5].(*[]byte)) = []byte(tc.metaJSON)
										} else {
											*(dest[5].(*[]byte)) = []byte("{}")
										}
										return nil
									}
									return nil
								}}
							},
						}, nil
					},
					queryFn: func(qCtx context.Context, sql string, args ...any) (pgx.Rows, error) {
						if tc.mockSelectErr != nil {
							return nil, tc.mockSelectErr
						}
						rows := &mockVanadisPgxRows{
							scanErr:   tc.mockScanErr,
							rowsErr:   tc.mockRowsErr,
							totalRows: tc.totalRows,
						}
						rows.scanRowFn = func(idx int, dest ...any) error {
							if tc.mockScanErr != nil {
								return tc.mockScanErr
							}
							if len(dest) == 4 {
								*(dest[0].(*int)) = 100 + idx
								*(dest[1].(*string)) = "feat/open"
								*(dest[2].(*string)) = "1555405874565091380"
								*(dest[3].(*string)) = "headsha"
								return nil
							}
							if len(dest) == 8 {
								*(dest[0].(*int64)) = int64(200 + idx)
								*(dest[1].(*string)) = "azylman/aerial"
								*(dest[2].(*int)) = 200 + idx
								*(dest[3].(*string)) = "main"
								*(dest[4].(*string)) = "headsha"
								*(dest[5].(*string)) = "mergesha"
								*(dest[6].(*string)) = "1555405874565091380"
								if tc.metaJSON != "" {
									*(dest[7].(*[]byte)) = []byte(tc.metaJSON)
								}
								return nil
							}
							if len(dest) == 3 {
								*(dest[0].(*string)) = "arcane103"
								*(dest[1].(*string)) = "wrap it up"
								*(dest[2].(*string)) = "done"
								return nil
							}
							return nil
						}
						return rows, nil
					},
				}
			}

			reg := NewPostgresPRRegistry(pool)
			if tc.nilPool {
				reg = NewPostgresPRRegistry((*pgxpool.Pool)(nil))
			}

			switch tc.op {
			case "UpdatePRMerged":
				gotTarget, err := reg.UpdatePRMerged(ctx, tc.repo, tc.prNumber, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("UpdatePRMerged err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "UpdatePRClosedUnmerged":
				gotTarget, err := reg.UpdatePRClosedUnmerged(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("UpdatePRClosedUnmerged err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "UpdatePRSync":
				gotTarget, err := reg.UpdatePRSync(ctx, tc.repo, tc.prNumber, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("UpdatePRSync err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "BackfillPushMergeSHA":
				gotTarget, err := reg.BackfillPushMergeSHA(ctx, tc.repo, tc.prNumber, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("BackfillPushMergeSHA err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "IncrementCIFailCount":
				gotCount, err := reg.IncrementCIFailCount(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("IncrementCIFailCount err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotCount != tc.wantPRNum {
					t.Errorf("gotCount = %d, want %d", gotCount, tc.wantPRNum)
				}
			case "AtomicTransitionCIFailed":
				gotTarget, updated, err := reg.AtomicTransitionCIFailed(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionCIFailed err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "AtomicTransitionConflict":
				gotTarget, updated, err := reg.AtomicTransitionConflict(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionConflict err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "AtomicTransitionDeployFailed":
				gotTarget, updated, err := reg.AtomicTransitionDeployFailed(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionDeployFailed err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "AtomicTransitionCIFailedByHeadSHA":
				gotTarget, gotPR, updated, err := reg.AtomicTransitionCIFailedByHeadSHA(ctx, tc.repo, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionCIFailedByHeadSHA err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "ResolvePRBySHA":
				gotPR, gotBranch, gotTarget, err := reg.ResolvePRBySHA(ctx, tc.repo, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ResolvePRBySHA err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
				if gotBranch != tc.branchReturn {
					t.Errorf("gotBranch = %q, want %q", gotBranch, tc.branchReturn)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "ResolvePRByNumber":
				gotTarget, gotBranch, _, _, err := reg.ResolvePRByNumber(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ResolvePRByNumber err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
				if gotBranch != tc.branchReturn {
					t.Errorf("gotBranch = %q, want %q", gotBranch, tc.branchReturn)
				}
			case "TransitionDeploying":
				gotTarget, err := reg.TransitionDeploying(ctx, tc.repo, tc.prNumber, tc.jobs)
				if (err != nil) != tc.wantErr {
					t.Fatalf("TransitionDeploying err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "ListOpenPRs":
				prs, err := reg.ListOpenPRs(ctx, tc.repo)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ListOpenPRs err = %v, wantErr = %v", err, tc.wantErr)
				}
				if len(prs) != tc.wantCount {
					t.Errorf("len(prs) = %d, want %d", len(prs), tc.wantCount)
				}
			case "ListDeployingPRs":
				prs, err := reg.ListDeployingPRs(ctx)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ListDeployingPRs err = %v, wantErr = %v", err, tc.wantErr)
				}
				if len(prs) != tc.wantCount {
					t.Errorf("len(prs) = %d, want %d", len(prs), tc.wantCount)
				}
			case "GetRecentChannelContext":
				ctxs, err := reg.GetRecentChannelContext(ctx, tc.targetID, tc.limit)
				if (err != nil) != tc.wantErr {
					t.Fatalf("GetRecentChannelContext err = %v, wantErr = %v", err, tc.wantErr)
				}
				if len(ctxs) != tc.wantCount {
					t.Errorf("len(ctxs) = %d, want %d", len(ctxs), tc.wantCount)
				}
			case "AtomicTransitionDeployedByJob":
				gotTarget, gotPR, _, _, updated, err := reg.AtomicTransitionDeployedByJob(ctx, tc.jobName)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionDeployedByJob err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
			case "AtomicTransitionDeployFailedByJob":
				gotTarget, gotPR, _, _, updated, err := reg.AtomicTransitionDeployFailedByJob(ctx, tc.jobName)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionDeployFailedByJob err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
			}
		})
	}
}

func TestPostgresPRRegistry_VanadisMethods_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	chunkStart := time.Date(2026, 10, 16, 16, 0, 0, 0, time.UTC)
	chunkEnd := chunkStart.Add(10 * time.Minute)
	twoChunks := []VanadisChunkSummary{
		{ID: "chunk-0", ChunkIndex: 0, Title: "C0", Status: "merged", PRNumber: 10, StartedAt: chunkStart, UpdatedAt: chunkEnd},
		{ID: "chunk-1", ChunkIndex: 1, Repo: "", Title: "C1", Status: "pending", PRNumber: 0},
	}
	allMergedChunks := []VanadisChunkSummary{
		{ID: "chunk-0", ChunkIndex: 0, Title: "C0", Status: "merged", PRNumber: 10, StartedAt: chunkStart, UpdatedAt: chunkEnd},
		{ID: "chunk-1", ChunkIndex: 1, Title: "C1", Status: "merged", PRNumber: 11, StartedAt: chunkStart, UpdatedAt: chunkEnd},
	}

	tests := []struct {
		name               string
		method             string
		useNilPool         bool
		emptyGuard         bool
		beginErr           error
		selectErr          error
		execErr            error
		commitErr          error
		queryErr           error
		chunkStatus        string
		buildStatus        string
		chunkIdx           int
		chunksResult       []VanadisChunkSummary
		queryScanErr       error
		queryRowsErr       error
		wantErr            bool
		wantBool           bool
		wantMatched        bool
		wantAdvanced       bool
		wantBuildCompleted bool
		wantNextChunkIdx   int
	}{
		{
			name:       "AdvanceVanadisChunkByPR typed nil pool returns empty",
			method:     "AdvanceVanadisChunkByPR",
			useNilPool: true,
		},
		{
			name:       "ShouldSuppressVanadisDeployWake typed nil pool returns false",
			method:     "ShouldSuppressVanadisDeployWake",
			useNilPool: true,
			wantBool:   false,
		},
		{
			name:       "FailVanadisChunkByPR typed nil pool returns false",
			method:     "FailVanadisChunkByPR",
			useNilPool: true,
			wantBool:   false,
		},
		{
			name:       "ShouldSuppressVanadisDeployWake empty guard",
			method:     "ShouldSuppressVanadisDeployWake",
			emptyGuard: true,
		},
		{
			name:       "FailVanadisChunkByPR empty guard",
			method:     "FailVanadisChunkByPR",
			emptyGuard: true,
		},
		{
			name:       "AdvanceVanadisChunkByPR empty guard",
			method:     "AdvanceVanadisChunkByPR",
			emptyGuard: true,
		},
		{
			name:     "ShouldSuppressVanadisDeployWake returns true on match",
			method:   "ShouldSuppressVanadisDeployWake",
			wantBool: true,
		},
		{
			name:      "ShouldSuppressVanadisDeployWake returns error on query failure",
			method:    "ShouldSuppressVanadisDeployWake",
			selectErr: errors.New("query err"),
			wantErr:   true,
		},
		{
			name:      "FailVanadisChunkByPR returns false on ErrNoRows",
			method:    "FailVanadisChunkByPR",
			selectErr: pgx.ErrNoRows,
			wantBool:  false,
		},
		{
			name:      "FailVanadisChunkByPR returns error on scan error",
			method:    "FailVanadisChunkByPR",
			selectErr: errors.New("scan err"),
			wantErr:   true,
		},
		{
			name:    "FailVanadisChunkByPR returns error on exec error",
			method:  "FailVanadisChunkByPR",
			execErr: errors.New("exec build fail err"),
			wantErr: true,
		},
		{
			name:     "FailVanadisChunkByPR returns true on success",
			method:   "FailVanadisChunkByPR",
			wantBool: true,
		},
		{
			name:     "AdvanceVanadisChunkByPR begin tx error",
			method:   "AdvanceVanadisChunkByPR",
			beginErr: errors.New("begin tx err"),
			wantErr:  true,
		},
		{
			name:        "AdvanceVanadisChunkByPR ErrNoRows returns Matched=false",
			method:      "AdvanceVanadisChunkByPR",
			selectErr:   pgx.ErrNoRows,
			wantMatched: false,
		},
		{
			name:      "AdvanceVanadisChunkByPR select error returns error",
			method:    "AdvanceVanadisChunkByPR",
			selectErr: errors.New("select chunk for update err"),
			wantErr:   true,
		},
		{
			name:         "AdvanceVanadisChunkByPR non-matching status commits and returns unadvanced",
			method:       "AdvanceVanadisChunkByPR",
			chunkStatus:  "merged",
			buildStatus:  "running",
			wantMatched:  true,
			wantAdvanced: false,
		},
		{
			name:         "AdvanceVanadisChunkByPR mid-run query chunks scan error",
			method:       "AdvanceVanadisChunkByPR",
			chunkStatus:  "pr_open",
			buildStatus:  "running",
			chunkIdx:     0,
			chunksResult: []VanadisChunkSummary{{ID: "chunk-0"}},
			queryScanErr: errors.New("scan chunk err"),
			wantErr:      true,
		},
		{
			name:         "AdvanceVanadisChunkByPR mid-run query chunks rows error",
			method:       "AdvanceVanadisChunkByPR",
			chunkStatus:  "pr_open",
			buildStatus:  "running",
			chunkIdx:     0,
			chunksResult: []VanadisChunkSummary{{ID: "chunk-0"}},
			queryRowsErr: errors.New("rows iter err"),
			wantErr:      true,
		},
		{
			name:             "AdvanceVanadisChunkByPR mid-run advancement success",
			method:           "AdvanceVanadisChunkByPR",
			chunkStatus:      "pr_open",
			buildStatus:      "running",
			chunkIdx:         0,
			chunksResult:     twoChunks,
			wantMatched:      true,
			wantAdvanced:     true,
			wantNextChunkIdx: 1,
		},
		{
			name:               "AdvanceVanadisChunkByPR build completed advancement success",
			method:             "AdvanceVanadisChunkByPR",
			chunkStatus:        "pr_open",
			buildStatus:        "running",
			chunkIdx:           1,
			chunksResult:       allMergedChunks,
			wantMatched:        true,
			wantAdvanced:       true,
			wantBuildCompleted: true,
			wantNextChunkIdx:   -1,
		},
		{
			name:         "AdvanceVanadisChunkByPR build completed commit error",
			method:       "AdvanceVanadisChunkByPR",
			chunkStatus:  "pr_open",
			buildStatus:  "running",
			chunkIdx:     1,
			chunksResult: allMergedChunks,
			commitErr:    errors.New("commit completed err"),
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.useNilPool {
				regNil := NewPostgresPRRegistry((*pgxpool.Pool)(nil))
				switch tc.method {
				case "AdvanceVanadisChunkByPR":
					res, err := regNil.AdvanceVanadisChunkByPR(ctx, "aerial", 1)
					if err != nil || res.Matched {
						t.Fatalf("expected Matched=false, err=nil on nil pool, got res=%+v err=%v", res, err)
					}
				case "ShouldSuppressVanadisDeployWake":
					sup, err := regNil.ShouldSuppressVanadisDeployWake(ctx, "aerial", 1)
					if err != nil || sup != tc.wantBool {
						t.Fatalf("got %v, err=%v", sup, err)
					}
				case "FailVanadisChunkByPR":
					failed, err := regNil.FailVanadisChunkByPR(ctx, "aerial", 1)
					if err != nil || failed != tc.wantBool {
						t.Fatalf("got %v, err=%v", failed, err)
					}
				}
				return
			}

			pool := &mockVanadisPgxPool{
				queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
					return mockVanadisPgxRow{scanFn: func(dest ...any) error {
						if tc.selectErr != nil {
							return tc.selectErr
						}
						if tc.method == "ShouldSuppressVanadisDeployWake" {
							*(dest[0].(*bool)) = tc.wantBool
							return nil
						}
						if tc.method == "FailVanadisChunkByPR" {
							*(dest[0].(*string)) = "build-fail-1"
							return nil
						}
						return nil
					}}
				},
				execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
					if tc.execErr != nil {
						return pgconn.CommandTag{}, tc.execErr
					}
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
				beginFn: func(qCtx context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return &mockVanadisPgxTx{
						commitErr: tc.commitErr,
						queryRowFn: func(txCtx context.Context, sql string, args ...any) pgx.Row {
							return mockVanadisPgxRow{scanFn: func(dest ...any) error {
								if tc.selectErr != nil {
									return tc.selectErr
								}
								if len(dest) >= 11 {
									*(dest[0].(*string)) = fmt.Sprintf("chunk-%d", tc.chunkIdx)
									*(dest[1].(*string)) = "build-100"
									*(dest[2].(*int)) = tc.chunkIdx
									*(dest[3].(*string)) = fmt.Sprintf("Title %d", tc.chunkIdx)
									*(dest[4].(*string)) = tc.chunkStatus
									*(dest[5].(*string)) = "123456789012345678"
									*(dest[6].(*int)) = 2
									*(dest[7].(*string)) = "Test Build Title"
									*(dest[8].(*string)) = "Test Objective"
									*(dest[9].(*string)) = tc.buildStatus
									*(dest[10].(*string)) = "msg-12345"
								}
								return nil
							}}
						},
						queryFn: func(txCtx context.Context, sql string, args ...any) (pgx.Rows, error) {
							if tc.queryErr != nil {
								return nil, tc.queryErr
							}
							return &mockVanadisPgxRows{
								items:   tc.chunksResult,
								scanErr: tc.queryScanErr,
								rowsErr: tc.queryRowsErr,
							}, nil
						},
					}, nil
				},
			}

			reg := NewPostgresPRRegistry(pool)
			repo := "aerial"
			prNum := 10
			if tc.emptyGuard {
				repo = ""
				prNum = 0
			}
			switch tc.method {
			case "ShouldSuppressVanadisDeployWake":
				sup, err := reg.ShouldSuppressVanadisDeployWake(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ShouldSuppressVanadisDeployWake() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr && sup != tc.wantBool {
					t.Errorf("got %v, want %v", sup, tc.wantBool)
				}
			case "FailVanadisChunkByPR":
				failed, err := reg.FailVanadisChunkByPR(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("FailVanadisChunkByPR() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr && failed != tc.wantBool {
					t.Errorf("got %v, want %v", failed, tc.wantBool)
				}
			case "AdvanceVanadisChunkByPR":
				res, err := reg.AdvanceVanadisChunkByPR(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AdvanceVanadisChunkByPR() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr {
					if res.Matched != tc.wantMatched {
						t.Errorf("Matched = %v, want %v", res.Matched, tc.wantMatched)
					}
					if res.Advanced != tc.wantAdvanced {
						t.Errorf("Advanced = %v, want %v", res.Advanced, tc.wantAdvanced)
					}
					if tc.wantAdvanced {
						if res.NextChunkIndex != tc.wantNextChunkIdx {
							t.Errorf("NextChunkIndex = %d, want %d", res.NextChunkIndex, tc.wantNextChunkIdx)
						}
						if res.BuildCompleted != tc.wantBuildCompleted {
							t.Errorf("BuildCompleted = %v, want %v", res.BuildCompleted, tc.wantBuildCompleted)
						}
					}
				}
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
		name           string
		useNilServer   bool
		useMockReg     bool
		mockTargetID   string
		mockBranch     string
		repo           string
		prNumber       int
		statusOverride string
		banner         string
		deployingJobs  []string
		vanadisMatch   bool
		dispErr        error
		queryErr       error
		prQueryErr     error
		wantErr        bool
		wantDispatched bool
		wantWidgetID   string
		wantStatus     string
		wantBanner     string
		wantChannel    string
		wantChunks     int
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
			name:           "non-Postgres registry fallback path",
			useMockReg:     true,
			mockTargetID:   "1555405874565091380",
			mockBranch:     "feat/fallback",
			repo:           "azylman/aerial",
			prNumber:       101,
			statusOverride: "open",
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
			name:           "Postgres standalone PR with mock pool",
			repo:           "azylman/aerial",
			prNumber:       101,
			statusOverride: "merged",
			wantDispatched: true,
			wantWidgetID:   "pr-azylman/aerial-101",
			wantStatus:     "merged",
			wantChannel:    "1555405874565091380",
			wantChunks:     1,
		},
		{
			name:           "Postgres Vanadis chunk match with mock pool",
			repo:           "azylman/aerial",
			prNumber:       102,
			statusOverride: "deployed",
			banner:         "Deploying now",
			deployingJobs:  []string{"job-a"},
			vanadisMatch:   true,
			wantDispatched: true,
			wantWidgetID:   "build-abc",
			wantStatus:     "running",
			wantBanner:     "Deploying now",
			wantChunks:     2,
		},
		{
			name:           "Postgres Vanadis chunk completed status override",
			repo:           "azylman/aerial",
			prNumber:       102,
			statusOverride: "completed",
			vanadisMatch:   true,
			wantDispatched: true,
			wantWidgetID:   "build-abc",
			wantStatus:     "completed",
			wantChunks:     2,
		},
		{
			name:           "Postgres Vanadis chunk failed status override",
			repo:           "azylman/aerial",
			prNumber:       102,
			statusOverride: "failed",
			vanadisMatch:   true,
			wantDispatched: true,
			wantWidgetID:   "build-abc",
			wantStatus:     "failed",
			wantChunks:     2,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := &mockOutboundDispatcher{widgetReturnMsgID: "msg-widget-101", widgetErr: tc.dispErr}

			if tc.useNilServer {
				var nilSrv *RouterServer
				if err := nilSrv.SyncPRWidget(ctx, tc.repo, tc.prNumber, tc.statusOverride, tc.banner, tc.deployingJobs); err != nil {
					t.Fatalf("SyncPRWidget nil server err = %v", err)
				}
				return
			}

			if tc.useMockReg {
				reg := &mockPRRegistry{
					resolveTargetID: tc.mockTargetID,
					resolveBranch:   tc.mockBranch,
				}
				srv := newTestRouter(Config{}, nil, reg, mockDisp)
				err := srv.SyncPRWidget(ctx, tc.repo, tc.prNumber, tc.statusOverride, tc.banner, tc.deployingJobs)
				if (err != nil) != tc.wantErr {
					t.Fatalf("SyncPRWidget fallback err = %v, wantErr = %v", err, tc.wantErr)
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
				}
				return
			}

			pool := &mockVanadisPgxPool{
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
							*dest[6].(*string) = "running"
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
							*dest[2].(*string) = "open"
							*dest[3].(*string) = "old-msg-id"
							if len(dest) > 4 {
								*dest[4].(*time.Time) = prStart
							}
							if len(dest) > 5 {
								*dest[5].(*time.Time) = prUpdate
							}
							return nil
						}}
					}
					return &mockVanadisPgxRow{}
				},
				queryFn: func(qCtx context.Context, sql string, args ...any) (pgx.Rows, error) {
					if tc.queryErr != nil {
						return nil, tc.queryErr
					}
					chunks := []VanadisChunkSummary{
						{ID: "chunk-0", ChunkIndex: 0, Repo: "aerial", Title: "Chunk 0", Status: "merged", PRNumber: 101, StartedAt: chunk0Start, UpdatedAt: chunk0Update},
						{ID: "chunk-1", ChunkIndex: 1, Repo: "", Title: "Chunk 1", Status: "running", PRNumber: 102},
					}
					return &mockVanadisPgxRows{items: chunks}, nil
				},
			}

			srv := newTestRouter(Config{}, nil, NewPostgresPRRegistry(pool), mockDisp)
			err := srv.SyncPRWidget(ctx, tc.repo, tc.prNumber, tc.statusOverride, tc.banner, tc.deployingJobs)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncPRWidget err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantDispatched {
				calls := mockDisp.WidgetCalls()
				if len(calls) == 0 {
					t.Fatalf("expected widget call dispatched")
				}
				last := calls[len(calls)-1]
				if last.ID != tc.wantWidgetID || last.Banner != tc.banner || len(last.Chunks) != tc.wantChunks {
					t.Errorf("unexpected widget request: %+v", last)
				}
			}
		})
	}
}

func TestDefaultOutboundDispatcher_DispatchWidget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		channelID  string
		statusCode int
		body       string
		wantErr    bool
		wantMsgID  string
	}{
		{
			name:      "invalid snowflake returns error",
			channelID: "bad-snowflake",
			wantErr:   true,
		},
		{
			name:       "server 200 OK returns message_id",
			channelID:  "1555405874565091380",
			statusCode: http.StatusOK,
			body:       `{"status":"ok","message_id":"msg-widget-dispatch-ok"}`,
			wantMsgID:  "msg-widget-dispatch-ok",
		},
		{
			name:       "server 400 bad request returns error",
			channelID:  "1555405874565091380",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"bad payload"}`,
			wantErr:    true,
		},
		{
			name:       "server invalid JSON returns error",
			channelID:  "1555405874565091380",
			statusCode: http.StatusOK,
			body:       `{not json`,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/discord/widget" {
					http.NotFound(w, r)
					return
				}
				code := tc.statusCode
				if code == 0 {
					code = http.StatusOK
				}
				w.WriteHeader(code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()

			disp := NewDefaultOutboundDispatcher("", ts.URL, ts.Client())
			msgID, err := disp.DispatchWidget(ctx, StatusWidgetRequest{
				ChannelID: tc.channelID,
				ID:        "widget-1",
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("DispatchWidget() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if msgID != tc.wantMsgID {
				t.Errorf("got msgID = %q, want %q", msgID, tc.wantMsgID)
			}
		})
	}
}


func TestFetchLatestRoboRevComment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/issues/10/comments") {
			_, _ = w.Write([]byte(`[
				{"id": 1, "body": "regular comment"},
				{"id": 2, "body": "## roborev: Review Complete (` + "`sha1111`" + `)\nold report"},
				{"id": 3, "body": "## roborev: Combined Review (` + "`sha2222`" + `)\nfound issue"},
				{"id": 4, "body": "## roborev: Combined Review (` + "`fullsha3333333333`" + `)\nfull sha match"}
			]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(mockServer.Close)

	router := newTestRouter(Config{GitHubAPIURL: mockServer.URL}, mockServer.Client(), &mockPRRegistry{}, &mockOutboundDispatcher{})

	tests := []struct {
		name     string
		srv      *RouterServer
		repo     string
		prNumber int
		headSHA  string
		wantSub  string
	}{
		{name: "nil server guard", srv: nil, repo: "azylman/aerial", prNumber: 10, headSHA: "sha2222", wantSub: ""},
		{name: "empty repo guard", srv: router, repo: "", prNumber: 10, headSHA: "sha2222", wantSub: ""},
		{name: "invalid prNumber guard", srv: router, repo: "azylman/aerial", prNumber: 0, headSHA: "sha2222", wantSub: ""},
		{name: "github api 404", srv: router, repo: "azylman/aerial", prNumber: 99, headSHA: "sha2222", wantSub: ""},
		{name: "matching short sha", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "sha2222extra", wantSub: "found issue"},
		{name: "matching full sha", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "fullsha3333333333", wantSub: "full sha match"},
		{name: "no matching sha", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "nomatchsha", wantSub: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.srv.fetchLatestRoboRevComment(ctx, tc.repo, tc.prNumber, tc.headSHA)
			if tc.wantSub == "" && got != "" {
				t.Fatalf("fetchLatestRoboRevComment got %q, want empty", got)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("fetchLatestRoboRevComment got %q, want substring %q", got, tc.wantSub)
			}
		})
	}
}
