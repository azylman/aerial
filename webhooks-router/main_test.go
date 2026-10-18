package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
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

type mockPRRegistry struct {
	mergedCalls []struct {
		repo     string
		prNumber int
		mergeSHA string
	}
	syncCalls []struct {
		repo     string
		prNumber int
		headSHA  string
	}
	closedUnmergedCalls []struct {
		repo     string
		prNumber int
	}
	ciFailedCalls []struct {
		repo     string
		prNumber int
	}
	ciFailedBySHACalls []struct {
		repo    string
		headSHA string
	}
	backfillCalls []struct {
		repo     string
		prNumber int
		mergeSHA string
	}
	resolveBySHACalls []struct {
		repo string
		sha  string
	}

	mergedTargetID   string
	syncTargetID     string
	closedTargetID   string
	backfillTargetID string

	resolvePRNum    int
	resolveBranch   string
	resolveTargetID string
	resolveErr      error

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

	conflictCalls []struct {
		repo     string
		prNumber int
	}
	conflictTargetID string
	conflictUpdated  bool
	conflictErr      error

	deployingCalls []struct {
		repo     string
		prNumber int
		jobs     []string
	}
	deployingTargetID string
	deployingErr      error

	deployedJobCalls []string
	deployedTargetID string
	deployedPRNum    int
	deployedMergeSHA string
	deployedRepo     string
	deployedUpdated  bool
	deployedErr      error

	deployFailedCalls []struct {
		repo     string
		prNumber int
	}
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

	advanceVanadisCalls []struct {
		repo     string
		prNumber int
	}
	advanceVanadisRes *VanadisAdvancementResult
	advanceVanadisErr error

	suppressVanadisCalls []struct {
		repo     string
		prNumber int
	}
	suppressVanadisRes bool
	suppressVanadisErr error

	failVanadisCalls []struct {
		repo     string
		prNumber int
	}
	failVanadisRes bool
	failVanadisErr error

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
	m.deployingCalls = append(m.deployingCalls, struct {
		repo     string
		prNumber int
		jobs     []string
	}{repo, prNumber, jobs})
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
	m.deployFailedCalls = append(m.deployFailedCalls, struct {
		repo     string
		prNumber int
	}{repo, prNumber})
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
	m.conflictCalls = append(m.conflictCalls, struct {
		repo     string
		prNumber int
	}{repo, prNumber})
	return m.conflictTargetID, m.conflictUpdated, nil
}

func (m *mockPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.mergedCalls = append(m.mergedCalls, struct {
		repo     string
		prNumber int
		mergeSHA string
	}{repo, prNumber, mergeSHA})
	return m.mergedTargetID, nil
}

func (m *mockPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.syncCalls = append(m.syncCalls, struct {
		repo     string
		prNumber int
		headSHA  string
	}{repo, prNumber, headSHA})
	return m.syncTargetID, nil
}

func (m *mockPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.closedUnmergedCalls = append(m.closedUnmergedCalls, struct {
		repo     string
		prNumber int
	}{repo, prNumber})
	return m.closedTargetID, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if err := m.checkErr(m.ciFailedErr); err != nil {
		return "", false, err
	}
	m.ciFailedCalls = append(m.ciFailedCalls, struct {
		repo     string
		prNumber int
	}{repo, prNumber})
	return m.ciFailedTargetID, m.ciFailedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	if err := m.checkErr(m.ciFailedBySHAErr); err != nil {
		return "", 0, false, err
	}
	m.ciFailedBySHACalls = append(m.ciFailedBySHACalls, struct {
		repo    string
		headSHA string
	}{repo, headSHA})
	return m.ciFailedBySHATarget, m.ciFailedBySHAPRNum, m.ciFailedBySHAUpdated, nil
}

func (m *mockPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.backfillCalls = append(m.backfillCalls, struct {
		repo     string
		prNumber int
		mergeSHA string
	}{repo, prNumber, mergeSHA})
	return m.backfillTargetID, nil
}

func (m *mockPRRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	if err := m.checkErr(m.resolveErr); err != nil {
		return 0, "", "", err
	}
	m.resolveBySHACalls = append(m.resolveBySHACalls, struct {
		repo string
		sha  string
	}{repo, sha})
	return m.resolvePRNum, m.resolveBranch, m.resolveTargetID, nil
}

func (m *mockPRRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	if err := m.checkErr(m.resolveErr); err != nil {
		return "", "", "", "", err
	}
	return m.resolveTargetID, m.resolveBranch, "", "", nil
}

func (m *mockPRRegistry) AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	m.advanceVanadisCalls = append(m.advanceVanadisCalls, struct {
		repo     string
		prNumber int
	}{repo, prNumber})
	if err := m.checkErr(m.advanceVanadisErr); err != nil {
		return nil, err
	}
	if m.advanceVanadisRes != nil {
		return m.advanceVanadisRes, nil
	}
	return &VanadisAdvancementResult{}, nil
}

func (m *mockPRRegistry) ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.suppressVanadisCalls = append(m.suppressVanadisCalls, struct {
		repo     string
		prNumber int
	}{repo, prNumber})
	if err := m.checkErr(m.suppressVanadisErr); err != nil {
		return false, err
	}
	return m.suppressVanadisRes, nil
}

func (m *mockPRRegistry) FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.failVanadisCalls = append(m.failVanadisCalls, struct {
		repo     string
		prNumber int
	}{repo, prNumber})
	if err := m.checkErr(m.failVanadisErr); err != nil {
		return false, err
	}
	return m.failVanadisRes, nil
}

type mockOutboundDispatcher struct {
	mu                 sync.Mutex
	gitPushCalls       []GitPushEventRequest
	gitPushCh          chan GitPushEventRequest
	imageReadyCalls    []ImageReadyEventRequest
	imageReadyCh       chan ImageReadyEventRequest
	promptCalls        []PromptRequest
	promptCh           chan PromptRequest
	directMessageCalls []DirectMessageRequest
	directMessageCh    chan DirectMessageRequest
	gitPushErr         error
	imageReadyErr      error
	promptErr          error
	directMessageErr   error
	widgetErr          error
	widgetReturnMsgID  string
	widgetCalls        []StatusWidgetRequest
	widgetCh           chan StatusWidgetRequest
	matchedJobs        map[string][]string
}

func (m *mockOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	m.mu.Lock()
	m.gitPushCalls = append(m.gitPushCalls, req)
	ch := m.gitPushCh
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
	return m.gitPushErr
}

func (m *mockOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error) {
	m.mu.Lock()
	m.imageReadyCalls = append(m.imageReadyCalls, req)
	ch := m.imageReadyCh
	var jobs []string
	if m.matchedJobs != nil {
		jobs = m.matchedJobs[req.Image]
	}
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
	return jobs, m.imageReadyErr
}

func (m *mockOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	m.mu.Lock()
	m.promptCalls = append(m.promptCalls, req)
	ch := m.promptCh
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
	return m.promptErr
}

func (m *mockOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	m.mu.Lock()
	m.directMessageCalls = append(m.directMessageCalls, req)
	ch := m.directMessageCh
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req:
		default:
		}
	}
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
	ch := m.widgetCh
	msgID := m.widgetReturnMsgID
	err := m.widgetErr
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- payload:
		default:
		}
	}
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

func getTestPostgresURL() string {
	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = os.Getenv("DATABASE_URL")
	}
	if pgURL != "" {
		return pgURL
	}
	if _, err := net.LookupHost("aerial-postgres"); err == nil {
		return "postgres://aerial:aerial_secure_pass@aerial-postgres:5432/aerial?sslmode=disable"
	}
	if _, err := net.LookupHost("postgres"); err == nil {
		return "postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable"
	}
	if conn, err := net.DialTimeout("tcp", "127.0.0.1:5432", 150*time.Millisecond); err == nil {
		conn.Close()
		return "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
	}
	if conn, err := net.DialTimeout("tcp", "192.168.1.14:5432", 150*time.Millisecond); err == nil {
		conn.Close()
		return "postgres://aerial:aerial_secure_pass@192.168.1.14:5432/aerial?sslmode=disable"
	}
	return "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
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
			wantPort:      "4020",
			wantInfURL:    "http://alternate:8085",
			wantCID:       "u-cid",
			wantCSec:      "u-csec",
			wantPID:       "w-id",
			wantEnv:       "prod",
			wantNomadAddr: "http://127.0.0.1:4646",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
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
			wantPort:      "4020",
			wantInfURL:    "http://infisical.aerial",
			wantEnv:       "prod",
			wantNomadAddr: "http://127.0.0.1:4646",
			wantGHTok:     "pat123",
			wantGHURL:     "https://api.github.test",
			wantPGURL:     "postgres://custom:pass@db:5432/customdb",
		},
		{
			name: "github token and database url fallback envs",
			env: map[string]string{
				"GITHUB_TOKEN": "tok456",
				"DATABASE_URL": "postgres://dbuser:pass@db:5432/db",
			},
			wantPort:      "4020",
			wantInfURL:    "http://infisical.aerial",
			wantEnv:       "prod",
			wantNomadAddr: "http://127.0.0.1:4646",
			wantGHTok:     "tok456",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://dbuser:pass@db:5432/db",
		},
		{
			name: "system channel id primary env",
			env: map[string]string{
				"SYSTEM_CHANNEL_ID": "123456789012345678",
			},
			wantPort:      "4020",
			wantInfURL:    "http://infisical.aerial",
			wantEnv:       "prod",
			wantNomadAddr: "http://127.0.0.1:4646",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
			wantSysChan:   "123456789012345678",
		},
		{
			name: "discord channel id fallback env",
			env: map[string]string{
				"DISCORD_CHANNEL_ID": "987654321098765432",
			},
			wantPort:      "4020",
			wantInfURL:    "http://infisical.aerial",
			wantEnv:       "prod",
			wantNomadAddr: "http://127.0.0.1:4646",
			wantGHURL:     "https://api.github.com",
			wantPGURL:     "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable",
			wantSysChan:   "987654321098765432",
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
			if tc.wantPort != "" && cfg.Port != tc.wantPort {
				t.Errorf("Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if tc.wantInfURL != "" && cfg.InfisicalURL != tc.wantInfURL {
				t.Errorf("InfisicalURL = %q, want %q", cfg.InfisicalURL, tc.wantInfURL)
			}
			if tc.wantCID != "" && cfg.InfisicalClientID != tc.wantCID {
				t.Errorf("InfisicalClientID = %q, want %q", cfg.InfisicalClientID, tc.wantCID)
			}
			if tc.wantCSec != "" && cfg.InfisicalClientSecret != tc.wantCSec {
				t.Errorf("InfisicalClientSecret = %q, want %q", cfg.InfisicalClientSecret, tc.wantCSec)
			}
			if tc.wantPID != "" && cfg.InfisicalProjectID != tc.wantPID {
				t.Errorf("InfisicalProjectID = %q, want %q", cfg.InfisicalProjectID, tc.wantPID)
			}
			if tc.wantEnv != "" && cfg.InfisicalEnvironment != tc.wantEnv {
				t.Errorf("InfisicalEnvironment = %q, want %q", cfg.InfisicalEnvironment, tc.wantEnv)
			}
			if tc.wantNomadAddr != "" && cfg.NomadAddr != tc.wantNomadAddr {
				t.Errorf("NomadAddr = %q, want %q", cfg.NomadAddr, tc.wantNomadAddr)
			}
			if tc.wantNomadTok != "" && cfg.NomadToken != tc.wantNomadTok {
				t.Errorf("NomadToken = %q, want %q", cfg.NomadToken, tc.wantNomadTok)
			}
			if tc.wantGHTok != "" && cfg.GitHubToken != tc.wantGHTok {
				t.Errorf("GitHubToken = %q, want %q", cfg.GitHubToken, tc.wantGHTok)
			}
			if tc.wantGHURL != "" && cfg.GitHubAPIURL != tc.wantGHURL {
				t.Errorf("GitHubAPIURL = %q, want %q", cfg.GitHubAPIURL, tc.wantGHURL)
			}
			if tc.wantPGURL != "" && cfg.PostgresURL != tc.wantPGURL {
				t.Errorf("PostgresURL = %q, want %q", cfg.PostgresURL, tc.wantPGURL)
			}
			if tc.wantSysChan != "" && cfg.SystemChannelID != tc.wantSysChan {
				t.Errorf("SystemChannelID = %q, want %q", cfg.SystemChannelID, tc.wantSysChan)
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
			if tc.wantPort != "" && cfg.Port != tc.wantPort {
				t.Errorf("Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if tc.wantInfURL != "" && cfg.InfisicalURL != tc.wantInfURL {
				t.Errorf("InfisicalURL = %q, want %q", cfg.InfisicalURL, tc.wantInfURL)
			}
			if tc.wantEnv != "" && cfg.InfisicalEnvironment != tc.wantEnv {
				t.Errorf("InfisicalEnvironment = %q, want %q", cfg.InfisicalEnvironment, tc.wantEnv)
			}
			if tc.wantNomadAddr != "" && cfg.NomadAddr != tc.wantNomadAddr {
				t.Errorf("NomadAddr = %q, want %q", cfg.NomadAddr, tc.wantNomadAddr)
			}
			if tc.wantHangarURL != "" && cfg.HangarURL != tc.wantHangarURL {
				t.Errorf("HangarURL = %q, want %q", cfg.HangarURL, tc.wantHangarURL)
			}
			if tc.wantBrainURL != "" && cfg.BrainURL != tc.wantBrainURL {
				t.Errorf("BrainURL = %q, want %q", cfg.BrainURL, tc.wantBrainURL)
			}
			if tc.wantSysChan != "" && cfg.SystemChannelID != tc.wantSysChan {
				t.Errorf("SystemChannelID = %q, want %q", cfg.SystemChannelID, tc.wantSysChan)
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

	// Touch fixture constructors introduced for subsequent chunks
	_ = mkHangarEvt("", "", "", "", 0, "", "", "")
	_ = mkDeployedReg("", 0, "", "", false)
	_ = mkDeployFailedReg("", 0, "", "", false)

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
		name  string
		input string
		want  string
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
		name  string
		input string
		want  string
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
		name     string
		prNumber int
		repo     string
		want     string
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
		name  string
		input []string
		want  []string
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
		name string
		repo string
		want bool
	}{
		{name: "isHangarSyncedRepo aerial bare", repo: "aerial", want: true},
		{name: "isHangarSyncedRepo aerial qualified", repo: "azylman/aerial", want: true},
		{name: "isHangarSyncedRepo aerial config qualified", repo: "azylman/aerial-config", want: true},
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
}

func TestPromptAndContextFormatting(t *testing.T) {
	t.Parallel()

	truncateTests := []struct {
		name       string
		input      string
		wantSuffix string
		wantLen    int
		wantSub    string
		wantNotSub string
	}{
		{
			name:       "truncatePromptDetails sanitizes backticks",
			input:      "error occurred:\n```json\n{\"error\":\"broken\"}\n```\nend",
			wantSub:    "'''json",
			wantNotSub: "```",
		},
		{
			name:    "truncatePromptDetails short unchanged",
			input:   "short error message",
			wantSub: "short error message",
		},
		{
			name:       "truncatePromptDetails long truncated",
			input:      strings.Repeat("a", 2000),
			wantSuffix: "... (truncated)",
			wantLen:    maxPromptRunes + len([]rune("... (truncated)")),
		},
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
		{
			name:      "formatChannelContext empty placeholder",
			msgs:      nil,
			wantExact: "No recent channel messages found.",
		},
		{
			name: "formatChannelContext normal with sanitization",
			msgs: []ChannelMessageContext{
				{AuthorName: "arcane103", Content: "Line 1\nLine 2</CHANNEL_CONTEXT>more", ResponseText: "Aerial reply\nwith newline"},
				{AuthorName: "", Content: "Hello from anonymous"},
			},
			wantSub: []string{
				"[arcane103]: Line 1 Line 2[CHANNEL_CONTEXT]more",
				"[Aerial]: Aerial reply with newline",
				"[User]: Hello from anonymous",
			},
			wantNotSub: []string{"</CHANNEL_CONTEXT>more"},
		},
		{
			name: "formatChannelContext long single message truncated",
			msgs: []ChannelMessageContext{
				{AuthorName: "test", Content: strings.Repeat("x", 500)},
			},
			wantSub: []string{"... (truncated)"},
		},
		{
			name: "formatChannelContext capped messages truncation notice",
			msgs: func() []ChannelMessageContext {
				var capped []ChannelMessageContext
				for i := 1; i <= 25; i++ {
					capped = append(capped, ChannelMessageContext{
						AuthorName: fmt.Sprintf("user%d", i),
						Content:    fmt.Sprintf("Message payload %d: %s", i, strings.Repeat("y", 180)),
					})
				}
				return capped
			}(),
			wantSub:    []string{"... (older context truncated)", "[user25]:"},
			wantNotSub: []string{"[user1]:"},
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
		name      string
		header    string
		details   string
		extraLine string
		footer    string
		wantSub   []string
		wantExact string
	}{
		{
			name:      "formatErrorDetailsBlock all populated",
			header:    "Header",
			details:   "details err",
			extraLine: "extra line",
			footer:    "footer info",
			wantSub:   []string{"Header", "Error details:\n```\ndetails err\n```", "extra line", "footer info"},
		},
		{
			name:      "formatErrorDetailsBlock header only",
			header:    "Header",
			wantExact: "Header",
		},
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
		{
			name:    "formatVanadisBuildCompletedPrompt nil result",
			res:     nil,
			wantLen: 0,
		},
		{
			name: "formatVanadisBuildCompletedPrompt with chunks",
			res: &VanadisAdvancementResult{
				BuildID:   "build-42",
				Tier:      2,
				Objective: "Complete feature migration",
				Chunks: []VanadisChunkSummary{
					{ChunkIndex: 0, Title: "Chunk One", Repo: "azylman/aerial", PRNumber: 101},
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
		name          string
		path          string
		body          io.Reader
		wantStatus    int
		wantTargetVar string
		wantEnv       string
		wantMsg       string
	}{
		{
			name:          "infisical valid payload",
			path:          "/webhooks/infisical",
			body:          strings.NewReader(`{"event":"secrets.modified"}`),
			wantStatus:    http.StatusAccepted,
			wantTargetVar: TargetNomadVariable,
		},
		{
			name:       "infisical malformed json",
			path:       "/webhooks/infisical",
			body:       strings.NewReader(`{not valid json}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "infisical body read error",
			path:       "/webhooks/infisical",
			body:       errReader{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "generic standard sync",
			path:          "/webhooks/generic",
			body:          strings.NewReader(`{"environment":"prod"}`),
			wantStatus:    http.StatusAccepted,
			wantTargetVar: TargetNomadVariable,
		},
		{
			name:          "generic direct secrets map",
			path:          "/webhooks/generic",
			body:          strings.NewReader(`{"secrets":{"KEY1":"VAL1"}}`),
			wantStatus:    http.StatusAccepted,
			wantTargetVar: TargetNomadVariable,
			wantMsg:       "Direct secret sync scheduled",
		},
		{
			name:          "generic explicit workspace and env",
			path:          "/webhooks/generic",
			body:          strings.NewReader(`{"workspaceId":"custom-ws","environment":"dev"}`),
			wantStatus:    http.StatusAccepted,
			wantTargetVar: TargetNomadVariable,
			wantEnv:       "dev",
		},
		{
			name:       "generic bad json",
			path:       "/webhooks/generic",
			body:       strings.NewReader(`{invalid`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "generic body read error",
			path:       "/webhooks/generic",
			body:       errReader{},
			wantStatus: http.StatusBadRequest,
		},
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
		{
			name:          "successful full sync flow",
			loginStatus:   http.StatusOK,
			loginBody:     `{"accessToken":"mock-jwt-token"}`,
			secretsStatus: http.StatusOK,
			secretsBody:   `{"secrets":[{"secretKey":"API_KEY","secretValue":"secret123"}]}`,
			nomadStatus:   http.StatusOK,
			wantErr:       false,
			wantNomadPuts: 1,
		},
		{
			name:        "missing infisical credentials",
			cfgOverride: &Config{},
			wantErr:     true,
		},
		{
			name:        "login 500 server error",
			loginStatus: http.StatusInternalServerError,
			loginBody:   "server error",
			wantErr:     true,
		},
		{
			name:        "login invalid json response",
			loginStatus: http.StatusOK,
			loginBody:   "{invalid-json",
			wantErr:     true,
		},
		{
			name:        "login empty token string",
			loginStatus: http.StatusOK,
			loginBody:   `{"accessToken":""}`,
			wantErr:     true,
		},
		{
			name:          "login fallback token field",
			loginStatus:   http.StatusOK,
			loginBody:     `{"token":"fallback-token"}`,
			secretsStatus: http.StatusOK,
			secretsBody:   `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`,
			nomadStatus:   http.StatusOK,
			wantErr:       false,
			wantNomadPuts: 1,
		},
		{
			name:          "secrets fetch 502 bad gateway",
			loginStatus:   http.StatusOK,
			loginBody:     `{"accessToken":"tok"}`,
			secretsStatus: http.StatusBadGateway,
			wantErr:       true,
		},
		{
			name:          "secrets fetch invalid json",
			loginStatus:   http.StatusOK,
			loginBody:     `{"accessToken":"tok"}`,
			secretsStatus: http.StatusOK,
			secretsBody:   "{invalid-secrets",
			wantErr:       true,
		},
		{
			name:          "nomad put 201 created",
			loginStatus:   http.StatusOK,
			loginBody:     `{"accessToken":"tok"}`,
			secretsStatus: http.StatusOK,
			secretsBody:   `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`,
			nomadStatus:   http.StatusCreated,
			wantErr:       false,
			wantNomadPuts: 1,
		},
		{
			name:          "nomad put 204 no content",
			loginStatus:   http.StatusOK,
			loginBody:     `{"accessToken":"tok"}`,
			secretsStatus: http.StatusOK,
			secretsBody:   `{"secrets":[{"secretKey":"A","secretValue":"B"}]}`,
			nomadStatus:   http.StatusNoContent,
			wantErr:       false,
			wantNomadPuts: 1,
		},
		{
			name:          "nomad put 500 internal server error",
			loginStatus:   http.StatusOK,
			loginBody:     `{"accessToken":"tok"}`,
			secretsStatus: http.StatusOK,
			secretsBody:   `{"secrets":[{"secretKey":"K","secretValue":"V"}]}`,
			nomadStatus:   http.StatusInternalServerError,
			wantErr:       true,
			wantNomadPuts: 1,
		},
		{
			name:        "unreachable client connections",
			unreachable: true,
			wantErr:     true,
		},
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
		name             string
		path             string
		body             io.Reader
		headers          map[string]string
		river            *mockRiverInserter
		wantStatus       int
		wantInserted     int
		wantMetricSubstr string
	}{
		{
			name:       "missing X-GitHub-Event header",
			path:       "/api/webhooks/github",
			body:       strings.NewReader(`{}`),
			headers:    nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "read error in body",
			path:       "/api/webhooks/github",
			body:       errReader{},
			headers:    map[string]string{"X-GitHub-Event": "ping"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "river client nil",
			path:       "/api/webhooks/github",
			body:       strings.NewReader(`{"zen":"Keep it simple"}`),
			headers:    map[string]string{"X-GitHub-Event": "ping"},
			river:      nil,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "river client insert error",
			path:       "/api/webhooks/github",
			body:       strings.NewReader(`{"zen":"Keep it simple"}`),
			headers:    map[string]string{"X-GitHub-Event": "ping"},
			river:      &mockRiverInserter{err: errors.New("simulated database failure")},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:         "api path success",
			path:         "/api/webhooks/github",
			body:         strings.NewReader(`{"zen":"Keep it simple"}`),
			headers:      map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234"},
			river:        &mockRiverInserter{},
			wantStatus:   http.StatusOK,
			wantInserted: 1,
		},
		{
			name:         "legacy path success",
			path:         "/webhooks/github",
			body:         strings.NewReader(`{"zen":"Keep it simple"}`),
			headers:      map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234"},
			river:        &mockRiverInserter{},
			wantStatus:   http.StatusOK,
			wantInserted: 1,
		},
		{
			name:             "duplicate skipped",
			path:             "/api/webhooks/github",
			body:             strings.NewReader(`{"zen":"Responsive is better than fast.","hook_id":9999,"repository":{"full_name":"azylman/aerial"}}`),
			headers:          map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "dup-gh-delivery-1"},
			river:            &mockRiverInserter{skipDuplicate: true},
			wantStatus:       http.StatusOK,
			wantInserted:     0,
			wantMetricSubstr: `webhooks_router_unique_skipped_total{kind="github_webhook"}`,
		},
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
			payload:  `{"action":"closed","number":512,"pull_request":{"number":512,"title":"feat: test pr","head":{"ref":"feat/test","sha":"headsha123"},"base":{"ref":"main","sha":"basesha456"},"merged":true,"merge_commit_sha":"mergesha789"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			wantRepo: "azylman/aerial", wantAction: "closed", wantPR: 512, wantBranch: "feat/test", wantHeadSHA: "headsha123", wantMergeSHA: "mergesha789", wantSource: "pull_request_payload", wantSender: "arcane103",
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
			reg:     &mockPRRegistry{closedTargetID: "target-closed-1"},
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
		name                string
		path                string
		body                io.Reader
		reg                 *mockPRRegistry
		river               *mockRiverInserter
		wantStatus          int
		wantJobName         string
		wantTargetID        string
		wantPR              int
		wantResolveSHACalls int
		wantRiverJobs       int
		wantMetricSubstr    string
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
					if tc.wantPR != 0 {
						prVal, _ := resp["pr_number"].(float64)
						if int(prVal) != tc.wantPR {
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
		wantDMs            int
		wantPrompts        int
		wantDMSubstr       string
		wantPromptSubs     []string
	}{
		{
			name: "deploy_started without snowflake does not call downstream",
			evt:  mkHangarEvt("deploy_started", "brain", "azylman/aerial", "sha-test", 0, "", "started", ""),
		},
		{
			name: "deploy_success without snowflake does not call downstream",
			evt:  mkHangarEvt("deploy_success", "brain", "azylman/aerial", "sha-test", 0, "", "success", ""),
		},
		{
			name: "deploy_failed without snowflake does not call downstream",
			evt:  mkHangarEvt("deploy_failed", "brain", "azylman/aerial", "sha-test", 0, "", "failed", ""),
		},
		{
			name: "deploy_rollback without snowflake does not call downstream",
			evt:  mkHangarEvt("deploy_rollback", "brain", "azylman/aerial", "sha-test", 0, "", "rollback", ""),
		},
		{
			name:               "deploy_started transitions registry",
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

	t.Run("AllMethodsAndRetries", func(t *testing.T) {
		t.Parallel()
		var receivedGitPush GitPushEventRequest
		var receivedImageReady ImageReadyEventRequest
		var promptAttempts, directAttempts int
		var receivedPrompt PromptRequest
		var receivedDirect DirectMessageRequest

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/events/git_push":
				_ = json.NewDecoder(r.Body).Decode(&receivedGitPush)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"accepted"}`))
			case "/events/image_ready":
				_ = json.NewDecoder(r.Body).Decode(&receivedImageReady)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"accepted"}`))
			case "/prompt":
				promptAttempts++
				if promptAttempts == 1 {
					http.Error(w, "transient error", http.StatusInternalServerError)
					return
				}
				_ = json.NewDecoder(r.Body).Decode(&receivedPrompt)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"queued"}`))
			case "/discord/message":
				directAttempts++
				if directAttempts == 1 {
					http.Error(w, "transient error", http.StatusInternalServerError)
					return
				}
				_ = json.NewDecoder(r.Body).Decode(&receivedDirect)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"sent"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer ts.Close()

		dispatcher := NewDefaultOutboundDispatcher(ts.URL, ts.URL, nil)
		dispatcher.SetRetryConfig(1*time.Millisecond, 4)

		if err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "testcommit123"}); err != nil || receivedGitPush.Commit != "testcommit123" {
			t.Fatalf("DispatchGitPush failed: err=%v, got=%+v", err, receivedGitPush)
		}

		if _, err := dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{Image: "ghcr.io/azylman/aerial-webhooks-router:latest", Digest: "sha256:abcd"}); err != nil || receivedImageReady.Digest != "sha256:abcd" {
			t.Fatalf("DispatchImageReady failed: err=%v, got=%+v", err, receivedImageReady)
		}

		if err := dispatcher.DispatchPrompt(ctx, PromptRequest{ChannelID: "not-a-snowflake", Prompt: "hello"}); err == nil {
			t.Fatalf("expected error on invalid snowflake for prompt")
		}
		if err := dispatcher.DispatchPrompt(ctx, PromptRequest{ChannelID: validSnowflake, Prompt: "test prompt message"}); err != nil || promptAttempts != 2 || receivedPrompt.Prompt != "test prompt message" {
			t.Fatalf("DispatchPrompt failed: err=%v, attempts=%d, got=%+v", err, promptAttempts, receivedPrompt)
		}

		if err := dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: "not-a-snowflake", Content: "hi"}); err == nil {
			t.Fatalf("expected error on invalid snowflake for direct message")
		}
		if err := dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: validSnowflake, Content: "deployment completed"}); err != nil || directAttempts != 2 || receivedDirect.Content != "deployment completed" {
			t.Fatalf("DispatchDirectMessage failed: err=%v, attempts=%d, got=%+v", err, directAttempts, receivedDirect)
		}

		// Client error 400 does not retry
		clientErrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad request", http.StatusBadRequest)
		}))
		defer clientErrServer.Close()
		clientErrDisp := NewDefaultOutboundDispatcher("", clientErrServer.URL, nil)
		clientErrDisp.SetRetryConfig(1*time.Millisecond, 3)
		if err := clientErrDisp.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: validSnowflake, Content: "hi"}); err == nil {
			t.Errorf("expected error on 400 bad request")
		}
	})

	t.Run("NotFoundToleratedAndHTML404Rejected", func(t *testing.T) {
		t.Parallel()
		var statusCode atomic.Int32
		statusCode.Store(http.StatusNotFound)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(int(statusCode.Load()))
			switch r.URL.Path {
			case "/events/git_push":
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "message": "repository not managed by hangar"})
			case "/events/image_ready":
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "not_found", "message": "no nomad jobs found using image"})
			default:
				http.NotFound(w, r)
			}
		}))
		defer server.Close()

		dispatcher := NewDefaultOutboundDispatcher(server.URL, "http://127.0.0.1:8080", nil)
		dispatcher.retryDelay = 1 * time.Millisecond

		// 1. JSON 404 from Hangar is tolerated
		if err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{Repo: "azylman/aerial-sidecars", Ref: "refs/heads/main", Commit: "abc"}); err != nil {
			t.Fatalf("expected nil error on 404 git_push, got: %v", err)
		}
		if _, err := dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{Image: "ghcr.io/azylman/mirrormere-eink-renderer:latest"}); err != nil {
			t.Fatalf("expected nil error on 404 image_ready, got: %v", err)
		}

		// 2. 500 error is NOT tolerated
		statusCode.Store(http.StatusInternalServerError)
		if err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "abc"}); err == nil {
			t.Fatal("expected error on 500 git_push, got nil")
		}
		if _, err := dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{Image: "ghcr.io/azylman/mirrormere:latest"}); err == nil {
			t.Fatal("expected error on 500 image_ready, got nil")
		}

		// 3. HTML 404 (misconfigured proxy) is NOT tolerated
		html404Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("<html><title>404 Not Found</title><body>Proxy Error</body></html>"))
		}))
		defer html404Server.Close()
		dispatcher.hangarURL = html404Server.URL
		if err := dispatcher.DispatchGitPush(ctx, GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "abc"}); err == nil {
			t.Fatal("expected error on HTML 404 git_push, got nil")
		}
		if _, err := dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{Image: "ghcr.io/azylman/mirrormere:latest"}); err == nil {
			t.Fatal("expected error on HTML 404 image_ready, got nil")
		}
	})
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

	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 1, Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"},
			{ID: 2, Name: "Build & Push Images to GHCR (webhooks-router)", Status: "completed", Conclusion: "success"},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mockJobs)
	}))
	defer ts.Close()

	mockDisp := &mockOutboundDispatcher{
		matchedJobs: map[string][]string{
			"ghcr.io/azylman/aerial-brain:latest":           {"brain"},
			"ghcr.io/azylman/aerial-webhooks-router:latest": {"webhooks-router", "brain"},
		},
	}
	mockReg := &mockPRRegistry{
		resolvePRNum:    534,
		resolveBranch:   "main",
		resolveTargetID: "1555405874565091380",
	}
	srv := newTestRouter(Config{GitHubAPIURL: ts.URL}, ts.Client(), mockReg, mockDisp)

	// 1. Successful workflow_run on main dispatches image_ready in parallel and transitions matched jobs to deploying
	wfSuccessPayload := `{"action":"completed","workflow_run":{"id":8888,"name":"Continuous Delivery","head_sha":"mainsha123","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`
	if _, err := srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-cd-success", []byte(wfSuccessPayload)); err != nil {
		t.Fatalf("ProcessGitHubEvent failed: %v", err)
	}

	calls := mockDisp.ImageReadyCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 image_ready dispatches, got %d: %+v", len(calls), calls)
	}
	imagesSeen := make(map[string]ImageReadyEventRequest)
	for _, call := range calls {
		imagesSeen[call.Image] = call
	}
	for _, img := range []string{"ghcr.io/azylman/aerial-brain:latest", "ghcr.io/azylman/aerial-webhooks-router:latest"} {
		if call, ok := imagesSeen[img]; !ok || call.Repo != "azylman/aerial" || call.CommitSHA != "mainsha123" || call.PRNumber != 534 || call.TargetID != "1555405874565091380" {
			t.Errorf("unexpected call for %s: %+v", img, call)
		}
	}

	if len(mockReg.deployingCalls) != 1 || mockReg.deployingCalls[0].repo != "azylman/aerial" || mockReg.deployingCalls[0].prNumber != 534 || len(mockReg.deployingCalls[0].jobs) != 2 {
		t.Errorf("unexpected deployingCalls: %+v", mockReg.deployingCalls)
	}

	// 2. Non-main branch, failed, and in_progress workflow_runs do NOT dispatch image_ready
	mockDisp.imageReadyCalls = nil
	for _, tc := range []struct {
		del, payload string
	}{
		{"del-wf-branch", `{"action":"completed","workflow_run":{"id":8889,"name":"Continuous Delivery","head_sha":"featsha456","head_branch":"feat/my-feature","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`},
		{"del-wf-fail", `{"action":"completed","workflow_run":{"id":8890,"name":"Continuous Delivery","head_sha":"failsha789","head_branch":"main","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`},
		{"del-wf-prog", `{"action":"in_progress","workflow_run":{"id":8891,"name":"Continuous Delivery","head_sha":"progsha111","head_branch":"main","status":"in_progress","conclusion":"","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`},
	} {
		if _, err := srv.ProcessGitHubEvent(ctx, "workflow_run", tc.del, []byte(tc.payload)); err != nil {
			t.Fatalf("ProcessGitHubEvent(%s) failed: %v", tc.del, err)
		}
		if len(mockDisp.ImageReadyCalls()) != 0 {
			t.Errorf("%s: expected 0 image_ready calls, got %d", tc.del, len(mockDisp.ImageReadyCalls()))
		}
	}
}

// --- 7. PR Conflict Checks & SHA/Thread Resolution ---

func TestCheckOpenPRConflicts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var retryAttempts atomic.Int32
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/pulls/543"):
			_, _ = w.Write([]byte(`{"number":543,"mergeable":false,"mergeable_state":"dirty"}`))
		case strings.Contains(r.URL.Path, "/pulls/544"):
			_, _ = w.Write([]byte(`{"number":544,"mergeable":true,"mergeable_state":"clean"}`))
		case strings.Contains(r.URL.Path, "/pulls/545"):
			if retryAttempts.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"number":545,"mergeable":null,"mergeable_state":"unknown"}`))
				return
			}
			_, _ = w.Write([]byte(`{"number":545,"mergeable":false,"mergeable_state":"dirty"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(apiServer.Close)

	t.Run("ConflictDetected", func(t *testing.T) {
		t.Parallel()
		mockReg := &mockPRRegistry{
			openPRs:          []RegisteredPR{{PRNumber: 543, Branch: "feat/my-feature", TargetID: "1555405874565091380", HeadSHA: "head543"}},
			conflictTargetID: "1555405874565091380",
			conflictUpdated:  true,
		}
		mockDisp := &mockOutboundDispatcher{}
		srv := newTestRouter(Config{GitHubAPIURL: apiServer.URL}, nil, mockReg, mockDisp)
		srv.SetConflictCheckDelay(0)

		if err := srv.checkOpenPRConflicts(ctx, "azylman/aerial"); err != nil {
			t.Fatalf("checkOpenPRConflicts failed: %v", err)
		}
		if len(mockReg.conflictCalls) != 1 || mockReg.conflictCalls[0].prNumber != 543 {
			t.Errorf("expected conflict call for PR 543, got %+v", mockReg.conflictCalls)
		}
		dms := mockDisp.DirectMessageCalls()
		prompts := mockDisp.PromptCalls()
		if len(dms) != 1 || !strings.Contains(dms[0].Content, "⛔ **(PR: #543, repo: aerial)** Merge conflict detected. Following up...") {
			t.Errorf("unexpected conflict direct message: %+v", dms)
		}
		if len(prompts) != 1 || !strings.Contains(prompts[0].Prompt, "Merge conflict detected on PR #543") {
			t.Errorf("unexpected conflict prompt: %+v", prompts)
		}
	})

	t.Run("CleanPR_NoAction", func(t *testing.T) {
		t.Parallel()
		mockReg := &mockPRRegistry{
			openPRs: []RegisteredPR{{PRNumber: 544, Branch: "feat/clean", TargetID: "1555405874565091380", HeadSHA: "head544"}},
		}
		mockDisp := &mockOutboundDispatcher{}
		srv := newTestRouter(Config{GitHubAPIURL: apiServer.URL}, nil, mockReg, mockDisp)
		srv.SetConflictCheckDelay(0)

		if err := srv.checkOpenPRConflicts(ctx, "azylman/aerial"); err != nil {
			t.Fatalf("checkOpenPRConflicts failed: %v", err)
		}
		if len(mockReg.conflictCalls) != 0 || len(mockDisp.PromptCalls()) != 0 {
			t.Errorf("expected 0 conflict/prompt calls for clean PR")
		}
	})

	t.Run("RetryNullMergeable", func(t *testing.T) {
		t.Parallel()
		mockReg := &mockPRRegistry{
			openPRs:          []RegisteredPR{{PRNumber: 545, Branch: "feat/retry", TargetID: "1555405874565091380", HeadSHA: "head545"}},
			conflictTargetID: "1555405874565091380",
			conflictUpdated:  true,
		}
		mockDisp := &mockOutboundDispatcher{}
		srv := newTestRouter(Config{GitHubAPIURL: apiServer.URL}, nil, mockReg, mockDisp)
		srv.SetConflictCheckDelay(1 * time.Millisecond)

		if err := srv.checkOpenPRConflicts(ctx, "azylman/aerial"); err != nil {
			t.Fatalf("checkOpenPRConflicts failed: %v", err)
		}
		if retryAttempts.Load() < 2 || len(mockReg.conflictCalls) != 1 {
			t.Errorf("expected >=2 attempts and 1 conflict call, got attempts=%d calls=%d", retryAttempts.Load(), len(mockReg.conflictCalls))
		}
	})
}

func TestPRAndSHAResolution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("ResolvePRFromCommitSHA_ErrorHandling", func(t *testing.T) {
		t.Parallel()
		srv := NewRouterServer(Config{}, nil)
		for _, tc := range []struct{ repo, sha string }{
			{"", "sha"},
			{"repo", ""},
			{"repo", "0000000000000000000000000000000000000000"},
		} {
			if _, _, err := srv.resolvePRFromCommitSHA(ctx, tc.repo, tc.sha); err == nil {
				t.Errorf("expected validation error for repo=%q sha=%q", tc.repo, tc.sha)
			}
		}

		var mode atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch mode.Load() {
			case 1: // 403 Rate Limited
				w.Header().Set("X-RateLimit-Remaining", "0")
				w.WriteHeader(http.StatusForbidden)
			case 2: // 404 Not Found
				w.WriteHeader(http.StatusNotFound)
			case 3: // Malformed JSON
				_, _ = w.Write([]byte(`{not-an-array`))
			case 4: // Empty PR list
				_, _ = w.Write([]byte(`[]`))
			}
		}))
		defer ts.Close()

		srvTS := NewRouterServer(Config{GitHubAPIURL: ts.URL, GitHubToken: "secret-token"}, ts.Client())
		for m := int32(1); m <= 4; m++ {
			mode.Store(m)
			if _, _, err := srvTS.resolvePRFromCommitSHA(ctx, "aerial", "sha123"); err == nil {
				t.Errorf("mode %d: expected error from resolvePRFromCommitSHA", m)
			}
		}
	})

	t.Run("ResolveSHA_FastPathAndFallback", func(t *testing.T) {
		t.Parallel()
		// 1 & 2. Fast path resolved locally
		mockRegFast := &mockPRRegistry{resolvePRNum: 42, resolveTargetID: "1555405874565091380", resolveBranch: "feat/fast-path"}
		srvFast := newTestRouter(Config{}, nil, mockRegFast, nil)
		if prNum, targetID, branch, err := srvFast.ResolveSHA(ctx, "aerial", "sha-fast-1"); err != nil || prNum != 42 || targetID != "1555405874565091380" || branch != "feat/fast-path" {
			t.Errorf("unexpected fast path result: pr=%d target=%s branch=%s err=%v", prNum, targetID, branch, err)
		}

		// 3. Fallback path: local SHA returns 0, GitHub API resolves PR and backfills
		ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/commits/sha-gh-3/pulls") {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`[{"number":99,"state":"closed","head":{"ref":"feat/gh-fallback","sha":"sha-head-99"}}]`))
				return
			}
			http.NotFound(w, r)
		}))
		defer ghServer.Close()

		mockRegGH := &mockPRRegistry{resolvePRNum: 0, resolveTargetID: "1555405874565091380", backfillTargetID: "1555405874565091380"}
		srvGH := newTestRouter(Config{GitHubAPIURL: ghServer.URL}, nil, mockRegGH, nil)
		prNum3, targetID3, branch3, err3 := srvGH.ResolveSHA(ctx, "azylman/aerial", "sha-gh-3")
		if err3 != nil || prNum3 != 99 || targetID3 != "1555405874565091380" || branch3 != "feat/gh-fallback" || len(mockRegGH.backfillCalls) != 1 {
			t.Errorf("unexpected github fallback result: pr=%d target=%s branch=%s err=%v calls=%+v", prNum3, targetID3, branch3, err3, mockRegGH.backfillCalls)
		}

		// 4. Zero / Empty SHA
		for _, tc := range []struct{ repo, sha string }{
			{"azylman/aerial", "0000000000000000000000000000000000000000"},
			{"", ""},
		} {
			if p, tgt, b, err := srvGH.ResolveSHA(ctx, tc.repo, tc.sha); err != nil || p != 0 || tgt != "" || b != "" {
				t.Errorf("expected zero results for %+v, got pr=%d target=%s branch=%s err=%v", tc, p, tgt, b, err)
			}
		}
	})

	t.Run("ResolveThreadForPRAndDeploymentSuccessPrompt", func(t *testing.T) {
		t.Parallel()
		srvNil := NewRouterServer(Config{}, nil)
		if tID, err := srvNil.ResolveThreadForPR(ctx, "azylman/aerial", 123); err != nil || tID != "" {
			t.Errorf("expected empty thread for nil registry, got %q err=%v", tID, err)
		}
		if tID, err := srvNil.ResolveThreadForPR(ctx, "azylman/aerial", 0); err != nil || tID != "" {
			t.Errorf("expected empty thread for prNum 0, got %q err=%v", tID, err)
		}

		mockDisp := &mockOutboundDispatcher{}
		mockReg := &mockPRRegistry{
			resolveTargetID: "1555405874565091380",
			channelContextMap: map[string][]ChannelMessageContext{
				"1555405874565091380": {{AuthorName: "arcane103", Content: "Wrap up the deployment", ResponseText: "Done!"}},
			},
		}
		srv := newTestRouter(Config{}, nil, mockReg, mockDisp)
		if targetID, err := srv.ResolveThreadForPR(ctx, "aerial", 456); err != nil || targetID != "1555405874565091380" {
			t.Errorf("expected targetID 1555405874565091380, got %q (err=%v)", targetID, err)
		}

		if err := srv.dispatchDeploymentSuccessPrompt(ctx, "1555405874565091380", "brain", "azylman/aerial", "sha123", 558, "alloc-1"); err != nil {
			t.Fatalf("dispatchDeploymentSuccessPrompt failed: %v", err)
		}
		pCalls := mockDisp.PromptCalls()
		if len(pCalls) != 1 || pCalls[0].ChannelID != "1555405874565091380" ||
			!strings.Contains(pCalls[0].Prompt, "Continuous Delivery deployment completed for job brain on azylman/aerial (PR #558, commit: sha123, ref: alloc-1).") ||
			!strings.Contains(pCalls[0].Prompt, "[arcane103]: Wrap up the deployment") ||
			!strings.Contains(pCalls[0].Prompt, "Directive: Based on the recent conversation context above") {
			t.Errorf("unexpected deployment success prompt: %+v", pCalls)
		}

		// Invalid snowflake is a no-op
		if err := srv.dispatchDeploymentSuccessPrompt(ctx, "invalid", "brain", "azylman/aerial", "sha123", 558, ""); err != nil || len(mockDisp.PromptCalls()) != 1 {
			t.Errorf("expected no-op on invalid snowflake, got err=%v calls=%d", err, len(mockDisp.PromptCalls()))
		}
	})
}

// --- 8. River Workers, Crash Alerts & Server Endpoints ---

func TestRiverWorkersAndUniqueOpts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("GitHubWebhookWorker", func(t *testing.T) {
		t.Parallel()
		worker := &GitHubWebhookWorker{server: NewRouterServer(Config{}, nil)}
		jobValid := &river.Job[GitHubWebhookArgs]{
			JobRow: &rivertype.JobRow{ID: 101},
			Args: GitHubWebhookArgs{
				Event:    "ping",
				Delivery: "del-worker-1",
				Body:     []byte(`{"zen":"Responsive is better than fast.","hook_id":123,"repository":{"full_name":"azylman/aerial"}}`),
			},
		}
		if err := worker.Work(ctx, jobValid); err != nil {
			t.Fatalf("worker.Work failed on valid job: %v", err)
		}
		if err := (&GitHubWebhookWorker{server: nil}).Work(ctx, jobValid); err == nil {
			t.Errorf("expected error when worker server is nil")
		}
		jobMalformed := &river.Job[GitHubWebhookArgs]{
			JobRow: &rivertype.JobRow{ID: 102},
			Args:   GitHubWebhookArgs{Event: "pull_request", Delivery: "del-worker-2", Body: []byte(`{malformed`)},
		}
		if err := worker.Work(ctx, jobMalformed); err == nil {
			t.Errorf("expected error when event processing fails")
		}
	})

	t.Run("HangarWebhookWorker", func(t *testing.T) {
		t.Parallel()
		mockDisp := &mockOutboundDispatcher{}
		worker := &HangarWebhookWorker{server: newTestRouter(Config{}, nil, nil, mockDisp)}
		jobValid := &river.Job[HangarWebhookArgs]{
			JobRow: &rivertype.JobRow{ID: 201},
			Args: HangarWebhookArgs{
				Event: HangarDeployEvent{
					Event: "deploy_success", JobName: "brain", Repo: "azylman/aerial",
					PRNumber: 558, TargetID: "1555405874565091380", Status: "success",
				},
			},
		}
		if err := worker.Work(ctx, jobValid); err != nil || len(mockDisp.PromptCalls()) != 1 {
			t.Fatalf("unexpected hangar worker result: err=%v, calls=%d", err, len(mockDisp.PromptCalls()))
		}
		if err := (&HangarWebhookWorker{server: nil}).Work(ctx, jobValid); err == nil {
			t.Errorf("expected error when worker server is nil")
		}
		workerErr := &HangarWebhookWorker{server: newTestRouter(Config{}, nil, nil, &mockOutboundDispatcher{promptErr: errors.New("503")})}
		if err := workerErr.Work(ctx, jobValid); err == nil {
			t.Errorf("expected error when downstream dispatch fails")
		}
	})

	t.Run("NomadEventWorkerAndPrematureAllocationGuard", func(t *testing.T) {
		t.Parallel()
		var isHealthy atomic.Bool
		isHealthy.Store(true)
		nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case strings.Contains(r.URL.Path, "/deployments"):
				status := "running"
				if isHealthy.Load() {
					status = "successful"
				}
				_, _ = fmt.Fprintf(w, `[{"ID":"dep-1","Status":%q}]`, status)
			case strings.Contains(r.URL.Path, "/allocations"):
				_, _ = w.Write([]byte(`[{"ID":"alloc-1","DesiredStatus":"run","ClientStatus":"running"}]`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer nomadServer.Close()

		mockReg := &mockPRRegistry{
			deployedTargetID: "1555405874565091380",
			deployedPRNum:    552,
			deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
			deployedRepo:     "azylman/aerial",
			deployedUpdated:  true,
		}
		mockDisp := &mockOutboundDispatcher{}
		srv := newTestRouter(Config{NomadAddr: nomadServer.URL}, nil, mockReg, mockDisp)
		worker := &NomadEventWorker{server: srv}

		// 1. Deployment event
		jobDep := &river.Job[NomadEventArgs]{
			JobRow: &rivertype.JobRow{ID: 301},
			Args:   NomadEventArgs{Topic: "Deployment", Type: "DeploymentStatusUpdate", Payload: []byte(`{"Deployment":{"ID":"dep-101","JobID":"brain","Status":"successful"}}`)},
		}
		if err := worker.Work(ctx, jobDep); err != nil || len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "brain" || len(mockDisp.PromptCalls()) != 1 {
			t.Fatalf("unexpected deployment worker result: err=%v, jobs=%v, prompts=%d", err, mockReg.deployedJobCalls, len(mockDisp.PromptCalls()))
		}

		// 2. Allocation event when healthy vs unhealthy (premature guard)
		mockReg.deployedJobCalls = nil
		mockDisp.promptCalls = nil
		allocPayload := []byte(`{"Allocation":{"ID":"alloc-202","JobID":"brain","DesiredStatus":"run","ClientStatus":"running"}}`)

		isHealthy.Store(false)
		if err := srv.ProcessNomadEvent(ctx, "Allocation", "AllocationUpdated", allocPayload); err != nil || len(mockReg.deployedJobCalls) != 0 {
			t.Fatalf("expected 0 deployedJobCalls when deployment unhealthy, got err=%v calls=%v", err, mockReg.deployedJobCalls)
		}

		isHealthy.Store(true)
		jobAlloc := &river.Job[NomadEventArgs]{
			JobRow: &rivertype.JobRow{ID: 302},
			Args:   NomadEventArgs{Topic: "Allocation", Type: "AllocationUpdated", Payload: allocPayload},
		}
		if err := worker.Work(ctx, jobAlloc); err != nil || len(mockReg.deployedJobCalls) != 1 || len(mockDisp.PromptCalls()) != 1 {
			t.Fatalf("unexpected healthy allocation worker result: err=%v, calls=%v", err, mockReg.deployedJobCalls)
		}

		// 3. Nil server & downstream error
		if err := (&NomadEventWorker{server: nil}).Work(ctx, jobDep); err == nil {
			t.Errorf("expected error when worker server is nil")
		}
		workerErr := &NomadEventWorker{server: newTestRouter(Config{}, nil, mockReg, &mockOutboundDispatcher{promptErr: errors.New("503")})}
		if err := workerErr.Work(ctx, jobDep); err == nil {
			t.Errorf("expected error when downstream prompt dispatch fails")
		}
	})

	t.Run("RiverUniqueOptsByArgs", func(t *testing.T) {
		t.Parallel()
		for _, opts := range []river.InsertOpts{
			(HangarWebhookArgs{}).InsertOpts(),
			(HangarWebhookArgs{IdempotencyKey: "hangar-deploy-started-603-brain", Event: HangarDeployEvent{Event: "deploy_started", JobName: "brain", PRNumber: 603}}).InsertOpts(),
			(NomadEventArgs{}).InsertOpts(),
		} {
			if !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
				t.Errorf("expected UniqueOpts.ByArgs=true and ByPeriod=15m, got %+v", opts)
			}
		}
	})
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
	if args.Kind() != "nomad_crash_alert" {
		t.Errorf("expected kind nomad_crash_alert, got %s", args.Kind())
	}
	if opts := args.InsertOpts(); !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
		t.Errorf("unexpected InsertOpts: %+v", opts)
	}

	if err := (&NomadCrashAlertWorker{server: nil}).Work(ctx, &river.Job[NomadCrashAlertArgs]{JobRow: &rivertype.JobRow{ID: 101}, Args: args}); err == nil {
		t.Errorf("expected error with nil server")
	}

	disp := &mockOutboundDispatcher{}
	srv := newTestRouter(Config{SystemChannelID: "1555405874565091380"}, nil, nil, disp)
	worker := &NomadCrashAlertWorker{server: srv}
	if err := worker.Work(ctx, &river.Job[NomadCrashAlertArgs]{JobRow: &rivertype.JobRow{ID: 101}, Args: args}); err != nil {
		t.Fatalf("unexpected error from worker: %v", err)
	}
	if dms := disp.DirectMessageCalls(); len(dms) != 1 || !strings.Contains(dms[0].Content, "💥 Nomad job `openobserve` crashed or failed. Following up...") {
		t.Errorf("unexpected dm calls: %+v", dms)
	}
	if prompts := disp.PromptCalls(); len(prompts) != 1 || !strings.Contains(prompts[0].Prompt, "Nomad job `openobserve` failed or crashed unexpectedly") {
		t.Errorf("unexpected prompt calls: %+v", prompts)
	}

	// Edge cases
	for _, tc := range []struct {
		name    string
		channel string
		disp    OutboundDispatcher
		wantErr bool
	}{
		{"invalid snowflake", "invalid-snowflake", &mockOutboundDispatcher{}, false},
		{"nil dispatcher", "1555405874565091380", nil, false},
		{"4xx on prompt", "1555405874565091380", &mockOutboundDispatcher{promptErr: errors.New("brain prompt returned status 404: channel not found")}, false},
		{"4xx on DM", "1555405874565091380", &mockOutboundDispatcher{directMessageErr: errors.New("brain internal message returned status 400: bad request")}, false},
		{"5xx on prompt", "1555405874565091380", &mockOutboundDispatcher{promptErr: errors.New("brain prompt returned status 500: internal server error")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newTestRouter(Config{SystemChannelID: tc.channel}, nil, nil, tc.disp)
			if err := s.ProcessCrashAlert(ctx, args); (err != nil) != tc.wantErr {
				t.Errorf("ProcessCrashAlert() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestServerEndpointsAndLifecycle(t *testing.T) {
	t.Run("HealthzAndMetrics", func(t *testing.T) {
		routes := NewRouterServer(Config{Port: "4020"}, nil).Routes()

		recH := doHTTPRequest(routes, http.MethodGet, "/healthz", nil, nil)
		if recH.Code != http.StatusOK {
			t.Fatalf("expected 200 OK from /healthz, got %d", recH.Code)
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(recH.Body.Bytes(), &resp); err != nil || resp["status"] != "ok" || resp["service"] != "webhooks-router" {
			t.Errorf("unexpected healthz body: %v (err=%v)", resp, err)
		}

		RecordUniqueSkipped("")
		RecordUniqueSkipped("test_kind_metric")
		recM := doHTTPRequest(routes, http.MethodGet, "/metrics", nil, nil)
		if recM.Code != http.StatusOK || !strings.Contains(recM.Body.String(), "webhooks_router_unique_skipped_total") || !strings.Contains(recM.Body.String(), `kind="test_kind_metric"`) {
			t.Errorf("unexpected /metrics response: code=%d body=%s", recM.Code, recM.Body.String())
		}
	})

	t.Run("CoverageFlushEndpoint", func(t *testing.T) {
		routes := NewRouterServer(Config{}, nil).Routes()
		oldFn := writeCountersDirFn
		t.Cleanup(func() { writeCountersDirFn = oldFn })
		oldMetaFn := writeMetaDirFn
		t.Cleanup(func() { writeMetaDirFn = oldMetaFn })

		t.Setenv("GOCOVERDIR", "")
		if rec := doHTTPRequest(routes, http.MethodGet, "/debug/coverage/flush", nil, nil); rec.Code != http.StatusOK {
			t.Errorf("expected 200 OK without GOCOVERDIR, got %d", rec.Code)
		}

		called := false
		writeCountersDirFn = func(dir string) error {
			called = true
			return nil
		}
		metaCalled := false
		writeMetaDirFn = func(dir string) error {
			metaCalled = true
			return nil
		}
		t.Setenv("GOCOVERDIR", t.TempDir())
		if rec := doHTTPRequest(routes, http.MethodGet, "/debug/coverage/flush", nil, nil); rec.Code != http.StatusOK || !called || !metaCalled {
			t.Errorf("expected 200 OK, called=true, metaCalled=true with GOCOVERDIR, got code=%d called=%v metaCalled=%v", rec.Code, called, metaCalled)
		}

		writeMetaDirFn = func(dir string) error {
			return errors.New("simulated meta flush error")
		}
		if rec := doHTTPRequest(routes, http.MethodGet, "/debug/coverage/flush", nil, nil); rec.Code != http.StatusOK {
			t.Errorf("expected 200 OK when meta flush errors, got %d", rec.Code)
		}

		writeCountersDirFn = func(dir string) error {
			return errors.New("simulated flush error")
		}
		if rec := doHTTPRequest(routes, http.MethodGet, "/debug/coverage/flush", nil, nil); rec.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 on flush error, got %d", rec.Code)
		}
	})

	t.Run("RunServerLifecycleAndErrors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan string, 1)
		done := make(chan error, 1)

		go func() {
			done <- runServer(ctx, Config{Port: "0"}, func(addr string) {
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

		if err := runServer(context.Background(), Config{Port: "-1"}, nil); err == nil {
			t.Errorf("expected error on invalid port -1")
		}

		ctxC, cancelC := context.WithCancel(context.Background())
		cancelC()
		if err := runApp(ctxC, Config{Port: "0"}); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("unexpected error from runApp: %v", err)
		}

		if err := runServer(context.Background(), Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"}, nil); err == nil {
			t.Errorf("expected error on invalid postgres url")
		}
	})
}

// --- 9. Live PostgreSQL & River Integration Tests ---

func openLivePostgresPool(t *testing.T, ctx context.Context, maxConns int32) (*pgxpool.Pool, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping live Postgres integration test in short mode")
	}
	pgURL := getTestPostgresURL()
	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		t.Skipf("cannot parse postgres url: %v", err)
	}
	poolConfig.MaxConns = maxConns
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if _, ok := poolConfig.ConnConfig.RuntimeParams["search_path"]; !ok {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = "sidecars, public"
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Skipf("skipping live test, could not create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping live test, postgres ping failed: %v", err)
	}
	return pool, pgURL
}

func TestRiverLiveIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, pgURL := openLivePostgresPool(t, ctx, 5)

	server := NewRouterServer(Config{PostgresURL: pgURL}, nil)
	processedCh := make(chan string, 1)
	server.onProcessed = func(event, delivery string) {
		if delivery == "live-integration-delivery" {
			processedCh <- event
		}
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &GitHubWebhookWorker{server: server})

	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 5}},
		Workers: workers,
	})
	if err != nil {
		t.Fatalf("failed to create river client: %v", err)
	}
	server.SetRiverClient(riverClient)

	if err := riverClient.Start(ctx); err != nil {
		t.Fatalf("failed to start river client: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		_ = riverClient.Stop(stopCtx)
	}()

	reqBody := `{"zen":"Responsive is better than fast.","hook_id":9999,"repository":{"full_name":"azylman/aerial"}}`
	rec := doHTTPRequest(server.Routes(), http.MethodPost, "/api/webhooks/github", strings.NewReader(reqBody), map[string]string{
		"X-GitHub-Event":    "ping",
		"X-GitHub-Delivery": "live-integration-delivery",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from handleGitHubWebhook, got %d: %s", rec.Code, rec.Body.String())
	}

	select {
	case event := <-processedCh:
		if event != "ping" {
			t.Errorf("expected processed event ping, got %s", event)
		}
	case <-time.After(2 * time.Second):
		var state string
		queryErr := pool.QueryRow(ctx, "SELECT state FROM river_job WHERE args->>'delivery' = 'live-integration-delivery' ORDER BY id DESC LIMIT 1").Scan(&state)
		if queryErr == nil && (state == "completed" || state == "running") {
			t.Logf("Job was successfully processed by live cluster worker (state=%s)", state)
			return
		}
		t.Fatalf("timeout waiting for River worker to process live job from postgres (db state: %s, queryErr: %v)", state, queryErr)
	}
}

func TestPostgresPRRegistry_Live(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, _ := openLivePostgresPool(t, ctx, 3)

	repo := "azylman/aerial"
	testPRNum := 999991
	targetID := "live-test-target-xyz"

	_, _ = pool.Exec(ctx, "DELETE FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum)
	}()

	insertSQL := `
		INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, status, title, created_at, updated_at)
		VALUES ($1, $2, 'feat/live-test', 'initialsha111', $3, 'open', 'Live Test PR', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
	`
	if _, err := pool.Exec(ctx, insertSQL, repo, testPRNum, targetID); err != nil {
		t.Fatalf("failed to insert initial test row: %v", err)
	}

	reg := NewPostgresPRRegistry(pool)

	assertRow := func(wantHead, wantMerge, wantStatus string) {
		t.Helper()
		var head, merge, status string
		if err := pool.QueryRow(ctx, "SELECT COALESCE(head_sha,''), COALESCE(merge_sha,''), status FROM pr_registry WHERE repo = $1 AND pr_number = $2", repo, testPRNum).Scan(&head, &merge, &status); err != nil {
			t.Fatalf("query row failed: %v", err)
		}
		if (wantHead != "" && head != wantHead) || (wantMerge != "" && merge != wantMerge) || (wantStatus != "" && status != wantStatus) {
			t.Errorf("row state = (head=%s, merge=%s, status=%s), want (head=%s, merge=%s, status=%s)", head, merge, status, wantHead, wantMerge, wantStatus)
		}
	}

	// 1. UpdatePRSync
	if target, err := reg.UpdatePRSync(ctx, repo, testPRNum, "syncsha222"); err != nil || target != targetID {
		t.Fatalf("UpdatePRSync failed (target=%s): %v", target, err)
	}
	assertRow("syncsha222", "", "open")

	// 2. AtomicTransitionCIFailed: 'open' -> 'ci_failed'
	if gotTarget, updated, err := reg.AtomicTransitionCIFailed(ctx, repo, testPRNum); err != nil || !updated || gotTarget != targetID {
		t.Fatalf("AtomicTransitionCIFailed: got updated=%v, target=%s, err=%v", updated, gotTarget, err)
	}
	assertRow("", "", "ci_failed")

	// 3. CAS gate: second AtomicTransitionCIFailed is no-op
	if _, updated, err := reg.AtomicTransitionCIFailed(ctx, repo, testPRNum); err != nil || updated {
		t.Errorf("second AtomicTransitionCIFailed should return updated=false, got updated=%v err=%v", updated, err)
	}

	// 4. UpdatePRSync resets 'ci_failed' back to 'open'
	if target, err := reg.UpdatePRSync(ctx, repo, testPRNum, "newhead333"); err != nil || target != targetID {
		t.Fatalf("UpdatePRSync reset failed (target=%s): %v", target, err)
	}
	assertRow("newhead333", "", "open")

	// 5. AtomicTransitionCIFailedByHeadSHA
	if gotTarget, gotPR, updated, err := reg.AtomicTransitionCIFailedByHeadSHA(ctx, repo, "newhead333"); err != nil || !updated || gotPR != testPRNum || gotTarget != targetID {
		t.Errorf("AtomicTransitionCIFailedByHeadSHA: got updated=%v, pr=%d, target=%s, err=%v", updated, gotPR, gotTarget, err)
	}

	// 6. UpdatePRMerged: transitions to 'merged'
	if target, err := reg.UpdatePRMerged(ctx, repo, testPRNum, "mergesha444"); err != nil || target != targetID {
		t.Fatalf("UpdatePRMerged failed (target=%s): %v", target, err)
	}
	assertRow("", "mergesha444", "merged")

	// 7. Merged PR cannot transition to ci_failed
	if _, updated, err := reg.AtomicTransitionCIFailed(ctx, repo, testPRNum); err != nil || updated {
		t.Errorf("AtomicTransitionCIFailed on merged PR must return updated=false, got %v (err=%v)", updated, err)
	}

	// 8. BackfillPushMergeSHA
	if target, err := reg.BackfillPushMergeSHA(ctx, repo, testPRNum, "mergesha555"); err != nil || target != targetID {
		t.Fatalf("BackfillPushMergeSHA failed (target=%s): %v", target, err)
	}
	assertRow("", "mergesha555", "")

	// 9. Untracked PR & Empty/Zero SHA guards
	if target, err := reg.UpdatePRMerged(ctx, repo, 999999, "sha"); err != nil || target != "" {
		t.Errorf("expected nil error and empty target on untracked PR, got %v, %q", err, target)
	}
	if target, err := reg.UpdatePRClosedUnmerged(ctx, repo, 999999); err != nil || target != "" {
		t.Errorf("expected nil error and empty target on untracked PR closed, got %v, %q", err, target)
	}
	if target, err := reg.UpdatePRSync(ctx, repo, 999999, "sha"); err != nil || target != "" {
		t.Errorf("expected nil error and empty target on untracked PR sync, got %v, %q", err, target)
	}
	if _, updated, err := reg.AtomicTransitionCIFailed(ctx, repo, 999999); err != nil || updated {
		t.Errorf("untracked AtomicTransitionCIFailed: got updated=%v, err=%v", updated, err)
	}
	for _, sha := range []string{"nonexistent-sha", "", "0000000000000000000000000000000000000000"} {
		if _, _, updated, err := reg.AtomicTransitionCIFailedByHeadSHA(ctx, repo, sha); err != nil || updated {
			t.Errorf("AtomicTransitionCIFailedByHeadSHA(%q): got updated=%v, err=%v", sha, updated, err)
		}
	}

	// 10. ResolvePRBySHA
	for _, sha := range []string{"newhead333", "mergesha555"} {
		if resPR, resBranch, resTarget, err := reg.ResolvePRBySHA(ctx, repo, sha); err != nil || resPR != testPRNum || resBranch != "feat/live-test" || resTarget != targetID {
			t.Errorf("ResolvePRBySHA(%q) = (%d, %s, %s, %v)", sha, resPR, resBranch, resTarget, err)
		}
	}
	for _, sha := range []string{"untracked-sha-999", "", "0000000000000000000000000000000000000000"} {
		if resPR, resBranch, resTarget, err := reg.ResolvePRBySHA(ctx, repo, sha); err != nil || resPR != 0 || resBranch != "" || resTarget != "" {
			t.Errorf("ResolvePRBySHA(%q) = (%d, %s, %s, %v), want 0", sha, resPR, resBranch, resTarget, err)
		}
	}

	// 11. AtomicTransitionDeployedByJob
	_, _ = pool.Exec(ctx, "UPDATE pr_registry SET status = 'merged', metadata = '{\"jobs\":[\"cadvisor\",\"node-exporter\"]}'::jsonb WHERE repo = $1 AND pr_number = $2", repo, testPRNum)
	if _, _, _, _, updated, err := reg.AtomicTransitionDeployedByJob(ctx, "cadvisor"); err != nil || updated {
		t.Fatalf("AtomicTransitionDeployedByJob first job: updated=%v err=%v", updated, err)
	}
	assertRow("", "", "deploying")

	if tID, pNum, _, rOut, updated, err := reg.AtomicTransitionDeployedByJob(ctx, "node-exporter"); err != nil || !updated || pNum != testPRNum || tID != targetID || rOut != repo {
		t.Fatalf("AtomicTransitionDeployedByJob second job: updated=%v pr=%d target=%s repo=%s err=%v", updated, pNum, tID, rOut, err)
	}
	assertRow("", "", "deployed")
}

// --- 10. Vanadis Build Queue Advancement & Wake Suppression Tests ---

func TestVanadisChunkAdvancementAndWakeSuppression(t *testing.T) {
	ctx := context.Background()

	t.Run("VanadisExecuteChunkArgs_KindAndInsertOpts", func(t *testing.T) {
		args := VanadisExecuteChunkArgs{BuildID: "build-01", ChunkIndex: 2}
		if got := args.Kind(); got != "vanadis_execute_chunk" {
			t.Fatalf("Kind() = %q, want vanadis_execute_chunk", got)
		}
		opts := args.InsertOpts()
		if opts.Queue != VanadisQueueName || opts.MaxAttempts != 3 || !opts.UniqueOpts.ByArgs {
			t.Fatalf("unexpected InsertOpts(): %+v", opts)
		}
	})

	t.Run("isHangarSyncedRepo_And_formatVanadisBuildCompletedPrompt", func(t *testing.T) {
		for _, repo := range []string{"azylman/aerial", "aerial", "AZYLMAN/AERIAL-CONFIG", "aerial-config"} {
			if !isHangarSyncedRepo(repo) {
				t.Errorf("expected isHangarSyncedRepo(%q) == true", repo)
			}
		}
		for _, repo := range []string{"azylman/mirrormere", "mirrormere", "azylman/aerial-sidecars", ""} {
			if isHangarSyncedRepo(repo) {
				t.Errorf("expected isHangarSyncedRepo(%q) == false", repo)
			}
		}

		if got := formatVanadisBuildCompletedPrompt(nil); got != "" {
			t.Errorf("expected empty prompt for nil result, got %q", got)
		}

		prompt := formatVanadisBuildCompletedPrompt(&VanadisAdvancementResult{
			BuildID:   "build-777",
			Tier:      3,
			Objective: "Ship Vanadis Phase 3",
			Chunks: []VanadisChunkSummary{
				{ChunkIndex: 0, Repo: "azylman/aerial", Title: "Schema", PRNumber: 101},
				{ChunkIndex: 1, Repo: "", Title: "Worker", PRNumber: 0},
			},
		})
		for _, sub := range []string{
			"Vanadis build build-777 completed (Tier 3, 2 chunks merged).",
			"Objective: Ship Vanadis Phase 3",
			"- Chunk 1/2: Schema (azylman/aerial, PR #101)",
			"- Chunk 2/2: Worker (azylman/aerial)",
			"All Vanadis build chunks have merged into main.",
		} {
			if !strings.Contains(prompt, sub) {
				t.Errorf("expected prompt to contain %q, got:\n%s", sub, prompt)
			}
		}
	})

	t.Run("advanceVanadisChunk_MidRun_And_Completed_And_Errors", func(t *testing.T) {
		validThread := "123456789012345678"

		// 1. Nil registry or non-positive PR
		srvNil := newTestRouter(Config{}, nil, nil, nil)
		if err := srvNil.advanceVanadisChunk(ctx, "aerial", 10); err != nil {
			t.Fatalf("expected nil on nil registry, got %v", err)
		}

		reg := &mockPRRegistry{}
		disp := &mockOutboundDispatcher{}
		inserter := &mockRiverInserter{}
		srv := newTestRouter(Config{}, nil, reg, disp, inserter)

		if err := srv.advanceVanadisChunk(ctx, "aerial", 0); err != nil {
			t.Fatalf("expected nil on prNumber=0, got %v", err)
		}

		// 2. Unmatched or unadvanced result
		reg.advanceVanadisRes = &VanadisAdvancementResult{Matched: false}
		if err := srv.advanceVanadisChunk(ctx, "aerial", 10); err != nil {
			t.Fatalf("expected nil on unmatched result, got %v", err)
		}

		// 3. Registry error
		reg.advanceVanadisErr = errors.New("db failure")
		if err := srv.advanceVanadisChunk(ctx, "aerial", 10); err == nil {
			t.Fatal("expected error from AdvanceVanadisChunkByPR")
		}
		reg.advanceVanadisErr = nil

		// 4. Mid-run chunk advancement: enqueues next chunk in River and sends DM
		reg.advanceVanadisRes = &VanadisAdvancementResult{
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
		}
		disp.directMessageErr = errors.New("dm transient failure") // should only log warning
		if err := srv.advanceVanadisChunk(ctx, "aerial", 42); err != nil {
			t.Fatalf("unexpected error on mid-run advance: %v", err)
		}
		disp.directMessageErr = nil
		if len(inserter.insertedVanadisJobs) != 1 || inserter.insertedVanadisJobs[0].BuildID != "build-mid" || inserter.insertedVanadisJobs[0].ChunkIndex != 1 {
			t.Fatalf("expected VanadisExecuteChunkArgs{build-mid, 1}, got %+v", inserter.insertedVanadisJobs)
		}
		// Mid-run advancement updates widget in-place (no chat DM)

		// 5. River insert error during mid-run advancement
		inserter.err = errors.New("river insert failed")
		if err := srv.advanceVanadisChunk(ctx, "aerial", 42); err == nil {
			t.Fatal("expected error when River insert fails")
		}
		inserter.err = nil

		// 6. Final build completion: dispatches completion DM + prompt
		disp.directMessageCalls = nil
		disp.promptCalls = nil
		reg.advanceVanadisRes = &VanadisAdvancementResult{
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
		}
		if err := srv.advanceVanadisChunk(ctx, "aerial", 42); err != nil {
			t.Fatalf("unexpected error on build completion: %v", err)
		}
		// Final build completion updates widget in-place with dmMsg: "" so only completion prompt runs
		if len(disp.DirectMessageCalls()) != 0 {
			t.Fatalf("expected 0 direct messages on build completion, got %+v", disp.DirectMessageCalls())
		}
		if len(disp.PromptCalls()) != 1 || !strings.Contains(disp.PromptCalls()[0].Prompt, "Vanadis build build-done completed") {
			t.Fatalf("expected completion prompt, got %+v", disp.PromptCalls())
		}
	})

	t.Run("ProcessHangarEvent_And_ProcessGitHubEvent_Wiring", func(t *testing.T) {
		validThread := "123456789012345678"
		reg := &mockPRRegistry{
			mergedTargetID:   validThread,
			closedTargetID:   validThread,
			backfillTargetID: validThread,
			resolvePRNum:     77,
			resolveTargetID:  validThread,
			advanceVanadisRes: &VanadisAdvancementResult{
				Matched:          true,
				Advanced:         true,
				BuildID:          "build-wire",
				ThreadID:         validThread,
				MergedChunkIndex: 0,
				MergedChunkTitle: "Chunk 1",
				NextChunkIndex:   1,
				Chunks:           []VanadisChunkSummary{{ChunkIndex: 0}, {ChunkIndex: 1}},
			},
		}
		disp := &mockOutboundDispatcher{}
		inserter := &mockRiverInserter{}
		srv := newTestRouter(Config{}, nil, reg, disp, inserter)
		srv.SetConflictCheckDelay(0)

		// 1. Hangar sync_success with valid TargetID advances Vanadis chunk
		if _, err := srv.ProcessHangarEvent(ctx, HangarDeployEvent{
			Event:    "sync_success",
			JobName:  "git-sync",
			Repo:     "azylman/aerial",
			PRNumber: 77,
			TargetID: validThread,
		}); err != nil {
			t.Fatalf("ProcessHangarEvent sync_success failed: %v", err)
		}
		if len(reg.advanceVanadisCalls) != 1 || reg.advanceVanadisCalls[0].prNumber != 77 {
			t.Fatalf("expected AdvanceVanadisChunkByPR call for PR 77, got %+v", reg.advanceVanadisCalls)
		}

		// 2. Hangar sync_success with empty TargetID still advances Vanadis chunk
		reg.resolveTargetID = ""
		if _, err := srv.ProcessHangarEvent(ctx, HangarDeployEvent{
			Event:    "sync_success",
			JobName:  "git-sync",
			Repo:     "azylman/aerial",
			PRNumber: 78,
		}); err != nil {
			t.Fatalf("ProcessHangarEvent sync_success without target_id failed: %v", err)
		}
		if len(reg.advanceVanadisCalls) != 2 || reg.advanceVanadisCalls[1].prNumber != 78 {
			t.Fatalf("expected AdvanceVanadisChunkByPR call for PR 78, got %+v", reg.advanceVanadisCalls)
		}
		reg.resolveTargetID = validThread

		// 3. Hangar sync_success error propagation
		reg.advanceVanadisErr = errors.New("advance err")
		if _, err := srv.ProcessHangarEvent(ctx, HangarDeployEvent{
			Event:    "sync_success",
			JobName:  "git-sync",
			Repo:     "azylman/aerial",
			PRNumber: 79,
			TargetID: validThread,
		}); err == nil {
			t.Fatal("expected error from ProcessHangarEvent when advanceVanadisChunk fails")
		}
		reg.advanceVanadisErr = nil

		// 4. GitHub pull_request closed (merged) on non-Hangar-synced repo advances Vanadis chunk
		reg.advanceVanadisCalls = nil
		prMergedMirrormere := `{"action":"closed","number":88,"pull_request":{"number":88,"merged":true,"merge_commit_sha":"abc1234","head":{"ref":"feat/ui","sha":"def5678"}},"repository":{"full_name":"azylman/mirrormere"}}`
		if _, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-1", []byte(prMergedMirrormere)); err != nil {
			t.Fatalf("ProcessGitHubEvent merged PR failed: %v", err)
		}
		if len(reg.advanceVanadisCalls) != 1 || reg.advanceVanadisCalls[0].prNumber != 88 {
			t.Fatalf("expected AdvanceVanadisChunkByPR for mirrormere#88, got %+v", reg.advanceVanadisCalls)
		}

		reg.advanceVanadisErr = errors.New("pr merge advance err")
		if _, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-1b", []byte(prMergedMirrormere)); err == nil {
			t.Fatal("expected error when advanceVanadisChunk fails on merged PR")
		}
		reg.advanceVanadisErr = nil

		// 5. GitHub pull_request closed (unmerged) calls FailVanadisChunkByPR
		reg.failVanadisErr = errors.New("fail chunk warning")
		prClosedUnmerged := `{"action":"closed","number":89,"pull_request":{"number":89,"merged":false,"head":{"ref":"feat/abandoned","sha":"def5678"}},"repository":{"full_name":"azylman/aerial"}}`
		if _, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-2", []byte(prClosedUnmerged)); err != nil {
			t.Fatalf("ProcessGitHubEvent unmerged PR failed: %v", err)
		}
		if len(reg.failVanadisCalls) != 1 || reg.failVanadisCalls[0].prNumber != 89 {
			t.Fatalf("expected FailVanadisChunkByPR for aerial#89, got %+v", reg.failVanadisCalls)
		}
		reg.failVanadisErr = nil

		// 6. GitHub push to main on non-Hangar-synced repo advances Vanadis chunk
		reg.advanceVanadisCalls = nil
		pushSidecars := `{"ref":"refs/heads/main","after":"1111222233334444555566667777888899990000","repository":{"full_name":"azylman/aerial-sidecars"}}`
		if _, err := srv.ProcessGitHubEvent(ctx, "push", "del-3", []byte(pushSidecars)); err != nil {
			t.Fatalf("ProcessGitHubEvent push failed: %v", err)
		}
		if len(reg.advanceVanadisCalls) != 1 || reg.advanceVanadisCalls[0].prNumber != 77 {
			t.Fatalf("expected AdvanceVanadisChunkByPR on push to aerial-sidecars, got %+v", reg.advanceVanadisCalls)
		}

		reg.advanceVanadisErr = errors.New("push advance err")
		if _, err := srv.ProcessGitHubEvent(ctx, "push", "del-3b", []byte(pushSidecars)); err == nil {
			t.Fatal("expected error when advanceVanadisChunk fails on push")
		}
		reg.advanceVanadisErr = nil
	})

	t.Run("dispatchDeploymentSuccessPrompt_WakeSuppression", func(t *testing.T) {
		validThread := "123456789012345678"
		reg := &mockPRRegistry{suppressVanadisRes: true}
		disp := &mockOutboundDispatcher{}
		srv := newTestRouter(Config{}, nil, reg, disp)

		// 1. Suppressed mid-run deploy sends DM only, no /prompt wake
		if err := srv.dispatchDeploymentSuccessPrompt(ctx, validThread, "vanadis", "azylman/aerial", "abc1234", 55, "dep-1"); err != nil {
			t.Fatalf("dispatchDeploymentSuccessPrompt failed: %v", err)
		}
		if len(disp.PromptCalls()) != 0 {
			t.Fatalf("expected 0 /prompt calls when suppressed, got %d", len(disp.PromptCalls()))
		}
		// Mid-run deploy_success updates widget in-place (no chat DM)
		if len(disp.DirectMessageCalls()) != 0 {
			t.Fatalf("expected 0 direct messages on suppressed deploy, got %+v", disp.DirectMessageCalls())
		}

		// 2. Suppression check error logs warning and falls back to normal /prompt dispatch
		reg.suppressVanadisRes = false
		reg.suppressVanadisErr = errors.New("db check err")
		disp.directMessageCalls = nil
		if err := srv.dispatchDeploymentSuccessPrompt(ctx, validThread, "vanadis", "azylman/aerial", "abc1234", 55, "dep-1"); err != nil {
			t.Fatalf("dispatchDeploymentSuccessPrompt fallback failed: %v", err)
		}
		if len(disp.PromptCalls()) != 1 {
			t.Fatalf("expected 1 /prompt call on fallback, got %d", len(disp.PromptCalls()))
		}
	})
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
	items   []VanadisChunkSummary
	idx     int
	scanErr error
	rowsErr error
}

func (r *mockVanadisPgxRows) Close()     {}
func (r *mockVanadisPgxRows) Err() error { return r.rowsErr }
func (r *mockVanadisPgxRows) Next() bool {
	if r.idx < len(r.items) {
		r.idx++
		return true
	}
	return false
}
func (r *mockVanadisPgxRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
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
	return t.queryRowFn(ctx, sql, args...)
}
func (t *mockVanadisPgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (t *mockVanadisPgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.queryFn(ctx, sql, args...)
}

type mockVanadisPgxPool struct {
	beginFn    func(ctx context.Context) (pgx.Tx, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (p *mockVanadisPgxPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.beginFn(ctx)
}
func (p *mockVanadisPgxPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.queryRowFn(ctx, sql, args...)
}
func (p *mockVanadisPgxPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if p.execFn != nil {
		return p.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (p *mockVanadisPgxPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.queryFn(ctx, sql, args...)
}

func TestPostgresPRRegistry_VanadisMethods_Unit(t *testing.T) {
	ctx := context.Background()

	// Typed nil pool guard
	nilReg := NewPostgresPRRegistry((*pgxpool.Pool)(nil))
	if res, err := nilReg.AdvanceVanadisChunkByPR(ctx, "aerial", 1); err != nil || res.Matched {
		t.Fatalf("expected empty result on nil pool, got %+v, err=%v", res, err)
	}
	if sup, err := nilReg.ShouldSuppressVanadisDeployWake(ctx, "aerial", 1); err != nil || sup {
		t.Fatalf("expected false on nil pool, got %v, err=%v", sup, err)
	}
	if failed, err := nilReg.FailVanadisChunkByPR(ctx, "aerial", 1); err != nil || failed {
		t.Fatalf("expected false on nil pool, got %v, err=%v", failed, err)
	}

	mockPool := &mockVanadisPgxPool{}
	reg := NewPostgresPRRegistry(mockPool)

	// 1. ShouldSuppressVanadisDeployWake
	mockPool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return mockVanadisPgxRow{scanFn: func(dest ...any) error {
			*(dest[0].(*bool)) = true
			return nil
		}}
	}
	if sup, err := reg.ShouldSuppressVanadisDeployWake(ctx, "aerial", 10); err != nil || !sup {
		t.Fatalf("expected true, got %v, err=%v", sup, err)
	}
	mockPool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return mockVanadisPgxRow{scanFn: func(dest ...any) error {
			return errors.New("query err")
		}}
	}
	if _, err := reg.ShouldSuppressVanadisDeployWake(ctx, "aerial", 10); err == nil {
		t.Fatal("expected error from ShouldSuppressVanadisDeployWake")
	}

	// 2. FailVanadisChunkByPR
	mockPool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return mockVanadisPgxRow{scanFn: func(dest ...any) error {
			return pgx.ErrNoRows
		}}
	}
	if failed, err := reg.FailVanadisChunkByPR(ctx, "aerial", 10); err != nil || failed {
		t.Fatalf("expected false on ErrNoRows, got %v, err=%v", failed, err)
	}
	mockPool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return mockVanadisPgxRow{scanFn: func(dest ...any) error {
			return errors.New("scan err")
		}}
	}
	if _, err := reg.FailVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected error on scan err")
	}
	mockPool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return mockVanadisPgxRow{scanFn: func(dest ...any) error {
			*(dest[0].(*string)) = "build-fail-1"
			return nil
		}}
	}
	mockPool.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, errors.New("exec build fail err")
	}
	if _, err := reg.FailVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected error on exec build fail err")
	}
	mockPool.execFn = nil
	if failed, err := reg.FailVanadisChunkByPR(ctx, "aerial", 10); err != nil || !failed {
		t.Fatalf("expected true on FailVanadisChunkByPR success, got %v, err=%v", failed, err)
	}

	// 3. AdvanceVanadisChunkByPR branches
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return nil, errors.New("begin tx err")
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected begin tx error")
	}

	populateChunkRow := func(cStatus, bStatus string, chunkIdx int) func(ctx context.Context, sql string, args ...any) pgx.Row {
		return func(ctx context.Context, sql string, args ...any) pgx.Row {
			return mockVanadisPgxRow{scanFn: func(dest ...any) error {
				*(dest[0].(*string)) = fmt.Sprintf("chunk-%d", chunkIdx)
				*(dest[1].(*string)) = "build-100"
				*(dest[2].(*int)) = chunkIdx
				*(dest[3].(*string)) = fmt.Sprintf("Title %d", chunkIdx)
				*(dest[4].(*string)) = cStatus
				*(dest[5].(*string)) = "123456789012345678"
				*(dest[6].(*int)) = 2
				*(dest[7].(*string)) = "Test Build Title"
				*(dest[8].(*string)) = "Test Objective"
				*(dest[9].(*string)) = bStatus
				*(dest[10].(*string)) = "msg-12345"
				return nil
			}}
		}
	}

	// ErrNoRows
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			rbErr: errors.New("rb warning"),
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockVanadisPgxRow{scanFn: func(dest ...any) error { return pgx.ErrNoRows }}
			},
		}, nil
	}
	if res, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err != nil || res.Matched {
		t.Fatalf("expected Matched=false on ErrNoRows, got %+v, err=%v", res, err)
	}

	// Select error
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockVanadisPgxRow{scanFn: func(dest ...any) error { return errors.New("select err") }}
			},
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected select error")
	}

	// Cancelled build or already merged chunk
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{queryRowFn: populateChunkRow("merged", "running", 0)}, nil
	}
	if res, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err != nil || !res.Matched || res.Advanced {
		t.Fatalf("expected Matched=true Advanced=false on already merged chunk, got %+v, err=%v", res, err)
	}

	// Update chunk error
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 0),
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, errors.New("update chunk err")
			},
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected update chunk error")
	}

	// Query all chunks error, scan error, rows.Err()
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 0),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return nil, errors.New("query chunks err")
			},
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected query chunks error")
	}

	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 0),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &mockVanadisPgxRows{items: []VanadisChunkSummary{{ID: "chunk-0"}}, scanErr: errors.New("scan chunk err")}, nil
			},
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected scan chunk error")
	}

	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 0),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &mockVanadisPgxRows{items: []VanadisChunkSummary{{ID: "chunk-0"}}, rowsErr: errors.New("rows iter err")}, nil
			},
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected rows iter error")
	}

	// Mid-run advancement (including fallbackIdx when earlier chunk is unmerged) + commit error & success
	twoChunks := []VanadisChunkSummary{
		{ID: "chunk-0", ChunkIndex: 0, Title: "Title 0", Status: "pending"},
		{ID: "chunk-1", ChunkIndex: 1, Title: "Title 1", Status: "pr_open"},
	}
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 1),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &mockVanadisPgxRows{items: twoChunks}, nil
			},
			commitErr: errors.New("commit mid-run err"),
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected commit mid-run error")
	}

	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 0),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &mockVanadisPgxRows{items: twoChunks}, nil
			},
		}, nil
	}
	resMid, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10)
	if err != nil || !resMid.Advanced || resMid.BuildCompleted || resMid.NextChunkIndex != 1 || resMid.Title != "Test Build Title" {
		t.Fatalf("expected mid-run advance to chunk 1, got %+v, err=%v", resMid, err)
	}

	// Build completion (all chunks merged): update build error, commit error, and success
	chunkStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	chunkEnd := chunkStart.Add(2 * time.Minute)
	allMergedChunks := []VanadisChunkSummary{
		{ID: "chunk-0", ChunkIndex: 0, Title: "Title 0", Status: "merged", PRNumber: 9, StartedAt: chunkStart, UpdatedAt: chunkEnd},
		{ID: "chunk-1", ChunkIndex: 1, Title: "Title 1", Status: "pr_open", PRNumber: 10},
	}
	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		calls := 0
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 1),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &mockVanadisPgxRows{items: allMergedChunks}, nil
			},
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				calls++
				if calls == 2 {
					return pgconn.CommandTag{}, errors.New("update build completed err")
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected update build completed error")
	}

	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 1),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &mockVanadisPgxRows{items: allMergedChunks}, nil
			},
			commitErr: errors.New("commit completed err"),
		}, nil
	}
	if _, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10); err == nil {
		t.Fatal("expected commit completed error")
	}

	mockPool.beginFn = func(ctx context.Context) (pgx.Tx, error) {
		return &mockVanadisPgxTx{
			queryRowFn: populateChunkRow("pr_open", "running", 1),
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &mockVanadisPgxRows{items: allMergedChunks}, nil
			},
		}, nil
	}
	resDone, err := reg.AdvanceVanadisChunkByPR(ctx, "aerial", 10)
	if err != nil || !resDone.Advanced || !resDone.BuildCompleted || resDone.BuildStatus != "completed" || resDone.NextChunkIndex != -1 || resDone.Title != "Test Build Title" || len(resDone.Chunks) != 2 || !resDone.Chunks[0].StartedAt.Equal(chunkStart) || !resDone.Chunks[0].UpdatedAt.Equal(chunkEnd) {
		t.Fatalf("expected build completed with chunk timestamps, got %+v, err=%v", resDone, err)
	}
}

func TestSyncPRWidget_Unit(t *testing.T) {
	ctx := context.Background()

	// 1. Nil receiver / dispatcher / invalid params
	var nilSrv *RouterServer
	if err := nilSrv.SyncPRWidget(ctx, "aerial", 1, "", "", nil); err != nil {
		t.Errorf("expected nil on nil server")
	}

	mockDisp := &mockOutboundDispatcher{
		widgetReturnMsgID: "msg-widget-101",
	}

	// 2. Standalone PR with mock pool
	prStart := time.Date(2026, 10, 16, 17, 30, 0, 0, time.UTC)
	prUpdate := prStart.Add(4 * time.Minute)
	pool := &mockVanadisPgxPool{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "FROM vanadis_chunks") {
				return &mockVanadisPgxRow{scanFn: func(dest ...any) error { return pgx.ErrNoRows }}
			}
			if strings.Contains(sql, "FROM pr_registry") {
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
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, nil
		},
	}

	reg := NewPostgresPRRegistry(pool)
	srv := newTestRouter(Config{}, nil, reg, mockDisp)

	err := srv.SyncPRWidget(ctx, "aerial", 101, "merged", "", nil)
	if err != nil {
		t.Fatalf("SyncPRWidget standalone PR failed: %v", err)
	}
	calls := mockDisp.WidgetCalls()
	if len(calls) == 0 {
		t.Fatalf("expected widget call dispatched")
	}
	lastCall := calls[len(calls)-1]
	if lastCall.ID != "pr-azylman/aerial-101" || lastCall.Status != "merged" || lastCall.ChannelID != "1555405874565091380" || len(lastCall.Chunks) != 1 || !lastCall.Chunks[0].StartedAt.Equal(prStart) || !lastCall.Chunks[0].UpdatedAt.After(prUpdate) {
		t.Errorf("unexpected widget request: %+v", lastCall)
	}

	// 3. Vanadis chunk match with mock pool (testing both pre-existing StartedAt and zero StartedAt fallback on status override)
	chunk0Start := time.Date(2026, 10, 16, 17, 0, 0, 0, time.UTC)
	chunk0Update := chunk0Start.Add(5 * time.Minute)
	vanadisPool := &mockVanadisPgxPool{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "FROM vanadis_chunks") {
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
			return &mockVanadisPgxRow{}
		},
		queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			chunks := []VanadisChunkSummary{
				{ID: "chunk-0", ChunkIndex: 0, Repo: "aerial", Title: "Chunk 0", Status: "merged", PRNumber: 101, StartedAt: chunk0Start, UpdatedAt: chunk0Update},
				{ID: "chunk-1", ChunkIndex: 1, Repo: "", Title: "Chunk 1", Status: "running", PRNumber: 102},
			}
			return &mockVanadisPgxRows{items: chunks}, nil
		},
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, nil
		},
	}

	srvVanadis := newTestRouter(Config{}, nil, NewPostgresPRRegistry(vanadisPool), mockDisp)
	err = srvVanadis.SyncPRWidget(ctx, "aerial", 102, "deployed", "Deploying now", []string{"job-a"})
	if err != nil {
		t.Fatalf("SyncPRWidget vanadis failed: %v", err)
	}
	calls = mockDisp.WidgetCalls()
	lastCall = calls[len(calls)-1]
	if lastCall.ID != "build-abc" || lastCall.Title != "Vanadis Build Title" || lastCall.Banner != "Deploying now" || len(lastCall.Chunks) != 2 || lastCall.Chunks[1].Status != "deployed" || lastCall.Chunks[1].Repo != "azylman/aerial" || !lastCall.Chunks[0].StartedAt.Equal(chunk0Start) || lastCall.Chunks[1].StartedAt.IsZero() || lastCall.Chunks[1].UpdatedAt.IsZero() {
		t.Errorf("unexpected vanadis widget request: %+v", lastCall)
	}
}

func TestDefaultOutboundDispatcher_DispatchWidget(t *testing.T) {
	ctx := context.Background()

	// 1. Invalid snowflake
	disp := NewDefaultOutboundDispatcher("", "", nil)
	if _, err := disp.DispatchWidget(ctx, StatusWidgetRequest{ChannelID: "bad"}); err == nil {
		t.Errorf("expected error for invalid snowflake")
	}

	// 2. Server 200 OK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/discord/widget" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":     "ok",
			"message_id": "msg-widget-dispatch-ok",
		})
	}))
	defer ts.Close()

	dispOK := NewDefaultOutboundDispatcher("", ts.URL, ts.Client())
	msgID, err := dispOK.DispatchWidget(ctx, StatusWidgetRequest{
		ChannelID: "1555405874565091380",
		ID:        "widget-1",
	})
	if err != nil || msgID != "msg-widget-dispatch-ok" {
		t.Fatalf("DispatchWidget failed: msgID=%q, err=%v", msgID, err)
	}

	// 3. Server 400 Bad Request
	ts400 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad payload"}`))
	}))
	defer ts400.Close()

	disp400 := NewDefaultOutboundDispatcher("", ts400.URL, ts400.Client())
	if _, err := disp400.DispatchWidget(ctx, StatusWidgetRequest{ChannelID: "1555405874565091380"}); err == nil {
		t.Errorf("expected error on 400 status")
	}

	// 4. Server bad JSON response
	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{not json`))
	}))
	defer tsBadJSON.Close()

	dispBadJSON := NewDefaultOutboundDispatcher("", tsBadJSON.URL, tsBadJSON.Client())
	if _, err := dispBadJSON.DispatchWidget(ctx, StatusWidgetRequest{ChannelID: "1555405874565091380"}); err == nil {
		t.Errorf("expected error on bad JSON response")
	}
}
