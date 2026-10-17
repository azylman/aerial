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
	mergedCalls         []struct{ repo string; prNumber int; mergeSHA string }
	syncCalls           []struct{ repo string; prNumber int; headSHA string }
	closedUnmergedCalls []struct{ repo string; prNumber int }
	ciFailedCalls       []struct{ repo string; prNumber int }
	ciFailedBySHACalls  []struct{ repo string; headSHA string }
	backfillCalls       []struct{ repo string; prNumber int; mergeSHA string }
	resolveBySHACalls   []struct{ repo string; sha string }

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

	conflictCalls    []struct{ repo string; prNumber int }
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

	deployFailedCalls    []struct{ repo string; prNumber int }
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

	advanceVanadisCalls []struct{ repo string; prNumber int }
	advanceVanadisRes   *VanadisAdvancementResult
	advanceVanadisErr   error

	suppressVanadisCalls []struct{ repo string; prNumber int }
	suppressVanadisRes   bool
	suppressVanadisErr   error

	failVanadisCalls []struct{ repo string; prNumber int }
	failVanadisRes   bool
	failVanadisErr   error

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
	m.deployFailedCalls = append(m.deployFailedCalls, struct{ repo string; prNumber int }{repo, prNumber})
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
	m.conflictCalls = append(m.conflictCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return m.conflictTargetID, m.conflictUpdated, nil
}

func (m *mockPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.mergedCalls = append(m.mergedCalls, struct{ repo string; prNumber int; mergeSHA string }{repo, prNumber, mergeSHA})
	return m.mergedTargetID, nil
}

func (m *mockPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.syncCalls = append(m.syncCalls, struct{ repo string; prNumber int; headSHA string }{repo, prNumber, headSHA})
	return m.syncTargetID, nil
}

func (m *mockPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.closedUnmergedCalls = append(m.closedUnmergedCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return m.closedTargetID, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	if err := m.checkErr(m.ciFailedErr); err != nil {
		return "", false, err
	}
	m.ciFailedCalls = append(m.ciFailedCalls, struct{ repo string; prNumber int }{repo, prNumber})
	return m.ciFailedTargetID, m.ciFailedUpdated, nil
}

func (m *mockPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	if err := m.checkErr(m.ciFailedBySHAErr); err != nil {
		return "", 0, false, err
	}
	m.ciFailedBySHACalls = append(m.ciFailedBySHACalls, struct{ repo string; headSHA string }{repo, headSHA})
	return m.ciFailedBySHATarget, m.ciFailedBySHAPRNum, m.ciFailedBySHAUpdated, nil
}

func (m *mockPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	m.backfillCalls = append(m.backfillCalls, struct{ repo string; prNumber int; mergeSHA string }{repo, prNumber, mergeSHA})
	return m.backfillTargetID, nil
}

func (m *mockPRRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	if err := m.checkErr(m.resolveErr); err != nil {
		return 0, "", "", err
	}
	m.resolveBySHACalls = append(m.resolveBySHACalls, struct{ repo string; sha string }{repo, sha})
	return m.resolvePRNum, m.resolveBranch, m.resolveTargetID, nil
}

func (m *mockPRRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	if err := m.checkErr(m.resolveErr); err != nil {
		return "", "", "", "", err
	}
	return m.resolveTargetID, m.resolveBranch, "", "", nil
}

func (m *mockPRRegistry) AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	m.advanceVanadisCalls = append(m.advanceVanadisCalls, struct{ repo string; prNumber int }{repo, prNumber})
	if err := m.checkErr(m.advanceVanadisErr); err != nil {
		return nil, err
	}
	if m.advanceVanadisRes != nil {
		return m.advanceVanadisRes, nil
	}
	return &VanadisAdvancementResult{}, nil
}

func (m *mockPRRegistry) ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.suppressVanadisCalls = append(m.suppressVanadisCalls, struct{ repo string; prNumber int }{repo, prNumber})
	if err := m.checkErr(m.suppressVanadisErr); err != nil {
		return false, err
	}
	return m.suppressVanadisRes, nil
}

func (m *mockPRRegistry) FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error) {
	m.failVanadisCalls = append(m.failVanadisCalls, struct{ repo string; prNumber int }{repo, prNumber})
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

func TestConfig(t *testing.T) {
	t.Run("LoadConfigFromEnv_InfisicalAndNomad", func(t *testing.T) {
		t.Setenv("PORT", "9999")
		t.Setenv("INFISICAL_URL", "http://infisical:8085/")
		t.Setenv("INFISICAL_CLIENT_ID", "cid")
		t.Setenv("INFISICAL_CLIENT_SECRET", "csec")
		t.Setenv("INFISICAL_PROJECT_ID", "pid")
		t.Setenv("INFISICAL_ENVIRONMENT", "staging")
		t.Setenv("NOMAD_ADDR", "http://nomad:4646/")
		t.Setenv("NOMAD_TOKEN", "ntok")

		cfg := LoadConfigFromEnv()
		if cfg.Port != "9999" || cfg.InfisicalURL != "http://infisical:8085" || cfg.NomadAddr != "http://nomad:4646" {
			t.Errorf("unexpected config loaded: %+v", cfg)
		}
		if cfg.InfisicalClientID != "cid" || cfg.InfisicalClientSecret != "csec" || cfg.InfisicalProjectID != "pid" {
			t.Errorf("unexpected infisical credentials: %+v", cfg)
		}
		if cfg.InfisicalEnvironment != "staging" {
			t.Errorf("unexpected env: %+v", cfg)
		}

		// Alternate environment variable keys
		t.Setenv("INFISICAL_URL", "")
		t.Setenv("INFISICAL_HOST_URL", "http://alternate:8085")
		t.Setenv("INFISICAL_CLIENT_ID", "")
		t.Setenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID", "u-cid")
		t.Setenv("INFISICAL_CLIENT_SECRET", "")
		t.Setenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET", "u-csec")
		t.Setenv("INFISICAL_PROJECT_ID", "")
		t.Setenv("INFISICAL_WORKSPACE_ID", "w-id")

		cfgAlt := LoadConfigFromEnv()
		if cfgAlt.InfisicalURL != "http://alternate:8085" || cfgAlt.InfisicalClientID != "u-cid" ||
			cfgAlt.InfisicalClientSecret != "u-csec" || cfgAlt.InfisicalProjectID != "w-id" {
			t.Errorf("unexpected alternate config: %+v", cfgAlt)
		}

		// Default fallback loading
		t.Setenv("INFISICAL_HOST_URL", "")
		t.Setenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID", "")
		t.Setenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET", "")
		t.Setenv("INFISICAL_WORKSPACE_ID", "")
		t.Setenv("PORT", "")
		t.Setenv("NOMAD_ADDR", "")
		t.Setenv("INFISICAL_ENVIRONMENT", "")
		t.Setenv("POSTGRES_URL", "")
		t.Setenv("DATABASE_URL", "")

		cfgDef := LoadConfigFromEnv()
		if cfgDef.Port != "4020" || cfgDef.InfisicalURL != "http://infisical.aerial" || cfgDef.NomadAddr != "http://127.0.0.1:4646" {
			t.Errorf("unexpected default config: %+v", cfgDef)
		}
	})

	t.Run("LoadConfigFromEnv_GitHubAndPostgres", func(t *testing.T) {
		t.Setenv("GITHUB_PAT", "pat123")
		t.Setenv("GITHUB_API_URL", "https://api.github.test/")
		t.Setenv("POSTGRES_URL", "postgres://custom:pass@db:5432/customdb")

		cfg := LoadConfigFromEnv()
		if cfg.GitHubToken != "pat123" || cfg.GitHubAPIURL != "https://api.github.test" || cfg.PostgresURL != "postgres://custom:pass@db:5432/customdb" {
			t.Errorf("unexpected GitHub/Postgres config: %+v", cfg)
		}

		// Fallback to GITHUB_TOKEN and DATABASE_URL
		t.Setenv("GITHUB_PAT", "")
		t.Setenv("POSTGRES_URL", "")
		t.Setenv("GITHUB_TOKEN", "tok456")
		t.Setenv("DATABASE_URL", "postgres://dbuser:pass@db:5432/db")

		cfg2 := LoadConfigFromEnv()
		if cfg2.GitHubToken != "tok456" || cfg2.PostgresURL != "postgres://dbuser:pass@db:5432/db" {
			t.Errorf("unexpected fallback GitHub/Database config: %+v", cfg2)
		}
	})

	t.Run("LoadConfig_YAMLAndEnvOverrides", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfgFile := filepath.Join(tmpDir, "webhooks-router.yaml")
		yamlContent := `
port: "4099"
infisical_url: "http://infisical.custom:8085"
infisical_environment: "staging"
nomad_addr: "http://nomad.custom:4646"
hangar_url: "http://hangar.custom:8087"
brain_url: "http://brain.custom:8088"
`
		if err := os.WriteFile(cfgFile, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("failed to write yaml: %v", err)
		}

		for _, k := range []string{"PORT", "INFISICAL_URL", "INFISICAL_HOST_URL", "INFISICAL_ENVIRONMENT", "NOMAD_ADDR", "HANGAR_URL", "BRAIN_URL"} {
			t.Setenv(k, "")
		}

		cfg := LoadConfig(cfgFile)
		if cfg.Port != "4099" || cfg.InfisicalURL != "http://infisical.custom:8085" || cfg.InfisicalEnvironment != "staging" ||
			cfg.NomadAddr != "http://nomad.custom:4646" || cfg.HangarURL != "http://hangar.custom:8087" || cfg.BrainURL != "http://brain.custom:8088" {
			t.Errorf("unexpected YAML config: %+v", cfg)
		}

		// Env overrides YAML
		t.Setenv("PORT", "5000")
		t.Setenv("INFISICAL_ENVIRONMENT", "dev")
		cfgOver := LoadConfig(cfgFile)
		if cfgOver.Port != "5000" || cfgOver.InfisicalEnvironment != "dev" || cfgOver.NomadAddr != "http://nomad.custom:4646" {
			t.Errorf("unexpected overridden YAML config: %+v", cfgOver)
		}
	})

	t.Run("LoadConfig_FallbackAndZeroByte", func(t *testing.T) {
		tmpDir := t.TempDir()
		localZero := filepath.Join(tmpDir, "local_zero.yaml")
		if err := os.WriteFile(localZero, []byte(""), 0644); err != nil {
			t.Fatalf("failed to create zero-byte file: %v", err)
		}

		fallbackValid := filepath.Join(tmpDir, "fallback_valid.yaml")
		fallbackContent := "port: \"4088\"\ninfisical_url: \"http://infisical.fallback:8085\"\nnomad_addr: \"http://nomad.fallback:4646\"\n"
		if err := os.WriteFile(fallbackValid, []byte(fallbackContent), 0644); err != nil {
			t.Fatalf("failed to create fallback config: %v", err)
		}

		oldFallback := webhooksRouterFallbackConfigPath
		webhooksRouterFallbackConfigPath = fallbackValid
		t.Cleanup(func() { webhooksRouterFallbackConfigPath = oldFallback })

		for _, k := range []string{"PORT", "INFISICAL_URL", "INFISICAL_HOST_URL", "NOMAD_ADDR"} {
			t.Setenv(k, "")
		}

		// 1. Zero-byte local config with fallback config existing
		cfgFallback := LoadConfig(localZero)
		if cfgFallback.Port != "4088" || cfgFallback.InfisicalURL != "http://infisical.fallback:8085" {
			t.Errorf("unexpected fallback config: %+v", cfgFallback)
		}

		// 2. Zero-byte local config with non-existent fallback -> uses defaults
		webhooksRouterFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
		cfgDefaults := LoadConfig(localZero)
		if cfgDefaults.Port != "4020" || cfgDefaults.InfisicalURL != "http://infisical.aerial" {
			t.Errorf("unexpected default config: %+v", cfgDefaults)
		}

		// 3. Empty configPath with valid fallback
		webhooksRouterFallbackConfigPath = fallbackValid
		if cfgEmptyPath := LoadConfig(""); cfgEmptyPath.Port != "4088" {
			t.Errorf("expected port 4088 from fallback on empty configPath, got %s", cfgEmptyPath.Port)
		}

		// 4. Non-empty local config takes precedence over fallback
		localValid := filepath.Join(tmpDir, "local_valid.yaml")
		_ = os.WriteFile(localValid, []byte("port: \"4077\"\n"), 0644)
		if cfgLocal := LoadConfig(localValid); cfgLocal.Port != "4077" {
			t.Errorf("expected local port 4077, got %s", cfgLocal.Port)
		}
	})

	t.Run("SystemChannelID", func(t *testing.T) {
		t.Setenv("SYSTEM_CHANNEL_ID", "123456789012345678")
		if cfg := LoadConfigFromEnv(); cfg.SystemChannelID != "123456789012345678" {
			t.Errorf("expected 123456789012345678, got %s", cfg.SystemChannelID)
		}
		t.Setenv("SYSTEM_CHANNEL_ID", "")

		t.Setenv("DISCORD_CHANNEL_ID", "987654321098765432")
		if cfg2 := LoadConfigFromEnv(); cfg2.SystemChannelID != "987654321098765432" {
			t.Errorf("expected 987654321098765432, got %s", cfg2.SystemChannelID)
		}
		t.Setenv("DISCORD_CHANNEL_ID", "")

		tmpDir := t.TempDir()
		yamlFile := filepath.Join(tmpDir, "config.yaml")
		if err := os.WriteFile(yamlFile, []byte("system_channel_id: \"112233445566778899\"\n"), 0644); err != nil {
			t.Fatalf("failed to write yaml config: %v", err)
		}
		if cfg3 := LoadConfig(yamlFile); cfg3.SystemChannelID != "112233445566778899" {
			t.Errorf("expected 112233445566778899 from yaml, got %s", cfg3.SystemChannelID)
		}
	})
}

func TestPayloadHelpers_TableDriven(t *testing.T) {
	t.Parallel()

	t.Run("InfisicalPayloadHelpers", func(t *testing.T) {
		t.Parallel()
		type dataStruct = struct {
			WorkspaceID string `json:"workspaceId"`
			ProjectID   string `json:"projectId"`
			Environment string `json:"environment"`
		}
		wsCases := []struct {
			payload InfisicalWebhookPayload
			want    string
		}{
			{InfisicalWebhookPayload{WorkspaceID: "ws-1"}, "ws-1"},
			{InfisicalWebhookPayload{ProjectID: "proj-1"}, "proj-1"},
			{InfisicalWebhookPayload{Data: &dataStruct{WorkspaceID: "ws-data"}}, "ws-data"},
			{InfisicalWebhookPayload{Data: &dataStruct{ProjectID: "proj-data"}}, "proj-data"},
			{InfisicalWebhookPayload{}, "fb"},
		}
		for _, tc := range wsCases {
			if got := tc.payload.GetWorkspaceID("fb"); got != tc.want {
				t.Errorf("GetWorkspaceID() = %q, want %q", got, tc.want)
			}
		}

		envCases := []struct {
			payload InfisicalWebhookPayload
			want    string
		}{
			{InfisicalWebhookPayload{Environment: "prod"}, "prod"},
			{InfisicalWebhookPayload{Data: &dataStruct{Environment: "staging"}}, "staging"},
			{InfisicalWebhookPayload{}, "fb"},
		}
		for _, tc := range envCases {
			if got := tc.payload.GetEnvironment("fb"); got != tc.want {
				t.Errorf("GetEnvironment() = %q, want %q", got, tc.want)
			}
		}
	})

	t.Run("IsZeroSHA", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			sha  string
			want bool
		}{
			{"", true},
			{"0000000000000000000000000000000000000000", true},
			{"0", true},
			{"000", true},
			{"0000000000000000000000000000000000000001", false},
			{"ed197571a381920f540e7a3c55b68008a45912a0", false},
		} {
			if got := isZeroSHA(tc.sha); got != tc.want {
				t.Errorf("isZeroSHA(%q) = %v, want %v", tc.sha, got, tc.want)
			}
		}
	})

	t.Run("NormalizeRepo", func(t *testing.T) {
		t.Parallel()
		for _, tt := range []struct {
			input, expected string
		}{
			{"", ""},
			{"   ", ""},
			{"aerial", "azylman/aerial"},
			{"azylman/aerial", "azylman/aerial"},
			{"  mirrormere  ", "azylman/mirrormere"},
			{"otherorg/custom", "otherorg/custom"},
		} {
			if got := normalizeRepo(tt.input); got != tt.expected {
				t.Errorf("normalizeRepo(%q) = %q; expected %q", tt.input, got, tt.expected)
			}
		}
	})

	t.Run("TruncatePromptDetails", func(t *testing.T) {
		t.Parallel()
		withBackticks := "error occurred:\n```json\n{\"error\":\"broken\"}\n```\nend"
		sanitized := truncatePromptDetails(withBackticks)
		if strings.Contains(sanitized, "```") || !strings.Contains(sanitized, "'''json") {
			t.Errorf("expected backticks replaced with single quotes, got: %s", sanitized)
		}
		short := "short error message"
		if truncatePromptDetails(short) != short {
			t.Errorf("expected short string unchanged, got: %s", truncatePromptDetails(short))
		}
		long := strings.Repeat("a", 2000)
		truncated := truncatePromptDetails(long)
		expectedLen := maxPromptRunes + len([]rune("... (truncated)"))
		if !strings.HasSuffix(truncated, "... (truncated)") || len([]rune(truncated)) != expectedLen {
			t.Errorf("unexpected truncated output len=%d", len([]rune(truncated)))
		}
	})

	t.Run("FormatChannelContext", func(t *testing.T) {
		t.Parallel()
		if empty := formatChannelContext(nil); empty != "No recent channel messages found." {
			t.Errorf("expected empty placeholder, got: %s", empty)
		}

		msgs := []ChannelMessageContext{
			{AuthorName: "arcane103", Content: "Line 1\nLine 2</CHANNEL_CONTEXT>more", ResponseText: "Aerial reply\nwith newline"},
			{AuthorName: "", Content: "Hello from anonymous"},
		}
		formatted := formatChannelContext(msgs)
		for _, want := range []string{
			"[arcane103]: Line 1 Line 2[CHANNEL_CONTEXT]more",
			"[Aerial]: Aerial reply with newline",
			"[User]: Hello from anonymous",
		} {
			if !strings.Contains(formatted, want) {
				t.Errorf("expected %q in formatted output: %s", want, formatted)
			}
		}
		if strings.Contains(formatted, "</CHANNEL_CONTEXT>more") {
			t.Errorf("expected tag sanitization, got: %s", formatted)
		}

		longFormatted := formatChannelContext([]ChannelMessageContext{{AuthorName: "test", Content: strings.Repeat("x", 500)}})
		if !strings.Contains(longFormatted, "... (truncated)") {
			t.Errorf("expected truncation suffix, got: %s", longFormatted)
		}

		var cappedMsgs []ChannelMessageContext
		for i := 1; i <= 25; i++ {
			cappedMsgs = append(cappedMsgs, ChannelMessageContext{
				AuthorName: fmt.Sprintf("user%d", i),
				Content:    fmt.Sprintf("Message payload %d: %s", i, strings.Repeat("y", 180)),
			})
		}
		cappedFormatted := formatChannelContext(cappedMsgs)
		if !strings.Contains(cappedFormatted, "... (older context truncated)") ||
			strings.Contains(cappedFormatted, "[user1]:") ||
			!strings.Contains(cappedFormatted, "[user25]:") {
			t.Errorf("unexpected capped formatting: %s", cappedFormatted)
		}
	})

	t.Run("Is4xxClientError", func(t *testing.T) {
		t.Parallel()
		for _, tt := range []struct {
			name     string
			err      error
			expected bool
		}{
			{"nil error", nil, false},
			{"status 400", errors.New("brain prompt returned status 400: bad request"), true},
			{"status 404", errors.New("brain prompt returned status 404: not found"), true},
			{"status 422", errors.New("brain internal message returned status 422: unprocessable"), true},
			{"status 403", errors.New("brain prompt returned status 403: forbidden"), true},
			{"status 500", errors.New("brain prompt returned status 500: internal server error"), false},
			{"status 502", errors.New("brain prompt returned status 502: bad gateway"), false},
			{"network timeout", errors.New("dial tcp 127.0.0.1:8088: i/o timeout"), false},
			{"invalid snowflake", errors.New("invalid discord snowflake channel id: \"abc\""), true},
		} {
			if got := is4xxClientError(tt.err); got != tt.expected {
				t.Errorf("%s: is4xxClientError(%v) = %v, expected %v", tt.name, tt.err, got, tt.expected)
			}
		}
	})
}

// --- 2. Infisical / Generic Secret Webhooks & Sync ---

func TestHandleInfisicalAndGenericWebhooks(t *testing.T) {
	t.Parallel()
	routes := NewRouterServer(Config{}, nil).Routes()

	cases := []struct {
		name       string
		path       string
		body       io.Reader
		wantStatus int
		checkBody  func(t *testing.T, resp map[string]interface{})
	}{
		{
			name:       "infisical valid payload",
			path:       "/webhooks/infisical",
			body:       strings.NewReader(`{"event":"secrets.modified"}`),
			wantStatus: http.StatusAccepted,
			checkBody: func(t *testing.T, resp map[string]interface{}) {
				if resp["targetVariable"] != TargetNomadVariable {
					t.Errorf("expected targetVariable %q, got %v", TargetNomadVariable, resp["targetVariable"])
				}
			},
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
			name:       "generic standard sync",
			path:       "/webhooks/generic",
			body:       strings.NewReader(`{"environment":"prod"}`),
			wantStatus: http.StatusAccepted,
			checkBody: func(t *testing.T, resp map[string]interface{}) {
				if resp["targetVariable"] != TargetNomadVariable {
					t.Errorf("expected targetVariable %q, got %v", TargetNomadVariable, resp["targetVariable"])
				}
			},
		},
		{
			name:       "generic direct secrets map",
			path:       "/webhooks/generic",
			body:       strings.NewReader(`{"secrets":{"KEY1":"VAL1"}}`),
			wantStatus: http.StatusAccepted,
			checkBody: func(t *testing.T, resp map[string]interface{}) {
				if resp["targetVariable"] != TargetNomadVariable || resp["message"] != "Direct secret sync scheduled" {
					t.Errorf("unexpected direct payload handling: %+v", resp)
				}
			},
		},
		{
			name:       "generic explicit workspace and env",
			path:       "/webhooks/generic",
			body:       strings.NewReader(`{"workspaceId":"custom-ws","environment":"dev"}`),
			wantStatus: http.StatusAccepted,
			checkBody: func(t *testing.T, resp map[string]interface{}) {
				if resp["targetVariable"] != TargetNomadVariable || resp["environment"] != "dev" {
					t.Errorf("unexpected explicit payload handling: %+v", resp)
				}
			},
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
			if tc.checkBody != nil {
				var resp map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse response: %v", err)
				}
				tc.checkBody(t, resp)
			}
		})
	}
}

func TestSyncSecrets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("FullFlowAndMissingCreds", func(t *testing.T) {
		t.Parallel()
		var nomadPutCount atomic.Int32
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/auth/universal-auth/login":
				_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{AccessToken: "mock-jwt-token"})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw":
				_ = json.NewEncoder(w).Encode(InfisicalSecretsResponse{
					Secrets: []InfisicalSecretItem{{SecretKey: "API_KEY", SecretValue: "secret123"}},
				})
			case r.Method == http.MethodPut && r.URL.Path == "/v1/var/"+TargetNomadVariable:
				nomadPutCount.Add(1)
				w.WriteHeader(http.StatusOK)
			default:
				http.NotFound(w, r)
			}
		}))
		defer mockServer.Close()

		server := NewRouterServer(Config{
			InfisicalURL:          mockServer.URL,
			InfisicalClientID:     "client-id",
			InfisicalClientSecret: "client-secret",
			InfisicalProjectID:    "proj-123",
			InfisicalEnvironment:  "prod",
			NomadAddr:             mockServer.URL,
			NomadToken:            "test-nomad-token",
		}, mockServer.Client())

		if err := server.SyncSecrets(ctx, "proj-123", "prod"); err != nil {
			t.Fatalf("SyncSecrets failed: %v", err)
		}
		if nomadPutCount.Load() != 1 {
			t.Errorf("expected 1 Nomad PUT, got %d", nomadPutCount.Load())
		}

		// Missing Infisical Credentials
		serverBad := NewRouterServer(Config{}, mockServer.Client())
		if err := serverBad.SyncSecrets(ctx, "", ""); err == nil {
			t.Errorf("expected error with unconfigured infisical credentials")
		}
	})

	t.Run("ErrorBranchesAndStatusCodes", func(t *testing.T) {
		t.Parallel()
		var mode atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch mode.Load() {
			case 1: // Login 500
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("server error"))
			case 2: // Login invalid JSON
				_, _ = w.Write([]byte("{invalid-json"))
			case 3: // Login empty token
				_, _ = w.Write([]byte(`{"accessToken":""}`))
			case 4: // Login fallback token field
				_, _ = w.Write([]byte(`{"token":"fallback-token"}`))
			case 5: // Fetch 502
				if r.URL.Path == "/api/v1/auth/universal-auth/login" {
					_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{AccessToken: "tok"})
					return
				}
				w.WriteHeader(http.StatusBadGateway)
			case 6: // Fetch invalid JSON
				_, _ = w.Write([]byte("{invalid-secrets"))
			case 7: // Nomad PUT 201
				w.WriteHeader(http.StatusCreated)
			case 8: // Nomad PUT 204
				w.WriteHeader(http.StatusNoContent)
			case 9: // Nomad PUT 500
				switch r.URL.Path {
				case "/api/v1/auth/universal-auth/login":
					_ = json.NewEncoder(w).Encode(UniversalAuthLoginResponse{AccessToken: "tok"})
				case "/api/v3/secrets/raw":
					_ = json.NewEncoder(w).Encode(InfisicalSecretsResponse{
						Secrets: []InfisicalSecretItem{{SecretKey: "K", SecretValue: "V"}},
					})
				default:
					w.WriteHeader(http.StatusInternalServerError)
				}
			}
		}))
		defer ts.Close()

		srv := NewRouterServer(Config{
			InfisicalURL:          ts.URL,
			InfisicalClientID:     "cid",
			InfisicalClientSecret: "csec",
			NomadAddr:             ts.URL,
		}, ts.Client())

		mode.Store(1)
		if err := srv.SyncSecrets(ctx, "", ""); err == nil {
			t.Errorf("expected error on infisical login 500")
		}

		mode.Store(2)
		if _, err := srv.getInfisicalToken(ctx); err == nil {
			t.Errorf("expected error on invalid login json")
		}

		mode.Store(3)
		if _, err := srv.getInfisicalToken(ctx); err == nil {
			t.Errorf("expected error on empty login token")
		}

		mode.Store(4)
		if tok, err := srv.getInfisicalToken(ctx); err != nil || tok != "fallback-token" {
			t.Errorf("expected fallback token, got %q, err: %v", tok, err)
		}

		mode.Store(5)
		if err := srv.SyncSecrets(ctx, "", ""); err == nil {
			t.Errorf("expected error on infisical fetch 502")
		}

		mode.Store(6)
		if _, err := srv.fetchInfisicalSecrets(ctx, "tok", "ws", "env"); err == nil {
			t.Errorf("expected error on invalid secrets json")
		}

		mode.Store(7)
		if err := srv.putNomadVariable(ctx, map[string]string{"A": "B"}); err != nil {
			t.Errorf("expected success on 201 Created: %v", err)
		}

		mode.Store(8)
		if err := srv.putNomadVariable(ctx, map[string]string{"A": "B"}); err != nil {
			t.Errorf("expected success on 204 No Content: %v", err)
		}

		mode.Store(9)
		if err := srv.SyncSecrets(ctx, "", ""); err == nil {
			t.Errorf("expected error on nomad put 500")
		}

		// Connection errors on unreachable client URL
		badClientServer := NewRouterServer(Config{
			InfisicalURL:          "http://127.0.0.1:1",
			InfisicalClientID:     "cid",
			InfisicalClientSecret: "csec",
			NomadAddr:             "http://127.0.0.1:1",
		}, &http.Client{Timeout: 50 * time.Millisecond})

		if _, err := badClientServer.getInfisicalToken(ctx); err == nil {
			t.Errorf("expected error on bad login client URL")
		}
		if _, err := badClientServer.fetchInfisicalSecrets(ctx, "tok", "ws", "env"); err == nil {
			t.Errorf("expected error on bad fetch client URL")
		}
		if err := badClientServer.putNomadVariable(ctx, map[string]string{"A": "B"}); err == nil {
			t.Errorf("expected error on bad nomad put client URL")
		}
	})
}

// --- 3. GitHub Webhook HTTP Handler & Event Processing ---

func TestHandleGitHubWebhook(t *testing.T) {
	t.Parallel()

	t.Run("EndpointsAndValidation", func(t *testing.T) {
		t.Parallel()
		mockRiver := &mockRiverInserter{}
		routes := NewRouterServer(Config{}, nil, mockRiver).Routes()

		// 1. Missing X-GitHub-Event header -> 400
		if rec := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/github", strings.NewReader(`{}`), nil); rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for missing event header, got %d", rec.Code)
		}

		// 2. Read error in body -> 400
		if rec := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/github", errReader{}, map[string]string{"X-GitHub-Event": "ping"}); rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 on read error, got %d", rec.Code)
		}

		// 3. River client nil -> 500
		srvNilRiver := NewRouterServer(Config{}, nil).Routes()
		if rec := doHTTPRequest(srvNilRiver, http.MethodPost, "/api/webhooks/github", strings.NewReader(`{"zen":"Keep it simple"}`), map[string]string{"X-GitHub-Event": "ping"}); rec.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when river client is nil, got %d", rec.Code)
		}

		// 4. River client insert error -> 500
		srvErrRiver := NewRouterServer(Config{}, nil, &mockRiverInserter{err: errors.New("simulated database failure")}).Routes()
		if rec := doHTTPRequest(srvErrRiver, http.MethodPost, "/api/webhooks/github", strings.NewReader(`{"zen":"Keep it simple"}`), map[string]string{"X-GitHub-Event": "ping"}); rec.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when river persistence fails, got %d", rec.Code)
		}

		// 5. Successful durable persistence on both routes -> 200 OK
		for _, path := range []string{"/api/webhooks/github", "/webhooks/github"} {
			mockRiver.insertedJobs = nil
			rec := doHTTPRequest(routes, http.MethodPost, path, strings.NewReader(`{"zen":"Keep it simple"}`), map[string]string{
				"X-GitHub-Event":    "ping",
				"X-GitHub-Delivery": "del-1234",
			})
			if rec.Code != http.StatusOK {
				t.Errorf("expected 200 OK on %s, got %d", path, rec.Code)
			}
			var resp map[string]interface{}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to parse json response: %v", err)
			}
			if resp["status"] != "accepted" || resp["event"] != "ping" || resp["delivery"] != "del-1234" {
				t.Errorf("unexpected response body: %+v", resp)
			}
			if len(mockRiver.insertedJobs) != 1 || mockRiver.insertedJobs[0].Event != "ping" || mockRiver.insertedJobs[0].Delivery != "del-1234" {
				t.Errorf("unexpected inserted jobs: %+v", mockRiver.insertedJobs)
			}
		}
	})

	t.Run("DuplicateSkipped", func(t *testing.T) {
		t.Parallel()
		mockRiver := &mockRiverInserter{skipDuplicate: true}
		routes := NewRouterServer(Config{}, nil, mockRiver).Routes()

		body := `{"zen":"Responsive is better than fast.","hook_id":9999,"repository":{"full_name":"azylman/aerial"}}`
		rec := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/github", strings.NewReader(body), map[string]string{
			"X-GitHub-Event":    "ping",
			"X-GitHub-Delivery": "dup-gh-delivery-1",
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on duplicate, got %d", rec.Code)
		}

		recM := doHTTPRequest(routes, http.MethodGet, "/metrics", nil, nil)
		if !strings.Contains(recM.Body.String(), `webhooks_router_unique_skipped_total{kind="github_webhook"}`) {
			t.Errorf("expected /metrics to include github_webhook duplicate count, got:\n%s", recM.Body.String())
		}

		opts := (GitHubWebhookArgs{Delivery: "test-delivery"}).InsertOpts()
		if !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
			t.Errorf("unexpected GitHubWebhookArgs InsertOpts: %+v", opts)
		}
	})
}

func TestProcessGitHubEvent_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/commits/commit-fallback/pulls"):
			_, _ = w.Write([]byte(`[{"number": 777, "state": "open", "head": {"ref": "feat/fallback", "sha": "commit-fallback"}}]`))
		case strings.Contains(r.URL.Path, "/commits/wf-fallback/pulls"):
			_, _ = w.Write([]byte(`[{"number": 888, "state": "open", "head": {"ref": "feat/wf-branch", "sha": "wf-fallback"}}]`))
		case strings.Contains(r.URL.Path, "/commits/squash123/pulls"):
			_, _ = w.Write([]byte(`[{"number": 999, "state": "closed", "head": {"ref": "feat/squash", "sha": "squash123"}}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(mockServer.Close)

	srv := NewRouterServer(Config{GitHubAPIURL: mockServer.URL}, mockServer.Client())

	cases := []struct {
		name     string
		event    string
		delivery string
		payload  string
		wantErr  bool
		check    func(t *testing.T, res *ResolvedGitHubEvent)
	}{
		{
			name:     "ping valid",
			event:    "ping",
			delivery: "del-ping-1",
			payload:  `{"zen":"Responsive is better than fast.","hook_id":98765,"repository":{"full_name":"azylman/aerial"}}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.Event != "ping" || res.Repo != "azylman/aerial" {
					t.Errorf("unexpected ping result: %+v", res)
				}
			},
		},
		{
			name:     "ping malformed",
			event:    "ping",
			delivery: "del-ping-2",
			payload:  `{invalid`,
			wantErr:  true,
		},
		{
			name:     "pull_request closed and merged",
			event:    "pull_request",
			delivery: "del-pr-1",
			payload: `{
				"action": "closed",
				"number": 512,
				"pull_request": {
					"number": 512,
					"title": "feat: test pr",
					"head": {"ref": "feat/test", "sha": "headsha123"},
					"base": {"ref": "main", "sha": "basesha456"},
					"merged": true,
					"merge_commit_sha": "mergesha789"
				},
				"repository": {"full_name": "azylman/aerial"},
				"sender": {"login": "arcane103"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.Repo != "azylman/aerial" || res.PRNumber != 512 || res.Action != "closed" ||
					res.HeadSHA != "headsha123" || res.MergeSHA != "mergesha789" || res.Branch != "feat/test" ||
					res.ResolutionSource != "pull_request_payload" || res.Sender != "arcane103" {
					t.Errorf("unexpected pull_request result: %+v", res)
				}
			},
		},
		{
			name:     "pull_request malformed",
			event:    "pull_request",
			delivery: "del-pr-2",
			payload:  `{invalid`,
			wantErr:  true,
		},
		{
			name:     "check_run with pull_requests array",
			event:    "check_run",
			delivery: "del-cr-1",
			payload: `{
				"action": "completed",
				"check_run": {
					"id": 111, "name": "Unit Tests", "head_sha": "headsha111",
					"status": "completed", "conclusion": "success",
					"pull_requests": [{"number": 515, "head": {"ref": "feat/awesome", "sha": "headsha111"}}]
				},
				"repository": {"full_name": "azylman/aerial"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.PRNumber != 515 || res.Branch != "feat/awesome" || res.ResolutionSource != "check_run_payload" || res.Conclusion != "success" {
					t.Errorf("unexpected check_run result: %+v", res)
				}
			},
		},
		{
			name:     "check_run fallback via commit SHA lookup",
			event:    "check_run",
			delivery: "del-cr-2",
			payload: `{
				"action": "completed",
				"check_run": {
					"id": 222, "name": "Integration Tests", "head_sha": "commit-fallback",
					"status": "completed", "conclusion": "failure", "pull_requests": []
				},
				"repository": {"full_name": "azylman/aerial"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.PRNumber != 777 || res.Branch != "feat/fallback" || res.ResolutionSource != "commit_sha_lookup" || res.Conclusion != "failure" {
					t.Errorf("unexpected check_run fallback result: %+v", res)
				}
			},
		},
		{
			name:     "check_run malformed",
			event:    "check_run",
			delivery: "del-cr-3",
			payload:  `{invalid`,
			wantErr:  true,
		},
		{
			name:     "workflow_run with PR array",
			event:    "workflow_run",
			delivery: "del-wf-1",
			payload: `{
				"action": "completed",
				"workflow_run": {
					"id": 333, "name": "Continuous Delivery", "head_sha": "headsha333",
					"head_branch": "feat/cd", "status": "completed", "conclusion": "success",
					"pull_requests": [{"number": 516, "head": {"ref": "feat/cd", "sha": "headsha333"}}]
				},
				"repository": {"full_name": "azylman/aerial"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.PRNumber != 516 || res.Branch != "feat/cd" || res.ResolutionSource != "workflow_run_payload" {
					t.Errorf("unexpected workflow_run result: %+v", res)
				}
			},
		},
		{
			name:     "workflow_run fallback to commit lookup",
			event:    "workflow_run",
			delivery: "del-wf-2",
			payload: `{
				"action": "completed",
				"workflow_run": {
					"id": 444, "name": "Continuous Delivery", "head_sha": "wf-fallback",
					"status": "completed", "conclusion": "success", "pull_requests": []
				},
				"repository": {"full_name": "azylman/aerial"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.PRNumber != 888 || res.Branch != "feat/wf-branch" || res.ResolutionSource != "commit_sha_lookup" {
					t.Errorf("unexpected workflow_run fallback result: %+v", res)
				}
			},
		},
		{
			name:     "workflow_run malformed",
			event:    "workflow_run",
			delivery: "del-wf-3",
			payload:  `{invalid`,
			wantErr:  true,
		},
		{
			name:     "push to main with commit resolution",
			event:    "push",
			delivery: "del-push-1",
			payload: `{
				"ref": "refs/heads/main", "before": "prev111", "after": "squash123", "deleted": false,
				"head_commit": {"id": "squash123", "message": "feat: merged feature (#999)"},
				"repository": {"full_name": "azylman/aerial"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.PRNumber != 999 || res.MergeSHA != "squash123" || res.ResolutionSource != "commit_sha_lookup" {
					t.Errorf("unexpected push result: %+v", res)
				}
			},
		},
		{
			name:     "push branch deletion",
			event:    "push",
			delivery: "del-push-2",
			payload: `{
				"ref": "refs/heads/feat/temp", "before": "temp111",
				"after": "0000000000000000000000000000000000000000", "deleted": true,
				"repository": {"full_name": "azylman/aerial"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.ResolutionSource != "branch_deleted" || res.PRNumber != 0 {
					t.Errorf("unexpected push deletion result: %+v", res)
				}
			},
		},
		{
			name:     "push tag non-branch ref",
			event:    "push",
			delivery: "del-push-3",
			payload:  `{"ref":"refs/tags/v1.0.0","after":"tagsha111","repository":{"full_name":"azylman/aerial"}}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.ResolutionSource != "non_branch_ref" || res.PRNumber != 0 {
					t.Errorf("unexpected push tag result: %+v", res)
				}
			},
		},
		{
			name:     "push feature branch",
			event:    "push",
			delivery: "del-push-feat",
			payload: `{
				"ref": "refs/heads/feat/new-feature", "before": "featprev", "after": "featsha222",
				"head_commit": {"id": "featsha222", "message": "feat: new feature"},
				"repository": {"full_name": "azylman/aerial"}
			}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.ResolutionSource != "non_main_branch" || res.PRNumber != 0 {
					t.Errorf("unexpected push feat result: %+v", res)
				}
			},
		},
		{
			name:     "push malformed",
			event:    "push",
			delivery: "del-push-4",
			payload:  `{invalid`,
			wantErr:  true,
		},
		{
			name:     "unhandled event",
			event:    "star",
			delivery: "del-star-1",
			payload:  `{"action":"created"}`,
			check: func(t *testing.T, res *ResolvedGitHubEvent) {
				if res.ResolutionSource != "unhandled_event" {
					t.Errorf("expected unhandled_event, got %s", res.ResolutionSource)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := srv.ProcessGitHubEvent(ctx, tc.event, tc.delivery, []byte(tc.payload))
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessGitHubEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && tc.check != nil {
				tc.check(t, res)
			}
		})
	}
}

func TestProcessGitHubEvent_RegistryAndDispatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("RegistryTransitionsAndLocalResolution", func(t *testing.T) {
		t.Parallel()
		mockReg := &mockPRRegistry{
			mergedTargetID:       "target-merged-1",
			closedTargetID:       "target-closed-1",
			syncTargetID:         "target-sync-1",
			ciFailedTargetID:     "target-111",
			ciFailedUpdated:      true,
			ciFailedBySHAPRNum:   520,
			ciFailedBySHATarget:  "target-222",
			ciFailedBySHAUpdated: true,
		}
		srv := newTestRouter(Config{}, nil, mockReg, nil)

		// 1. PullRequest closed and merged
		payloadPRMerged := `{"action":"closed","number":515,"pull_request":{"number":515,"head":{"ref":"feat/decouple","sha":"head111"},"merged":true,"merge_commit_sha":"merge111"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`
		resPRMerged, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged", []byte(payloadPRMerged))
		if err != nil || len(mockReg.mergedCalls) != 1 || mockReg.mergedCalls[0].prNumber != 515 || mockReg.mergedCalls[0].mergeSHA != "merge111" || resPRMerged.TargetID != "target-merged-1" {
			t.Fatalf("pull_request merged failed: err=%v, calls=%+v, res=%+v", err, mockReg.mergedCalls, resPRMerged)
		}

		// 2. PullRequest closed unmerged
		payloadPRClosed := `{"action":"closed","number":516,"pull_request":{"number":516,"head":{"ref":"feat/discard","sha":"head222"},"merged":false},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`
		resPRClosed, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-closed", []byte(payloadPRClosed))
		if err != nil || len(mockReg.closedUnmergedCalls) != 1 || mockReg.closedUnmergedCalls[0].prNumber != 516 || resPRClosed.TargetID != "target-closed-1" {
			t.Fatalf("pull_request closed unmerged failed: err=%v, calls=%+v, res=%+v", err, mockReg.closedUnmergedCalls, resPRClosed)
		}

		// 3. PullRequest synchronize
		payloadPRSync := `{"action":"synchronize","number":517,"pull_request":{"number":517,"head":{"ref":"feat/update","sha":"newhead333"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`
		resPRSync, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-sync", []byte(payloadPRSync))
		if err != nil || len(mockReg.syncCalls) != 1 || mockReg.syncCalls[0].prNumber != 517 || mockReg.syncCalls[0].headSHA != "newhead333" || resPRSync.TargetID != "target-sync-1" {
			t.Fatalf("pull_request synchronize failed: err=%v, calls=%+v, res=%+v", err, mockReg.syncCalls, resPRSync)
		}

		// 4 & 5. CheckRun failure & timed_out with PR in payload
		for i, tc := range []struct {
			del, conclusion, sha string
			pr                   int
		}{
			{"del-cr-fail", "failure", "checkhead444", 518},
			{"del-cr-timeout", "timed_out", "checkhead555", 519},
		} {
			payload := fmt.Sprintf(`{"action":"completed","check_run":{"name":"Tests","head_sha":%q,"status":"completed","conclusion":%q,"pull_requests":[{"number":%d,"head":{"ref":"feat/test","sha":%q}}]},"repository":{"full_name":"azylman/aerial"}}`, tc.sha, tc.conclusion, tc.pr, tc.sha)
			if _, err := srv.ProcessGitHubEvent(ctx, "check_run", tc.del, []byte(payload)); err != nil {
				t.Fatalf("check_run %s failed: %v", tc.conclusion, err)
			}
			if len(mockReg.ciFailedCalls) != i+1 || mockReg.ciFailedCalls[i].prNumber != tc.pr {
				t.Errorf("unexpected ciFailedCalls at %d: %+v", i, mockReg.ciFailedCalls)
			}
		}

		// 6. CheckRun failure without PR (falls back to headSHA transition)
		payloadCRFallback := `{"action":"completed","check_run":{"name":"Lint","head_sha":"orphansha666","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`
		if _, err := srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-fallback", []byte(payloadCRFallback)); err != nil || len(mockReg.ciFailedBySHACalls) != 1 || mockReg.ciFailedBySHACalls[0].headSHA != "orphansha666" {
			t.Errorf("check_run fallback failed: err=%v, calls=%+v", err, mockReg.ciFailedBySHACalls)
		}

		// 7. WorkflowRun failure with PR
		payloadWFFail := `{"action":"completed","workflow_run":{"name":"CI","head_sha":"wfhead777","status":"completed","conclusion":"failure","pull_requests":[{"number":521,"head":{"ref":"feat/wf","sha":"wfhead777"}}]},"repository":{"full_name":"azylman/aerial"}}`
		if _, err := srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-fail", []byte(payloadWFFail)); err != nil || len(mockReg.ciFailedCalls) != 3 || mockReg.ciFailedCalls[2].prNumber != 521 {
			t.Errorf("workflow_run failure failed: err=%v, calls=%+v", err, mockReg.ciFailedCalls)
		}

		// 8. WorkflowRun timed_out without PR
		payloadWFTimeout := `{"action":"completed","workflow_run":{"name":"Build Docker","head_sha":"wfhead888","status":"completed","conclusion":"timed_out","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`
		if _, err := srv.ProcessGitHubEvent(ctx, "workflow_run", "del-wf-timeout", []byte(payloadWFTimeout)); err != nil || len(mockReg.ciFailedBySHACalls) != 2 || mockReg.ciFailedBySHACalls[1].headSHA != "wfhead888" {
			t.Errorf("workflow_run timeout failed: err=%v, calls=%+v", err, mockReg.ciFailedBySHACalls)
		}

		// 9. Push to main with resolved PR vs feature branch push
		ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]GitHubCommitPRItem{{Number: 522, State: "closed"}})
		}))
		defer ghMock.Close()

		srvWithGH := newTestRouter(Config{GitHubAPIURL: ghMock.URL}, ghMock.Client(), mockReg, nil)
		payloadPush := `{"ref":"refs/heads/main","after":"pushsha999","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"pushsha999","message":"feat: merge pr"}}`
		if _, err := srvWithGH.ProcessGitHubEvent(ctx, "push", "del-push-main", []byte(payloadPush)); err != nil || len(mockReg.backfillCalls) != 1 || mockReg.backfillCalls[0].prNumber != 522 || mockReg.backfillCalls[0].mergeSHA != "pushsha999" {
			t.Errorf("push to main backfill failed: err=%v, calls=%+v", err, mockReg.backfillCalls)
		}

		payloadFeatPush := `{"ref":"refs/heads/feat/branch-1","after":"featsha888","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"featsha888","message":"feat: wip"}}`
		resFeatPush, err := srvWithGH.ProcessGitHubEvent(ctx, "push", "del-push-feat-reg", []byte(payloadFeatPush))
		if err != nil || len(mockReg.backfillCalls) != 1 || resFeatPush.ResolutionSource != "non_main_branch" {
			t.Errorf("feature push unexpected result: err=%v, calls=%d, res=%+v", err, len(mockReg.backfillCalls), resFeatPush)
		}

		// 10. Registry error propagation
		srvErr := newTestRouter(Config{}, nil, &mockPRRegistry{err: errors.New("simulated database failure")}, nil)
		if _, err := srvErr.ProcessGitHubEvent(ctx, "pull_request", "del-err", []byte(payloadPRMerged)); err == nil {
			t.Errorf("expected error when registry returns failure, got nil")
		}

		// 11. Local PR resolution priority avoids hitting GitHub API
		var ghHit bool
		ghSpy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ghHit = true
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]GitHubCommitPRItem{{Number: 999, State: "closed"}})
		}))
		defer ghSpy.Close()

		mockLocalReg := &mockPRRegistry{
			resolvePRNum:    530,
			resolveBranch:   "feat/local-resolved",
			resolveTargetID: "thread-local-530",
		}
		srvLocal := newTestRouter(Config{GitHubAPIURL: ghSpy.URL}, ghSpy.Client(), mockLocalReg, nil)

		for _, tc := range []struct {
			event, del, body string
			wantMergeSHA     string
		}{
			{"check_run", "del-cr-local", `{"action":"completed","check_run":{"name":"Integration Tests","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`, ""},
			{"workflow_run", "del-wf-local", `{"action":"completed","workflow_run":{"name":"Build Pipeline","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`, ""},
			{"push", "del-push-local", `{"ref":"refs/heads/main","after":"localsha123","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"localsha123","message":"feat: merged commit"}}`, "localsha123"},
		} {
			ghHit = false
			res, err := srvLocal.ProcessGitHubEvent(ctx, tc.event, tc.del, []byte(tc.body))
			if err != nil || ghHit || res.ResolutionSource != "local_pr_registry" || res.PRNumber != 530 || res.TargetID != "thread-local-530" || res.MergeSHA != tc.wantMergeSHA {
				t.Errorf("%s local resolution failed: err=%v, ghHit=%v, res=%+v", tc.event, err, ghHit, res)
			}
		}
		if len(mockLocalReg.backfillCalls) != 1 || mockLocalReg.backfillCalls[0].prNumber != 530 || mockLocalReg.backfillCalls[0].mergeSHA != "localsha123" {
			t.Errorf("unexpected backfillCalls on local push: %+v", mockLocalReg.backfillCalls)
		}
	})

	t.Run("OutboundDispatchAndMergedAndConflicts", func(t *testing.T) {
		t.Parallel()
		mockDisp := &mockOutboundDispatcher{
			gitPushCh:       make(chan GitPushEventRequest, 5),
			promptCh:        make(chan PromptRequest, 5),
			directMessageCh: make(chan DirectMessageRequest, 5),
		}
		mockReg := &mockPRRegistry{
			resolvePRNum:     542,
			resolveBranch:    "main",
			resolveTargetID:  "1555405874565091380",
			mergedTargetID:   "1555405874565091380",
			ciFailedTargetID: "1555405874565091380",
			ciFailedUpdated:  true,
			conflictTargetID: "1555405874565091380",
			conflictUpdated:  true,
		}
		srv := newTestRouter(Config{}, nil, mockReg, mockDisp)

		// 1. Push to main triggers DispatchGitPush carrying TargetID and PRNumber
		pushPayloadMain := []byte(`{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`)
		if _, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-main", pushPayloadMain); err != nil {
			t.Fatalf("ProcessGitHubEvent push failed: %v", err)
		}
		calls := mockDisp.GitPushCalls()
		if len(calls) != 1 || calls[0].Repo != "azylman/aerial" || calls[0].Commit != "maincommit123" || calls[0].Ref != "refs/heads/main" || calls[0].TargetID != "1555405874565091380" || calls[0].PRNumber != 542 {
			t.Errorf("unexpected git push calls: %+v", calls)
		}

		// 2. Push to feature branch does NOT trigger DispatchGitPush
		pushPayloadFeat := []byte(`{"ref":"refs/heads/feat/test-branch","after":"featcommit456","repository":{"full_name":"azylman/aerial"}}`)
		if _, err := srv.ProcessGitHubEvent(ctx, "push", "del-push-feat", pushPayloadFeat); err != nil {
			t.Fatalf("ProcessGitHubEvent push feat failed: %v", err)
		}
		if len(mockDisp.GitPushCalls()) != 1 {
			t.Fatalf("expected still 1 git push dispatch, got %d", len(mockDisp.GitPushCalls()))
		}

		// 3. check_run failure with valid snowflake triggers DispatchPrompt; duplicate (ciFailedUpdated=false) does not
		crFailurePayload := []byte(`{
			"action": "completed",
			"repository": {"full_name": "azylman/aerial"},
			"check_run": {
				"name": "Service Unit Tests", "head_sha": "headsha789",
				"status": "completed", "conclusion": "failure",
				"html_url": "https://github.com/azylman/aerial/actions/runs/123",
				"output": {"title": "Build Failed", "summary": "Compilation error in brain/main.go:42"},
				"pull_requests": [{"number": 530, "head": {"ref": "feat/test"}}]
			}
		}`)
		if _, err := srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-fail", crFailurePayload); err != nil {
			t.Fatalf("ProcessGitHubEvent check_run failure failed: %v", err)
		}
		pCalls := mockDisp.PromptCalls()
		if len(pCalls) != 1 || pCalls[0].ChannelID != "1555405874565091380" ||
			!strings.Contains(pCalls[0].Prompt, "Service Unit Tests") ||
			!strings.Contains(pCalls[0].Prompt, "Compilation error in brain/main.go:42") ||
			!strings.Contains(pCalls[0].Prompt, "https://github.com/azylman/aerial/actions/runs/123") ||
			!strings.Contains(pCalls[0].Prompt, "Please investigate and fix") {
			t.Errorf("unexpected prompt call: %+v", pCalls)
		}

		mockReg.ciFailedUpdated = false
		if _, err := srv.ProcessGitHubEvent(ctx, "check_run", "del-cr-dup", crFailurePayload); err != nil {
			t.Fatalf("ProcessGitHubEvent duplicate check_run failed: %v", err)
		}
		if len(mockDisp.PromptCalls()) != 1 {
			t.Fatalf("expected still 1 prompt dispatch for duplicate, got %d", len(mockDisp.PromptCalls()))
		}

		// 4. pull_request closed+merged updates widget via SyncPRWidget (suppresses direct message)
		prMergedPayload := `{"action":"closed","number":542,"pull_request":{"number":542,"merged":true,"head":{"ref":"feat/batch1","sha":"head542"},"merge_commit_sha":"merge542"},"repository":{"full_name":"azylman/aerial"}}`
		if res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merge", []byte(prMergedPayload)); err != nil || res.PRNumber != 542 || res.Action != "closed" {
			t.Fatalf("PR merged event failed: err=%v, res=%+v", err, res)
		}

		// 5. pull_request auto_merge_disabled dispatches conflict DM + prompt
		amdPayload := `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"}},"repository":{"full_name":"azylman/aerial"}}`
		if res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-amd", []byte(amdPayload)); err != nil || res.Action != "auto_merge_disabled" || res.PRNumber != 543 {
			t.Fatalf("auto_merge_disabled failed: err=%v, res=%+v", err, res)
		}
		dmCalls := mockDisp.DirectMessageCalls()
		pCalls = mockDisp.PromptCalls()
		if !strings.Contains(dmCalls[len(dmCalls)-1].Content, "⛔ **(PR: #543, repo: aerial)** Merge conflict detected. Following up...") ||
			!strings.Contains(pCalls[len(pCalls)-1].Prompt, "Merge conflict detected on PR #543") {
			t.Errorf("unexpected auto_merge_disabled dispatches: dm=%+v, prompt=%+v", dmCalls, pCalls)
		}

		// 6. Push to main triggers conflict check on open PRs
		apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"number":543,"mergeable":false,"mergeable_state":"dirty"}`))
		}))
		defer apiServer.Close()

		conflictDisp := &mockOutboundDispatcher{promptCh: make(chan PromptRequest, 5)}
		conflictReg := &mockPRRegistry{
			openPRs:          []RegisteredPR{{PRNumber: 543, Branch: "feat/conflict-check", TargetID: "1555405874565091380", HeadSHA: "head543"}},
			conflictTargetID: "1555405874565091380",
			conflictUpdated:  true,
		}
		srvConflict := newTestRouter(Config{GitHubAPIURL: apiServer.URL}, nil, conflictReg, conflictDisp)
		srvConflict.SetConflictCheckDelay(1 * time.Millisecond)

		if _, err := srvConflict.ProcessGitHubEvent(ctx, "push", "del-push-conflict", pushPayloadMain); err != nil {
			t.Fatalf("ProcessGitHubEvent push conflict check failed: %v", err)
		}
		select {
		case prompt := <-conflictDisp.promptCh:
			if prompt.ChannelID != "1555405874565091380" || !strings.Contains(prompt.Prompt, "Merge conflict detected on PR #543") {
				t.Errorf("unexpected conflict prompt: %+v", prompt)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for conflict check prompt on push to main")
		}
	})
}

// --- 4. Hangar Webhook HTTP Handler & Event Processing ---

func TestHandleHangarWebhook(t *testing.T) {
	t.Parallel()

	t.Run("ValidationAndSyncJobDefault", func(t *testing.T) {
		t.Parallel()
		srv := NewRouterServer(Config{}, nil)
		routes := srv.Routes()

		// Malformed JSON on both paths -> 400
		for _, path := range []string{"/api/webhooks/hangar", "/webhooks/hangar"} {
			rec := doHTTPRequest(routes, http.MethodPost, path, strings.NewReader(`{invalid json`), nil)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for malformed json on %s, got %d", path, rec.Code)
			}
			var errResp map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil || errResp["error"] != "invalid json payload" {
				t.Errorf("unexpected error response on %s: %+v", path, errResp)
			}
		}

		// Read error & missing required fields -> 400
		for _, body := range []io.Reader{
			errReader{},
			strings.NewReader(`{"job_name":"brain"}`),
			strings.NewReader(`{"event":"deploy_started"}`),
			strings.NewReader(`{}`),
			strings.NewReader(`{"event":"deploy_success","job_name":"","repo":"azylman/aerial","status":"success"}`),
		} {
			if rec := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/hangar", body, nil); rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400 Bad Request, got %d", rec.Code)
			}
		}

		// sync_success and sync_failed with empty job_name default to git-sync -> 200 OK
		for _, evt := range []string{"sync_success", "sync_failed"} {
			body := fmt.Sprintf(`{"event":%q,"job_name":"","repo":"azylman/aerial","commit_sha":"abc1234","status":"success"}`, evt)
			rec := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body), nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200 OK for %s with empty job_name, got %d", evt, rec.Code)
			}
			var resp map[string]interface{}
			_ = json.Unmarshal(rec.Body.Bytes(), &resp)
			if resp["job_name"] != "git-sync" {
				t.Errorf("expected job_name git-sync for %s, got %v", evt, resp["job_name"])
			}
		}
	})

	t.Run("ValidPayloads", func(t *testing.T) {
		t.Parallel()
		routes := NewRouterServer(Config{}, nil).Routes()
		for _, tc := range []struct {
			event, status, details string
		}{
			{"deploy_started", "started", "Nomad job evaluation placed"},
			{"deploy_success", "success", "Healthy deployment completed"},
			{"deploy_failed", "failed", "Allocation failed healthcheck"},
			{"deploy_rollback", "rollback", "Reverted to previous healthy release"},
		} {
			for _, path := range []string{"/api/webhooks/hangar", "/webhooks/hangar"} {
				body := fmt.Sprintf(`{"event":%q,"job_name":"brain","repo":"azylman/aerial","pr_number":420,"target_id":"thread-999","image":"ghcr.io/azylman/aerial:latest","digest":"sha256:abc1234","status":%q,"deployment_id":"dep-uuid-1","details":%q,"timestamp":"2026-10-02T20:00:00Z"}`, tc.event, tc.status, tc.details)
				rec := doHTTPRequest(routes, http.MethodPost, path, strings.NewReader(body), nil)
				if rec.Code != http.StatusOK {
					t.Fatalf("expected 200 OK for %s on %s, got %d", tc.event, path, rec.Code)
				}
				var resp map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse response json: %v", err)
				}
				if resp["status"] != "accepted" || resp["event"] != tc.event || resp["job_name"] != "brain" ||
					int(resp["pr_number"].(float64)) != 420 || resp["target_id"] != "thread-999" {
					t.Errorf("unexpected response for %s: %+v", tc.event, resp)
				}
			}
		}
	})

	t.Run("ResolvePRBySHA", func(t *testing.T) {
		t.Parallel()
		mockReg := &mockPRRegistry{resolvePRNum: 789, resolveTargetID: "thread-resolved-789"}
		routes := newTestRouter(Config{}, nil, mockReg, nil).Routes()

		// 1. CommitSHA provided without PRNumber -> resolves PR and TargetID
		body := `{"event":"deploy_success","job_name":"webhooks-router","repo":"azylman/aerial","commit_sha":"deploycommit123","status":"success","details":"Deployed successfully"}`
		rec := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body), nil)
		if rec.Code != http.StatusOK || len(mockReg.resolveBySHACalls) != 1 || mockReg.resolveBySHACalls[0].sha != "deploycommit123" {
			t.Fatalf("expected 200 OK and 1 ResolvePRBySHA call, got code=%d calls=%+v", rec.Code, mockReg.resolveBySHACalls)
		}
		var resp map[string]interface{}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if int(resp["pr_number"].(float64)) != 789 || resp["target_id"] != "thread-resolved-789" {
			t.Errorf("unexpected resolved fields: %+v", resp)
		}

		// 2. CommitSHA provided with existing TargetID -> preserves existing TargetID
		mockReg.resolveBySHACalls = nil
		bodyPreserve := `{"event":"deploy_rollback","job_name":"webhooks-router","repo":"azylman/aerial","commit_sha":"deploycommit123","target_id":"existing-target-snowflake","status":"rollback"}`
		recPreserve := doHTTPRequest(routes, http.MethodPost, "/webhooks/hangar", strings.NewReader(bodyPreserve), nil)
		_ = json.Unmarshal(recPreserve.Body.Bytes(), &resp)
		if recPreserve.Code != http.StatusOK || int(resp["pr_number"].(float64)) != 789 || resp["target_id"] != "existing-target-snowflake" {
			t.Errorf("expected preserved target_id, got %+v", resp)
		}

		// 3. PRNumber already provided -> does not call ResolvePRBySHA
		mockReg.resolveBySHACalls = nil
		bodyWithPR := `{"event":"deploy_started","job_name":"brain","repo":"azylman/aerial","commit_sha":"deploycommit123","pr_number":123,"status":"started"}`
		if recWithPR := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(bodyWithPR), nil); recWithPR.Code != http.StatusOK || len(mockReg.resolveBySHACalls) != 0 {
			t.Errorf("expected 200 OK and 0 ResolvePRBySHA calls, got code=%d calls=%d", recWithPR.Code, len(mockReg.resolveBySHACalls))
		}

		// 4. Registry error & nil registry -> still accepts event with 200 OK
		srvErrRoutes := newTestRouter(Config{}, nil, &mockPRRegistry{resolveErr: errors.New("db error")}, nil).Routes()
		if recErr := doHTTPRequest(srvErrRoutes, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body), nil); recErr.Code != http.StatusOK {
			t.Errorf("expected 200 OK when registry resolve errors, got %d", recErr.Code)
		}
		srvNilRoutes := NewRouterServer(Config{}, nil).Routes()
		if recNil := doHTTPRequest(srvNilRoutes, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body), nil); recNil.Code != http.StatusOK {
			t.Errorf("expected 200 OK when registry is nil, got %d", recNil.Code)
		}
	})

	t.Run("WithRiverAndDuplicates", func(t *testing.T) {
		t.Parallel()
		mockRiver := &mockRiverInserter{}
		routes := NewRouterServer(Config{}, nil, mockRiver).Routes()
		body := `{"event":"deploy_success","job_name":"brain","repo":"azylman/aerial","pr_number":558,"target_id":"1555405874565091380","status":"success","details":"Deployment healthy"}`

		for _, path := range []string{"/api/webhooks/hangar", "/webhooks/hangar"} {
			mockRiver.insertedHangarJobs = nil
			mockRiver.insertedOpts = nil
			rec := doHTTPRequest(routes, http.MethodPost, path, strings.NewReader(body), nil)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("expected 202 Accepted on %s, got %d", path, rec.Code)
			}
			var resp map[string]interface{}
			_ = json.Unmarshal(rec.Body.Bytes(), &resp)
			if resp["status"] != "accepted" || resp["event"] != "deploy_success" || resp["job_name"] != "brain" {
				t.Errorf("unexpected response body: %+v", resp)
			}
			if len(mockRiver.insertedHangarJobs) != 1 || mockRiver.insertedHangarJobs[0].Event.Event != "deploy_success" {
				t.Errorf("unexpected inserted hangar jobs: %+v", mockRiver.insertedHangarJobs)
			}
			if len(mockRiver.insertedOpts) != 1 || mockRiver.insertedOpts[0].MaxAttempts != 5 ||
				!mockRiver.insertedOpts[0].UniqueOpts.ByArgs || mockRiver.insertedOpts[0].UniqueOpts.ByPeriod != 15*time.Minute {
				t.Errorf("unexpected insert opts: %+v", mockRiver.insertedOpts)
			}
		}

		// River insertion failure -> 500
		srvErr := NewRouterServer(Config{}, nil, &mockRiverInserter{err: errors.New("simulated river insert error")}).Routes()
		if recErr := doHTTPRequest(srvErr, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body), nil); recErr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500 when river persistence fails, got %d", recErr.Code)
		}

		// Duplicate skipped with commit_sha increments metric and exercises deterministic idempotency key
		srvDup := NewRouterServer(Config{}, nil, &mockRiverInserter{skipDuplicate: true}).Routes()
		bodyDup := `{
			"event": "deploy_success",
			"job_name": "brain",
			"commit_sha": "abc1234"
		}`
		if recDup := doHTTPRequest(srvDup, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(bodyDup), nil); recDup.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted on duplicate, got %d", recDup.Code)
		}
		recM := doHTTPRequest(srvDup, http.MethodGet, "/metrics", nil, nil)
		if !strings.Contains(recM.Body.String(), `webhooks_router_unique_skipped_total{kind="hangar_webhook"}`) {
			t.Errorf("expected /metrics to include hangar_webhook duplicate count, got:\n%s", recM.Body.String())
		}
	})
}

func TestProcessHangarEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("ZeroDownstreamCallsWithoutSnowflake", func(t *testing.T) {
		t.Parallel()
		transport := &zeroCallTransport{}
		srv := newTestRouter(
			Config{},
			&http.Client{Transport: transport},
			&mockPRRegistry{resolvePRNum: 101, resolveTargetID: "target-101"},
			nil,
		)
		routes := srv.Routes()

		for _, evt := range []string{"deploy_started", "deploy_success", "deploy_failed", "deploy_rollback"} {
			body := fmt.Sprintf(`{
				"event": %q,
				"job_name": "brain",
				"repo": "azylman/aerial",
				"commit_sha": "sha-test",
				"status": "started"
			}`, evt)
			if rec := doHTTPRequest(routes, http.MethodPost, "/api/webhooks/hangar", strings.NewReader(body), nil); rec.Code != http.StatusOK {
				t.Fatalf("expected 200 OK for %s, got %d", evt, rec.Code)
			}
		}
		if calls := transport.calls.Load(); calls != 0 {
			t.Fatalf("expected 0 outbound calls, got %d", calls)
		}
	})

	t.Run("DirectTransitionsAndErrorTolerance", func(t *testing.T) {
		t.Parallel()
		mockReg := &mockPRRegistry{
			resolvePRNum:    99,
			resolveTargetID: "target-99",
		}
		srv := newTestRouter(Config{}, nil, mockReg, nil)

		evtStarted := HangarDeployEvent{
			Event:     "deploy_started",
			JobName:   "webhooks-router",
			Repo:      "azylman/aerial",
			CommitSHA: "sha111",
			Status:    "started",
		}
		resStarted, err := srv.ProcessHangarEvent(ctx, evtStarted)
		if err != nil || resStarted.PRNumber != 99 || resStarted.TargetID != "target-99" || len(mockReg.deployingCalls) != 1 || mockReg.deployingCalls[0].prNumber != 99 {
			t.Fatalf("unexpected deploy_started result: err=%v, res=%+v, calls=%+v", err, resStarted, mockReg.deployingCalls)
		}

		mockReg.deployingErr = errors.New("db error")
		if _, err := srv.ProcessHangarEvent(ctx, evtStarted); err != nil {
			t.Errorf("ProcessHangarEvent should not fail when TransitionDeploying errors: %v", err)
		}
		mockReg.deployingErr = nil

		for _, evt := range []HangarDeployEvent{
			{
				Event:    "deploy_success",
				JobName:  "webhooks-router",
				Repo:     "azylman/aerial",
				PRNumber: 99,
				Status:   "success",
				TargetID: "target-99",
				Details:  "deployment completed",
			},
			{
				Event:    "deploy_rollback",
				JobName:  "webhooks-router",
				Repo:     "azylman/aerial",
				PRNumber: 99,
				Status:   "rollback",
				Details:  "rolled back",
			},
			{
				Event:    "deploy_failed",
				JobName:  "webhooks-router",
				Repo:     "azylman/aerial",
				PRNumber: 99,
				Status:   "failed",
				Details:  "deploy unhealthy",
			},
		} {
			if res, err := srv.ProcessHangarEvent(ctx, evt); err != nil || res.Event != evt.Event {
				t.Errorf("ProcessHangarEvent(%s) failed: err=%v, res=%+v", evt.Event, err, res)
			}
		}
	})

	t.Run("OutboundDispatchAllEventTypes", func(t *testing.T) {
		t.Parallel()
		mockDisp := &mockOutboundDispatcher{}
		mockReg := &mockPRRegistry{
			deployFailedUpdated: true,
			channelContextMap: map[string][]ChannelMessageContext{
				"1555405874565091380": {
					{
						AuthorName:   "arcane103",
						Content:      "Deploy the new router",
						ResponseText: "Working on it!",
					},
				},
			},
		}
		srv := newTestRouter(Config{}, nil, mockReg, mockDisp)

		cases := []struct {
			name        string
			before      func()
			evt         HangarDeployEvent
			wantDMs     int
			wantPrompts int
			wantDMSub   string
			wantPrSub   []string
		}{
			{
				name: "deploy_started valid snowflake",
				evt: HangarDeployEvent{
					Event:     "deploy_started",
					JobName:   "brain",
					Repo:      "azylman/aerial",
					PRNumber:  527,
					CommitSHA: "sha1234",
					TargetID:  "1555405874565091380",
					Status:    "started",
				},
				wantDMs:     0,
				wantPrompts: 0,
			},
			{
				name: "deploy_success valid snowflake with channel context",
				evt: HangarDeployEvent{
					Event:     "deploy_success",
					JobName:   "brain",
					Repo:      "azylman/aerial",
					PRNumber:  528,
					CommitSHA: "sha3d2a",
					TargetID:  "1555405874565091380",
					Status:    "success",
				},
				wantDMs:     0,
				wantPrompts: 1,
				wantPrSub: []string{
					"Continuous Delivery deployment completed for job brain",
					"[arcane103]: Deploy the new router",
					"[Aerial]: Working on it!",
					"Directive: Based on the recent conversation context above",
				},
			},
			{
				name: "deploy_rollback valid snowflake",
				evt: HangarDeployEvent{
					Event:     "deploy_rollback",
					JobName:   "webhooks-router",
					Repo:      "azylman/aerial",
					PRNumber:  529,
					CommitSHA: "shafail",
					TargetID:  "1555405874565091380",
					Status:    "rollback",
					Details:   strings.Repeat("error detail line\n", 50),
				},
				wantDMs:     0,
				wantPrompts: 2,
				wantPrSub: []string{
					"Deployment failed and rolled back",
					"Please investigate and fix",
					"```",
				},
			},
			{
				name: "deploy_failed valid snowflake",
				evt: HangarDeployEvent{
					Event:     "deploy_failed",
					JobName:   "hangar",
					Repo:      "azylman/aerial",
					PRNumber:  530,
					CommitSHA: "shabroken",
					TargetID:  "1555405874565091380",
					Status:    "failed",
					Details:   "exit status 1",
				},
				wantDMs:     0,
				wantPrompts: 3,
				wantPrSub: []string{
					"Deployment failed for job hangar",
					"Please investigate and fix",
				},
			},
			{
				name: "sync_success without PR number",
				evt: HangarDeployEvent{
					Event:     "sync_success",
					Repo:      "azylman/aerial-config",
					CommitSHA: "sha999",
					TargetID:  "1555405874565091380",
				},
				wantDMs:     0,
				wantPrompts: 3,
			},
			{
				name: "sync_failed with details",
				evt: HangarDeployEvent{
					Event:     "sync_failed",
					Repo:      "azylman/aerial-config",
					CommitSHA: "sha888",
					TargetID:  "1555405874565091380",
					Details:   "merge conflict in rules/persona",
				},
				wantDMs:     1,
				wantPrompts: 4,
				wantDMSub:   "⚠️ **(repo: aerial-config)** Git file sync failed. Following up...",
				wantPrSub: []string{
					"Git sync failed",
					"merge conflict in rules/persona",
					"Please investigate and fix",
				},
			},
			{
				name: "deploy_success invalid snowflake ignored",
				evt: HangarDeployEvent{
					Event:    "deploy_success",
					JobName:  "hangar",
					TargetID: "not-a-snowflake",
				},
				wantDMs:     1,
				wantPrompts: 4,
			},
			{
				name: "deploy_started invalid snowflake ignored",
				evt: HangarDeployEvent{
					Event:    "deploy_started",
					JobName:  "hangar",
					TargetID: "not-a-snowflake",
				},
				wantDMs:     1,
				wantPrompts: 4,
			},
			{
				name: "sync_success late resolution via ResolvePRBySHA",
				before: func() {
					mockReg.resolvePRNum = 241
					mockReg.resolveTargetID = "1555405874565091380"
				},
				evt: HangarDeployEvent{
					Event:     "sync_success",
					Repo:      "azylman/aerial-config",
					CommitSHA: "dccc646186d7bf7d17efa6cac6f730d17f66125b",
				},
				wantDMs:     1,
				wantPrompts: 4,
			},
			{
				name: "deploy_success on aerial-config",
				evt: HangarDeployEvent{
					Event:     "deploy_success",
					JobName:   "homepage",
					Repo:      "azylman/aerial-config",
					PRNumber:  242,
					CommitSHA: "sha_cfg_succ",
					TargetID:  "1555405874565091380",
					Status:    "success",
				},
				wantDMs:     1,
				wantPrompts: 5,
				wantPrSub: []string{
					"Continuous Delivery deployment completed for job homepage on azylman/aerial-config",
				},
			},
			{
				name: "sync_failed with PR number",
				evt: HangarDeployEvent{
					Event:     "sync_failed",
					Repo:      "azylman/aerial",
					PRNumber:  542,
					CommitSHA: "merge542",
					TargetID:  "1555405874565091380",
					Status:    "failed",
					Details:   "merge conflict in rules/foo.md",
				},
				wantDMs:     1,
				wantPrompts: 6,
				wantPrSub: []string{
					"Git sync failed for repo azylman/aerial",
					"merge conflict in rules/foo.md",
					"Please investigate and fix the git sync failure.",
				},
			},
		}

		for _, tc := range cases {
			if tc.before != nil {
				tc.before()
			}
			if _, err := srv.ProcessHangarEvent(ctx, tc.evt); err != nil {
				t.Fatalf("%s: ProcessHangarEvent failed: %v", tc.name, err)
			}
			dms := mockDisp.DirectMessageCalls()
			prompts := mockDisp.PromptCalls()
			if len(dms) != tc.wantDMs || len(prompts) != tc.wantPrompts {
				t.Fatalf("%s: expected DMs=%d Prompts=%d, got DMs=%d Prompts=%d", tc.name, tc.wantDMs, tc.wantPrompts, len(dms), len(prompts))
			}
			if tc.wantDMSub != "" && !strings.Contains(dms[len(dms)-1].Content, tc.wantDMSub) {
				t.Errorf("%s: expected DM to contain %q, got %q", tc.name, tc.wantDMSub, dms[len(dms)-1].Content)
			}
			for _, sub := range tc.wantPrSub {
				if !strings.Contains(prompts[len(prompts)-1].Prompt, sub) {
					t.Errorf("%s: expected prompt to contain %q, got %q", tc.name, sub, prompts[len(prompts)-1].Prompt)
				}
			}
		}

		// Error propagation when DispatchWidget fails on deploy_started
		mockDisp.widgetErr = errors.New("brain offline")
		_, err := srv.ProcessHangarEvent(ctx, HangarDeployEvent{
			Event:     "deploy_started",
			Repo:      "azylman/aerial",
			PRNumber:  603,
			JobName:   "brain",
			CommitSHA: "sha_start_test",
			TargetID:  "1555405874565091380",
		})
		if err == nil || !strings.Contains(err.Error(), "brain offline") {
			t.Fatalf("expected 'brain offline' error from ProcessHangarEvent, got: %v", err)
		}
	})
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
			"status": "ok",
			"message_id": "msg-widget-dispatch-ok",
		})
	}))
	defer ts.Close()

	dispOK := NewDefaultOutboundDispatcher("", ts.URL, ts.Client())
	msgID, err := dispOK.DispatchWidget(ctx, StatusWidgetRequest{
		ChannelID: "1555405874565091380",
		ID: "widget-1",
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
