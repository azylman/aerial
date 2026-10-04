package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime/coverage"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
	"gopkg.in/yaml.v3"
)

// TargetNomadVariable is the hardcoded destination Nomad variable for all synced secrets.
const TargetNomadVariable = "nomad/jobs/shared"

// Config holds runtime configuration for webhooks-router.
type Config struct {
	Port                  string
	InfisicalURL          string
	InfisicalClientID     string
	InfisicalClientSecret string
	InfisicalProjectID    string
	InfisicalEnvironment  string
	NomadAddr             string
	NomadToken            string
	GitHubToken           string
	GitHubAPIURL          string
	PostgresURL           string
	HangarURL             string
	BrainURL              string
}

// RouterYAMLConfig defines the declarative file schema for services/webhooks-router/webhooks-router.yaml.
type RouterYAMLConfig struct {
	Port                 string `yaml:"port"`
	InfisicalURL         string `yaml:"infisical_url"`
	InfisicalEnvironment string `yaml:"infisical_environment"`
	NomadAddr            string `yaml:"nomad_addr"`
	HangarURL            string `yaml:"hangar_url"`
	BrainURL             string `yaml:"brain_url"`
}

var webhooksRouterFallbackConfigPath = "/config/webhooks-router.yaml"

// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
func LoadConfig(configPath string) Config {
	var fileCfg RouterYAMLConfig
	targetPath := strings.TrimSpace(configPath)
	if targetPath != "" {
		if fi, err := os.Stat(targetPath); err != nil || fi.Size() == 0 {
			targetPath = ""
		}
	}
	if targetPath == "" {
		if fi, err := os.Stat(webhooksRouterFallbackConfigPath); err == nil && fi.Size() > 0 {
			targetPath = webhooksRouterFallbackConfigPath
		}
	}

	if targetPath != "" {
		if data, err := os.ReadFile(targetPath); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[WebhooksRouter] Warning: failed to parse yaml config at %s: %v", targetPath, err)
			}
		}
	}

	port := os.Getenv("PORT")
	if port == "" && fileCfg.Port != "" {
		port = fileCfg.Port
	}
	if port == "" {
		port = "4020"
	}

	infURL := os.Getenv("INFISICAL_URL")
	if infURL == "" {
		infURL = os.Getenv("INFISICAL_HOST_URL")
	}
	if infURL == "" && fileCfg.InfisicalURL != "" {
		infURL = fileCfg.InfisicalURL
	}
	if infURL == "" {
		infURL = "http://127.0.0.1:8085"
	}

	clientID := os.Getenv("INFISICAL_CLIENT_ID")
	if clientID == "" {
		clientID = os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID")
	}

	clientSecret := os.Getenv("INFISICAL_CLIENT_SECRET")
	if clientSecret == "" {
		clientSecret = os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET")
	}

	projectID := os.Getenv("INFISICAL_PROJECT_ID")
	if projectID == "" {
		projectID = os.Getenv("INFISICAL_WORKSPACE_ID")
	}

	envName := os.Getenv("INFISICAL_ENVIRONMENT")
	if envName == "" && fileCfg.InfisicalEnvironment != "" {
		envName = fileCfg.InfisicalEnvironment
	}
	if envName == "" {
		envName = "prod"
	}

	nomadAddr := os.Getenv("NOMAD_ADDR")
	if nomadAddr == "" && fileCfg.NomadAddr != "" {
		nomadAddr = fileCfg.NomadAddr
	}
	if nomadAddr == "" {
		nomadAddr = "http://127.0.0.1:4646"
	}

	ghToken := os.Getenv("GITHUB_PAT")
	if ghToken == "" {
		ghToken = os.Getenv("GITHUB_TOKEN")
	}

	ghAPI := os.Getenv("GITHUB_API_URL")
	if ghAPI == "" {
		ghAPI = "https://api.github.com"
	}

	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = os.Getenv("DATABASE_URL")
	}
	if pgURL == "" {
		if _, err := net.LookupHost("aerial-postgres"); err == nil {
			pgURL = "postgres://aerial:aerial_secure_pass@aerial-postgres:5432/aerial?sslmode=disable"
		} else if _, err := net.LookupHost("postgres"); err == nil {
			pgURL = "postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable"
		} else {
			pgURL = "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
		}
	}

	hangarURL := os.Getenv("HANGAR_URL")
	if hangarURL == "" && fileCfg.HangarURL != "" {
		hangarURL = fileCfg.HangarURL
	}
	if hangarURL == "" {
		hangarURL = "http://127.0.0.1:8087"
	}

	brainURL := os.Getenv("BRAIN_INTERNAL_URL")
	if brainURL == "" {
		brainURL = os.Getenv("BRAIN_URL")
	}
	if brainURL == "" && fileCfg.BrainURL != "" {
		brainURL = fileCfg.BrainURL
	}
	if brainURL == "" {
		brainURL = "http://127.0.0.1:8088"
	}

	return Config{
		Port:                  port,
		InfisicalURL:          strings.TrimRight(infURL, "/"),
		InfisicalClientID:     clientID,
		InfisicalClientSecret: clientSecret,
		InfisicalProjectID:    projectID,
		InfisicalEnvironment:  envName,
		NomadAddr:             strings.TrimRight(nomadAddr, "/"),
		NomadToken:            os.Getenv("NOMAD_TOKEN"),
		GitHubToken:           ghToken,
		GitHubAPIURL:          strings.TrimRight(ghAPI, "/"),
		PostgresURL:           pgURL,
		HangarURL:             strings.TrimRight(hangarURL, "/"),
		BrainURL:              strings.TrimRight(brainURL, "/"),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config {
	return LoadConfig(os.Getenv("CONFIG_PATH"))
}

// InfisicalWebhookPayload captures common payload variants from Infisical webhooks.
type InfisicalWebhookPayload struct {
	Event       string `json:"event"`
	WorkspaceID string `json:"workspaceId"`
	ProjectID   string `json:"projectId"`
	Environment string `json:"environment"`
	Data        *struct {
		WorkspaceID string `json:"workspaceId"`
		ProjectID   string `json:"projectId"`
		Environment string `json:"environment"`
	} `json:"data"`
}

func (p *InfisicalWebhookPayload) GetWorkspaceID(fallback string) string {
	if p.WorkspaceID != "" {
		return p.WorkspaceID
	}
	if p.ProjectID != "" {
		return p.ProjectID
	}
	if p.Data != nil {
		if p.Data.WorkspaceID != "" {
			return p.Data.WorkspaceID
		}
		if p.Data.ProjectID != "" {
			return p.Data.ProjectID
		}
	}
	return fallback
}

func (p *InfisicalWebhookPayload) GetEnvironment(fallback string) string {
	if p.Environment != "" {
		return p.Environment
	}
	if p.Data != nil && p.Data.Environment != "" {
		return p.Data.Environment
	}
	return fallback
}

type UniversalAuthLoginResponse struct {
	AccessToken string `json:"accessToken"`
	Token       string `json:"token"`
}

type InfisicalSecretItem struct {
	SecretKey   string `json:"secretKey"`
	SecretValue string `json:"secretValue"`
}

type InfisicalSecretsResponse struct {
	Secrets []InfisicalSecretItem `json:"secrets"`
}

type NomadVariable struct {
	Path        string            `json:"Path"`
	Items       map[string]string `json:"Items"`
	CreateIndex uint64            `json:"CreateIndex,omitempty"`
	ModifyIndex uint64            `json:"ModifyIndex,omitempty"`
}

// RiverInserter defines the interface for inserting jobs into River.
type RiverInserter interface {
	Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

// GitHubWebhookArgs contains payload data for durable background processing of GitHub webhooks.
type GitHubWebhookArgs struct {
	Event    string `json:"event"`
	Delivery string `json:"delivery"`
	Body     []byte `json:"body"`
}

func (GitHubWebhookArgs) Kind() string {
	return "github_webhook"
}

// GitHubWebhookWorker processes GitHub webhook events asynchronously from River.
type GitHubWebhookWorker struct {
	river.WorkerDefaults[GitHubWebhookArgs]
	server *RouterServer
}

func (w *GitHubWebhookWorker) Work(ctx context.Context, job *river.Job[GitHubWebhookArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	log.Printf("[webhooks-router] [river] processing github job id=%d event=%s delivery=%s", job.ID, job.Args.Event, job.Args.Delivery)
	_, err := w.server.ProcessGitHubEvent(ctx, job.Args.Event, job.Args.Delivery, job.Args.Body)
	if err != nil {
		log.Printf("[webhooks-router] [river] error processing github job id=%d event=%s delivery=%s: %v", job.ID, job.Args.Event, job.Args.Delivery, err)
		return err
	}
	return nil
}

// HangarWebhookArgs contains payload data for durable background processing of Hangar webhooks.
type HangarWebhookArgs struct {
	Event HangarDeployEvent `json:"event"`
}

func (HangarWebhookArgs) Kind() string {
	return "hangar_webhook"
}

// HangarWebhookWorker processes Hangar deployment and sync webhook events from River.
type HangarWebhookWorker struct {
	river.WorkerDefaults[HangarWebhookArgs]
	server *RouterServer
}

func (w *HangarWebhookWorker) Work(ctx context.Context, job *river.Job[HangarWebhookArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	log.Printf("[webhooks-router] [river] processing hangar job id=%d event=%s job_name=%s", job.ID, job.Args.Event.Event, job.Args.Event.JobName)
	_, err := w.server.ProcessHangarEvent(ctx, job.Args.Event)
	if err != nil {
		log.Printf("[webhooks-router] [river] error processing hangar job id=%d: %v", job.ID, err)
		return err
	}
	return nil
}

// NomadEventArgs contains payload data for durable background processing of Nomad stream events.
type NomadEventArgs struct {
	Topic   string          `json:"topic"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Index   uint64          `json:"index,omitempty"`
}

func (NomadEventArgs) Kind() string {
	return "nomad_event"
}

// NomadEventWorker processes Nomad deployment and allocation events from River.
type NomadEventWorker struct {
	river.WorkerDefaults[NomadEventArgs]
	server *RouterServer
}

func (w *NomadEventWorker) Work(ctx context.Context, job *river.Job[NomadEventArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	log.Printf("[webhooks-router] [river] processing nomad job id=%d topic=%s type=%s", job.ID, job.Args.Topic, job.Args.Type)
	err := w.server.ProcessNomadEvent(ctx, job.Args.Topic, job.Args.Type, job.Args.Payload)
	if err != nil {
		log.Printf("[webhooks-router] [river] error processing nomad job id=%d: %v", job.ID, err)
		return err
	}
	return nil
}


// normalizeRepo ensures repository names are normalized with the owner prefix.
func normalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return ""
	}
	if !strings.Contains(repo, "/") {
		return "azylman/" + repo
	}
	return repo
}

// cleanRepoName strips the owner prefix (e.g. "azylman/") for concise Discord notifications.
func cleanRepoName(repo string) string {
	repo = strings.TrimSpace(repo)
	return strings.TrimPrefix(repo, "azylman/")
}

// formatDirectMessagePrefix formats the bold PR and repo prefix for direct Discord messages.
func formatDirectMessagePrefix(prNumber int, repo string) string {
	repoName := cleanRepoName(repo)
	if prNumber > 0 && repoName != "" {
		return fmt.Sprintf("**(PR: #%d, repo: %s)** ", prNumber, repoName)
	} else if repoName != "" {
		return fmt.Sprintf("**(repo: %s)** ", repoName)
	} else if prNumber > 0 {
		return fmt.Sprintf("**(PR: #%d)** ", prNumber)
	}
	return ""
}

// RegisteredPR holds basic identification for an active pull request.
type RegisteredPR struct {
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	TargetID string `json:"target_id"`
	HeadSHA  string `json:"head_sha"`
}

// DeployingPR holds metadata for a PR currently undergoing deployment rollout.
type DeployingPR struct {
	ID       int64                  `json:"id"`
	Repo     string                 `json:"repo"`
	PRNumber int                    `json:"pr_number"`
	Branch   string                 `json:"branch"`
	HeadSHA  string                 `json:"head_sha"`
	MergeSHA string                 `json:"merge_sha"`
	TargetID string                 `json:"target_id"`
	Metadata map[string]interface{} `json:"metadata"`
}

// PRRegistryUpdater defines the interface for updating PR lifecycle state in PostgreSQL pr_registry.
type PRRegistryUpdater interface {
	UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (targetID string, err error)
	UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (targetID string, err error)
	AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (targetID string, prNumber int, updated bool, err error)
	BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	ResolvePRBySHA(ctx context.Context, repo string, sha string) (prNumber int, branch, targetID string, err error)
	ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (targetID string, branch, headSHA, mergeSHA string, err error)
	ListOpenPRs(ctx context.Context, repo string) ([]RegisteredPR, error)
	AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (targetID string, err error)
	AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (targetID string, prNumber int, mergeSHA, repo string, updated bool, err error)
	AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (targetID string, prNumber int, mergeSHA, repo string, updated bool, err error)
	ListDeployingPRs(ctx context.Context) ([]DeployingPR, error)
	GetRecentChannelContext(ctx context.Context, targetID string, limit int) ([]ChannelMessageContext, error)
}

// ChannelMessageContext holds historical conversation snippets for prompt enrichment.
type ChannelMessageContext struct {
	AuthorName   string
	Content      string
	ResponseText string
}

// PostgresPRRegistry implements PRRegistryUpdater backed by a pgxpool.Pool.
type PostgresPRRegistry struct {
	pool *pgxpool.Pool
}

// NewPostgresPRRegistry constructs a new PostgresPRRegistry.
func NewPostgresPRRegistry(pool *pgxpool.Pool) *PostgresPRRegistry {
	return &PostgresPRRegistry{pool: pool}
}

func (r *PostgresPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET status = 'merged',
			merge_sha = CASE WHEN $1 != '' THEN $1 ELSE merge_sha END,
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3
		RETURNING target_id;
	`
	var targetID string
	err := r.pool.QueryRow(qCtx, query, strings.TrimSpace(mergeSHA), repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Printf("[webhooks-router] [registry] pr not found in registry (untracked): repo=%s pr=%d", repo, prNumber)
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("update pr merged: %w", err)
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET status = 'closed',
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2
		RETURNING target_id;
	`
	var targetID string
	err := r.pool.QueryRow(qCtx, query, repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Printf("[webhooks-router] [registry] pr not found in registry (untracked): repo=%s pr=%d", repo, prNumber)
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("update pr closed unmerged: %w", err)
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET head_sha = $1,
			status = 'open',
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3
		RETURNING target_id;
	`
	var targetID string
	err := r.pool.QueryRow(qCtx, query, strings.TrimSpace(headSHA), repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Printf("[webhooks-router] [registry] pr not found in registry (untracked): repo=%s pr=%d", repo, prNumber)
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("update pr sync: %w", err)
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET status = 'ci_failed',
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2 AND status NOT IN ('ci_failed', 'merged', 'closed')
		RETURNING target_id;
	`
	var targetID string
	err := r.pool.QueryRow(qCtx, query, repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Printf("[webhooks-router] [registry] ci_failed transition skipped (already terminal or untracked): repo=%s pr=%d", repo, prNumber)
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("atomic transition ci_failed: %w", err)
	}
	return targetID, true, nil
}

func (r *PostgresPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	repo = normalizeRepo(repo)
	headSHA = strings.TrimSpace(headSHA)
	if repo == "" || headSHA == "" || isZeroSHA(headSHA) {
		return "", 0, false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET status = 'ci_failed',
			updated_at = CURRENT_TIMESTAMP
		WHERE id = (
			SELECT id FROM pr_registry
			WHERE repo = $1 AND head_sha = $2 AND status NOT IN ('ci_failed', 'merged', 'closed')
			ORDER BY updated_at DESC
			LIMIT 1
		)
		RETURNING target_id, pr_number;
	`
	var targetID string
	var prNumber int
	err := r.pool.QueryRow(qCtx, query, repo, headSHA).Scan(&targetID, &prNumber)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Printf("[webhooks-router] [registry] ci_failed by head_sha skipped (already terminal or untracked): repo=%s sha=%s", repo, headSHA)
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("atomic transition ci_failed by head_sha: %w", err)
	}
	return targetID, prNumber, true, nil
}

func (r *PostgresPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET merge_sha = $1,
			status = 'merged',
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3
		RETURNING target_id;
	`
	var targetID string
	err := r.pool.QueryRow(qCtx, query, strings.TrimSpace(mergeSHA), repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Printf("[webhooks-router] [registry] pr not found in registry (untracked): repo=%s pr=%d", repo, prNumber)
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("backfill push merge_sha: %w", err)
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	repo = normalizeRepo(repo)
	sha = strings.TrimSpace(sha)
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT pr_number, branch, target_id
		FROM pr_registry
		WHERE repo = $1 AND (merge_sha = $2 OR head_sha = $2)
		ORDER BY updated_at DESC
		LIMIT 1;
	`
	var prNum int
	var branch, targetID string
	err := r.pool.QueryRow(qCtx, query, repo, sha).Scan(&prNum, &branch, &targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", "", nil
	}
	if err != nil {
		return 0, "", "", fmt.Errorf("resolve pr by sha: %w", err)
	}
	return prNum, branch, targetID, nil
}

func (r *PostgresPRRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", "", "", "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT target_id, branch, head_sha, COALESCE(merge_sha, '')
		FROM pr_registry
		WHERE repo = $1 AND pr_number = $2
		LIMIT 1;
	`
	var targetID, branch, headSHA, mergeSHA string
	err := r.pool.QueryRow(qCtx, query, repo, prNumber).Scan(&targetID, &branch, &headSHA, &mergeSHA)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", "", nil
	}
	if err != nil {
		return "", "", "", "", fmt.Errorf("resolve pr by number: %w", err)
	}
	return targetID, branch, headSHA, mergeSHA, nil
}

func (r *PostgresPRRegistry) ListOpenPRs(ctx context.Context, repo string) ([]RegisteredPR, error) {
	repo = normalizeRepo(repo)
	if repo == "" {
		return nil, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT pr_number, branch, target_id, head_sha
		FROM pr_registry
		WHERE repo = $1 AND status NOT IN ('merged', 'closed', 'conflict')
		ORDER BY pr_number ASC;
	`
	rows, err := r.pool.Query(qCtx, query, repo)
	if err != nil {
		return nil, fmt.Errorf("list open prs: %w", err)
	}
	defer rows.Close()

	var prs []RegisteredPR
	for rows.Next() {
		var p RegisteredPR
		if err := rows.Scan(&p.PRNumber, &p.Branch, &p.TargetID, &p.HeadSHA); err != nil {
			return nil, fmt.Errorf("scan open pr: %w", err)
		}
		prs = append(prs, p)
	}
	return prs, rows.Err()
}

func (r *PostgresPRRegistry) AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET status = 'conflict',
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2 AND status != 'conflict' AND status NOT IN ('merged', 'closed')
		RETURNING target_id;
	`
	var targetID string
	err := r.pool.QueryRow(qCtx, query, repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("atomic transition conflict: %w", err)
	}
	return targetID, true, nil
}

func (r *PostgresPRRegistry) TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 || len(jobs) == 0 {
		return "", nil
	}
	var cleanJobs []string
	seen := make(map[string]struct{})
	for _, j := range jobs {
		cj := strings.TrimSpace(j)
		if cj != "" {
			if _, exists := seen[cj]; !exists {
				seen[cj] = struct{}{}
				cleanJobs = append(cleanJobs, cj)
			}
		}
	}
	if len(cleanJobs) == 0 {
		return "", nil
	}
	jobsJSON, err := json.Marshal(cleanJobs)
	if err != nil {
		jobsJSON = []byte("[]")
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET status = 'deploying',
			metadata = jsonb_set(
				COALESCE(metadata, '{}'::jsonb),
				'{jobs}',
				(
					SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
					FROM jsonb_array_elements(COALESCE(metadata->'jobs', '[]'::jsonb) || $1::jsonb) AS elem
				)
			),
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3 AND status NOT IN ('closed', 'conflict')
		RETURNING target_id;
	`
	var targetID string
	err = r.pool.QueryRow(qCtx, query, jobsJSON, repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("transition deploying: %w", err)
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return "", 0, "", "", false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := r.pool.Begin(qCtx)
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(qCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[webhooks-router] [registry] tx rollback error: %v", rbErr)
		}
	}()

	selectQuery := `
		SELECT id, target_id, pr_number, COALESCE(NULLIF(merge_sha, ''), head_sha), repo, COALESCE(metadata, '{}'::jsonb)
		FROM pr_registry
		WHERE status = 'deploying' AND (metadata->'jobs' ? $1 OR metadata->>'job' = $1)
		ORDER BY updated_at DESC
		LIMIT 1
		FOR UPDATE
	`

	var id int64
	var targetID, mergeSHA, repoOut string
	var prNum int
	var metaBytes []byte
	err = tx.QueryRow(qCtx, selectQuery, jobName).Scan(&id, &targetID, &prNum, &mergeSHA, &repoOut, &metaBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, "", "", false, nil
	}
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("select deploying pr: %w", err)
	}

	var meta map[string]interface{}
	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			log.Printf("[webhooks-router] [registry] failed to unmarshal metadata for job deploy: %v", err)
		}
	}
	if meta == nil {
		meta = make(map[string]interface{})
	}

	jobs := extractJobsFromMetadata(meta)
	var remaining []string
	for _, j := range jobs {
		if !strings.EqualFold(j, jobName) {
			remaining = append(remaining, j)
		}
	}

	if len(remaining) > 0 {
		// More jobs are still deploying for this PR; update remaining jobs in metadata and remain in deploying state
		remainingJSON, err := json.Marshal(remaining)
		if err != nil {
			remainingJSON = []byte("[]")
		}
		updateQuery := `
			UPDATE pr_registry
			SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{jobs}', $1::jsonb),
			    updated_at = CURRENT_TIMESTAMP
			WHERE id = $2 AND status = 'deploying'
		`
		if _, err := tx.Exec(qCtx, updateQuery, remainingJSON, id); err != nil {
			return "", 0, "", "", false, fmt.Errorf("update remaining jobs: %w", err)
		}
		if err := tx.Commit(qCtx); err != nil {
			return "", 0, "", "", false, fmt.Errorf("commit remaining jobs tx: %w", err)
		}
		return "", 0, "", "", false, nil
	}

	// All candidate jobs for this PR have successfully deployed!
	updateQuery := `
		UPDATE pr_registry
		SET status = 'deployed',
		    metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{jobs}', '[]'::jsonb),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'deploying'
	`
	tag, err := tx.Exec(qCtx, updateQuery, id)
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("update pr deployed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", 0, "", "", false, nil
	}

	if err := tx.Commit(qCtx); err != nil {
		return "", 0, "", "", false, fmt.Errorf("commit pr deployed tx: %w", err)
	}

	return targetID, prNum, mergeSHA, repoOut, true, nil
}

func (r *PostgresPRRegistry) AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET status = 'deploy_failed',
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2 AND status = 'deploying'
		RETURNING target_id;
	`
	var targetID string
	err := r.pool.QueryRow(qCtx, query, repo, prNumber).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("atomic transition deploy_failed: %w", err)
	}
	return targetID, true, nil
}

func (r *PostgresPRRegistry) AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return "", 0, "", "", false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	selectQuery := `
		SELECT id, target_id, pr_number, COALESCE(merge_sha, ''), repo
		FROM pr_registry
		WHERE status = 'deploying' AND (metadata->'jobs' ? $1 OR metadata->>'job' = $1)
		ORDER BY updated_at DESC
		LIMIT 1;
	`
	var id int64
	var targetID, mergeSHA, repoOut string
	var prNum int
	err := r.pool.QueryRow(qCtx, selectQuery, jobName).Scan(&id, &targetID, &prNum, &mergeSHA, &repoOut)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, "", "", false, nil
	}
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("select deploying pr for failure: %w", err)
	}

	updateQuery := `
		UPDATE pr_registry
		SET status = 'deploy_failed',
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'deploying';
	`
	tag, err := r.pool.Exec(qCtx, updateQuery, id)
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("update pr deploy_failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", 0, "", "", false, nil
	}

	return targetID, prNum, mergeSHA, repoOut, true, nil
}

func (r *PostgresPRRegistry) ListDeployingPRs(ctx context.Context) ([]DeployingPR, error) {
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, COALESCE(metadata, '{}'::jsonb)
		FROM pr_registry
		WHERE status = 'deploying'
		ORDER BY id ASC;
	`
	rows, err := r.pool.Query(qCtx, query)
	if err != nil {
		return nil, fmt.Errorf("list deploying prs: %w", err)
	}
	defer rows.Close()

	var prs []DeployingPR
	for rows.Next() {
		var p DeployingPR
		var metaBytes []byte
		if err := rows.Scan(&p.ID, &p.Repo, &p.PRNumber, &p.Branch, &p.HeadSHA, &p.MergeSHA, &p.TargetID, &metaBytes); err != nil {
			return nil, fmt.Errorf("scan deploying pr: %w", err)
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &p.Metadata); err != nil {
				log.Printf("[webhooks-router] [registry] failed to unmarshal metadata for pr %s#%d: %v", p.Repo, p.PRNumber, err)
			}
		}
		prs = append(prs, p)
	}
	return prs, rows.Err()
}

func (r *PostgresPRRegistry) GetRecentChannelContext(ctx context.Context, targetID string, limit int) ([]ChannelMessageContext, error) {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT author_name, content, COALESCE(response_text, '')
		FROM (
			SELECT author_name, content, response_text, created_at
			FROM messages
			WHERE thread_id = $1
			ORDER BY created_at DESC
			LIMIT $2
		) sub
		ORDER BY created_at ASC;
	`
	rows, err := r.pool.Query(qCtx, query, targetID, limit)
	if err != nil {
		return nil, fmt.Errorf("get recent channel context: %w", err)
	}
	defer rows.Close()

	var results []ChannelMessageContext
	for rows.Next() {
		var m ChannelMessageContext
		if err := rows.Scan(&m.AuthorName, &m.Content, &m.ResponseText); err != nil {
			return nil, fmt.Errorf("scan recent channel context: %w", err)
		}
		results = append(results, m)
	}
	return results, rows.Err()
}

// Discord Snowflake validation
var snowflakeRegex = regexp.MustCompile(`^[0-9]{17,20}$`)

func IsValidDiscordSnowflake(id string) bool {
	return snowflakeRegex.MatchString(strings.TrimSpace(id))
}

type GitPushEventRequest struct {
	Repo     string `json:"repo"`
	Ref      string `json:"ref"`
	Commit   string `json:"commit"`
	TargetID string `json:"target_id,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
}

type ImageReadyEventRequest struct {
	Image     string `json:"image"`
	Digest    string `json:"digest,omitempty"`
	Repo      string `json:"repo,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
}

type PromptRequest struct {
	ChannelID string `json:"channel_id"`
	Prompt    string `json:"prompt"`
}

type DirectMessageRequest struct {
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
}

type ImageReadyEventResponse struct {
	Status      string   `json:"status"`
	Image       string   `json:"image,omitempty"`
	MatchedJobs []string `json:"matched_jobs,omitempty"`
	Message     string   `json:"message,omitempty"`
}

type OutboundDispatcher interface {
	DispatchGitPush(ctx context.Context, req GitPushEventRequest) error
	DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error)
	DispatchPrompt(ctx context.Context, req PromptRequest) error
	DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error
}

type DefaultOutboundDispatcher struct {
	hangarURL   string
	brainURL    string
	httpClient  *http.Client
	retryDelay  time.Duration
	maxAttempts int
}

func NewDefaultOutboundDispatcher(hangarURL, brainURL string, client *http.Client) *DefaultOutboundDispatcher {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &DefaultOutboundDispatcher{
		hangarURL:   strings.TrimRight(hangarURL, "/"),
		brainURL:    strings.TrimRight(brainURL, "/"),
		httpClient:  client,
		retryDelay:  1500 * time.Millisecond,
		maxAttempts: 6,
	}
}

// SetRetryConfig overrides retry delay and attempt bounds for fast hermetic unit tests.
func (d *DefaultOutboundDispatcher) SetRetryConfig(delay time.Duration, maxAttempts int) {
	d.retryDelay = delay
	d.maxAttempts = maxAttempts
}

func (d *DefaultOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal git push request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/events/git_push", d.hangarURL)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create git push request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("git push request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr == nil && isHangarUnmanagedError(respBody) {
			log.Printf("[webhooks-router] [dispatcher] hangar confirmed repo %s is not managed by hangar (skipping): %s", req.Repo, string(respBody))
			return nil
		}
		return fmt.Errorf("hangar git push returned 404: %s", string(respBody))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("hangar git push returned status %d (read error: %w)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("hangar git push returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (d *DefaultOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error) {
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal image ready request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/events/image_ready", d.hangarURL)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create image ready request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("image ready request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr == nil && isHangarUnmanagedError(respBody) {
			log.Printf("[webhooks-router] [dispatcher] hangar confirmed image %s has no matching nomad jobs (skipping): %s", req.Image, string(respBody))
			return nil, nil
		}
		return nil, fmt.Errorf("hangar image ready returned 404: %s", string(respBody))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("hangar image ready returned status %d (read error: %w)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("hangar image ready returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var res ImageReadyEventResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		log.Printf("[webhooks-router] [dispatcher] warning decoding image ready response for %s: %v", req.Image, err)
		return nil, nil
	}
	return res.MatchedJobs, nil
}

func (d *DefaultOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	if !IsValidDiscordSnowflake(req.ChannelID) {
		return fmt.Errorf("invalid discord snowflake channel id: %q", req.ChannelID)
	}

	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal prompt request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/prompt", d.brainURL)
	delay := d.retryDelay
	if delay <= 0 {
		delay = 1500 * time.Millisecond
	}

	maxAttempts := d.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 6
	}
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create prompt request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("prompt request failed (attempt %d/%d): %w", attempt, maxAttempts, err)
			if attempt < maxAttempts {
				select {
				case <-time.After(delay):
					delay = time.Duration(float64(delay) * 1.5)
					if delay > 5*time.Second {
						delay = 5 * time.Second
					}
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return lastErr
		}

		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte(fmt.Sprintf("error reading body: %v", readErr))
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("brain prompt returned status %d (attempt %d/%d): %s", resp.StatusCode, attempt, maxAttempts, string(respBody))
			if attempt < maxAttempts {
				select {
				case <-time.After(delay):
					delay = time.Duration(float64(delay) * 1.5)
					if delay > 5*time.Second {
						delay = 5 * time.Second
					}
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return lastErr
		}

		// Non-5xx and non-2xx (e.g. 4xx client errors) - do not retry
		return fmt.Errorf("brain prompt returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return lastErr
}

func (d *DefaultOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	if !IsValidDiscordSnowflake(req.ChannelID) {
		return fmt.Errorf("invalid discord snowflake channel id: %q", req.ChannelID)
	}

	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal direct message request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/discord/message", d.brainURL)
	delay := d.retryDelay
	if delay <= 0 {
		delay = 500 * time.Millisecond
	}

	const maxAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create direct message request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("direct message request failed (attempt %d/%d): %w", attempt, maxAttempts, err)
			if attempt < maxAttempts {
				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return lastErr
		}

		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte(fmt.Sprintf("error reading body: %v", readErr))
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("brain internal message returned status %d (attempt %d/%d): %s", resp.StatusCode, attempt, maxAttempts, string(respBody))
			if attempt < maxAttempts {
				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return lastErr
		}

		// Non-5xx and non-2xx (e.g. 4xx client errors) - do not retry
		return fmt.Errorf("brain internal message returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return lastErr
}

// RouterServer handles webhook requests and orchestrates secret synchronization.
type RouterServer struct {
	cfg                Config
	httpClient         *http.Client
	riverClient        RiverInserter
	registry           PRRegistryUpdater
	dispatcher         OutboundDispatcher
	onProcessed        func(event, delivery string)
	conflictCheckDelay time.Duration
}

// NewRouterServer constructs a new RouterServer instance.
func NewRouterServer(cfg Config, client *http.Client, riverClient ...RiverInserter) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	s := &RouterServer{
		cfg:                cfg,
		httpClient:         client,
		conflictCheckDelay: 2 * time.Second,
	}
	if len(riverClient) > 0 {
		s.riverClient = riverClient[0]
	}
	return s
}

// SetConflictCheckDelay configures the delay before checking merge conflicts on push to main.
func (s *RouterServer) SetConflictCheckDelay(d time.Duration) {
	s.conflictCheckDelay = d
}

// SetRiverClient assigns the RiverInserter instance to RouterServer.
func (s *RouterServer) SetRiverClient(rc RiverInserter) {
	s.riverClient = rc
}

// SetRegistry assigns the PRRegistryUpdater instance to RouterServer.
func (s *RouterServer) SetRegistry(reg PRRegistryUpdater) {
	s.registry = reg
}

// SetDispatcher assigns the OutboundDispatcher instance to RouterServer.
func (s *RouterServer) SetDispatcher(d OutboundDispatcher) {
	s.dispatcher = d
}

// GetDispatcher returns the OutboundDispatcher configured on RouterServer.
func (s *RouterServer) GetDispatcher() OutboundDispatcher {
	return s.dispatcher
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[webhooks-router] JSON encode error: %v", err)
	}
}

// Routes initializes and returns the HTTP mux for the service.
func (s *RouterServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /health", s.handleHealthz)
	mux.HandleFunc("POST /webhooks/infisical", s.handleInfisicalWebhook)
	mux.HandleFunc("POST /webhooks/generic", s.handleGenericWebhook)
	mux.HandleFunc("POST /api/webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("POST /webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("POST /api/webhooks/hangar", s.handleHangarWebhook)
	mux.HandleFunc("POST /webhooks/hangar", s.handleHangarWebhook)
	mux.HandleFunc("POST /debug/coverage/flush", s.handleCoverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", s.handleCoverageFlush)
	return mux
}

func (s *RouterServer) handleCoverageFlush(w http.ResponseWriter, r *http.Request) {
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		if err := coverage.WriteCountersDir(dir); err != nil {
			log.Printf("[webhooks-router] coverage flush error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "flushed"})
}

func (s *RouterServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"service": "webhooks-router",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *RouterServer) handleInfisicalWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	var payload InfisicalWebhookPayload
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
			return
		}
	}

	workspaceID := payload.GetWorkspaceID(s.cfg.InfisicalProjectID)
	envName := payload.GetEnvironment(s.cfg.InfisicalEnvironment)

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"message":        "Secret sync scheduled",
		"targetVariable": TargetNomadVariable,
		"environment":    envName,
	})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] PANIC recovered in infisical webhook sync: %v", r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.SyncSecrets(ctx, workspaceID, envName); err != nil {
			log.Printf("[webhooks-router] Background sync failed: %v", err)
		}
	}()
}

type GenericWebhookPayload struct {
	WorkspaceID string            `json:"workspaceId"`
	Environment string            `json:"environment"`
	Secrets     map[string]string `json:"secrets"`
}

func (s *RouterServer) handleGenericWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	var payload GenericWebhookPayload
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
			return
		}
	}

	// If direct secrets map was supplied in payload, write directly to Nomad variables
	if len(payload.Secrets) > 0 {
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":         "accepted",
			"message":        "Direct secret sync scheduled",
			"targetVariable": TargetNomadVariable,
			"keyCount":       len(payload.Secrets),
		})

		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[webhooks-router] PANIC recovered in direct generic sync: %v", r)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := s.putNomadVariable(ctx, payload.Secrets); err != nil {
				log.Printf("[webhooks-router] Background direct sync failed: %v", err)
			}
		}()
		return
	}

	workspaceID := payload.WorkspaceID
	if workspaceID == "" {
		workspaceID = s.cfg.InfisicalProjectID
	}

	envName := payload.Environment
	if envName == "" {
		envName = s.cfg.InfisicalEnvironment
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"message":        "Generic secret sync scheduled",
		"targetVariable": TargetNomadVariable,
		"environment":    envName,
	})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] PANIC recovered in generic webhook sync: %v", r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.SyncSecrets(ctx, workspaceID, envName); err != nil {
			log.Printf("[webhooks-router] Background sync failed: %v", err)
		}
	}()
}

// Hangar Webhook Payloads and Resolution Types

type HangarDeployEvent struct {
	Event        string    `json:"event"`                   // "deploy_started", "deploy_success", "deploy_failed", "deploy_rollback"
	JobName      string    `json:"job_name"`                // Nomad job name (e.g. "webhooks-router", "brain")
	Repo         string    `json:"repo,omitempty"`          // repository name (e.g. "azylman/aerial", "azylman/aerial-config")
	CommitSHA    string    `json:"commit_sha,omitempty"`    // git commit SHA (head or merge commit)
	PRNumber     int       `json:"pr_number,omitempty"`     // PR number if known
	TargetID     string    `json:"target_id,omitempty"`     // Discord thread / channel snowflake
	Image        string    `json:"image,omitempty"`         // container image (e.g. ghcr.io/azylman/aerial-webhooks-router:latest)
	Digest       string    `json:"digest,omitempty"`        // image digest (sha256:...)
	Status       string    `json:"status"`                  // "started", "success", "failed", "rollback"
	DeploymentID string    `json:"deployment_id,omitempty"` // Nomad deployment UUID
	Details      string    `json:"details,omitempty"`       // status description or error message
	Timestamp    time.Time `json:"timestamp"`
}

func (s *RouterServer) handleHangarWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	var evt HangarDeployEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
		return
	}

	if strings.TrimSpace(evt.Event) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing required fields"})
		return
	}

	if strings.TrimSpace(evt.JobName) == "" {
		if strings.HasPrefix(evt.Event, "sync_") {
			evt.JobName = "git-sync"
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing required fields"})
			return
		}
	}

	if s.riverClient != nil {
		args := HangarWebhookArgs{Event: evt}
		if _, err := s.riverClient.Insert(r.Context(), args, &river.InsertOpts{MaxAttempts: 5}); err != nil {
			log.Printf("[webhooks-router] [hangar] ERROR: failed to insert job into river: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to persist webhook"})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":   "accepted",
			"event":    evt.Event,
			"job_name": evt.JobName,
		})
		return
	}

	processed, err := s.ProcessHangarEvent(r.Context(), evt)
	if err != nil {
		log.Printf("[webhooks-router] [hangar] error processing hangar event: %v", err)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "accepted",
		"event":     processed.Event,
		"job_name":  processed.JobName,
		"pr_number": processed.PRNumber,
		"target_id": processed.TargetID,
	})
}

// ResolveSHA resolves a commit SHA to its PR number, target Discord thread ID, and branch.
// It checks local pr_registry first, then falls back to GitHub commit PR lookup and backfills pr_registry.
func (s *RouterServer) ResolveSHA(ctx context.Context, repo, sha string) (int, string, string, error) {
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", "", nil
	}

	repo = normalizeRepo(repo)

	// 1. Check local pr_registry by SHA (matches head_sha or merge_sha)
	if s.registry != nil {
		prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, repo, sha)
		if err != nil {
			log.Printf("[webhooks-router] [registry] failed resolving PR by SHA %s locally: %v", sha, err)
		} else if prNum > 0 {
			if targetID == "" {
				tID, _, _, _, errNum := s.registry.ResolvePRByNumber(ctx, repo, prNum)
				if errNum == nil && tID != "" {
					targetID = tID
				}
			}
			return prNum, targetID, branch, nil
		}
	}

	// 2. Fall back to GitHub API commit PR lookup
	prNum, branch, err := s.resolvePRFromCommitSHA(ctx, repo, sha)
	if err != nil {
		log.Printf("[webhooks-router] [github] failed resolving PR for commit %s via GitHub API: %v", sha, err)
		return 0, "", "", err
	}
	if prNum == 0 {
		return 0, "", "", nil
	}

	var targetID string
	if s.registry != nil {
		tID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNum)
		if err == nil && tID != "" {
			targetID = tID
		}
		// Backfill merge_sha if it was missing
		backfillTID, err := s.registry.BackfillPushMergeSHA(ctx, repo, prNum, sha)
		if err != nil {
			log.Printf("[webhooks-router] [registry] failed backfilling merge_sha %s for PR %s#%d: %v", sha, repo, prNum, err)
		} else if targetID == "" && backfillTID != "" {
			targetID = backfillTID
		}
	}

	return prNum, targetID, branch, nil
}

// ResolveThreadForPR resolves the Discord thread snowflake (target_id) for a PR.
func (s *RouterServer) ResolveThreadForPR(ctx context.Context, repo string, prNum int) (string, error) {
	if s.registry == nil || prNum <= 0 {
		return "", nil
	}
	repo = normalizeRepo(repo)
	targetID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNum)
	return targetID, err
}

func (s *RouterServer) ProcessHangarEvent(ctx context.Context, evt HangarDeployEvent) (*HangarDeployEvent, error) {
	if evt.CommitSHA != "" && evt.PRNumber == 0 {
		prNum, targetID, _, err := s.ResolveSHA(ctx, evt.Repo, evt.CommitSHA)
		if err != nil {
			log.Printf("[webhooks-router] [hangar] failed resolving SHA %s: %v", evt.CommitSHA, err)
		} else if prNum > 0 {
			evt.PRNumber = prNum
			if evt.TargetID == "" {
				evt.TargetID = targetID
			}
		}
	} else if evt.PRNumber > 0 && evt.TargetID == "" {
		targetID, err := s.ResolveThreadForPR(ctx, evt.Repo, evt.PRNumber)
		if err != nil {
			log.Printf("[webhooks-router] [hangar] failed resolving thread for PR %s#%d: %v", evt.Repo, evt.PRNumber, err)
		} else if targetID != "" {
			evt.TargetID = targetID
		}
	}

	log.Printf("[webhooks-router] [hangar] [deploy] event=%s job=%s repo=%s pr=%d commit=%s status=%s target_id=%s deployment_id=%s details=%q",
		evt.Event, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA, evt.Status, evt.TargetID, evt.DeploymentID, evt.Details)

	if s.registry != nil && evt.PRNumber > 0 {
		if evt.Event == "deploy_started" {
			if evt.JobName != "" {
				if _, err := s.registry.TransitionDeploying(ctx, evt.Repo, evt.PRNumber, []string{evt.JobName}); err != nil {
					log.Printf("[webhooks-router] [registry] warning transitioning pr %s#%d to deploying for job %s: %v", evt.Repo, evt.PRNumber, evt.JobName, err)
				}
			}
			log.Printf("[webhooks-router] [registry] pr %s#%d deployment started for job %s", evt.Repo, evt.PRNumber, evt.JobName)
		} else if evt.Event == "deploy_success" {
			log.Printf("[webhooks-router] [registry] pr %s#%d deployed successfully for job %s", evt.Repo, evt.PRNumber, evt.JobName)
		} else if evt.Event == "deploy_rollback" {
			log.Printf("[webhooks-router] [registry] pr %s#%d deployment rolled back for job %s", evt.Repo, evt.PRNumber, evt.JobName)
		}
	}

	if evt.Event == "deploy_started" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		msg := fmt.Sprintf("🚀 %sStarting deploy for %s", formatDirectMessagePrefix(evt.PRNumber, evt.Repo), evt.JobName)
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
			ChannelID: targetID,
			Content:   msg,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching deploy_started direct message: %v", err)
			return &evt, fmt.Errorf("dispatch deploy_started direct message: %w", err)
		}
		log.Printf("[webhooks-router] [dispatcher] successfully dispatched deploy_started direct message for job %s to %s", evt.JobName, targetID)
	} else if evt.Event == "deploy_success" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		if err := s.dispatchDeploymentSuccessPrompt(ctx, evt.TargetID, evt.JobName, evt.Repo, evt.CommitSHA, evt.PRNumber, evt.DeploymentID); err != nil {
			return &evt, err
		}
	} else if evt.Event == "deploy_rollback" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		if s.registry != nil && evt.PRNumber > 0 {
			tID, updated, err := s.registry.AtomicTransitionDeployFailed(ctx, evt.Repo, evt.PRNumber)
			if err != nil {
				log.Printf("[webhooks-router] [registry] error transitioning deploy_rollback: %v", err)
				return &evt, err
			} else if !updated {
				return &evt, nil
			} else if tID != "" {
				targetID = tID
			}
		}
		dmMsg := fmt.Sprintf("💥 %sDeployment failed and rolled back for %s. Following up...", formatDirectMessagePrefix(evt.PRNumber, evt.Repo), evt.JobName)
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
			ChannelID: targetID,
			Content:   dmMsg,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] warning dispatching deploy_rollback direct message: %v", err)
		}
		lines := []string{
			fmt.Sprintf("Deployment failed and rolled back for job %s on %s (PR #%d, commit: %s).", evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA),
		}
		if cleanDetails := strings.TrimSpace(evt.Details); cleanDetails != "" {
			lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(cleanDetails)))
		}
		lines = append(lines, "Please investigate and fix the deployment rollback failure.")
		prompt := strings.Join(lines, "\n")
		if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{
			ChannelID: targetID,
			Prompt:    prompt,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching deploy_rollback prompt: %v", err)
			return &evt, fmt.Errorf("dispatch deploy_rollback prompt: %w", err)
		}
	} else if evt.Event == "deploy_failed" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		if s.registry != nil && evt.PRNumber > 0 {
			tID, updated, err := s.registry.AtomicTransitionDeployFailed(ctx, evt.Repo, evt.PRNumber)
			if err != nil {
				log.Printf("[webhooks-router] [registry] error transitioning deploy_failed: %v", err)
				return &evt, err
			} else if !updated {
				return &evt, nil
			} else if tID != "" {
				targetID = tID
			}
		}
		dmMsg := fmt.Sprintf("💥 %sDeployment failed for %s. Following up...", formatDirectMessagePrefix(evt.PRNumber, evt.Repo), evt.JobName)
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
			ChannelID: targetID,
			Content:   dmMsg,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] warning dispatching deploy_failed direct message: %v", err)
		}
		lines := []string{
			fmt.Sprintf("Deployment failed for job %s on %s (PR #%d, commit: %s).", evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA),
		}
		if cleanDetails := strings.TrimSpace(evt.Details); cleanDetails != "" {
			lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(cleanDetails)))
		}
		lines = append(lines, "Please investigate and fix the deployment failure.")
		prompt := strings.Join(lines, "\n")
		if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{
			ChannelID: targetID,
			Prompt:    prompt,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching deploy_failed prompt: %v", err)
			return &evt, fmt.Errorf("dispatch deploy_failed prompt: %w", err)
		}
	} else if evt.Event == "sync_success" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		msg := fmt.Sprintf("🔄 %sGit file sync completed", formatDirectMessagePrefix(evt.PRNumber, evt.Repo))
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
			ChannelID: targetID,
			Content:   msg,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching sync_success direct message: %v", err)
			return &evt, fmt.Errorf("dispatch sync_success direct message: %w", err)
		}
	} else if evt.Event == "sync_failed" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		dmMsg := fmt.Sprintf("⚠️ %sGit file sync failed. Following up...", formatDirectMessagePrefix(evt.PRNumber, evt.Repo))
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
			ChannelID: targetID,
			Content:   dmMsg,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] warning dispatching sync_failed direct message: %v", err)
		}
		lines := []string{
			fmt.Sprintf("Git sync failed for repo %s (commit: %s, PR #%d).", evt.Repo, evt.CommitSHA, evt.PRNumber),
		}
		if evt.PRNumber == 0 {
			lines[0] = fmt.Sprintf("Git sync failed for repo %s (commit: %s).", evt.Repo, evt.CommitSHA)
		}
		if cleanDetails := strings.TrimSpace(evt.Details); cleanDetails != "" {
			lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(cleanDetails)))
		}
		lines = append(lines, "Please investigate and fix the git sync failure.")
		prompt := strings.Join(lines, "\n")
		if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{
			ChannelID: targetID,
			Prompt:    prompt,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching sync_failed prompt: %v", err)
			return &evt, fmt.Errorf("dispatch sync_failed prompt: %w", err)
		}
	}

	return &evt, nil
}

const maxPromptRunes = 1500

func truncatePromptDetails(s string) string {
	s = strings.ReplaceAll(s, "```", "'''")
	runes := []rune(s)
	if len(runes) <= maxPromptRunes {
		return s
	}
	return string(runes[:maxPromptRunes]) + "... (truncated)"
}

func truncatePromptRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "... (truncated)"
}

func sanitizeChannelContextText(s string) string {
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "</CHANNEL_CONTEXT>", "[CHANNEL_CONTEXT]")
	s = strings.ReplaceAll(s, "<CHANNEL_CONTEXT>", "[CHANNEL_CONTEXT]")
	return strings.TrimSpace(s)
}

func formatChannelContext(msgs []ChannelMessageContext) string {
	if len(msgs) == 0 {
		return "No recent channel messages found."
	}

	var bullets []string
	for _, m := range msgs {
		author := strings.TrimSpace(m.AuthorName)
		if author == "" {
			author = "User"
		}
		if cleanContent := sanitizeChannelContextText(m.Content); cleanContent != "" {
			bullets = append(bullets, fmt.Sprintf("- [%s]: %s", author, truncatePromptRunes(cleanContent, 300)))
		}
		if cleanResp := sanitizeChannelContextText(m.ResponseText); cleanResp != "" {
			bullets = append(bullets, fmt.Sprintf("- [Aerial]: %s", truncatePromptRunes(cleanResp, 300)))
		}
	}

	// Iterate backwards from the most recent bullet to ensure the latest conversation context is preserved
	totalRunes := 0
	truncated := false
	var selectedBullets []string
	for i := len(bullets) - 1; i >= 0; i-- {
		b := bullets[i]
		bRunes := len([]rune(b))
		if totalRunes+bRunes > 4000 {
			truncated = true
			break
		}
		totalRunes += bRunes
		selectedBullets = append(selectedBullets, b)
	}

	// Reverse selected bullets so they render in chronological order (oldest to newest)
	for i, j := 0, len(selectedBullets)-1; i < j; i, j = i+1, j-1 {
		selectedBullets[i], selectedBullets[j] = selectedBullets[j], selectedBullets[i]
	}

	var lines []string
	lines = append(lines, "Recent Discord channel context:")
	lines = append(lines, "<CHANNEL_CONTEXT>")
	if truncated {
		lines = append(lines, "... (older context truncated)")
	}
	lines = append(lines, selectedBullets...)
	lines = append(lines, "</CHANNEL_CONTEXT>")
	return strings.Join(lines, "\n")
}

func (s *RouterServer) dispatchDeploymentSuccessPrompt(ctx context.Context, targetID, jobName, repo, commitSHA string, prNum int, refID string) error {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return nil
	}

	var contextMsgs []ChannelMessageContext
	if s.registry != nil {
		msgs, err := s.registry.GetRecentChannelContext(ctx, targetID, 20)
		if err != nil {
			log.Printf("[webhooks-router] [registry] warning fetching channel context for %s: %v", targetID, err)
		} else {
			contextMsgs = msgs
		}
	}

	var lines []string
	header := fmt.Sprintf("Continuous Delivery deployment completed for job %s (PR #%d, commit: %s).", jobName, prNum, commitSHA)
	if repo != "" && refID != "" {
		header = fmt.Sprintf("Continuous Delivery deployment completed for job %s on %s (PR #%d, commit: %s, ref: %s).", jobName, repo, prNum, commitSHA, refID)
	} else if repo != "" {
		header = fmt.Sprintf("Continuous Delivery deployment completed for job %s on %s (PR #%d, commit: %s).", jobName, repo, prNum, commitSHA)
	} else if refID != "" {
		header = fmt.Sprintf("Continuous Delivery deployment completed for job %s (PR #%d, commit: %s, ref: %s).", jobName, prNum, commitSHA, refID)
	}
	lines = append(lines, header)

	if ctxStr := formatChannelContext(contextMsgs); ctxStr != "" {
		lines = append(lines, "", ctxStr)
	}

	lines = append(lines, "", "Directive: Based on the recent conversation context above, suggest logical next steps if any work or follow-ups remain; otherwise, celebrate completion.")
	prompt := strings.Join(lines, "\n")

	if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{
		ChannelID: targetID,
		Prompt:    prompt,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching deploy_success prompt: %v", err)
		return fmt.Errorf("dispatch deploy_success prompt: %w", err)
	}
	log.Printf("[webhooks-router] [dispatcher] successfully dispatched deploy_success prompt for job %s to %s", jobName, targetID)
	return nil
}

func (s *RouterServer) dispatchCIFailurePrompt(ctx context.Context, targetID, checkName, repo, headSHA string, prNum int, details, checkURL string) error {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return nil
	}
	dmMsg := fmt.Sprintf("❌ %sCI check %q failed. Following up...", formatDirectMessagePrefix(prNum, repo), checkName)
	if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
		ChannelID: targetID,
		Content:   dmMsg,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] warning dispatching ci_failed direct message: %v", err)
	}
	lines := []string{
		fmt.Sprintf("CI check %q failed for PR #%d on %s (head commit: %s).", checkName, prNum, repo, headSHA),
	}
	if cleanDetails := strings.TrimSpace(details); cleanDetails != "" {
		lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(cleanDetails)))
	}
	if cleanURL := strings.TrimSpace(checkURL); cleanURL != "" {
		lines = append(lines, fmt.Sprintf("Check URL: %s", cleanURL))
	}
	lines = append(lines, "Please investigate and fix the failure by inspecting the failing logs, diagnosing the issue, verifying with local checks, and pushing a fix.")
	prompt := strings.Join(lines, "\n")

	if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{
		ChannelID: targetID,
		Prompt:    prompt,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching ci_failed prompt: %v", err)
		return fmt.Errorf("dispatch ci_failed prompt: %w", err)
	}
	log.Printf("[webhooks-router] [dispatcher] successfully dispatched ci_failed prompt for PR #%d to %s", prNum, targetID)
	return nil
}

func (s *RouterServer) dispatchConflictPrompt(ctx context.Context, targetID, repo, branch, headSHA string, prNum int) error {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return nil
	}
	dmMsg := fmt.Sprintf("⛔ %sMerge conflict detected. Following up...", formatDirectMessagePrefix(prNum, repo))
	if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
		ChannelID: targetID,
		Content:   dmMsg,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] warning dispatching conflict direct message: %v", err)
	}
	prompt := fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please checkout the branch, merge origin/main to resolve conflicts, verify locally, and push to update the PR.", prNum, repo, branch, headSHA)
	if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{
		ChannelID: targetID,
		Prompt:    prompt,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching conflict prompt: %v", err)
		return fmt.Errorf("dispatch conflict prompt: %w", err)
	}
	log.Printf("[webhooks-router] [dispatcher] successfully dispatched conflict prompt for PR #%d to %s", prNum, targetID)
	return nil
}

// GitHubPRDetails encapsulates mergeability information returned by GitHub's Pull Request API.
type GitHubPRDetails struct {
	Number         int    `json:"number"`
	Mergeable      *bool  `json:"mergeable"`
	MergeableState string `json:"mergeable_state"`
}

func (s *RouterServer) fetchPRDetails(ctx context.Context, repo string, prNumber int) (*GitHubPRDetails, error) {
	if repo == "" || prNumber <= 0 {
		return nil, fmt.Errorf("invalid repo or prNumber for pr details")
	}
	if !strings.Contains(repo, "/") {
		repo = "azylman/" + repo
	}
	apiURL := s.cfg.GitHubAPIURL
	if apiURL == "" {
		apiURL = "https://api.github.com"
	}
	endpoint := fmt.Sprintf("%s/repos/%s/pulls/%d", apiURL, repo, prNumber)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create github pr details request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if s.cfg.GitHubToken != "" {
		req.Header.Set("Authorization", "token "+s.cfg.GitHubToken)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github pr details request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-RateLimit-Remaining")
		return nil, fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github pr details returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read github pr details response: %w", err)
	}
	var details GitHubPRDetails
	if err := json.Unmarshal(body, &details); err != nil {
		return nil, fmt.Errorf("unmarshal github pr details: %w", err)
	}
	return &details, nil
}

func (s *RouterServer) checkOpenPRConflicts(ctx context.Context, repo string) error {
	if s.registry == nil {
		return nil
	}
	openPRs, err := s.registry.ListOpenPRs(ctx, repo)
	if err != nil {
		return fmt.Errorf("list open prs: %w", err)
	}
	if len(openPRs) == 0 {
		return nil
	}

	for _, pr := range openPRs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var details *GitHubPRDetails
		// Polling retry for mergeable: null (GitHub computes asynchronously)
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				delay := s.conflictCheckDelay
				if delay <= 0 {
					delay = 200 * time.Millisecond
				}
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			det, fetchErr := s.fetchPRDetails(ctx, repo, pr.PRNumber)
			if fetchErr != nil {
				log.Printf("[webhooks-router] [github] error fetching PR #%d details for conflict check: %v", pr.PRNumber, fetchErr)
				if strings.Contains(fetchErr.Error(), "403") || strings.Contains(strings.ToLower(fetchErr.Error()), "rate limit") {
					return fetchErr
				}
				break
			}
			details = det
			if details.Mergeable != nil || (details.MergeableState != "" && details.MergeableState != "unknown") {
				break
			}
		}

		if details == nil {
			continue
		}

		isConflict := (details.Mergeable != nil && !*details.Mergeable) || details.MergeableState == "dirty"
		if isConflict {
			targetID, updated, trErr := s.registry.AtomicTransitionConflict(ctx, repo, pr.PRNumber)
			if trErr != nil {
				log.Printf("[webhooks-router] [registry] error transitioning PR #%d to conflict: %v", pr.PRNumber, trErr)
				continue
			}
			if updated {
				if targetID == "" {
					targetID = pr.TargetID
				}
				log.Printf("[webhooks-router] [registry] pr %s#%d transitioned to conflict (target_id=%s)", repo, pr.PRNumber, targetID)
				if err := s.dispatchConflictPrompt(ctx, targetID, repo, pr.Branch, pr.HeadSHA, pr.PRNumber); err != nil {
					log.Printf("[webhooks-router] [registry] error dispatching conflict prompt for PR #%d: %v", pr.PRNumber, err)
				}
			}
		}
	}
	return nil
}

func (s *RouterServer) dispatchGitPush(ctx context.Context, repo, ref, commit, targetID string, prNum int) error {
	if s.dispatcher == nil || !strings.HasPrefix(ref, "refs/heads/main") || isZeroSHA(commit) {
		return nil
	}
	if err := s.dispatcher.DispatchGitPush(ctx, GitPushEventRequest{
		Repo:     repo,
		Ref:      ref,
		Commit:   commit,
		TargetID: targetID,
		PRNumber: prNum,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching git_push to hangar for %s: %v", repo, err)
		return fmt.Errorf("dispatch git_push to hangar: %w", err)
	}
	log.Printf("[webhooks-router] [dispatcher] successfully dispatched git_push to hangar for %s (%s)", repo, commit)
	return nil
}

func (s *RouterServer) dispatchWorkflowRunImages(ctx context.Context, repo string, runID int64, headBranch, conclusion, headSHA, targetID string, prNum int) error {
	if s.dispatcher == nil {
		return nil
	}
	if conclusion != "success" || !(headBranch == "main" || strings.HasPrefix(headBranch, "refs/heads/main")) {
		return nil
	}

	images, err := s.resolveWorkflowRunImages(ctx, repo, runID)
	if err != nil {
		log.Printf("[webhooks-router] [dispatcher] error resolving images for workflow run %d (%s): %v", runID, repo, err)
		return fmt.Errorf("resolve workflow run images: %w", err)
	}
	if len(images) == 0 {
		log.Printf("[webhooks-router] [dispatcher] no built images detected for workflow run %d (%s)", runID, repo)
		return nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	seenJobs := make(map[string]struct{})
	var allMatchedJobs []string
	errCh := make(chan error, len(images))

	for _, img := range images {
		wg.Add(1)
		go func(imageRef string) {
			defer wg.Done()
			req := ImageReadyEventRequest{
				Image:     imageRef,
				Repo:      repo,
				CommitSHA: headSHA,
				PRNumber:  prNum,
				TargetID:  targetID,
			}
			matchedJobs, err := s.dispatcher.DispatchImageReady(ctx, req)
			if err != nil {
				log.Printf("[webhooks-router] [dispatcher] error dispatching image_ready to hangar for %s: %v", imageRef, err)
				errCh <- err
			} else {
				log.Printf("[webhooks-router] [dispatcher] successfully dispatched image_ready to hangar for %s (matched: %v)", imageRef, matchedJobs)
				if len(matchedJobs) > 0 {
					mu.Lock()
					for _, j := range matchedJobs {
						cleanJ := strings.TrimSpace(j)
						if cleanJ != "" {
							if _, ok := seenJobs[cleanJ]; !ok {
								seenJobs[cleanJ] = struct{}{}
								allMatchedJobs = append(allMatchedJobs, cleanJ)
							}
						}
					}
					mu.Unlock()
				}
			}
		}(img)
	}
	wg.Wait()
	close(errCh)

	if len(allMatchedJobs) > 0 && s.registry != nil && prNum > 0 {
		if _, err := s.registry.TransitionDeploying(ctx, repo, prNum, allMatchedJobs); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error transitioning pr %s#%d to deploying: %v", repo, prNum, err)
			return fmt.Errorf("transition pr deploying: %w", err)
		}
		log.Printf("[webhooks-router] [dispatcher] pr %s#%d transitioned to deploying with jobs: %v", repo, prNum, allMatchedJobs)
	}

	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GitHub Webhook Payloads and Resolution Types

type GitHubPingPayload struct {
	Zen    string `json:"zen"`
	HookID int64  `json:"hook_id"`
	Repo   *struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubPRRef struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type GitHubPullRequest struct {
	Number         int         `json:"number"`
	Title          string      `json:"title"`
	Head           GitHubPRRef `json:"head"`
	Base           GitHubPRRef `json:"base"`
	Merged         bool        `json:"merged"`
	MergeCommitSHA string      `json:"merge_commit_sha"`
}

type GitHubPullRequestPayload struct {
	Action      string            `json:"action"`
	Number      int               `json:"number"`
	PullRequest GitHubPullRequest `json:"pull_request"`
	Repository  struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
}

type GitHubCheckRunPR struct {
	Number int         `json:"number"`
	Head   GitHubPRRef `json:"head"`
	Base   GitHubPRRef `json:"base"`
}

type GitHubCheckRunPayload struct {
	Action   string `json:"action"`
	CheckRun struct {
		ID           int64              `json:"id"`
		Name         string             `json:"name"`
		HeadSHA      string             `json:"head_sha"`
		Status       string             `json:"status"`
		Conclusion   string             `json:"conclusion"`
		DetailsURL   string             `json:"details_url"`
		HTMLURL      string             `json:"html_url"`
		Output       struct {
			Title   string `json:"title"`
			Summary string `json:"summary"`
			Text    string `json:"text"`
		} `json:"output"`
		PullRequests []GitHubCheckRunPR `json:"pull_requests"`
	} `json:"check_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubWorkflowRunPayload struct {
	Action      string `json:"action"`
	WorkflowRun struct {
		ID           int64              `json:"id"`
		Name         string             `json:"name"`
		HeadSHA      string             `json:"head_sha"`
		HeadBranch   string             `json:"head_branch"`
		Status       string             `json:"status"`
		Conclusion   string             `json:"conclusion"`
		HTMLURL      string             `json:"html_url"`
		PullRequests []GitHubCheckRunPR `json:"pull_requests"`
	} `json:"workflow_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubWorkflowJobStep struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	Number     int    `json:"number"`
}

type GitHubWorkflowJobItem struct {
	ID         int64                   `json:"id"`
	Name       string                  `json:"name"`
	Status     string                  `json:"status"`
	Conclusion string                  `json:"conclusion"`
	Steps      []GitHubWorkflowJobStep `json:"steps,omitempty"`
}

// hasExecutedBuildStep verifies that if steps are present and a build/push step is defined,
// that step actually concluded with success rather than being skipped.
type hangarJSONResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func isHangarUnmanagedError(body []byte) bool {
	var hResp hangarJSONResponse
	if err := json.Unmarshal(body, &hResp); err != nil {
		return false
	}
	if hResp.Status == "not_found" {
		return true
	}
	msg := strings.ToLower(hResp.Message)
	return strings.Contains(msg, "is not managed by hangar") || strings.Contains(msg, "not managed by hangar") || strings.Contains(msg, "no nomad jobs found")
}

func hasExecutedBuildStep(steps []GitHubWorkflowJobStep) bool {
	if len(steps) == 0 {
		return true
	}
	var pushSteps []GitHubWorkflowJobStep
	for _, step := range steps {
		nameLower := strings.ToLower(step.Name)
		if (strings.Contains(nameLower, "build") && strings.Contains(nameLower, "push")) ||
			(strings.Contains(nameLower, "build") && strings.Contains(nameLower, "publish")) ||
			strings.Contains(nameLower, "docker push") ||
			strings.Contains(nameLower, "push image") ||
			strings.Contains(nameLower, "push docker") {
			pushSteps = append(pushSteps, step)
		}
	}
	if len(pushSteps) == 0 {
		return true
	}
	for _, ps := range pushSteps {
		if ps.Conclusion == "success" {
			return true
		}
	}
	return false
}

type GitHubWorkflowJobsResponse struct {
	TotalCount int                     `json:"total_count"`
	Jobs       []GitHubWorkflowJobItem `json:"jobs"`
}

var buildJobRegex = regexp.MustCompile(`(?i)Build\s*(?:&|and)\s*(?:Push|Publish)(?:\s+(?:Sidecar\s+)?(?:Container\s+)?Images?(?:\s+to\s+GHCR)?)?\s*\(([^)]+)\)`)


type GitHubCommit struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

type GitHubPushPayload struct {
	Ref        string        `json:"ref"`
	Before     string        `json:"before"`
	After      string        `json:"after"`
	Deleted    bool          `json:"deleted"`
	HeadCommit *GitHubCommit `json:"head_commit"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type ResolvedGitHubEvent struct {
	Event            string `json:"event"`
	Delivery         string `json:"delivery"`
	Action           string `json:"action,omitempty"`
	Repo             string `json:"repo,omitempty"`
	PRNumber         int    `json:"pr_number,omitempty"`
	Branch           string `json:"branch,omitempty"`
	HeadSHA          string `json:"head_sha,omitempty"`
	MergeSHA         string `json:"merge_sha,omitempty"`
	TargetID         string `json:"target_id,omitempty"`
	CheckName        string `json:"check_name,omitempty"`
	Status           string `json:"status,omitempty"`
	Conclusion       string `json:"conclusion,omitempty"`
	Sender           string `json:"sender,omitempty"`
	ResolutionSource string `json:"resolution_source,omitempty"`
}

type GitHubCommitPRItem struct {
	Number int    `json:"number"`
	State  string `json:"state"`
	Head   struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
}

func isZeroSHA(sha string) bool {
	if sha == "" {
		return true
	}
	for i := 0; i < len(sha); i++ {
		if sha[i] != '0' {
			return false
		}
	}
	return true
}

func (s *RouterServer) handleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	event := r.Header.Get("X-GitHub-Event")
	if strings.TrimSpace(event) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-GitHub-Event header"})
		return
	}
	delivery := r.Header.Get("X-GitHub-Delivery")

	body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	if s.riverClient == nil {
		log.Printf("[webhooks-router] [github] ERROR: river client not initialized")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "river queue not initialized"})
		return
	}

	args := GitHubWebhookArgs{
		Event:    event,
		Delivery: delivery,
		Body:     body,
	}

	// Synchronously persist to durable storage (PostgreSQL river_job table)
	// before acknowledging to GitHub (transactional outbox invariant).
	if _, err := s.riverClient.Insert(r.Context(), args, nil); err != nil {
		log.Printf("[webhooks-router] [github] ERROR: failed to insert job into river: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to persist webhook"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":   "accepted",
		"event":    event,
		"delivery": delivery,
	})
}

func (s *RouterServer) ProcessGitHubEvent(ctx context.Context, event, delivery string, body []byte) (*ResolvedGitHubEvent, error) {
	if s.onProcessed != nil {
		defer s.onProcessed(event, delivery)
	}
	res := &ResolvedGitHubEvent{
		Event:    event,
		Delivery: delivery,
	}

	switch event {
	case "ping":
		var p GitHubPingPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal ping payload: %w", err)
		}
		repo := ""
		if p.Repo != nil {
			repo = p.Repo.FullName
		}
		res.Repo = repo
		log.Printf("[webhooks-router] [github] [ping] hook_id=%d repo=%s zen=%q delivery=%s", p.HookID, repo, p.Zen, delivery)
		return res, nil

	case "pull_request":
		var p GitHubPullRequestPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal pull_request payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Action = p.Action
		res.PRNumber = p.Number
		if res.PRNumber == 0 {
			res.PRNumber = p.PullRequest.Number
		}
		res.Branch = p.PullRequest.Head.Ref
		res.HeadSHA = p.PullRequest.Head.SHA
		res.MergeSHA = p.PullRequest.MergeCommitSHA
		res.Sender = p.Sender.Login
		res.ResolutionSource = "pull_request_payload"
		log.Printf("[webhooks-router] [github] [pull_request] repo=%s pr=%d action=%s head_sha=%s merge_sha=%s branch=%s merged=%v sender=%s delivery=%s",
			res.Repo, res.PRNumber, res.Action, res.HeadSHA, res.MergeSHA, res.Branch, p.PullRequest.Merged, res.Sender, delivery)

		if s.registry != nil && res.PRNumber > 0 {
			if res.Action == "closed" {
				if p.PullRequest.Merged {
					targetID, err := s.registry.UpdatePRMerged(ctx, res.Repo, res.PRNumber, res.MergeSHA)
					if err != nil {
						log.Printf("[webhooks-router] [registry] error updating pr merged: %v", err)
						return nil, err
					}
					if targetID != "" && res.TargetID == "" {
						res.TargetID = targetID
					}
					log.Printf("[webhooks-router] [registry] pr %s#%d updated to merged (merge_sha=%s, target_id=%s)", res.Repo, res.PRNumber, res.MergeSHA, res.TargetID)

					if IsValidDiscordSnowflake(res.TargetID) && s.dispatcher != nil {
						msg := fmt.Sprintf("🔀 %sMerged into main", formatDirectMessagePrefix(res.PRNumber, res.Repo))
						if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
							ChannelID: res.TargetID,
							Content:   msg,
						}); err != nil {
							log.Printf("[webhooks-router] [dispatcher] error dispatching pr_merged direct message: %v", err)
							return nil, err
						}
					}
				} else {
					targetID, err := s.registry.UpdatePRClosedUnmerged(ctx, res.Repo, res.PRNumber)
					if err != nil {
						log.Printf("[webhooks-router] [registry] error updating pr closed: %v", err)
						return nil, err
					}
					if targetID != "" && res.TargetID == "" {
						res.TargetID = targetID
					}
					log.Printf("[webhooks-router] [registry] pr %s#%d updated to closed (unmerged, target_id=%s)", res.Repo, res.PRNumber, res.TargetID)
				}
			} else if res.Action == "synchronize" {
				targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [registry] error updating pr synchronize: %v", err)
					return nil, err
				}
				if targetID != "" && res.TargetID == "" {
					res.TargetID = targetID
				}
				log.Printf("[webhooks-router] [registry] pr %s#%d synchronized (head_sha=%s, status=open, target_id=%s)", res.Repo, res.PRNumber, res.HeadSHA, res.TargetID)
			} else if res.Action == "auto_merge_disabled" {
				targetID, updated, err := s.registry.AtomicTransitionConflict(ctx, res.Repo, res.PRNumber)
				if err != nil {
					log.Printf("[webhooks-router] [registry] error transitioning pr auto_merge_disabled to conflict: %v", err)
					return nil, err
				}
				if targetID != "" && res.TargetID == "" {
					res.TargetID = targetID
				}
				if updated {
					log.Printf("[webhooks-router] [registry] pr %s#%d auto_merge_disabled transitioned to conflict (target_id=%s)", res.Repo, res.PRNumber, res.TargetID)
					if err := s.dispatchConflictPrompt(ctx, res.TargetID, res.Repo, res.Branch, res.HeadSHA, res.PRNumber); err != nil {
						return nil, err
					}
				}
			}
		}
		return res, nil

	case "check_run":
		var p GitHubCheckRunPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal check_run payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Action = p.Action
		res.CheckName = p.CheckRun.Name
		res.HeadSHA = p.CheckRun.HeadSHA
		res.Status = p.CheckRun.Status
		res.Conclusion = p.CheckRun.Conclusion

		if len(p.CheckRun.PullRequests) > 0 {
			res.PRNumber = p.CheckRun.PullRequests[0].Number
			res.Branch = p.CheckRun.PullRequests[0].Head.Ref
			res.ResolutionSource = "check_run_payload"
			if s.registry != nil && res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
				if _, _, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err == nil && targetID != "" {
					res.TargetID = targetID
				}
			}
		} else if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
			if s.registry != nil {
				prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [registry] check_run failed resolving PR by SHA %s locally: %v", res.HeadSHA, err)
				} else if prNum > 0 {
					res.PRNumber = prNum
					res.Branch = branch
					res.TargetID = targetID
					res.ResolutionSource = "local_pr_registry"
				}
			}

			if res.PRNumber == 0 {
				prNum, branch, err := s.resolvePRFromCommitSHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [github] check_run failed resolving PR for commit %s: %v", res.HeadSHA, err)
				} else {
					res.PRNumber = prNum
					res.Branch = branch
					res.ResolutionSource = "commit_sha_lookup"
				}
			}
		}
		log.Printf("[webhooks-router] [github] [check_run] repo=%s check=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
			res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

		if s.registry != nil && (res.Conclusion == "failure" || res.Conclusion == "timed_out") {
			var details string
			if strings.TrimSpace(p.CheckRun.Output.Text) != "" {
				details = strings.TrimSpace(p.CheckRun.Output.Text)
			} else if strings.TrimSpace(p.CheckRun.Output.Summary) != "" {
				details = strings.TrimSpace(p.CheckRun.Output.Summary)
			} else if strings.TrimSpace(p.CheckRun.Output.Title) != "" {
				details = strings.TrimSpace(p.CheckRun.Output.Title)
			}
			checkURL := strings.TrimSpace(p.CheckRun.HTMLURL)
			if checkURL == "" {
				checkURL = strings.TrimSpace(p.CheckRun.DetailsURL)
			}

			if res.PRNumber > 0 {
				targetID, updated, err := s.registry.AtomicTransitionCIFailed(ctx, res.Repo, res.PRNumber)
				if err != nil {
					log.Printf("[webhooks-router] [registry] error transitioning pr ci_failed: %v", err)
					return nil, err
				}
				if targetID != "" && res.TargetID == "" {
					res.TargetID = targetID
				}
				if updated {
					log.Printf("[webhooks-router] [registry] pr %s#%d transitioned to ci_failed (check=%s, target_id=%s)", res.Repo, res.PRNumber, res.CheckName, targetID)
					if err := s.dispatchCIFailurePrompt(ctx, res.TargetID, res.CheckName, res.Repo, res.HeadSHA, res.PRNumber, details, checkURL); err != nil {
						return nil, err
					}
				}
			} else if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
				targetID, prNum, updated, err := s.registry.AtomicTransitionCIFailedByHeadSHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [registry] error transitioning head_sha ci_failed: %v", err)
					return nil, err
				}
				if targetID != "" && res.TargetID == "" {
					res.TargetID = targetID
				}
				if prNum > 0 && res.PRNumber == 0 {
					res.PRNumber = prNum
				}
				if updated {
					log.Printf("[webhooks-router] [registry] commit %s (pr %s#%d) transitioned to ci_failed (check=%s, target_id=%s)", res.HeadSHA, res.Repo, prNum, res.CheckName, targetID)
					pr := res.PRNumber
					if pr == 0 {
						pr = prNum
					}
					if err := s.dispatchCIFailurePrompt(ctx, res.TargetID, res.CheckName, res.Repo, res.HeadSHA, pr, details, checkURL); err != nil {
						return nil, err
					}
				}
			}
		}
		return res, nil

	case "workflow_run":
		var p GitHubWorkflowRunPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal workflow_run payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Action = p.Action
		res.CheckName = p.WorkflowRun.Name
		res.HeadSHA = p.WorkflowRun.HeadSHA
		res.Branch = p.WorkflowRun.HeadBranch
		res.Status = p.WorkflowRun.Status
		res.Conclusion = p.WorkflowRun.Conclusion

		if len(p.WorkflowRun.PullRequests) > 0 {
			res.PRNumber = p.WorkflowRun.PullRequests[0].Number
			res.ResolutionSource = "workflow_run_payload"
			if s.registry != nil && res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
				if _, _, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err == nil && targetID != "" {
					res.TargetID = targetID
				}
			}
		} else if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
			if s.registry != nil {
				prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [registry] workflow_run failed resolving PR by SHA %s locally: %v", res.HeadSHA, err)
				} else if prNum > 0 {
					res.PRNumber = prNum
					if res.Branch == "" {
						res.Branch = branch
					}
					res.TargetID = targetID
					res.ResolutionSource = "local_pr_registry"
				}
			}

			if res.PRNumber == 0 {
				prNum, branch, err := s.resolvePRFromCommitSHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [github] workflow_run failed resolving PR for commit %s: %v", res.HeadSHA, err)
				} else {
					res.PRNumber = prNum
					if res.Branch == "" {
						res.Branch = branch
					}
					res.ResolutionSource = "commit_sha_lookup"
				}
			}
		}
		log.Printf("[webhooks-router] [github] [workflow_run] repo=%s workflow=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
			res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

		if s.registry != nil && (res.Conclusion == "failure" || res.Conclusion == "timed_out") {
			details := fmt.Sprintf("Workflow %q concluded with %s", p.WorkflowRun.Name, p.WorkflowRun.Conclusion)
			checkURL := strings.TrimSpace(p.WorkflowRun.HTMLURL)

			if res.PRNumber > 0 {
				targetID, updated, err := s.registry.AtomicTransitionCIFailed(ctx, res.Repo, res.PRNumber)
				if err != nil {
					log.Printf("[webhooks-router] [registry] error transitioning pr ci_failed: %v", err)
					return nil, err
				}
				if targetID != "" && res.TargetID == "" {
					res.TargetID = targetID
				}
				if updated {
					log.Printf("[webhooks-router] [registry] pr %s#%d transitioned to ci_failed (workflow=%s, target_id=%s)", res.Repo, res.PRNumber, res.CheckName, targetID)
					if err := s.dispatchCIFailurePrompt(ctx, res.TargetID, res.CheckName, res.Repo, res.HeadSHA, res.PRNumber, details, checkURL); err != nil {
						return nil, err
					}
				}
			} else if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
				targetID, prNum, updated, err := s.registry.AtomicTransitionCIFailedByHeadSHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [registry] error transitioning head_sha ci_failed: %v", err)
					return nil, err
				}
				if targetID != "" && res.TargetID == "" {
					res.TargetID = targetID
				}
				if prNum > 0 && res.PRNumber == 0 {
					res.PRNumber = prNum
				}
				if updated {
					log.Printf("[webhooks-router] [registry] commit %s (pr %s#%d) transitioned to ci_failed (workflow=%s, target_id=%s)", res.HeadSHA, res.Repo, prNum, res.CheckName, targetID)
					pr := res.PRNumber
					if pr == 0 {
						pr = prNum
					}
					if err := s.dispatchCIFailurePrompt(ctx, res.TargetID, res.CheckName, res.Repo, res.HeadSHA, pr, details, checkURL); err != nil {
						return nil, err
					}
				}
			}
		}

		if p.Action == "completed" && p.WorkflowRun.Status == "completed" && p.WorkflowRun.Conclusion == "success" && (p.WorkflowRun.HeadBranch == "main" || strings.HasPrefix(p.WorkflowRun.HeadBranch, "refs/heads/main")) {
			if err := s.dispatchWorkflowRunImages(ctx, res.Repo, p.WorkflowRun.ID, p.WorkflowRun.HeadBranch, p.WorkflowRun.Conclusion, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
				return nil, err
			}
		}

		return res, nil

	case "push":
		var p GitHubPushPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal push payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Branch = p.Ref

		// Guard: Devil's Advocate requirements:
		// 1. Branch deletion / zero SHA guard
		// 2. Non-branch push (e.g. tag pushes)
		if p.Deleted || isZeroSHA(p.After) {
			log.Printf("[webhooks-router] [github] [push] branch deletion detected on ref %s (repo %s), skipping PR resolution", p.Ref, res.Repo)
			res.ResolutionSource = "branch_deleted"
			return res, nil
		}
		if !strings.HasPrefix(p.Ref, "refs/heads/") {
			log.Printf("[webhooks-router] [github] [push] non-branch push on ref %s (repo %s), skipping PR resolution", p.Ref, res.Repo)
			res.ResolutionSource = "non_branch_ref"
			return res, nil
		}

		res.HeadSHA = p.After
		if p.HeadCommit != nil && p.HeadCommit.ID != "" {
			res.HeadSHA = p.HeadCommit.ID
		}

		if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
			if s.registry != nil {
				prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [registry] push failed resolving PR by SHA %s locally: %v", res.HeadSHA, err)
				} else if prNum > 0 {
					res.PRNumber = prNum
					res.MergeSHA = res.HeadSHA
					res.TargetID = targetID
					if branch != "" {
						res.Branch = branch
					}
					res.ResolutionSource = "local_pr_registry"
				}
			}

			if res.PRNumber == 0 {
				prNum, branch, err := s.resolvePRFromCommitSHA(ctx, res.Repo, res.HeadSHA)
				if err != nil {
					log.Printf("[webhooks-router] [github] push failed resolving PR for commit %s: %v", res.HeadSHA, err)
				} else {
					res.PRNumber = prNum
					res.MergeSHA = res.HeadSHA
					if branch != "" {
						res.Branch = branch
					}
					res.ResolutionSource = "commit_sha_lookup"
				}
			}
		}
		log.Printf("[webhooks-router] [github] [push] repo=%s ref=%s head_sha=%s resolved_pr=%d branch=%s source=%s target_id=%s delivery=%s",
			res.Repo, p.Ref, res.HeadSHA, res.PRNumber, res.Branch, res.ResolutionSource, res.TargetID, delivery)

		if s.registry != nil && res.PRNumber > 0 && res.MergeSHA != "" {
			targetID, err := s.registry.BackfillPushMergeSHA(ctx, res.Repo, res.PRNumber, res.MergeSHA)
			if err != nil {
				log.Printf("[webhooks-router] [registry] error backfilling push merge_sha: %v", err)
				return nil, err
			}
			if targetID != "" && res.TargetID == "" {
				res.TargetID = targetID
			}
			log.Printf("[webhooks-router] [registry] pr %s#%d backfilled merge_sha (%s, target_id=%s)", res.Repo, res.PRNumber, res.MergeSHA, res.TargetID)
		}

		if err := s.dispatchGitPush(ctx, res.Repo, p.Ref, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
			return nil, err
		}
		if strings.HasPrefix(p.Ref, "refs/heads/main") {
			if s.conflictCheckDelay > 0 {
				select {
				case <-time.After(s.conflictCheckDelay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			if err := s.checkOpenPRConflicts(ctx, res.Repo); err != nil {
				log.Printf("[webhooks-router] [github] error checking open pr conflicts for %s: %v", res.Repo, err)
			}
		}
		return res, nil

	default:
		log.Printf("[webhooks-router] [github] [unhandled] event=%s delivery=%s payload_size=%d bytes", event, delivery, len(body))
		res.ResolutionSource = "unhandled_event"
		return res, nil
	}
}

func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", fmt.Errorf("invalid repo or sha for pr resolution")
	}

	if !strings.Contains(repo, "/") {
		repo = "azylman/" + repo
	}

	apiURL := s.cfg.GitHubAPIURL
	if apiURL == "" {
		apiURL = "https://api.github.com"
	}

	endpoint := fmt.Sprintf("%s/repos/%s/commits/%s/pulls", apiURL, repo, url.PathEscape(sha))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, "", fmt.Errorf("create github api request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if s.cfg.GitHubToken != "" {
		req.Header.Set("Authorization", "token "+s.cfg.GitHubToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("github api request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-RateLimit-Remaining")
		log.Printf("[webhooks-router] [github] WARN: GitHub API 403 Forbidden (rate-limited? X-RateLimit-Remaining=%s)", remaining)
		return 0, "", fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("github api returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, "", fmt.Errorf("read github api response: %w", err)
	}

	var prs []GitHubCommitPRItem
	if err := json.Unmarshal(body, &prs); err != nil {
		return 0, "", fmt.Errorf("unmarshal github api prs: %w", err)
	}

	if len(prs) == 0 {
		return 0, "", fmt.Errorf("no pull requests associated with commit %s", sha)
	}

	selected := prs[0]
	for _, item := range prs[1:] {
		if selected.State != "closed" && item.State == "closed" {
			selected = item
		} else if selected.State == item.State && item.Number > selected.Number {
			selected = item
		}
	}

	return selected.Number, selected.Head.Ref, nil
}

func (s *RouterServer) resolveWorkflowRunImages(ctx context.Context, repo string, runID int64) ([]string, error) {
	if repo == "" || runID <= 0 {
		return nil, fmt.Errorf("invalid repo or runID for workflow run images resolution")
	}

	repo = normalizeRepo(repo)

	apiURL := s.cfg.GitHubAPIURL
	if apiURL == "" {
		apiURL = "https://api.github.com"
	}

	endpoint := fmt.Sprintf("%s/repos/%s/actions/runs/%d/jobs?per_page=100", apiURL, repo, runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create github api request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if s.cfg.GitHubToken != "" {
		req.Header.Set("Authorization", "token "+s.cfg.GitHubToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github api request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-RateLimit-Remaining")
		log.Printf("[webhooks-router] [github] WARN: GitHub API 403 Forbidden in resolveWorkflowRunImages (rate-limited? X-RateLimit-Remaining=%s)", remaining)
		return nil, fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github api returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
	if err != nil {
		return nil, fmt.Errorf("read github api response: %w", err)
	}

	var jobsResp GitHubWorkflowJobsResponse
	if err := json.Unmarshal(body, &jobsResp); err != nil {
		return nil, fmt.Errorf("unmarshal github api workflow jobs: %w", err)
	}

	owner := "azylman"
	parts := strings.Split(repo, "/")
	if len(parts) == 2 {
		owner = strings.ToLower(parts[0])
	}

	seen := make(map[string]struct{})
	var images []string

	for _, job := range jobsResp.Jobs {
		if job.Status != "completed" || job.Conclusion != "success" {
			continue
		}
		if !hasExecutedBuildStep(job.Steps) {
			continue
		}
		m := buildJobRegex.FindStringSubmatch(job.Name)
		if len(m) < 2 {
			continue
		}
		service := strings.TrimSpace(m[1])
		var img string
		cleanRepo := strings.ToLower(repo)
		if strings.HasSuffix(cleanRepo, "aerial-sidecars") || strings.Contains(cleanRepo, "sidecar") {
			if strings.HasPrefix(service, "orin-") {
				img = fmt.Sprintf("ghcr.io/%s/%s:latest", owner, service)
			} else {
				img = fmt.Sprintf("ghcr.io/%s/aerial-sidecar-%s:latest", owner, service)
			}
		} else if strings.HasSuffix(cleanRepo, "mirrormere") || strings.Contains(cleanRepo, "mirrormere") {
			img = fmt.Sprintf("ghcr.io/%s/%s:latest", owner, service)
		} else {
			img = fmt.Sprintf("ghcr.io/%s/aerial-%s:latest", owner, service)
		}
		if _, exists := seen[img]; !exists {
			seen[img] = struct{}{}
			images = append(images, img)
		}
	}

	return images, nil
}

func (s *RouterServer) getInfisicalToken(ctx context.Context) (string, error) {
	if s.cfg.InfisicalClientID == "" || s.cfg.InfisicalClientSecret == "" {
		return "", fmt.Errorf("infisical client ID or client secret not configured")
	}

	loginURL := fmt.Sprintf("%s/api/v1/auth/universal-auth/login", s.cfg.InfisicalURL)
	payload, err := json.Marshal(map[string]string{
		"clientId":     s.cfg.InfisicalClientID,
		"clientSecret": s.cfg.InfisicalClientSecret,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, strings.NewReader(string(payload)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("infisical login request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("reading infisical login response: %w", readErr)
		}
		return "", fmt.Errorf("infisical login returned status %d: %s", resp.StatusCode, string(body))
	}

	var loginResp UniversalAuthLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return "", fmt.Errorf("decoding infisical login response: %w", err)
	}

	token := loginResp.AccessToken
	if token == "" {
		token = loginResp.Token
	}
	if token == "" {
		return "", fmt.Errorf("empty access token in infisical login response")
	}

	return token, nil
}

func (s *RouterServer) fetchInfisicalSecrets(ctx context.Context, token, workspaceID, envName string) (map[string]string, error) {
	if workspaceID == "" {
		workspaceID = s.cfg.InfisicalProjectID
	}
	if envName == "" {
		envName = s.cfg.InfisicalEnvironment
	}

	params := url.Values{}
	params.Set("workspaceId", workspaceID)
	params.Set("environment", envName)
	params.Set("secretPath", "/")

	fetchURL := fmt.Sprintf("%s/api/v3/secrets/raw?%s", s.cfg.InfisicalURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fetchURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("infisical fetch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("reading infisical fetch response: %w", readErr)
		}
		return nil, fmt.Errorf("infisical fetch returned status %d: %s", resp.StatusCode, string(body))
	}

	var secretsResp InfisicalSecretsResponse
	if err := json.NewDecoder(resp.Body).Decode(&secretsResp); err != nil {
		return nil, fmt.Errorf("decoding infisical secrets response: %w", err)
	}

	items := make(map[string]string, len(secretsResp.Secrets))
	for _, item := range secretsResp.Secrets {
		items[item.SecretKey] = item.SecretValue
	}
	return items, nil
}

func (s *RouterServer) putNomadVariable(ctx context.Context, items map[string]string) error {
	reqURL := fmt.Sprintf("%s/v1/var/%s", s.cfg.NomadAddr, TargetNomadVariable)

	nomadVar := NomadVariable{
		Path:  TargetNomadVariable,
		Items: items,
	}
	data, err := json.Marshal(nomadVar)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.NomadToken != "" {
		req.Header.Set("X-Nomad-Token", s.cfg.NomadToken)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("nomad put variable request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("reading nomad put var response: %w", readErr)
		}
		return fmt.Errorf("nomad put variable returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// SyncSecrets coordinates fetching secrets from Infisical and synchronizing directly to Nomad.
func (s *RouterServer) SyncSecrets(ctx context.Context, workspaceID, envName string) error {
	log.Printf("[webhooks-router] Initiating secret sync to %s (env: %s)", TargetNomadVariable, envName)

	token, err := s.getInfisicalToken(ctx)
	if err != nil {
		log.Printf("[webhooks-router] ERROR authenticating with infisical: %v", err)
		return err
	}

	newItems, err := s.fetchInfisicalSecrets(ctx, token, workspaceID, envName)
	if err != nil {
		log.Printf("[webhooks-router] ERROR fetching secrets from infisical: %v", err)
		return err
	}

	if err := s.putNomadVariable(ctx, newItems); err != nil {
		log.Printf("[webhooks-router] ERROR updating nomad variable %s: %v", TargetNomadVariable, err)
		return err
	}

	log.Printf("[webhooks-router] Successfully synchronized %d secrets to %s", len(newItems), TargetNomadVariable)
	return nil
}

// runServer runs the HTTP server with graceful shutdown and optional ready callback.
func runServer(ctx context.Context, cfg Config, onReady func(addr string)) error {
	server := NewRouterServer(cfg, nil)
	server.SetDispatcher(NewDefaultOutboundDispatcher(cfg.HangarURL, cfg.BrainURL, nil))

	var riverClient *river.Client[pgx.Tx]
	var dbPool *pgxpool.Pool

	if cfg.PostgresURL != "" {
		poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
		if err != nil {
			return fmt.Errorf("parse postgres url: %w", err)
		}
		poolConfig.MaxConns = 10
		poolConfig.MinConns = 2
		if poolConfig.ConnConfig.RuntimeParams == nil {
			poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
		}
		if _, ok := poolConfig.ConnConfig.RuntimeParams["search_path"]; !ok {
			poolConfig.ConnConfig.RuntimeParams["search_path"] = "sidecars, public"
		}

		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return fmt.Errorf("create pgxpool: %w", err)
		}
		defer pool.Close()
		dbPool = pool
		server.SetRegistry(NewPostgresPRRegistry(dbPool))

		workers := river.NewWorkers()
		river.AddWorker(workers, &GitHubWebhookWorker{server: server})
		river.AddWorker(workers, &HangarWebhookWorker{server: server})
		river.AddWorker(workers, &NomadEventWorker{server: server})

		rc, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.QueueDefault: {MaxWorkers: 10},
			},
			Workers: workers,
		})
		if err != nil {
			return fmt.Errorf("create river client: %w", err)
		}
		riverClient = rc
		server.SetRiverClient(riverClient)

		if err := riverClient.Start(ctx); err != nil {
			return fmt.Errorf("start river client: %w", err)
		}
		log.Println("[webhooks-router] River worker pool started successfully")
	}

	// Initialize and launch Nomad Event Stream subscriber
	nomadSub := NewNomadStreamSubscriber(server)
	nomadSub.Start(ctx)

	// Reconcile any active deployments from pr_registry (startup sweep)
	go func() {
		sweepCtx, sweepCancel := context.WithTimeout(ctx, 15*time.Second)
		defer sweepCancel()
		if sweepErr := server.ReconcileActiveDeployments(sweepCtx); sweepErr != nil {
			log.Printf("[webhooks-router] [sweep] startup active deployment reconciliation error: %v", sweepErr)
		}
	}()

	ln, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		return fmt.Errorf("listening on port %s: %w", cfg.Port, err)
	}

	httpServer := &http.Server{
		Handler:      server.Routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if onReady != nil {
		onReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[webhooks-router] Server listening on %s (Infisical: %s, Nomad: %s, TargetVariable: %s)",
			ln.Addr().String(), cfg.InfisicalURL, cfg.NomadAddr, TargetNomadVariable)
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errChan:
		runErr = err
	}

	log.Println("[webhooks-router] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if shutErr := httpServer.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[webhooks-router] HTTP shutdown error: %v", shutErr)
	}

	if riverClient != nil {
		if stopErr := riverClient.Stop(shutdownCtx); stopErr != nil && !errors.Is(stopErr, context.Canceled) {
			log.Printf("[webhooks-router] River client stop error: %v", stopErr)
		}
	}

	log.Println("[webhooks-router] Server stopped")
	return runErr
}

// runApp runs the webhooks-router application.
func runApp(ctx context.Context, cfg Config) error {
	return runServer(ctx, cfg, nil)
}

func main() {
	cfg := LoadConfigFromEnv()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[webhooks-router] Error: %v", err)
	}
}
