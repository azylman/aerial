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
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
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

// LoadConfigFromEnv initializes configuration from environment variables.
func LoadConfigFromEnv() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "4020"
	}

	infURL := os.Getenv("INFISICAL_URL")
	if infURL == "" {
		infURL = os.Getenv("INFISICAL_HOST_URL")
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
	if envName == "" {
		envName = "prod"
	}

	nomadAddr := os.Getenv("NOMAD_ADDR")
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
	if hangarURL == "" {
		hangarURL = "http://127.0.0.1:8087"
	}

	brainURL := os.Getenv("BRAIN_INTERNAL_URL")
	if brainURL == "" {
		brainURL = os.Getenv("BRAIN_URL")
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
	log.Printf("[webhooks-router] [river] processing job id=%d event=%s delivery=%s", job.ID, job.Args.Event, job.Args.Delivery)
	_, err := w.server.ProcessGitHubEvent(ctx, job.Args.Event, job.Args.Delivery, job.Args.Body)
	if err != nil {
		log.Printf("[webhooks-router] [river] error processing job id=%d event=%s delivery=%s: %v", job.ID, job.Args.Event, job.Args.Delivery, err)
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

// PRRegistryUpdater defines the interface for updating PR lifecycle state in PostgreSQL pr_registry.
type PRRegistryUpdater interface {
	UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (targetID string, err error)
	UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (targetID string, err error)
	AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (targetID string, prNumber int, updated bool, err error)
	BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	ResolvePRBySHA(ctx context.Context, repo string, sha string) (prNumber int, branch, targetID string, err error)
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

// Discord Snowflake validation
var snowflakeRegex = regexp.MustCompile(`^[0-9]{17,20}$`)

func IsValidDiscordSnowflake(id string) bool {
	return snowflakeRegex.MatchString(strings.TrimSpace(id))
}

type GitPushEventRequest struct {
	Repo   string `json:"repo"`
	Ref    string `json:"ref"`
	Commit string `json:"commit"`
}

type ImageReadyEventRequest struct {
	Image  string `json:"image"`
	Digest string `json:"digest,omitempty"`
}

type PromptRequest struct {
	ChannelID string `json:"channel_id"`
	Prompt    string `json:"prompt"`
}

type DirectMessageRequest struct {
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
}

type OutboundDispatcher interface {
	DispatchGitPush(ctx context.Context, req GitPushEventRequest) error
	DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) error
	DispatchPrompt(ctx context.Context, req PromptRequest) error
	DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error
}

type DefaultOutboundDispatcher struct {
	hangarURL  string
	brainURL   string
	httpClient *http.Client
	retryDelay time.Duration
}

func NewDefaultOutboundDispatcher(hangarURL, brainURL string, client *http.Client) *DefaultOutboundDispatcher {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &DefaultOutboundDispatcher{
		hangarURL:  strings.TrimRight(hangarURL, "/"),
		brainURL:   strings.TrimRight(brainURL, "/"),
		httpClient: client,
		retryDelay: 500 * time.Millisecond,
	}
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("hangar git push returned status %d (read error: %w)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("hangar git push returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (d *DefaultOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) error {
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal image ready request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/events/image_ready", d.hangarURL)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create image ready request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("image ready request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("hangar image ready returned status %d (read error: %w)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("hangar image ready returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
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
			return fmt.Errorf("create prompt request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("prompt request failed (attempt %d/%d): %w", attempt, maxAttempts, err)
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
			lastErr = fmt.Errorf("brain prompt returned status %d (attempt %d/%d): %s", resp.StatusCode, attempt, maxAttempts, string(respBody))
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
	cfg         Config
	httpClient  *http.Client
	riverClient RiverInserter
	registry    PRRegistryUpdater
	dispatcher  OutboundDispatcher
	onProcessed func(event, delivery string)
}

// NewRouterServer constructs a new RouterServer instance.
func NewRouterServer(cfg Config, client *http.Client, riverClient ...RiverInserter) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	s := &RouterServer{
		cfg:        cfg,
		httpClient: client,
	}
	if len(riverClient) > 0 {
		s.riverClient = riverClient[0]
	}
	return s
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
	return mux
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

	if strings.TrimSpace(evt.Event) == "" || strings.TrimSpace(evt.JobName) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing required fields"})
		return
	}

	processed := s.ProcessHangarEvent(r.Context(), evt)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "accepted",
		"event":     processed.Event,
		"job_name":  processed.JobName,
		"pr_number": processed.PRNumber,
		"target_id": processed.TargetID,
	})
}

func (s *RouterServer) ProcessHangarEvent(ctx context.Context, evt HangarDeployEvent) *HangarDeployEvent {
	if evt.CommitSHA != "" && evt.PRNumber == 0 && s.registry != nil {
		prNum, _, targetID, err := s.registry.ResolvePRBySHA(ctx, evt.Repo, evt.CommitSHA)
		if err != nil {
			log.Printf("[webhooks-router] [hangar] failed resolving PR by SHA %s: %v", evt.CommitSHA, err)
		} else if prNum > 0 {
			evt.PRNumber = prNum
			if evt.TargetID == "" {
				evt.TargetID = targetID
			}
		}
	}

	log.Printf("[webhooks-router] [hangar] [deploy] event=%s job=%s repo=%s pr=%d commit=%s status=%s target_id=%s deployment_id=%s details=%q",
		evt.Event, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA, evt.Status, evt.TargetID, evt.DeploymentID, evt.Details)

	if s.registry != nil && evt.PRNumber > 0 {
		if evt.Event == "deploy_success" {
			log.Printf("[webhooks-router] [registry] pr %s#%d deployed successfully for job %s", evt.Repo, evt.PRNumber, evt.JobName)
		} else if evt.Event == "deploy_rollback" {
			log.Printf("[webhooks-router] [registry] pr %s#%d deployment rolled back for job %s", evt.Repo, evt.PRNumber, evt.JobName)
		}
	}

	if evt.Event == "deploy_success" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		msg := fmt.Sprintf("Deployment succeeded for job %s (PR #%d, commit %s).", evt.JobName, evt.PRNumber, evt.CommitSHA)
		go func(tID, m string) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[webhooks-router] [dispatcher] PANIC recovered in DispatchDirectMessage (deploy_success): %v", r)
				}
			}()
			dispCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.dispatcher.DispatchDirectMessage(dispCtx, DirectMessageRequest{
				ChannelID: tID,
				Content:   m,
			}); err != nil {
				log.Printf("[webhooks-router] [dispatcher] error dispatching deploy_success direct message: %v", err)
			}
		}(targetID, msg)
	} else if evt.Event == "deploy_rollback" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		details := evt.Details
		runes := []rune(details)
		if len(runes) > 500 {
			details = string(runes[:500])
		}
		msg := fmt.Sprintf("Deployment failed and rolled back for job %s (PR #%d, commit %s): %s.", evt.JobName, evt.PRNumber, evt.CommitSHA, details)
		go func(tID, m string) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[webhooks-router] [dispatcher] PANIC recovered in DispatchDirectMessage (deploy_rollback): %v", r)
				}
			}()
			dispCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.dispatcher.DispatchDirectMessage(dispCtx, DirectMessageRequest{
				ChannelID: tID,
				Content:   m,
			}); err != nil {
				log.Printf("[webhooks-router] [dispatcher] error dispatching deploy_rollback direct message: %v", err)
			}
		}(targetID, msg)
	} else if evt.Event == "deploy_failed" && IsValidDiscordSnowflake(evt.TargetID) && s.dispatcher != nil {
		targetID := evt.TargetID
		details := evt.Details
		runes := []rune(details)
		if len(runes) > 500 {
			details = string(runes[:500])
		}
		msg := fmt.Sprintf("Deployment failed for job %s (PR #%d, commit %s): %s.", evt.JobName, evt.PRNumber, evt.CommitSHA, details)
		go func(tID, m string) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[webhooks-router] [dispatcher] PANIC recovered in DispatchDirectMessage (deploy_failed): %v", r)
				}
			}()
			dispCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.dispatcher.DispatchDirectMessage(dispCtx, DirectMessageRequest{
				ChannelID: tID,
				Content:   m,
			}); err != nil {
				log.Printf("[webhooks-router] [dispatcher] error dispatching deploy_failed direct message: %v", err)
			}
		}(targetID, msg)
	}

	return &evt
}

func (s *RouterServer) dispatchCIFailurePrompt(targetID, checkName, repo, headSHA string, prNum int) {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return
	}
	prompt := fmt.Sprintf("CI check %q failed for PR #%d on %s (head: %s). Please diagnose and fix the failure.", checkName, prNum, repo, headSHA)
	go func(tID, p string) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] [dispatcher] PANIC recovered in DispatchPrompt (ci_failed): %v", r)
			}
		}()
		dispCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.dispatcher.DispatchPrompt(dispCtx, PromptRequest{
			ChannelID: tID,
			Prompt:    p,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching ci_failed prompt: %v", err)
		} else {
			log.Printf("[webhooks-router] [dispatcher] successfully dispatched ci_failed prompt for PR #%d to %s", prNum, tID)
		}
	}(targetID, prompt)
}

func (s *RouterServer) dispatchGitPush(repo, ref, commit string) {
	if s.dispatcher == nil || !strings.HasPrefix(ref, "refs/heads/main") || isZeroSHA(commit) {
		return
	}
	go func(rp, rf, cm string) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] [dispatcher] PANIC recovered in DispatchGitPush: %v", r)
			}
		}()
		dispCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.dispatcher.DispatchGitPush(dispCtx, GitPushEventRequest{
			Repo:   rp,
			Ref:    rf,
			Commit: cm,
		}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error dispatching git_push to hangar for %s: %v", rp, err)
		} else {
			log.Printf("[webhooks-router] [dispatcher] successfully dispatched git_push to hangar for %s (%s)", rp, cm)
		}
	}(repo, ref, commit)
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
		PullRequests []GitHubCheckRunPR `json:"pull_requests"`
	} `json:"workflow_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

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
					s.dispatchCIFailurePrompt(res.TargetID, res.CheckName, res.Repo, res.HeadSHA, res.PRNumber)
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
					s.dispatchCIFailurePrompt(res.TargetID, res.CheckName, res.Repo, res.HeadSHA, pr)
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
					s.dispatchCIFailurePrompt(res.TargetID, res.CheckName, res.Repo, res.HeadSHA, res.PRNumber)
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
					s.dispatchCIFailurePrompt(res.TargetID, res.CheckName, res.Repo, res.HeadSHA, pr)
				}
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

		s.dispatchGitPush(res.Repo, p.Ref, res.HeadSHA)

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

	return prs[0].Number, prs[0].Head.Ref, nil
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
