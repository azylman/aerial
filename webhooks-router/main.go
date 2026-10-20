package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
	"gopkg.in/yaml.v3"
)

// TargetNomadVariable is the hardcoded destination Nomad variable for all synced secrets.
const TargetNomadVariable = "nomad/jobs/shared"

// VanadisQueueName is the River queue name for Vanadis build chunk execution jobs.
const VanadisQueueName = "vanadis"

// UniqueSkippedTotal records the total number of River jobs dropped due to unique constraint debouncing.
var UniqueSkippedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "webhooks_router_unique_skipped_total",
		Help: "Total number of River jobs skipped as duplicates due to unique constraints.",
	},
	[]string{"kind"},
)

func init() {
	prometheus.MustRegister(UniqueSkippedTotal)
}

// RecordUniqueSkipped increments the UniqueSkippedTotal counter for the specified kind.
func RecordUniqueSkipped(kind string) {
	if kind == "" {
		return
	}
	UniqueSkippedTotal.WithLabelValues(kind).Inc()
}

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
	SystemChannelID       string
}

// RouterYAMLConfig defines the declarative file schema for services/webhooks-router/webhooks-router.yaml.
type RouterYAMLConfig struct {
	Port                 string `yaml:"port"`
	InfisicalURL         string `yaml:"infisical_url"`
	InfisicalEnvironment string `yaml:"infisical_environment"`
	NomadAddr            string `yaml:"nomad_addr"`
	HangarURL            string `yaml:"hangar_url"`
	BrainURL             string `yaml:"brain_url"`
	SystemChannelID      string `yaml:"system_channel_id"`
}

var webhooksRouterFallbackConfigPath = "/config/webhooks-router.yaml"

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

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

	pgURL := firstNonEmpty(os.Getenv("POSTGRES_URL"), os.Getenv("DATABASE_URL"), "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable")

	return Config{
		Port:                  firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4020"),
		InfisicalURL:          strings.TrimRight(firstNonEmpty(os.Getenv("INFISICAL_URL"), os.Getenv("INFISICAL_HOST_URL"), fileCfg.InfisicalURL, "http://infisical.aerial"), "/"),
		InfisicalClientID:     firstNonEmpty(os.Getenv("INFISICAL_CLIENT_ID"), os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID")),
		InfisicalClientSecret: firstNonEmpty(os.Getenv("INFISICAL_CLIENT_SECRET"), os.Getenv("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET")),
		InfisicalProjectID:    firstNonEmpty(os.Getenv("INFISICAL_PROJECT_ID"), os.Getenv("INFISICAL_WORKSPACE_ID")),
		InfisicalEnvironment:  firstNonEmpty(os.Getenv("INFISICAL_ENVIRONMENT"), fileCfg.InfisicalEnvironment, "prod"),
		NomadAddr:             strings.TrimRight(firstNonEmpty(os.Getenv("NOMAD_ADDR"), fileCfg.NomadAddr, "http://127.0.0.1:4646"), "/"),
		NomadToken:            os.Getenv("NOMAD_TOKEN"),
		GitHubToken:           firstNonEmpty(os.Getenv("GITHUB_PAT"), os.Getenv("GITHUB_TOKEN")),
		GitHubAPIURL:          strings.TrimRight(firstNonEmpty(os.Getenv("GITHUB_API_URL"), "https://api.github.com"), "/"),
		PostgresURL:           pgURL,
		HangarURL:             strings.TrimRight(firstNonEmpty(os.Getenv("HANGAR_URL"), fileCfg.HangarURL, "http://127.0.0.1:8087"), "/"),
		BrainURL:              strings.TrimRight(firstNonEmpty(os.Getenv("BRAIN_INTERNAL_URL"), os.Getenv("BRAIN_URL"), fileCfg.BrainURL, "http://127.0.0.1:8088"), "/"),
		SystemChannelID:       strings.TrimSpace(firstNonEmpty(os.Getenv("SYSTEM_CHANNEL_ID"), os.Getenv("DISCORD_CHANNEL_ID"), fileCfg.SystemChannelID)),
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
	var dataWS, dataProj string
	if p.Data != nil {
		dataWS, dataProj = p.Data.WorkspaceID, p.Data.ProjectID
	}
	return firstNonEmpty(p.WorkspaceID, p.ProjectID, dataWS, dataProj, fallback)
}

func (p *InfisicalWebhookPayload) GetEnvironment(fallback string) string {
	var dataEnv string
	if p.Data != nil {
		dataEnv = p.Data.Environment
	}
	return firstNonEmpty(p.Environment, dataEnv, fallback)
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

type riverJobArgsWithOpts interface {
	river.JobArgs
	InsertOpts() river.InsertOpts
}

func defaultRiverInsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: 5,
		UniqueOpts:  river.UniqueOpts{ByArgs: true, ByPeriod: 15 * time.Minute},
	}
}

func (s *RouterServer) insertRiverJob(ctx context.Context, args riverJobArgsWithOpts) error {
	opts := args.InsertOpts()
	res, err := s.riverClient.Insert(ctx, args, &opts)
	if err != nil {
		return err
	}
	if res != nil && res.UniqueSkippedAsDuplicate {
		RecordUniqueSkipped(args.Kind())
	}
	return nil
}

// GitHubWebhookArgs contains payload data for durable background processing of GitHub webhooks.
type GitHubWebhookArgs struct {
	Event    string `json:"event"`
	Delivery string `json:"delivery" river:"unique"`
	Body     []byte `json:"body"`
}

func (GitHubWebhookArgs) Kind() string                 { return "github_webhook" }
func (GitHubWebhookArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

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
	if _, err := w.server.ProcessGitHubEvent(ctx, job.Args.Event, job.Args.Delivery, job.Args.Body); err != nil {
		log.Printf("[webhooks-router] [river] error processing github job id=%d event=%s delivery=%s: %v", job.ID, job.Args.Event, job.Args.Delivery, err)
		return err
	}
	return nil
}

func randomNonce() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// HangarWebhookArgs contains payload data for durable background processing of Hangar webhooks.
type HangarWebhookArgs struct {
	IdempotencyKey string            `json:"idempotency_key,omitempty" river:"unique"`
	Event          HangarDeployEvent `json:"event"`
}

func (HangarWebhookArgs) Kind() string                 { return "hangar_webhook" }
func (HangarWebhookArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

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
	if _, err := w.server.ProcessHangarEvent(ctx, job.Args.Event); err != nil {
		log.Printf("[webhooks-router] [river] error processing hangar job id=%d: %v", job.ID, err)
		return err
	}
	return nil
}

// NomadEventArgs contains payload data for durable background processing of Nomad stream events.
type NomadEventArgs struct {
	IdempotencyKey string          `json:"idempotency_key,omitempty" river:"unique"`
	Topic          string          `json:"topic"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Index          uint64          `json:"index,omitempty"`
}

func (NomadEventArgs) Kind() string                 { return "nomad_event" }
func (NomadEventArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

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
	if err := w.server.ProcessNomadEvent(ctx, job.Args.Topic, job.Args.Type, job.Args.Payload); err != nil {
		log.Printf("[webhooks-router] [river] error processing nomad job id=%d: %v", job.ID, err)
		return err
	}
	return nil
}

// NomadCrashAlertArgs contains payload data for durable background processing of unmanaged Nomad crash alerts.
type NomadCrashAlertArgs struct {
	JobID      string `json:"job_id" river:"unique"`
	AllocID    string `json:"alloc_id"`
	Status     string `json:"status"`
	StatusDesc string `json:"status_desc"`
}

func (NomadCrashAlertArgs) Kind() string                 { return "nomad_crash_alert" }
func (NomadCrashAlertArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

// NomadCrashAlertWorker processes unmanaged Nomad job crash alerts from River.
type NomadCrashAlertWorker struct {
	river.WorkerDefaults[NomadCrashAlertArgs]
	server *RouterServer
}

func (w *NomadCrashAlertWorker) Work(ctx context.Context, job *river.Job[NomadCrashAlertArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	var jobID int64
	if job != nil && job.JobRow != nil {
		jobID = job.ID
	}
	log.Printf("[webhooks-router] [river] processing nomad crash alert job id=%d job_id=%s alloc_id=%s", jobID, job.Args.JobID, job.Args.AllocID)
	if err := w.server.ProcessCrashAlert(ctx, job.Args); err != nil {
		log.Printf("[webhooks-router] [river] error processing nomad crash alert job id=%d: %v", jobID, err)
		return err
	}
	return nil
}

// VanadisExecuteChunkArgs defines the River job payload for executing a single Vanadis build chunk.
type VanadisExecuteChunkArgs struct {
	BuildID    string `json:"build_id" river:"unique"`
	ChunkIndex int    `json:"chunk_index" river:"unique"`
}

func (VanadisExecuteChunkArgs) Kind() string { return "vanadis_execute_chunk" }

func (VanadisExecuteChunkArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       VanadisQueueName,
		MaxAttempts: 3,
		UniqueOpts:  river.UniqueOpts{ByArgs: true, ByPeriod: 15 * time.Minute},
	}
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
	return strings.TrimPrefix(strings.TrimSpace(repo), "azylman/")
}

// formatDirectMessagePrefix formats the bold PR and repo prefix for direct Discord messages.
func formatDirectMessagePrefix(prNumber int, repo string) string {
	repoName := cleanRepoName(repo)
	switch {
	case prNumber > 0 && repoName != "":
		return fmt.Sprintf("**(PR: #%d, repo: %s)** ", prNumber, repoName)
	case repoName != "":
		return fmt.Sprintf("**(repo: %s)** ", repoName)
	case prNumber > 0:
		return fmt.Sprintf("**(PR: #%d)** ", prNumber)
	default:
		return ""
	}
}

func dedupeTrimmedStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	var out []string
	for _, item := range items {
		if clean := strings.TrimSpace(item); clean != "" {
			if _, exists := seen[clean]; !exists {
				seen[clean] = struct{}{}
				out = append(out, clean)
			}
		}
	}
	return out
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

// VanadisChunkSummary holds condensed status metadata for a single chunk in a Vanadis build.
type VanadisChunkSummary struct {
	ID         string    `json:"id"`
	ChunkIndex int       `json:"chunk_index"`
	Repo       string    `json:"repo,omitempty"`
	Title      string    `json:"title"`
	Status     string    `json:"status"`
	PRNumber   int       `json:"pr_number,omitempty"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
}

// VanadisAdvancementResult describes the outcome of advancing a Vanadis build chunk upon PR merge.
type VanadisAdvancementResult struct {
	Matched          bool                  `json:"matched"`
	Advanced         bool                  `json:"advanced"`
	BuildID          string                `json:"build_id"`
	ThreadID         string                `json:"thread_id"`
	Tier             int                   `json:"tier"`
	Title            string                `json:"title"`
	Objective        string                `json:"objective"`
	BuildStatus      string                `json:"build_status"`
	StatusMessageID  string                `json:"status_message_id,omitempty"`
	MergedChunkIndex int                   `json:"merged_chunk_index"`
	MergedChunkTitle string                `json:"merged_chunk_title"`
	NextChunkIndex   int                   `json:"next_chunk_index"`
	BuildCompleted   bool                  `json:"build_completed"`
	Chunks           []VanadisChunkSummary `json:"chunks,omitempty"`
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
	AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error)
	ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error)
	FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error)
	IncrementCIFailCount(ctx context.Context, repo string, prNumber int) (int, error)
}

// ChannelMessageContext holds historical conversation snippets for prompt enrichment.
// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task.
type StatusWidgetChunk struct {
	Index         int       `json:"index"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	PRNumber      int       `json:"pr_number,omitempty"`
	DeployingJobs []string  `json:"deploying_jobs,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
}

type ChannelMessageContext struct {
	AuthorName   string
	Content      string
	ResponseText string
}

type pgxQueryExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PostgresPRRegistry implements PRRegistryUpdater backed by a pgxpool.Pool.
type PostgresPRRegistry struct {
	pool pgxQueryExecer
}

// NewPostgresPRRegistry constructs a new PostgresPRRegistry.
func NewPostgresPRRegistry(pool pgxQueryExecer) *PostgresPRRegistry {
	if typedPool, ok := pool.(*pgxpool.Pool); ok && typedPool == nil {
		pool = nil
	}
	return &PostgresPRRegistry{pool: pool}
}

func (r *PostgresPRRegistry) queryRowScan(ctx context.Context, opName, query string, dest []any, args ...any) (bool, error) {
	if r == nil || r.pool == nil {
		return false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.pool.QueryRow(qCtx, query, args...).Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("%s: %w", opName, err)
	}
	return true, nil
}

func (r *PostgresPRRegistry) execReturningTargetID(ctx context.Context, repo string, prNumber int, opName, query string, extraArgs ...any) (string, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return "", nil
	}
	args := append([]any{repo, prNumber}, extraArgs...)
	var targetID string
	found, err := r.queryRowScan(ctx, opName, query, []any{&targetID}, args...)
	if err != nil {
		return "", err
	}
	if !found {
		log.Printf("[webhooks-router] [registry] pr not found in registry (untracked): repo=%s pr=%d", repo, prNumber)
		return "", nil
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) execAtomicTransition(ctx context.Context, repo string, prNumber int, opName, query string) (string, bool, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return "", false, nil
	}
	var targetID string
	found, err := r.queryRowScan(ctx, opName, query, []any{&targetID}, repo, prNumber)
	if err != nil {
		return "", false, err
	}
	return targetID, found, nil
}

func (r *PostgresPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	query := `UPDATE pr_registry SET status = CASE WHEN status IN ('deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END, merge_sha = CASE WHEN $3 != '' THEN $3 ELSE merge_sha END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr merged", query, strings.TrimSpace(mergeSHA))
}

func (r *PostgresPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	query := `UPDATE pr_registry SET status = 'closed', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr closed unmerged", query)
}

func (r *PostgresPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	query := `UPDATE pr_registry SET head_sha = $3, status = 'open', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('merged', 'closed', 'deploying', 'deployed', 'deploy_failed') RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr sync", query, strings.TrimSpace(headSHA))
}

func (r *PostgresPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	query := `UPDATE pr_registry SET merge_sha = $3, status = CASE WHEN status IN ('deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "backfill push merge_sha", query, strings.TrimSpace(mergeSHA))
}

func (r *PostgresPRRegistry) AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	query := `UPDATE pr_registry SET status = 'ci_failed', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'closed', 'deploying', 'deployed', 'deploy_failed') RETURNING target_id;`
	return r.execAtomicTransition(ctx, repo, prNumber, "atomic transition ci_failed", query)
}

func (r *PostgresPRRegistry) AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	query := `UPDATE pr_registry SET status = 'conflict', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('conflict', 'merged', 'closed', 'deploying', 'deployed', 'deploy_failed') RETURNING target_id;`
	return r.execAtomicTransition(ctx, repo, prNumber, "atomic transition conflict", query)
}

func (r *PostgresPRRegistry) AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	query := `UPDATE pr_registry SET status = 'deploy_failed', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND status = 'deploying' RETURNING target_id;`
	return r.execAtomicTransition(ctx, repo, prNumber, "atomic transition deploy_failed", query)
}

func (r *PostgresPRRegistry) IncrementCIFailCount(ctx context.Context, repo string, prNumber int) (int, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return 0, nil
	}
	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{ci_fail_count}', to_jsonb(COALESCE((metadata->>'ci_fail_count')::int, 0) + 1), true),
		    updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2
		RETURNING (metadata->>'ci_fail_count')::int;`
	var count int
	found, err := r.queryRowScan(ctx, "increment ci_fail_count", query, []any{&count}, repo, prNumber)
	if err != nil || !found {
		return 0, err
	}
	return count, nil
}

func (r *PostgresPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	repo = normalizeRepo(repo)
	headSHA = strings.TrimSpace(headSHA)
	if repo == "" || headSHA == "" || isZeroSHA(headSHA) {
		return "", 0, false, nil
	}

	query := `
		UPDATE pr_registry SET status = 'ci_failed', updated_at = CURRENT_TIMESTAMP
		WHERE id = (SELECT id FROM pr_registry WHERE repo = $1 AND head_sha = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'closed', 'deploying', 'deployed', 'deploy_failed') ORDER BY updated_at DESC LIMIT 1)
		RETURNING target_id, pr_number;`
	var targetID string
	var prNumber int
	found, err := r.queryRowScan(ctx, "atomic transition ci_failed by head_sha", query, []any{&targetID, &prNumber}, repo, headSHA)
	if err != nil {
		return "", 0, false, err
	}
	return targetID, prNumber, found, nil
}

func (r *PostgresPRRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	repo = normalizeRepo(repo)
	sha = strings.TrimSpace(sha)
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", "", nil
	}

	query := `SELECT pr_number, branch, target_id FROM pr_registry WHERE repo = $1 AND (merge_sha = $2 OR head_sha = $2) ORDER BY updated_at DESC LIMIT 1;`
	var prNum int
	var branch, targetID string
	found, err := r.queryRowScan(ctx, "resolve pr by sha", query, []any{&prNum, &branch, &targetID}, repo, sha)
	if err != nil {
		return 0, "", "", err
	}
	if !found {
		return 0, "", "", nil
	}
	return prNum, branch, targetID, nil
}

func (r *PostgresPRRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", "", "", "", nil
	}

	query := `SELECT target_id, branch, head_sha, COALESCE(merge_sha, '') FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`
	var targetID, branch, headSHA, mergeSHA string
	found, err := r.queryRowScan(ctx, "resolve pr by number", query, []any{&targetID, &branch, &headSHA, &mergeSHA}, repo, prNumber)
	if err != nil {
		return "", "", "", "", err
	}
	if !found {
		return "", "", "", "", nil
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

	query := `SELECT pr_number, branch, target_id, head_sha FROM pr_registry WHERE repo = $1 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('merged', 'closed', 'conflict', 'deploying', 'deployed', 'deploy_failed') ORDER BY pr_number ASC;`
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

func (r *PostgresPRRegistry) TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (string, error) {
	cleanJobs := dedupeTrimmedStrings(jobs)
	if len(cleanJobs) == 0 {
		return "", nil
	}
	jobsJSON, err := json.Marshal(cleanJobs)
	if err != nil {
		jobsJSON = []byte("[]")
	}
	query := `
		UPDATE pr_registry
		SET status = 'deploying',
			metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{jobs}', (SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM jsonb_array_elements(COALESCE(metadata->'jobs', '[]'::jsonb) || $3::jsonb) AS elem)),
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2 AND status NOT IN ('closed', 'conflict', 'deployed')
		RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "transition deploying", query, jobsJSON)
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
		WHERE status IN ('deploying', 'merged') AND (metadata->'jobs' ? $1 OR metadata->>'job' = $1)
		ORDER BY updated_at DESC LIMIT 1 FOR UPDATE`

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

	var remaining []string
	for _, j := range extractJobsFromMetadata(meta) {
		if !strings.EqualFold(j, jobName) {
			remaining = append(remaining, j)
		}
	}

	if len(remaining) > 0 {
		remainingJSON, err := json.Marshal(remaining)
		if err != nil {
			remainingJSON = []byte("[]")
		}
		updateQuery := `UPDATE pr_registry SET status = 'deploying', metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{jobs}', $1::jsonb), updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status IN ('deploying', 'merged')`
		if _, err := tx.Exec(qCtx, updateQuery, remainingJSON, id); err != nil {
			return "", 0, "", "", false, fmt.Errorf("update remaining jobs: %w", err)
		}
		if err := tx.Commit(qCtx); err != nil {
			return "", 0, "", "", false, fmt.Errorf("commit remaining jobs tx: %w", err)
		}
		return "", 0, "", "", false, nil
	}

	updateQuery := `UPDATE pr_registry SET status = 'deployed', metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{jobs}', '[]'::jsonb), updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status IN ('deploying', 'merged')`
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

func (r *PostgresPRRegistry) AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return "", 0, "", "", false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	selectQuery := `SELECT id, target_id, pr_number, COALESCE(merge_sha, ''), repo FROM pr_registry WHERE status IN ('deploying', 'merged') AND (metadata->'jobs' ? $1 OR metadata->>'job' = $1) ORDER BY updated_at DESC LIMIT 1;`
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

	updateQuery := `UPDATE pr_registry SET status = 'deploy_failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status IN ('deploying', 'merged');`
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
		WHERE status = 'deploying' OR (status = 'merged' AND (
			CASE WHEN jsonb_typeof(metadata->'jobs') = 'array' THEN jsonb_array_length(metadata->'jobs') ELSE 0 END > 0
			OR COALESCE(metadata->>'job', '') != ''
		))
		ORDER BY id ASC;`
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

	query := `SELECT author_name, content, COALESCE(response_text, '') FROM (SELECT author_name, content, response_text, created_at FROM messages WHERE thread_id = $1 ORDER BY created_at DESC LIMIT $2) sub ORDER BY created_at ASC;`
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

func queryVanadisChunks(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, buildID string, forUpdate bool) ([]VanadisChunkSummary, error) {
	query := `SELECT id, chunk_index, repo, title, status, COALESCE(pr_number, 0), started_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC`
	if forUpdate {
		query += " FOR UPDATE"
	}
	rows, err := q.Query(ctx, query, buildID)
	if err != nil {
		return nil, fmt.Errorf("query vanadis build chunks: %w", err)
	}
	defer rows.Close()

	var chunks []VanadisChunkSummary
	for rows.Next() {
		var cs VanadisChunkSummary
		var startedAt sql.NullTime
		if scanErr := rows.Scan(&cs.ID, &cs.ChunkIndex, &cs.Repo, &cs.Title, &cs.Status, &cs.PRNumber, &startedAt, &cs.UpdatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan vanadis build chunk: %w", scanErr)
		}
		if startedAt.Valid {
			cs.StartedAt = startedAt.Time
		}
		chunks = append(chunks, cs)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate vanadis build chunks: %w", err)
	}
	return chunks, nil
}

func (r *PostgresPRRegistry) AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return &VanadisAdvancementResult{}, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := r.pool.Begin(qCtx)
	if err != nil {
		return nil, fmt.Errorf("begin vanadis advance tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(qCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[webhooks-router] [registry] vanadis tx rollback error: %v", rbErr)
		}
	}()

	selectChunkQuery := `
		SELECT c.id, c.build_id, c.chunk_index, c.title, c.status, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, b.status, b.status_message_id
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.repo = $1 AND c.pr_number = $2
		ORDER BY c.updated_at DESC LIMIT 1 FOR UPDATE`

	var chunkID, buildID, cTitle, cStatus, threadID, bTitle, objective, bStatus, bStatusMsgID string
	var chunkIdx, tier int
	err = tx.QueryRow(qCtx, selectChunkQuery, repo, prNumber).Scan(
		&chunkID, &buildID, &chunkIdx, &cTitle, &cStatus, &threadID, &tier, &bTitle, &objective, &bStatus, &bStatusMsgID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return &VanadisAdvancementResult{Matched: false}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select vanadis chunk by pr: %w", err)
	}

	if bStatus == "cancelled" || bStatus == "failed" || cStatus == "merged" {
		return &VanadisAdvancementResult{
			Matched:          true,
			Advanced:         false,
			BuildID:          buildID,
			ThreadID:         threadID,
			Tier:             tier,
			Title:            bTitle,
			Objective:        objective,
			BuildStatus:      bStatus,
			StatusMessageID:  bStatusMsgID,
			MergedChunkIndex: chunkIdx,
			MergedChunkTitle: cTitle,
			NextChunkIndex:   -1,
		}, nil
	}

	updateChunkQuery := `UPDATE vanadis_chunks SET status = 'merged', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	if _, err := tx.Exec(qCtx, updateChunkQuery, chunkID); err != nil {
		return nil, fmt.Errorf("update vanadis chunk merged: %w", err)
	}

	allChunks, err := queryVanadisChunks(qCtx, tx, buildID, true)
	if err != nil {
		return nil, err
	}
	for i := range allChunks {
		if allChunks[i].ID == chunkID {
			allChunks[i].Status = "merged"
		}
	}

	nextChunkIdx := -1
	fallbackIdx := -1
	for _, cs := range allChunks {
		if cs.Status != "merged" {
			if cs.ChunkIndex > chunkIdx && (nextChunkIdx == -1 || cs.ChunkIndex < nextChunkIdx) {
				nextChunkIdx = cs.ChunkIndex
			} else if fallbackIdx == -1 {
				fallbackIdx = cs.ChunkIndex
			}
		}
	}
	if nextChunkIdx == -1 {
		nextChunkIdx = fallbackIdx
	}

	if nextChunkIdx >= 0 {
		if err := tx.Commit(qCtx); err != nil {
			return nil, fmt.Errorf("commit vanadis chunk advance tx: %w", err)
		}
		return &VanadisAdvancementResult{
			Matched:          true,
			Advanced:         true,
			BuildID:          buildID,
			ThreadID:         threadID,
			Tier:             tier,
			Title:            bTitle,
			Objective:        objective,
			BuildStatus:      bStatus,
			StatusMessageID:  bStatusMsgID,
			MergedChunkIndex: chunkIdx,
			MergedChunkTitle: cTitle,
			NextChunkIndex:   nextChunkIdx,
			BuildCompleted:   false,
			Chunks:           allChunks,
		}, nil
	}

	updateBuildQuery := `UPDATE vanadis_builds SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	if _, err := tx.Exec(qCtx, updateBuildQuery, buildID); err != nil {
		return nil, fmt.Errorf("update vanadis build completed: %w", err)
	}
	if err := tx.Commit(qCtx); err != nil {
		return nil, fmt.Errorf("commit vanadis build completed tx: %w", err)
	}
	return &VanadisAdvancementResult{
		Matched:          true,
		Advanced:         true,
		BuildID:          buildID,
		ThreadID:         threadID,
		Tier:             tier,
		Title:            bTitle,
		Objective:        objective,
		BuildStatus:      "completed",
		StatusMessageID:  bStatusMsgID,
		MergedChunkIndex: chunkIdx,
		MergedChunkTitle: cTitle,
		NextChunkIndex:   -1,
		BuildCompleted:   true,
		Chunks:           allChunks,
	}, nil
}

func (r *PostgresPRRegistry) ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return false, nil
	}
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM vanadis_chunks c
			JOIN vanadis_builds b ON c.build_id = b.id
			WHERE c.repo = $1 AND c.pr_number = $2
			  AND (
				  b.status = 'running'
				  OR EXISTS (
					  SELECT 1 FROM vanadis_chunks c2
					  WHERE c2.build_id = c.build_id AND c2.chunk_index > c.chunk_index
				  )
			  )
		);`
	var exists bool
	found, err := r.queryRowScan(ctx, "check vanadis deploy wake suppression", query, []any{&exists}, repo, prNumber)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	return exists, nil
}

func (r *PostgresPRRegistry) FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return false, nil
	}
	query := `
		UPDATE vanadis_chunks c
		SET status = 'failed', updated_at = CURRENT_TIMESTAMP
		FROM vanadis_builds b
		WHERE c.build_id = b.id AND c.repo = $1 AND c.pr_number = $2 AND c.status NOT IN ('merged', 'failed')
		RETURNING c.build_id;`
	var buildID string
	found, err := r.queryRowScan(ctx, "fail vanadis chunk by pr", query, []any{&buildID}, repo, prNumber)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	updateBuild := `UPDATE vanadis_builds SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status = 'running';`
	if _, err := r.pool.Exec(qCtx, updateBuild, buildID); err != nil {
		return false, fmt.Errorf("fail vanadis build by pr: %w", err)
	}
	return true, nil
}

// Discord Snowflake validation
var snowflakeRegex = regexp.MustCompile(`^[0-9]{17,20}$`)

func IsValidDiscordSnowflake(id string) bool {
	return snowflakeRegex.MatchString(strings.TrimSpace(id))
}

var re4xx = regexp.MustCompile(`\bstatus 4\d{2}\b`)

// is4xxClientError returns true if the error indicates a 4xx HTTP client error or invalid Discord snowflake.
func is4xxClientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return re4xx.MatchString(msg) || strings.Contains(msg, "invalid discord snowflake")
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
	DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error)
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
		client = &http.Client{Timeout: 65 * time.Second}
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

func (d *DefaultOutboundDispatcher) postHangar(ctx context.Context, endpointPath, opName, targetDesc string, payload any) ([]byte, bool, error) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, false, fmt.Errorf("marshal %s request: %w", opName, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, d.hangarURL+endpointPath, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, false, fmt.Errorf("create %s request: %w", opName, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return nil, false, fmt.Errorf("%s request failed: %w", opName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr == nil && isHangarUnmanagedError(respBody) {
			log.Printf("[webhooks-router] [dispatcher] hangar confirmed %s (skipping): %s", targetDesc, string(respBody))
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("hangar %s returned 404: %s", opName, string(respBody))
	}
	respBody, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if readErr != nil {
			return nil, false, fmt.Errorf("hangar %s returned status %d (read error: %w)", opName, resp.StatusCode, readErr)
		}
		return nil, false, fmt.Errorf("hangar %s returned status %d: %s", opName, resp.StatusCode, string(respBody))
	}
	return respBody, false, nil
}

func (d *DefaultOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	_, _, err := d.postHangar(ctx, "/events/git_push", "git push", "repo "+req.Repo+" is not managed by hangar", req)
	return err
}

func (d *DefaultOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error) {
	respBody, skipped, err := d.postHangar(ctx, "/events/image_ready", "image ready", "image "+req.Image+" has no matching nomad jobs", req)
	if err != nil || skipped {
		return nil, err
	}
	var res ImageReadyEventResponse
	if err := json.Unmarshal(respBody, &res); err != nil {
		log.Printf("[webhooks-router] [dispatcher] warning decoding image ready response for %s: %v", req.Image, err)
		return nil, nil
	}
	return res.MatchedJobs, nil
}

func (d *DefaultOutboundDispatcher) postBrainWithRetry(ctx context.Context, endpointPath, reqLabel, statusLabel, channelID string, maxAttempts int, defaultDelay time.Duration, expBackoff bool, payload any) error {
	if !IsValidDiscordSnowflake(channelID) {
		return fmt.Errorf("invalid discord snowflake channel id: %q", channelID)
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", reqLabel, err)
	}
	delay := d.retryDelay
	if delay <= 0 {
		delay = defaultDelay
	}
	if maxAttempts <= 0 {
		maxAttempts = 6
	}
	endpoint := d.brainURL + endpointPath
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create %s request: %w", reqLabel, err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("%s request failed (attempt %d/%d): %w", reqLabel, attempt, maxAttempts, err)
		} else {
			respBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				respBody = []byte(fmt.Sprintf("error reading body: %v", readErr))
			}
			resp.Body.Close()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if resp.StatusCode < 500 {
				return fmt.Errorf("%s returned status %d: %s", statusLabel, resp.StatusCode, string(respBody))
			}
			lastErr = fmt.Errorf("%s returned status %d (attempt %d/%d): %s", statusLabel, resp.StatusCode, attempt, maxAttempts, string(respBody))
		}
		if attempt < maxAttempts {
			select {
			case <-time.After(delay):
				if expBackoff {
					if delay = time.Duration(float64(delay) * 1.5); delay > 5*time.Second {
						delay = 5 * time.Second
					}
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

func (d *DefaultOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	return d.postBrainWithRetry(ctx, "/prompt", "prompt", "brain prompt", req.ChannelID, d.maxAttempts, 1500*time.Millisecond, true, req)
}

func (d *DefaultOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	return d.postBrainWithRetry(ctx, "/discord/message", "direct message", "brain internal message", req.ChannelID, 3, 500*time.Millisecond, false, req)
}

func (d *DefaultOutboundDispatcher) DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error) {
	if !IsValidDiscordSnowflake(payload.ChannelID) {
		return "", fmt.Errorf("invalid discord snowflake channel id: %q", payload.ChannelID)
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal widget request: %w", err)
	}
	endpoint := d.brainURL + "/discord/widget"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create widget request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("dispatch widget request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read widget response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("brain widget returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var res struct {
		Status    string `json:"status"`
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(respBody, &res); err != nil {
		return "", fmt.Errorf("decode widget response: %w", err)
	}
	return res.MessageID, nil
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
func (s *RouterServer) SetConflictCheckDelay(d time.Duration)     { s.conflictCheckDelay = d }
func (s *RouterServer) SetRiverClient(rc RiverInserter)            { s.riverClient = rc }
func (s *RouterServer) SetRegistry(reg PRRegistryUpdater)          { s.registry = reg }
func (s *RouterServer) SetDispatcher(d OutboundDispatcher)         { s.dispatcher = d }

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[webhooks-router] JSON encode error: %v", err)
	}
}

func readWebhookBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return nil, false
	}
	return body, true
}

func readWebhookJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, allowEmpty bool, dst any) bool {
	body, ok := readWebhookBody(w, r, maxBytes)
	if !ok {
		return false
	}
	if len(body) == 0 && allowEmpty {
		return true
	}
	if err := json.Unmarshal(body, dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
		return false
	}
	return true
}

func runBackgroundSync(name string, fn func(ctx context.Context) error) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] PANIC recovered in %s: %v", name, r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			log.Printf("[webhooks-router] Background %s failed: %v", name, err)
		}
	}()
}

// Routes initializes and returns the HTTP mux for the service.
func (s *RouterServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /health", s.handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())
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

var (
	writeCountersDirFn = coverage.WriteCountersDir
	writeMetaDirFn     = coverage.WriteMetaDir
)

func (s *RouterServer) handleCoverageFlush(w http.ResponseWriter, r *http.Request) {
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		if err := writeMetaDirFn(dir); err != nil {
			log.Printf("[webhooks-router] coverage meta flush error: %v", err)
		}
		if err := writeCountersDirFn(dir); err != nil {
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
	var payload InfisicalWebhookPayload
	if !readWebhookJSON(w, r, 1<<20, true, &payload) {
		return
	}
	workspaceID := payload.GetWorkspaceID(s.cfg.InfisicalProjectID)
	envName := payload.GetEnvironment(s.cfg.InfisicalEnvironment)

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"message":        "Secret sync scheduled",
		"targetVariable": TargetNomadVariable,
		"environment":    envName,
	})
	runBackgroundSync("infisical webhook sync", func(ctx context.Context) error {
		return s.SyncSecrets(ctx, workspaceID, envName)
	})
}

type GenericWebhookPayload struct {
	WorkspaceID string            `json:"workspaceId"`
	Environment string            `json:"environment"`
	Secrets     map[string]string `json:"secrets"`
}

func (s *RouterServer) handleGenericWebhook(w http.ResponseWriter, r *http.Request) {
	var payload GenericWebhookPayload
	if !readWebhookJSON(w, r, 1<<20, true, &payload) {
		return
	}

	if len(payload.Secrets) > 0 {
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":         "accepted",
			"message":        "Direct secret sync scheduled",
			"targetVariable": TargetNomadVariable,
			"keyCount":       len(payload.Secrets),
		})
		runBackgroundSync("direct generic sync", func(ctx context.Context) error {
			return s.putNomadVariable(ctx, payload.Secrets)
		})
		return
	}

	workspaceID := firstNonEmpty(payload.WorkspaceID, s.cfg.InfisicalProjectID)
	envName := firstNonEmpty(payload.Environment, s.cfg.InfisicalEnvironment)

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":         "accepted",
		"message":        "Generic secret sync scheduled",
		"targetVariable": TargetNomadVariable,
		"environment":    envName,
	})
	runBackgroundSync("generic webhook sync", func(ctx context.Context) error {
		return s.SyncSecrets(ctx, workspaceID, envName)
	})
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
	var evt HangarDeployEvent
	if !readWebhookJSON(w, r, 1<<20, false, &evt) {
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
		idempotencyKey := randomNonce()
		if evt.CommitSHA != "" && evt.JobName != "" {
			idempotencyKey = fmt.Sprintf("%s:%s:%s:%s", evt.Event, evt.Repo, evt.JobName, evt.CommitSHA)
		}
		if err := s.insertRiverJob(r.Context(), HangarWebhookArgs{IdempotencyKey: idempotencyKey, Event: evt}); err != nil {
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

	if s.registry != nil {
		prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, repo, sha)
		if err != nil {
			log.Printf("[webhooks-router] [registry] failed resolving PR by SHA %s locally: %v", sha, err)
		} else if prNum > 0 {
			if targetID == "" {
				if tID, _, _, _, errNum := s.registry.ResolvePRByNumber(ctx, repo, prNum); errNum == nil && tID != "" {
					targetID = tID
				}
			}
			return prNum, targetID, branch, nil
		}
	}

	prNum, branch, err := s.resolvePRFromCommitSHA(ctx, repo, sha)
	if err != nil || prNum == 0 {
		return 0, "", "", err
	}

	var targetID string
	if s.registry != nil {
		if tID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNum); err == nil && tID != "" {
			targetID = tID
		}
		if backfillTID, err := s.registry.BackfillPushMergeSHA(ctx, repo, prNum, sha); err != nil {
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
	targetID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(repo), prNum)
	return targetID, err
}

func formatErrorDetailsBlock(header, details, extraLine, footer string) string {
	lines := []string{header}
	if clean := strings.TrimSpace(details); clean != "" {
		lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(clean)))
	}
	if cleanExtra := strings.TrimSpace(extraLine); cleanExtra != "" {
		lines = append(lines, cleanExtra)
	}
	if footer != "" {
		lines = append(lines, footer)
	}
	return strings.Join(lines, "\n")
}


func (s *RouterServer) SyncPRWidget(ctx context.Context, repo string, prNumber int, statusOverride string, banner string, deployingJobs []string) error {
	if s == nil || s.dispatcher == nil || repo == "" || prNumber <= 0 {
		return nil
	}
	repo = normalizeRepo(repo)
	qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		targetID, branch, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNumber)
		if err != nil {
			log.Printf("[webhooks-router] [widget] resolve pr error: %v", err)
		}
		if targetID == "" {
			return nil
		}
		widgetReq := StatusWidgetRequest{
			ChannelID: targetID,
			ID:        fmt.Sprintf("pr-%s-%d", repo, prNumber),
			Title:     branch,
			Status:    statusOverride,
			Banner:    banner,
			Chunks: []StatusWidgetChunk{
				{
					Index:         0,
					Repo:          repo,
					Title:         branch,
					Status:        statusOverride,
					PRNumber:      prNumber,
					DeployingJobs: deployingJobs,
				},
			},
		}
		_, err = s.dispatcher.DispatchWidget(ctx, widgetReq)
		return err
	}

	// 1. Check if (repo, prNumber) belongs to a vanadis_chunks row
	selectChunkQuery := `
		SELECT c.id, c.build_id, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, b.status, b.status_message_id
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.repo = $1 AND c.pr_number = $2
		ORDER BY c.updated_at DESC LIMIT 1`

	var cID, buildID, threadID, bTitle, objective, bStatus, bStatusMsgID string
	var tier int
	err := pgReg.pool.QueryRow(qCtx, selectChunkQuery, repo, prNumber).Scan(
		&cID, &buildID, &threadID, &tier, &bTitle, &objective, &bStatus, &bStatusMsgID,
	)
	if err == nil && buildID != "" {
		// Parent Vanadis build found
		allChunks, qErr := queryVanadisChunks(qCtx, pgReg.pool, buildID, false)
		if qErr != nil {
			return fmt.Errorf("sync widget query chunks: %w", qErr)
		}
		var wChunks []StatusWidgetChunk
		for _, cs := range allChunks {
			chunkRepo := cs.Repo
			if chunkRepo == "" {
				chunkRepo = "azylman/aerial"
			}
			chunkStatus := cs.Status
			csStartedAt := cs.StartedAt
			csUpdatedAt := cs.UpdatedAt
			var cJobs []string
			if cs.PRNumber == prNumber {
				if statusOverride != "" {
					if cs.Status != statusOverride {
						csUpdatedAt = time.Now().UTC()
						if csStartedAt.IsZero() {
							csStartedAt = csUpdatedAt
						}
					}
					chunkStatus = statusOverride
				}
				if len(deployingJobs) > 0 {
					cJobs = deployingJobs
				}
			}
			wChunks = append(wChunks, StatusWidgetChunk{
				Index:         cs.ChunkIndex,
				Repo:          chunkRepo,
				Title:         cs.Title,
				Status:        chunkStatus,
				PRNumber:      cs.PRNumber,
				DeployingJobs: cJobs,
				StartedAt:     csStartedAt,
				UpdatedAt:     csUpdatedAt,
			})
		}

		widgetStatus := bStatus
		if statusOverride == "completed" {
			widgetStatus = "completed"
		} else if statusOverride == "failed" {
			widgetStatus = "failed"
		}

		widgetReq := StatusWidgetRequest{
			ChannelID: threadID,
			MessageID: bStatusMsgID,
			ID:        buildID,
			Tier:      tier,
			Title:     firstNonEmpty(bTitle, objective),
			Status:    widgetStatus,
			Banner:    banner,
			Chunks:    wChunks,
		}

		newMsgID, dispErr := s.dispatcher.DispatchWidget(qCtx, widgetReq)
		if dispErr != nil {
			return fmt.Errorf("dispatch vanadis widget: %w", dispErr)
		}
		if newMsgID != "" && newMsgID != bStatusMsgID {
			updateMsgQuery := `UPDATE vanadis_builds SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`
			if _, execErr := pgReg.pool.Exec(qCtx, updateMsgQuery, newMsgID, buildID); execErr != nil {
				log.Printf("[webhooks-router] [widget] warning updating vanadis status_message_id: %v", execErr)
			}
		}
		return nil
	}

	// 2. Standalone PR in pr_registry
	prQuery := `SELECT target_id, title, status, status_message_id, created_at, updated_at FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1`
	var targetID, title, prStatus, prMsgID string
	var prCreatedAt, prUpdatedAt time.Time
	err = pgReg.pool.QueryRow(qCtx, prQuery, repo, prNumber).Scan(&targetID, &title, &prStatus, &prMsgID, &prCreatedAt, &prUpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) || targetID == "" {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sync pr widget query pr: %w", err)
	}

	finalStatus := prStatus
	if statusOverride != "" {
		if prStatus != statusOverride {
			prUpdatedAt = time.Now().UTC()
		}
		finalStatus = statusOverride
	}
	widgetTitle := title
	if widgetTitle == "" {
		widgetTitle = fmt.Sprintf("PR #%d", prNumber)
	}

	widgetReq := StatusWidgetRequest{
		ChannelID: targetID,
		MessageID: prMsgID,
		ID:        fmt.Sprintf("pr-%s-%d", repo, prNumber),
		Title:     widgetTitle,
		Status:    finalStatus,
		Banner:    banner,
		Chunks: []StatusWidgetChunk{
			{
				Index:         0,
				Repo:          repo,
				Title:         widgetTitle,
				Status:        finalStatus,
				PRNumber:      prNumber,
				DeployingJobs: deployingJobs,
				StartedAt:     prCreatedAt,
				UpdatedAt:     prUpdatedAt,
			},
		},
	}

	newMsgID, dispErr := s.dispatcher.DispatchWidget(qCtx, widgetReq)
	if dispErr != nil {
		return fmt.Errorf("dispatch pr widget: %w", dispErr)
	}
	if newMsgID != "" && newMsgID != prMsgID {
		updatePRMsgQuery := `UPDATE pr_registry SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE repo = $2 AND pr_number = $3`
		if _, execErr := pgReg.pool.Exec(qCtx, updatePRMsgQuery, newMsgID, repo, prNumber); execErr != nil {
			log.Printf("[webhooks-router] [widget] warning updating pr status_message_id: %v", execErr)
		}
	}
	return nil
}

func (s *RouterServer) dispatchAlertWithPrompt(ctx context.Context, targetID, label, dmMsg, prompt string) error {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return nil
	}
	if dmMsg != "" {
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: targetID, Content: dmMsg}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] warning dispatching %s direct message: %v", label, err)
		}
	}
	if err := s.dispatcher.DispatchPrompt(ctx, PromptRequest{ChannelID: targetID, Prompt: prompt}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching %s prompt: %v", label, err)
		return fmt.Errorf("dispatch %s prompt: %w", label, err)
	}
	return nil
}

func isHangarSyncedRepo(repo string) bool {
	norm := strings.ToLower(normalizeRepo(repo))
	return norm == "azylman/aerial" || norm == "azylman/aerial-config"
}

func formatVanadisBuildCompletedPrompt(res *VanadisAdvancementResult) string {
	if res == nil {
		return ""
	}
	total := len(res.Chunks)
	lines := []string{
		fmt.Sprintf("Vanadis build %s completed (Tier %d, %d chunks merged).", res.BuildID, res.Tier, total),
	}
	if obj := strings.TrimSpace(res.Objective); obj != "" {
		lines = append(lines, fmt.Sprintf("Objective: %s", obj))
	}
	if total > 0 {
		lines = append(lines, "", "Merged Chunks Ledger:")
		for _, c := range res.Chunks {
			cRepo := strings.TrimSpace(c.Repo)
			if cRepo == "" {
				cRepo = "azylman/aerial"
			}
			prLabel := fmt.Sprintf(" (%s)", cRepo)
			if c.PRNumber > 0 {
				prLabel = fmt.Sprintf(" (%s, PR #%d)", cRepo, c.PRNumber)
			}
			line := fmt.Sprintf("- Chunk %d/%d: %s%s", c.ChunkIndex+1, total, c.Title, prLabel)
			lines = append(lines, line)
		}
	}
	lines = append(lines, "", "All Vanadis build chunks have merged into main. Summarize the completed build and verification outcomes for the user.")
	return strings.Join(lines, "\n")
}

func (s *RouterServer) advanceVanadisChunk(ctx context.Context, repo string, prNumber int) error {
	if s.registry == nil || prNumber <= 0 {
		return nil
	}
	res, err := s.registry.AdvanceVanadisChunkByPR(ctx, repo, prNumber)
	if err != nil {
		return err
	}
	if res == nil || !res.Matched || !res.Advanced {
		return nil
	}
	if !res.BuildCompleted && res.NextChunkIndex >= 0 {
		if s.riverClient != nil {
			if err := s.insertRiverJob(ctx, VanadisExecuteChunkArgs{
				BuildID:    res.BuildID,
				ChunkIndex: res.NextChunkIndex,
			}); err != nil {
				return fmt.Errorf("enqueue next vanadis chunk %d for build %s: %w", res.NextChunkIndex, res.BuildID, err)
			}
		}
		if err := s.SyncPRWidget(ctx, repo, prNumber, "merged", "", nil); err != nil {
			log.Printf("[webhooks-router] [vanadis] warning syncing chunk advancement widget for build %s: %v", res.BuildID, err)
		}
		return nil
	}
	if res.BuildCompleted {
		if err := s.SyncPRWidget(ctx, repo, prNumber, "completed", "", nil); err != nil {
			log.Printf("[webhooks-router] [vanadis] warning syncing build completed widget for build %s: %v", res.BuildID, err)
		}
		prompt := formatVanadisBuildCompletedPrompt(res)
		return s.dispatchAlertWithPrompt(ctx, res.ThreadID, "vanadis_build_completed", "", prompt)
	}
	return nil
}

func (s *RouterServer) ProcessHangarEvent(ctx context.Context, evt HangarDeployEvent) (*HangarDeployEvent, error) {
	if evt.CommitSHA != "" && evt.PRNumber == 0 {
		if prNum, targetID, _, err := s.ResolveSHA(ctx, evt.Repo, evt.CommitSHA); err != nil {
			log.Printf("[webhooks-router] [hangar] failed resolving SHA %s: %v", evt.CommitSHA, err)
		} else if prNum > 0 {
			evt.PRNumber = prNum
			evt.TargetID = firstNonEmpty(evt.TargetID, targetID)
		}
	} else if evt.PRNumber > 0 && evt.TargetID == "" {
		if targetID, err := s.ResolveThreadForPR(ctx, evt.Repo, evt.PRNumber); err != nil {
			log.Printf("[webhooks-router] [hangar] failed resolving thread for PR %s#%d: %v", evt.Repo, evt.PRNumber, err)
		} else if targetID != "" {
			evt.TargetID = targetID
		}
	}

	log.Printf("[webhooks-router] [hangar] [deploy] event=%s job=%s repo=%s pr=%d commit=%s status=%s target_id=%s deployment_id=%s details=%q",
		evt.Event, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA, evt.Status, evt.TargetID, evt.DeploymentID, evt.Details)

	if s.registry != nil && evt.PRNumber > 0 && evt.Event == "deploy_started" && evt.JobName != "" {
		if _, err := s.registry.TransitionDeploying(ctx, evt.Repo, evt.PRNumber, []string{evt.JobName}); err != nil {
			log.Printf("[webhooks-router] [registry] warning transitioning pr %s#%d to deploying for job %s: %v", evt.Repo, evt.PRNumber, evt.JobName, err)
		}
	}

	if !IsValidDiscordSnowflake(evt.TargetID) || s.dispatcher == nil {
		if evt.Event == "sync_success" && evt.PRNumber > 0 {
			return &evt, s.advanceVanadisChunk(ctx, evt.Repo, evt.PRNumber)
		}
		return &evt, nil
	}

	prefix := formatDirectMessagePrefix(evt.PRNumber, evt.Repo)
	switch evt.Event {
	case "deploy_started":
		if evt.PRNumber > 0 {
			if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber, "deploying", "", []string{evt.JobName}); err != nil {
				log.Printf("[webhooks-router] [widget] warning syncing deploy_started widget: %v", err)
				return &evt, err
			}
		}
		return &evt, nil
	case "deploy_success":
		return &evt, s.dispatchDeploymentSuccessPrompt(ctx, evt.TargetID, evt.JobName, evt.Repo, evt.CommitSHA, evt.PRNumber, evt.DeploymentID)
	case "deploy_rollback", "deploy_failed":
		targetID := evt.TargetID
		if s.registry != nil && evt.PRNumber > 0 {
			tID, updated, err := s.registry.AtomicTransitionDeployFailed(ctx, evt.Repo, evt.PRNumber)
			if err != nil || !updated {
				return &evt, err
			}
			targetID = firstNonEmpty(tID, targetID)
		}
		actionDesc, fixDesc := "Deployment failed", "deployment failure"
		if evt.Event == "deploy_rollback" {
			actionDesc, fixDesc = "Deployment failed and rolled back", "deployment rollback failure"
		}
		dmMsg := fmt.Sprintf("💥 %s%s for %s. Following up...", prefix, actionDesc, evt.JobName)
		banner := fmt.Sprintf("Deployment failed for %s", evt.JobName)
		if evt.Event == "deploy_rollback" {
			banner = fmt.Sprintf("Deployment failed and rolled back for %s", evt.JobName)
		}
		if evt.PRNumber > 0 {
			if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber, "failed", banner, nil); err == nil {
				dmMsg = ""
			}
		}
		prompt := formatErrorDetailsBlock(
			fmt.Sprintf("%s for job %s on %s (PR #%d, commit: %s).", actionDesc, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA),
			evt.Details, "", fmt.Sprintf("Please investigate and fix the %s.", fixDesc),
		)
		return &evt, s.dispatchAlertWithPrompt(ctx, targetID, evt.Event, dmMsg, prompt)
	case "sync_success":
		if evt.PRNumber > 0 {
			if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber, "merged", "", nil); err != nil {
				log.Printf("[webhooks-router] [widget] warning syncing sync_success widget: %v", err)
			}
			if err := s.advanceVanadisChunk(ctx, evt.Repo, evt.PRNumber); err != nil {
				return &evt, err
			}
		}
		return &evt, nil
	case "sync_failed":
		header := fmt.Sprintf("Git sync failed for repo %s (commit: %s, PR #%d).", evt.Repo, evt.CommitSHA, evt.PRNumber)
		if evt.PRNumber == 0 {
			header = fmt.Sprintf("Git sync failed for repo %s (commit: %s).", evt.Repo, evt.CommitSHA)
		}
		dmMsg := fmt.Sprintf("⚠️ %sGit file sync failed. Following up...", prefix)
		if evt.PRNumber > 0 {
			if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber, "failed", "Git file sync failed", nil); err == nil {
				dmMsg = ""
			}
		}
		prompt := formatErrorDetailsBlock(header, evt.Details, "", "Please investigate and fix the git sync failure.")
		return &evt, s.dispatchAlertWithPrompt(ctx, evt.TargetID, "sync_failed", dmMsg, prompt)
	}
	return &evt, nil
}

const maxPromptRunes = 1500
const maxCIRemediationAttempts = 3
const maxRoboRevCommentRunes = 4000

func truncatePromptDetails(s string) string {
	return truncatePromptRunes(strings.ReplaceAll(s, "```", "'''"), maxPromptRunes)
}

func truncatePromptRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "... (truncated)"
}

func sanitizeChannelContextText(s string) string {
	r := strings.NewReplacer("\r", "", "\n", " ", "</CHANNEL_CONTEXT>", "[CHANNEL_CONTEXT]", "<CHANNEL_CONTEXT>", "[CHANNEL_CONTEXT]")
	return strings.TrimSpace(r.Replace(s))
}

func formatChannelContext(msgs []ChannelMessageContext) string {
	if len(msgs) == 0 {
		return "No recent channel messages found."
	}

	var bullets []string
	for _, m := range msgs {
		author := firstNonEmpty(strings.TrimSpace(m.AuthorName), "User")
		if cleanContent := sanitizeChannelContextText(m.Content); cleanContent != "" {
			bullets = append(bullets, fmt.Sprintf("- [%s]: %s", author, truncatePromptRunes(cleanContent, 300)))
		}
		if cleanResp := sanitizeChannelContextText(m.ResponseText); cleanResp != "" {
			bullets = append(bullets, fmt.Sprintf("- [Aerial]: %s", truncatePromptRunes(cleanResp, 300)))
		}
	}

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

	for i, j := 0, len(selectedBullets)-1; i < j; i, j = i+1, j-1 {
		selectedBullets[i], selectedBullets[j] = selectedBullets[j], selectedBullets[i]
	}

	lines := []string{"Recent Discord channel context:", "<CHANNEL_CONTEXT>"}
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

	if s.registry != nil && prNum > 0 {
		suppressed, err := s.registry.ShouldSuppressVanadisDeployWake(ctx, repo, prNum)
		if err != nil {
			log.Printf("[webhooks-router] [vanadis] warning checking deploy wake suppression for %s#%d: %v", repo, prNum, err)
		} else if suppressed {
			log.Printf("[webhooks-router] [vanadis] suppressing mid-run deploy_success /prompt wake for %s#%d (job=%s)", repo, prNum, jobName)
			if err := s.SyncPRWidget(ctx, repo, prNum, "deployed", "", nil); err != nil {
				log.Printf("[webhooks-router] [widget] warning syncing suppressed deploy_success widget: %v", err)
			}
			return nil
		}
	}

	var contextMsgs []ChannelMessageContext
	if s.registry != nil {
		if msgs, err := s.registry.GetRecentChannelContext(ctx, targetID, 20); err != nil {
			log.Printf("[webhooks-router] [registry] warning fetching channel context for %s: %v", targetID, err)
		} else {
			contextMsgs = msgs
		}
	}

	repoPart := ""
	if repo != "" {
		repoPart = " on " + repo
	}
	refPart := ""
	if refID != "" {
		refPart = ", ref: " + refID
	}
	header := fmt.Sprintf("Continuous Delivery deployment completed for job %s%s (PR #%d, commit: %s%s).", jobName, repoPart, prNum, commitSHA, refPart)
	lines := []string{header}
	if ctxStr := formatChannelContext(contextMsgs); ctxStr != "" {
		lines = append(lines, "", ctxStr)
	}
	lines = append(lines, "", "Directive: Based on the recent conversation context above, suggest logical next steps if any work or follow-ups remain; otherwise, celebrate completion.")
	return s.dispatchAlertWithPrompt(ctx, targetID, "deploy_success", "", strings.Join(lines, "\n"))
}

func (s *RouterServer) dispatchCIFailurePrompt(ctx context.Context, targetID, checkName, repo, headSHA string, prNum int, details, checkURL string) error {
	dmMsg := fmt.Sprintf("❌ %sCI check %q failed. Following up...", formatDirectMessagePrefix(prNum, repo), checkName)
	if prNum > 0 {
		banner := fmt.Sprintf("CI check %q failed", checkName)
		if s.registry != nil {
			if _, isPg := s.registry.(*PostgresPRRegistry); isPg {
				if err := s.SyncPRWidget(ctx, repo, prNum, "failed", banner, nil); err == nil {
					dmMsg = ""
				}
			}
		}
	}
	var urlLine string
	if cleanURL := strings.TrimSpace(checkURL); cleanURL != "" {
		urlLine = "Check URL: " + cleanURL
	}
	prompt := formatErrorDetailsBlock(
		fmt.Sprintf("CI check %q failed for PR #%d on %s (head commit: %s).", checkName, prNum, repo, headSHA),
		details, urlLine,
		"Please investigate and fix the failure by inspecting the failing logs, diagnosing the issue, verifying with local checks, and pushing a fix.",
	)
	return s.dispatchAlertWithPrompt(ctx, targetID, "ci_failed", dmMsg, prompt)
}

func (s *RouterServer) dispatchConflictPrompt(ctx context.Context, targetID, repo, branch, headSHA string, prNum int) error {
	dmMsg := fmt.Sprintf("⛔ %sMerge conflict detected. Following up...", formatDirectMessagePrefix(prNum, repo))
	if prNum > 0 {
		if s.registry != nil {
			if _, isPg := s.registry.(*PostgresPRRegistry); isPg {
				if err := s.SyncPRWidget(ctx, repo, prNum, "conflict", "Merge conflict detected", nil); err == nil {
					dmMsg = ""
				}
			}
		}
	}
	prompt := fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please checkout the branch, merge origin/main to resolve conflicts, verify locally, and push to update the PR.", prNum, repo, branch, headSHA)
	return s.dispatchAlertWithPrompt(ctx, targetID, "conflict", dmMsg, prompt)
}

// GitHubIssueComment represents a comment on a GitHub issue or pull request.
type GitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

// GitHubPRDetails encapsulates mergeability information returned by GitHub's Pull Request API.
type GitHubPRDetails struct {
	Number         int    `json:"number"`
	Mergeable      *bool  `json:"mergeable"`
	MergeableState string `json:"mergeable_state"`
}

func (s *RouterServer) getGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	apiURL := firstNonEmpty(s.cfg.GitHubAPIURL, "https://api.github.com")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+path, nil)
	if err != nil {
		return fmt.Errorf("create github api request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if s.cfg.GitHubToken != "" {
		req.Header.Set("Authorization", "token "+s.cfg.GitHubToken)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("github api request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-RateLimit-Remaining")
		return fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("github api returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return fmt.Errorf("read github api response: %w", err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal github api response: %w", err)
	}
	return nil
}

func (s *RouterServer) fetchPRDetails(ctx context.Context, repo string, prNumber int) (*GitHubPRDetails, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return nil, fmt.Errorf("invalid repo or prNumber for pr details")
	}
	var details GitHubPRDetails
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/pulls/%d", repo, prNumber), 1<<20, &details); err != nil {
		return nil, err
	}
	return &details, nil
}

func (s *RouterServer) fetchLatestRoboRevComment(ctx context.Context, repo string, prNumber int, headSHA string) string {
	repo = normalizeRepo(repo)
	if s == nil || repo == "" || prNumber <= 0 {
		return ""
	}
	var comments []GitHubIssueComment
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repo, prNumber)
	if err := s.getGitHubJSON(ctx, path, 2<<20, &comments); err != nil {
		log.Printf("[webhooks-router] [github] warning fetching roborev comments for %s#%d: %v", repo, prNumber, err)
		return ""
	}
	shortSHA := strings.TrimSpace(headSHA)
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	for i := len(comments) - 1; i >= 0; i-- {
		body := strings.TrimSpace(comments[i].Body)
		if !strings.Contains(body, "## roborev:") {
			continue
		}
		if shortSHA != "" && !strings.Contains(body, "`"+shortSHA) && !strings.Contains(body, headSHA) {
			continue
		}
		return truncatePromptRunes(body, maxRoboRevCommentRunes)
	}
	return ""
}

func (s *RouterServer) checkOpenPRConflicts(ctx context.Context, repo string) error {
	if s.registry == nil {
		return nil
	}
	openPRs, err := s.registry.ListOpenPRs(ctx, repo)
	if err != nil {
		return fmt.Errorf("list open prs: %w", err)
	}
	for _, pr := range openPRs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var details *GitHubPRDetails
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
		if (details.Mergeable != nil && !*details.Mergeable) || details.MergeableState == "dirty" {
			targetID, updated, trErr := s.registry.AtomicTransitionConflict(ctx, repo, pr.PRNumber)
			if trErr != nil {
				log.Printf("[webhooks-router] [registry] error transitioning PR #%d to conflict: %v", pr.PRNumber, trErr)
				continue
			}
			if updated {
				targetID = firstNonEmpty(targetID, pr.TargetID)
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
	return nil
}

func (s *RouterServer) dispatchWorkflowRunImages(ctx context.Context, repo string, runID int64, headBranch, conclusion, headSHA, targetID string, prNum int) error {
	if s.dispatcher == nil || conclusion != "success" || !(headBranch == "main" || strings.HasPrefix(headBranch, "refs/heads/main")) {
		return nil
	}
	images, err := s.resolveWorkflowRunImages(ctx, repo, runID)
	if err != nil {
		log.Printf("[webhooks-router] [dispatcher] error resolving images for workflow run %d (%s): %v", runID, repo, err)
		return fmt.Errorf("resolve workflow run images: %w", err)
	}
	if len(images) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var rawJobs []string
	errCh := make(chan error, len(images))

	for _, img := range images {
		wg.Add(1)
		go func(imageRef string) {
			defer wg.Done()
			matchedJobs, err := s.dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{
				Image:     imageRef,
				Repo:      repo,
				CommitSHA: headSHA,
				PRNumber:  prNum,
				TargetID:  targetID,
			})
			if err != nil {
				log.Printf("[webhooks-router] [dispatcher] error dispatching image_ready to hangar for %s: %v", imageRef, err)
				errCh <- err
				return
			}
			if len(matchedJobs) > 0 {
				mu.Lock()
				rawJobs = append(rawJobs, matchedJobs...)
				mu.Unlock()
			}
		}(img)
	}
	wg.Wait()
	close(errCh)

	if allMatchedJobs := dedupeTrimmedStrings(rawJobs); len(allMatchedJobs) > 0 && s.registry != nil && prNum > 0 {
		if _, err := s.registry.TransitionDeploying(ctx, repo, prNum, allMatchedJobs); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error transitioning pr %s#%d to deploying: %v", repo, prNum, err)
			return fmt.Errorf("transition pr deploying: %w", err)
		}
	}

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
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
	return strings.Contains(msg, "not managed by hangar") || strings.Contains(msg, "no nomad jobs found")
}

// hasExecutedBuildStep verifies that if steps are present and a build/push step is defined,
// that step actually concluded with success rather than being skipped.
func hasExecutedBuildStep(steps []GitHubWorkflowJobStep) bool {
	hasPushStep := false
	for _, step := range steps {
		nameLower := strings.ToLower(step.Name)
		isPush := (strings.Contains(nameLower, "build") && (strings.Contains(nameLower, "push") || strings.Contains(nameLower, "publish"))) ||
			strings.Contains(nameLower, "docker push") || strings.Contains(nameLower, "push image") || strings.Contains(nameLower, "push docker")
		if isPush {
			hasPushStep = true
			if step.Conclusion == "success" {
				return true
			}
		}
	}
	return !hasPushStep
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
	return strings.Trim(sha, "0") == ""
}

func (s *RouterServer) handleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	event := r.Header.Get("X-GitHub-Event")
	if strings.TrimSpace(event) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-GitHub-Event header"})
		return
	}
	delivery := r.Header.Get("X-GitHub-Delivery")

	body, ok := readWebhookBody(w, r, 5<<20)
	if !ok {
		return
	}
	if s.riverClient == nil {
		log.Printf("[webhooks-router] [github] ERROR: river client not initialized")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "river queue not initialized"})
		return
	}
	if err := s.insertRiverJob(r.Context(), GitHubWebhookArgs{Event: event, Delivery: delivery, Body: body}); err != nil {
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

func (s *RouterServer) resolveEventPR(ctx context.Context, res *ResolvedGitHubEvent, prs []GitHubCheckRunPR, payloadSource string) {
	if len(prs) > 0 {
		res.PRNumber = prs[0].Number
		if res.Branch == "" {
			res.Branch = prs[0].Head.Ref
		}
		res.ResolutionSource = payloadSource
		if s.registry != nil && res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
			if _, _, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err == nil && targetID != "" {
				res.TargetID = targetID
			}
		}
		return
	}
	if res.HeadSHA == "" || isZeroSHA(res.HeadSHA) {
		return
	}
	if s.registry != nil {
		if prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err != nil {
			log.Printf("[webhooks-router] [registry] %s failed resolving PR by SHA %s locally: %v", res.Event, res.HeadSHA, err)
		} else if prNum > 0 {
			res.PRNumber = prNum
			if res.Branch == "" || strings.HasPrefix(res.Branch, "refs/heads/") {
				res.Branch = firstNonEmpty(branch, res.Branch)
			}
			res.TargetID = targetID
			res.ResolutionSource = "local_pr_registry"
			return
		}
	}
	if prNum, branch, err := s.resolvePRFromCommitSHA(ctx, res.Repo, res.HeadSHA); err != nil {
		log.Printf("[webhooks-router] [github] %s failed resolving PR for commit %s: %v", res.Event, res.HeadSHA, err)
	} else {
		res.PRNumber = prNum
		if res.Branch == "" || strings.HasPrefix(res.Branch, "refs/heads/") {
			res.Branch = firstNonEmpty(branch, res.Branch)
		}
		res.ResolutionSource = "commit_sha_lookup"
	}
}

func (s *RouterServer) completeCIFailureTransition(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	if res.PRNumber > 0 && strings.Contains(strings.ToLower(res.CheckName), "roborev") {
		if comment := s.fetchLatestRoboRevComment(ctx, res.Repo, res.PRNumber, res.HeadSHA); comment != "" {
			details = comment
		}
	}
	if res.PRNumber > 0 {
		failCount, fcErr := s.registry.IncrementCIFailCount(ctx, res.Repo, res.PRNumber)
		if fcErr != nil {
			log.Printf("[webhooks-router] [registry] warning incrementing ci_fail_count for %s#%d: %v", res.Repo, res.PRNumber, fcErr)
		}
		if failCount > maxCIRemediationAttempts {
			if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber, "failed", fmt.Sprintf("CI remediation cap (%d/%d) reached — awaiting human review", maxCIRemediationAttempts, maxCIRemediationAttempts), nil); err != nil {
				log.Printf("[webhooks-router] [widget] sync pr widget error: %v", err)
			}
			if s.dispatcher != nil && IsValidDiscordSnowflake(res.TargetID) {
				if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
					ChannelID: res.TargetID,
					Content:   fmt.Sprintf("🛑 %sCI check %q failed %d times (remediation cap %d/%d reached). Automatic fix loop paused for human review.", formatDirectMessagePrefix(res.PRNumber, res.Repo), res.CheckName, failCount, maxCIRemediationAttempts, maxCIRemediationAttempts),
				}); err != nil {
					log.Printf("[webhooks-router] [dispatcher] error dispatching cap direct message: %v", err)
				}
			}
			return nil
		}
	}
	return s.dispatchCIFailurePrompt(ctx, res.TargetID, res.CheckName, res.Repo, res.HeadSHA, res.PRNumber, details, checkURL)
}

func (s *RouterServer) handleCIFailureTransition(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	if s.registry == nil || (res.Conclusion != "failure" && res.Conclusion != "timed_out") {
		return nil
	}
	if res.PRNumber > 0 {
		targetID, updated, err := s.registry.AtomicTransitionCIFailed(ctx, res.Repo, res.PRNumber)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		if updated {
			return s.completeCIFailureTransition(ctx, res, details, checkURL)
		}
		return nil
	}
	if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
		targetID, prNum, updated, err := s.registry.AtomicTransitionCIFailedByHeadSHA(ctx, res.Repo, res.HeadSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		if prNum > 0 && res.PRNumber == 0 {
			res.PRNumber = prNum
		}
		if updated {
			return s.completeCIFailureTransition(ctx, res, details, checkURL)
		}
	}
	return nil
}

func (s *RouterServer) ProcessGitHubEvent(ctx context.Context, event, delivery string, body []byte) (*ResolvedGitHubEvent, error) {
	if s.onProcessed != nil {
		defer s.onProcessed(event, delivery)
	}
	res := &ResolvedGitHubEvent{Event: event, Delivery: delivery}

	switch event {
	case "ping":
		var p GitHubPingPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal ping payload: %w", err)
		}
		if p.Repo != nil {
			res.Repo = p.Repo.FullName
		}
		log.Printf("[webhooks-router] [github] [ping] hook_id=%d repo=%s zen=%q delivery=%s", p.HookID, res.Repo, p.Zen, delivery)
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
			switch res.Action {
			case "closed":
				if p.PullRequest.Merged {
					targetID, err := s.registry.UpdatePRMerged(ctx, res.Repo, res.PRNumber, res.MergeSHA)
					if err != nil {
						return nil, err
					}
					res.TargetID = firstNonEmpty(res.TargetID, targetID)
					if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber, "merged", "", nil); err != nil {
						log.Printf("[webhooks-router] [widget] warning syncing pr_merged widget for %s#%d: %v", res.Repo, res.PRNumber, err)
					}
					if !isHangarSyncedRepo(res.Repo) {
						if err := s.advanceVanadisChunk(ctx, res.Repo, res.PRNumber); err != nil {
							return nil, err
						}
					}
				} else {
					targetID, err := s.registry.UpdatePRClosedUnmerged(ctx, res.Repo, res.PRNumber)
					if err != nil {
						return nil, err
					}
					res.TargetID = firstNonEmpty(res.TargetID, targetID)
					if _, err := s.registry.FailVanadisChunkByPR(ctx, res.Repo, res.PRNumber); err != nil {
						log.Printf("[webhooks-router] [vanadis] warning failing vanadis chunk for closed unmerged PR %s#%d: %v", res.Repo, res.PRNumber, err)
					}
					if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber, "closed", "", nil); err != nil {
						log.Printf("[webhooks-router] [widget] warning syncing pr_closed widget for %s#%d: %v", res.Repo, res.PRNumber, err)
					}
				}
			case "synchronize":
				targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
				if err != nil {
					return nil, err
				}
				res.TargetID = firstNonEmpty(res.TargetID, targetID)
			case "auto_merge_disabled":
				targetID, updated, err := s.registry.AtomicTransitionConflict(ctx, res.Repo, res.PRNumber)
				if err != nil {
					return nil, err
				}
				res.TargetID = firstNonEmpty(res.TargetID, targetID)
				if updated {
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
		s.resolveEventPR(ctx, res, p.CheckRun.PullRequests, "check_run_payload")
		log.Printf("[webhooks-router] [github] [check_run] repo=%s check=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
			res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

		details := firstNonEmpty(strings.TrimSpace(p.CheckRun.Output.Text), strings.TrimSpace(p.CheckRun.Output.Summary), strings.TrimSpace(p.CheckRun.Output.Title))
		checkURL := firstNonEmpty(strings.TrimSpace(p.CheckRun.HTMLURL), strings.TrimSpace(p.CheckRun.DetailsURL))
		if err := s.handleCIFailureTransition(ctx, res, details, checkURL); err != nil {
			return nil, err
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
		s.resolveEventPR(ctx, res, p.WorkflowRun.PullRequests, "workflow_run_payload")
		log.Printf("[webhooks-router] [github] [workflow_run] repo=%s workflow=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
			res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

		details := fmt.Sprintf("Workflow %q concluded with %s", p.WorkflowRun.Name, p.WorkflowRun.Conclusion)
		if err := s.handleCIFailureTransition(ctx, res, details, strings.TrimSpace(p.WorkflowRun.HTMLURL)); err != nil {
			return nil, err
		}
		if p.Action == "completed" && p.WorkflowRun.Status == "completed" {
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

		if p.Deleted || isZeroSHA(p.After) {
			res.ResolutionSource = "branch_deleted"
			return res, nil
		}
		if !strings.HasPrefix(p.Ref, "refs/heads/") {
			res.ResolutionSource = "non_branch_ref"
			return res, nil
		}
		res.HeadSHA = p.After
		if p.HeadCommit != nil && p.HeadCommit.ID != "" {
			res.HeadSHA = p.HeadCommit.ID
		}
		if !strings.HasPrefix(p.Ref, "refs/heads/main") {
			res.ResolutionSource = "non_main_branch"
			return res, nil
		}

		s.resolveEventPR(ctx, res, nil, "")
		if res.PRNumber > 0 {
			res.MergeSHA = res.HeadSHA
		}
		log.Printf("[webhooks-router] [github] [push] repo=%s ref=%s head_sha=%s resolved_pr=%d branch=%s source=%s target_id=%s delivery=%s",
			res.Repo, p.Ref, res.HeadSHA, res.PRNumber, res.Branch, res.ResolutionSource, res.TargetID, delivery)

		if s.registry != nil && res.PRNumber > 0 && res.MergeSHA != "" {
			targetID, err := s.registry.BackfillPushMergeSHA(ctx, res.Repo, res.PRNumber, res.MergeSHA)
			if err != nil {
				return nil, err
			}
			res.TargetID = firstNonEmpty(res.TargetID, targetID)
		}

		if err := s.dispatchGitPush(ctx, res.Repo, p.Ref, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
			return nil, err
		}
		if res.PRNumber > 0 && !isHangarSyncedRepo(res.Repo) {
			if err := s.advanceVanadisChunk(ctx, res.Repo, res.PRNumber); err != nil {
				return nil, err
			}
		}
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
		return res, nil

	default:
		log.Printf("[webhooks-router] [github] [unhandled] event=%s delivery=%s payload_size=%d bytes", event, delivery, len(body))
		res.ResolutionSource = "unhandled_event"
		return res, nil
	}
}

func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", fmt.Errorf("invalid repo or sha for pr resolution")
	}
	var prs []GitHubCommitPRItem
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s/pulls", repo, url.PathEscape(sha)), 1<<20, &prs); err != nil {
		return 0, "", err
	}
	if len(prs) == 0 {
		return 0, "", fmt.Errorf("no pull requests associated with commit %s", sha)
	}
	selected := prs[0]
	for _, item := range prs[1:] {
		if (selected.State != "closed" && item.State == "closed") || (selected.State == item.State && item.Number > selected.Number) {
			selected = item
		}
	}
	return selected.Number, selected.Head.Ref, nil
}

func (s *RouterServer) resolveWorkflowRunImages(ctx context.Context, repo string, runID int64) ([]string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || runID <= 0 {
		return nil, fmt.Errorf("invalid repo or runID for workflow run images resolution")
	}
	var jobsResp GitHubWorkflowJobsResponse
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?per_page=100", repo, runID), 5<<20, &jobsResp); err != nil {
		return nil, err
	}

	owner := "azylman"
	if parts := strings.Split(repo, "/"); len(parts) == 2 {
		owner = strings.ToLower(parts[0])
	}

	cleanRepo := strings.ToLower(repo)
	var rawImages []string
	for _, job := range jobsResp.Jobs {
		if job.Status != "completed" || job.Conclusion != "success" || !hasExecutedBuildStep(job.Steps) {
			continue
		}
		m := buildJobRegex.FindStringSubmatch(job.Name)
		if len(m) < 2 {
			continue
		}
		service := strings.TrimSpace(m[1])
		switch {
		case (strings.HasSuffix(cleanRepo, "aerial-sidecars") || strings.Contains(cleanRepo, "sidecar")) && !strings.HasPrefix(service, "orin-"):
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/aerial-sidecar-%s:latest", owner, service))
		case strings.HasSuffix(cleanRepo, "aerial-sidecars") || strings.Contains(cleanRepo, "sidecar") || strings.HasSuffix(cleanRepo, "mirrormere") || strings.Contains(cleanRepo, "mirrormere"):
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/%s:latest", owner, service))
		default:
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/aerial-%s:latest", owner, service))
		}
	}
	return dedupeTrimmedStrings(rawImages), nil
}

func (s *RouterServer) getInfisicalToken(ctx context.Context) (string, error) {
	if s.cfg.InfisicalClientID == "" || s.cfg.InfisicalClientSecret == "" {
		return "", fmt.Errorf("infisical client ID or client secret not configured")
	}
	payload, err := json.Marshal(map[string]string{
		"clientId":     s.cfg.InfisicalClientID,
		"clientSecret": s.cfg.InfisicalClientSecret,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.InfisicalURL+"/api/v1/auth/universal-auth/login", bytes.NewReader(payload))
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
		return "", fmt.Errorf("infisical login returned status %d: %s", resp.StatusCode, readBodyString(resp.Body))
	}
	var loginResp UniversalAuthLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return "", fmt.Errorf("decoding infisical login response: %w", err)
	}
	token := firstNonEmpty(loginResp.AccessToken, loginResp.Token)
	if token == "" {
		return "", fmt.Errorf("empty access token in infisical login response")
	}
	return token, nil
}

func readBodyString(r io.Reader) string {
	body, err := io.ReadAll(r)
	if err != nil {
		return ""
	}
	return string(body)
}

func (s *RouterServer) fetchInfisicalSecrets(ctx context.Context, token, workspaceID, envName string) (map[string]string, error) {
	params := url.Values{
		"workspaceId": {firstNonEmpty(workspaceID, s.cfg.InfisicalProjectID)},
		"environment": {firstNonEmpty(envName, s.cfg.InfisicalEnvironment)},
		"secretPath":  {"/"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.InfisicalURL+"/api/v3/secrets/raw?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("infisical fetch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("infisical fetch returned status %d: %s", resp.StatusCode, readBodyString(resp.Body))
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
	data, err := json.Marshal(NomadVariable{Path: TargetNomadVariable, Items: items})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/v1/var/%s", s.cfg.NomadAddr, TargetNomadVariable), bytes.NewReader(data))
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
		return fmt.Errorf("nomad put variable returned status %d: %s", resp.StatusCode, readBodyString(resp.Body))
	}
	return nil
}

// SyncSecrets coordinates fetching secrets from Infisical and synchronizing directly to Nomad.
func (s *RouterServer) SyncSecrets(ctx context.Context, workspaceID, envName string) error {
	log.Printf("[webhooks-router] Initiating secret sync to %s (env: %s)", TargetNomadVariable, envName)
	token, err := s.getInfisicalToken(ctx)
	if err != nil {
		return err
	}
	newItems, err := s.fetchInfisicalSecrets(ctx, token, workspaceID, envName)
	if err != nil {
		return err
	}
	if err := s.putNomadVariable(ctx, newItems); err != nil {
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
		river.AddWorker(workers, &NomadCrashAlertWorker{server: server})

		rc, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.QueueDefault: {MaxWorkers: 10},
			},
			Workers:             workers,
			SkipUnknownJobCheck: true,
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

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cfg.Port)
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
