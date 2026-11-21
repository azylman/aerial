// Package store holds the shared persistence core for vanadis-restate: the
// pgx query interface, the Store handle, row scanners, request DTOs, the
// VanadisStore interface, and pure validation and normalization helpers.
// Values for modes, efforts, status vocabulary, and SQL fragments stay frozen
// with the root package copies until later chunks move their consumers.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

const (
	chunkModeAction  = "action"
	defaultAgent     = "aerial"
	defaultRepoOwner = "azylman"
	effortLow        = "low"
	effortMedium     = "medium"
	jsonNull         = "null"
)

// BuildColumns is the canonical build-row select list.
const BuildColumns = `id, COALESCE(NULLIF(agent, ''), 'aerial'), thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, quiet, created_at, updated_at, COALESCE(NULLIF(runner_agent, ''), 'aerial')`

// PgxQueryExecer abstracts the pgx query surface used by the store.
type PgxQueryExecer interface {
	Exec(ctx context.Context, query string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

type rowScanner interface {
	Scan(dest ...any) error
}

// Store is the shared pgx-backed persistence handle embedded by the root
// PostgresVanadisStore.
type Store struct {
	db PgxQueryExecer
}

// New returns a Store backed by db.
func New(db PgxQueryExecer) *Store {
	return &Store{db: db}
}

// DB returns the underlying query surface so sibling packages (the prstate
// pr_registry projection) share the one pool instead of opening another.
// A nil Store returns nil.
func (s *Store) DB() PgxQueryExecer {
	if s == nil {
		return nil
	}
	return s.db
}

type VanadisChunkSummary struct {
	ID          string    `json:"id"`
	ChunkIndex  int       `json:"chunk_index"`
	Mode        string    `json:"mode"`
	Repo        string    `json:"repo,omitempty"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	PRNumber    int       `json:"pr_number,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

type VanadisAdvancementResult struct {
	CompletionAccepted bool                  `json:"completion_accepted,omitempty"`
	Matched            bool                  `json:"matched"`
	Advanced           bool                  `json:"advanced"`
	BuildID            string                `json:"build_id"`
	ThreadID           string                `json:"thread_id"`
	Tier               int                   `json:"tier"`
	Title              string                `json:"title"`
	Objective          string                `json:"objective"`
	BuildStatus        string                `json:"build_status"`
	StatusMessageID    string                `json:"status_message_id,omitempty"`
	MergedChunkIndex   int                   `json:"merged_chunk_index"`
	MergedChunkTitle   string                `json:"merged_chunk_title"`
	ChunkMode          string                `json:"chunk_mode,omitempty"`
	NextChunkIndex     int                   `json:"next_chunk_index"`
	BuildCompleted     bool                  `json:"build_completed"`
	Chunks             []VanadisChunkSummary `json:"chunks,omitempty"`
}

type VanadisChunkPRLookup struct {
	Matched bool   `json:"matched"`
	ChunkID string `json:"chunk_id,omitempty"`
	BuildID string `json:"build_id,omitempty"`
	Effort  string `json:"effort,omitempty"`
}

type AttachChunkPRRequest struct {
	ChunkID  string `json:"chunk_id,omitempty"`
	PRNumber int    `json:"pr_number"`
	Repo     string `json:"repo"`
	Branch   string `json:"branch,omitempty"`
	Title    string `json:"title,omitempty"`
	TargetID string `json:"target_id,omitempty"`
	Quiet    bool   `json:"quiet,omitempty"`
	// Agent is the caller agent that owns a new standalone build.
	Agent string `json:"agent,omitempty"`
	// RunnerAgent is the agent of the Build object executing the attach. The
	// Build object overwrites it with its own agent; requests never set it.
	RunnerAgent string `json:"runner_agent,omitempty"`
}

type CreateChunkInput struct {
	ID                 string   `json:"id"`
	Mode               string   `json:"mode,omitempty"`
	Repo               string   `json:"repo,omitempty"`
	Title              string   `json:"title"`
	Instructions       string   `json:"instructions"`
	TargetFiles        []string `json:"target_files"`
	TargetSymbols      []string `json:"target_symbols"`
	Effort             string   `json:"effort,omitempty"`
	AcceptanceCriteria []string `json:"acceptance_criteria"`
	DependsOn          []string `json:"depends_on,omitempty"`
}

type UpdateChunkRequest struct {
	ChunkRef      string    `json:"chunk_ref,omitempty"`
	Mode          *string   `json:"mode,omitempty"`
	Repo          string    `json:"repo,omitempty"`
	Title         string    `json:"title,omitempty"`
	Instructions  string    `json:"instructions,omitempty"`
	TargetFiles   []string  `json:"target_files,omitempty"`
	TargetSymbols []string  `json:"target_symbols,omitempty"`
	Effort        string    `json:"effort,omitempty"`
	DependsOn     *[]string `json:"depends_on,omitempty"`
}

type UpdateBuildRequest struct {
	Title         string `json:"title,omitempty"`
	Objective     string `json:"objective,omitempty"`
	SharedContext string `json:"shared_context,omitempty"`
}

// RetryActionChunkRequest fences a single operator-requested action retry.
type RetryActionChunkRequest struct {
	BuildID                 string  `json:"build_id"`
	ChunkID                 string  `json:"chunk_id"`
	ExpectedDispatchID      string  `json:"expected_dispatch_id"`
	Reason                  string  `json:"reason"`
	ExpectedInstructions    *string `json:"expected_instructions,omitempty"    jsonschema:"Exact stored instructions required for correction; supply with replacement_instructions"`
	ReplacementInstructions *string `json:"replacement_instructions,omitempty" jsonschema:"Corrected nonblank worker instructions atomically saved before retry; supply with expected_instructions"`
}

type RetryActionChunkResult struct {
	BuildID    string `json:"build_id"`
	ChunkID    string `json:"chunk_id"`
	DispatchID string `json:"dispatch_id"`
	Accepted   bool   `json:"accepted"`
}

// VanadisStore is the persistence authority for multi-chunk builds.
type VanadisStore interface {
	RetryActionChunk(ctx context.Context, req RetryActionChunkRequest) (*RetryActionChunkResult, error)
	CreateBuild(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) error
	GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error)
	UpdateChunkStatus(ctx context.Context, chunkID, status string) error
	ReserveChunkForLaunch(ctx context.Context, chunkID string) error
	UpdateBuildStatus(ctx context.Context, buildID, status string) error
	UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error
	GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error)
	LookupPRChunk(ctx context.Context, repo string, prNumber int) (route BuildRoute, chunkID string, err error)
	LookupChunkBuildID(ctx context.Context, chunkID string) (route BuildRoute, err error)
	LookupUnattachedChunkByThreadAndRepo(ctx context.Context, threadID, repo string) (route BuildRoute, chunkID string, err error)
	AttachChunkPR(ctx context.Context, req AttachChunkPRRequest) (buildID, chunkID, threadID string, err error)
	CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*VanadisAdvancementResult, error)
	FailChunk(ctx context.Context, chunkID, reason, dispatchID string) (*model.VanadisBuild, *model.VanadisChunk, bool, error)
	UpdateBuild(ctx context.Context, buildID string, req UpdateBuildRequest) (*model.VanadisBuild, error)
	UpdateChunk(ctx context.Context, buildID, chunkRef string, req UpdateChunkRequest) (*model.VanadisChunk, error)
	AppendChunks(ctx context.Context, buildID string, inputs []CreateChunkInput) ([]model.VanadisChunk, error)
	RemoveChunk(ctx context.Context, buildID, chunkRef string) (*model.VanadisChunk, []model.VanadisChunk, error)
	RecordPRRolloutState(ctx context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error
	RecordPRCheckStatus(ctx context.Context, chunkID, headSHA, checkName, checkState string) error
	ResetPRCheckState(ctx context.Context, chunkID, headSHA, mergeSHA string) error
	IncrementChunkCIFailCount(ctx context.Context, chunkID, banner string, headSHA ...string) (int, error)
	CompletePRDeployment(ctx context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error
	MarkActionRepairDeployed(ctx context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error
	SweepEarlierDeploymentsAcrossBuilds(ctx context.Context, deploying model.VanadisChunk) (swept, recovered []SweptChunk, err error)
	UpdatePRLifecycleStatus(ctx context.Context, chunkID, status string) error
	UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error
}

// FirstNonEmpty returns the first trimmed non-empty value.
func FirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// NonNilSlice normalizes a nil slice to an empty slice.
func NonNilSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// CapChunkEffort clamps an effort value to a valid effort.
func CapChunkEffort(effort string) string {
	eff, err := buildplan.NormalizeChunkEffort(effort)
	if err != nil {
		return effortMedium
	}
	return eff
}

// RepoMatchCandidates returns the repo spellings a chunk lookup should match.
func RepoMatchCandidates(repo string) []string {
	repo = buildplan.NormalizeRepo(repo)
	if repo == "" {
		return nil
	}
	if !strings.Contains(repo, "/") {
		return []string{repo, defaultRepoOwner + "/" + repo}
	}
	if bare, ok := strings.CutPrefix(repo, defaultRepoOwner+"/"); ok && bare != "" && !strings.Contains(bare, "/") {
		return []string{repo, bare}
	}
	return []string{repo}
}

// ScanBuild scans a single build row.
func ScanBuild(s rowScanner) (model.VanadisBuild, error) {
	if s == nil {
		return model.VanadisBuild{}, errors.New("row scanner is nil")
	}
	var b model.VanadisBuild
	err := s.Scan(&b.ID, &b.Agent, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.Quiet, &b.CreatedAt, &b.UpdatedAt, &b.RunnerAgent)
	if err == nil && strings.TrimSpace(b.Agent) == "" {
		b.Agent = defaultAgent
	}
	if err == nil {
		b.RunnerAgent = NormalizeRunnerAgent(b.RunnerAgent)
	}
	return b, err
}

// NormalizeRunnerAgent canonicalizes a runner agent name: trimmed and
// lowercased, with empty meaning the default agent.
func NormalizeRunnerAgent(agent string) string {
	if name := model.NormalizeAgentName(agent); name != "" {
		return name
	}
	return defaultAgent
}

// BuildRoute is the Restate routing key for one build: its ID and the agent
// whose vanadis-restate runner executes it.
type BuildRoute struct {
	BuildID     string `json:"build_id"`
	RunnerAgent string `json:"runner_agent"`
}

// ApplyRawChunkCIChecks folds raw CI check JSON into derived chunk fields.
func ApplyRawChunkCIChecks(c *model.VanadisChunk, rawCIChecks []byte) {
	if len(rawCIChecks) == 0 || string(rawCIChecks) == jsonNull || string(rawCIChecks) == "{}" {
		return
	}
	var checks map[string]string
	if err := json.Unmarshal(rawCIChecks, &checks); err != nil {
		return
	}
	c.HeadSHA = strings.TrimSpace(checks["_head_sha"])
	c.RemediatedSHA = strings.TrimSpace(checks["_remediated_sha"])
	c.MergeSHA = strings.TrimSpace(checks["_merge_sha"])
	for k := range checks {
		if strings.HasPrefix(k, "_") {
			delete(checks, k)
		}
	}
	if len(checks) > 0 {
		c.CIChecks = checks
	}
}

// ScanChunks scans ordered chunk rows.
func ScanChunks(rows pgx.Rows) ([]model.VanadisChunk, error) {
	chunks := make([]model.VanadisChunk, 0)
	for rows.Next() {
		c, err := scanChunkRow(rows)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating vanadis chunks: %w", err)
	}
	return chunks, nil
}

// scanChunkRow scans one canonical chunk row followed by optional trailing
// columns (for joins that select extra build fields after the chunk).
func scanChunkRow(row rowScanner, extra ...any) (model.VanadisChunk, error) {
	var c model.VanadisChunk
	var startedAt, completedAt sql.NullTime
	var rawCIChecks, rawRolloutState []byte
	dest := make([]any, 0, 25+len(extra))
	dest = append(dest,
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Repo, &c.Title, &c.Instructions,
		&c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status,
		&c.PRNumber, &c.StallNudges, &c.DependsOn, &c.CIFailCount, &rawCIChecks,
		&rawRolloutState, &c.Banner, &c.MergeAwakeableID, &c.DeployAwakeableID,
		&startedAt, &completedAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return model.VanadisChunk{}, fmt.Errorf("failed to scan vanadis chunk: %w", err)
	}
	if startedAt.Valid {
		c.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		c.CompletedAt = completedAt.Time
	}
	c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria = NonNilSlice(c.TargetFiles), NonNilSlice(c.TargetSymbols), NonNilSlice(c.AcceptanceCriteria)
	c.DependsOn = NonNilSlice(c.DependsOn)
	c.Effort = CapChunkEffort(c.Effort)
	ApplyRawChunkCIChecks(&c, rawCIChecks)
	if len(rawRolloutState) > 0 && string(rawRolloutState) != jsonNull && string(rawRolloutState) != "{}" {
		var rs pubsub.PRRolloutState
		if err := json.Unmarshal(rawRolloutState, &rs); err == nil {
			c.RolloutState = &rs
		}
	}
	return c, nil
}
