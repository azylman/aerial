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

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

const (
	chunkModeAction = "action"
	defaultAgent    = "aerial"
	effortLow       = "low"
	effortMedium    = "medium"
	jsonNull        = "null"
)

// BuildColumns is the canonical build-row select list.
const BuildColumns = `id, COALESCE(NULLIF(agent, ''), 'aerial'), thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, quiet, created_at, updated_at`

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
	db          PgxQueryExecer
	gitHubOwner string
}

// New returns a Store backed by db. gitHubOwner is the configured GitHub
// owner whose bare and qualified repo spellings chunk lookups treat as one.
func New(db PgxQueryExecer, gitHubOwner string) *Store {
	return &Store{db: db, gitHubOwner: gitHubOwner}
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

type AttachChunkPRRequest = buildplan.AttachChunkPRRequest

type CreateChunkInput = buildplan.CreateChunkInput

type UpdateChunkRequest = buildplan.UpdateChunkRequest

type UpdateBuildRequest struct {
	Title         string `json:"title,omitempty"`
	Objective     string `json:"objective,omitempty"`
	SharedContext string `json:"shared_context,omitempty"`
}

// RetryActionChunkRequest fences a single operator-requested action retry.
type RetryActionChunkRequest = buildplan.RetryActionChunkRequest

type RetryActionChunkResult = buildplan.RetryActionChunkResult

// VanadisStore is the persistence authority for multi-chunk builds.
type VanadisStore interface {
	RetryActionChunk(ctx context.Context, req RetryActionChunkRequest) (*RetryActionChunkResult, error)
	CreateBuild(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) error
	GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error)
	ReserveChunkForLaunch(ctx context.Context, chunkID string) error
	UpdateBuildStatus(ctx context.Context, buildID, status string) error
	CompareAndSwapBuildStatusMessageID(ctx context.Context, buildID, oldID, newID string) (bool, error)
	GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error)
	LookupPRChunk(ctx context.Context, repo string, prNumber int) (route BuildRoute, chunkID string, err error)
	LookupPRBuildRoutes(ctx context.Context, repo string, prNumber int) ([]BuildRoute, error)
	LookupChunkBuildID(ctx context.Context, chunkID string) (route BuildRoute, err error)
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
	SweepEarlierDeploymentsAcrossBuilds(ctx context.Context, deploying model.VanadisChunk) ([]SweptChunk, error)
	UpdatePRLifecycleStatus(ctx context.Context, chunkID, status string) error
	UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error
}

// CapChunkEffort clamps an effort value to a valid effort.
func CapChunkEffort(effort string) string {
	eff, err := buildplan.NormalizeChunkEffort(effort)
	if err != nil {
		return effortMedium
	}
	return eff
}

// RepoMatchCandidates returns the repo spellings a chunk lookup should
// match: a bare repo also matches its owner-qualified form, and a repo
// under owner also matches its bare name. Other owners match only as given.
func RepoMatchCandidates(owner, repo string) []string {
	repo = buildplan.TrimRepo(repo)
	if repo == "" {
		return nil
	}
	owner = strings.Trim(strings.TrimSpace(owner), "/")
	if owner == "" {
		return []string{repo}
	}
	if !strings.Contains(repo, "/") {
		return []string{repo, githubapi.QualifyRepo(owner, repo)}
	}
	if bare, ok := strings.CutPrefix(repo, owner+"/"); ok && bare != "" && !strings.Contains(bare, "/") {
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
	err := s.Scan(&b.ID, &b.Agent, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.Quiet, &b.CreatedAt, &b.UpdatedAt)
	if err == nil && strings.TrimSpace(b.Agent) == "" {
		b.Agent = defaultAgent
	}
	return b, err
}

// NormalizeRunnerAgent canonicalizes a runner agent name: trimmed and
// lowercased, with empty meaning the default agent.
func NormalizeRunnerAgent(runner string) string {
	if name := agent.Clean(runner); name != "" {
		return name
	}
	return defaultAgent
}

// BuildRoute is the Restate routing key for one build: its ID.
type BuildRoute struct {
	BuildID string `json:"build_id"`
}

// ApplyRawChunkCIChecks folds raw CI check JSON into derived chunk fields.
func ApplyRawChunkCIChecks(c *model.VanadisChunk, rawCIChecks []byte) {
	buildplan.ApplyRawChunkCIChecks(c, rawCIChecks)
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
	dest := make([]any, 0, 23+len(extra))
	dest = append(dest,
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Repo, &c.Title, &c.Instructions,
		&c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status,
		&c.PRNumber, &c.StallNudges, &c.DependsOn, &c.CIFailCount, &rawCIChecks,
		&rawRolloutState, &c.Banner,
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
	c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria = buildplan.NonNilSlice(c.TargetFiles), buildplan.NonNilSlice(c.TargetSymbols), buildplan.NonNilSlice(c.AcceptanceCriteria)
	c.DependsOn = buildplan.NonNilSlice(c.DependsOn)
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
