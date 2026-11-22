package prstate

// Journaled pr_registry projection. Restate K/V in PRObject is the ONLY
// lifecycle authority; Postgres pr_registry is a replayable projection of
// committed PRObject state plus a SHA lookup index. Routing may use the
// projection; authority may not.
//
// Ownership ruling: prstate owns every pr_registry column and every metadata
// key in OwnedMetadataKeys, which is the full set of keys a registry row
// carries in prod. BuildProjection maps the whole PRState row; the upsert
// replaces the owned keys wholesale (an owned key absent from the snapshot
// is removed, exactly like the registry's `- 'banner'` idioms) and leaves any
// unknown key untouched. Synapse registry code is not imported: the SQL below
// mirrors its metadata-blob idioms (jsonb `-` / `||`, ON CONFLICT (repo,
// pr_number)).
//
// Ordering contract: the projection runs as a one-way PRObject self-send AFTER the state commit (PRObject.ProjectRegistry).
// The send re-derives the snapshot from committed state at delivery time, so
// a queued older send can never land a stale row: sends for one PR are
// totally ordered by the PRObject queue and every one projects current
// state. Projection failure never discards or gates the transition; the
// committed state stays and a delayed self-send retries. The upsert is
// idempotent per ProjectionJournalKey, and PRObject records the last
// projected key so redundant sends never rewrite an identical row.
// RebuildProjection enumerates only the RepoObject touched-PR index, never
// the projection being rebuilt, so rebuild is non-circular.
//
// External idempotency tolerance: the registry upsert has no external
// idempotency-key parameter, so at-least-once delivery is accepted here and
// absorbed by the idempotent upsert (same snapshot, same row).

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ProjectionMetadata is the typed pr_registry metadata blob. JSON tags match
// the synapse registry keys exactly, and every field is omitempty so an empty
// value projects as an absent key (the registry removes or never sets keys
// it has no value for).
type ProjectionMetadata struct {
	Agent                  string                          `json:"agent,omitempty"`
	BotID                  string                          `json:"bot_id,omitempty"`
	Banner                 string                          `json:"banner,omitempty"`
	BuildMetadata          *BuildMetadata                  `json:"build_metadata,omitempty"`
	CIChecks               map[string]string               `json:"ci_checks,omitempty"`
	CIFailCount            int                             `json:"ci_fail_count,omitempty"`
	DeployFailedPublished  bool                            `json:"deploy_failed_published,omitempty"`
	DeploySuccessPending   bool                            `json:"deploy_success_pending,omitempty"`
	DeployedJobs           []string                        `json:"deployed_jobs,omitempty"`
	DeployedKustomizations []string                        `json:"deployed_kustomizations,omitempty"`
	JobStates              map[string]string               `json:"job_states,omitempty"`
	Jobs                   []string                        `json:"jobs,omitempty"`
	KustomizationRequests  map[string]KustomizationRequest `json:"kustomization_requests,omitempty"`
	StartedJobs            []string                        `json:"started_jobs,omitempty"`
	VanadisChunkID         string                          `json:"vanadis_chunk_id,omitempty"`
	WidgetState            string                          `json:"widget_state,omitempty"`
}

// ProjectionSnapshot is one row write derived from committed PRObject state.
type ProjectionSnapshot struct {
	Repo       string             `json:"repo"`
	PRNumber   int                `json:"pr_number"`
	Agent      string             `json:"agent"`
	Branch     string             `json:"branch"`
	HeadSHA    string             `json:"head_sha"`
	MergeSHA   string             `json:"merge_sha"`
	Title      string             `json:"title"`
	TargetID   string             `json:"target_id"`
	Status     string             `json:"status"`
	Generation uint64             `json:"generation"`
	MetaEpoch  uint64             `json:"meta_epoch"`
	Metadata   ProjectionMetadata `json:"metadata"`
}

// OwnedMetadataKeys returns every metadata key prstate writes, sorted. The
// upsert replaces exactly these keys; any other key in the blob survives.
func OwnedMetadataKeys() []string {
	return []string{
		"agent", "banner", "bot_id", "build_metadata", "ci_checks", "ci_fail_count",
		"deploy_failed_published", "deploy_success_pending", "deployed_jobs",
		"deployed_kustomizations", "job_states", "jobs", "kustomization_requests",
		"started_jobs", "vanadis_chunk_id", "widget_state",
	}
}

// BuildProjection maps committed PRState to a full registry row write. It
// reads only the state it is given and never consults the projection, so
// the projection can never feed back into a transition decision. It always
// writes metadata.agent from the stored owner, which matches the typed
// owner column the registry moves to.
func BuildProjection(s *PRState) ProjectionSnapshot {
	snap := ProjectionSnapshot{Agent: s.OwnerAgent(), Status: s.RegistryStatus()}
	if s == nil {
		return snap
	}
	snap.Repo = NormalizeRepo(s.Repo)
	snap.PRNumber = s.PRNumber
	snap.Branch = strings.TrimSpace(s.Branch)
	snap.HeadSHA = strings.TrimSpace(s.HeadSHA)
	snap.MergeSHA = strings.TrimSpace(s.MergeSHA)
	snap.Title = strings.TrimSpace(s.Title)
	snap.TargetID = strings.TrimSpace(s.TargetID)
	snap.Generation = s.GateGeneration
	snap.MetaEpoch = s.ReopenEpoch
	snap.Metadata = normalizeMetadata(ProjectionMetadata{
		Agent:                  strings.TrimSpace(s.Agent),
		BotID:                  strings.TrimSpace(s.BotID),
		Banner:                 strings.TrimSpace(s.Banner),
		BuildMetadata:          s.BuildMetadata,
		CIChecks:               s.CIChecks,
		CIFailCount:            s.CIFailCount,
		DeployFailedPublished:  s.DeployFailedPublished,
		DeploySuccessPending:   s.DeploySuccessPending,
		DeployedJobs:           s.DeployedJobs,
		DeployedKustomizations: s.DeployedKustomizations,
		JobStates:              s.JobStates,
		Jobs:                   s.Jobs,
		KustomizationRequests:  s.KustomizationRequests,
		StartedJobs:            s.StartedJobs,
		VanadisChunkID:         strings.TrimSpace(s.VanadisChunkID),
		WidgetState:            strings.TrimSpace(s.WidgetState),
	})
	return snap
}

// normalizeMetadata returns a copy with set-valued slices cleaned and
// sorted and empty collections nil, so equal sets marshal identically no
// matter which writer produced them.
func normalizeMetadata(m ProjectionMetadata) ProjectionMetadata {
	m.DeployedJobs = cleanStrings(m.DeployedJobs)
	m.DeployedKustomizations = cleanStrings(m.DeployedKustomizations)
	m.Jobs = cleanStrings(m.Jobs)
	m.StartedJobs = cleanStrings(m.StartedJobs)
	m.CIChecks = cloneNonEmpty(m.CIChecks)
	m.JobStates = cloneNonEmpty(m.JobStates)
	m.KustomizationRequests = cloneNonEmpty(m.KustomizationRequests)
	if m.BuildMetadata != nil {
		clone := *m.BuildMetadata
		m.BuildMetadata = &clone
	}
	return m
}

func cloneNonEmpty[V any](in map[string]V) map[string]V {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]V, len(in))
	maps.Copy(out, in)
	return out
}

// metadataJSON marshals the metadata blob. Every field type marshals
// infallibly (strings, ints, bools, string maps and slices, plain structs),
// so a failure is impossible; it still degrades to an empty object.
func metadataJSON(m ProjectionMetadata) []byte {
	raw, err := json.Marshal(m)
	if err != nil {
		return []byte("{}")
	}
	return raw
}

// ProjectionJournalKey scopes one projection to its exact content:
// repo+PR+headSHA+generation+reopen epoch plus a digest of every projected
// field. Identical snapshots share the key and dedupe; any content or epoch
// change mints a new key. The digest input is NUL-delimited.
func ProjectionJournalKey(snap ProjectionSnapshot) string {
	agent := strings.TrimSpace(snap.Agent)
	if agent == "" {
		agent = DefaultAgent
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s\x00%s\x00%d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%s",
		agent, NormalizeRepo(snap.Repo), snap.PRNumber,
		strings.TrimSpace(snap.Branch), strings.TrimSpace(snap.HeadSHA),
		strings.TrimSpace(snap.MergeSHA), strings.TrimSpace(snap.Title),
		strings.TrimSpace(snap.TargetID), strings.TrimSpace(snap.Status),
		snap.Generation, snap.MetaEpoch, metadataJSON(normalizeMetadata(snap.Metadata)))
	sum := sha256.Sum256([]byte(sb.String()))
	return fmt.Sprintf("projection:%s:%s:%d@%s:gen%d:epoch%d:%x",
		agent, NormalizeRepo(snap.Repo), snap.PRNumber,
		strings.TrimSpace(snap.HeadSHA), snap.Generation, snap.MetaEpoch,
		sum[:8])
}

// projectionUpsertSQL writes every column and replaces the owned metadata
// keys ($11) wholesale with the snapshot blob ($10). A blank text column
// never overwrites a stored value (state that has not learned a field yet
// cannot erase it); status and agent always follow state.
const projectionUpsertSQL = `INSERT INTO pr_registry (repo, pr_number, agent, branch, head_sha, merge_sha, target_id, title, status, metadata, created_at, updated_at) ` +
	`VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, $9, $10::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ` +
	`ON CONFLICT (repo, pr_number) DO UPDATE SET ` +
	`agent = EXCLUDED.agent, ` +
	`branch = CASE WHEN EXCLUDED.branch != '' THEN EXCLUDED.branch ELSE pr_registry.branch END, ` +
	`head_sha = CASE WHEN EXCLUDED.head_sha != '' THEN EXCLUDED.head_sha ELSE pr_registry.head_sha END, ` +
	`merge_sha = COALESCE(EXCLUDED.merge_sha, pr_registry.merge_sha), ` +
	`target_id = CASE WHEN EXCLUDED.target_id != '' THEN EXCLUDED.target_id ELSE pr_registry.target_id END, ` +
	`title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END, ` +
	`status = EXCLUDED.status, ` +
	`metadata = (COALESCE(pr_registry.metadata, '{}'::jsonb) - $11::text[]) || EXCLUDED.metadata, ` +
	`updated_at = CURRENT_TIMESTAMP;`

// ProjectionUpsertQuery builds the full-row pr_registry upsert for snap.
func ProjectionUpsertQuery(snap ProjectionSnapshot) (string, []any) {
	agent := strings.TrimSpace(snap.Agent)
	if agent == "" {
		agent = DefaultAgent
	}
	args := []any{
		NormalizeRepo(snap.Repo), snap.PRNumber,
		agent,
		strings.TrimSpace(snap.Branch), strings.TrimSpace(snap.HeadSHA),
		strings.TrimSpace(snap.MergeSHA), strings.TrimSpace(snap.TargetID),
		strings.TrimSpace(snap.Title), strings.TrimSpace(snap.Status),
		string(metadataJSON(normalizeMetadata(snap.Metadata))),
		OwnedMetadataKeys(),
	}
	return projectionUpsertSQL, args
}

// ProjectionStore is the pr_registry write boundary.
type ProjectionStore interface {
	UpsertProjection(ctx context.Context, snap ProjectionSnapshot) error
}

// ProjectionDB is the pgx surface the Postgres projection store needs;
// *pgxpool.Pool satisfies it.
type ProjectionDB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// PgProjectionStore is the production ProjectionStore over vanadis-restate's existing Postgres pool.
type PgProjectionStore struct {
	db ProjectionDB
}

// NewPgProjectionStore returns a projection store over db.
func NewPgProjectionStore(db ProjectionDB) *PgProjectionStore {
	return &PgProjectionStore{db: db}
}

// UpsertProjection runs the full-row upsert.
func (p *PgProjectionStore) UpsertProjection(ctx context.Context, snap ProjectionSnapshot) error {
	if p == nil || p.db == nil {
		return errors.New("projection database is nil")
	}
	query, args := ProjectionUpsertQuery(snap)
	if _, err := p.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("upsert pr_registry projection %s#%d: %w", snap.Repo, snap.PRNumber, err)
	}
	return nil
}

// ProjectRegistry validates the snapshot and performs exactly one store
// write; on store failure it returns the error and the caller keeps the
// already-committed state for a later retry.
func ProjectRegistry(ctx context.Context, store ProjectionStore, snap ProjectionSnapshot) error {
	if store == nil {
		return errors.New("projection store is nil")
	}
	if NormalizeRepo(snap.Repo) == "" || snap.PRNumber <= 0 {
		return errors.New("projection snapshot missing repo or pr number")
	}
	if strings.TrimSpace(snap.Status) == "" {
		return errors.New("projection snapshot missing status")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.UpsertProjection(ctx, snap)
}

// Tracked reports whether the snapshot is for a PR the registry tracks: one
// that Register or a target-carrying event gave a target thread.
// Synapse writes a row only for registered PRs, and registration always
// resolves a target, so an untracked PR has no row on either writer.
func (s ProjectionSnapshot) Tracked() bool {
	return strings.TrimSpace(s.TargetID) != ""
}

// Compile-time freeze: the Postgres store satisfies the write boundary.
var _ ProjectionStore = (*PgProjectionStore)(nil)
