package prstate

// Journaled pr_registry projection. Restate K/V in PRObject is the ONLY
// lifecycle authority; Postgres pr_registry is a replayable projection of
// committed PRObject state plus a SHA lookup index. Routing may use the
// projection; authority may not.
//
// Typed-only write: one transaction writes the row columns, the typed
// scalar columns and the child tables (pr_registry_checks, _jobs,
// _kustomizations, _build_selections, _build_results). The legacy
// pr_registry.metadata JSON column is neither read nor written; it keeps
// whatever it last held until it is dropped.
//
// Ownership ruling: prstate owns every pr_registry column and child table.
// BuildProjection maps the whole PRState row; ProjectionMetadata is only the
// in-memory snapshot of the non-column state those typed columns and child
// tables derive from (it also feeds the ProjectionJournalKey digest, whose
// encoding is unchanged so recorded ProjectedKey values stay valid).
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
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ProjectionMetadata is the in-memory snapshot of the per-PR state the typed
// scalar columns and child tables derive from. It is never stored as JSON;
// its JSON encoding only feeds the ProjectionJournalKey digest, so the tags
// and omitempty markers must stay as they are.
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

// BuildProjection maps committed PRState to a full registry row write. It
// reads only the state it is given and never consults the projection, so
// the projection can never feed back into a transition decision. The agent
// column always carries the stored owner.
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

// digestJSON encodes the snapshot state for the ProjectionJournalKey digest.
// Every field type marshals infallibly (strings, ints, bools, string maps and
// slices, plain structs), so a failure is impossible; it still degrades to
// an empty object.
func digestJSON(m ProjectionMetadata) []byte {
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
		snap.Generation, snap.MetaEpoch, digestJSON(normalizeMetadata(snap.Metadata)))
	sum := sha256.Sum256([]byte(sb.String()))
	return fmt.Sprintf("projection:%s:%s:%d@%s:gen%d:epoch%d:%x",
		agent, NormalizeRepo(snap.Repo), snap.PRNumber,
		strings.TrimSpace(snap.HeadSHA), snap.Generation, snap.MetaEpoch,
		sum[:8])
}

// projectionUpsertSQL writes every column except the legacy metadata JSON,
// which it never touches (an insert takes the column default). A blank text
// column never overwrites a stored value (state that has not learned a field
// yet cannot erase it); status and agent always follow state. The typed
// scalar columns ($10-$34) always follow state.
const projectionUpsertSQL = `INSERT INTO pr_registry (repo, pr_number, agent, branch, head_sha, merge_sha, target_id, title, status, ` +
	`vanadis_chunk_id, bot_id, ci_fail_count, deploy_failed_published, deploy_success_pending, banner, widget_state, ` +
	`build_version, build_kind, build_repository, build_workflow_path, build_run_id, build_run_number, build_run_attempt, ` +
	`build_event, build_source_sha, build_execution_sha, build_pr_number, build_service, build_image_name, build_state, ` +
	`build_pushed, build_source_attempt, build_image_refs, build_digest, created_at, updated_at) ` +
	`VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, $9, ` +
	`$10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33::text[], $34, ` +
	`CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ` +
	`ON CONFLICT (repo, pr_number) DO UPDATE SET ` +
	`agent = EXCLUDED.agent, ` +
	`branch = CASE WHEN EXCLUDED.branch != '' THEN EXCLUDED.branch ELSE pr_registry.branch END, ` +
	`head_sha = CASE WHEN EXCLUDED.head_sha != '' THEN EXCLUDED.head_sha ELSE pr_registry.head_sha END, ` +
	`merge_sha = COALESCE(EXCLUDED.merge_sha, pr_registry.merge_sha), ` +
	`target_id = CASE WHEN EXCLUDED.target_id != '' THEN EXCLUDED.target_id ELSE pr_registry.target_id END, ` +
	`title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END, ` +
	`status = EXCLUDED.status, ` +
	`vanadis_chunk_id = EXCLUDED.vanadis_chunk_id, bot_id = EXCLUDED.bot_id, ci_fail_count = EXCLUDED.ci_fail_count, ` +
	`deploy_failed_published = EXCLUDED.deploy_failed_published, deploy_success_pending = EXCLUDED.deploy_success_pending, ` +
	`banner = EXCLUDED.banner, widget_state = EXCLUDED.widget_state, ` +
	`build_version = EXCLUDED.build_version, build_kind = EXCLUDED.build_kind, build_repository = EXCLUDED.build_repository, ` +
	`build_workflow_path = EXCLUDED.build_workflow_path, build_run_id = EXCLUDED.build_run_id, ` +
	`build_run_number = EXCLUDED.build_run_number, build_run_attempt = EXCLUDED.build_run_attempt, ` +
	`build_event = EXCLUDED.build_event, build_source_sha = EXCLUDED.build_source_sha, ` +
	`build_execution_sha = EXCLUDED.build_execution_sha, build_pr_number = EXCLUDED.build_pr_number, ` +
	`build_service = EXCLUDED.build_service, build_image_name = EXCLUDED.build_image_name, ` +
	`build_state = EXCLUDED.build_state, build_pushed = EXCLUDED.build_pushed, ` +
	`build_source_attempt = EXCLUDED.build_source_attempt, build_image_refs = EXCLUDED.build_image_refs, ` +
	`build_digest = EXCLUDED.build_digest, ` +
	`updated_at = CURRENT_TIMESTAMP;`

// ProjectionUpsertQuery builds the full-row pr_registry upsert for snap:
// the row columns and the typed scalar columns, never metadata.
func ProjectionUpsertQuery(snap ProjectionSnapshot) (string, []any) {
	agent := strings.TrimSpace(snap.Agent)
	if agent == "" {
		agent = DefaultAgent
	}
	meta := normalizeMetadata(snap.Metadata)
	args := make([]any, 0, 34)
	args = append(args,
		NormalizeRepo(snap.Repo), snap.PRNumber,
		agent,
		strings.TrimSpace(snap.Branch), strings.TrimSpace(snap.HeadSHA),
		strings.TrimSpace(snap.MergeSHA), strings.TrimSpace(snap.TargetID),
		strings.TrimSpace(snap.Title), strings.TrimSpace(snap.Status),
		meta.VanadisChunkID, meta.BotID, meta.CIFailCount,
		meta.DeployFailedPublished, meta.DeploySuccessPending,
		meta.Banner, meta.WidgetState,
	)
	return projectionUpsertSQL, append(args, buildColumnArgs(meta.BuildMetadata)...)
}

// buildColumnArgs maps the build-metadata snapshot to the build_* columns
// ($17-$34) with the typed backfill's semantics: no
// snapshot projects all NULL; a field whose JSON tag is omitempty projects
// NULL at its zero value (the key is absent from the blob); every other
// field projects its value.
func buildColumnArgs(m *BuildMetadata) []any {
	if m == nil {
		return make([]any, 18)
	}
	var refs []string
	if len(m.ImageRefs) > 0 {
		refs = slices.Clone(m.ImageRefs)
	}
	return []any{
		m.Version, m.Kind, m.Repository, m.WorkflowPath, m.RunID, m.RunNumber, m.RunAttempt,
		m.Event, m.SourceSHA, m.ExecutionSHA, m.PRNumber,
		nonZero(m.Service), nonZero(m.ImageName), nonZero(m.State), nonZero(m.Pushed),
		nonZero(m.SourceAttempt), refs, nonZero(m.Digest),
	}
}

// nonZero returns nil for a zero value and a pointer to v otherwise.
func nonZero[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}

// ProjectionStatement is one SQL statement of a projection transaction.
type ProjectionStatement struct {
	SQL  string
	Args []any
}

// Child-table sync statements. Each table gets one delete of the entries the
// snapshot no longer holds and one upsert that skips unchanged entries, the
// same full sync the typed backfill migration ran. Arrays
// are always non-nil so an empty set deletes every entry instead of binding
// NULL. Checks take the row head_sha the upsert just wrote.
const (
	projectionChecksDeleteSQL = `DELETE FROM pr_registry_checks WHERE repo = $1 AND pr_number = $2 AND NOT (check_name = ANY($3::text[]))`
	projectionChecksUpsertSQL = `INSERT INTO pr_registry_checks (repo, pr_number, check_name, head_sha, state) ` +
		`SELECT p.repo, p.pr_number, c.check_name, p.head_sha, c.state FROM pr_registry AS p, unnest($3::text[], $4::text[]) AS c(check_name, state) ` +
		`WHERE p.repo = $1 AND p.pr_number = $2 ` +
		`ON CONFLICT (repo, pr_number, check_name) DO UPDATE SET head_sha = EXCLUDED.head_sha, state = EXCLUDED.state ` +
		`WHERE (pr_registry_checks.head_sha, pr_registry_checks.state) IS DISTINCT FROM (EXCLUDED.head_sha, EXCLUDED.state)`
	projectionJobsDeleteSQL = `DELETE FROM pr_registry_jobs WHERE repo = $1 AND pr_number = $2 AND NOT (job_name = ANY($3::text[]))`
	projectionJobsUpsertSQL = `INSERT INTO pr_registry_jobs (repo, pr_number, job_name, expected, started, deployed, state) ` +
		`SELECT $1, $2, j.job_name, j.expected, j.started, j.deployed, j.state ` +
		`FROM unnest($3::text[], $4::boolean[], $5::boolean[], $6::boolean[], $7::text[]) AS j(job_name, expected, started, deployed, state) ` +
		`ON CONFLICT (repo, pr_number, job_name) DO UPDATE SET expected = EXCLUDED.expected, started = EXCLUDED.started, ` +
		`deployed = EXCLUDED.deployed, state = EXCLUDED.state ` +
		`WHERE (pr_registry_jobs.expected, pr_registry_jobs.started, pr_registry_jobs.deployed, pr_registry_jobs.state) ` +
		`IS DISTINCT FROM (EXCLUDED.expected, EXCLUDED.started, EXCLUDED.deployed, EXCLUDED.state)`
	projectionKustDeleteSQL = `DELETE FROM pr_registry_kustomizations WHERE repo = $1 AND pr_number = $2 AND NOT (name = ANY($3::text[]))`
	projectionKustUpsertSQL = `INSERT INTO pr_registry_kustomizations (repo, pr_number, name, namespace, requested_at, deployed) ` +
		`SELECT $1, $2, k.name, k.namespace, NULLIF(k.requested_at, '')::timestamptz, k.deployed ` +
		`FROM unnest($3::text[], $4::text[], $5::text[], $6::boolean[]) AS k(name, namespace, requested_at, deployed) ` +
		`ON CONFLICT (repo, pr_number, name) DO UPDATE SET namespace = EXCLUDED.namespace, ` +
		`requested_at = EXCLUDED.requested_at, deployed = EXCLUDED.deployed ` +
		`WHERE (pr_registry_kustomizations.namespace, pr_registry_kustomizations.requested_at, pr_registry_kustomizations.deployed) ` +
		`IS DISTINCT FROM (EXCLUDED.namespace, EXCLUDED.requested_at, EXCLUDED.deployed)`
	projectionSelectionsDeleteSQL = `DELETE FROM pr_registry_build_selections AS t WHERE t.repo = $1 AND t.pr_number = $2 ` +
		`AND NOT EXISTS (SELECT 1 FROM unnest($3::text[], $4::text[]) AS k(service, image_name) WHERE k.service = t.service AND k.image_name = t.image_name)`
	projectionSelectionsUpsertSQL = `INSERT INTO pr_registry_build_selections (repo, pr_number, service, image_name, position) ` +
		`SELECT $1, $2, s.service, s.image_name, s.position FROM unnest($3::text[], $4::text[], $5::integer[]) AS s(service, image_name, position) ` +
		`ON CONFLICT (repo, pr_number, service, image_name) DO UPDATE SET position = EXCLUDED.position ` +
		`WHERE pr_registry_build_selections.position IS DISTINCT FROM EXCLUDED.position`
	projectionResultsDeleteSQL = `DELETE FROM pr_registry_build_results AS t WHERE t.repo = $1 AND t.pr_number = $2 ` +
		`AND NOT EXISTS (SELECT 1 FROM unnest($3::text[], $4::text[]) AS k(service, image_name) WHERE k.service = t.service AND k.image_name = t.image_name)`
	projectionResultUpsertSQL = `INSERT INTO pr_registry_build_results (repo, pr_number, service, image_name, position, state, pushed, source_attempt, image_refs, digest) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::text[], $10) ` +
		`ON CONFLICT (repo, pr_number, service, image_name) DO UPDATE SET position = EXCLUDED.position, state = EXCLUDED.state, ` +
		`pushed = EXCLUDED.pushed, source_attempt = EXCLUDED.source_attempt, image_refs = EXCLUDED.image_refs, digest = EXCLUDED.digest ` +
		`WHERE (pr_registry_build_results.position, pr_registry_build_results.state, pr_registry_build_results.pushed, ` +
		`pr_registry_build_results.source_attempt, pr_registry_build_results.image_refs, pr_registry_build_results.digest) ` +
		`IS DISTINCT FROM (EXCLUDED.position, EXCLUDED.state, EXCLUDED.pushed, EXCLUDED.source_attempt, EXCLUDED.image_refs, EXCLUDED.digest)`
)

// ProjectionStatements returns the ordered statements of one projection
// transaction: the row upsert (columns and typed scalars) followed by
// the child-table syncs for checks, jobs, kustomizations, build selections
// and build results. Result rows carry a string list each, so they upsert
// one statement per result. Every value derives from the snapshot.
func ProjectionStatements(snap ProjectionSnapshot) []ProjectionStatement {
	query, args := ProjectionUpsertQuery(snap)
	repo, pr := NormalizeRepo(snap.Repo), snap.PRNumber
	meta := normalizeMetadata(snap.Metadata)
	checkNames := sortedKeys(meta.CIChecks)
	checkStates := make([]string, 0, len(checkNames))
	for _, name := range checkNames {
		checkStates = append(checkStates, meta.CIChecks[name])
	}
	jobs := projectionJobs(meta)
	kust := projectionKustomizations(meta)
	sel, res := projectionBuildEntries(meta.BuildMetadata)
	out := make([]ProjectionStatement, 0, 10+len(res.rows))
	out = append(out, []ProjectionStatement{
		{SQL: query, Args: args},
		{SQL: projectionChecksDeleteSQL, Args: []any{repo, pr, checkNames}},
		{SQL: projectionChecksUpsertSQL, Args: []any{repo, pr, checkNames, checkStates}},
		{SQL: projectionJobsDeleteSQL, Args: []any{repo, pr, jobs.names}},
		{SQL: projectionJobsUpsertSQL, Args: []any{repo, pr, jobs.names, jobs.expected, jobs.started, jobs.deployed, jobs.states}},
		{SQL: projectionKustDeleteSQL, Args: []any{repo, pr, kust.names}},
		{SQL: projectionKustUpsertSQL, Args: []any{repo, pr, kust.names, kust.namespaces, kust.requestedAt, kust.deployed}},
		{SQL: projectionSelectionsDeleteSQL, Args: []any{repo, pr, sel.services, sel.images}},
		{SQL: projectionSelectionsUpsertSQL, Args: []any{repo, pr, sel.services, sel.images, sel.positions}},
		{SQL: projectionResultsDeleteSQL, Args: []any{repo, pr, res.services, res.images}},
	}...)
	for _, r := range res.rows {
		out = append(out, ProjectionStatement{SQL: projectionResultUpsertSQL, Args: []any{
			repo, pr, r.Service, r.ImageName, r.position, r.State, r.Pushed, r.SourceAttempt, r.refs, r.Digest,
		}})
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

type jobColumns struct {
	names                       []string
	expected, started, deployed []bool
	states                      []string
}

// projectionJobs is the union of jobs, started_jobs, deployed_jobs and
// job_states keys, sorted, with one flag per list and the job state.
func projectionJobs(m ProjectionMetadata) jobColumns {
	names := sortedKeys(m.JobStates)
	names = unionStrings(names, m.Jobs...)
	names = unionStrings(names, m.StartedJobs...)
	names = unionStrings(names, m.DeployedJobs...)
	out := jobColumns{names: make([]string, 0, len(names)), states: make([]string, 0, len(names))}
	for _, name := range names {
		out.names = append(out.names, name)
		out.expected = append(out.expected, slices.Contains(m.Jobs, name))
		out.started = append(out.started, slices.Contains(m.StartedJobs, name))
		out.deployed = append(out.deployed, slices.Contains(m.DeployedJobs, name))
		out.states = append(out.states, m.JobStates[name])
	}
	if out.expected == nil {
		out.expected, out.started, out.deployed = []bool{}, []bool{}, []bool{}
	}
	return out
}

type kustColumns struct {
	names, namespaces, requestedAt []string
	deployed                       []bool
}

// projectionKustomizations is the union of kustomization_requests keys and
// deployed_kustomizations, sorted. requested_at binds as text the database
// casts, so it parses exactly like the backfill; a value that is not
// RFC 3339 binds empty (NULL) instead of failing the whole projection.
func projectionKustomizations(m ProjectionMetadata) kustColumns {
	names := unionStrings(sortedKeys(m.KustomizationRequests), m.DeployedKustomizations...)
	out := kustColumns{
		names: make([]string, 0, len(names)), namespaces: make([]string, 0, len(names)),
		requestedAt: make([]string, 0, len(names)), deployed: make([]bool, 0, len(names)),
	}
	for _, name := range names {
		req := m.KustomizationRequests[name]
		at := req.RequestedAt
		if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
			at = ""
		}
		out.names = append(out.names, name)
		out.namespaces = append(out.namespaces, req.Namespace)
		out.requestedAt = append(out.requestedAt, at)
		out.deployed = append(out.deployed, slices.Contains(m.DeployedKustomizations, name))
	}
	return out
}

type buildKeyColumns struct {
	services, images []string
	positions        []int
}

type projectedResult struct {
	BuildResult

	position int
	refs     []string
}

type buildResultColumns struct {
	services, images []string
	rows             []projectedResult
}

// projectionBuildEntries maps build_metadata.selected and results to child
// rows keyed by (service, image_name) with their 1-based array position; a
// repeated key keeps its first position, like the backfill's DISTINCT ON.
func projectionBuildEntries(m *BuildMetadata) (buildKeyColumns, buildResultColumns) {
	sel := buildKeyColumns{services: []string{}, images: []string{}, positions: []int{}}
	res := buildResultColumns{services: []string{}, images: []string{}}
	if m == nil {
		return sel, res
	}
	seen := map[BuildSelection]bool{}
	for i, s := range m.Selected {
		if seen[s] {
			continue
		}
		seen[s] = true
		sel.services, sel.images, sel.positions = append(sel.services, s.Service), append(sel.images, s.ImageName), append(sel.positions, i+1)
	}
	seen = map[BuildSelection]bool{}
	for i, r := range m.Results {
		key := BuildSelection{Service: r.Service, ImageName: r.ImageName}
		if seen[key] {
			continue
		}
		seen[key] = true
		refs := append([]string{}, r.ImageRefs...)
		res.services, res.images = append(res.services, r.Service), append(res.images, r.ImageName)
		res.rows = append(res.rows, projectedResult{BuildResult: r, position: i + 1, refs: refs})
	}
	return sel, res
}

// ProjectionStore is the pr_registry write boundary.
type ProjectionStore interface {
	UpsertProjection(ctx context.Context, snap ProjectionSnapshot) error
}

// ProjectionDB is the pgx surface the Postgres projection store needs (it
// opens one transaction per projection);
// *pgxpool.Pool satisfies it.
type ProjectionDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PgProjectionStore is the production ProjectionStore over vanadis-restate's existing Postgres pool.
type PgProjectionStore struct {
	db ProjectionDB
}

// NewPgProjectionStore returns a projection store over db.
func NewPgProjectionStore(db ProjectionDB) *PgProjectionStore {
	return &PgProjectionStore{db: db}
}

// UpsertProjection writes the row, the typed columns and the child tables in
// one transaction, so a reader never sees a half-written projection and a
// failure leaves the previous row intact.
func (p *PgProjectionStore) UpsertProjection(ctx context.Context, snap ProjectionSnapshot) error {
	if p == nil || p.db == nil {
		return errors.New("projection database is nil")
	}
	err := pgx.BeginFunc(ctx, p.db, func(tx pgx.Tx) error {
		for _, stmt := range ProjectionStatements(snap) {
			if _, err := tx.Exec(ctx, stmt.SQL, stmt.Args...); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
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
