package prstate

// Journaled pr_registry projection. Restate K/V in PRObject/RepoObject is the
// ONLY writer/authority; Postgres pr_registry is a replayable read-only
// projection plus SHA lookup index. Routing may use the projection; authority
// may not.
//
// Ownership ruling: projection-key ownership of pr_registry transfers to the
// prstate service under a per-key field-ownership partition. This file owns
// ONLY the keys in OwnedMetadataKeys plus the head_sha/merge_sha/status
// columns; every other metadata key (jobs, started_jobs, deployed_jobs,
// job_states, kustomization_requests, deployed_kustomizations,
// deploy_failed_published, deploy_success_pending, build_metadata) belongs to
// later chunks or other writers, and the upsert never overwrites them.
// Synapse registry code is read-only in this chunk: the SQL below mirrors its
// metadata-blob idioms (jsonb_set per key, banner removal, ON CONFLICT
// (repo, pr_number)) without importing it.
//
// Ordering contract: ProjectRegistry runs as a journaled send AFTER the
// Restate state commit. Projection failure never discards or gates the
// transition; the caller keeps the committed state and retries the send.
// Replay returns the recorded result and never re-fires the row write twice
// observably (the upsert is idempotent per ProjectionJournalKey).
// RebuildProjection enumerates only the RepoObject touched-PR index, never
// the projection being rebuilt, so rebuild is non-circular; snapshot restore
// remains the fallback.
//
// Live-send ordering: sends for one PR are totally ordered by the PRObject's
// single-threaded journal — a retried unrecorded send completes before any
// later invocation's send, and recorded sends never re-fire, so no stale
// live send can land last. Rebuild replays current state through this same
// send path and must run quiesced against live traffic; the cutover re-derives
// the snapshot from current committed state at send time rather than
// forwarding event-captured snapshots. The journal key below only ever
// distinguishes content for dedup; it never decides a transition.
//
// External idempotency tolerance: the registry upsert has no external
// idempotency-key parameter, so at-least-once delivery is accepted here and
// absorbed by the idempotent upsert (same journal key replays to the same
// row). Later chunks pass the journaled EffectKey as the external
// idempotency key wherever a Discord/GitHub API supports one, and state the
// accepted at-least-once tolerance per effect where it does not.
//
// Dark only: no caller entrypoint wires this in; the cutover chunk does.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// ProjectionKind is the journaled effect kind for registry projection sends.
const ProjectionKind = "projection"

// ProjectionStatusOpen is the default registry status for a tracked PR.
const ProjectionStatusOpen = "open"

// ProjectionSnapshot is one journaled row write derived from committed
// Restate state. Metadata carries ONLY owned keys.
type ProjectionSnapshot struct {
	Repo       string
	PRNumber   int
	Agent      string
	Branch     string
	HeadSHA    string
	MergeSHA   string
	Title      string
	TargetID   string
	Status     string
	Generation uint64
	MetaEpoch  uint64
	Metadata   map[string]any
}

// OwnedMetadataKeys returns the metadata keys prstate may write. Every other
// key in the blob belongs to another writer and is never touched.
func OwnedMetadataKeys() []string {
	return []string{"ci_checks"}
}

// IsOwnedMetadataKey reports whether key is in the prstate-owned partition.
func IsOwnedMetadataKey(key string) bool {
	return slices.Contains(OwnedMetadataKeys(), key)
}

// ProjectionStatus derives the registry status column purely from committed
// Restate state. Every failure signal is scoped to the current head SHA:
// stale markers recorded for a superseded SHA never project failure onto a
// head that never failed. Terminal states win; conflict beats ci_failed;
// deploy and sync lifecycle states belong to later chunks and are never
// invented here.
func ProjectionStatus(s *PRState) string {
	if s == nil {
		return ProjectionStatusOpen
	}
	switch s.Terminal {
	case TerminalMerged:
		return "merged"
	case TerminalClosed:
		return "closed"
	case TerminalNone:
	default:
	}
	if s.WasConflictPublished(s.HeadSHA) {
		return "conflict"
	}
	if s.HasFailedCISHA(s.HeadSHA) || s.HasActiveFailedCheckRuns(s.HeadSHA) {
		return "ci_failed"
	}
	return ProjectionStatusOpen
}

// BuildProjection maps committed PRState to a registry row write. It reads
// only the state it is given and never consults the projection, so the
// projection can never feed back into a transition decision.
func BuildProjection(s *PRState, branch, title, targetID, mergeSHA string) ProjectionSnapshot {
	agent := DefaultAgent
	if s != nil && strings.TrimSpace(s.Agent) != "" {
		agent = strings.TrimSpace(s.Agent)
	}
	snap := ProjectionSnapshot{
		Agent:    agent,
		Branch:   strings.TrimSpace(branch),
		Title:    strings.TrimSpace(title),
		TargetID: strings.TrimSpace(targetID),
		MergeSHA: strings.TrimSpace(mergeSHA),
		Status:   ProjectionStatusOpen,
		Metadata: map[string]any{},
	}
	if s == nil {
		return snap
	}
	snap.Repo = NormalizeRepo(s.Repo)
	snap.PRNumber = s.PRNumber
	snap.HeadSHA = strings.TrimSpace(s.HeadSHA)
	snap.Generation = s.GateGeneration
	snap.MetaEpoch = s.ReopenEpoch
	snap.Status = ProjectionStatus(s)
	checks := map[string]any{}
	if head := strings.TrimSpace(s.HeadSHA); head != "" && !IsZeroSHA(head) &&
		strings.EqualFold(strings.TrimSpace(s.Checks.HeadSHA), head) {
		for name := range s.Checks.ActiveFailed {
			checks[name] = "failed"
		}
		if s.Checks.RoboRevActiveFailed {
			checks["roborev"] = "failed"
		}
	}
	snap.Metadata["ci_checks"] = checks
	return snap
}

// ProjectionJournalKey scopes one projection send to its exact content:
// repo+PR+headSHA+generation+reopen epoch plus a digest of every projected
// field. Status, checks, and titles change many times within one generation
// on one head SHA, so a generation-scoped key alone would suppress a newer
// projection as a replay of an older one. Retries of the identical snapshot
// reuse the key and dedupe; any content or epoch change mints a new key.
// The digest input is length-delimited by NUL separators (mirroring
// EffectKey.String's Builder pattern), so no marshaling error path exists.
func ProjectionJournalKey(snap ProjectionSnapshot) string {
	meta := snap.Metadata
	if meta == nil {
		meta = map[string]any{}
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		metaBytes = []byte("{}")
	}
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
		snap.Generation, snap.MetaEpoch, metaBytes)
	sum := sha256.Sum256([]byte(sb.String()))
	return fmt.Sprintf("projection:%s:%s:%d@%s:gen%d:epoch%d:%x",
		agent, NormalizeRepo(snap.Repo), snap.PRNumber,
		strings.TrimSpace(snap.HeadSHA), snap.Generation, snap.MetaEpoch,
		sum[:8])
}

// ownedChecksMerge builds the per-key merge expression for the owned
// ci_checks key over base: banner removed, a missing key defaulting to an
// empty object so the merge can never null the blob.
func ownedChecksMerge(base string) string {
	return `jsonb_set(` + base + ` - 'banner', '{ci_checks}', COALESCE($10::jsonb->'ci_checks', '{}'::jsonb), true)`
}

// ProjectionUpsertQuery builds the real pr_registry upsert: column writes for
// branch/head_sha/merge_sha/target_id/title/status plus a per-key
// jsonb_set merge for owned metadata keys in BOTH the insert and the
// conflict branch, so a first-time projection lands ci_checks instead of an
// empty blob. Unowned keys are never mentioned, so they survive the merge
// untouched.
func ProjectionUpsertQuery(snap ProjectionSnapshot) (string, []any) {
	meta := snap.Metadata
	if meta == nil {
		meta = map[string]any{}
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		metaBytes = []byte("{}")
	}
	agent := strings.TrimSpace(snap.Agent)
	if agent == "" {
		agent = DefaultAgent
	}
	query := `INSERT INTO pr_registry (repo, pr_number, agent, branch, head_sha, merge_sha, target_id, title, status, metadata, created_at, updated_at) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, ` + ownedChecksMerge(`'{}'::jsonb`) + `, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ` +
		`ON CONFLICT (repo, pr_number) DO UPDATE SET ` +
		`agent = CASE WHEN EXCLUDED.agent != '' THEN EXCLUDED.agent ELSE pr_registry.agent END, ` +
		`branch = CASE WHEN EXCLUDED.branch != '' THEN EXCLUDED.branch ELSE pr_registry.branch END, ` +
		`head_sha = CASE WHEN EXCLUDED.head_sha != '' THEN EXCLUDED.head_sha ELSE pr_registry.head_sha END, ` +
		`merge_sha = CASE WHEN EXCLUDED.merge_sha != '' THEN EXCLUDED.merge_sha ELSE pr_registry.merge_sha END, ` +
		`target_id = CASE WHEN EXCLUDED.target_id != '' THEN EXCLUDED.target_id ELSE pr_registry.target_id END, ` +
		`title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END, ` +
		`status = EXCLUDED.status, ` +
		`metadata = ` + ownedChecksMerge(`COALESCE(metadata, '{}'::jsonb)`) + `, ` +
		`updated_at = CURRENT_TIMESTAMP;`
	args := []any{
		NormalizeRepo(snap.Repo), snap.PRNumber,
		agent,
		strings.TrimSpace(snap.Branch), strings.TrimSpace(snap.HeadSHA),
		strings.TrimSpace(snap.MergeSHA), strings.TrimSpace(snap.TargetID),
		strings.TrimSpace(snap.Title), strings.TrimSpace(snap.Status),
		string(metaBytes),
	}
	return query, args
}

// ProjectionStore is the pr_registry write boundary. Production runs the
// ProjectionUpsertQuery SQL; tests use FakeProjectionStore. There is no read
// method by design: the projection never consults the registry.
type ProjectionStore interface {
	UpsertProjection(ctx context.Context, snap ProjectionSnapshot) error
}

// ProjectRegistry is the journaled projection send invoked AFTER the
// Restate state commit. It validates the snapshot and performs exactly one
// store write; on store failure it returns the error and the caller keeps
// the already-committed state for a later retry under the same journal key.
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

// SnapshotLoader resolves one touched PR key to its projection snapshot.
// Implementations load from Restate object state, never from the projection.
type SnapshotLoader interface {
	LoadPR(prKey string) (ProjectionSnapshot, bool)
}

// RebuildProjection replays the full projection by enumerating the
// RepoObject touched-PR index and projecting each key via loader. Keys the
// loader cannot resolve skip silently; a store failure aborts the rebuild
// with the committed object state untouched.
func RebuildProjection(ctx context.Context, store ProjectionStore, repo *RepoState, loader SnapshotLoader) error {
	if store == nil {
		return errors.New("projection store is nil")
	}
	if repo == nil || loader == nil {
		return errors.New("rebuild requires repo state and snapshot loader")
	}
	seen := make(map[string]bool)
	for _, key := range repo.PRKeys() {
		if strings.TrimSpace(key) == "" || seen[key] {
			continue
		}
		seen[key] = true
		snap, ok := loader.LoadPR(key)
		if !ok {
			continue
		}
		if err := ProjectRegistry(ctx, store, snap); err != nil {
			return err
		}
	}
	return nil
}

// FakeProjectionStore is the hermetic ProjectionStore fake: it records
// snapshots on success and fails closed with Err without recording.
type FakeProjectionStore struct {
	mu       sync.Mutex
	Err      error
	Recorded []ProjectionSnapshot
}

// Compile-time freeze: the fake must satisfy the store interface.
var _ ProjectionStore = (*FakeProjectionStore)(nil)

// UpsertProjection records the snapshot or returns the configured error.
func (f *FakeProjectionStore) UpsertProjection(_ context.Context, snap ProjectionSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	f.Recorded = append(f.Recorded, snap)
	return nil
}

// RecordedCount returns the number of recorded projection writes.
func (f *FakeProjectionStore) RecordedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Recorded)
}

// MapSnapshotLoader is the hermetic SnapshotLoader fake backed by a map.
type MapSnapshotLoader struct {
	Snaps map[string]ProjectionSnapshot
}

// Compile-time freeze: the fake must satisfy the loader interface.
var _ SnapshotLoader = MapSnapshotLoader{}

// LoadPR resolves one PR key from the map.
func (m MapSnapshotLoader) LoadPR(prKey string) (ProjectionSnapshot, bool) {
	snap, ok := m.Snaps[prKey]
	return snap, ok
}
