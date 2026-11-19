package store

// PR lifecycle persistence for vanadis-restate.
//
// CompletePRDeployment, UpdatePRLifecycleStatus, RecordPRCheckStatus,
// ResetPRCheckState and IncrementChunkCIFailCount moved here from the root
// package so they promote through the embedded *Store. SQL text, check-state
// JSON, status transition rules and error behavior are byte-identical to the
// root copies they replace.
//
// Recovery helpers (recoverFailedPR, failedPRHeadAccepted,
// missingFailedPRBaseline, seedFailedPRBaseline) now live in
// store_pr_recovery.go; the shared lock helper below stays here alongside
// its retry-module consumer. ResetPRCheckState depends on the recovery
// fallback, which is its only caller, and the lock helper it needs.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// PR lifecycle status vocabulary mirrored from the root package.
// String-compared only, so these local copies preserve behavior exactly;
// the facade-removal chunk unifies them.
const (
	statusPROpened = "pr_opened"
	statusCIFailed = "ci_failed"
	statusConflict = "conflict"
)

func (s *Store) CompletePRDeployment(ctx context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk_id cannot be empty")
	}
	var evidence any
	if rolloutState != nil {
		data, err := json.Marshal(rolloutState)
		if err != nil {
			return fmt.Errorf("marshal rollout state: %w", err)
		}
		evidence = data
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks AS c SET status = 'completed', banner = '', rollout_state = COALESCE($1::jsonb, c.rollout_state), completed_at = COALESCE(c.completed_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds AS b WHERE c.id = $2 AND b.id = c.build_id AND (c.mode = 'pr' OR (c.mode = 'action' AND COALESCE(c.pr_number, 0) > 0)) AND c.status NOT IN ('failed', 'cancelled') AND b.status != 'cancelled' AND (b.status != 'failed' OR c.status = 'completed');`, evidence, chunkID)
	if err != nil {
		return fmt.Errorf("complete pr deployment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrPRTransitionSkipped
	}
	return nil
}

// SweptChunk identifies one cross-build swept chunk and the foreign build
// that owns it.
type SweptChunk struct {
	ChunkID string
	BuildID string
}

// buildPushJobRegex mirrors the widget Build-and-Push check-name parser so
// the store can derive a deploy job from CI checks without importing the
// widget package (which imports the store).
var buildPushJobRegex = regexp.MustCompile(`(?i)Build\s*(?:&|and)\s*(?:Push|Publish)(?:\s+(?:Sidecar\s+)?(?:Container\s+)?Images?(?:\s+to\s+GHCR)?)?\s*\(([^)]+)\)`)

// parseBuildPushServices extracts Build-and-Push job services from CI check
// names, lowercased for case-insensitive intersection.
func parseBuildPushServices(checks map[string]string) map[string]struct{} {
	services := make(map[string]struct{})
	for name := range checks {
		m := buildPushJobRegex.FindStringSubmatch(name)
		if len(m) < 2 {
			continue
		}
		if svc := strings.ToLower(strings.TrimSpace(m[1])); svc != "" && !strings.Contains(svc, "${{") {
			services[svc] = struct{}{}
		}
	}
	return services
}

// deployJobSet returns the provable deploy jobs for a chunk: the rollout
// ExpectedJobs when present, otherwise the Build-and-Push services parsed
// from CI checks. An empty set means no shared job can be proven.
func deployJobSet(c model.VanadisChunk) map[string]struct{} {
	if c.RolloutState != nil {
		jobs := make(map[string]struct{})
		for _, job := range c.RolloutState.ExpectedJobs {
			if clean := strings.ToLower(strings.TrimSpace(job)); clean != "" {
				jobs[clean] = struct{}{}
			}
		}
		if len(jobs) > 0 {
			return jobs
		}
	}
	return parseBuildPushServices(c.CIChecks)
}

// sharedDeployJob reports whether both chunks provably deploy at least one
// common job. Deployment rolls out per job, so repo-only overlap sweeps
// nothing: either side without a derivable job set blocks the sweep.
func sharedDeployJob(a, b model.VanadisChunk) bool {
	aJobs, bJobs := deployJobSet(a), deployJobSet(b)
	if len(aJobs) == 0 || len(bJobs) == 0 {
		return false
	}
	for job := range aJobs {
		if _, ok := bJobs[job]; ok {
			return true
		}
	}
	return false
}

// sweepPreDeployStatus reports whether a chunk is still pre-deploy (merged,
// synced, or deploying). Terminal and never-merged chunks are never swept.
func sweepPreDeployStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusMerged, "synced", "deploying":
		return true
	default:
		return false
	}
}

// sweepStampProvesInclusion reports whether the candidate's merge stamp
// proves it landed no later than the deploying chunk's landed commit, with
// stamp ties broken by lower chunk_index. Either stamp unknown means
// inclusion is unprovable.
func sweepStampProvesInclusion(candidate, deploying model.VanadisChunk) bool {
	if candidate.CompletedAt.IsZero() || deploying.CompletedAt.IsZero() {
		return false
	}
	if candidate.CompletedAt.After(deploying.CompletedAt) {
		return false
	}
	if candidate.CompletedAt.Equal(deploying.CompletedAt) && candidate.ChunkIndex >= deploying.ChunkIndex {
		return false
	}
	return true
}

// sweepStampStrictlyBefore reports whether the candidate's merge stamp
// proves it landed strictly before the deploying chunk's landed commit.
// ChunkIndex is scoped within its own build and cannot order merges across
// independent builds, so cross-build inclusion never uses the same-build
// stamp tie-break: a same-tick foreign merge may have landed after the
// deploying commit and must not complete.
func sweepStampStrictlyBefore(candidate, deploying model.VanadisChunk) bool {
	if candidate.CompletedAt.IsZero() || deploying.CompletedAt.IsZero() {
		return false
	}
	return candidate.CompletedAt.Before(deploying.CompletedAt)
}

// crossBuildSweepEligible checks the gates shared by sweep and recovery:
// another build, PR mode with a number, same repo, proven merge inclusion,
// and a shared deploy job. Status gating stays with the callers.
func crossBuildSweepEligible(candidate, deploying model.VanadisChunk) bool {
	if candidate.BuildID == "" || candidate.BuildID == deploying.BuildID {
		return false
	}
	if candidate.Mode != "pr" || candidate.PRNumber <= 0 {
		return false
	}
	return sameDeployRepo(candidate.Repo, deploying.Repo) && sweepStampStrictlyBefore(candidate, deploying) && sharedDeployJob(candidate, deploying)
}

// supersededAcrossBuilds reports whether candidate in another build merged
// early enough and on a shared job to be contained in the deploying chunk's
// landed commit.
func supersededAcrossBuilds(candidate, deploying model.VanadisChunk) bool {
	return crossBuildSweepEligible(candidate, deploying) && sweepPreDeployStatus(candidate.Status)
}

// recoveredAcrossBuilds reports whether an already-completed chunk in
// another build matches the cross-build sweep eligibility. A retry that
// lands after a prior attempt completed the writes but failed before the
// side effects re-derives these so events, widget sync, and build
// completion still happen.
func recoveredAcrossBuilds(candidate, deploying model.VanadisChunk) bool {
	return crossBuildSweepEligible(candidate, deploying) && strings.EqualFold(strings.TrimSpace(candidate.Status), statusCompleted)
}

// CrossBuildDeploymentSweepIDs returns the earlier chunks in OTHER builds a
// deploy_success also completes: same-repo PR chunks still pre-deploy whose
// merge stamp is at or before the deploying chunk's merge stamp and that
// share at least one deploy job with the deploying chunk. Terminal and
// never-merged chunks, same-build chunks, and job-disjoint chunks are never
// swept.
func CrossBuildDeploymentSweepIDs(deploying model.VanadisChunk, candidates []model.VanadisChunk) []SweptChunk {
	if strings.TrimSpace(deploying.BuildID) == "" {
		return nil
	}
	var swept []SweptChunk
	for _, candidate := range candidates {
		if supersededAcrossBuilds(candidate, deploying) {
			swept = append(swept, SweptChunk{ChunkID: candidate.ID, BuildID: candidate.BuildID})
		}
	}
	return swept
}

// SweepEarlierDeploymentsAcrossBuilds completes eligible earlier chunks in
// other builds superseded by the deploying chunk's landed commit and
// returns every affected chunk and build ID, plus already-completed chunks
// matching the same eligibility so a retry that lands after the writes but
// before the side effects can still publish events, re-sync widgets, and
// complete foreign builds. Skipped transitions (a concurrent completion,
// failure, or cancellation) are harmless; other errors return the chunks
// completed so far alongside the error so the caller processes partial
// progress before the journaled step retries the remainder.
func (s *Store) SweepEarlierDeploymentsAcrossBuilds(ctx context.Context, deploying model.VanadisChunk) (swept, recovered []SweptChunk, err error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	if strings.TrimSpace(deploying.BuildID) == "" || strings.TrimSpace(deploying.ID) == "" {
		return nil, nil, nil
	}
	repos := RepoMatchCandidates(deploying.Repo)
	if len(repos) == 0 {
		return nil, nil, nil
	}
	rows, err := s.db.Query(ctx, `SELECT c.id, c.build_id, c.chunk_index, c.mode, c.repo, c.title, c.instructions, c.target_files, c.target_symbols, COALESCE(c.effort, 'medium'), c.acceptance_criteria, c.status, COALESCE(c.pr_number, 0), COALESCE(c.stall_nudges, 0), c.depends_on, c.ci_fail_count, c.ci_checks, c.rollout_state, c.banner, c.merge_awakeable_id, c.deploy_awakeable_id, c.started_at, c.completed_at, c.created_at, c.updated_at FROM vanadis_chunks AS c JOIN vanadis_builds AS b ON b.id = c.build_id WHERE c.build_id != $1 AND c.mode = 'pr' AND COALESCE(c.pr_number, 0) > 0 AND c.status IN ('merged', 'synced', 'deploying', 'completed') AND c.repo = ANY($2) AND b.status NOT IN ('cancelled', 'failed', 'completed') ORDER BY c.build_id ASC, c.chunk_index ASC;`, deploying.BuildID, repos)
	if err != nil {
		return nil, nil, fmt.Errorf("query cross-build sweep candidates: %w", err)
	}
	defer rows.Close()
	candidates, err := ScanChunks(rows)
	if err != nil {
		return nil, nil, fmt.Errorf("scan cross-build sweep candidates: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate cross-build sweep candidates: %w", err)
	}
	for _, sw := range CrossBuildDeploymentSweepIDs(deploying, candidates) {
		if err := s.CompletePRDeployment(ctx, sw.ChunkID, nil); err != nil {
			if errors.Is(err, ErrPRTransitionSkipped) {
				continue
			}
			return swept, recoveredCrossBuildChunks(deploying, candidates), err
		}
		swept = append(swept, sw)
	}
	return swept, recoveredCrossBuildChunks(deploying, candidates), nil
}

// recoveredCrossBuildChunks re-derives already-completed chunks matching
// the cross-build sweep eligibility so a retry can finish their side
// effects idempotently.
func recoveredCrossBuildChunks(deploying model.VanadisChunk, candidates []model.VanadisChunk) []SweptChunk {
	if strings.TrimSpace(deploying.BuildID) == "" {
		return nil
	}
	var recovered []SweptChunk
	for _, candidate := range candidates {
		if recoveredAcrossBuilds(candidate, deploying) {
			recovered = append(recovered, SweptChunk{ChunkID: candidate.ID, BuildID: candidate.BuildID})
		}
	}
	return recovered
}

// EarlierDeploymentSweepIDs returns the IDs of chunks a deploy_success also
// completes: every earlier chunk in the same build and repo that is still
// pre-deploy (merged, synced, or deploying) and whose merge stamp is at or
// before the deploying chunk's merge stamp, breaking stamp ties by lower
// chunk_index. Either stamp unknown means inclusion is unprovable, so the
// candidate is skipped. Terminal (failed, cancelled, completed) and
// never-merged chunks are never swept, so in-flight builds keep working and
// completed work is unaffected.
func EarlierDeploymentSweepIDs(chunks []model.VanadisChunk, chunkID string) []string {
	var deploying *model.VanadisChunk
	for i := range chunks {
		if chunks[i].ID == chunkID {
			deploying = &chunks[i]
			break
		}
	}
	if deploying == nil {
		return nil
	}
	var swept []string
	for i := range chunks {
		if supersededByDeployment(chunks[i], *deploying) {
			swept = append(swept, chunks[i].ID)
		}
	}
	return swept
}

// supersededByDeployment reports whether candidate merged early enough to be
// contained in the deploying chunk's landed commit.
func supersededByDeployment(candidate, deploying model.VanadisChunk) bool {
	if candidate.ID == deploying.ID || candidate.BuildID != deploying.BuildID {
		return false
	}
	return sameDeployRepo(candidate.Repo, deploying.Repo) && deployCompletableMode(candidate) && sweepPreDeployStatus(candidate.Status) && sweepStampProvesInclusion(candidate, deploying)
}

// deployCompletableMode matches the CompletePRDeployment mode guard: plain
// PR chunks and PR-attached action chunks.
func deployCompletableMode(c model.VanadisChunk) bool {
	if c.Mode == "pr" {
		return true
	}
	return c.Mode == "action" && c.PRNumber > 0
}

// sameDeployRepo matches repo spellings the chunk lookup already treats as
// the same repository, including the owner-qualified variant.
func sameDeployRepo(a, b string) bool {
	a, b = NormalizeRepo(a), NormalizeRepo(b)
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	return slices.Contains(RepoMatchCandidates(a), b) || slices.Contains(RepoMatchCandidates(b), a)
}

// UpdatePRLifecycleStatus prevents delayed pre-deployment events from reopening
// a terminal chunk. Notifications follow only an applied write. The deploying
// transition preserves the merge stamp without ever creating one: the landed
// commit contains every earlier merge, so deploy_success needs the deploying
// chunk's real merge time to prove which earlier chunks it includes, and a
// deploying chunk with no recorded merge keeps an unknown stamp so the sweep
// skips it instead of completing unprovable chunks.
func (s *Store) UpdatePRLifecycleStatus(ctx context.Context, chunkID, status string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	if strings.TrimSpace(chunkID) == "" {
		return errors.New("chunk_id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET status = $1, completed_at = CASE WHEN $1 = 'merged' THEN COALESCE(completed_at, CURRENT_TIMESTAMP) WHEN $1 = 'deploying' THEN completed_at ELSE NULL END, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status NOT IN ('completed', 'failed', 'cancelled');`, status, strings.TrimSpace(chunkID))
	if err != nil {
		return fmt.Errorf("update pr lifecycle status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrPRTransitionSkipped
	}
	return nil
}

func (s *Store) RecordPRCheckStatus(ctx context.Context, chunkID, headSHA, checkName, checkState string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	name := strings.TrimSpace(checkName)
	sha := strings.TrimSpace(headSHA)
	if id == "" || name == "" {
		return errors.New("chunk_id and check_name cannot be empty")
	}
	const q = `UPDATE vanadis_chunks
SET ci_checks = jsonb_set(
        CASE
            WHEN (status IN ('merged', 'synced', 'deploy_failed') OR COALESCE(ci_checks->>'_merge_sha', '') != '') AND $4 != '' AND COALESCE(ci_checks->>'_merge_sha', '') = ''
                THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb) - '_head_sha', '{_merge_sha}', to_jsonb($4::text), true)
            WHEN status NOT IN ('merged', 'synced', 'deploy_failed') AND COALESCE(ci_checks->>'_merge_sha', '') = '' AND $4 != '' AND COALESCE(ci_checks->>'_head_sha', '') = ''
                THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb), '{_head_sha}', to_jsonb($4::text), true)
            ELSE COALESCE(ci_checks, '{}'::jsonb)
        END,
        ARRAY[$1],
        to_jsonb($2::text),
        true
    ),
    status = CASE
        WHEN status = 'ci_failed' AND $2 = 'passed' AND NOT EXISTS (
            SELECT 1 FROM jsonb_each_text(jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true))
            WHERE key NOT LIKE '\_%' AND value = 'failed'
        ) THEN 'pr_opened'
        ELSE status
    END,
    banner = CASE
        WHEN status = 'ci_failed' AND $2 = 'passed' AND NOT EXISTS (
            SELECT 1 FROM jsonb_each_text(jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true))
            WHERE key NOT LIKE '\_%' AND value = 'failed'
        ) THEN ''
        ELSE banner
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $3
  AND status NOT IN ('deploying', 'deployed', 'completed', 'failed', 'cancelled')
  AND (
      $4 = ''
      OR ((status IN ('merged', 'synced', 'deploy_failed') OR COALESCE(ci_checks->>'_merge_sha', '') != '') AND (
          COALESCE(ci_checks->>'_merge_sha', '') = $4
          OR (COALESCE(ci_checks->>'_merge_sha', '') = '' AND COALESCE(ci_checks->>'_head_sha', '') IS DISTINCT FROM $4)
      ))
      OR (status NOT IN ('merged', 'synced', 'deploy_failed') AND COALESCE(ci_checks->>'_merge_sha', '') = '' AND (
          COALESCE(ci_checks->>'_head_sha', '') = ''
          OR ci_checks->>'_head_sha' = $4
      ))
  );`
	tag, err := s.db.Exec(ctx, q, name, strings.TrimSpace(checkState), id, sha)
	switch {
	case err != nil:
		return fmt.Errorf("record pr check status: %w", err)
	case tag.RowsAffected() == 0:
		return ErrPRTransitionSkipped
	default:
		return nil
	}
}

func (s *Store) ResetPRCheckState(ctx context.Context, chunkID, headSHA, mergeSHA string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return errors.New("chunk_id cannot be empty")
	}
	cleanHead := strings.TrimSpace(headSHA)
	cleanMerge := strings.TrimSpace(mergeSHA)
	if cleanMerge != "" {
		const mergeQ = `UPDATE vanadis_chunks
SET ci_checks = CASE
        WHEN COALESCE(ci_checks->>'_merge_sha', '') = $3
            THEN jsonb_strip_nulls(COALESCE(ci_checks, '{}'::jsonb) || jsonb_build_object('_head_sha', COALESCE(NULLIF($2::text, ''), NULLIF(ci_checks->>'_head_sha', '')), '_merge_sha', NULLIF($3::text, '')))
        ELSE jsonb_strip_nulls(jsonb_build_object('_head_sha', COALESCE(NULLIF($2::text, ''), NULLIF(ci_checks->>'_head_sha', '')), '_merge_sha', NULLIF($3::text, '')))
    END,
    banner = '',
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND status NOT IN ('completed', 'failed', 'cancelled');`
		tag, err := s.db.Exec(ctx, mergeQ, id, cleanHead, cleanMerge)
		if err != nil {
			return fmt.Errorf("reset merge check state: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrPRTransitionSkipped
		}
		return nil
	}
	const syncQ = `UPDATE vanadis_chunks AS c
SET ci_checks = CASE
        WHEN $2 != '' AND LOWER(COALESCE(c.ci_checks->>'_head_sha', '')) = LOWER($2) THEN c.ci_checks - '_merge_sha'
        WHEN $2 != '' THEN jsonb_build_object('_head_sha', $2::text)
        ELSE '{}'::jsonb
    END,
    banner = '',
    status = CASE WHEN c.status IN ('ci_failed', 'conflict') THEN 'pr_opened' ELSE c.status END,
    updated_at = CURRENT_TIMESTAMP
FROM vanadis_builds AS b
WHERE c.id = $1
  AND b.id = c.build_id
  AND c.status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed', 'failed', 'cancelled')
  AND COALESCE(c.ci_checks->>'_merge_sha', '') = ''
  AND b.status NOT IN ('completed', 'failed', 'cancelled')
  AND ($3::boolean OR $2 = '' OR COALESCE(c.ci_checks->>'_head_sha', '') = '' OR LOWER(c.ci_checks->>'_head_sha') = LOWER($2));`
	tag, err := s.db.Exec(ctx, syncQ, id, cleanHead, PrHeadVerifiedFromContext(ctx))
	if err != nil {
		return fmt.Errorf("reset pr check state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.recoverFailedPR(ctx, id, cleanHead)
	}
	return nil
}

func (s *Store) IncrementChunkCIFailCount(ctx context.Context, chunkID, banner string, headSHA ...string) (int, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return 0, errors.New("chunk_id cannot be empty")
	}
	var cleanSHA string
	if len(headSHA) > 0 {
		cleanSHA = strings.TrimSpace(headSHA[0])
	}
	const q = `UPDATE vanadis_chunks
SET ci_fail_count = ci_fail_count + 1,
    status = 'ci_failed',
    banner = $1,
    ci_checks = CASE WHEN $3 != '' THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb), '{_remediated_sha}', to_jsonb($3::text), true) ELSE ci_checks END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $2
  AND status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed', 'failed', 'cancelled')
RETURNING ci_fail_count;`
	var failCount int
	err := s.db.QueryRow(ctx, q, strings.TrimSpace(banner), id, cleanSHA).Scan(&failCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrPRTransitionSkipped
	}
	if err != nil {
		return 0, fmt.Errorf("increment chunk ci fail count: %w", err)
	}
	return failCount, nil
}

// PrHeadVerifiedContextKey carries the router-set verified-head marker across
// the package boundary. It is defined here so the moved ResetPRCheckState and
// recoverFailedPR read the same key the root producer writes; the root
// package aliases it to keep existing references compiling unchanged.
type PrHeadVerifiedContextKey struct{}

// PrHeadVerifiedFromContext reports whether ctx carries the verified-head marker.
func PrHeadVerifiedFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	verified, ok := ctx.Value(PrHeadVerifiedContextKey{}).(bool)
	return ok && verified
}

// lockRetryBuildAndChunks follows the parent-first ordering used by existing mutations.
func lockRetryBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) (model.VanadisBuild, []model.VanadisChunk, error) {
	b, err := ScanBuild(tx.QueryRow(ctx, `SELECT `+BuildColumns+` FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID))
	if errors.Is(err, pgx.ErrNoRows) {
		return b, nil, ErrBuildNotFound
	}
	if err != nil {
		return b, nil, err
	}
	rows, err := tx.Query(ctx, SelectChunksForUpdateSQL, buildID)
	if err != nil {
		return b, nil, err
	}
	defer rows.Close()
	chunks, err := ScanChunks(rows)
	return b, chunks, err
}
