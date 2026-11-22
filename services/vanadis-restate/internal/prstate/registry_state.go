package prstate

// Registry row state. PRState carries everything a pr_registry row holds
// (the branch/head/merge/target/title/status columns plus every metadata key
// observed in prod: jobs, started_jobs, deployed_jobs, job_states,
// kustomization_requests, deployed_kustomizations, deploy_failed_published,
// deploy_success_pending, build_metadata, banner, ci_checks, ci_fail_count,
// vanadis_chunk_id, agent, bot_id, widget_state) so PRObject can become the
// single registry writer. The transitions below are pure ports of the
// synapse registry SQL (services/synapse/internal/registry/registry.go and
// kustomization.go): each names the statement it mirrors, and none performs
// I/O, reads clocks, or touches Restate.

import (
	"regexp"
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/prwire"
)

// Registry status values. They match the pr_registry status column exactly.
const (
	RegistryStatusOpen         = "open"
	RegistryStatusCIFailed     = "ci_failed"
	RegistryStatusConflict     = "conflict"
	RegistryStatusMerged       = "merged"
	RegistryStatusSynced       = "synced"
	RegistryStatusClosed       = "closed"
	RegistryStatusDeploying    = "deploying"
	RegistryStatusDeployed     = "deployed"
	RegistryStatusDeployFailed = "deploy_failed"
)

// Register metadata keys PRObject records as typed row fields.
const (
	MetaVanadisChunkID = "vanadis_chunk_id"
	MetaWidgetState    = "widget_state"
)

// ciFailCountResetThreshold mirrors the registry rule that a head change
// drops ci_fail_count only once it exceeded three.
const ciFailCountResetThreshold = 3

// buildMetadataPhase orders snapshot kinds: final is phase 2, every other
// recordable kind (manifest, progress) is phase 1.
func buildMetadataPhase(m *BuildMetadata) int {
	if m != nil && m.Kind == prwire.BuildMetadataFinal {
		return 2
	}
	return 1
}

// RegisterVerdict classifies a register request before it mutates state.
type RegisterVerdict string

// Register verdicts.
const (
	RegisterOK            RegisterVerdict = "ok"
	RegisterInvalid       RegisterVerdict = "invalid"
	RegisterOwnerMismatch RegisterVerdict = "owner_mismatch"
	RegisterBadTarget     RegisterVerdict = "bad_target"
)

var discordSnowflake = regexp.MustCompile(`^\d{17,20}$`)

// IsValidDiscordSnowflake reports whether id is a 17-20 digit snowflake.
func IsValidDiscordSnowflake(id string) bool {
	return discordSnowflake.MatchString(strings.TrimSpace(id))
}

func statusIn(status string, set ...string) bool {
	return slices.Contains(set, status)
}

// isPostMergeStatus is the registry's ('merged','synced','deploying','deployed') set.
func isPostMergeStatus(status string) bool {
	return statusIn(status, RegistryStatusMerged, RegistryStatusSynced, RegistryStatusDeploying, RegistryStatusDeployed)
}

// isLockedStatus is the registry's set of statuses that pre-merge
// transitions (ci_failed, conflict, sync) never leave.
func isLockedStatus(status string) bool {
	return statusIn(status, RegistryStatusMerged, RegistryStatusSynced, RegistryStatusClosed, RegistryStatusDeploying, RegistryStatusDeployed, RegistryStatusDeployFailed)
}

// RegistryStatus returns the stored registry status, defaulting to open.
func (s *PRState) RegistryStatus() string {
	if s == nil || strings.TrimSpace(s.Status) == "" {
		return RegistryStatusOpen
	}
	return s.Status
}

// clearHeadScoped drops the metadata a head change invalidates: banner,
// ci_checks and build_metadata, plus ci_fail_count once it exceeded three.
func (s *PRState) clearHeadScoped() {
	s.Banner = ""
	s.CIChecks = nil
	s.BuildMetadata = nil
	if s.CIFailCount > ciFailCountResetThreshold {
		s.CIFailCount = 0
	}
}

// ResolveRegister validates a register request against the stored row and
// returns the verdict plus the resolved target ID, without mutating state.
// It mirrors validatePRRegisterRequest (headless target resolution: request
// snowflake, else the stored target, else the system-channel fallback) and
// authorizeExistingPROwner (a stored owner must equal the owner synapse
// authorized the caller for).
func (s *PRState) ResolveRegister(req RegisterRequest) (RegisterVerdict, string) {
	if s == nil || strings.TrimSpace(req.Repo) == "" || req.PRNumber <= 0 || strings.TrimSpace(req.Branch) == "" || strings.TrimSpace(req.HeadSHA) == "" {
		return RegisterInvalid, ""
	}
	if req.EnforceOwner {
		stored := strings.TrimSpace(s.Agent)
		if stored != "" && stored != strings.TrimSpace(req.ExpectedOwner) {
			return RegisterOwnerMismatch, ""
		}
	}
	for _, candidate := range []string{req.TargetID, s.TargetID, req.FallbackTargetID} {
		if clean := strings.TrimSpace(candidate); IsValidDiscordSnowflake(clean) {
			return RegisterOK, clean
		}
	}
	return RegisterBadTarget, ""
}

// ApplyRegister mirrors the RegisterPR upsert: branch, head and target are
// replaced, a blank title keeps the stored one, post-merge statuses are
// kept (every other status reopens), a head change outside post-merge
// clears the head-scoped metadata, and the typed register metadata merges
// over the row. Register claims ownership. targetID must come from
// ResolveRegister.
func (s *PRState) ApplyRegister(req RegisterRequest, targetID string) {
	if s == nil {
		return
	}
	head := strings.TrimSpace(req.HeadSHA)
	if !isPostMergeStatus(s.Status) {
		if s.HeadSHA != head {
			s.clearHeadScoped()
		} else {
			s.Banner = ""
		}
		s.Status = RegistryStatusOpen
	}
	s.Registered = true
	s.Branch = strings.TrimSpace(req.Branch)
	s.SetHead(head)
	s.TargetID = targetID
	if title := strings.TrimSpace(req.Title); title != "" {
		s.Title = title
	}
	agent := strings.TrimSpace(req.Agent)
	if agent == "" {
		agent = strings.TrimSpace(req.BotID)
	}
	if agent != "" {
		s.ClaimOwner(agent)
		s.BotID = agent
	}
	if v := strings.TrimSpace(req.Metadata[MetaVanadisChunkID]); v != "" {
		s.VanadisChunkID = v
	}
	if v := strings.TrimSpace(req.Metadata[MetaWidgetState]); v != "" {
		s.WidgetState = v
	}
}

// ApplyIdentity fills branch and title from an event without overwriting
// stored values with blanks (EnsurePRRegistered fill-if-empty semantics for
// title, replace-if-set for branch).
func (s *PRState) ApplyIdentity(branch, title string) {
	if s == nil {
		return
	}
	if b := strings.TrimSpace(branch); b != "" {
		s.Branch = b
	}
	if t := strings.TrimSpace(title); t != "" && s.Title == "" {
		s.Title = t
	}
}

// ApplySelfRegistration mirrors synapse's EnsurePRRegistered self-heal
// upsert (pull_request self-registration of default-owner dependabot PRs and
// PR-body metadata PRs, and the dependabot CI-failure heal). Like that
// upsert it is fill-only: an invalid target writes nothing; otherwise the
// branch is replaced when set, and head, target and title fill only when
// empty. The owner is seeded only when the registration creates the row,
// which for PRObject means no target was stored yet (Register always
// stores one): it is then claimed as the inserted row's owner
// (claimRowOwner) and also fills BotID, the two keys the insert writes. The
// status is untouched. Filling the target makes the PR tracked, so the
// untracked guard stops applying and Project writes the row legacy writes.
// Returns true when the state changed.
func (s *PRState) ApplySelfRegistration(branch, head, targetID, title, agent string) bool {
	targetID = strings.TrimSpace(targetID)
	if s == nil || !IsValidDiscordSnowflake(targetID) {
		return false
	}
	changed := false
	if b := strings.TrimSpace(branch); b != "" && s.Branch != b {
		s.Branch = b
		changed = true
	}
	if h := strings.TrimSpace(head); h != "" && !prwire.IsZeroSHA(h) && strings.TrimSpace(s.HeadSHA) == "" {
		s.SetHead(h)
		changed = true
	}
	if t := strings.TrimSpace(title); t != "" && strings.TrimSpace(s.Title) == "" {
		s.Title = t
		changed = true
	}
	if strings.TrimSpace(s.TargetID) != "" {
		return changed
	}
	s.TargetID = targetID
	if agent = strings.ToLower(strings.TrimSpace(agent)); agent != "" {
		s.claimRowOwner(agent)
		if strings.TrimSpace(s.BotID) == "" {
			s.BotID = agent
		}
	}
	return true
}

// ApplyRegistrySync mirrors UpdatePRSync: an open (non-merged, non-locked)
// PR moves to head and reopens; a head change clears head-scoped metadata,
// otherwise only the banner clears. Returns true when the row changed.
func (s *PRState) ApplyRegistrySync(prevHead, head string) bool {
	head = strings.TrimSpace(head)
	if s == nil || head == "" || prwire.IsZeroSHA(head) || s.MergeSHA != "" || isLockedStatus(s.Status) {
		return false
	}
	headChanged := prevHead != head
	if !headChanged && !statusIn(s.Status, RegistryStatusCIFailed, RegistryStatusConflict) && s.Banner == "" {
		return false
	}
	if headChanged {
		s.clearHeadScoped()
	} else {
		s.Banner = ""
	}
	s.Status = RegistryStatusOpen
	return true
}

// SynchronizeDisposition is the compare-and-set guard for a synchronize:
// same-epoch terminal states absorb and an empty or zero head drops. The
// head applies from the stored head (BeforeSHA match), an unknown stored
// head, or a redelivery of the same new head. A head this PR already moved
// past (PriorHeads) is a late redelivery and drops stale. Any other
// mismatch means PRObject missed a synchronize; it recovers last-write-wins
// like the legacy row instead of wedging the head until merge.
func (s *PRState) SynchronizeDisposition(ev PREvent) EventDisposition {
	if s == nil {
		return DispositionDropStale
	}
	if s.Terminal.Absorbing() && ev.ReopenEpoch <= s.ReopenEpoch {
		return DispositionAbsorbTerminal
	}
	head := strings.TrimSpace(ev.HeadSHA)
	if head == "" || prwire.IsZeroSHA(head) {
		return DispositionDropStale
	}
	stored := strings.TrimSpace(s.HeadSHA)
	if stored == "" || strings.EqualFold(stored, head) || strings.EqualFold(stored, strings.TrimSpace(ev.BeforeSHA)) {
		return DispositionApply
	}
	if s.HasPriorHead(head) {
		return DispositionDropStale
	}
	return DispositionRecoverStale
}

// ApplyRegistryMerged mirrors UpdatePRMerged/BackfillPushMergeSHA: status
// becomes merged unless already post-merge or deploy_failed, a non-empty
// merge SHA is recorded, and the banner clears unless deploy_failed.
func (s *PRState) ApplyRegistryMerged(mergeSHA string) {
	if s == nil {
		return
	}
	if clean := strings.TrimSpace(mergeSHA); clean != "" && !prwire.IsZeroSHA(clean) {
		s.MergeSHA = clean
	}
	if s.Status == RegistryStatusDeployFailed {
		return
	}
	s.Banner = ""
	if !statusIn(s.Status, RegistryStatusSynced, RegistryStatusDeploying, RegistryStatusDeployed) {
		s.Status = RegistryStatusMerged
	}
}

// ApplyRegistryClosed mirrors UpdatePRClosedUnmerged.
func (s *PRState) ApplyRegistryClosed() {
	if s == nil {
		return
	}
	s.Status = RegistryStatusClosed
	s.Banner = ""
}

// ApplyRegistryCIFailed mirrors AtomicTransitionCIFailed. Returns true on
// the transition.
func (s *PRState) ApplyRegistryCIFailed() bool {
	if s == nil || s.MergeSHA != "" || s.Status == RegistryStatusCIFailed || isLockedStatus(s.Status) {
		return false
	}
	s.Status = RegistryStatusCIFailed
	return true
}

// ApplyRegistryConflict mirrors AtomicTransitionConflict. Returns true on
// the transition.
func (s *PRState) ApplyRegistryConflict() bool {
	if s == nil || s.MergeSHA != "" || s.Status == RegistryStatusConflict || isLockedStatus(s.Status) {
		return false
	}
	s.Status = RegistryStatusConflict
	return true
}

// ApplyRegistryRecovered reopens a ci_failed or conflict row once the
// failure it recorded no longer holds.
func (s *PRState) ApplyRegistryRecovered(from string) bool {
	if s == nil || s.Status != from {
		return false
	}
	s.Status = RegistryStatusOpen
	return true
}

// RecordCheckState mirrors RecordPRCheckStatus: ci_checks[check] = state for
// an open/ci_failed/conflict PR on its head SHA or a merged/synced PR on its
// merge SHA. Returns true when the stored value changed.
func (s *PRState) RecordCheckState(sha, checkName, state string) bool {
	sha = strings.TrimSpace(sha)
	checkName = strings.TrimSpace(checkName)
	state = strings.TrimSpace(state)
	if s == nil || sha == "" || prwire.IsZeroSHA(sha) || checkName == "" || state == "" {
		return false
	}
	status := s.RegistryStatus()
	preMerge := statusIn(status, RegistryStatusOpen, RegistryStatusCIFailed, RegistryStatusConflict) && s.HeadSHA == sha
	postMerge := statusIn(status, RegistryStatusMerged, RegistryStatusSynced) && s.MergeSHA == sha
	if !preMerge && !postMerge {
		return false
	}
	if s.CIChecks[checkName] == state {
		return false
	}
	if s.CIChecks == nil {
		s.CIChecks = make(map[string]string)
	}
	s.CIChecks[checkName] = state
	return true
}

func unionStrings(base []string, add ...string) []string {
	out := slices.Clone(base)
	for _, v := range add {
		if clean := strings.TrimSpace(v); clean != "" && !slices.Contains(out, clean) {
			out = append(out, clean)
		}
	}
	slices.Sort(out)
	return out
}

func withoutStrings(base, drop []string) []string {
	var out []string
	for _, v := range base {
		if !slices.Contains(drop, v) {
			out = append(out, v)
		}
	}
	return out
}

func cleanStrings(in []string) []string {
	return unionStrings(nil, in...)
}

// RecordExpectedJobs mirrors RecordExpectedJobs: jobs ∪ requested minus
// already-deployed jobs, for any PR not closed or in conflict.
func (s *PRState) RecordExpectedJobs(jobs []string) bool {
	jobs = cleanStrings(jobs)
	if s == nil || len(jobs) == 0 || statusIn(s.Status, RegistryStatusClosed, RegistryStatusConflict) {
		return false
	}
	next := withoutStrings(unionStrings(s.Jobs, jobs...), s.DeployedJobs)
	if slices.Equal(next, s.Jobs) {
		return false
	}
	s.Jobs = next
	return true
}

// Untracked reports whether st never saw any event that makes a PR exist
// for the legacy registry: no register or self-registration (Registered,
// TargetID), no opened (it carries the head SHA, and PR-body bot metadata
// sets BotID), and no merge or close (Terminal, MergeSHA). Such a state was built only from deploy-phase
// events, for example an upstream Flux source commit resolved to a PR of a
// repo nobody registers. The legacy registry has no row for it, so its
// deploy transitions and request records are no-ops there; prstate must not
// start a rollout for it either. A nil state is not untracked: there is
// nothing to drop.
func (s *PRState) Untracked() bool {
	return s != nil && !s.Registered && s.Terminal == TerminalNone &&
		strings.TrimSpace(s.HeadSHA) == "" && strings.TrimSpace(s.MergeSHA) == "" &&
		strings.TrimSpace(s.TargetID) == "" && strings.TrimSpace(s.BotID) == ""
}

// HasDeployResidue reports whether st still holds rollout state: a status,
// recorded or started jobs, reconcile requests, or rollout-index membership.
func (s *PRState) HasDeployResidue() bool {
	return s != nil && (s.Status != "" || len(s.Jobs) > 0 || len(s.StartedJobs) > 0 ||
		len(s.KustomizationRequests) > 0 || s.RolloutIndexed || s.PendingDeployFailure != nil)
}

// ClearDeployResidue drops every rollout field of st, so an untracked state
// created before the untracked-event guard stops reading as a rollout. It
// keeps the generation and index sequences, so a later index update for the
// PR still orders after the inactive one this clear sends.
func (s *PRState) ClearDeployResidue() {
	if s == nil {
		return
	}
	s.Status = ""
	s.Jobs, s.StartedJobs, s.DeployedJobs = nil, nil, nil
	s.JobStates = nil
	s.KustomizationRequests, s.DeployedKustomizations = nil, nil
	s.DeploySuccessPending, s.DeployFailedPublished = false, false
	s.PendingDeployFailure = nil
	s.Banner = ""
}

// TransitionDeploying mirrors TransitionDeploying: when at least one
// requested job is not yet deployed and the PR is not closed or in conflict,
// the PR moves to deploying, jobs gain the undeployed requests, started_jobs
// gain every request, and banner/deploy_failed_published clear.
func (s *PRState) TransitionDeploying(jobs []string) bool {
	jobs = cleanStrings(jobs)
	if s == nil || len(jobs) == 0 || statusIn(s.Status, RegistryStatusClosed, RegistryStatusConflict) {
		return false
	}
	if len(withoutStrings(jobs, s.DeployedJobs)) == 0 {
		return false
	}
	s.Status = RegistryStatusDeploying
	s.Jobs = withoutStrings(unionStrings(s.Jobs, jobs...), s.DeployedJobs)
	s.StartedJobs = unionStrings(s.StartedJobs, jobs...)
	s.Banner = ""
	s.DeployFailedPublished = false
	return true
}

// RecordKustomizationRequestSet mirrors RecordKustomizationRequests: the
// requests replace the stored set wholesale and deployed_kustomizations and
// deployed_jobs clear, so completion tracks only the latest reconcile.
func (s *PRState) RecordKustomizationRequestSet(requests map[string]KustomizationRequest) bool {
	if s == nil || len(requests) == 0 {
		return false
	}
	next := make(map[string]KustomizationRequest, len(requests))
	for name, req := range requests {
		if clean := strings.TrimSpace(name); clean != "" {
			next[clean] = req
		}
	}
	if len(next) == 0 {
		return false
	}
	s.KustomizationRequests = next
	s.DeployedKustomizations = nil
	s.DeployedJobs = nil
	return true
}

// MarkJobsDeployed mirrors MarkJobsDeployed: for a deploying or
// deploy_failed PR, ready jobs eligible in started_jobs ∪ jobs (or any job
// when both are empty) union into deployed_jobs. Returns true when at least
// one new job was recorded.
func (s *PRState) MarkJobsDeployed(jobs []string) bool {
	jobs = cleanStrings(jobs)
	if s == nil || len(jobs) == 0 || !statusIn(s.Status, RegistryStatusDeploying, RegistryStatusDeployFailed) {
		return false
	}
	eligible := unionStrings(s.StartedJobs, s.Jobs...)
	var add []string
	for _, j := range jobs {
		if (len(eligible) == 0 || slices.Contains(eligible, j)) && !slices.Contains(s.DeployedJobs, j) {
			add = append(add, j)
		}
	}
	if len(add) == 0 {
		return false
	}
	s.DeployedJobs = unionStrings(s.DeployedJobs, add...)
	return true
}

// MarkKustomizationsDeployed mirrors MarkKustomizationsDeployed: for a
// deploying or deploy_failed PR with recorded requests, requested names
// union into deployed_kustomizations, and once every request is deployed
// the PR flips to deployed with deploy_success_pending set and banner and
// deploy_failed_published cleared. Returns true only on the flip.
func (s *PRState) MarkKustomizationsDeployed(names []string) bool {
	names = cleanStrings(names)
	if s == nil || len(names) == 0 || len(s.KustomizationRequests) == 0 || !statusIn(s.Status, RegistryStatusDeploying, RegistryStatusDeployFailed) {
		return false
	}
	for _, n := range names {
		if _, ok := s.KustomizationRequests[n]; ok {
			s.DeployedKustomizations = unionStrings(s.DeployedKustomizations, n)
		}
	}
	for name := range s.KustomizationRequests {
		if !slices.Contains(s.DeployedKustomizations, name) {
			return false
		}
	}
	s.Status = RegistryStatusDeployed
	s.DeploySuccessPending = true
	s.Banner = ""
	s.DeployFailedPublished = false
	return true
}

// RecordJobState mirrors RecordPRJobState for one PR: a deploying PR that
// expects and started the job records job_states[job] = state.
func (s *PRState) RecordJobState(jobName, state string) bool {
	jobName = strings.TrimSpace(jobName)
	state = strings.TrimSpace(state)
	if s == nil || jobName == "" || state == "" || s.Status != RegistryStatusDeploying {
		return false
	}
	if !slices.Contains(s.Jobs, jobName) || !slices.Contains(s.StartedJobs, jobName) || s.JobStates[jobName] == state {
		return false
	}
	if s.JobStates == nil {
		s.JobStates = make(map[string]string)
	}
	s.JobStates[jobName] = state
	return true
}

// AckDeploySuccess mirrors UpdatePRStatus(deployed, deploy_success_pending=false).
func (s *PRState) AckDeploySuccess() {
	if s == nil {
		return
	}
	s.Status = RegistryStatusDeployed
	s.DeploySuccessPending = false
}

// ApplyRegistryDeployFailed mirrors AtomicTransitionDeployFailed: a
// deploying/merged/synced PR, or a deploy_failed PR whose failure was not
// yet published, moves to deploy_failed; entering the state resets
// deploy_failed_published. A non-empty banner is recorded. Returns true on
// the transition.
func (s *PRState) ApplyRegistryDeployFailed(banner string) bool {
	if s == nil {
		return false
	}
	switch {
	case statusIn(s.Status, RegistryStatusDeploying, RegistryStatusMerged, RegistryStatusSynced):
		s.DeployFailedPublished = false
	case s.Status == RegistryStatusDeployFailed && !s.DeployFailedPublished:
	default:
		return false
	}
	s.Status = RegistryStatusDeployFailed
	if b := strings.TrimSpace(banner); b != "" {
		s.Banner = b
	}
	return true
}

// AckDeployFailedPublished mirrors AcknowledgeDeployFailedPublished.
func (s *PRState) AckDeployFailedPublished() bool {
	if s == nil || s.Status != RegistryStatusDeployFailed || s.DeployFailedPublished {
		return false
	}
	s.DeployFailedPublished = true
	return true
}

// buildMetadataRevisionMatches is the registry revision fence shared by
// record and load: pre-merge rows match their head SHA, merged/synced rows
// their merge SHA.
func (s *PRState) buildMetadataRevisionMatches(sha string) bool {
	status := s.RegistryStatus()
	if statusIn(status, RegistryStatusOpen, RegistryStatusCIFailed, RegistryStatusConflict) {
		return sha != "" && s.HeadSHA == sha
	}
	if statusIn(status, RegistryStatusMerged, RegistryStatusSynced) {
		return sha != "" && s.MergeSHA == sha
	}
	return false
}

func sameSelection(a, b []BuildSelection) bool {
	return slices.Equal(a, b)
}

// RecordBuildMetadata mirrors RecordPRBuildMetadata's fence: the snapshot
// must target this repo#pr revision; it replaces a missing snapshot, an
// older (run_number, run_attempt), or the same run attempt at a version and
// phase no lower; a different run_id never replaces a stored run with the
// same identity fields mismatched. Result snapshots never record. Returns
// true when stored.
func (s *PRState) RecordBuildMetadata(m *BuildMetadata) bool {
	if s == nil || m == nil || m.Kind == "result" || strings.TrimSpace(m.Repository) != s.Repo || (m.PRNumber != nil && *m.PRNumber != s.PRNumber) {
		return false
	}
	if !s.buildMetadataRevisionMatches(strings.TrimSpace(m.SourceSHA)) {
		return false
	}
	if cur := s.BuildMetadata; cur != nil && !buildMetadataReplaces(cur, m) {
		return false
	}
	clone := *m
	s.BuildMetadata = &clone
	return true
}

// buildMetadataReplaces is the RecordPRBuildMetadata ordering fence: m
// replaces cur when its (run_number, run_attempt) is newer or it is the same
// run attempt at a version and phase no lower, and a snapshot from the same
// run_id must keep that run's identity fields.
func buildMetadataReplaces(cur, m *BuildMetadata) bool {
	newer := cur.RunNumber < m.RunNumber || (cur.RunNumber == m.RunNumber && cur.RunAttempt < m.RunAttempt)
	if !newer && !sameBuildAttempt(cur, m) {
		return false
	}
	return cur.RunID != m.RunID || sameBuildIdentity(cur, m)
}

func sameBuildAttempt(cur, m *BuildMetadata) bool {
	return cur.RunID == m.RunID && cur.RunAttempt == m.RunAttempt && cur.WorkflowPath == m.WorkflowPath &&
		cur.ExecutionSHA == m.ExecutionSHA && sameSelection(cur.Selected, m.Selected) &&
		cur.Version <= m.Version && buildMetadataPhase(cur) <= buildMetadataPhase(m)
}

func sameBuildIdentity(cur, m *BuildMetadata) bool {
	return cur.RunNumber == m.RunNumber && cur.SourceSHA == m.SourceSHA && cur.ExecutionSHA == m.ExecutionSHA &&
		cur.WorkflowPath == m.WorkflowPath && cur.Event == m.Event && samePRNumber(cur.PRNumber, m.PRNumber) &&
		sameSelection(cur.Selected, m.Selected)
}

func samePRNumber(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// BuildLifecycleView mirrors LoadPRBuildLifecycle plus LoadPRBuildMetadata:
// status, revisions, whether any job started, and the build metadata only
// when it still matches the current revision.
func (s *PRState) BuildLifecycleView() BuildLifecycle {
	if s == nil {
		return BuildLifecycle{}
	}
	out := BuildLifecycle{
		Found:    true,
		Status:   s.RegistryStatus(),
		HeadSHA:  s.HeadSHA,
		MergeSHA: s.MergeSHA,
		Started:  len(s.StartedJobs) > 0,
		Agent:    strings.TrimSpace(s.Agent),
	}
	if m := s.BuildMetadata; m != nil && s.buildMetadataRevisionMatches(m.SourceSHA) {
		clone := *m
		out.BuildMetadata = &clone
		out.HasBuildMetadata = true
	}
	return out
}

// claimRowOwner claims owner for a newly inserted row unless Register
// already claimed one, in which case it only fills an empty slot.
func (s *PRState) claimRowOwner(owner string) bool {
	if s.Registered {
		return s.ApplyAgentHint(owner)
	}
	return s.ClaimOwner(owner)
}
