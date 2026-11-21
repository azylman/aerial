package prstate

// Pure state transitions. Every function below is a deterministic transition
// over PRState/RepoState with no I/O, no clocks, and no Restate calls, so
// chunks 03a-06 can journal them directly. The transitions port
// services/synapse/internal/github StateTracker semantics one-for-one into
// Restate-owned state, including the RecordFailedCheckRunDetailed
// triple-return with SHA-change reset, transactional rollback, the
// ClearFailedCISHA seven-way fan-out (updated-for-main survives, exactly like
// the tracker), BOTH deploy-dedup keys, and the RepoObject durable SHA index
// replacing commitSHAPRs with newest-writer-wins ambiguity ordering (the
// ORDER BY updated_at DESC LIMIT 1 semantics of the registry lookup).

import (
	"slices"
	"sort"
	"strings"
)

// FailedCheckState is the frozen nested per-SHA check failure shape:
// headSHA, everFailed, activeFailed, roboRevActiveFailed.
type FailedCheckState struct {
	HeadSHA             string
	EverFailed          map[string]bool
	ActiveFailed        map[string]bool
	RoboRevActiveFailed bool
}

// PRState is the Restate-owned per-PR state. It replaces the in-memory
// StateTracker maps for one PR with a single serializable struct.
type PRState struct {
	Repo                 string
	PRNumber             int
	Agent                string
	HeadSHA              string
	GateGeneration       uint64
	ReopenEpoch          uint64
	Terminal             TerminalState
	Checks               FailedCheckState
	FailedCISHA          string
	NonRoboRevSHA        string
	CancelledSHA         string
	CollaboratorAlerted  string
	CollaboratorConflict string
	ConflictPublished    string
	UpdatedForMain       bool
	DispatchedEffects    map[string]bool
	DeployPRDispatched   bool
	DeploySHAJob         map[string]bool

	// Registry row: everything a pr_registry row holds, so PRObject can
	// become its single writer. See registry_state.go for the transitions.
	Registered             bool
	Branch                 string
	MergeSHA               string
	TargetID               string
	Title                  string
	Status                 string
	BotID                  string
	Jobs                   []string
	StartedJobs            []string
	DeployedJobs           []string
	JobStates              map[string]string
	KustomizationRequests  map[string]KustomizationRequest
	DeployedKustomizations []string
	DeployFailedPublished  bool
	DeploySuccessPending   bool
	BuildMetadata          *BuildMetadata
	Banner                 string
	CIChecks               map[string]string
	CIFailCount            int
	VanadisChunkID         string
	WidgetState            string

	// ProjectedKey is the ProjectionJournalKey of the last pr_registry row
	// PRObject.Project wrote, so an identical snapshot is never rewritten.
	ProjectedKey string

	// IntendedEffects is the bounded ring of effects prstate recorded but
	// skipped because synapse owned the event's effects (legacy). Shadow
	// gates compare it key for key with synapse's [synapse-effect] log.
	IntendedEffects []IntendedEffect

	// PendingDeployFailure is the armed deploy-failure deadline for a
	// restate-owned deploy attempt that failed; DeployFailureSeq numbers
	// the deadlines so a stale timer never fires a newer attempt.
	PendingDeployFailure *PendingDeployFailure
	DeployFailureSeq     uint64

	// ReadinessRetries numbers the accepted readiness retries, so each
	// operator retry re-requests its reconcile under its own effect key
	// without minting a generation.
	ReadinessRetries uint64

	// RolloutIndexed is the rollout-index membership last sent for this PR
	// (InRollout); RolloutIndexSeq numbers those sends so the index drops a
	// reordered older one.
	RolloutIndexed  bool
	RolloutIndexSeq uint64
}

// NewPRState returns an empty PR state for repo#prNumber.
func NewPRState(repo string, prNumber int) *PRState {
	return &PRState{
		Repo:     NormalizeRepo(repo),
		PRNumber: prNumber,
		Checks: FailedCheckState{
			EverFailed:   make(map[string]bool),
			ActiveFailed: make(map[string]bool),
		},
		DispatchedEffects: make(map[string]bool),
		DeploySHAJob:      make(map[string]bool),
	}
}

// ClaimOwner sets the owning agent from an authoritative source: register,
// Hydrate from the registry row, or PR-body bot metadata on opened. A blank
// agent never clears a stored owner. Returns true when the owner changed.
func (s *PRState) ClaimOwner(agent string) bool {
	agent = strings.TrimSpace(agent)
	if s == nil || agent == "" || s.Agent == agent {
		return false
	}
	s.Agent = agent
	return true
}

// ApplyAgentHint fills the owner from a lifecycle event's agent hint only
// when no owner is stored; a hint never overwrites an existing owner.
// Returns true when the hint was adopted.
func (s *PRState) ApplyAgentHint(agent string) bool {
	if s == nil || strings.TrimSpace(s.Agent) != "" {
		return false
	}
	return s.ClaimOwner(agent)
}

// OwnerAgent returns the stored owner, falling back to DefaultAgent when no
// owner has been claimed or hinted yet.
func (s *PRState) OwnerAgent() string {
	if s == nil || strings.TrimSpace(s.Agent) == "" {
		return DefaultAgent
	}
	return s.Agent
}

// RecordFailedCISHA records the failed CI head SHA. Returns true when the SHA
// is newly recorded or changed.
func (s *PRState) RecordFailedCISHA(headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || s.Repo == "" || s.PRNumber <= 0 || headSHA == "" || IsZeroSHA(headSHA) {
		return false
	}
	if strings.EqualFold(s.FailedCISHA, headSHA) {
		return false
	}
	s.FailedCISHA = headSHA
	s.HeadSHA = headSHA
	return true
}

// HasFailedCISHA reports whether a failed CI SHA is recorded; a non-empty
// query must match the recorded SHA.
func (s *PRState) HasFailedCISHA(headSHA string) bool {
	if s == nil || s.PRNumber <= 0 || s.FailedCISHA == "" {
		return false
	}
	if clean := strings.TrimSpace(headSHA); clean != "" {
		return strings.EqualFold(s.FailedCISHA, clean)
	}
	return true
}

// GetFailedCISHA returns the recorded failed CI SHA.
func (s *PRState) GetFailedCISHA() (string, bool) {
	if s == nil || s.FailedCISHA == "" {
		return "", false
	}
	return s.FailedCISHA, true
}

// IsCIFailedForSHA reports whether the PR is CI-failed for the given head
// SHA from the Restate-owned failure slots. It mirrors the projection's
// ci_failed status: a recorded failed-CI SHA or any active failed check run
// on the queried head. An empty query matches any recorded failure; a zero
// SHA never matches.
func (s *PRState) IsCIFailedForSHA(headSHA string) bool {
	if s == nil || s.PRNumber <= 0 {
		return false
	}
	if clean := strings.TrimSpace(headSHA); clean != "" {
		if IsZeroSHA(clean) {
			return false
		}
		if s.HasFailedCISHA(clean) {
			return true
		}
		return s.HasActiveFailedCheckRuns(clean)
	}
	return s.HasFailedCISHA("") || s.HasActiveFailedCheckRuns(s.Checks.HeadSHA)
}

// ClearFailedCISHA clears all failure, alert, and conflict tracking for the
// PR: failed, non-roborev, cancelled, granular check runs, collaborator
// alerted/conflict, and conflict-published SHAs. UpdatedForMain survives,
// exactly like StateTracker.ClearFailedCISHA.
func (s *PRState) ClearFailedCISHA() {
	if s == nil {
		return
	}
	s.FailedCISHA = ""
	s.NonRoboRevSHA = ""
	s.CancelledSHA = ""
	s.Checks = FailedCheckState{
		EverFailed:   make(map[string]bool),
		ActiveFailed: make(map[string]bool),
	}
	s.CollaboratorAlerted = ""
	s.CollaboratorConflict = ""
	s.ConflictPublished = ""
}

// RecordNonRoboRevFailedSHA caches a non-roborev check failure head SHA.
func (s *PRState) RecordNonRoboRevFailedSHA(headSHA string) {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || headSHA == "" || IsZeroSHA(headSHA) {
		return
	}
	s.NonRoboRevSHA = headSHA
}

// DeleteNonRoboRevFailedSHA removes the non-roborev failure slot.
func (s *PRState) DeleteNonRoboRevFailedSHA() {
	if s == nil {
		return
	}
	s.NonRoboRevSHA = ""
}

// HasNonRoboRevFailedSHA reports whether a non-roborev failure is tracked;
// a non-empty query must match.
func (s *PRState) HasNonRoboRevFailedSHA(headSHA string) bool {
	if s == nil || s.PRNumber <= 0 || s.NonRoboRevSHA == "" {
		return false
	}
	if clean := strings.TrimSpace(headSHA); clean != "" {
		return strings.EqualFold(s.NonRoboRevSHA, clean)
	}
	return true
}

// RecordCancelledCheckSHA caches a cancelled check run SHA.
func (s *PRState) RecordCancelledCheckSHA(headSHA string) {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || headSHA == "" || IsZeroSHA(headSHA) {
		return
	}
	s.CancelledSHA = headSHA
}

// DeleteCancelledCheckSHA removes the cancelled check slot.
func (s *PRState) DeleteCancelledCheckSHA() {
	if s == nil {
		return
	}
	s.CancelledSHA = ""
}

// HasCancelledCheckSHA reports whether a cancelled check matches the SHA.
func (s *PRState) HasCancelledCheckSHA(headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || headSHA == "" || s.CancelledSHA == "" {
		return false
	}
	return strings.EqualFold(s.CancelledSHA, headSHA)
}

// IsCachedFailedCISHA reports whether headSHA appears in any of the failed,
// non-roborev, or cancelled SHA slots.
func (s *PRState) IsCachedFailedCISHA(headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || s.Repo == "" || s.PRNumber <= 0 || headSHA == "" || IsZeroSHA(headSHA) {
		return false
	}
	for _, v := range []string{s.FailedCISHA, s.NonRoboRevSHA, s.CancelledSHA} {
		if v != "" && strings.EqualFold(v, headSHA) {
			return true
		}
	}
	return false
}

// RecordFailedCheckRunDetailed records a check failure into the granular
// tracker and returns (emitAdditional, wasActive, hadPriorEverFailed). A head
// SHA change resets the nested shape before recording.
func (s *PRState) RecordFailedCheckRunDetailed(headSHA, checkName string) (emitAdditional, wasActive, hadPriorEverFailed bool) {
	normHeadSHA := strings.TrimSpace(headSHA)
	normCheck := CanonicalFailedCheckKey(checkName)
	if s == nil || s.PRNumber <= 0 || normHeadSHA == "" || IsZeroSHA(normHeadSHA) || normCheck == "" {
		return false, false, false
	}
	if s.Checks.EverFailed == nil {
		s.Checks.EverFailed = make(map[string]bool)
	}
	if s.Checks.ActiveFailed == nil {
		s.Checks.ActiveFailed = make(map[string]bool)
	}
	if !strings.EqualFold(s.Checks.HeadSHA, normHeadSHA) {
		s.Checks.HeadSHA = normHeadSHA
		s.Checks.EverFailed = make(map[string]bool)
		s.Checks.ActiveFailed = make(map[string]bool)
		s.Checks.RoboRevActiveFailed = false
	}
	hadPriorFailure := len(s.Checks.EverFailed) > 0 || s.Checks.RoboRevActiveFailed
	if IsRoboRevCheck(checkName) {
		wasActive = s.Checks.RoboRevActiveFailed
		hadPriorEverFailed = s.Checks.EverFailed["roborev"]
		s.Checks.EverFailed["roborev"] = true
		s.Checks.RoboRevActiveFailed = true
		return hadPriorFailure && !wasActive, wasActive, hadPriorEverFailed
	}
	wasActive = s.Checks.ActiveFailed[normCheck]
	hadPriorEverFailed = s.Checks.EverFailed[normCheck]
	s.Checks.EverFailed[normCheck] = true
	s.Checks.ActiveFailed[normCheck] = true
	return hadPriorFailure && !wasActive, wasActive, hadPriorEverFailed
}

// RollbackFailedCheckRun restores prior failure state when dispatch failed.
// It is a no-op when the check was already active before recording or when
// the entry no longer matches headSHA.
func (s *PRState) RollbackFailedCheckRun(headSHA, checkName string, wasActive, hadPriorEverFailed bool) {
	normCheck := CanonicalFailedCheckKey(checkName)
	if s == nil || s.PRNumber <= 0 || normCheck == "" || wasActive {
		return
	}
	if !strings.EqualFold(s.Checks.HeadSHA, strings.TrimSpace(headSHA)) {
		return
	}
	if IsRoboRevCheck(checkName) {
		s.Checks.RoboRevActiveFailed = false
		if !hadPriorEverFailed {
			delete(s.Checks.EverFailed, "roborev")
		}
		return
	}
	delete(s.Checks.ActiveFailed, normCheck)
	if !hadPriorEverFailed {
		delete(s.Checks.EverFailed, normCheck)
	}
	if len(s.Checks.ActiveFailed) == 0 {
		s.NonRoboRevSHA = ""
	}
}

// ClearActiveFailedCheckRun removes one active failure for the check name on
// the matching head SHA; ever-failed history is kept.
func (s *PRState) ClearActiveFailedCheckRun(headSHA, checkName string) {
	normCheck := CanonicalFailedCheckKey(checkName)
	if s == nil || normCheck == "" {
		return
	}
	if !strings.EqualFold(s.Checks.HeadSHA, strings.TrimSpace(headSHA)) {
		return
	}
	if IsRoboRevCheck(checkName) {
		s.Checks.RoboRevActiveFailed = false
		return
	}
	delete(s.Checks.ActiveFailed, normCheck)
}

// ClearFailedCheckRun clears one active failure and reports whether an active
// entry was cleared.
func (s *PRState) ClearFailedCheckRun(headSHA, checkName string) bool {
	if CanonicalFailedCheckKey(checkName) == "" {
		return false
	}
	if s == nil || !strings.EqualFold(s.Checks.HeadSHA, strings.TrimSpace(headSHA)) {
		return false
	}
	if IsRoboRevCheck(checkName) {
		was := s.Checks.RoboRevActiveFailed
		s.Checks.RoboRevActiveFailed = false
		return was
	}
	normCheck := CanonicalFailedCheckKey(checkName)
	was := s.Checks.ActiveFailed[normCheck]
	delete(s.Checks.ActiveFailed, normCheck)
	return was
}

// HasActiveFailedCheckRuns reports whether any check failure is active for headSHA.
func (s *PRState) HasActiveFailedCheckRuns(headSHA string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || normHeadSHA == "" || IsZeroSHA(normHeadSHA) {
		return false
	}
	if !strings.EqualFold(s.Checks.HeadSHA, normHeadSHA) {
		return false
	}
	return s.Checks.RoboRevActiveFailed || len(s.Checks.ActiveFailed) > 0
}

// HasActiveNonRoboRevFailures reports whether any non-roborev failure is active.
func (s *PRState) HasActiveNonRoboRevFailures(headSHA string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || normHeadSHA == "" || IsZeroSHA(normHeadSHA) {
		return false
	}
	if !strings.EqualFold(s.Checks.HeadSHA, normHeadSHA) {
		return false
	}
	return len(s.Checks.ActiveFailed) > 0
}

// HasActiveRoboRevFailure reports whether a roborev failure is active.
func (s *PRState) HasActiveRoboRevFailure(headSHA string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || normHeadSHA == "" || IsZeroSHA(normHeadSHA) {
		return false
	}
	if !strings.EqualFold(s.Checks.HeadSHA, normHeadSHA) {
		return false
	}
	return s.Checks.RoboRevActiveFailed
}

// HasActiveCheckRun reports whether checkName is an active failure for headSHA.
func (s *PRState) HasActiveCheckRun(headSHA, checkName string) bool {
	if CanonicalFailedCheckKey(checkName) == "" {
		return false
	}
	if s == nil || !strings.EqualFold(s.Checks.HeadSHA, strings.TrimSpace(headSHA)) {
		return false
	}
	if IsRoboRevCheck(checkName) {
		return s.Checks.RoboRevActiveFailed
	}
	return s.Checks.ActiveFailed[CanonicalFailedCheckKey(checkName)]
}

// HasEverFailedCheckRun reports whether checkName ever failed on headSHA.
func (s *PRState) HasEverFailedCheckRun(headSHA, checkName string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	normCheck := CanonicalFailedCheckKey(checkName)
	if s == nil || s.PRNumber <= 0 || normHeadSHA == "" || IsZeroSHA(normHeadSHA) || normCheck == "" {
		return false
	}
	if !strings.EqualFold(s.Checks.HeadSHA, normHeadSHA) {
		return false
	}
	return s.Checks.EverFailed[normCheck]
}

// CanRecoverNonGateCheckSuccess clears one non-roborev success and reports
// whether the PR can leave CI failure. It mirrors the tracker: clearing the
// last non-roborev failure also clears the non-roborev SHA slot and recovers
// only when no roborev failure stays active.
func (s *PRState) CanRecoverNonGateCheckSuccess(checkName, headSHA string) bool {
	normCheck := CanonicalFailedCheckKey(checkName)
	if s == nil || normCheck == "" {
		return false
	}
	if !strings.EqualFold(s.Checks.HeadSHA, strings.TrimSpace(headSHA)) || !s.Checks.EverFailed[normCheck] {
		return false
	}
	delete(s.Checks.ActiveFailed, normCheck)
	if len(s.Checks.ActiveFailed) == 0 {
		s.NonRoboRevSHA = ""
		return !s.Checks.RoboRevActiveFailed
	}
	return false
}

// MarkCollaboratorAlerted records a collaborator CI-failure alert for the SHA.
// Returns true when newly recorded or changed.
func (s *PRState) MarkCollaboratorAlerted(headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || headSHA == "" || IsZeroSHA(headSHA) {
		return false
	}
	if strings.EqualFold(s.CollaboratorAlerted, headSHA) {
		return false
	}
	s.CollaboratorAlerted = headSHA
	return true
}

// WasCollaboratorAlerted reports whether the collaborator alert matches;
// an empty query matches any recorded alert.
func (s *PRState) WasCollaboratorAlerted(headSHA string) bool {
	if s == nil || s.CollaboratorAlerted == "" {
		return false
	}
	if clean := strings.TrimSpace(headSHA); clean != "" {
		return strings.EqualFold(s.CollaboratorAlerted, clean)
	}
	return true
}

// ClearCollaboratorAlerted clears the alert record; a non-empty SHA clears
// only on exact match.
func (s *PRState) ClearCollaboratorAlerted(headSHA string) {
	if s == nil {
		return
	}
	if clean := strings.TrimSpace(headSHA); clean != "" {
		if !strings.EqualFold(s.CollaboratorAlerted, clean) {
			return
		}
	}
	s.CollaboratorAlerted = ""
}

// MarkCollaboratorConflict records a collaborator merge-conflict alert.
func (s *PRState) MarkCollaboratorConflict(headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || headSHA == "" || IsZeroSHA(headSHA) {
		return false
	}
	if strings.EqualFold(s.CollaboratorConflict, headSHA) {
		return false
	}
	s.CollaboratorConflict = headSHA
	return true
}

// WasCollaboratorConflict reports whether a conflict alert matches.
func (s *PRState) WasCollaboratorConflict(headSHA string) bool {
	if s == nil || s.CollaboratorConflict == "" {
		return false
	}
	if clean := strings.TrimSpace(headSHA); clean != "" {
		return strings.EqualFold(s.CollaboratorConflict, clean)
	}
	return true
}

// ClearCollaboratorConflict clears the conflict alert record.
func (s *PRState) ClearCollaboratorConflict(headSHA string) {
	if s == nil {
		return
	}
	if clean := strings.TrimSpace(headSHA); clean != "" {
		if !strings.EqualFold(s.CollaboratorConflict, clean) {
			return
		}
	}
	s.CollaboratorConflict = ""
}

// MarkConflictPublished records a conflict lifecycle publication for the SHA.
func (s *PRState) MarkConflictPublished(headSHA string) {
	normHeadSHA := strings.TrimSpace(headSHA)
	if s == nil || s.PRNumber <= 0 || normHeadSHA == "" {
		return
	}
	s.ConflictPublished = normHeadSHA
}

// WasConflictPublished reports whether a conflict event was published for the
// PR and head SHA.
func (s *PRState) WasConflictPublished(headSHA string) bool {
	if s == nil || s.PRNumber <= 0 || s.ConflictPublished == "" {
		return false
	}
	clean := strings.TrimSpace(headSHA)
	if clean == "" {
		return false
	}
	return strings.EqualFold(s.ConflictPublished, clean)
}

// ClearConflictPublished removes the conflict publication record.
func (s *PRState) ClearConflictPublished() {
	if s == nil {
		return
	}
	s.ConflictPublished = ""
}

// MarkUpdatedForMain records that the PR was updated with current main.
func (s *PRState) MarkUpdatedForMain() {
	if s == nil {
		return
	}
	s.UpdatedForMain = true
}

// WasUpdatedForMain reports the updated-for-main flag.
func (s *PRState) WasUpdatedForMain() bool {
	return s != nil && s.UpdatedForMain
}

// ClearUpdatedForMain clears the updated-for-main flag.
func (s *PRState) ClearUpdatedForMain() {
	if s == nil {
		return
	}
	s.UpdatedForMain = false
}

// RecordKustomizationRequests starts a new gate generation: it bumps
// GateGeneration and clears every journaled effect key so effects re-fire
// exactly once per generation. This is the ONLY transition that mints a
// generation or clears effect keys.
func (s *PRState) RecordKustomizationRequests() uint64 {
	if s == nil {
		return 0
	}
	s.GateGeneration++
	s.DispatchedEffects = make(map[string]bool)
	return s.GateGeneration
}

// RetryPRReadiness is the readiness-retry transition. By contract it never
// mints a generation and never clears effect keys; it is an explicit no-op
// so future retry bookkeeping has one frozen home.
func (*PRState) RetryPRReadiness() {}

// EffectKeyFor builds the generation-scoped journaled key for one effect.
func (s *PRState) EffectKeyFor(kind, headSHA, jobName, checkName string) EffectKey {
	if s == nil {
		return EffectKey{}
	}
	return EffectKey{
		Repo:           s.Repo,
		PRNumber:       s.PRNumber,
		HeadSHA:        strings.TrimSpace(headSHA),
		GateGeneration: s.GateGeneration,
		Kind:           strings.TrimSpace(kind),
		JobName:        strings.TrimSpace(jobName),
		CheckName:      checkName,
	}
}

// MarkEffectDispatched records a journaled effect as fired.
func (s *PRState) MarkEffectDispatched(k EffectKey) {
	if s == nil {
		return
	}
	if s.DispatchedEffects == nil {
		s.DispatchedEffects = make(map[string]bool)
	}
	s.DispatchedEffects[k.String()] = true
}

// WasEffectDispatched reports whether the journaled effect already fired.
func (s *PRState) WasEffectDispatched(k EffectKey) bool {
	if s == nil || s.DispatchedEffects == nil {
		return false
	}
	return s.DispatchedEffects[k.String()]
}

// RecordDeployedPR marks the PR-keyed deploy dedup slot.
func (s *PRState) RecordDeployedPR() {
	if s == nil {
		return
	}
	s.DeployPRDispatched = true
}

// WasDeployedPR reports the PR-keyed deploy dedup slot.
func (s *PRState) WasDeployedPR() bool {
	return s != nil && s.DeployPRDispatched
}

// RecordDeployedSHAJob marks the job-plus-SHA deploy dedup slot.
func (s *PRState) RecordDeployedSHAJob(jobName, sha string) {
	if s == nil {
		return
	}
	if s.DeploySHAJob == nil {
		s.DeploySHAJob = make(map[string]bool)
	}
	s.DeploySHAJob[DeploySHAJobKey(s.Repo, jobName, sha)] = true
}

// WasDeployedSHAJob reports the job-plus-SHA deploy dedup slot.
func (s *PRState) WasDeployedSHAJob(jobName, sha string) bool {
	if s == nil || s.DeploySHAJob == nil {
		return false
	}
	return s.DeploySHAJob[DeploySHAJobKey(s.Repo, jobName, sha)]
}

// MarkTerminal absorbs the PR into a terminal state for the current epoch.
func (s *PRState) MarkTerminal(t TerminalState) {
	if s == nil {
		return
	}
	s.Terminal = t
}

// Reopen starts a new reopen epoch: it clears the terminal state so
// terminal-absorbing never bricks a reopen.
func (s *PRState) Reopen() {
	if s == nil {
		return
	}
	s.Terminal = TerminalNone
	s.ReopenEpoch++
}

// ApplyDeployEvent computes the disposition for one deploy-phase event
// (kustomization requested/status, rollout progress, deploy dispatched,
// success, or failure, build metadata). Deploys happen after merge, so a
// same-epoch merged PR still applies them as long as every carried SHA is
// the PR head or merge SHA (or the merge SHA is not known yet); a mismatch
// drops stale. Closed-unmerged PRs stay fully absorbing, and every other
// state falls through to ApplyEvent.
func (s *PRState) ApplyDeployEvent(ev PREvent) EventDisposition {
	if s == nil {
		return DispositionDropStale
	}
	if s.Terminal != TerminalMerged || ev.ReopenEpoch > s.ReopenEpoch {
		return s.ApplyEvent(ev)
	}
	for _, ref := range []string{ev.HeadSHA, ev.MergeSHA} {
		clean := strings.TrimSpace(ref)
		if clean == "" || IsZeroSHA(clean) || strings.TrimSpace(s.MergeSHA) == "" {
			continue
		}
		if !strings.EqualFold(clean, s.HeadSHA) && !strings.EqualFold(clean, s.MergeSHA) {
			return DispositionDropStale
		}
	}
	return DispositionApply
}

// ApplyEvent computes the pure disposition for one inbound PR-lifecycle
// event: stale SHAs drop, same-epoch terminal states absorb, and a newer
// reopen epoch applies. Deploy-phase events use ApplyDeployEvent instead.
func (s *PRState) ApplyEvent(ev PREvent) EventDisposition {
	if s == nil {
		return DispositionDropStale
	}
	if s.Terminal.Absorbing() && ev.ReopenEpoch <= s.ReopenEpoch {
		return DispositionAbsorbTerminal
	}
	head := strings.TrimSpace(ev.HeadSHA)
	if head == "" || IsMergeOnlyRef(head, ev.MergeSHA) {
		// A merge-only reference (direct deploy alert, sync success, or a
		// squash-merge push) names the merge commit, which is never the PR
		// head: judge it against the stored merge SHA, and apply while no
		// merge SHA is known yet.
		merge := strings.TrimSpace(ev.MergeSHA)
		if merge != "" && !IsZeroSHA(merge) && s.MergeSHA != "" && !strings.EqualFold(s.MergeSHA, merge) && !strings.EqualFold(s.HeadSHA, merge) {
			return DispositionDropStale
		}
		return DispositionApply
	}
	if s.HeadSHA != "" && IsStaleEvent(s.HeadSHA, head) {
		return DispositionDropStale
	}
	return DispositionApply
}

// IsMergeOnlyRef reports whether an event's head SHA is only its merge SHA
// (synapse resolves a squash-merge push or deploy alert to the PR and sends
// the pushed commit as both).
func IsMergeOnlyRef(headSHA, mergeSHA string) bool {
	head, merge := strings.TrimSpace(headSHA), strings.TrimSpace(mergeSHA)
	return head != "" && !IsZeroSHA(head) && strings.EqualFold(head, merge)
}

// RepoState is the Restate-owned per-repo state: the bounded main-push
// index, dispatched-SHA tombstones scoped by generation, the journaled index
// of PR keys touched (rebuild enumerates it; snapshot restore is the
// fallback), and the routing index RepoObject keeps for itself: the
// SHA->PR map, per-PR open/closed/deploying status, and SHA-only events held
// until their SHA is indexed. PRObject feeds the routing index with one-way
// sends; RepoObject never reads or writes PR state. All heavy per-PR work
// stays in PRState.
type RepoState struct {
	Repo          string
	MainPushIndex []string
	LatestMainSHA string
	// FannedOutMainSHA is the main SHA whose main-advanced fan-out already
	// went out; the fan-out effect fires once per main SHA.
	FannedOutMainSHA string
	TouchedPRKeys    []string
	touchedSet       map[string]bool
	Tombstones       map[string]uint64
	// SHAIndex maps a lower-cased head or merge SHA to the PR that most
	// recently claimed it.
	SHAIndex map[string]SHAIndexEntry
	// PRs is the per-PR routing status keyed by PR number.
	PRs map[int]*RepoPRStatus
	// Held are SHA-only events whose SHA was not indexed yet, oldest first.
	Held []HeldSHAEvent
	// Seq is the monotonic write counter ordering index entries, closes,
	// and held event IDs. It replaces wall-clock updated_at ordering.
	Seq uint64
	// RepoEffects are the keys of repo-level GitHub effects (auto-update
	// cascades, workflow reruns) already executed or recorded, oldest
	// first, bounded by MaxRepoEffects.
	RepoEffects []string
	// IntendedEffects is the bounded ring of repo effects recorded but
	// skipped because synapse owned the triggering event (legacy).
	IntendedEffects []IntendedEffect
}

// MaxRepoEffects bounds the RepoObject repo-effect key history.
const MaxRepoEffects = 64

// WasRepoEffect reports whether the repo effect key already fired or was
// recorded.
func (s *RepoState) WasRepoEffect(k EffectKey) bool {
	return s != nil && slices.Contains(s.RepoEffects, k.String())
}

// MarkRepoEffect records one repo effect key, keeping the newest
// MaxRepoEffects keys.
func (s *RepoState) MarkRepoEffect(k EffectKey) {
	if s == nil || s.WasRepoEffect(k) {
		return
	}
	s.RepoEffects = append(s.RepoEffects, k.String())
	if over := len(s.RepoEffects) - MaxRepoEffects; over > 0 {
		s.RepoEffects = append([]string(nil), s.RepoEffects[over:]...)
	}
}

// RecordIntendedEffect appends one intended repo effect, keeping the newest
// MaxIntendedEffects entries.
func (s *RepoState) RecordIntendedEffect(e IntendedEffect) {
	if s == nil {
		return
	}
	s.IntendedEffects = append(s.IntendedEffects, e)
	if over := len(s.IntendedEffects) - MaxIntendedEffects; over > 0 {
		s.IntendedEffects = append([]IntendedEffect(nil), s.IntendedEffects[over:]...)
	}
}

// NewRepoState returns an empty repo state.
func NewRepoState(repo string) *RepoState {
	return &RepoState{
		Repo:       NormalizeRepo(repo),
		Tombstones: make(map[string]uint64),
		touchedSet: make(map[string]bool),
	}
}

// RecordMainPushSHA records a main push SHA into the bounded index.
// Returns true when newly recorded. The index keeps at most the latest
// MaxMainPushIndex entries; LatestMainSHA tracks the newest push.
func (s *RepoState) RecordMainPushSHA(sha string) bool {
	cleanSHA := strings.ToLower(strings.TrimSpace(sha))
	if s == nil || s.Repo == "" || cleanSHA == "" || IsZeroSHA(cleanSHA) {
		return false
	}
	if slices.Contains(s.MainPushIndex, cleanSHA) {
		return false
	}
	s.MainPushIndex = append(s.MainPushIndex, cleanSHA)
	if len(s.MainPushIndex) > MaxMainPushIndex {
		s.MainPushIndex = s.MainPushIndex[len(s.MainPushIndex)-MaxMainPushIndex:]
	}
	s.LatestMainSHA = cleanSHA
	return true
}

// TouchPRKey records a PR key in the journaled rebuild index (idempotent).
// Returns true when the key is newly recorded.
func (s *RepoState) TouchPRKey(prKey string) bool {
	if s == nil || strings.TrimSpace(prKey) == "" {
		return false
	}
	if s.touchedSet == nil {
		// Rebuild after a JSON/Restate restore, which preserves
		// TouchedPRKeys but drops the unexported index.
		s.touchedSet = make(map[string]bool, len(s.TouchedPRKeys))
		for _, k := range s.TouchedPRKeys {
			s.touchedSet[k] = true
		}
	}
	if s.touchedSet[prKey] {
		return false
	}
	s.touchedSet[prKey] = true
	s.TouchedPRKeys = append(s.TouchedPRKeys, prKey)
	return true
}

// PRKeys returns the journaled index of PR keys touched.
func (s *RepoState) PRKeys() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.TouchedPRKeys...)
}

// RecordTombstone scopes a dispatched SHA to its gate generation.
func (s *RepoState) RecordTombstone(sha string, generation uint64) {
	if s == nil {
		return
	}
	if s.Tombstones == nil {
		s.Tombstones = make(map[string]uint64)
	}
	s.Tombstones[strings.ToLower(strings.TrimSpace(sha))] = generation
}

// HasTombstone reports whether a dispatched-SHA tombstone exists.
func (s *RepoState) HasTombstone(sha string) bool {
	if s == nil || s.Tombstones == nil {
		return false
	}
	_, ok := s.Tombstones[strings.ToLower(strings.TrimSpace(sha))]
	return ok
}

// EvictTombstonesBelowGeneration drops tombstones below minGen. Eviction is
// by generation, never by count alone.
func (s *RepoState) EvictTombstonesBelowGeneration(minGen uint64) {
	if s == nil || s.Tombstones == nil {
		return
	}
	for sha, gen := range s.Tombstones {
		if gen < minGen {
			delete(s.Tombstones, sha)
		}
	}
}

// Routing index bounds. Open PRs keep their most recent SHAs; closed PRs
// keep their SHAs only until MaxClosedPRsIndexed newer closes evict them, so
// a squash-merge push or late check run still resolves shortly after close
// without the index growing forever.
const (
	MaxSHAsPerPR        = 20
	MaxClosedPRsIndexed = 100
	MaxHeldSHAEvents    = 64
)

// SHAIndexEntry is one durable SHA lookup row. Seq orders ambiguous claims:
// the newest claim wins, mirroring ORDER BY updated_at DESC LIMIT 1.
type SHAIndexEntry struct {
	PRNumber int
	Branch   string
	Seq      uint64
}

// RepoPRStatus is the routing status RepoObject keeps per PR. Epoch mirrors
// the PRObject reopen epoch: a close absorbs same-epoch updates and only a
// newer epoch reopens. Deploying replaces the registry ListDeployingPRs
// scan for kustomization status fan-out.
type RepoPRStatus struct {
	Branch           string
	Epoch            uint64
	Closed           bool
	ClosedSeq        uint64
	Deploying        bool
	DeploySHA        string
	DeployGeneration uint64
}

// HeldSHAEvent is one SHA-only event waiting for its SHA to be indexed.
type HeldSHAEvent struct {
	ID    uint64
	Kind  string
	Event SHAEvent
}

func normalizeIndexSHA(sha string) string {
	clean := strings.ToLower(strings.TrimSpace(sha))
	if clean == "" || IsZeroSHA(clean) {
		return ""
	}
	return clean
}

func (s *RepoState) nextSeq() uint64 {
	s.Seq++
	return s.Seq
}

func (s *RepoState) ensureIndex() {
	if s.SHAIndex == nil {
		s.SHAIndex = make(map[string]SHAIndexEntry)
	}
	if s.PRs == nil {
		s.PRs = make(map[int]*RepoPRStatus)
	}
}

// ApplyPRIndex applies one PRObject index update: it records the PR status
// transition and maps the carried head and merge SHAs to the PR. Updates
// from an older reopen epoch are ignored entirely. It returns whether state
// changed and the newly indexed SHAs that have held events to replay.
func (s *RepoState) ApplyPRIndex(ev PRIndexEvent) (bool, []string) {
	if s == nil || s.Repo == "" || ev.PRNumber <= 0 {
		return false, nil
	}
	s.ensureIndex()
	changed := s.TouchPRKey(PRObjectKey(s.Repo, ev.PRNumber))
	status := s.PRs[ev.PRNumber]
	if status == nil {
		status = &RepoPRStatus{Epoch: ev.ReopenEpoch}
		s.PRs[ev.PRNumber] = status
		changed = true
	}
	if ev.ReopenEpoch < status.Epoch {
		return changed, nil
	}
	if ev.ReopenEpoch > status.Epoch {
		status.Epoch = ev.ReopenEpoch
		status.Closed = false
		status.ClosedSeq = 0
		changed = true
	}
	if branch := strings.TrimSpace(ev.Branch); branch != "" && branch != status.Branch {
		status.Branch = branch
		changed = true
	}
	if s.applyIndexLifecycle(status, ev) {
		changed = true
	}
	var replay []string
	for _, sha := range []string{ev.HeadSHA, ev.MergeSHA} {
		clean := normalizeIndexSHA(sha)
		if clean == "" {
			continue
		}
		if cur, ok := s.SHAIndex[clean]; ok && cur.PRNumber == ev.PRNumber && cur.Branch == status.Branch {
			continue
		}
		s.SHAIndex[clean] = SHAIndexEntry{PRNumber: ev.PRNumber, Branch: status.Branch, Seq: s.nextSeq()}
		changed = true
		if s.hasHeld(clean) && !slices.Contains(replay, clean) {
			replay = append(replay, clean)
		}
	}
	s.evictPRSHAs(ev.PRNumber)
	s.evictClosedPRs()
	return changed, replay
}

// applyIndexLifecycle applies the lifecycle op of one index update to the
// PR status. Merged PRs leave the open set but stay deploy candidates;
// closed (unmerged) PRs also leave the deploying set.
func (s *RepoState) applyIndexLifecycle(status *RepoPRStatus, ev PRIndexEvent) bool {
	switch ev.Lifecycle {
	case IndexMerged, IndexClosed:
		changed := false
		if !status.Closed {
			status.Closed = true
			status.ClosedSeq = s.nextSeq()
			changed = true
		}
		if ev.Lifecycle == IndexClosed && status.Deploying {
			status.Deploying = false
			status.DeploySHA = ""
			changed = true
		}
		return changed
	case IndexDeploying:
		sha := normalizeIndexSHA(ev.MergeSHA)
		if sha == "" {
			sha = normalizeIndexSHA(ev.HeadSHA)
		}
		if status.Deploying && status.DeploySHA == sha && status.DeployGeneration == ev.Generation {
			return false
		}
		status.Deploying = true
		status.DeploySHA = sha
		status.DeployGeneration = ev.Generation
		return true
	case IndexDeployed:
		if !status.Deploying || ev.Generation < status.DeployGeneration {
			return false
		}
		status.Deploying = false
		status.DeploySHA = ""
		return true
	default:
		return false
	}
}

// evictPRSHAs keeps at most MaxSHAsPerPR index entries for one PR, dropping
// the oldest claims first.
func (s *RepoState) evictPRSHAs(prNumber int) {
	type claim struct {
		sha string
		seq uint64
	}
	var claims []claim
	for sha, e := range s.SHAIndex {
		if e.PRNumber == prNumber {
			claims = append(claims, claim{sha: sha, seq: e.Seq})
		}
	}
	if len(claims) <= MaxSHAsPerPR {
		return
	}
	sort.Slice(claims, func(i, j int) bool { return claims[i].seq < claims[j].seq })
	for _, c := range claims[:len(claims)-MaxSHAsPerPR] {
		delete(s.SHAIndex, c.sha)
	}
}

// evictClosedPRs bounds retention for closed PRs: beyond MaxClosedPRsIndexed
// the oldest closes lose their status and SHA entries. PRs still deploying
// are never evicted, so kustomization fan-out cannot lose a rollout.
func (s *RepoState) evictClosedPRs() {
	type closed struct {
		pr  int
		seq uint64
	}
	var all []closed
	for pr, st := range s.PRs {
		if st != nil && st.Closed && !st.Deploying {
			all = append(all, closed{pr: pr, seq: st.ClosedSeq})
		}
	}
	if len(all) <= MaxClosedPRsIndexed {
		return
	}
	sort.Slice(all, func(i, j int) bool { return all[i].seq < all[j].seq })
	for _, c := range all[:len(all)-MaxClosedPRsIndexed] {
		delete(s.PRs, c.pr)
		for sha, e := range s.SHAIndex {
			if e.PRNumber == c.pr {
				delete(s.SHAIndex, sha)
			}
		}
	}
}

// ResolveSHA resolves a commit SHA to the PR that most recently claimed it.
func (s *RepoState) ResolveSHA(sha string) (SHAIndexEntry, bool) {
	clean := normalizeIndexSHA(sha)
	if s == nil || clean == "" || s.SHAIndex == nil {
		return SHAIndexEntry{}, false
	}
	e, ok := s.SHAIndex[clean]
	if !ok || e.PRNumber <= 0 {
		return SHAIndexEntry{}, false
	}
	return e, true
}

// OpenPRNumbers returns the PRs whose current epoch is not closed, ascending.
func (s *RepoState) OpenPRNumbers() []int {
	if s == nil {
		return nil
	}
	var out []int
	for pr, st := range s.PRs {
		if st != nil && !st.Closed {
			out = append(out, pr)
		}
	}
	slices.Sort(out)
	return out
}

// DeployingPRNumbers returns the PRs awaiting rollout completion, ascending.
func (s *RepoState) DeployingPRNumbers() []int {
	if s == nil {
		return nil
	}
	var out []int
	for pr, st := range s.PRs {
		if st != nil && st.Deploying {
			out = append(out, pr)
		}
	}
	slices.Sort(out)
	return out
}

// PRStatus returns the routing status for one PR.
func (s *RepoState) PRStatus(prNumber int) (RepoPRStatus, bool) {
	if s == nil || s.PRs == nil || s.PRs[prNumber] == nil {
		return RepoPRStatus{}, false
	}
	return *s.PRs[prNumber], true
}

// HoldSHAEvent holds one SHA-only event until its SHA is indexed. Beyond
// MaxHeldSHAEvents the oldest held event is evicted and returned so the
// caller can log the drop.
func (s *RepoState) HoldSHAEvent(kind string, ev SHAEvent) (HeldSHAEvent, []HeldSHAEvent) {
	if s == nil {
		return HeldSHAEvent{}, nil
	}
	ev.SHA = normalizeIndexSHA(ev.SHA)
	held := HeldSHAEvent{ID: s.nextSeq(), Kind: kind, Event: ev}
	s.Held = append(s.Held, held)
	var evicted []HeldSHAEvent
	if over := len(s.Held) - MaxHeldSHAEvents; over > 0 {
		evicted = append(evicted, s.Held[:over]...)
		s.Held = append([]HeldSHAEvent(nil), s.Held[over:]...)
	}
	return held, evicted
}

func (s *RepoState) hasHeld(sha string) bool {
	return slices.ContainsFunc(s.Held, func(h HeldSHAEvent) bool { return h.Event.SHA == sha })
}

// TakeHeld removes and returns the held events for one SHA, oldest first.
func (s *RepoState) TakeHeld(sha string) []HeldSHAEvent {
	clean := normalizeIndexSHA(sha)
	if s == nil || clean == "" {
		return nil
	}
	var taken, kept []HeldSHAEvent
	for _, h := range s.Held {
		if h.Event.SHA == clean {
			taken = append(taken, h)
		} else {
			kept = append(kept, h)
		}
	}
	if len(taken) == 0 {
		return nil
	}
	s.Held = kept
	return taken
}

// ExpireHeld drops one held event by ID. Returns the dropped event and
// true when it was still held; a replayed or evicted ID reports false.
func (s *RepoState) ExpireHeld(id uint64) (HeldSHAEvent, bool) {
	if s == nil {
		return HeldSHAEvent{}, false
	}
	for i, h := range s.Held {
		if h.ID == id {
			s.Held = append(s.Held[:i:i], s.Held[i+1:]...)
			return h, true
		}
	}
	return HeldSHAEvent{}, false
}
