package prstate

// Pure state transitions. Every function below is a deterministic transition
// over PRState/RepoState with no I/O, no clocks, and no Restate calls, so
// chunks 03a-06 can journal them directly. The transitions port
// services/synapse/internal/github StateTracker semantics one-for-one into
// Restate-owned state, including the RecordFailedCheckRunDetailed
// triple-return with SHA-change reset, transactional rollback, the
// ClearFailedCISHA seven-way fan-out (updated-for-main survives, exactly like
// the tracker), BOTH deploy-dedup keys, and durable ResolvePRBySHA replacing
// commitSHAPRs with ORDER BY updated_at DESC LIMIT 1 ambiguity ordering.

import (
	"slices"
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

// ApplyEvent computes the pure disposition for one inbound event: stale SHAs
// drop, same-epoch terminal states absorb, and a newer reopen epoch applies.
func (s *PRState) ApplyEvent(ev PREvent) EventDisposition {
	if s == nil {
		return DispositionDropStale
	}
	if s.Terminal.Absorbing() && ev.ReopenEpoch <= s.ReopenEpoch {
		return DispositionAbsorbTerminal
	}
	if ref := strings.TrimSpace(ev.HeadSHA); ref == "" {
		ref = strings.TrimSpace(ev.MergeSHA)
		if ref != "" && s.HeadSHA != "" && !strings.EqualFold(s.HeadSHA, ref) {
			return DispositionDropStale
		}
		return DispositionApply
	} else if s.HeadSHA != "" && IsStaleEvent(s.HeadSHA, ref) {
		return DispositionDropStale
	}
	return DispositionApply
}

// RepoState is the Restate-owned per-repo state: ONLY the bounded main-push
// index, dispatched-SHA tombstones scoped by generation, and the journaled
// index of PR keys touched (rebuild enumerates it; snapshot restore is the
// fallback). All heavy per-PR work stays in PRState.
type RepoState struct {
	Repo          string
	MainPushIndex []string
	LatestMainSHA string
	TouchedPRKeys []string
	touchedSet    map[string]bool
	Tombstones    map[string]uint64
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
func (s *RepoState) TouchPRKey(prKey string) {
	if s == nil || strings.TrimSpace(prKey) == "" {
		return
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
		return
	}
	s.touchedSet[prKey] = true
	s.TouchedPRKeys = append(s.TouchedPRKeys, prKey)
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

// SHAEntry is one durable SHA lookup row. UpdatedAt orders ambiguous
// duplicate rows newest-first, mirroring the projection query.
type SHAEntry struct {
	Repo      string
	SHA       string
	PRNumber  int
	Branch    string
	UpdatedAt int64
}

// ResolvePRBySHA resolves a commit SHA to its PR, replacing the deleted
// commitSHAPRs map with a durable lookup. Ambiguous duplicates resolve by
// ORDER BY updated_at DESC LIMIT 1: the newest row wins.
func ResolvePRBySHA(entries []SHAEntry, repo, sha string) (int, string, bool) {
	normRepo := strings.ToLower(NormalizeRepo(repo))
	normSHA := strings.ToLower(strings.TrimSpace(sha))
	if normRepo == "" || normSHA == "" || IsZeroSHA(normSHA) {
		return 0, "", false
	}
	best := -1
	for i, e := range entries {
		if strings.ToLower(NormalizeRepo(e.Repo)) != normRepo {
			continue
		}
		if strings.ToLower(strings.TrimSpace(e.SHA)) != normSHA {
			continue
		}
		if e.PRNumber <= 0 {
			continue
		}
		if best == -1 || e.UpdatedAt >= entries[best].UpdatedAt {
			best = i
		}
	}
	if best == -1 {
		return 0, "", false
	}
	return entries[best].PRNumber, entries[best].Branch, true
}
