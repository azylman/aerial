package github

import (
	"fmt"
	"strings"
	"sync"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

// PRCheckKey computes the canonical cache key for a PR in a repository.
func PRCheckKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s#%d", model.NormalizeRepo(repo), prNumber)
}

// CanonicalFailedCheckKey canonicalizes check names, grouping roborev variants.
func CanonicalFailedCheckKey(checkName string) string {
	if IsRoboRevCheck(checkName) {
		return "roborev"
	}
	return strings.ToLower(strings.TrimSpace(checkName))
}

type failedCheckState struct {
	mu                  sync.Mutex
	headSHA             string
	everFailed          map[string]bool
	activeFailed        map[string]bool
	roboRevActiveFailed bool
}

// StateTracker encapsulates in-memory state tracking for GitHub check-runs, PRs,
// conflicts, and collaborator alert deduplication. All methods are nil-safe.
type StateTracker struct {
	nonRoboRevFailedSHAs     sync.Map
	cancelledCheckSHAs       sync.Map
	failedCheckSHAs          sync.Map
	failedCheckRuns          sync.Map
	collaboratorAlertedSHAs  sync.Map
	collaboratorConflictSHAs sync.Map
	updatedForCurrentMain    sync.Map
	lastMainPushSHA          sync.Map
	conflictPublishedSHAs    sync.Map
	commitSHAPRs             sync.Map
}

// RecordFailedCISHA records the failed CI head SHA for a PR. Returns true if the SHA
// was not previously recorded or changed.
func (t *StateTracker) RecordFailedCISHA(repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	normRepo := model.NormalizeRepo(repo)
	if t == nil || normRepo == "" || prNumber <= 0 || headSHA == "" || model.IsZeroSHA(headSHA) {
		return false
	}
	prev, loaded := t.failedCheckSHAs.Swap(PRCheckKey(normRepo, prNumber), headSHA)
	return !loaded || !strings.EqualFold(fmt.Sprint(prev), headSHA)
}

// HasFailedCISHA returns true if a failed CI SHA is recorded for the PR.
// If headSHA is non-empty, it also requires that the recorded SHA matches headSHA.
func (t *StateTracker) HasFailedCISHA(repo string, prNumber int, headSHA string) bool {
	if t == nil || prNumber <= 0 {
		return false
	}
	v, ok := t.failedCheckSHAs.Load(PRCheckKey(repo, prNumber))
	if !ok {
		return false
	}
	cleanHead := strings.TrimSpace(headSHA)
	if cleanHead == "" {
		return true
	}
	return strings.EqualFold(fmt.Sprint(v), cleanHead)
}

// GetFailedCISHA retrieves the cached failed CI head SHA for a PR.
func (t *StateTracker) GetFailedCISHA(repo string, prNumber int) (string, bool) {
	if t == nil || prNumber <= 0 {
		return "", false
	}
	v, ok := t.failedCheckSHAs.Load(PRCheckKey(repo, prNumber))
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// ClearFailedCISHA clears all failure, alert, and conflict tracking state for a PR.
func (t *StateTracker) ClearFailedCISHA(repo string, prNumber int) {
	normRepo := model.NormalizeRepo(repo)
	if t == nil || normRepo == "" || prNumber <= 0 {
		return
	}
	key := PRCheckKey(normRepo, prNumber)
	t.failedCheckSHAs.Delete(key)
	t.nonRoboRevFailedSHAs.Delete(key)
	t.cancelledCheckSHAs.Delete(key)
	t.failedCheckRuns.Delete(key)
	t.collaboratorAlertedSHAs.Delete(key)
	t.collaboratorConflictSHAs.Delete(key)
	t.conflictPublishedSHAs.Delete(key)
}

// RecordNonRoboRevFailedSHA caches the head SHA of a non-roborev check failure.
func (t *StateTracker) RecordNonRoboRevFailedSHA(repo string, prNumber int, headSHA string) {
	headSHA = strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || headSHA == "" || model.IsZeroSHA(headSHA) {
		return
	}
	t.nonRoboRevFailedSHAs.Store(PRCheckKey(repo, prNumber), headSHA)
}

// DeleteNonRoboRevFailedSHA removes the non-roborev failure tracking for a PR.
func (t *StateTracker) DeleteNonRoboRevFailedSHA(repo string, prNumber int) {
	if t == nil || prNumber <= 0 {
		return
	}
	t.nonRoboRevFailedSHAs.Delete(PRCheckKey(repo, prNumber))
}

// HasNonRoboRevFailedSHA checks if a non-roborev check failure is tracked for the PR.
// If headSHA is non-empty, it also requires that the tracked SHA matches headSHA.
func (t *StateTracker) HasNonRoboRevFailedSHA(repo string, prNumber int, headSHA string) bool {
	if t == nil || prNumber <= 0 {
		return false
	}
	v, ok := t.nonRoboRevFailedSHAs.Load(PRCheckKey(repo, prNumber))
	if !ok {
		return false
	}
	cleanHead := strings.TrimSpace(headSHA)
	if cleanHead == "" {
		return true
	}
	return strings.EqualFold(fmt.Sprint(v), cleanHead)
}

// GetNonRoboRevFailedSHA retrieves the cached non-roborev failed head SHA for a PR.
func (t *StateTracker) GetNonRoboRevFailedSHA(repo string, prNumber int) (string, bool) {
	if t == nil || prNumber <= 0 {
		return "", false
	}
	v, ok := t.nonRoboRevFailedSHAs.Load(PRCheckKey(repo, prNumber))
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// RecordCancelledCheckSHA caches that a check run was cancelled for this PR and SHA.
func (t *StateTracker) RecordCancelledCheckSHA(repo string, prNumber int, headSHA string) {
	headSHA = strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || headSHA == "" || model.IsZeroSHA(headSHA) {
		return
	}
	t.cancelledCheckSHAs.Store(PRCheckKey(repo, prNumber), headSHA)
}

// DeleteCancelledCheckSHA deletes the cancelled check record for a PR.
func (t *StateTracker) DeleteCancelledCheckSHA(repo string, prNumber int) {
	if t == nil || prNumber <= 0 {
		return
	}
	t.cancelledCheckSHAs.Delete(PRCheckKey(repo, prNumber))
}

// HasCancelledCheckSHA returns true if a cancelled check was recorded for the PR and SHA.
func (t *StateTracker) HasCancelledCheckSHA(repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || headSHA == "" {
		return false
	}
	cancelledSHA, ok := t.cancelledCheckSHAs.Load(PRCheckKey(repo, prNumber))
	return ok && strings.EqualFold(fmt.Sprint(cancelledSHA), headSHA)
}

// IsCachedFailedCISHA checks if the given headSHA is cached in failedCheckSHAs,
// nonRoboRevFailedSHAs, or cancelledCheckSHAs.
func (t *StateTracker) IsCachedFailedCISHA(repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	normRepo := model.NormalizeRepo(repo)
	if t == nil || normRepo == "" || prNumber <= 0 || headSHA == "" || model.IsZeroSHA(headSHA) {
		return false
	}
	key := PRCheckKey(normRepo, prNumber)
	for _, m := range []*sync.Map{&t.failedCheckSHAs, &t.nonRoboRevFailedSHAs, &t.cancelledCheckSHAs} {
		if v, ok := m.Load(key); ok && strings.EqualFold(fmt.Sprint(v), headSHA) {
			return true
		}
	}
	return false
}

func (t *StateTracker) loadMatchingFailedCheckState(repo string, prNumber int, headSHA string) *failedCheckState {
	normHeadSHA := strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || normHeadSHA == "" || model.IsZeroSHA(normHeadSHA) {
		return nil
	}
	val, ok := t.failedCheckRuns.Load(PRCheckKey(repo, prNumber))
	if !ok {
		return nil
	}
	entry, ok := val.(*failedCheckState)
	if !ok || entry == nil {
		return nil
	}
	return entry
}

// RecordFailedCheckRunDetailed records a check failure into the granular check run tracker.
func (t *StateTracker) RecordFailedCheckRunDetailed(repo string, prNumber int, headSHA, checkName string) (emitAdditional bool, wasActive bool, hadPriorEverFailed bool) {
	normHeadSHA := strings.TrimSpace(headSHA)
	normCheck := CanonicalFailedCheckKey(checkName)
	if t == nil || prNumber <= 0 || normHeadSHA == "" || model.IsZeroSHA(normHeadSHA) || normCheck == "" {
		return false, false, false
	}
	key := PRCheckKey(repo, prNumber)
	val, _ := t.failedCheckRuns.LoadOrStore(key, &failedCheckState{
		everFailed:   make(map[string]bool),
		activeFailed: make(map[string]bool),
	})
	entry, ok := val.(*failedCheckState)
	if !ok || entry == nil {
		return false, false, false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if !strings.EqualFold(entry.headSHA, normHeadSHA) {
		entry.headSHA = normHeadSHA
		entry.everFailed = make(map[string]bool)
		entry.activeFailed = make(map[string]bool)
		entry.roboRevActiveFailed = false
	}
	hadPriorFailure := len(entry.everFailed) > 0 || entry.roboRevActiveFailed
	if IsRoboRevCheck(checkName) {
		wasActive = entry.roboRevActiveFailed
		hadPriorEverFailed = entry.everFailed["roborev"]
		entry.everFailed["roborev"] = true
		entry.roboRevActiveFailed = true
		return hadPriorFailure && !wasActive, wasActive, hadPriorEverFailed
	}
	wasActive = entry.activeFailed[normCheck]
	hadPriorEverFailed = entry.everFailed[normCheck]
	entry.everFailed[normCheck] = true
	entry.activeFailed[normCheck] = true
	return hadPriorFailure && !wasActive, wasActive, hadPriorEverFailed
}

// RollbackFailedCheckRun restores prior failure state if dispatch failed.
func (t *StateTracker) RollbackFailedCheckRun(repo string, prNumber int, headSHA, checkName string, wasActive, hadPriorEverFailed bool) {
	normCheck := CanonicalFailedCheckKey(checkName)
	if t == nil || prNumber <= 0 || normCheck == "" || wasActive {
		return
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !strings.EqualFold(entry.headSHA, strings.TrimSpace(headSHA)) {
		return
	}
	if IsRoboRevCheck(checkName) {
		entry.roboRevActiveFailed = false
		if !hadPriorEverFailed {
			delete(entry.everFailed, "roborev")
		}
		return
	}
	delete(entry.activeFailed, normCheck)
	if !hadPriorEverFailed {
		delete(entry.everFailed, normCheck)
	}
	if len(entry.activeFailed) == 0 {
		t.nonRoboRevFailedSHAs.Delete(PRCheckKey(repo, prNumber))
	}
}

// ClearActiveFailedCheckRun removes an active failure for the specific check name.
func (t *StateTracker) ClearActiveFailedCheckRun(repo string, prNumber int, headSHA, checkName string) {
	normCheck := CanonicalFailedCheckKey(checkName)
	if normCheck == "" {
		return
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if strings.EqualFold(entry.headSHA, strings.TrimSpace(headSHA)) {
		if IsRoboRevCheck(checkName) {
			entry.roboRevActiveFailed = false
		} else {
			delete(entry.activeFailed, normCheck)
		}
	}
}

func (s *failedCheckState) hasActive(headSHA, checkName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.EqualFold(s.headSHA, headSHA) {
		return false
	}
	if IsRoboRevCheck(checkName) {
		return s.roboRevActiveFailed
	}
	return s.activeFailed[CanonicalFailedCheckKey(checkName)]
}

func (s *failedCheckState) clearActive(headSHA, checkName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.EqualFold(s.headSHA, headSHA) {
		return false
	}
	if IsRoboRevCheck(checkName) {
		was := s.roboRevActiveFailed
		s.roboRevActiveFailed = false
		return was
	}
	normCheck := CanonicalFailedCheckKey(checkName)
	was := s.activeFailed[normCheck]
	delete(s.activeFailed, normCheck)
	return was
}

// ClearFailedCheckRun clears an active failure for the check and returns true if an active entry was cleared.
func (t *StateTracker) ClearFailedCheckRun(repo string, prNumber int, headSHA, checkName string) bool {
	if CanonicalFailedCheckKey(checkName) == "" {
		return false
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	return entry != nil && entry.clearActive(strings.TrimSpace(headSHA), checkName)
}

// HasActiveFailedCheckRuns returns true if any check failure is currently active for the given head SHA.
func (t *StateTracker) HasActiveFailedCheckRuns(repo string, prNumber int, headSHA string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || normHeadSHA == "" || model.IsZeroSHA(normHeadSHA) {
		return false
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, normHeadSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !strings.EqualFold(entry.headSHA, normHeadSHA) {
		return false
	}
	return entry.roboRevActiveFailed || len(entry.activeFailed) > 0
}

// HasActiveNonRoboRevFailures returns true if any non-roborev check failure is active for the head SHA.
func (t *StateTracker) HasActiveNonRoboRevFailures(repo string, prNumber int, headSHA string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || normHeadSHA == "" || model.IsZeroSHA(normHeadSHA) {
		return false
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, normHeadSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !strings.EqualFold(entry.headSHA, normHeadSHA) {
		return false
	}
	return len(entry.activeFailed) > 0
}

// HasActiveRoboRevFailure returns true if a roborev review failure is active for the head SHA.
func (t *StateTracker) HasActiveRoboRevFailure(repo string, prNumber int, headSHA string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || normHeadSHA == "" || model.IsZeroSHA(normHeadSHA) {
		return false
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, normHeadSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if strings.EqualFold(entry.headSHA, normHeadSHA) {
		return entry.roboRevActiveFailed
	}
	return false
}

// HasActiveCheckRun returns true if checkName is currently an active failure for the given head SHA.
func (t *StateTracker) HasActiveCheckRun(repo string, prNumber int, headSHA, checkName string) bool {
	normCheck := CanonicalFailedCheckKey(checkName)
	if normCheck == "" {
		return false
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	return entry != nil && entry.hasActive(strings.TrimSpace(headSHA), checkName)
}

// HasEverFailedCheckRun returns true if checkName has ever failed on this commit.
func (t *StateTracker) HasEverFailedCheckRun(repo string, prNumber int, headSHA, checkName string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	normCheck := CanonicalFailedCheckKey(checkName)
	if t == nil || prNumber <= 0 || normHeadSHA == "" || model.IsZeroSHA(normHeadSHA) || normCheck == "" {
		return false
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, normHeadSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !strings.EqualFold(entry.headSHA, normHeadSHA) {
		return false
	}
	return entry.everFailed[normCheck]
}

// CanRecoverNonGateCheckSuccess evaluates if a successful check run clears all non-roborev failures
// and can transition the PR out of CI failure.
func (t *StateTracker) CanRecoverNonGateCheckSuccess(repo string, prNumber int, checkName, headSHA string) bool {
	normCheck := CanonicalFailedCheckKey(checkName)
	if normCheck == "" {
		return false
	}
	entry := t.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	if !strings.EqualFold(entry.headSHA, strings.TrimSpace(headSHA)) || !entry.everFailed[normCheck] {
		entry.mu.Unlock()
		return false
	}
	delete(entry.activeFailed, normCheck)
	allNonRoboRevCleared := len(entry.activeFailed) == 0
	roboRevActive := entry.roboRevActiveFailed
	entry.mu.Unlock()

	if allNonRoboRevCleared {
		t.nonRoboRevFailedSHAs.Delete(PRCheckKey(repo, prNumber))
		return !roboRevActive
	}
	return false
}

func (t *StateTracker) recordCollaboratorSHAAlert(m *sync.Map, repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	normRepo := model.NormalizeRepo(repo)
	if t == nil || m == nil || normRepo == "" || prNumber <= 0 || headSHA == "" || model.IsZeroSHA(headSHA) {
		return false
	}
	prev, loaded := m.Swap(PRCheckKey(normRepo, prNumber), headSHA)
	return !loaded || !strings.EqualFold(fmt.Sprint(prev), headSHA)
}

func (t *StateTracker) clearCollaboratorSHAAlert(m *sync.Map, repo string, prNumber int, headSHA string) {
	if t == nil || m == nil || prNumber <= 0 {
		return
	}
	cleanHead := strings.TrimSpace(headSHA)
	if cleanHead != "" {
		m.CompareAndDelete(PRCheckKey(repo, prNumber), cleanHead)
		return
	}
	m.Delete(PRCheckKey(repo, prNumber))
}

// MarkCollaboratorAlerted records that a collaborator was alerted for CI failure on this SHA.
func (t *StateTracker) MarkCollaboratorAlerted(repo string, prNumber int, headSHA string) bool {
	if t == nil {
		return false
	}
	return t.recordCollaboratorSHAAlert(&t.collaboratorAlertedSHAs, repo, prNumber, headSHA)
}

// ClearCollaboratorAlerted clears the collaborator alerted record.
func (t *StateTracker) ClearCollaboratorAlerted(repo string, prNumber int, headSHA string) {
	if t == nil {
		return
	}
	t.clearCollaboratorSHAAlert(&t.collaboratorAlertedSHAs, repo, prNumber, headSHA)
}

// MarkCollaboratorConflict records that a collaborator was alerted for a merge conflict.
func (t *StateTracker) MarkCollaboratorConflict(repo string, prNumber int, headSHA string) bool {
	if t == nil {
		return false
	}
	return t.recordCollaboratorSHAAlert(&t.collaboratorConflictSHAs, repo, prNumber, headSHA)
}

// ClearCollaboratorConflict clears the collaborator conflict alert record.
func (t *StateTracker) ClearCollaboratorConflict(repo string, prNumber int, headSHA string) {
	if t == nil {
		return
	}
	t.clearCollaboratorSHAAlert(&t.collaboratorConflictSHAs, repo, prNumber, headSHA)
}

// RecordMainPushSHA records that main branch was pushed with the given commit SHA.
// Returns true if newly recorded, false if already seen.
func (t *StateTracker) RecordMainPushSHA(repo string, sha string) bool {
	if t == nil {
		return false
	}
	normRepo := strings.ToLower(model.NormalizeRepo(repo))
	cleanSHA := strings.ToLower(strings.TrimSpace(sha))
	if normRepo == "" || cleanSHA == "" || model.IsZeroSHA(cleanSHA) {
		return false
	}
	_, loaded := t.lastMainPushSHA.LoadOrStore(normRepo+"@"+cleanSHA, true)
	if !loaded {
		t.lastMainPushSHA.Store(normRepo+"_latest", cleanSHA)
	}
	return !loaded
}

// GetMainPushSHA returns the latest recorded main push SHA for a repo.
func (t *StateTracker) GetMainPushSHA(repo string) string {
	if t == nil {
		return ""
	}
	normRepo := strings.ToLower(model.NormalizeRepo(repo))
	if v, ok := t.lastMainPushSHA.Load(normRepo + "_latest"); ok {
		return fmt.Sprint(v)
	}
	return ""
}

// MarkUpdatedForMain records that a PR was updated with current main branch.
func (t *StateTracker) MarkUpdatedForMain(repo string, prNumber int) {
	if t == nil || prNumber <= 0 {
		return
	}
	t.updatedForCurrentMain.Store(PRCheckKey(repo, prNumber), true)
}

// WasUpdatedForMain checks if a PR was already updated for current main.
func (t *StateTracker) WasUpdatedForMain(repo string, prNumber int) bool {
	if t == nil || prNumber <= 0 {
		return false
	}
	v, ok := t.updatedForCurrentMain.Load(PRCheckKey(repo, prNumber))
	return ok && v == true
}

// ClearUpdatedForMain deletes the updated-for-main flag for a specific PR.
func (t *StateTracker) ClearUpdatedForMain(repo string, prNumber int) {
	if t == nil || prNumber <= 0 {
		return
	}
	t.updatedForCurrentMain.Delete(PRCheckKey(repo, prNumber))
}

// ClearUpdatedPRsForRepo deletes updated-for-main flags for all PRs in the given repo.
func (t *StateTracker) ClearUpdatedPRsForRepo(repo string) {
	if t == nil {
		return
	}
	prefix := model.NormalizeRepo(repo) + "#"
	t.updatedForCurrentMain.Range(func(k, _ any) bool {
		if keyStr, ok := k.(string); ok && strings.HasPrefix(keyStr, prefix) {
			t.updatedForCurrentMain.Delete(k)
		}
		return true
	})
}

// ClearUpdatedPRsForMainCommit clears updated PRs for a repo unless the main commit was already seen.
func (t *StateTracker) ClearUpdatedPRsForMainCommit(repo, headSHA string) {
	if t == nil {
		return
	}
	if !t.RecordMainPushSHA(repo, headSHA) {
		return
	}
	t.ClearUpdatedPRsForRepo(repo)
}

// MarkConflictPublished records that a conflict lifecycle event was published for the PR and head SHA.
func (t *StateTracker) MarkConflictPublished(repo string, prNumber int, headSHA string) {
	normHeadSHA := strings.TrimSpace(headSHA)
	if t == nil || prNumber <= 0 || normHeadSHA == "" {
		return
	}
	t.conflictPublishedSHAs.Store(PRCheckKey(repo, prNumber), normHeadSHA)
}

// WasConflictPublished checks if a conflict event has already been published for this PR and head SHA.
func (t *StateTracker) WasConflictPublished(repo string, prNumber int, headSHA string) bool {
	if t == nil || prNumber <= 0 {
		return false
	}
	cleanHead := strings.TrimSpace(headSHA)
	publishedSHA, ok := t.conflictPublishedSHAs.Load(PRCheckKey(repo, prNumber))
	if !ok || cleanHead == "" {
		return false
	}
	return strings.EqualFold(fmt.Sprint(publishedSHA), cleanHead)
}

// ClearConflictPublished removes the conflict publication cache entry.
func (t *StateTracker) ClearConflictPublished(repo string, prNumber int) {
	if t == nil || prNumber <= 0 {
		return
	}
	t.conflictPublishedSHAs.Delete(PRCheckKey(repo, prNumber))
}

// RecordCommitSHAPR caches the mapping from repo@commitSHA to PR number and branch ref.
func (t *StateTracker) RecordCommitSHAPR(repo, sha string, prNumber int, branch string) {
	normRepo := strings.ToLower(model.NormalizeRepo(repo))
	normSHA := strings.ToLower(strings.TrimSpace(sha))
	if t == nil || normRepo == "" || normSHA == "" || model.IsZeroSHA(normSHA) || prNumber <= 0 {
		return
	}
	t.commitSHAPRs.Store(normRepo+"@"+normSHA, GitHubCheckRunPR{
		Number: prNumber,
		Head: model.GitHubPRRef{
			Ref: strings.TrimSpace(branch),
			SHA: normSHA,
		},
	})
}

// LookupCommitSHAPR looks up the cached PR number and branch ref for a commit SHA.
func (t *StateTracker) LookupCommitSHAPR(repo, sha string) (int, string, bool) {
	if t == nil {
		return 0, "", false
	}
	normRepo := strings.ToLower(model.NormalizeRepo(repo))
	normSHA := strings.ToLower(strings.TrimSpace(sha))
	if normRepo == "" || normSHA == "" || model.IsZeroSHA(normSHA) {
		return 0, "", false
	}
	v, ok := t.commitSHAPRs.Load(normRepo + "@" + normSHA)
	if !ok {
		return 0, "", false
	}
	item, ok := v.(GitHubCheckRunPR)
	if !ok || item.Number <= 0 {
		return 0, "", false
	}
	return item.Number, item.Head.Ref, true
}

// StoreRawCommitSHAPRForTest allows test suites to inject invalid or corrupt entries into the commitSHAPRs map.
func (t *StateTracker) StoreRawCommitSHAPRForTest(key string, val any) {
	if t == nil {
		return
	}
	t.commitSHAPRs.Store(key, val)
}
