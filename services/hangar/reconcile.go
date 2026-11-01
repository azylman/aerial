package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/services/hangar/pkg/metrics"
)

// resolveGitDir checks if repoPath contains a .git directory or a .git file (e.g., worktree/submodule).
func resolveGitDir(repoPath string) (string, error) {
	gitPath := filepath.Join(repoPath, ".git")
	fi, err := os.Stat(gitPath)
	if err != nil {
		return "", err
	}
	if fi.IsDir() {
		return gitPath, nil
	}

	data, err := os.ReadFile(gitPath)
	if err != nil {
		return "", err
	}
	content := strings.TrimSpace(string(data))
	const prefix = "gitdir:"
	if strings.HasPrefix(content, prefix) {
		target := strings.TrimSpace(content[len(prefix):])
		if !filepath.IsAbs(target) {
			target = filepath.Join(repoPath, target)
		}
		return filepath.Clean(target), nil
	}
	return gitPath, nil
}

// runGitDiffWithFallback executes git diff or diff-tree when prevHead is all zeros or primary diff fails.
func (d *SyncDaemon) runGitDiffWithFallback(ctx context.Context, repoPath, diffFlag, prevHead, currHead, fallbackRef string, pathArgs ...string) (string, error) {
	if isAllZeros(prevHead) {
		treeArgs := []string{"diff-tree", "--no-commit-id", diffFlag, "-r", fallbackRef}
		if len(pathArgs) > 0 {
			treeArgs = append(append(treeArgs, "--"), pathArgs...)
		}
		out, _, err := d.getGitExecutor()(ctx, repoPath, treeArgs...)
		if err != nil {
			return "", err
		}
		return string(out), nil
	}

	diffArgs := []string{"diff", diffFlag, prevHead, currHead}
	if len(pathArgs) > 0 {
		diffArgs = append(append(diffArgs, "--"), pathArgs...)
	}
	out, _, err := d.getGitExecutor()(ctx, repoPath, diffArgs...)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		treeArgs := []string{"diff-tree", "--no-commit-id", diffFlag, "-r", fallbackRef}
		if len(pathArgs) > 0 {
			treeArgs = append(append(treeArgs, "--"), pathArgs...)
		}
		outFallback, _, errFallback := d.getGitExecutor()(ctx, repoPath, treeArgs...)
		if errFallback != nil {
			return "", errFallback
		}
		return string(outFallback), nil
	}

	return string(out), nil
}

func (d *SyncDaemon) diffNomadCommitRange(ctx context.Context, repoPath, diffFlag, warnPrefix, prevHead, currHead string, pathArgs ...string) (string, bool, error) {
	if ctx.Err() != nil {
		return "", false, ctx.Err()
	}
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return "", false, nil
	}
	if repoPath == "" {
		return "", false, nil
	}

	out, err := d.runGitDiffWithFallback(ctx, repoPath, diffFlag, prevHead, currHead, "HEAD", pathArgs...)
	if err != nil {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		log.Printf("%s in %s (%v)", warnPrefix, repoPath, err)
		return "", false, nil
	}

	return out, true, nil
}

// HasNomadConfigChanges checks whether Nomad daemon configuration files changed between commits.
func (d *SyncDaemon) HasNomadConfigChanges(ctx context.Context, repoPath, prevHead, currHead string) (bool, error) {
	out, ok, err := d.diffNomadCommitRange(ctx, repoPath, "--name-only", "[Hangar] Warning: Failed to inspect nomad diff", prevHead, currHead, "nomad")
	if err != nil || !ok {
		return false, err
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	return len(FilterNomadConfigFiles(lines)) > 0, nil
}

// HasNomadChanges checks whether any Nomad job specifications changed between commits.
func (d *SyncDaemon) HasNomadChanges(ctx context.Context, repoPath, prevHead, currHead string) ([]NomadFileChange, error) {
	out, ok, err := d.diffNomadCommitRange(ctx, repoPath, "--name-status", "[Hangar:Nomad] Warning: Failed to inspect diff", prevHead, currHead)
	if err != nil || !ok {
		return nil, err
	}

	return ParseNomadGitStatus(out), nil
}

func parseNomadMeta(meta []string) (repo, commit, targetID string, prNumber int) {
	if len(meta) >= 1 {
		repo = meta[0]
	}
	if len(meta) >= 2 {
		commit = meta[1]
	}
	if len(meta) >= 3 {
		targetID = meta[2]
	}
	if len(meta) >= 4 {
		if parsed, err := strconv.Atoi(meta[3]); err == nil {
			prNumber = parsed
		}
	}
	return repo, commit, targetID, prNumber
}

func (d *SyncDaemon) recordPendingNomadChanges(repoPath string, changes []NomadFileChange, meta ...string) {
	d.pendingNomadMu.Lock()
	defer d.pendingNomadMu.Unlock()
	repo, commit, targetID, prNumber := parseNomadMeta(meta)
	d.pendingNomadChanges = append(d.pendingNomadChanges, NomadChangeEvent{
		RepoPath:  repoPath,
		Changes:   changes,
		Repo:      repo,
		CommitSHA: commit,
		PRNumber:  prNumber,
		TargetID:  targetID,
		Timestamp: time.Now(),
	})
}

func (d *SyncDaemon) drainPendingNomadChanges() []NomadChangeEvent {
	d.pendingNomadMu.Lock()
	defer d.pendingNomadMu.Unlock()
	out := d.pendingNomadChanges
	d.pendingNomadChanges = nil
	return out
}

// drainPendingNomadDeletes extracts and returns only delete actions from pendingNomadChanges,
// leaving any non-delete actions (add, modify, apply) intact in the queue.
func (d *SyncDaemon) drainPendingNomadDeletes() []NomadChangeEvent {
	d.pendingNomadMu.Lock()
	defer d.pendingNomadMu.Unlock()

	var deleteEvents []NomadChangeEvent
	var remainingEvents []NomadChangeEvent

	for _, evt := range d.pendingNomadChanges {
		var deletes []NomadFileChange
		var keeps []NomadFileChange
		for _, ch := range evt.Changes {
			if ch.Action == "delete" {
				deletes = append(deletes, ch)
			} else {
				keeps = append(keeps, ch)
			}
		}
		if len(deletes) > 0 {
			evtCopy := evt
			evtCopy.Changes = deletes
			deleteEvents = append(deleteEvents, evtCopy)
		}
		if len(keeps) > 0 {
			evtCopy := evt
			evtCopy.Changes = keeps
			remainingEvents = append(remainingEvents, evtCopy)
		}
	}
	d.pendingNomadChanges = remainingEvents
	return deleteEvents
}

func (d *SyncDaemon) getRepoLock(repoPath string) *sync.Mutex {
	cleaned := filepath.Clean(repoPath)
	d.repoLocksMu.Lock()
	defer d.repoLocksMu.Unlock()
	if d.repoLocks == nil {
		d.repoLocks = make(map[string]*sync.Mutex)
	}
	l, exists := d.repoLocks[cleaned]
	if !exists {
		l = &sync.Mutex{}
		d.repoLocks[cleaned] = l
	}
	return l
}

func (d *SyncDaemon) recordPendingNomadServerRestart() {
	if d == nil {
		return
	}
	d.pendingNomadServerRestartMu.Lock()
	defer d.pendingNomadServerRestartMu.Unlock()
	d.pendingNomadServerRestart = true
}

func (d *SyncDaemon) consumePendingNomadServerRestart() bool {
	if d == nil {
		return false
	}
	d.pendingNomadServerRestartMu.Lock()
	defer d.pendingNomadServerRestartMu.Unlock()
	restart := d.pendingNomadServerRestart
	d.pendingNomadServerRestart = false
	return restart
}

// TriggerReconcile requests reconciliation of pending Nomad jobs. If async is true, it queues
// reconciliation onto the debounced worker channel. If async is false, it executes
// ReconcilePendingNomad synchronously.
func (d *SyncDaemon) TriggerReconcile(ctx context.Context, async bool) error {
	if !async {
		if d.reconcileFn != nil {
			return d.reconcileFn(ctx)
		}
		return d.ReconcilePendingNomad(ctx)
	}
	d.mu.Lock()
	if d.reconcileCh == nil {
		d.reconcileCh = make(chan struct{}, 1)
	}
	ch := d.reconcileCh
	d.mu.Unlock()

	select {
	case ch <- struct{}{}:
	default:
	}
	return nil
}

var (
	reconcilerDebounceDuration = 5 * time.Second
	reconcilerMinCooldown      = 15 * time.Second
	reconcilerServerReadyDelay = 3 * time.Second
)

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (d *SyncDaemon) runNomadValidate(ctx context.Context, subCmd, targetPath string, requireNonNilExec bool, extraArgs ...string) error {
	valCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	nomadExec := d.getNomadExecutor()
	if requireNonNilExec && nomadExec == nil {
		return errors.New("nomad executor unavailable, failing safe")
	}
	args := append([]string{subCmd, "validate"}, extraArgs...)
	args = append(args, targetPath)
	stdout, stderr, err := nomadExec(valCtx, args...)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return fmt.Errorf("nomad %s validate failed for %s: %s (%w)", subCmd, targetPath, SanitizeLog(strings.TrimSpace(combined)), err)
	}
	return nil
}

// ResolveJobVarFile locates an optional <jobName>.vars.hcl file inside configDir/jobs or configDir/nomad/jobs.
func ResolveJobVarFile(jobName, configDir string, fileExists ...func(string) bool) string {
	jobName = strings.TrimSpace(jobName)
	configDir = strings.TrimSpace(configDir)
	if jobName == "" || configDir == "" {
		return ""
	}
	exists := func(p string) bool {
		if len(fileExists) > 0 && fileExists[0] != nil {
			return fileExists[0](p)
		}
		fi, err := os.Stat(p)
		return err == nil && !fi.IsDir()
	}
	for _, sub := range []string{"jobs", filepath.Join("nomad", "jobs")} {
		cand := filepath.Join(filepath.Clean(configDir), sub, jobName+".vars.hcl")
		if exists(cand) {
			return cand
		}
	}
	return ""
}

func (d *SyncDaemon) resolveApplyJobPath(repoPath string, ch NomadFileChange) (string, bool) {
	if IsNomadVarFile(ch.Path) {
		foundPath, ok := FindJobDefinitionInRepos(ch.JobName, "", d.candidateJobRepos())
		if !ok {
			log.Printf("[Hangar:Nomad] Notice: job file for %s (%s) not found across repos, skipping", ch.JobName, ch.Path)
			return "", false
		}
		return foundPath, true
	}
	fullPath := filepath.Join(repoPath, ch.Path)
	if !IsSafeJobPath(repoPath, fullPath) {
		return "", false
	}
	return fullPath, true
}

func (d *SyncDaemon) nomadJobRunArgs(jobName, fullPath string) []string {
	runArgs := []string{"job", "run", "-detach"}
	if d != nil && d.configDir != "" {
		if vf := ResolveJobVarFile(jobName, d.configDir); vf != "" {
			runArgs = append(runArgs, "-var-file="+vf)
		}
	}
	return append(runArgs, fullPath)
}

// ValidateNomadJob validates a Nomad job specification using nomad job validate.
func (d *SyncDaemon) ValidateNomadJob(ctx context.Context, jobPath string) error {
	var extraArgs []string
	if d != nil && d.configDir != "" {
		var content []byte
		if raw, err := os.ReadFile(jobPath); err == nil {
			content = raw
		}
		jobName := ExtractJobName(string(content), jobPath)
		if vf := ResolveJobVarFile(jobName, d.configDir); vf != "" {
			extraArgs = append(extraArgs, "-var-file="+vf)
		}
	}
	return d.runNomadValidate(ctx, "job", jobPath, false, extraArgs...)
}

// ValidateNomadConfig validates a Nomad daemon configuration path using nomad config validate.
func (d *SyncDaemon) ValidateNomadConfig(ctx context.Context, configPath string) error {
	return d.runNomadValidate(ctx, "config", configPath, true)
}

// reconcileNomadServer validates the Nomad daemon configuration when nomad/ files change.
func (d *SyncDaemon) reconcileNomadServer(ctx context.Context, composeDir string) error {
	nomadDir := filepath.Join(composeDir, "nomad")
	if info, err := os.Stat(nomadDir); err == nil && info.IsDir() {
		if errVal := d.ValidateNomadConfig(ctx, nomadDir); errVal != nil {
			log.Printf("[Hangar:GitOps] ERROR: %v. Aborting nomad-server config reconcile.", errVal)
			return errVal
		}
		log.Printf("[Hangar:GitOps] Nomad daemon configuration changes detected and validated.")
		return nil
	}
	log.Printf("[Hangar:GitOps] Notice: %s not found on disk, skipping nomad-server config validation", nomadDir)
	return nil
}

// candidateJobRepos returns all distinct repository paths known to the daemon.
func (d *SyncDaemon) candidateJobRepos() []string {
	var repos []string
	seen := make(map[string]struct{})
	add := func(p string) {
		if p == "" {
			return
		}
		clean := filepath.Clean(p)
		if _, ok := seen[clean]; !ok {
			seen[clean] = struct{}{}
			repos = append(repos, clean)
		}
	}
	for _, r := range d.repos {
		add(r)
	}
	add(d.composeDir)
	add(d.configDir)
	return repos
}

// ReconcileNomadChanges validates and applies or stops Nomad job specifications.
func (d *SyncDaemon) stopDeletedNomadJob(ctx context.Context, logPrefix, repoPath, jobName string) {
	if survivingPath, found := FindJobDefinitionInRepos(jobName, repoPath, d.candidateJobRepos()); found {
		log.Printf("%s Notice: skipping stop for deleted job %s (deleted from %s): job is still defined in %s", logPrefix, jobName, repoPath, survivingPath)
		return
	}
	out, errBytes, err := d.getNomadExecutor()(ctx, "job", "stop", "-purge", jobName)
	if err != nil {
		log.Printf("%s Warning: failed to stop deleted job %s: %s (%v)", logPrefix, jobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), err)
	} else {
		log.Printf("%s Successfully stopped deleted job %s", logPrefix, jobName)
	}
}

func isNomadJobTypeChangeError(output string) bool {
	return strings.Contains(strings.ToLower(output), "cannot update job from type")
}

func isBatchNomadRun(runArgs []string) bool {
	if len(runArgs) < 3 || runArgs[0] != "job" || runArgs[1] != "run" {
		return false
	}
	raw, err := os.ReadFile(runArgs[len(runArgs)-1])
	return err == nil && strings.Contains(string(raw), `"batch"`)
}

func (d *SyncDaemon) handleNomadJobTypeChange(ctx context.Context, jobName, sanitizedCombined string, runArgs []string) ([]byte, string, error, bool) {
	log.Printf("[Hangar:Nomad] Job %s changed scheduler type (%s); stopping with -purge before re-applying...", jobName, sanitizedCombined)
	stopOut, stopErrBytes, stopErr := d.getNomadExecutor()(ctx, "job", "stop", "-purge", jobName)
	if stopErr != nil {
		log.Printf("[Hangar:Nomad] Warning: failed to purge job %s during type transition: %s (%v)", jobName, SanitizeLog(strings.TrimSpace(string(append(stopOut, stopErrBytes...)))), stopErr)
		return nil, sanitizedCombined, nil, false
	}
	out, errBytes, err := d.getNomadExecutor()(ctx, runArgs...)
	if err != nil {
		sanitizedCombined = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
	}
	return out, sanitizedCombined, err, true
}

func (d *SyncDaemon) runNomadJobWithEvents(ctx context.Context, baseEvt HangarDeployEvent, runArgs ...string) (out []byte, sanitizedCombined string, err error) {
	baseEvt.Event = "deploy_started"
	baseEvt.Status = "started"
	baseEvt.Timestamp = time.Now().UTC()
	d.DispatchDeployEvent(baseEvt)

	if baseEvt.JobName != "" && isBatchNomadRun(runArgs) {
		if stopOut, stopErrBytes, stopErr := d.getNomadExecutor()(ctx, "job", "stop", "-purge", baseEvt.JobName); stopErr != nil {
			log.Printf("[Hangar:Nomad] Notice: pre-run purge of %s: %s (%v)", baseEvt.JobName, SanitizeLog(strings.TrimSpace(string(append(stopOut, stopErrBytes...)))), stopErr)
		}
	}

	var errBytes []byte
	out, errBytes, err = d.getNomadExecutor()(ctx, runArgs...)
	if err != nil {
		sanitizedCombined = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
		if baseEvt.JobName != "" && isNomadJobTypeChangeError(sanitizedCombined) {
			if retryOut, retryCombined, retryErr, ok := d.handleNomadJobTypeChange(ctx, baseEvt.JobName, sanitizedCombined, runArgs); ok {
				out, sanitizedCombined, err = retryOut, retryCombined, retryErr
			}
		}
	}
	if err != nil {
		failEvt := baseEvt
		failEvt.Event = "deploy_failed"
		failEvt.Status = "failed"
		failEvt.Details = sanitizedCombined
		failEvt.Timestamp = time.Now().UTC()
		d.DispatchDeployEvent(failEvt)
	}
	return out, sanitizedCombined, err
}

func (d *SyncDaemon) ReconcileNomadChanges(ctx context.Context, repoPath string, changes []NomadFileChange, meta ...string) error {
	if len(changes) == 0 {
		return nil
	}

	start := time.Now()

	repo, commit, targetID, prNumber := parseNomadMeta(meta)
	if repo == "" && repoPath != "" {
		repo = NormalizeGitHubSlug(repoPath)
	}
	if commit == "" && repoPath != "" {
		if headOut, _, headErr := d.getGitExecutor()(ctx, repoPath, "rev-parse", "HEAD"); headErr == nil {
			commit = strings.TrimSpace(string(headOut))
		}
	}

	d.nomadMu.Lock()
	defer d.nomadMu.Unlock()

	recCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	_, _, err := d.applyNomadChangesDirectly(recCtx, repoPath, changes, true, false, nil, nil, repo, commit, targetID, strconv.Itoa(prNumber))
	if err != nil {
		metrics.RecordReconciliation("error", time.Since(start))
	} else {
		metrics.RecordReconciliation("success", time.Since(start))
	}
	return err
}

func (d *SyncDaemon) ExecuteNomadTeardowns(ctx context.Context, pending []NomadChangeEvent) {
	if len(pending) == 0 {
		return
	}
	for _, evt := range pending {
		for _, change := range evt.Changes {
			if change.Action == "delete" {
				d.stopDeletedNomadJob(ctx, "[Hangar:Nomad]", evt.RepoPath, change.JobName)
			}
		}
	}
}

// ReconcilePendingNomad applies all pending Nomad changes.
func (d *SyncDaemon) ReconcilePendingNomad(ctx context.Context) error {
	pending := d.drainPendingNomadChanges()
	if len(pending) == 0 {
		return nil
	}
	var errs []error
	for _, evt := range pending {
		if err := d.ReconcileNomadChanges(ctx, evt.RepoPath, evt.Changes, evt.Repo, evt.CommitSHA, evt.TargetID, strconv.Itoa(evt.PRNumber)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (d *SyncDaemon) executeDebouncedReconcileCycle(ctx context.Context) {
	// Stop immediately if loop context is canceled before callback fires
	if ctx.Err() != nil {
		return
	}
	if elapsed := time.Since(d.GetLastReconcile()); elapsed < reconcilerMinCooldown {
		if !sleepWithContext(ctx, reconcilerMinCooldown-elapsed) {
			return
		}
	}
	if ctx.Err() != nil {
		return
	}

	// Phase 1: Teardowns for any deleted Nomad jobs (releases ports)
	pendingDeletes := d.drainPendingNomadDeletes()
	d.ExecuteNomadTeardowns(ctx, pendingDeletes)

	// Phase 2: Check for Nomad server daemon config changes
	if d.consumePendingNomadServerRestart() {
		if errNomad := d.reconcileNomadServer(ctx, d.composeDir); errNomad != nil {
			log.Printf("[Hangar:GitOps] Warning: reconcileNomadServer encountered error: %v", errNomad)
		} else if !sleepWithContext(ctx, reconcilerServerReadyDelay) {
			// Wait briefly for Nomad API readiness after server container restart
			return
		}
	}

	// Phase 3: Nomad reconciliation (job applications)
	if nomadErr := d.ReconcilePendingNomad(ctx); nomadErr != nil {
		log.Printf("[Hangar:GitOps] Debounced Nomad reconcile failed: %v", nomadErr)
	}
	d.SetLastReconcile(time.Now())
}

// StartReconcilerLoop runs the background debounced worker goroutine.
func (d *SyncDaemon) StartReconcilerLoop(ctx context.Context) {
	d.mu.Lock()
	if d.reconcileCh == nil {
		d.reconcileCh = make(chan struct{}, 1)
	}
	reconcileCh := d.reconcileCh
	d.mu.Unlock()
	debounceDuration := reconcilerDebounceDuration

	go func() {
		var debounceTimer *time.Timer
		var timerCh <-chan time.Time

		for {
			select {
			case <-ctx.Done():
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			case <-reconcileCh:
				debounceTimer, timerCh = resetDebounceTimer(debounceTimer, debounceDuration)
			case <-timerCh:
				timerCh = nil
				d.executeDebouncedReconcileCycle(ctx)
			}
		}
	}()
}
