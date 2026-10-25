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

// HasNomadConfigChanges checks whether Nomad daemon configuration files changed between commits.
func (d *SyncDaemon) HasNomadConfigChanges(ctx context.Context, repoPath, prevHead, currHead string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return false, nil
	}
	if repoPath == "" {
		return false, nil
	}

	out, err := d.runGitDiffWithFallback(ctx, repoPath, "--name-only", prevHead, currHead, "HEAD", "nomad")
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		log.Printf("[Hangar] Warning: Failed to inspect nomad diff in %s (%v); failing safe to false", repoPath, err)
		return false, nil
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	return len(FilterNomadConfigFiles(lines)) > 0, nil
}

// HasNomadChanges checks whether any Nomad job specifications changed between commits.
func (d *SyncDaemon) HasNomadChanges(ctx context.Context, repoPath, prevHead, currHead string) ([]NomadFileChange, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return nil, nil
	}
	if repoPath == "" {
		return nil, nil
	}

	out, err := d.runGitDiffWithFallback(ctx, repoPath, "--name-status", prevHead, currHead, "HEAD")
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		log.Printf("[Hangar:Nomad] Warning: Failed to inspect diff in %s (%v)", repoPath, err)
		return nil, nil
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

// getComposeArgs constructs the compose CLI flags with base docker-compose.yml and any present overrides.
func (d *SyncDaemon) getComposeArgs(composeDir string, subCmd ...string) []string {
	baseFile := filepath.Join(composeDir, "docker-compose.yml")
	args := []string{
		"--project-name", "aerial",
		"--project-directory", composeDir,
		"-f", baseFile,
	}

	envFile := filepath.Join(composeDir, ".env")
	if fi, err := os.Stat(envFile); err == nil && !fi.IsDir() {
		args = append(args, "--env-file", envFile)
	}

	configDir := d.configDir

	var overrideCandidates []string
	overrideCandidates = append(overrideCandidates,
		filepath.Join(composeDir, "docker-compose.override.yml"),
		filepath.Join(composeDir, "docker-compose.override.yaml"),
	)
	if configDir != "" {
		overrideCandidates = append(overrideCandidates,
			filepath.Join(configDir, "docker-compose.override.yml"),
			filepath.Join(configDir, "docker-compose.override.yaml"),
		)
	}

	seen := make(map[string]bool)
	for _, cand := range overrideCandidates {
		cleaned := filepath.Clean(cand)
		if seen[cleaned] {
			continue
		}
		seen[cleaned] = true
		if _, err := os.Stat(cleaned); err == nil {
			args = append(args, "-f", cleaned)
		}
	}

	return append(args, subCmd...)
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

func (d *SyncDaemon) runNomadValidate(ctx context.Context, subCmd, targetPath string, requireNonNilExec bool) error {
	valCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	nomadExec := d.getNomadExecutor()
	if requireNonNilExec && nomadExec == nil {
		return errors.New("nomad executor unavailable, failing safe")
	}
	stdout, stderr, err := nomadExec(valCtx, subCmd, "validate", targetPath)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return fmt.Errorf("nomad %s validate failed for %s: %s (%w)", subCmd, targetPath, SanitizeLog(strings.TrimSpace(combined)), err)
	}
	return nil
}

// ValidateNomadJob validates a Nomad job specification using nomad job validate.
func (d *SyncDaemon) ValidateNomadJob(ctx context.Context, jobPath string) error {
	return d.runNomadValidate(ctx, "job", jobPath, false)
}

// ValidateNomadConfig validates a Nomad daemon configuration path using nomad config validate.
func (d *SyncDaemon) ValidateNomadConfig(ctx context.Context, configPath string) error {
	return d.runNomadValidate(ctx, "config", configPath, true)
}

// reconcileNomadServer safely restarts the nomad-server container after validating daemon configuration.
func (d *SyncDaemon) reconcileNomadServer(ctx context.Context, composeDir string) error {
	nomadDir := filepath.Join(composeDir, "nomad")
	if _, statErr := os.Stat(nomadDir); statErr != nil {
		log.Printf("[Hangar:GitOps] Notice: %s not found on disk, skipping nomad-server restart", nomadDir)
		return nil
	}

	if errVal := d.ValidateNomadConfig(ctx, nomadDir); errVal != nil {
		log.Printf("[Hangar:GitOps] ERROR: %v. Aborting nomad-server restart to prevent control plane outage.", errVal)
		return errVal
	}

	log.Printf("[Hangar:GitOps] Nomad daemon configuration changes detected and validated. Restarting nomad-server...")
	restartCtx, restartCancel := context.WithTimeout(ctx, 60*time.Second)
	defer restartCancel()

	restartArgs := d.getComposeArgs(composeDir, "restart", "nomad-server")
	out, stderr, err := d.getComposeExecutor()(restartCtx, composeDir, restartArgs...)
	if err != nil {
		combined := SanitizeLog(strings.TrimSpace(string(append(out, stderr...))))
		log.Printf("[Hangar:GitOps] ERROR: Failed to restart nomad-server: %s (%v)", combined, err)
		return fmt.Errorf("failed to restart nomad-server: %s (%w)", combined, err)
	}
	log.Printf("[Hangar:GitOps] Successfully restarted nomad-server for updated daemon configuration.")
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

func (d *SyncDaemon) runNomadJobWithEvents(ctx context.Context, baseEvt HangarDeployEvent, runArgs ...string) (out []byte, sanitizedCombined string, err error) {
	baseEvt.Event = "deploy_started"
	baseEvt.Status = "started"
	baseEvt.Timestamp = time.Now().UTC()
	d.DispatchDeployEvent(baseEvt)

	var errBytes []byte
	out, errBytes, err = d.getNomadExecutor()(ctx, runArgs...)
	if err != nil {
		sanitizedCombined = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
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

	_, _, err := d.applyNomadChangesDirectly(recCtx, repoPath, changes, false, true, false, nil, nil, repo, commit, targetID, strconv.Itoa(prNumber))
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

func (d *SyncDaemon) resolvePolledImageCommitSHA(pollCtx context.Context, repo, imgRef string) string {
	if rev, revErr := d.GetRemoteImageRevision(pollCtx, imgRef); revErr == nil && rev != "" {
		log.Printf("[Hangar:NomadImagePoll] Resolved commit SHA %s from image labels for %s", rev, imgRef)
		return rev
	} else if revErr != nil {
		log.Printf("[Hangar:NomadImagePoll] Notice: could not resolve revision from image %s: %v (falling back to git HEAD)", imgRef, revErr)
	}

	if headOut, _, headErr := d.getGitExecutor()(pollCtx, repo, "rev-parse", "HEAD"); headErr == nil {
		return strings.TrimSpace(string(headOut))
	}
	return ""
}

func (d *SyncDaemon) checkSingleJobImageDigest(pollCtx context.Context, repo, jobName, imgRef string) {
	remoteDigest, digestErr := d.GetRemoteImageDigest(pollCtx, imgRef)
	if digestErr != nil || remoteDigest == "" {
		return
	}

	key := jobName + ":" + imgRef
	d.nomadDigestsMu.RLock()
	lastDigest, seen := d.nomadKnownDigests[key]
	d.nomadDigestsMu.RUnlock()

	if !seen {
		d.nomadDigestsMu.Lock()
		if d.nomadKnownDigests == nil {
			d.nomadKnownDigests = make(map[string]string)
		}
		d.nomadKnownDigests[key] = remoteDigest
		d.nomadDigestsMu.Unlock()
		return
	}

	if lastDigest == remoteDigest {
		return
	}

	log.Printf("[Hangar:NomadImagePoll] New image digest %s detected for Nomad job %s (image %s, previous %s). Rescheduling allocation.", remoteDigest, jobName, imgRef, lastDigest)
	d.nomadDigestsMu.Lock()
	d.nomadKnownDigests[key] = remoteDigest
	d.nomadDigestsMu.Unlock()

	headSHA := d.resolvePolledImageCommitSHA(pollCtx, repo, imgRef)

	repoName := ImageSourceRepo(imgRef)
	if repoName == "" {
		repoName = filepath.Base(repo)
		if repoName == "aerial" || repoName == "aerial-config" || repoName == "aerial-sidecars" {
			repoName = "azylman/" + repoName
		}
	}

	deployEvt := HangarDeployEvent{
		Event:     "deploy_started",
		JobName:   jobName,
		Repo:      repoName,
		CommitSHA: headSHA,
		Image:     imgRef,
		Digest:    remoteDigest,
		Status:    "started",
		Details:   fmt.Sprintf("nomad image poll restart for %s (digest %s)", imgRef, remoteDigest),
		Timestamp: time.Now().UTC(),
	}
	d.DispatchDeployEvent(deployEvt)

	out, errBytes, runErr := d.getNomadExecutor()(pollCtx, "job", "restart", "-yes", "-reschedule", "-on-error=fail", jobName)
	if runErr != nil {
		log.Printf("[Hangar:NomadImagePoll] Warning: nomad job restart failed for %s: %s (%v)", jobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), runErr)
	} else {
		log.Printf("[Hangar:NomadImagePoll] Successfully triggered reschedule restart for Nomad job %s", jobName)
	}
}

// CheckAndReconcileNomadImages scans jobs/*.nomad in configDir for updated image digests and triggers rolling restarts.
func (d *SyncDaemon) CheckAndReconcileNomadImages(ctx context.Context) error {
	d.nomadPollMu.Lock()
	if !d.lastNomadImagePoll.IsZero() && time.Since(d.lastNomadImagePoll) < 5*time.Minute {
		d.nomadPollMu.Unlock()
		return nil
	}
	d.lastNomadImagePoll = time.Now()
	d.nomadPollMu.Unlock()

	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	seenFiles := make(map[string]struct{})
	for _, repo := range d.candidateJobRepos() {
		candidateDirs := []string{
			filepath.Join(repo, "jobs"),
			filepath.Join(repo, "nomad", "jobs"),
		}
		walkNomadJobFiles(candidateDirs, func(dir, jobPath, jobName, content string) bool {
			if _, seen := seenFiles[jobPath]; seen {
				return true
			}
			seenFiles[jobPath] = struct{}{}

			images := ExtractNomadJobImages(content)
			for _, imgRef := range images {
				d.checkSingleJobImageDigest(pollCtx, repo, jobName, imgRef)
			}
			return true
		})
	}

	return nil
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
	d.pendingNomadMu.Lock()
	pendingNomad := append([]NomadChangeEvent(nil), d.pendingNomadChanges...)
	d.pendingNomadMu.Unlock()
	d.ExecuteNomadTeardowns(ctx, pendingNomad)

	// Phase 2: Check for Nomad server daemon config changes
	if d.consumePendingNomadServerRestart() {
		if errNomad := d.reconcileNomadServer(ctx, d.composeDir); errNomad != nil {
			log.Printf("[Hangar:GitOps] Warning: reconcileNomadServer encountered error: %v", errNomad)
		} else {
			// Wait briefly for Nomad API readiness after server container restart
			if !sleepWithContext(ctx, reconcilerServerReadyDelay) {
				return
			}
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
	if d.reconcileCh == nil {
		d.reconcileCh = make(chan struct{}, 1)
	}

	go func() {
		var debounceTimer *time.Timer

		for {
			select {
			case <-ctx.Done():
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			case <-d.reconcileCh:
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.AfterFunc(reconcilerDebounceDuration, func() {
					d.executeDebouncedReconcileCycle(ctx)
				})
			}
		}
	}()
}
