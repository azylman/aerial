package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func isAllZeros(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c != '0' {
			return false
		}
	}
	return true
}

func fileInChangedList(target string, changed []string) bool {
	targetClean := filepath.Clean(filepath.ToSlash(target))
	targetClean = strings.TrimPrefix(targetClean, "/")
	for _, f := range changed {
		clean := filepath.Clean(filepath.ToSlash(f))
		clean = strings.TrimPrefix(clean, "/")
		if clean == targetClean {
			return true
		}
	}
	return false
}

func homepageInChangedList(changed []string) bool {
	targetClean := "services/homepage/homepage.yaml"
	for _, f := range changed {
		clean := filepath.Clean(filepath.ToSlash(f))
		clean = strings.TrimPrefix(clean, "/")
		if clean == targetClean {
			return true
		}
		if clean == "homepage" || strings.HasPrefix(clean, "homepage/") {
			return true
		}
	}
	return false
}

func (d *SyncDaemon) getChangedFiles(ctx context.Context, repoPath, prevHead, currHead string) ([]string, error) {
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return nil, nil
	}
	out, err := d.runGitDiffWithFallback(ctx, repoPath, "--name-only", prevHead, currHead, currHead)
	if err != nil {
		return nil, err
	}
	return splitLines(out), nil
}

func splitLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func (d *SyncDaemon) shouldDeferNomadJob(ctx context.Context, jobName, jobSpec string, checkRepoBuilds bool) bool {
	for _, img := range ExtractNomadJobImages(jobSpec) {
		srcRepo := ImageSourceRepo(img)
		if srcRepo != "" {
			if checkRepoBuilds && d.CheckGitHubRepoBuildInProgress(ctx, srcRepo) {
				log.Printf("[Hangar:GitPush] Job %s deferred: source repo %s has builds in progress", jobName, srcRepo)
				return true
			}
			dgst, dgstErr := d.GetRemoteImageDigest(ctx, img)
			if dgstErr != nil || dgst == "" {
				log.Printf("[Hangar:GitPush] Job %s deferred: image %s not ready in registry (%v)", jobName, img, dgstErr)
				return true
			}
		}
	}
	return false
}

func (d *SyncDaemon) applyNomadChangesDirectly(ctx context.Context, repoPath string, changes []NomadFileChange, checkRepoBuilds, failOnJobErr, containersBuilding bool, changedFiles, expectedJobs []string, meta ...string) (applied, deferred []string, err error) {
	repo, commit, targetID, prNumber := parseNomadMeta(meta)

	SortNomadChangesHangarLast(changes)

	type readyApply struct {
		change   NomadFileChange
		fullPath string
	}
	var readyApplies []readyApply
	var deleteChanges []NomadFileChange

	for _, ch := range changes {
		if ch.Action == "delete" {
			deleteChanges = append(deleteChanges, ch)
			continue
		}
		fullPath := filepath.Join(repoPath, ch.Path)
		if !IsSafeJobPath(repoPath, fullPath) {
			continue
		}
		content, readErr := os.ReadFile(fullPath)
		if readErr != nil {
			log.Printf("[Hangar:Nomad] Notice: job file %s not found on disk, skipping", fullPath)
			continue
		}
		jobSpec := string(content)
		if (containersBuilding && JobHasContainerBuildChanges(jobSpec, changedFiles)) || d.shouldDeferNomadJob(ctx, ch.JobName, jobSpec, checkRepoBuilds) {
			log.Printf("[Hangar:GitPush] Deferring Nomad job %s until container image is ready", ch.JobName)
			deferred = append(deferred, ch.JobName)
			continue
		}
		readyApplies = append(readyApplies, readyApply{change: ch, fullPath: fullPath})
	}

	// Phase 1: Teardowns (stop deleted jobs, or defer if core container build rollout is pending)
	if len(deleteChanges) > 0 {
		if !checkRepoBuilds && (containersBuilding || len(deferred) > 0) {
			d.recordPendingNomadChanges(repoPath, deleteChanges, repo, commit, targetID, strconv.Itoa(prNumber))
		} else {
			for _, ch := range deleteChanges {
				d.stopDeletedNomadJob(ctx, "[Hangar:Nomad]", repoPath, ch.JobName)
			}
		}
	}

	// Phase 2: Applications (validate, then run)
	for _, item := range readyApplies {
		ch, fullPath := item.change, item.fullPath
		if errVal := d.ValidateNomadJob(ctx, fullPath); errVal != nil {
			log.Printf("[Hangar:Nomad] ERROR: Validation failed for %s: %v", ch.Path, errVal)
			if failOnJobErr {
				return applied, deferred, errVal
			}
			continue
		}

		baseEvt := HangarDeployEvent{
			JobName:      ch.JobName,
			Repo:         repo,
			CommitSHA:    commit,
			PRNumber:     prNumber,
			TargetID:     targetID,
			ExpectedJobs: expectedJobs,
		}

		log.Printf("[Hangar:Nomad] Applying job %s (%s)...", ch.JobName, ch.Path)
		out, sanitizedCombined, errRun := d.runNomadJobWithEvents(ctx, baseEvt, "job", "run", "-detach", fullPath)
		if errRun != nil {
			log.Printf("[Hangar:Nomad] nomad job run failed for %s: %s (%v)", ch.JobName, sanitizedCombined, errRun)
			if failOnJobErr {
				return applied, deferred, fmt.Errorf("nomad job run failed for %s: %s (%w)", ch.Path, sanitizedCombined, errRun)
			}
			continue
		}
		log.Printf("[Hangar:Nomad] Successfully applied job %s: %s", ch.JobName, SanitizeLog(strings.TrimSpace(string(out))))
		applied = append(applied, ch.JobName)
	}
	return applied, deferred, nil
}

// ExecuteGitPushEvent processes an incoming git push webhook event.
func (d *SyncDaemon) ExecuteGitPushEvent(ctx context.Context, req GitPushEventRequest) (GitPushEventResponse, int) {
	if req.Repo == "" {
		return GitPushEventResponse{
			Status:  "error",
			Message: "missing required field: repo",
		}, http.StatusBadRequest
	}

	// Filter out non-main branch pushes
	ref := strings.TrimSpace(req.Ref)
	if ref != "" && ref != "refs/heads/main" && ref != "main" && !strings.HasSuffix(ref, "/main") {
		return GitPushEventResponse{
			Status:  "ignored",
			Repo:    req.Repo,
			Ref:     req.Ref,
			Commit:  req.Commit,
			Message: fmt.Sprintf("push to non-main ref %q ignored", req.Ref),
		}, http.StatusOK
	}

	repoPath := ResolveRepoPath(req.Repo, d.repos)
	if repoPath == "" {
		return GitPushEventResponse{
			Status:  "error",
			Repo:    req.Repo,
			Message: fmt.Sprintf("repository %q is not managed by hangar", req.Repo),
		}, http.StatusNotFound
	}

	commitSHA := req.Commit
	sc := ConfigSyncContext{
		Repo:             req.Repo,
		CommitSHA:        commitSHA,
		PRNumber:         req.PRNumber,
		TargetID:         req.TargetID,
		SkipPendingNomad: true,
	}
	res := d.SyncRepo(ctx, repoPath, sc)
	if res.Error != "" {
		if res.CurrentHead != "" {
			commitSHA = res.CurrentHead
		}
		failEvt := HangarDeployEvent{
			Event:     "sync_failed",
			JobName:   "git-sync",
			Repo:      req.Repo,
			PRNumber:  req.PRNumber,
			TargetID:  req.TargetID,
			CommitSHA: commitSHA,
			Status:    "failed",
			Details:   res.Error,
			Timestamp: time.Now().UTC(),
		}
		d.DispatchDeployEvent(failEvt)
		return GitPushEventResponse{
			Status:  "error",
			Repo:    req.Repo,
			Commit:  res.CurrentHead,
			Message: fmt.Sprintf("git sync failed: %s", res.Error),
		}, http.StatusInternalServerError
	}

	resp := GitPushEventResponse{
		Status: "accepted",
		Repo:   req.Repo,
		Ref:    req.Ref,
		Commit: res.CurrentHead,
	}

	if res.CurrentHead != "" {
		commitSHA = res.CurrentHead
	}
	sc.CommitSHA = commitSHA

	cleanRepo := strings.ToLower(filepath.Clean(repoPath))
	isAerialConfig := strings.Contains(cleanRepo, "aerial-config")
	isAerialCore := strings.HasSuffix(cleanRepo, "/aerial") || cleanRepo == "aerial"

	changedFiles, errDiff := d.getChangedFiles(ctx, repoPath, res.PreviousHead, res.CurrentHead)
	if isAerialConfig && errDiff != nil {
		log.Printf("[Hangar:GitPush] Error: getChangedFiles failed for %s: %v", repoPath, errDiff)
		return GitPushEventResponse{
			Status:  "error",
			Repo:    req.Repo,
			Commit:  res.CurrentHead,
			Message: fmt.Sprintf("failed inspecting git diff: %v", errDiff),
		}, http.StatusInternalServerError
	}

	nomadChanges, errNomad := d.HasNomadChanges(ctx, repoPath, res.PreviousHead, res.CurrentHead)
	if isAerialConfig && errNomad != nil {
		log.Printf("[Hangar:GitPush] Warning: HasNomadChanges failed for %s: %v", repoPath, errNomad)
	}
	if isAerialCore && errDiff == nil && HasMigrationChanges(changedFiles) {
		nomadChanges = EnsureDBMigrateNomadChange(nomadChanges)
		errNomad = nil
	}

	expectedJobs := ResolveExpectedDeployJobs(repoPath, changedFiles, nomadChanges, d.GetServiceConfigMappings(repoPath), d.candidateJobRepos())
	resp.ExpectedJobs = expectedJobs
	sc.ExpectedJobs = expectedJobs

	syncEvt := HangarDeployEvent{
		Event:        "sync_success",
		JobName:      "git-sync",
		Repo:         req.Repo,
		PRNumber:     req.PRNumber,
		TargetID:     req.TargetID,
		CommitSHA:    commitSHA,
		Status:       "success",
		Details:      "git sync completed",
		ExpectedJobs: expectedJobs,
		Timestamp:    time.Now().UTC(),
	}
	d.DispatchDeployEvent(syncEvt)

	if isAerialCore {
		return d.handleAerialCorePush(ctx, repoPath, req, resp, changedFiles, errDiff, nomadChanges, errNomad, expectedJobs)
	}

	if isAerialConfig {
		return d.handleAerialConfigPush(ctx, repoPath, req, sc, resp, changedFiles, nomadChanges, expectedJobs)
	}

	resp.Message = "repository synced"
	return resp, http.StatusOK
}

func (d *SyncDaemon) handleAerialCorePush(ctx context.Context, repoPath string, req GitPushEventRequest, resp GitPushEventResponse, changedFiles []string, errDiff error, nomadChanges []NomadFileChange, errNomad error, expectedJobs []string) (GitPushEventResponse, int) {
	containersBuilding := errDiff == nil && HasContainerBuildChanges(repoPath, changedFiles)
	if containersBuilding {
		resp.ContainersBuilding = true
	}

	if errNomad == nil && len(nomadChanges) > 0 {
		resp.NomadChanged = true
		applied, deferred, errRec := d.applyNomadChangesDirectly(ctx, repoPath, nomadChanges, false, true, containersBuilding, changedFiles, expectedJobs, req.Repo, req.Commit, req.TargetID, strconv.Itoa(req.PRNumber))
		resp.AppliedJobs = applied
		if len(deferred) > 0 {
			resp.ContainersBuilding = true
		}
		if errRec != nil {
			resp.Status = "error"
			resp.Message = fmt.Sprintf("failed applying nomad changes: %v", errRec)
			return resp, http.StatusInternalServerError
		}
		if resp.ContainersBuilding {
			resp.Message = fmt.Sprintf("repository synced; applied %d nomad job(s), deferred container build rollout until image_ready", len(applied))
		} else {
			resp.Message = fmt.Sprintf("repository synced; applied %d nomad job(s)", len(applied))
		}
		return resp, http.StatusOK
	}

	if containersBuilding {
		resp.Message = "repository synced; container build changes detected, deferring rollout until image_ready"
		log.Printf("[Hangar:GitPush] Container build changes detected in %s (%d files). Deferring Nomad rollout.", repoPath, len(changedFiles))
		return resp, http.StatusOK
	}

	resp.Message = "repository synced; no container build or nomad changes"
	return resp, http.StatusOK
}

func (d *SyncDaemon) handleAerialConfigPush(ctx context.Context, repoPath string, req GitPushEventRequest, sc ConfigSyncContext, resp GitPushEventResponse, changedFiles []string, nomadChanges []NomadFileChange, expectedJobs []string) (GitPushEventResponse, int) {
	sc.DiffGated = true
	sc.ChangedFiles = changedFiles

	if len(nomadChanges) > 0 {
		resp.NomadChanged = true
		applied, deferred, errRec := d.applyNomadChangesDirectly(ctx, repoPath, nomadChanges, true, false, false, nil, expectedJobs, req.Repo, req.Commit, req.TargetID, strconv.Itoa(req.PRNumber))
		if errRec != nil {
			log.Printf("[Hangar:GitPush] Notice: applyNomadChangesDirectly notice: %v", errRec)
		}
		resp.AppliedJobs = applied
		if len(deferred) > 0 {
			resp.ContainersBuilding = true
			resp.Message = fmt.Sprintf("applied %d job(s); deferred %d job(s) pending container builds", len(applied), len(deferred))
		} else {
			resp.Message = fmt.Sprintf("applied %d nomad job(s)", len(applied))
		}
	} else {
		resp.Message = "configuration synced; no nomad job changes"
	}

	d.syncAllConfigsToNomad(ctx, "[Hangar:GitPush]", repoPath, sc)

	return resp, http.StatusOK
}

func (d *SyncDaemon) findAllNomadJobsByImage(imgRef string) []MatchedNomadJob {
	var dirs []string
	for _, repo := range d.candidateJobRepos() {
		dirs = append(dirs, filepath.Join(repo, "jobs"), filepath.Join(repo, "nomad", "jobs"))
	}
	var matches []MatchedNomadJob
	seenJobs := make(map[string]struct{})
	walkNomadJobFiles(dirs, func(dir, jobPath, jobName, content string) bool {
		for _, img := range ExtractNomadJobImages(content) {
			if ImageMatches(imgRef, img) {
				if _, ok := seenJobs[jobName]; !ok {
					seenJobs[jobName] = struct{}{}
					matches = append(matches, MatchedNomadJob{
						JobName: jobName,
						JobPath: jobPath,
					})
				}
				break
			}
		}
		return true
	})
	return matches
}

// ExecuteImageReadyEvent processes an incoming image ready event by deploying matching Nomad jobs.
func (d *SyncDaemon) ExecuteImageReadyEvent(ctx context.Context, req ImageReadyEventRequest) (ImageReadyEventResponse, int) {
	imgRef := strings.TrimSpace(req.Image)
	if imgRef == "" {
		return ImageReadyEventResponse{
			Status:  "error",
			Message: "missing required field: image",
		}, http.StatusBadRequest
	}

	matches := d.findAllNomadJobsByImage(imgRef)

	if len(matches) == 0 {
		log.Printf("[Hangar:ImageReady] No Nomad jobs found referencing image %s", imgRef)
		return ImageReadyEventResponse{
			Status:  "not_found",
			Image:   imgRef,
			Message: fmt.Sprintf("no nomad jobs found using image %s", imgRef),
		}, http.StatusNotFound
	}

	d.nomadMu.Lock()
	defer d.nomadMu.Unlock()

	SortMatchedJobsHangarLast(matches)

	pendingTeardowns := d.drainPendingNomadDeletes()
	if len(pendingTeardowns) > 0 {
		d.ExecuteNomadTeardowns(ctx, pendingTeardowns)
	}

	digest := req.Digest
	if digest == "" {
		if remoteDigest, err := d.GetRemoteImageDigest(ctx, imgRef); err == nil && remoteDigest != "" {
			digest = remoteDigest
		} else if err != nil {
			log.Printf("[Hangar:ImageReady] Warning: could not resolve remote digest for %s: %v", imgRef, err)
		}
	}

	var appliedJobs []string
	var runErrors []string
	for _, job := range matches {
		if err := d.applyImageReadyToJob(ctx, job, req, imgRef, digest); err != nil {
			runErrors = append(runErrors, err.Error())
			continue
		}
		appliedJobs = append(appliedJobs, job.JobName)
	}

	if len(runErrors) > 0 {
		errMsg := strings.Join(runErrors, "; ")
		return ImageReadyEventResponse{
			Status:      "error",
			Image:       imgRef,
			MatchedJobs: appliedJobs,
			Message:     errMsg,
		}, http.StatusInternalServerError
	}

	return ImageReadyEventResponse{
		Status:      "accepted",
		Image:       imgRef,
		MatchedJobs: appliedJobs,
		Message:     fmt.Sprintf("successfully applied %d matching nomad job(s)", len(appliedJobs)),
	}, http.StatusOK
}

func buildImageReadyNomadRunArgs(jobSpec, jobPath, commitSHA, digest string) ([]string, bool) {
	runArgs := []string{"job", "run", "-detach"}
	varInjected := false
	if HasNomadJobVariable(jobSpec, "image_tag") {
		tag := commitSHA
		if tag == "" {
			if digest != "" {
				cleanDigest := strings.TrimPrefix(digest, "@")
				if !strings.HasPrefix(cleanDigest, "sha256:") {
					cleanDigest = "sha256:" + cleanDigest
				}
				tag = "latest@" + cleanDigest
			} else {
				tag = "latest"
			}
		}
		runArgs = append(runArgs, "-var=image_tag="+tag)
		varInjected = true
	}
	runArgs = append(runArgs, jobPath)
	return runArgs, varInjected
}

func (d *SyncDaemon) applyImageReadyToJob(ctx context.Context, job MatchedNomadJob, req ImageReadyEventRequest, imgRef, digest string) error {
	content, readErr := os.ReadFile(job.JobPath)
	if readErr != nil {
		log.Printf("[Hangar:ImageReady] Failed reading job file %s: %v", job.JobPath, readErr)
		return fmt.Errorf("%s: read error: %w", job.JobName, readErr)
	}

	jobSpec := string(content)

	if errVal := d.ValidateNomadJob(ctx, job.JobPath); errVal != nil {
		log.Printf("[Hangar:ImageReady] Validation failed for job %s (%s): %v", job.JobName, job.JobPath, errVal)
		return fmt.Errorf("%s validation failed: %w", job.JobName, errVal)
	}

	baseEvt := HangarDeployEvent{
		JobName:   job.JobName,
		Repo:      req.Repo,
		CommitSHA: req.CommitSHA,
		PRNumber:  req.PRNumber,
		TargetID:  req.TargetID,
		Image:     req.Image,
		Digest:    digest,
	}

	runArgs, varInjected := buildImageReadyNomadRunArgs(jobSpec, job.JobPath, req.CommitSHA, digest)

	log.Printf("[Hangar:ImageReady] Applying Nomad job %s from %s (args: %v)...", job.JobName, job.JobPath, runArgs)
	if _, sanitizedCombined, errRun := d.runNomadJobWithEvents(ctx, baseEvt, runArgs...); errRun != nil {
		log.Printf("[Hangar:ImageReady] Error: job run failed for %s: %s (%v)", job.JobName, sanitizedCombined, errRun)
		return fmt.Errorf("%s: %w", job.JobName, errRun)
	}

	if !varInjected {
		log.Printf("[Hangar:ImageReady] Rescheduling allocation restart for unparameterized job %s...", job.JobName)
		outRest, errRestBytes, errRest := d.getNomadExecutor()(ctx, "job", "restart", "-yes", "-reschedule", "-on-error=fail", job.JobName)
		if errRest != nil {
			log.Printf("[Hangar:ImageReady] Warning: job restart -reschedule failed for %s: %s (%v)", job.JobName, SanitizeLog(strings.TrimSpace(string(append(outRest, errRestBytes...)))), errRest)
		}
	}

	if digest != "" {
		key := job.JobName + ":" + imgRef
		d.nomadDigestsMu.Lock()
		if d.nomadKnownDigests == nil {
			d.nomadKnownDigests = make(map[string]string)
		}
		d.nomadKnownDigests[key] = digest
		d.nomadDigestsMu.Unlock()
	}

	return nil
}

// EnqueueImageReadyEvent validates an incoming image ready notification, records all matching
// Nomad jobs into the pending rollout batch, signals the debouncer worker, and returns HTTP 202 Accepted immediately.
func (d *SyncDaemon) EnqueueImageReadyEvent(req ImageReadyEventRequest) (ImageReadyEventResponse, int) {
	imgRef := strings.TrimSpace(req.Image)
	if imgRef == "" {
		return ImageReadyEventResponse{
			Status:  "error",
			Message: "missing required field: image",
		}, http.StatusBadRequest
	}

	matches := d.findAllNomadJobsByImage(imgRef)

	if len(matches) == 0 {
		log.Printf("[Hangar:ImageReady] No Nomad jobs found referencing image %s", imgRef)
		return ImageReadyEventResponse{
			Status:  "not_found",
			Image:   imgRef,
			Message: fmt.Sprintf("no nomad jobs found using image %s", imgRef),
		}, http.StatusNotFound
	}

	d.pendingImageMu.Lock()
	if d.pendingImageRollouts == nil {
		d.pendingImageRollouts = make(map[string]PendingImageRollout)
	}
	appliedJobs := make([]string, 0, len(matches))
	for _, m := range matches {
		d.pendingImageRollouts[m.JobName] = PendingImageRollout{
			JobName: m.JobName,
			JobPath: m.JobPath,
			Request: req,
		}
		appliedJobs = append(appliedJobs, m.JobName)
	}
	d.pendingImageMu.Unlock()

	// Non-blocking channel signal to debounce worker
	if d.imageRolloutCh != nil {
		select {
		case d.imageRolloutCh <- struct{}{}:
		default:
		}
	}

	log.Printf("[Hangar:ImageReady] Queued debounced rollout for image %s (matched %d job(s): %v)", imgRef, len(appliedJobs), appliedJobs)

	return ImageReadyEventResponse{
		Status:      "accepted",
		Image:       imgRef,
		MatchedJobs: appliedJobs,
		Message:     fmt.Sprintf("queued debounced rollout for %d job(s)", len(appliedJobs)),
	}, http.StatusAccepted
}

func resetDebounceTimer(timer *time.Timer, d time.Duration) (*time.Timer, <-chan time.Time) {
	if timer != nil {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	t := time.NewTimer(d)
	return t, t.C
}

func (d *SyncDaemon) safeExecutePendingImageRollouts(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Hangar:ImageRollout] PANIC recovered in executePendingImageRollouts: %v", r)
		}
	}()
	d.executePendingImageRollouts(ctx)
}

// StartImageRolloutLoop runs the background debounced worker goroutine for image rollouts.
func (d *SyncDaemon) StartImageRolloutLoop(ctx context.Context) {
	if d.imageRolloutCh == nil {
		d.imageRolloutCh = make(chan struct{}, 1)
	}

	debounceDuration := d.imageRolloutDebounce
	if debounceDuration <= 0 {
		debounceDuration = 3 * time.Second
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Hangar:ImageRollout] PANIC recovered in StartImageRolloutLoop: %v", r)
			}
		}()

		var (
			debounceTimer *time.Timer
			timerCh       <-chan time.Time
		)

		for {
			select {
			case <-ctx.Done():
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			case <-d.imageRolloutCh:
				debounceTimer, timerCh = resetDebounceTimer(debounceTimer, debounceDuration)
			case <-timerCh:
				timerCh = nil
				d.safeExecutePendingImageRollouts(ctx)
			}
		}
	}()
}

func (d *SyncDaemon) isHangarRequest(req ImageReadyEventRequest) bool {
	if IsHangarImage(req.Image) {
		return true
	}
	for _, m := range d.findAllNomadJobsByImage(req.Image) {
		if IsHangarJob(m.JobName) {
			return true
		}
	}
	return false
}

func (d *SyncDaemon) executePendingImageRollouts(ctx context.Context) {
	d.pendingImageMu.Lock()
	if len(d.pendingImageRollouts) == 0 {
		d.pendingImageMu.Unlock()
		return
	}
	// Deduplicate rollout requests by image to execute unified job rollout per image
	uniqueRequests := make(map[string]ImageReadyEventRequest)
	for _, v := range d.pendingImageRollouts {
		uniqueRequests[v.Request.Image] = v.Request
	}
	d.pendingImageRollouts = make(map[string]PendingImageRollout)
	d.pendingImageMu.Unlock()

	log.Printf("[Hangar:ImageRollout] Debounce timer expired, executing rollout batch for %d unique image(s)...", len(uniqueRequests))

	var nonHangar, hangar []ImageReadyEventRequest
	for _, req := range uniqueRequests {
		if d.isHangarRequest(req) {
			hangar = append(hangar, req)
		} else {
			nonHangar = append(nonHangar, req)
		}
	}

	for _, req := range append(nonHangar, hangar...) {
		if ctx.Err() != nil {
			return
		}
		resp, code := d.ExecuteImageReadyEvent(ctx, req)
		log.Printf("[Hangar:ImageRollout] Rollout for %s completed with code %d: status=%s, message=%s", req.Image, code, resp.Status, resp.Message)
	}
}
