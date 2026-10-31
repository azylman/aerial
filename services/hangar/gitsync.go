package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/services/hangar/pkg/metrics"
	"gopkg.in/yaml.v3"
)

// EnsureRepo clones or initializes the repository if .git does not exist.
func (d *SyncDaemon) EnsureRepo(ctx context.Context, repoPath, repoURL string) error {
	if repoPath == "" || repoURL == "" {
		return nil
	}

	if _, err := resolveGitDir(repoPath); err == nil {
		return nil
	}

	log.Printf("[Hangar] Bootstrapping repository at %s from %s...", repoPath, repoURL)
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		return fmt.Errorf("failed to create repo directory %s: %w", repoPath, err)
	}

	entries, err := os.ReadDir(repoPath)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		out, errBytes, err := d.getGitExecutor()(ctx, "", "clone", "--depth", "50", "-b", "main", repoURL, repoPath)
		if err != nil {
			combined := string(append(out, errBytes...))
			return fmt.Errorf("git clone failed for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
		}
		return nil
	}

	if out, errBytes, err := d.getGitExecutor()(ctx, repoPath, "init", "-b", "main"); err != nil {
		combined := string(append(out, errBytes...))
		return fmt.Errorf("git init failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
	}
	// Best-effort remote add: if origin already exists, do not fail
	if _, _, err := d.getGitExecutor()(ctx, repoPath, "remote", "add", "origin", repoURL); err != nil {
		log.Printf("[Hangar] Notice adding remote origin for %s (continuing): %v", repoPath, err)
	}
	out, errBytes, err := d.getGitExecutor()(ctx, repoPath, "fetch", "--depth", "50", "origin", "main")
	if err != nil {
		combined := string(append(out, errBytes...))
		return fmt.Errorf("git fetch failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
	}

	if out, errBytes, err := d.getGitExecutor()(ctx, repoPath, "reset", "--soft", "FETCH_HEAD"); err != nil {
		combined := string(append(out, errBytes...))
		return fmt.Errorf("git reset soft failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
	}
	return nil
}

// SyncRepo synchronizes a single repository via git pull --ff-only with fallback to reset --hard FETCH_HEAD.
func (d *SyncDaemon) SyncRepo(ctx context.Context, repoPath string, syncCtx ...ConfigSyncContext) (res RepoSyncResult) {
	res = RepoSyncResult{Repo: repoPath}

	if repoPath == "" {
		return res
	}

	repoLock := d.getRepoLock(repoPath)
	repoLock.Lock()
	defer repoLock.Unlock()

	start := time.Now()
	defer func() {
		status := "success"
		if res.Error != "" {
			status = "error"
		}
		metrics.RecordPull(repoPath, status, res.Changed, time.Since(start))
		if status == "success" {
			now := time.Now()
			metrics.RecordLastSync(repoPath, now)
			d.statusMu.Lock()
			if d.lastSyncTimes == nil {
				d.lastSyncTimes = make(map[string]time.Time)
			}
			d.lastSyncTimes[repoPath] = now
			d.statusMu.Unlock()
		}
	}()

	if repoURL, ok := d.repoUrls[repoPath]; ok && repoURL != "" {
		if err := d.EnsureRepo(ctx, repoPath, repoURL); err != nil {
			log.Printf("[Hangar] Warning: EnsureRepo failed for %s: %v", repoPath, err)
		}
	}

	gitDir, err := resolveGitDir(repoPath)
	if err != nil {
		res.Error = fmt.Sprintf("git dir not found: %v", err)
		return res
	}

	lockFile := filepath.Join(gitDir, "index.lock")
	if _, err := os.Stat(lockFile); err == nil {
		log.Printf("[Hangar] %s has index.lock present, skipping sync cycle", repoPath)
		res.Error = "index.lock active"
		return res
	}

	opCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()

	if _, stderr, err := d.getGitExecutor()(opCtx, "", "config", "--global", "safe.directory", "*"); err != nil {
		log.Printf("[Hangar] Warning: failed to configure safe.directory: %s (%v)", SanitizeLog(strings.TrimSpace(string(stderr))), err)
	}

	outBefore, _, err := d.getGitExecutor()(opCtx, repoPath, "rev-parse", "HEAD")
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[Hangar] Warning: failed to rev-parse HEAD before pull for %s: %s", repoPath, sanitizedErr)
		res.Error = sanitizedErr
		return res
	}
	res.PreviousHead = strings.TrimSpace(string(outBefore))
	res.CurrentHead = res.PreviousHead

	if syncErr := d.fetchAndFastForwardRepo(opCtx, repoPath); syncErr != "" {
		res.Error = syncErr
		return res
	}

	outAfter, _, err := d.getGitExecutor()(opCtx, repoPath, "rev-parse", "HEAD")
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		res.Error = sanitizedErr
		return res
	}
	res.CurrentHead = strings.TrimSpace(string(outAfter))

	if res.PreviousHead != res.CurrentHead {
		res.Changed = true
		log.Printf("[Hangar] Repository %s updated: %s -> %s", repoPath, res.PreviousHead, res.CurrentHead)
		d.enqueuePostSyncReconciliation(opCtx, repoPath, res.PreviousHead, res.CurrentHead, syncCtx)
	}

	return res
}

func (d *SyncDaemon) fetchAndFastForwardRepo(opCtx context.Context, repoPath string) string {
	// 1. Fetch remote commit metadata without modifying working tree
	outFetch, errFetchBytes, errFetch := d.getGitExecutor()(opCtx, repoPath, "fetch", "origin", "main")
	if errFetch != nil {
		combinedFetch := string(append(outFetch, errFetchBytes...))
		sanitizedOut := SanitizeLog(strings.TrimSpace(combinedFetch))
		sanitizedErr := SanitizeLog(errFetch.Error())
		log.Printf("[Hangar] Notice: git fetch origin main failed for %s (%s, %s). Attempting git pull fallback...", repoPath, sanitizedErr, sanitizedOut)

		outPull, errPullBytes, errPull := d.getGitExecutor()(opCtx, repoPath, "pull", "--ff-only")
		if errPull != nil {
			combinedPull := string(append(outPull, errPullBytes...))
			return fmt.Sprintf("fetch failed: %s; pull failed: %s", sanitizedOut, SanitizeLog(strings.TrimSpace(combinedPull)))
		}
		return ""
	}

	// 2. Fast-forward merge verified clean upstream commit
	outMerge, errMergeBytes, errMerge := d.getGitExecutor()(opCtx, repoPath, "merge", "--ff-only", "FETCH_HEAD")
	if errMerge != nil {
		combinedMerge := string(append(outMerge, errMergeBytes...))
		sanitizedOut := SanitizeLog(strings.TrimSpace(combinedMerge))
		sanitizedErr := SanitizeLog(errMerge.Error())
		log.Printf("[Hangar] Notice: git merge --ff-only failed for %s (%s, %s). Attempting safe reset recovery...", repoPath, sanitizedErr, sanitizedOut)

		outReset, errResetBytes, errReset := d.getGitExecutor()(opCtx, repoPath, "reset", "--hard", "FETCH_HEAD")
		if errReset != nil {
			combinedReset := string(append(outReset, errResetBytes...))
			return fmt.Sprintf("reset failed: %s", SanitizeLog(combinedReset))
		}

		if _, cleanStderr, cleanErr := d.getGitExecutor()(opCtx, repoPath, "clean", "-fd"); cleanErr != nil {
			log.Printf("[Hangar] Warning: git clean -fd failed for %s: %s (%v)", repoPath, SanitizeLog(strings.TrimSpace(string(cleanStderr))), cleanErr)
		}

		log.Printf("[Hangar] Successfully recovered %s via reset to FETCH_HEAD", repoPath)
	}

	return ""
}

func (d *SyncDaemon) enqueuePostSyncReconciliation(opCtx context.Context, repoPath, prevHead, currHead string, syncCtx []ConfigSyncContext) {
	nomadCfgChanged, errNomadCfg := d.HasNomadConfigChanges(opCtx, repoPath, prevHead, currHead)
	if errNomadCfg != nil {
		log.Printf("[Hangar] Warning: Failed to check nomad config changes in %s: %v", repoPath, errNomadCfg)
	}
	if nomadCfgChanged {
		log.Printf("[Hangar:GitOps] Nomad daemon configuration changes detected in %s (%s -> %s). Triggering debounced reconciliation.", repoPath, prevHead, currHead)
		d.recordPendingNomadServerRestart()
		if d.reconcileCh != nil {
			select {
			case d.reconcileCh <- struct{}{}:
			default:
			}
		}
	}

	if len(syncCtx) > 0 && syncCtx[0].SkipPendingNomad {
		return
	}

	nomadChanges, errNomad := d.HasNomadChanges(opCtx, repoPath, prevHead, currHead)
	if errNomad != nil {
		log.Printf("[Hangar] Warning: Failed to check Nomad changes in %s: %v", repoPath, errNomad)
	}
	if len(nomadChanges) > 0 {
		log.Printf("[Hangar:GitOps] Nomad job changes detected in %s (%d changes). Triggering debounced reconciliation.", repoPath, len(nomadChanges))
		var meta []string
		if len(syncCtx) > 0 {
			meta = []string{
				syncCtx[0].Repo,
				syncCtx[0].CommitSHA,
				syncCtx[0].TargetID,
				strconv.Itoa(syncCtx[0].PRNumber),
			}
		} else {
			meta = []string{
				NormalizeGitHubSlug(repoPath),
				currHead,
				"",
				"0",
			}
		}
		d.recordPendingNomadChanges(repoPath, nomadChanges, meta...)
		if d.reconcileCh != nil {
			select {
			case d.reconcileCh <- struct{}{}:
			default:
			}
		}
	}
}

// TriggerSync runs a synchronous singleflight sync across all managed repositories.
func (d *SyncDaemon) TriggerSync() ([]RepoSyncResult, error) {
	if d.triggerFn != nil {
		return d.triggerFn()
	}

	val, err, _ := d.sfg.Do("sync", func() (interface{}, error) {
		d.mu.Lock()
		if d.ticker != nil {
			d.ticker.Reset(d.interval)
		}
		d.mu.Unlock()

		syncCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		results := make([]RepoSyncResult, 0, len(d.repos))
		hasError := false
		anyChanged := false

		for _, repo := range d.repos {
			res := d.SyncRepo(syncCtx, repo)
			if res.Error != "" {
				hasError = true
			}
			if res.Changed {
				anyChanged = true
			}
			results = append(results, res)
		}

		status := "no_change"
		if hasError {
			status = "error"
		} else if anyChanged {
			status = "synced"
		}

		if !hasError && d.configDir != "" {
			d.syncAllConfigsToNomad(syncCtx, "[Hangar:Periodic]", d.configDir)
		}
		metrics.RecordSyncRequest("periodic", status)

		return results, nil
	})

	if err != nil {
		return nil, err
	}
	res, ok := val.([]RepoSyncResult)
	if !ok {
		return nil, fmt.Errorf("unexpected return type from singleflight: %T", val)
	}
	return res, nil
}

func (d *SyncDaemon) pushNomadConfigVar(ctx context.Context, cacheKey, nomadVar, jobName, configHash string, isChangedInGit bool, varArgs []string, syncCtx []ConfigSyncContext) (bool, error) {
	nomadExec := d.getNomadExecutor()
	if nomadExec == nil || d.nomadAddr == "" {
		return false, nil
	}

	cacheToken := configHash
	if len(syncCtx) > 0 && strings.TrimSpace(syncCtx[0].CommitSHA) != "" {
		cacheToken = configHash + ":" + strings.TrimSpace(syncCtx[0].CommitSHA)
	}

	d.lastPushedConfigMu.Lock()
	cachedHash := ""
	if d.lastPushedConfig != nil {
		cachedHash = d.lastPushedConfig[cacheKey]
	}
	if !isChangedInGit {
		if d.lastPushedConfig == nil {
			d.lastPushedConfig = make(map[string]string)
		}
		d.lastPushedConfig[cacheKey] = configHash
		d.lastPushedConfigMu.Unlock()
		return false, nil
	}
	if cachedHash == cacheToken || (cacheToken == configHash && strings.HasPrefix(cachedHash, configHash+":")) {
		d.lastPushedConfigMu.Unlock()
		return false, nil
	}
	d.lastPushedConfigMu.Unlock()

	valCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	args := append([]string{"var", "put", "-force", nomadVar}, varArgs...)
	if len(syncCtx) > 0 && syncCtx[0].CommitSHA != "" {
		args = append(args, "COMMIT_SHA="+syncCtx[0].CommitSHA)
		baseEvt := HangarDeployEvent{
			Event:        "deploy_started",
			JobName:      jobName,
			Repo:         syncCtx[0].Repo,
			CommitSHA:    syncCtx[0].CommitSHA,
			PRNumber:     syncCtx[0].PRNumber,
			TargetID:     syncCtx[0].TargetID,
			Status:       "started",
			ExpectedJobs: syncCtx[0].ExpectedJobs,
			Timestamp:    time.Now().UTC(),
		}
		go d.DispatchDeployEvent(baseEvt)
	}

	stdout, stderr, err := nomadExec(valCtx, args...)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return false, fmt.Errorf("failed updating Nomad variable %s: %s (%w)", nomadVar, SanitizeLog(strings.TrimSpace(combined)), err)
	}

	d.lastPushedConfigMu.Lock()
	if d.lastPushedConfig == nil {
		d.lastPushedConfig = make(map[string]string)
	}
	d.lastPushedConfig[cacheKey] = cacheToken
	d.lastPushedConfigMu.Unlock()

	return true, nil
}

func (d *SyncDaemon) pushNomadVarQuiet(ctx context.Context, cacheKey, nomadVar, jobName, configHash string, isChangedInGit bool, varArgs []string) (bool, error) {
	return d.pushNomadConfigVar(ctx, cacheKey, nomadVar, jobName, configHash, isChangedInGit, varArgs, nil)
}

func (d *SyncDaemon) computeBundleHash(bundle, configDir string) string {
	sources, excludes, err := ResolveAssetBundleSources(bundle, d.GetAssetBundleConfigs(configDir))
	if err != nil {
		return ""
	}
	_, hash, err := BuildAssetBundleTarGz(sources, excludes...)
	if err != nil {
		return ""
	}
	return hash
}

func (d *SyncDaemon) syncAllConfigsToNomad(ctx context.Context, logPrefix, configDir string, syncCtx ...ConfigSyncContext) {
	if err := d.SyncServiceConfigsToNomad(ctx, configDir, syncCtx...); err != nil {
		log.Printf("%s Notice: SyncServiceConfigsToNomad: %v", logPrefix, err)
	}
	if err := d.SyncAssetBundlesToNomad(ctx, configDir, syncCtx...); err != nil {
		log.Printf("%s Notice: SyncAssetBundlesToNomad: %v", logPrefix, err)
	}
}

// readAndValidateYAML reads filePath and unmarshals into out if non-nil.
// If the file does not exist, it returns (nil, nil).
func readAndValidateYAML(filePath, label string, out any) ([]byte, error) {
	raw, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read %s at %s: %w", label, filePath, err)
	}
	if out != nil {
		if err := yaml.Unmarshal(raw, out); err != nil {
			return nil, fmt.Errorf("invalid YAML syntax in %s: %w", filePath, err)
		}
		if filepath.Base(filePath) == "config.yaml" {
			var ch struct {
				Channels map[string]any `yaml:"channels"`
			}
			if yaml.Unmarshal(raw, &ch) == nil && ch.Channels != nil {
				hasDefault := false
				for k := range ch.Channels {
					if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(k)), "#") == "default" {
						hasDefault = true
						break
					}
				}
				if !hasDefault {
					return nil, fmt.Errorf("semantic validation error in %s: channels.default is required", filePath)
				}
			}
		}
	}
	return raw, nil
}

// ServiceConfigMapping defines the declarative mapping between a repository configuration file
// and its destination Nomad variable.
type ServiceConfigMapping struct {
	RelPath  string `yaml:"rel_path"`
	NomadVar string `yaml:"nomad_var"`
	VarKey   string `yaml:"var_key"`
}

// GetServiceConfigMappings resolves the active service config mappings for a given configDir.
// If configDir contains services/hangar/hangar.yaml with non-empty service_configs, it dynamically parses and returns them.
// Otherwise, it falls back to daemon-configured serviceConfigs, returning nil if neither is configured.
func (d *SyncDaemon) GetServiceConfigMappings(configDir string) []ServiceConfigMapping {
	if configDir != "" {
		hangarCfgPath := filepath.Join(configDir, "services", "hangar", "hangar.yaml")
		var hCfg HangarYAMLConfig
		if _, err := readAndValidateYAML(hangarCfgPath, "hangar config", &hCfg); err == nil && len(hCfg.ServiceConfigs) > 0 {
			return hCfg.ServiceConfigs
		}
	}
	if d != nil && len(d.serviceConfigs) > 0 {
		return d.serviceConfigs
	}
	return nil
}

// GetAssetBundleConfigs resolves the active asset bundle configs for a given configDir.
// If configDir contains services/hangar/hangar.yaml with non-empty asset_bundles, it dynamically parses and returns them.
// Otherwise, it falls back to daemon-configured assetBundles, returning nil if neither is configured.
func (d *SyncDaemon) GetAssetBundleConfigs(configDir string) []AssetBundleConfig {
	if configDir != "" {
		hangarCfgPath := filepath.Join(configDir, "services", "hangar", "hangar.yaml")
		var hCfg HangarYAMLConfig
		if _, err := readAndValidateYAML(hangarCfgPath, "hangar config", &hCfg); err == nil && len(hCfg.AssetBundles) > 0 {
			return hCfg.AssetBundles
		}
	}
	if d != nil && len(d.assetBundles) > 0 {
		return d.assetBundles
	}
	return nil
}

func anyFileInChangedList(targets []string, changedFiles []string) bool {
	for _, t := range targets {
		if fileInChangedList(t, changedFiles) {
			return true
		}
	}
	return false
}

// SyncServiceConfigsToNomad iterates over resolved service config mappings, validates YAML syntax,
// and pushes non-empty configurations to their respective Nomad variables using `nomad var put`.
func (d *SyncDaemon) SyncServiceConfigsToNomad(ctx context.Context, configDir string, syncCtx ...ConfigSyncContext) error {
	mappings := d.GetServiceConfigMappings(configDir)
	var order []string
	grouped := make(map[string][]ServiceConfigMapping)
	for _, m := range mappings {
		if _, ok := grouped[m.NomadVar]; !ok {
			order = append(order, m.NomadVar)
		}
		grouped[m.NomadVar] = append(grouped[m.NomadVar], m)
	}
	sort.SliceStable(order, func(i, j int) bool {
		return order[i] != "nomad/jobs/hangar" && order[j] == "nomad/jobs/hangar"
	})

	diffGated := len(syncCtx) > 0 && syncCtx[0].DiffGated
	var changedFiles []string
	if len(syncCtx) > 0 {
		changedFiles = syncCtx[0].ChangedFiles
	}

	for _, nomadVar := range order {
		var varArgs, varKeys, checkPaths []string
		var singleRaw []byte
		h := sha256.New()
		for _, mapping := range grouped[nomadVar] {
			filePath := filepath.Join(configDir, mapping.RelPath)
			ext := strings.ToLower(filepath.Ext(filePath))
			var parsedTarget any
			if ext == ".yaml" || ext == ".yml" {
				var parsed any
				parsedTarget = &parsed
			}
			raw, err := readAndValidateYAML(filePath, "service config", parsedTarget)
			if err != nil {
				return err
			}
			if raw == nil {
				continue
			}
			singleRaw = raw
			varKeys = append(varKeys, mapping.VarKey)
			varArgs = append(varArgs, mapping.VarKey+"=@"+filePath)
			checkPaths = append(checkPaths, mapping.RelPath)
			h.Write([]byte(mapping.VarKey + ":"))
			h.Write(raw)
			h.Write([]byte("\n"))
		}
		varKeys, varArgs = d.appendServiceAssetHash(nomadVar, configDir, varKeys, varArgs, h)

		if len(varArgs) == 0 {
			continue
		}
		jobName := strings.TrimPrefix(nomadVar, "nomad/jobs/")
		checkPaths = append(checkPaths, "jobs/"+jobName+".nomad", "nomad/jobs/"+jobName+".nomad")
		var configHash string
		if len(varArgs) == 1 && len(varKeys) == 1 && varKeys[0] != "ASSET_HASH" {
			sum := sha256.Sum256(singleRaw)
			configHash = hex.EncodeToString(sum[:])
		} else {
			configHash = hex.EncodeToString(h.Sum(nil))
		}
		// Invariant: nomad/jobs/homepage.nomad interpolates CONFIG_HASH inside its
		// local/homepage.yaml template (which has change_mode = "restart").
		// ASSET_HASH only drives a change_mode = "script" template. Writing CONFIG_HASH
		// ensures Nomad restarts the homepage allocation when either homepage config
		// or homepage asset bundle changes, enabling Synapse to track deployment lifecycle.
		if nomadVar == "nomad/jobs/homepage" {
			varKeys = append(varKeys, "CONFIG_HASH")
			varArgs = append(varArgs, "CONFIG_HASH="+configHash)
		}
		cacheKey := nomadVar + ":" + strings.Join(varKeys, "+")
		isChangedInGit := isServiceConfigChangedInGit(nomadVar, diffGated, checkPaths, changedFiles, d.GetAssetBundleConfigs(configDir))
		pushed, err := d.pushNomadConfigVar(ctx, cacheKey, nomadVar, jobName, configHash, isChangedInGit, varArgs, syncCtx)
		if err != nil {
			return err
		}
		if pushed {
			log.Printf("[Hangar:Nomad] Successfully pushed %s to %s", strings.Join(varKeys, ", "), nomadVar)
		}
	}
	return nil
}

// SyncAssetBundlesToNomad computes asset bundle hashes and synchronizes ASSET_HASH to Nomad variables.
func (d *SyncDaemon) SyncAssetBundlesToNomad(ctx context.Context, configDir string, syncCtx ...ConfigSyncContext) error {
	diffGated := len(syncCtx) > 0 && syncCtx[0].DiffGated
	var changedFiles []string
	if len(syncCtx) > 0 {
		changedFiles = syncCtx[0].ChangedFiles
	}

	mappedVars := make(map[string]bool)
	for _, m := range d.GetServiceConfigMappings(configDir) {
		mappedVars[m.NomadVar] = true
	}

	var firstErr error
	var serviceConfigsSynced bool
	for _, bundle := range d.GetAssetBundleConfigs(configDir) {
		if bundle.NomadVar == "" {
			continue
		}
		isChanged := !diffGated || AssetBundleMatchesRepoChanges(bundle, "", changedFiles)
		if mappedVars[bundle.NomadVar] {
			if isChanged && !serviceConfigsSynced {
				serviceConfigsSynced = true
				if err := d.SyncServiceConfigsToNomad(ctx, configDir, syncCtx...); err != nil {
					log.Printf("[Hangar:Nomad] Notice: SyncAssetBundlesToNomad (%s): %v", bundle.Name, err)
					if firstErr == nil {
						firstErr = err
					}
				}
			}
			continue
		}

		if hash := d.computeBundleHash(bundle.Name, configDir); hash != "" {
			jobName := strings.TrimPrefix(bundle.NomadVar, "nomad/jobs/")
			cacheKey := bundle.NomadVar + ":ASSET_HASH"
			if _, err := d.pushNomadVarQuiet(ctx, cacheKey, bundle.NomadVar, jobName, hash, isChanged, []string{"ASSET_HASH=" + hash}); err != nil {
				log.Printf("[Hangar:Nomad] Notice: SyncAssetBundlesToNomad (%s): %v", bundle.Name, err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	return firstErr
}

// StartPeriodicLoop runs the background ticker loop.
func (d *SyncDaemon) StartPeriodicLoop(ctx context.Context) {
	d.mu.Lock()
	d.ticker = time.NewTicker(d.interval)
	ticker := d.ticker
	d.mu.Unlock()

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, syncErr := d.TriggerSync(); syncErr != nil {
					log.Printf("[Hangar] Periodic sync notice: %v", syncErr)
				}
				if imgErr := d.CheckAndReconcileNomadImages(ctx); imgErr != nil {
					log.Printf("[Hangar] Periodic image check notice: %v", imgErr)
				}
			}
		}
	}()
}

// getRepoCommit extracts the commit SHA and author timestamp for a given ref in a repository.
func (d *SyncDaemon) getRepoCommit(ctx context.Context, repoPath, ref string) (string, *time.Time, error) {
	out, _, err := d.getGitExecutor()(ctx, repoPath, "log", "-1", "--format=%H%x00%aI", ref)
	if err != nil {
		return "", nil, err
	}
	parts := strings.Split(strings.TrimSpace(string(out)), "\x00")
	if len(parts) < 2 {
		return strings.TrimSpace(string(out)), nil, nil
	}
	sha := strings.TrimSpace(parts[0])
	if t, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(parts[1])); parseErr == nil {
		return sha, &t, nil
	}
	return sha, nil, nil
}

func (d *SyncDaemon) resolveRepoGitHubSlug(ctx context.Context, repo string) string {
	d.statusMu.RLock()
	var ghSlug string
	if d.repoGitHubSlugs != nil {
		ghSlug = d.repoGitHubSlugs[repo]
	}
	d.statusMu.RUnlock()
	if ghSlug != "" {
		return ghSlug
	}

	out, _, err := d.getGitExecutor()(ctx, repo, "remote", "get-url", "origin")
	if err != nil || len(out) == 0 {
		out, _, err = d.getGitExecutor()(ctx, repo, "config", "--get", "remote.origin.url")
	}
	if err != nil {
		out = nil
	}
	rawURL := strings.TrimSpace(string(out))
	if rawURL == "" && d.repoUrls != nil {
		rawURL = d.repoUrls[repo]
	}
	if rawURL == "" {
		return ""
	}

	ghSlug = ParseGitHubSlug(rawURL)
	if ghSlug != "" {
		d.statusMu.Lock()
		if d.repoGitHubSlugs == nil {
			d.repoGitHubSlugs = make(map[string]string)
		}
		d.repoGitHubSlugs[repo] = ghSlug
		d.statusMu.Unlock()
	}
	return ghSlug
}

func (d *SyncDaemon) buildSingleRepoStatus(ctx context.Context, repo string, syncTimes map[string]time.Time) RepoStatus {
	st := RepoStatus{
		Repo:       repo,
		SyncStatus: "synced",
	}

	st.GitHubRepo = d.resolveRepoGitHubSlug(ctx, repo)

	if t, ok := syncTimes[repo]; ok {
		st.LastSyncTime = t
	}

	// Check disk HEAD
	diskSha, diskTime, err := d.getRepoCommit(ctx, repo, "HEAD")
	if err != nil {
		st.SyncStatus = "error"
		st.Error = fmt.Sprintf("failed to get disk HEAD: %v", SanitizeLog(err.Error()))
		return st
	}
	st.DiskCommit = diskSha
	st.DiskCommitTime = diskTime

	// Check remote commit: try origin/main, fallback to FETCH_HEAD
	remoteSha, remoteTime, err := d.getRepoCommit(ctx, repo, "origin/main")
	if err != nil || remoteSha == "" {
		remoteSha, remoteTime, err = d.getRepoCommit(ctx, repo, "FETCH_HEAD")
	}
	if err != nil || remoteSha == "" {
		remoteSha = diskSha
		remoteTime = diskTime
	}
	st.RemoteCommit = remoteSha
	st.RemoteCommitTime = remoteTime

	if diskTime != nil && remoteTime != nil {
		lag := int64(remoteTime.Sub(*diskTime).Seconds())
		if lag < 0 {
			lag = 0
		}
		st.TimeLagSeconds = lag
		if diskSha != remoteSha && lag > 0 {
			st.SyncStatus = "lagging"
		}
	}

	return st
}

// GetStatus computes real-time synchronization telemetry across all configured repositories.
func (d *SyncDaemon) GetStatus(ctx context.Context) HangarStatusResponse {
	rec := d.GetReconciliationStatus()
	resp := HangarStatusResponse{
		Status:         "synced",
		Repos:          make(map[string]RepoStatus),
		Reconciliation: &rec,
	}

	d.statusMu.RLock()
	syncTimes := make(map[string]time.Time, len(d.lastSyncTimes))
	for k, v := range d.lastSyncTimes {
		syncTimes[k] = v
	}
	d.statusMu.RUnlock()

	var maxLag int64
	var latestSync time.Time

	for _, repo := range d.repos {
		st := d.buildSingleRepoStatus(ctx, repo, syncTimes)
		if st.LastSyncTime.After(latestSync) {
			latestSync = st.LastSyncTime
		}
		if st.TimeLagSeconds > maxLag {
			maxLag = st.TimeLagSeconds
		}
		resp.Repos[repo] = st
	}

	resp.MaxLagSeconds = maxLag
	resp.LastSyncTime = latestSync
	if resp.LastSyncTime.IsZero() {
		resp.LastSyncTime = time.Now()
	}

	resp.Status = CalculateSyncStatus(resp.Repos)

	return resp
}

func (d *SyncDaemon) appendServiceAssetHash(nomadVar, configDir string, varKeys, varArgs []string, h io.Writer) ([]string, []string) {
	var bundleName string
	for _, b := range d.GetAssetBundleConfigs(configDir) {
		if b.NomadVar == nomadVar {
			bundleName = b.Name
			break
		}
	}
	if bundleName == "" {
		return varKeys, varArgs
	}
	if assetHash := d.computeBundleHash(bundleName, configDir); assetHash != "" {
		varKeys = append(varKeys, "ASSET_HASH")
		varArgs = append(varArgs, "ASSET_HASH="+assetHash)
		if _, err := h.Write([]byte("ASSET_HASH:" + assetHash + "\n")); err != nil {
			return varKeys, varArgs
		}
	}
	return varKeys, varArgs
}

func isServiceConfigChangedInGit(nomadVar string, diffGated bool, checkPaths, changedFiles []string, configured ...[]AssetBundleConfig) bool {
	if !diffGated || anyFileInChangedList(checkPaths, changedFiles) {
		return true
	}
	var bundles []AssetBundleConfig
	if len(configured) > 0 {
		bundles = configured[0]
	}
	for _, b := range bundles {
		if b.NomadVar == nomadVar {
			return AssetBundleMatchesRepoChanges(b, "", changedFiles)
		}
	}
	return false
}
