package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// GitPushEventRequest represents an incoming git push event.
type GitPushEventRequest struct {
	Repo     string `json:"repo"`
	Ref      string `json:"ref"`
	Commit   string `json:"commit"`
	TargetID string `json:"target_id,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
}

// GitPushEventResponse represents the acknowledgment for a git push event.
type GitPushEventResponse struct {
	Status             string   `json:"status"` // "accepted", "ignored", "error"
	Repo               string   `json:"repo,omitempty"`
	Ref                string   `json:"ref,omitempty"`
	Commit             string   `json:"commit,omitempty"`
	ContainersBuilding bool     `json:"containers_building,omitempty"`
	NomadChanged       bool     `json:"nomad_changed,omitempty"`
	AppliedJobs        []string `json:"applied_jobs,omitempty"`
	ExpectedJobs       []string `json:"expected_jobs,omitempty"`
	Message            string   `json:"message,omitempty"`
}

// ImageReadyEventRequest represents an incoming notification of a published container image.
type ImageReadyEventRequest struct {
	Image     string `json:"image"`
	Digest    string `json:"digest,omitempty"`
	Repo      string `json:"repo,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
}

// ImageReadyEventResponse represents the acknowledgment for an image ready event.
type ImageReadyEventResponse struct {
	Status      string   `json:"status"` // "accepted", "not_found", "error"
	Image       string   `json:"image,omitempty"`
	MatchedJobs []string `json:"matched_jobs,omitempty"`
	Message     string   `json:"message,omitempty"`
}

// PendingImageRollout represents an enqueued container rollout pending debounced execution.
type PendingImageRollout struct {
	JobName string
	JobPath string
	Request ImageReadyEventRequest
}

var (
	catalogBlockRegex = regexp.MustCompile(`(?s)const\s+catalog\s*=\s*\[(.*?)\]`)
	catalogObjRegex   = regexp.MustCompile(`(?s)\{([^{}]+)\}`)
	servicePropRegex  = regexp.MustCompile(`(?i)["']?service["']?\s*:\s*["']([^"']+)["']`)
	imagePropRegex    = regexp.MustCompile(`(?i)["']?image_name["']?\s*:\s*["']([^"']+)["']`)
)

// ParseWorkflowBuildRules extracts paths-filter rules and catalog image mappings from workflow YAML bytes.
func ParseWorkflowBuildRules(workflowYAML []byte) (filters map[string][]string, catalog map[string]string) {
	filters = make(map[string][]string)
	catalog = make(map[string]string)
	if len(workflowYAML) == 0 {
		return filters, catalog
	}

	// 1. Extract catalog: scan for const catalog = [...]
	for _, blockMatch := range catalogBlockRegex.FindAllSubmatch(workflowYAML, -1) {
		if len(blockMatch) < 2 {
			continue
		}
		for _, objMatch := range catalogObjRegex.FindAllSubmatch(blockMatch[1], -1) {
			if len(objMatch) < 2 {
				continue
			}
			objStr := string(objMatch[1])
			sMatch := servicePropRegex.FindStringSubmatch(objStr)
			iMatch := imagePropRegex.FindStringSubmatch(objStr)
			if len(sMatch) >= 2 && len(iMatch) >= 2 {
				svc := strings.TrimSpace(sMatch[1])
				img := strings.TrimSpace(iMatch[1])
				if svc != "" && img != "" {
					catalog[svc] = img
				}
			}
		}
	}

	// 2. Extract dorny/paths-filter via direct struct unmarshaling
	var parsed struct {
		Jobs map[string]struct {
			Steps []struct {
				With struct {
					Filters string `yaml:"filters"`
				} `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(workflowYAML, &parsed); err == nil {
		for _, job := range parsed.Jobs {
			for _, step := range job.Steps {
				if strings.TrimSpace(step.With.Filters) == "" {
					continue
				}
				var stepFilters map[string][]string
				if err := yaml.Unmarshal([]byte(step.With.Filters), &stepFilters); err == nil {
					for k, v := range stepFilters {
						filters[k] = v
					}
				}
			}
		}
	}

	return filters, catalog
}

// DiscoverRepoWorkflowRules reads .github/workflows/*.yml and *.yaml inside repoPath and merges discovered filters and catalog maps.
// Returns empty maps if repoPath has no workflow files or no catalog.
func DiscoverRepoWorkflowRules(repoPath string) (filters map[string][]string, catalog map[string]string) {
	filters = make(map[string][]string)
	catalog = make(map[string]string)
	if strings.TrimSpace(repoPath) == "" {
		return filters, catalog
	}

	wfDir := filepath.Join(repoPath, ".github", "workflows")
	entries, err := os.ReadDir(wfDir)
	if err != nil {
		return filters, catalog
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := strings.ToLower(entry.Name())
		if !strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(wfDir, entry.Name()))
		if err != nil {
			continue
		}
		f, c := ParseWorkflowBuildRules(data)
		for svc, globs := range f {
			existing := filters[svc]
			seen := make(map[string]bool, len(existing))
			for _, g := range existing {
				seen[g] = true
			}
			for _, g := range globs {
				if !seen[g] {
					seen[g] = true
					existing = append(existing, g)
				}
			}
			filters[svc] = existing
		}
		for svc, img := range c {
			catalog[svc] = img
		}
	}

	if len(catalog) == 0 {
		return make(map[string][]string), make(map[string]string)
	}

	return filters, catalog
}

func matchWorkflowPattern(pattern, cleanFile string) bool {
	cleanPattern := cleanChangedFilePath(pattern)
	if cleanPattern == "" {
		return false
	}
	if cleanPattern == "**" {
		return true
	}
	if strings.HasSuffix(cleanPattern, "/**") {
		prefix := strings.TrimSuffix(cleanPattern, "/**")
		return cleanFile == prefix || strings.HasPrefix(cleanFile, prefix+"/")
	}
	matched, err := path.Match(cleanPattern, cleanFile)
	return err == nil && matched
}

// AffectedWorkflowImages matches changedFiles against filters[service] globs for each service -> imageName
// in DiscoverRepoWorkflowRules(repoPath) and returns the sorted, deduplicated slice of affected imageName strings.
func AffectedWorkflowImages(repoPath string, changedFiles []string) []string {
	if strings.TrimSpace(repoPath) == "" || len(changedFiles) == 0 {
		return nil
	}
	filters, catalog := DiscoverRepoWorkflowRules(repoPath)
	if len(filters) == 0 || len(catalog) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	for _, f := range changedFiles {
		clean := cleanChangedFilePath(f)
		if clean == "" || clean == "." {
			continue
		}
		for svc, imageName := range catalog {
			if _, already := seen[imageName]; already {
				continue
			}
			for _, g := range filters[svc] {
				if matchWorkflowPattern(g, clean) {
					seen[imageName] = struct{}{}
					break
				}
			}
		}
	}

	if len(seen) == 0 {
		return nil
	}
	images := make([]string, 0, len(seen))
	for img := range seen {
		images = append(images, img)
	}
	sort.Strings(images)
	return images
}

// HasContainerBuildChanges evaluates if any changed file in a repository touches paths that trigger container builds.
// Strictly returns len(AffectedWorkflowImages(repoPath, changedFiles)) > 0 with zero hardcoded path-list or Dockerfile fallbacks.
func HasContainerBuildChanges(repoPath string, changedFiles []string) bool {
	return len(AffectedWorkflowImages(repoPath, changedFiles)) > 0
}

// ResolveAssetBundleSources maps a bundle name ("migrations", "dashboard", "docs", "grafana", "homepage", "mirrormere")
// to its prefix->sourceDir map and excluded relative paths.
func ResolveAssetBundleSources(bundle string, bundles []AssetBundleConfig) (map[string]string, []string, error) {
	b := strings.Trim(strings.TrimSuffix(strings.Trim(filepath.ToSlash(bundle), "/"), ".tar.gz"), "/")
	for _, cfg := range bundles {
		if strings.EqualFold(cfg.Name, b) {
			return cfg.Sources, cfg.Excludes, nil
		}
	}
	return nil, nil, fmt.Errorf("unknown asset bundle: %s", bundle)
}

func writeTarFile(tw *tar.Writer, p, arcPath string, d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		return err
	}
	if d.IsDir() {
		return tw.WriteHeader(&tar.Header{Name: arcPath + "/", Typeflag: tar.TypeDir, Mode: 0o755, ModTime: time.Unix(0, 0).UTC()})
	}
	mode := int64(0o644)
	if info.Mode()&0o111 != 0 || strings.HasSuffix(strings.ToLower(d.Name()), ".sh") {
		mode = 0o755
	}
	if err := tw.WriteHeader(&tar.Header{Name: arcPath, Typeflag: tar.TypeReg, Mode: mode, Size: info.Size(), ModTime: time.Unix(0, 0).UTC()}); err != nil {
		return err
	}
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(tw, f)
	if cErr := f.Close(); copyErr == nil {
		copyErr = cErr
	}
	return copyErr
}

func walkTarSource(tw *tar.Writer, prefix, srcDir string, excludeRelPaths []string) error {
	stat, err := os.Stat(srcDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !stat.IsDir() {
		return nil
	}
	return filepath.WalkDir(srcDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || p == srcDir {
			return walkErr
		}
		if name := d.Name(); name == ".git" || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() && !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, p)
		if err != nil {
			return err
		}
		arcPath := filepath.ToSlash(rel)
		if prefix != "" {
			arcPath = prefix + "/" + arcPath
		}
		for _, excl := range excludeRelPaths {
			if c := strings.Trim(filepath.ToSlash(excl), "/"); c != "" && (arcPath == c || strings.HasPrefix(arcPath, c+"/")) {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		return writeTarFile(tw, p, arcPath, d)
	})
}

// BuildAssetBundleTarGz packs one or more source directories (mapped to archive prefixes)
// into a deterministic gzip-compressed tar archive and returns the raw bytes and hex SHA-256 digest.
func BuildAssetBundleTarGz(sources map[string]string, excludeRelPaths ...string) ([]byte, string, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.ModTime = time.Unix(0, 0).UTC()
	tw := tar.NewWriter(gw)

	prefixes := make([]string, 0, len(sources))
	for p := range sources {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)
	for _, prefixKey := range prefixes {
		prefix := strings.Trim(filepath.ToSlash(prefixKey), "/")
		if prefix == "." {
			prefix = ""
		}
		if prefix != "" && tw.WriteHeader(&tar.Header{Name: prefix + "/", Typeflag: tar.TypeDir, Mode: 0o755, ModTime: time.Unix(0, 0).UTC()}) != nil {
			return nil, "", fmt.Errorf("failed writing tar header for %s", prefix)
		}
		if err := walkTarSource(tw, prefix, sources[prefixKey], excludeRelPaths); err != nil {
			return nil, "", err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, "", err
	}
	if err := gw.Close(); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:]), nil
}

func isJobMatch(clean, job string) bool {
	return clean == "nomad/jobs/"+job+".nomad" || clean == "jobs/"+job+".nomad"
}

func bundleJobMatches(bundle AssetBundleConfig, cleanF string) bool {
	if bundle.TriggerJob != "" {
		cleanTrigger := cleanChangedFilePath(bundle.TriggerJob)
		triggerBase := strings.TrimSuffix(filepath.Base(cleanTrigger), ".nomad")
		if cleanF == cleanTrigger || isJobMatch(cleanF, triggerBase) {
			return true
		}
	}
	if bundle.NomadVar != "" {
		jobName := strings.TrimPrefix(bundle.NomadVar, "nomad/jobs/")
		if isJobMatch(cleanF, jobName) {
			return true
		}
	}
	return false
}

func isPathExcluded(cleanF, rel, arcPath string, excludes []string) bool {
	for _, excl := range excludes {
		c := strings.Trim(filepath.ToSlash(excl), "/")
		if c != "" && (arcPath == c || strings.HasPrefix(arcPath, c+"/") || rel == c || strings.HasPrefix(rel, c+"/") || cleanF == c || strings.HasPrefix(cleanF, c+"/")) {
			return true
		}
	}
	return false
}

func computeArcPath(prefixKey, rel string) string {
	prefix := strings.Trim(filepath.ToSlash(prefixKey), "/")
	if prefix == "." {
		prefix = ""
	}
	arcPath := filepath.ToSlash(rel)
	if prefix != "" {
		if rel == "" || rel == "." {
			return prefix
		}
		return prefix + "/" + arcPath
	}
	return arcPath
}

func checkFileMatchesSources(absFile, cleanF string, bundle AssetBundleConfig) bool {
	for prefixKey, srcDir := range bundle.Sources {
		cleanSrc := filepath.Clean(srcDir)
		var rel string
		var isUnder bool
		if filepath.IsAbs(cleanSrc) {
			relToSrc, err := filepath.Rel(cleanSrc, absFile)
			if err == nil && !strings.HasPrefix(relToSrc, "..") {
				rel = relToSrc
				isUnder = true
			}
		} else {
			cleanSrcSlash := cleanChangedFilePath(cleanSrc)
			if cleanF == cleanSrcSlash || strings.HasPrefix(cleanF, cleanSrcSlash+"/") {
				rel = strings.TrimPrefix(strings.TrimPrefix(cleanF, cleanSrcSlash), "/")
				isUnder = true
			}
		}
		if !isUnder {
			continue
		}
		arcPath := computeArcPath(prefixKey, rel)
		if !isPathExcluded(cleanF, rel, arcPath, bundle.Excludes) {
			return true
		}
	}
	return false
}

// AssetBundleMatchesRepoChanges checks if a git push in repoPath affects an AssetBundleConfig.
func AssetBundleMatchesRepoChanges(bundle AssetBundleConfig, repoPath string, changedFiles []string) bool {
	cleanRepo := filepath.Clean(repoPath)
	for _, f := range changedFiles {
		cleanF := cleanChangedFilePath(f)
		if cleanF == "" || cleanF == "." {
			continue
		}
		if bundleJobMatches(bundle, cleanF) {
			return true
		}
		absFile := filepath.Clean(filepath.Join(cleanRepo, filepath.FromSlash(cleanF)))
		if checkFileMatchesSources(absFile, cleanF, bundle) {
			return true
		}
	}
	return false
}

// AssetBundleChangedInList evaluates whether any changed file affects the specified asset bundle.
func AssetBundleChangedInList(bundle string, changedFiles []string, configured ...[]AssetBundleConfig) bool {
	b := strings.Trim(strings.TrimSuffix(strings.ToLower(strings.TrimSpace(bundle)), ".tar.gz"), "/")
	var bundles []AssetBundleConfig
	if len(configured) > 0 {
		bundles = configured[0]
	}
	for _, cfg := range bundles {
		if strings.EqualFold(cfg.Name, b) {
			return matchAssetBundleFiles(cfg, changedFiles)
		}
	}
	return false
}

func checkRelMatchesSources(cleanF string, bundle AssetBundleConfig) bool {
	for prefixKey, srcDir := range bundle.Sources {
		var rel string
		var isUnder bool
		if filepath.IsAbs(srcDir) {
			cleanSrc := filepath.ToSlash(filepath.Clean(srcDir))
			parts := strings.Split(cleanSrc, "/")
			for i := 1; i < len(parts); i++ {
				candidate := strings.Join(parts[i:], "/")
				if cleanF == candidate || strings.HasPrefix(cleanF, candidate+"/") {
					rel = strings.TrimPrefix(strings.TrimPrefix(cleanF, candidate), "/")
					isUnder = true
					break
				}
			}
		} else {
			cleanSrc := cleanChangedFilePath(srcDir)
			cleanSrcRel := strings.TrimPrefix(cleanSrc, "./")
			if cleanSrcRel == "." {
				cleanSrcRel = ""
			}
			if cleanSrcRel != "" && (cleanF == cleanSrcRel || strings.HasPrefix(cleanF, cleanSrcRel+"/")) {
				rel = strings.TrimPrefix(strings.TrimPrefix(cleanF, cleanSrcRel), "/")
				isUnder = true
			}
		}
		if !isUnder {
			continue
		}
		arcPath := computeArcPath(prefixKey, rel)
		if !isPathExcluded(cleanF, rel, arcPath, bundle.Excludes) {
			return true
		}
	}
	return false
}

func matchAssetBundleFiles(bundle AssetBundleConfig, changedFiles []string) bool {
	for _, f := range changedFiles {
		cleanF := cleanChangedFilePath(f)
		if cleanF == "" || cleanF == "." {
			continue
		}
		if bundleJobMatches(bundle, cleanF) {
			return true
		}
		if checkRelMatchesSources(cleanF, bundle) {
			return true
		}
	}
	return false
}

// ImageMatches compares an incoming image reference with an image declared in a Nomad job specification.
func ImageMatches(query, target string) bool {
	q := strings.TrimSpace(strings.ToLower(query))
	t := strings.TrimSpace(strings.ToLower(target))
	if q == "" || t == "" {
		return false
	}
	if q == t {
		return true
	}
	cleanQ := stripImageTag(q)
	cleanT := stripImageTag(t)
	if cleanQ == cleanT {
		return true
	}
	baseQ := path.Base(cleanQ)
	baseT := path.Base(cleanT)
	return baseQ == baseT
}

// ReplaceImageTag returns imageRef with its tag replaced by tag.
// If either imageRef or tag is empty after trimming, the trimmed imageRef is returned unchanged.
// It strips any existing tag or @sha256:... digest via stripImageTag and returns stripImageTag(ref) + ":" + t.
func ReplaceImageTag(imageRef, tag string) string {
	ref := strings.TrimSpace(imageRef)
	t := strings.TrimSpace(tag)
	if ref == "" || t == "" {
		return ref
	}
	return stripImageTag(ref) + ":" + t
}

func stripImageTag(img string) string {
	if idx := strings.Index(img, "@"); idx != -1 {
		img = img[:idx]
	}
	lastSlash := strings.LastIndex(img, "/")
	if lastColon := strings.LastIndex(img, ":"); lastColon > lastSlash {
		img = img[:lastColon]
	}
	return img
}

// IsSafeJobPath validates that targetPath resides strictly within baseDir and ends with .nomad or .nomad.hcl.
func IsSafeJobPath(baseDir, targetPath string) bool {
	if baseDir == "" || targetPath == "" {
		return false
	}
	cleanBase := filepath.Clean(baseDir)
	cleanTarget := filepath.Clean(targetPath)
	rel, err := filepath.Rel(cleanBase, cleanTarget)
	if err != nil || strings.HasPrefix(rel, "..") || rel == "." {
		return false
	}
	return IsNomadJobFile(cleanTarget)
}

// MatchedNomadJob holds metadata for a Nomad job matching an image reference.
type MatchedNomadJob struct {
	JobName string
	JobPath string
}

// NormalizeGitHubSlug normalizes an input repository name or URL to an "owner/repo" slug.
// Defaults to "azylman" as the owner if none is specified.
func NormalizeGitHubSlug(input string) string {
	s := strings.TrimSpace(input)
	if s == "" {
		return ""
	}
	s = strings.TrimPrefix(s, "https://github.com/")
	s = strings.TrimPrefix(s, "http://github.com/")
	s = strings.TrimPrefix(s, "git@github.com:")
	s = strings.TrimSuffix(s, ".git")
	s = strings.TrimPrefix(s, "/")

	// Handle local paths like /share/aerial
	if strings.HasPrefix(input, "/") {
		base := filepath.Base(filepath.Clean(input))
		if base != "" && base != "." && base != "/" {
			return "azylman/" + base
		}
	}

	parts := strings.Split(s, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + "/" + parts[len(parts)-1]
	}
	if len(parts) == 1 && parts[0] != "" {
		return "azylman/" + parts[0]
	}
	return s
}

// ResolveRepoPath matches an incoming repository query (slug, URL, or short name)
// against the daemon's configured repository paths on disk.
func ResolveRepoPath(query string, configuredRepos []string) string {
	q := strings.TrimSpace(strings.ToLower(query))
	if q == "" {
		return ""
	}
	q = strings.TrimSuffix(q, ".git")
	baseQ := path.Base(q)
	cleanQ := filepath.Clean(q)

	for _, repo := range configuredRepos {
		cleanRepo := filepath.Clean(repo)
		cleanLower := strings.ToLower(cleanRepo)
		baseRepo := strings.ToLower(filepath.Base(cleanRepo))

		if cleanLower == cleanQ || cleanLower == q {
			return repo
		}
		if baseRepo == q || baseRepo == baseQ {
			return repo
		}
		if strings.HasSuffix(cleanLower, "/"+baseQ) {
			return repo
		}
	}
	return ""
}

// ResolveImageSourceRepo resolves which GitHub source repository is responsible for building a container image.
// Iterates candidateRepos, inspects DiscoverRepoWorkflowRules(r), and returns NormalizeGitHubSlug(r) for the first
// repo whose catalog contains an imageName matching imageRef via ImageMatches(imageRef, imageName) (or "" if external).
func ResolveImageSourceRepo(imageRef string, candidateRepos []string) string {
	ref := strings.TrimSpace(imageRef)
	if ref == "" {
		return ""
	}
	for _, r := range candidateRepos {
		if strings.TrimSpace(r) == "" {
			continue
		}
		_, catalog := DiscoverRepoWorkflowRules(r)
		for _, imageName := range catalog {
			if ImageMatches(ref, imageName) {
				return NormalizeGitHubSlug(r)
			}
		}
	}
	return ""
}

// HangarDeployEvent represents deployment and rollback lifecycle events.
type HangarDeployEvent struct {
	Event        string    `json:"event"`                   // "deploy_started", "deploy_success", "deploy_failed", "deploy_rollback"
	JobName      string    `json:"job_name"`                // Nomad job name (e.g. "synapse", "brain")
	Repo         string    `json:"repo,omitempty"`          // repository name (e.g. "azylman/aerial", "azylman/aerial-config")
	CommitSHA    string    `json:"commit_sha,omitempty"`    // git commit SHA (head or merge commit)
	PRNumber     int       `json:"pr_number,omitempty"`     // PR number if known
	TargetID     string    `json:"target_id,omitempty"`     // Discord thread / channel snowflake
	Image        string    `json:"image,omitempty"`         // container image (e.g. ghcr.io/azylman/aerial-synapse:latest)
	Digest       string    `json:"digest,omitempty"`        // image digest (sha256:...)
	Status       string    `json:"status"`                  // "started", "success", "failed", "rollback"
	DeploymentID string    `json:"deployment_id,omitempty"` // Nomad deployment UUID
	Details      string    `json:"details,omitempty"`       // status description or error message
	ExpectedJobs []string  `json:"expected_jobs,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
}

// IsHangarJob checks whether a job name represents the hangar service.
func IsHangarJob(name string) bool { return strings.EqualFold(strings.TrimSpace(name), "hangar") }

// IsHangarImage checks whether an image reference is for the hangar service.
func IsHangarImage(img string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(img)), "hangar")
}

// SortNomadChangesHangarLast sorts NomadFileChange slices so hangar changes execute last.
func SortNomadChangesHangarLast(changes []NomadFileChange) {
	sort.SliceStable(changes, func(i, j int) bool {
		return !IsHangarJob(changes[i].JobName) && IsHangarJob(changes[j].JobName)
	})
}

// SortMatchedJobsHangarLast sorts MatchedNomadJob slices so hangar is always executed last.
func SortMatchedJobsHangarLast(matches []MatchedNomadJob) {
	sort.SliceStable(matches, func(i, j int) bool {
		return !IsHangarJob(matches[i].JobName) && IsHangarJob(matches[j].JobName)
	})
}

func resolveAerialConfigExpectedJobs(changedFiles []string, serviceConfigs []ServiceConfigMapping, add func(string)) {
	if fileInChangedList("config.yaml", changedFiles) {
		add("eri")
	}
	if homepageInChangedList(changedFiles) {
		add("homepage")
	}
	for _, m := range serviceConfigs {
		if fileInChangedList(m.RelPath, changedFiles) {
			add(strings.TrimPrefix(strings.TrimSpace(m.NomadVar), "nomad/jobs/"))
		}
	}
}

func resolveContainerWorkflowExpectedJobs(repoPath string, changedFiles, candidateRepos []string, add func(string)) {
	affectedImages := AffectedWorkflowImages(repoPath, changedFiles)
	if len(affectedImages) == 0 {
		return
	}
	var dirs []string
	for _, r := range append([]string{repoPath}, candidateRepos...) {
		dirs = append(dirs, filepath.Join(r, "jobs"), filepath.Join(r, "nomad", "jobs"))
	}
	walkNomadJobFiles(dirs, func(dir, jobPath, jobName, content string) bool {
		for _, img := range ExtractNomadJobImages(content) {
			for _, affected := range affectedImages {
				if ImageMatches(img, affected) {
					add(jobName)
					return true
				}
			}
		}
		return true
	})
}

// ResolveExpectedDeployJobs computes the deduplicated, deterministically ordered list of Nomad jobs
// expected to deploy for a git push across all 4 triggers (.nomad specs, TriggerJob asset bundles,
// aerial-config Nomad variable mappings, and container build changes).
func ResolveExpectedDeployJobs(
	repoPath string,
	changedFiles []string,
	nomadChanges []NomadFileChange,
	serviceConfigs []ServiceConfigMapping,
	candidateRepos []string,
	assetBundles ...[]AssetBundleConfig,
) []string {
	cleanRepo := filepath.ToSlash(strings.ToLower(filepath.Clean(repoPath)))

	var bundles []AssetBundleConfig
	if len(assetBundles) > 0 {
		bundles = assetBundles[0]
	}

	var configDir string
	for _, b := range bundles {
		if b.Name == "docs" {
			if contentDir, ok := b.Sources["content"]; ok {
				configDir = filepath.Dir(filepath.Clean(contentDir))
			}
		}
	}
	isAerialConfig := strings.Contains(cleanRepo, "aerial-config") || (configDir != "" && filepath.Clean(repoPath) == filepath.Clean(configDir))

	seen := make(map[string]struct{})
	var jobs []string
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			jobs = append(jobs, name)
		}
	}

	// 1. Non-deleted .nomad specification changes
	for _, ch := range nomadChanges {
		if ch.Action != "delete" {
			add(ch.JobName)
		}
	}

	// 2. TriggerJob asset bundles (e.g. migrations -> db-migrate)
	for _, bundle := range bundles {
		if bundle.TriggerJob != "" && AssetBundleMatchesRepoChanges(bundle, repoPath, changedFiles) {
			jobName := strings.TrimSuffix(filepath.Base(bundle.TriggerJob), ".nomad")
			add(jobName)
		}
	}

	// 3. aerial-config Nomad variable mappings
	if isAerialConfig {
		resolveAerialConfigExpectedJobs(changedFiles, serviceConfigs, add)
	}

	// 4. Container build changes across repositories (aerial, aerial-sidecars, mirrormere)
	resolveContainerWorkflowExpectedJobs(repoPath, changedFiles, candidateRepos, add)

	if len(jobs) == 0 {
		return nil
	}

	sort.Slice(jobs, func(i, j int) bool {
		if IsHangarJob(jobs[i]) != IsHangarJob(jobs[j]) {
			return !IsHangarJob(jobs[i])
		}
		return jobs[i] < jobs[j]
	})

	return jobs
}

// IsValidGitCommitSHA returns true if s is 7 to 40 characters and contains only hexadecimal characters.
func IsValidGitCommitSHA(s string) bool {
	if len(s) < 7 || len(s) > 40 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// ExtractRevisionFromAnnotations extracts and validates a git commit SHA from image or manifest annotations.
// It checks standard and aerial-specific keys: org.opencontainers.image.revision, aerial.commit_sha, org.label-schema.vcs-ref.
func ExtractRevisionFromAnnotations(annotations map[string]string) string {
	if len(annotations) == 0 {
		return ""
	}
	keys := []string{
		"org.opencontainers.image.revision",
		"aerial.commit_sha",
		"org.label-schema.vcs-ref",
	}
	for _, k := range keys {
		if val, ok := annotations[k]; ok {
			trimmed := strings.TrimSpace(val)
			if IsValidGitCommitSHA(trimmed) {
				return trimmed
			}
		}
	}
	return ""
}

// ExtractRevisionFromConfigJSON extracts and validates a git commit SHA from an OCI/Docker container image configuration blob JSON.
// Parses JSON config labels and returns the first valid commit SHA.
func ExtractRevisionFromConfigJSON(configJSON []byte) string {
	if len(configJSON) == 0 {
		return ""
	}
	var doc struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"config"`
		Labels map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(configJSON, &doc); err != nil {
		return ""
	}
	if rev := ExtractRevisionFromAnnotations(doc.Config.Labels); rev != "" {
		return rev
	}
	return ExtractRevisionFromAnnotations(doc.Labels)
}

// ExtractPlatformManifestDigest parses an OCI Image Index or Docker Manifest List and returns
// the digest of the manifest matching targetOS and targetArch. If none matches, returns first entry's Digest.
func ExtractPlatformManifestDigest(indexJSON []byte, targetOS, targetArch string) string {
	if len(indexJSON) == 0 {
		return ""
	}
	var doc struct {
		Manifests []struct {
			Digest   string `json:"digest"`
			Platform struct {
				Architecture string `json:"architecture"`
				OS           string `json:"os"`
			} `json:"platform"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(indexJSON, &doc); err != nil || len(doc.Manifests) == 0 {
		return ""
	}

	targetOS = strings.TrimSpace(strings.ToLower(targetOS))
	targetArch = strings.TrimSpace(strings.ToLower(targetArch))

	if targetOS != "" || targetArch != "" {
		for _, m := range doc.Manifests {
			matchOS := targetOS == "" || strings.EqualFold(m.Platform.OS, targetOS)
			matchArch := targetArch == "" || strings.EqualFold(m.Platform.Architecture, targetArch)
			if matchOS && matchArch && strings.TrimSpace(m.Digest) != "" {
				return strings.TrimSpace(m.Digest)
			}
		}
	}

	return strings.TrimSpace(doc.Manifests[0].Digest)
}

// ExtractConfigBlobDigest parses OCI/Docker Manifest JSON and returns the configuration blob digest.
func ExtractConfigBlobDigest(manifestJSON []byte) string {
	if len(manifestJSON) == 0 {
		return ""
	}
	var doc struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Annotations map[string]string `json:"annotations"`
	}
	if err := json.Unmarshal(manifestJSON, &doc); err != nil {
		return ""
	}
	return strings.TrimSpace(doc.Config.Digest)
}

// ExtractManifestAnnotations parses an OCI/Docker manifest or index JSON and extracts its annotations map.
func ExtractManifestAnnotations(manifestJSON []byte) map[string]string {
	if len(manifestJSON) == 0 {
		return nil
	}
	var doc struct {
		Annotations map[string]string `json:"annotations"`
	}
	if err := json.Unmarshal(manifestJSON, &doc); err != nil {
		return nil
	}
	return doc.Annotations
}
