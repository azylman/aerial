package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

// buildMetadataActionsAppID is GitHub's official Actions app, verified against
// each producer run's authoritative check-suite API. Transport checks created by
// any other app are never treated as producer output.
const buildMetadataActionsAppID int64 = 15368

const (
	prConflict        = "conflict"
	prSynced          = "synced"
	defaultMainBranch = "main"

	maxMetadataListPages  = 20
	metadataListPageSize  = 100
	metadataExternalParts = 6
)

var (
	metadataDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

	// errStaleWorkflowEvent marks a webhook for an attempt GitHub has since superseded.
	errStaleWorkflowEvent = errors.New("stale workflow run attempt")
	// errNotImageWorkflow marks a workflow run that is not its repository's image producer.
	errNotImageWorkflow = errors.New("workflow run is not the image producer")
	// errBuildMetadataMissing marks a completed producer run without metadata or an inventoried legacy blob.
	errBuildMetadataMissing = errors.New("required build metadata missing")
)

// metadataWorkflowRun is the authoritative workflow run API identity.
type metadataWorkflowRun struct {
	ID         int64  `json:"id"`
	RunNumber  int64  `json:"run_number"`
	RunAttempt int    `json:"run_attempt"`
	Path       string `json:"path"`
	Event      string `json:"event"`
	HeadSHA    string `json:"head_sha"`
	HeadBranch string `json:"head_branch"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

// workflowRunEvent is the exact run identity a workflow_run webhook delivered.
type workflowRunEvent struct {
	ID      int64
	Attempt int
	HeadSHA string
}

// metadataSource classifies how a run's service selection is known.
type metadataSource int

const (
	metadataPending    metadataSource = iota // running producer that has not published yet
	metadataStructured                       // validated structured metadata
	metadataLegacy                           // inventoried historical workflow blob
)

// metadataWorkflowPath returns the image producer workflow for a repository,
// or "" when the repository publishes no images.
func metadataWorkflowPath(repo string) string {
	switch repo[strings.LastIndex(repo, "/")+1:] {
	case "mirrormere":
		return ".github/workflows/ci.yml"
	case "aerial", "aerial-sidecars":
		return ".github/workflows/docker-publish.yml"
	default:
		return ""
	}
}

func validateMetadataRunIdentity(r metadataWorkflowRun, repo string, id int64) error {
	switch {
	case r.ID != id || r.Repository.FullName != repo:
		return fmt.Errorf("workflow run identity mismatch")
	case r.Path != metadataWorkflowPath(repo):
		return fmt.Errorf("unexpected producer workflow %q", r.Path)
	case r.RunAttempt < 1 || r.RunNumber < 1 || !metadataSHA.MatchString(r.HeadSHA):
		return fmt.Errorf("invalid workflow run identity")
	case r.Event == ghEventPullRequest:
		return nil
	case isMainBuildEvent(r.Event) && r.HeadBranch == defaultMainBranch:
		return nil
	default:
		return fmt.Errorf("unexpected producer event %q on %q", r.Event, r.HeadBranch)
	}
}

func (s *RouterServer) fetchMetadataRun(ctx context.Context, repo string, id int64) (metadataWorkflowRun, error) {
	var r metadataWorkflowRun
	if id <= 0 || metadataWorkflowPath(repo) == "" {
		return r, fmt.Errorf("unsupported producer run")
	}
	err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d", repo, id), 2<<20, &r)
	return r, err
}

func metadataExternalID(m BuildMetadata) string {
	suffix := m.Kind
	if m.Kind == buildMetadataResult {
		suffix = m.Service
	}
	return fmt.Sprintf("build-metadata:v1:%s:%d:%d:%s", m.Repository, m.RunID, m.RunAttempt, suffix)
}

// parseMetadataExternalRun extracts the repository and run an external ID claims.
func parseMetadataExternalRun(externalID string) (string, int64, bool) {
	parts := strings.Split(externalID, ":")
	if len(parts) != metadataExternalParts || parts[0] != buildMetadataCheckName || parts[1] != "v1" || !metadataRepo.MatchString(parts[2]) {
		return "", 0, false
	}
	runID, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || runID <= 0 {
		return "", 0, false
	}
	if attempt, attemptErr := strconv.Atoi(parts[4]); attemptErr != nil || attempt <= 0 || parts[5] == "" {
		return "", 0, false
	}
	return parts[2], runID, true
}

// metadataRecords holds one run's provenance-validated, replica-deduplicated records.
type metadataRecords struct {
	identity  *BuildMetadata
	snapshots map[int]BuildMetadata // manifest or final per attempt (final wins)
	results   map[string]BuildMetadata
}

// newestResult returns the latest result record for a service at or before attempt.
func (rs *metadataRecords) newestResult(service string, attempt int) (BuildMetadata, bool) {
	var newest BuildMetadata
	found := false
	for _, r := range rs.results {
		if r.Service == service && r.RunAttempt <= attempt && (!found || r.RunAttempt > newest.RunAttempt) {
			newest, found = r, true
		}
	}
	return newest, found
}

// collectMetadataRecords validates every replica's provenance before exact-raw
// deduplication. Custom check suites need not equal the Actions suite: the app,
// external ID, source SHA and JSON run identity bind each record instead.
func collectMetadataRecords(checks []GitHubMetadataCheck, repo string, r metadataWorkflowRun) (metadataRecords, error) {
	rs := metadataRecords{snapshots: map[int]BuildMetadata{}, results: map[string]BuildMetadata{}}
	raws := map[string]string{}
	for _, c := range checks {
		if !isBuildMetadataCheck(c.Name) || c.App.ID != buildMetadataActionsAppID {
			continue
		}
		claimedRepo, runID, ok := parseMetadataExternalRun(c.ExternalID)
		if !ok || claimedRepo != repo {
			return rs, fmt.Errorf("unattributable build metadata check %d", c.ID)
		}
		if runID != r.ID {
			continue
		}
		m, err := ParseBuildMetadata([]byte(c.Output.Text))
		if err != nil {
			return rs, fmt.Errorf("malformed build metadata check %d: %w", c.ID, err)
		}
		if err = validateMetadataProvenance(c, m, repo, r); err != nil {
			return rs, fmt.Errorf("build metadata check %d: %w", c.ID, err)
		}
		if raw, seen := raws[c.ExternalID]; seen {
			if raw != c.Output.Text {
				return rs, fmt.Errorf("conflicting build metadata replicas for %s", c.ExternalID)
			}
			continue
		}
		raws[c.ExternalID] = c.Output.Text
		if err = rs.add(m); err != nil {
			return rs, err
		}
	}
	return rs, nil
}

func (rs *metadataRecords) add(m BuildMetadata) error {
	if rs.identity == nil {
		identity := m
		rs.identity = &identity
	} else if !sameMetadataIdentity(*rs.identity, m) {
		return fmt.Errorf("conflicting immutable build metadata")
	}
	switch m.Kind {
	case buildMetadataResult:
		rs.results[metadataExternalID(m)] = m
	case buildMetadataFinal:
		rs.snapshots[m.RunAttempt] = m
	default:
		if prior, ok := rs.snapshots[m.RunAttempt]; !ok || prior.Kind != buildMetadataFinal {
			rs.snapshots[m.RunAttempt] = m
		}
	}
	return nil
}

func sameMetadataIdentity(a, b BuildMetadata) bool {
	return a.Event == b.Event && a.SourceSHA == b.SourceSHA && a.ExecutionSHA == b.ExecutionSHA &&
		reflect.DeepEqual(a.PRNumber, b.PRNumber) && reflect.DeepEqual(a.Selected, b.Selected)
}

func validateMetadataProvenance(c GitHubMetadataCheck, m BuildMetadata, repo string, r metadataWorkflowRun) error {
	if err := validateMetadataTransport(c, m, r); err != nil {
		return err
	}
	if err := validateMetadataRevisions(m, repo, r); err != nil {
		return err
	}
	for _, sel := range m.Selected {
		_, image, workload := resolveImageTag(repo, sel.Service)
		if workload != sel.Service || image != sel.ImageName {
			return fmt.Errorf("service %q does not map to image %q", sel.Service, sel.ImageName)
		}
	}
	return validateMetadataImages(m)
}

// validateMetadataTransport binds the check itself: fixed name, exact external
// ID, a completed-neutral record on the run's source commit.
func validateMetadataTransport(c GitHubMetadataCheck, m BuildMetadata, r metadataWorkflowRun) error {
	name := buildMetadataCheckName
	if m.Kind == buildMetadataResult {
		name = buildMetadataResultCheckName
	}
	switch {
	case c.ID <= 0 || c.Name != name || c.ExternalID != metadataExternalID(m):
		return fmt.Errorf("transport identity mismatch")
	case c.Status != ghStatusCompleted || c.Conclusion != "neutral":
		return fmt.Errorf("transport check is not a completed neutral record")
	case c.HeadSHA != r.HeadSHA:
		return fmt.Errorf("source revision mismatch")
	}
	return nil
}

// validateMetadataRevisions binds the JSON identity to the authoritative run.
func validateMetadataRevisions(m BuildMetadata, repo string, r metadataWorkflowRun) error {
	switch {
	case m.SourceSHA != r.HeadSHA:
		return fmt.Errorf("source revision mismatch")
	case m.Repository != repo || m.RunID != r.ID || m.RunNumber != r.RunNumber || m.RunAttempt > r.RunAttempt || m.WorkflowPath != r.Path || m.Event != r.Event:
		return fmt.Errorf("workflow run identity mismatch")
	case r.Event == ghEventPullRequest && (m.PRNumber == nil || m.ExecutionSHA == m.SourceSHA):
		return fmt.Errorf("invalid pull request identity")
	case r.Event != ghEventPullRequest && (m.PRNumber != nil || m.ExecutionSHA != m.SourceSHA):
		return fmt.Errorf("invalid branch build identity")
	}
	return nil
}

// validateMetadataImages checks result subsets, owner-scoped image references and digests.
func validateMetadataImages(m BuildMetadata) error {
	results := m.Results
	if m.Kind == buildMetadataResult {
		if !slicesContainsSelection(m.Selected, m.Service, m.ImageName) || m.SourceAttempt != m.RunAttempt {
			return fmt.Errorf("result outside selection or attempt")
		}
		results = []BuildResult{{Service: m.Service, ImageName: m.ImageName, State: m.State, Pushed: m.Pushed, ImageRefs: m.ImageRefs, Digest: m.Digest}}
	}
	owner := metadataOwner(m.Repository)
	for _, result := range results {
		if result.Pushed && len(result.ImageRefs) == 0 {
			return fmt.Errorf("pushed result %q has no image reference", result.Service)
		}
		prefix := "ghcr.io/" + owner + "/" + result.ImageName + ":"
		for _, ref := range result.ImageRefs {
			tag, ok := strings.CutPrefix(ref, prefix)
			if !ok || !metadataSHA.MatchString(strings.TrimPrefix(tag, "tree-")) {
				return fmt.Errorf("invalid image reference %q", ref)
			}
		}
		if result.Digest != "" && !metadataDigest.MatchString(result.Digest) {
			return fmt.Errorf("invalid image digest for %q", result.Service)
		}
	}
	return nil
}

// metadataOwner is the lowercase registry namespace of an owner/name repository.
func metadataOwner(repo string) string {
	owner, _, _ := strings.Cut(repo, "/")
	return strings.ToLower(owner)
}

func slicesContainsSelection(selected []BuildSelection, service, image string) bool {
	for _, sel := range selected {
		if sel.Service == service && sel.ImageName == image {
			return true
		}
	}
	return false
}

// currentMetadataSnapshot projects records onto the run's current attempt. A
// final for that attempt must agree with the newest result record of every
// service. Without one, only current-attempt results are shown so inherited or
// superseded successes never read as current progress.
func currentMetadataSnapshot(rs metadataRecords, attempt int) (BuildMetadata, bool, error) {
	if rs.identity == nil {
		return BuildMetadata{}, false, nil
	}
	if final, ok := rs.snapshots[attempt]; ok && final.Kind == buildMetadataFinal {
		for _, result := range final.Results {
			newest, found := rs.newestResult(result.Service, attempt)
			if !found || newest.RunAttempt != result.SourceAttempt || newest.State != result.State || newest.Pushed != result.Pushed ||
				!reflect.DeepEqual(newest.ImageRefs, result.ImageRefs) || newest.Digest != result.Digest {
				return final, false, fmt.Errorf("final result for %q disagrees with its transport record", result.Service)
			}
		}
		return final, true, nil
	}
	snapshot := *rs.identity
	snapshot.Kind = buildMetadataManifest
	snapshot.RunAttempt = attempt
	snapshot.Results = nil
	snapshot.Service, snapshot.ImageName, snapshot.State, snapshot.Pushed = "", "", "", false
	snapshot.SourceAttempt, snapshot.ImageRefs, snapshot.Digest = 0, nil, ""
	for _, sel := range snapshot.Selected {
		if r, ok := rs.results[metadataExternalID(BuildMetadata{Kind: buildMetadataResult, Repository: snapshot.Repository, RunID: snapshot.RunID, RunAttempt: attempt, Service: sel.Service})]; ok {
			snapshot.Kind = buildMetadataProgress
			snapshot.Results = append(snapshot.Results, BuildResult{Service: r.Service, ImageName: r.ImageName, State: r.State, Pushed: r.Pushed, SourceAttempt: attempt, ImageRefs: r.ImageRefs, Digest: r.Digest})
		}
	}
	return snapshot, true, nil
}

func (s *RouterServer) listCommitCheckRuns(ctx context.Context, repo, sha string) ([]GitHubMetadataCheck, error) {
	var all []GitHubMetadataCheck
	for page := 1; page <= maxMetadataListPages; page++ {
		var batch GitHubCheckRunsResponse
		path := fmt.Sprintf("/repos/%s/commits/%s/check-runs?filter=all&per_page=%d&page=%d", repo, sha, metadataListPageSize, page)
		if err := s.getGitHubJSON(ctx, path, 8<<20, &batch); err != nil {
			return nil, err
		}
		all = append(all, batch.CheckRuns...)
		if len(batch.CheckRuns) < metadataListPageSize {
			return all, nil
		}
	}
	return nil, fmt.Errorf("check run listing exceeds %d pages", maxMetadataListPages)
}

// resolveRunMetadata returns the run's current-attempt snapshot. A run without
// structured records is legacy only when its exact executed workflow blob is in
// the immutable allowlist; a completed run otherwise lacks required metadata.
func (s *RouterServer) resolveRunMetadata(ctx context.Context, repo string, r metadataWorkflowRun) (BuildMetadata, metadataSource, error) {
	checks, err := s.listCommitCheckRuns(ctx, repo, r.HeadSHA)
	if err != nil {
		return BuildMetadata{}, metadataPending, err
	}
	records, err := collectMetadataRecords(checks, repo, r)
	if err != nil {
		return BuildMetadata{}, metadataPending, err
	}
	m, found, err := currentMetadataSnapshot(records, r.RunAttempt)
	if err != nil {
		return m, metadataPending, err
	}
	if found {
		if err = s.validateMetadataExecution(ctx, repo, r, m.ExecutionSHA); err != nil {
			return m, metadataPending, err
		}
		return m, metadataStructured, nil
	}
	if r.Status != ghStatusCompleted {
		return m, metadataPending, nil
	}
	legacy, err := s.isLegacyWorkflowRun(ctx, repo, r)
	if err != nil {
		return m, metadataPending, err
	}
	if !legacy {
		return m, metadataPending, errBuildMetadataMissing
	}
	return m, metadataLegacy, nil
}

func (s *RouterServer) isLegacyWorkflowRun(ctx context.Context, repo string, r metadataWorkflowRun) (bool, error) {
	revision, err := s.legacyWorkflowRevision(ctx, repo, r)
	if err != nil {
		return false, err
	}
	var blob struct {
		SHA string `json:"sha"`
	}
	if err = s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/contents/%s?ref=%s", repo, r.Path, url.QueryEscape(revision)), 2<<20, &blob); err != nil {
		return false, err
	}
	if !metadataSHA.MatchString(blob.SHA) {
		return false, fmt.Errorf("invalid workflow blob for %s", r.Path)
	}
	for _, legacy := range s.cfgSrc.Current().LegacyBuildWorkflows {
		if legacy.Repository == repo && legacy.WorkflowPath == r.Path && legacy.BlobSHA == blob.SHA {
			return true, nil
		}
	}
	return false, nil
}

// validateMetadataExecution binds the execution revision: a branch build runs
// its source commit; a PR build runs GitHub's merge of the source head.
func (s *RouterServer) validateMetadataExecution(ctx context.Context, repo string, r metadataWorkflowRun, executionSHA string) error {
	if r.Event != ghEventPullRequest {
		if executionSHA != r.HeadSHA {
			return fmt.Errorf("branch execution revision mismatch")
		}
		return nil
	}
	if executionSHA == r.HeadSHA || !metadataSHA.MatchString(executionSHA) {
		return fmt.Errorf("PR execution is not a merge revision")
	}
	var commit struct {
		SHA     string `json:"sha"`
		Parents []struct {
			SHA string `json:"sha"`
		} `json:"parents"`
	}
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s", repo, executionSHA), 2<<20, &commit); err != nil {
		return err
	}
	if commit.SHA != executionSHA || len(commit.Parents) != 2 || commit.Parents[1].SHA != r.HeadSHA {
		return fmt.Errorf("execution revision is not bound to the PR head")
	}
	return nil
}

// metadataPublishable reports whether a snapshot authorizes promotion: the
// current attempt's final aggregate, every selected image pushed, and the whole
// workflow (including any separate promotion job) successful.
func metadataPublishable(m BuildMetadata, r metadataWorkflowRun) bool {
	if m.Kind != buildMetadataFinal || m.RunAttempt != r.RunAttempt || r.Status != ghStatusCompleted || r.Conclusion != conclusionSuccess || len(m.Results) != len(m.Selected) {
		return false
	}
	for _, result := range m.Results {
		if result.State != conclusionSuccess || !result.Pushed {
			return false
		}
	}
	return true
}

// resolveWorkflowRunImages returns the images a successful main-branch producer
// run published, bound to the exact webhook attempt.
func (s *RouterServer) resolveWorkflowRunImages(ctx context.Context, repo string, ev workflowRunEvent, prNum int) ([]string, error) {
	repo = normalizeRepo(repo)
	if metadataWorkflowPath(repo) == "" {
		return nil, nil
	}
	r, err := s.fetchMetadataRun(ctx, repo, ev.ID)
	if err != nil {
		return nil, err
	}
	if r.Path != metadataWorkflowPath(repo) {
		return nil, errNotImageWorkflow
	}
	if err = validateMetadataRunIdentity(r, repo, ev.ID); err != nil {
		return nil, err
	}
	switch {
	case r.Event == ghEventPullRequest:
		return nil, errNotImageWorkflow
	case ev.Attempt < 1:
		return nil, fmt.Errorf("workflow run webhook lacks its attempt")
	case r.RunAttempt > ev.Attempt:
		return nil, errStaleWorkflowEvent
	case r.RunAttempt != ev.Attempt || r.HeadSHA != ev.HeadSHA:
		return nil, fmt.Errorf("workflow run API disagrees with webhook (attempt %d/%d)", r.RunAttempt, ev.Attempt)
	case r.Status != ghStatusCompleted || r.Conclusion != conclusionSuccess:
		return nil, fmt.Errorf("workflow run has not succeeded (%s/%s)", r.Status, r.Conclusion)
	}
	m, source, err := s.resolveRunMetadata(ctx, repo, r)
	if err != nil {
		return nil, err
	}
	if source == metadataLegacy {
		return s.resolveLegacyWorkflowRunImages(ctx, repo, r.ID)
	}
	if !metadataPublishable(m, r) {
		return nil, fmt.Errorf("build metadata for run %d attempt %d is not publishable", r.ID, r.RunAttempt)
	}
	s.recordPRBuildSnapshot(ctx, repo, prNum, m)
	owner := metadataOwner(repo)
	images := make([]string, 0, len(m.Selected))
	for _, sel := range m.Selected {
		images = append(images, fmt.Sprintf("ghcr.io/%s/%s:latest", owner, sel.Service))
	}
	return images, nil
}

func (s *RouterServer) recordPRBuildSnapshot(ctx context.Context, repo string, prNum int, m BuildMetadata) {
	if s.registry == nil || prNum <= 0 {
		return
	}
	if _, err := s.registry.RecordPRBuildMetadata(ctx, repo, prNum, m); err != nil {
		logBuildMetadataWarning(ctx, "recording build metadata snapshot", repo, prNum, err)
	}
}

// newestMetadataRun selects the newest eligible producer run for a revision,
// ordered by run number then attempt, so an older success can never hide a
// newer pending or failed run.
func (s *RouterServer) newestMetadataRun(ctx context.Context, repo, sha string, pullRequest bool) (metadataWorkflowRun, bool, error) {
	var newest metadataWorkflowRun
	for page := 1; page <= maxMetadataListPages; page++ {
		var runs struct {
			Runs []metadataWorkflowRun `json:"workflow_runs"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs?head_sha=%s&per_page=%d&page=%d", repo, url.QueryEscape(sha), metadataListPageSize, page)
		if err := s.getGitHubJSON(ctx, path, 4<<20, &runs); err != nil {
			return newest, false, err
		}
		for _, r := range runs.Runs {
			eligible := r.HeadSHA == sha && r.Path == metadataWorkflowPath(repo) &&
				((pullRequest && r.Event == ghEventPullRequest) || (!pullRequest && isMainBuildEvent(r.Event) && r.HeadBranch == defaultMainBranch))
			if eligible && (r.RunNumber > newest.RunNumber || (r.RunNumber == newest.RunNumber && r.RunAttempt > newest.RunAttempt)) {
				newest = r
			}
		}
		if len(runs.Runs) < metadataListPageSize {
			return newest, newest.ID != 0, nil
		}
	}
	return newest, false, fmt.Errorf("workflow run listing exceeds %d pages", maxMetadataListPages)
}

// prBuildRevision decides which producer revision a PR's cards follow: the
// current head while open, or the merge commit once merged.
func prBuildRevision(lifecycle PRBuildLifecycle, sha string) (pullRequest, ok bool) {
	switch lifecycle.Status {
	case prOpen, prCIFailed, prConflict:
		return true, lifecycle.HeadSHA != "" && lifecycle.HeadSHA == sha
	case prMerged, prSynced:
		return false, lifecycle.MergeSHA != "" && lifecycle.MergeSHA == sha
	default:
		return false, false
	}
}

// refreshPRBuildMetadata re-resolves the newest eligible producer run for the
// PR's current revision and atomically records its snapshot. Stale revisions,
// runs that have not published yet, and inventoried legacy runs record nothing.
func (s *RouterServer) refreshPRBuildMetadata(ctx context.Context, repo string, pr int, sha string) (bool, error) {
	repo = normalizeRepo(repo)
	if s.registry == nil || pr <= 0 || metadataWorkflowPath(repo) == "" || !metadataSHA.MatchString(sha) {
		return false, nil
	}
	lifecycle, err := s.registry.LoadPRBuildLifecycle(ctx, repo, pr)
	if err != nil {
		return false, err
	}
	pullRequest, ok := prBuildRevision(lifecycle, sha)
	if !ok {
		return false, nil
	}
	newest, found, err := s.newestMetadataRun(ctx, repo, sha, pullRequest)
	if err != nil || !found {
		return false, err
	}
	r, err := s.fetchMetadataRun(ctx, repo, newest.ID)
	if err != nil {
		return false, err
	}
	if err = validateMetadataRunIdentity(r, repo, newest.ID); err != nil {
		return false, err
	}
	m, source, err := s.resolveRunMetadata(ctx, repo, r)
	if err != nil || source != metadataStructured {
		return false, err
	}
	if pullRequest && (m.PRNumber == nil || *m.PRNumber != pr) {
		return false, fmt.Errorf("build metadata PR identity mismatch")
	}
	return s.registry.RecordPRBuildMetadata(ctx, repo, pr, m)
}

// supersedeStaleBuildAttempt demotes a stored snapshot once a newer attempt of
// the same run starts, without needing the GitHub API, so a prior attempt's
// success is never shown as current.
func (s *RouterServer) supersedeStaleBuildAttempt(ctx context.Context, repo string, pr int, ev workflowRunEvent) {
	if s.registry == nil || pr <= 0 || ev.ID <= 0 {
		return
	}
	m, found, err := s.registry.LoadPRBuildMetadata(ctx, repo, pr)
	if err != nil || !found || m.RunID != ev.ID || m.RunAttempt >= ev.Attempt || m.SourceSHA != ev.HeadSHA {
		return
	}
	pending := BuildMetadata{Version: m.Version, Kind: buildMetadataManifest, Repository: m.Repository, WorkflowPath: m.WorkflowPath, RunID: m.RunID, RunNumber: m.RunNumber, RunAttempt: ev.Attempt, Event: m.Event, SourceSHA: m.SourceSHA, ExecutionSHA: m.ExecutionSHA, PRNumber: m.PRNumber, Selected: m.Selected}
	s.recordPRBuildSnapshot(ctx, repo, pr, pending)
}

func (s *RouterServer) handleWorkflowRunBuildMetadata(ctx context.Context, p GitHubWorkflowRunPayload, res *ResolvedGitHubEvent) {
	repo := normalizeRepo(res.Repo)
	if s.registry == nil || res.PRNumber <= 0 || metadataWorkflowPath(repo) == "" || (p.WorkflowRun.Path != "" && p.WorkflowRun.Path != metadataWorkflowPath(repo)) || !s.ownsWorkflowRunEvent(ctx, repo, res.PRNumber) {
		return
	}
	s.supersedeStaleBuildAttempt(ctx, repo, res.PRNumber, workflowRunEvent{ID: p.WorkflowRun.ID, Attempt: p.WorkflowRun.RunAttempt, HeadSHA: p.WorkflowRun.HeadSHA})
	if _, err := s.refreshPRBuildMetadata(ctx, repo, res.PRNumber, p.WorkflowRun.HeadSHA); err != nil {
		logBuildMetadataWarning(ctx, "refreshing build metadata on workflow run", repo, res.PRNumber, err)
	}
}

// handleBuildMetadataCheck ingests a transport check from authoritative APIs,
// independently of Dependabot CI filtering, preserving owner routing.
func (s *RouterServer) handleBuildMetadataCheck(ctx context.Context, p GitHubCheckRunPayload, res *ResolvedGitHubEvent) error {
	if s.registry == nil || metadataWorkflowPath(normalizeRepo(res.Repo)) == "" {
		return nil
	}
	s.resolveEventPR(ctx, res, p.CheckRun.PullRequests, "build_metadata")
	if res.PRNumber <= 0 || !s.ownsWorkflowRunEvent(ctx, res.Repo, res.PRNumber) {
		return nil
	}
	updated, err := s.refreshPRBuildMetadata(ctx, res.Repo, res.PRNumber, res.HeadSHA)
	if err != nil {
		return fmt.Errorf("refresh build metadata: %w", err)
	}
	if updated {
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        "check_updated",
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			TargetID:     res.TargetID,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			Quiet:        isDependabotBranch(res.Branch),
			OccurredAt:   time.Now().UTC(),
		})
	}
	return nil
}

func logBuildMetadataWarning(ctx context.Context, msg, repo string, pr int, err error) {
	slog.WarnContext(ctx, "[synapse] [build_metadata] "+msg, "repo", repo, "pr", pr, "error", err)
}
