package buildmeta

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

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// BuildMetadataActionsAppID is GitHub's official Actions app, verified against
// each producer run's authoritative check-suite API. Transport checks created by
// any other app are never treated as producer output.
const BuildMetadataActionsAppID int64 = 15368

const (
	MaxMetadataListPages  = 20
	MetadataListPageSize  = 100
	MetadataExternalParts = 6
	DefaultMainBranch     = "main"
	statusCompleted       = "completed"
)

var (
	MetadataDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

	ErrStaleWorkflowEvent   = errors.New("stale workflow run attempt")
	ErrNotImageWorkflow     = errors.New("workflow run is not the image producer")
	ErrBuildMetadataMissing = errors.New("required build metadata missing")
)

// GitHubGetter specifies the outbound operations required to inspect workflow
// runs and checks on GitHub.
type GitHubGetter interface {
	GetGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error
}

// MetadataWorkflowRun is the authoritative workflow run API identity.
type MetadataWorkflowRun struct {
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

// WorkflowRunEvent is the exact run identity a workflow_run webhook delivered.
type WorkflowRunEvent struct {
	ID      int64  `json:"id"`
	Attempt int    `json:"attempt"`
	HeadSHA string `json:"head_sha"`
}

// MetadataSource classifies how a run's service selection is known.
type MetadataSource int

const (
	MetadataPending    MetadataSource = iota // running producer that has not published yet
	MetadataStructured                       // validated structured metadata
)

// MetadataWorkflowPath returns the image producer workflow for a repository,
// or "" when the repository publishes no images.
func MetadataWorkflowPath(repo string) string {
	switch repo[strings.LastIndex(repo, "/")+1:] {
	case "mirrormere":
		return ".github/workflows/ci.yml"
	case "aerial", "aerial-sidecars":
		return ".github/workflows/docker-publish.yml"
	default:
		return ""
	}
}

func ValidateMetadataRunIdentity(r MetadataWorkflowRun, repo string, id int64) error {
	switch {
	case r.ID != id || r.Repository.FullName != repo:
		return fmt.Errorf("workflow run identity mismatch")
	case r.Path != MetadataWorkflowPath(repo):
		return fmt.Errorf("unexpected producer workflow %q", r.Path)
	case r.RunAttempt < 1 || r.RunNumber < 1 || !model.MetadataSHA.MatchString(r.HeadSHA):
		return fmt.Errorf("invalid workflow run identity")
	case r.Event == model.GHEventPullRequest:
		return nil
	case model.IsMainBuildEvent(r.Event) && r.HeadBranch == DefaultMainBranch:
		return nil
	default:
		return fmt.Errorf("unexpected producer event %q on %q", r.Event, r.HeadBranch)
	}
}

func MetadataExternalID(m prwire.BuildMetadata) string {
	suffix := m.Kind
	if m.Kind == prwire.BuildMetadataResult {
		suffix = m.Service
	}
	return fmt.Sprintf("build-metadata:v1:%s:%d:%d:%s", m.Repository, m.RunID, m.RunAttempt, suffix)
}

// ParseMetadataExternalRun extracts the repository and run an external ID claims.
func ParseMetadataExternalRun(externalID string) (string, int64, bool) {
	parts := strings.Split(externalID, ":")
	if len(parts) != MetadataExternalParts || parts[0] != model.BuildMetadataCheckName || parts[1] != "v1" || !model.MetadataRepo.MatchString(parts[2]) {
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

// MetadataRecords holds one run's provenance-validated, replica-deduplicated records.
type MetadataRecords struct {
	Identity  *prwire.BuildMetadata
	Snapshots map[int]prwire.BuildMetadata // manifest or final per attempt (final wins)
	Results   map[string]prwire.BuildMetadata
}

func (rs *MetadataRecords) NewestResult(service string, attempt int) (prwire.BuildMetadata, bool) {
	var newest prwire.BuildMetadata
	found := false
	for _, r := range rs.Results {
		if r.Service == service && r.RunAttempt <= attempt && (!found || r.RunAttempt > newest.RunAttempt) {
			newest, found = r, true
		}
	}
	return newest, found
}

func (rs *MetadataRecords) Add(m prwire.BuildMetadata) error {
	if rs.Identity == nil {
		identity := m
		rs.Identity = &identity
	} else if !SameMetadataIdentity(*rs.Identity, m) {
		return fmt.Errorf("conflicting immutable build metadata")
	}
	switch m.Kind {
	case prwire.BuildMetadataResult:
		rs.Results[MetadataExternalID(m)] = m
	case prwire.BuildMetadataFinal:
		rs.Snapshots[m.RunAttempt] = m
	default:
		if prior, ok := rs.Snapshots[m.RunAttempt]; !ok || prior.Kind != prwire.BuildMetadataFinal {
			rs.Snapshots[m.RunAttempt] = m
		}
	}
	return nil
}

func SameMetadataIdentity(a, b prwire.BuildMetadata) bool {
	return a.Event == b.Event && a.SourceSHA == b.SourceSHA && a.ExecutionSHA == b.ExecutionSHA &&
		reflect.DeepEqual(a.PRNumber, b.PRNumber) && reflect.DeepEqual(a.Selected, b.Selected)
}

// CollectMetadataRecords validates every replica's provenance before exact-raw
// deduplication. Custom check suites need not equal the Actions suite: the app,
// external ID, source SHA and JSON run identity bind each record instead.
func CollectMetadataRecords(checks []model.GitHubMetadataCheck, repo string, r MetadataWorkflowRun) (MetadataRecords, error) {
	rs := MetadataRecords{Snapshots: map[int]prwire.BuildMetadata{}, Results: map[string]prwire.BuildMetadata{}}
	raws := map[string]string{}
	for _, c := range checks {
		if !model.IsBuildMetadataCheck(c.Name) || c.App.ID != BuildMetadataActionsAppID {
			continue
		}
		claimedRepo, runID, ok := ParseMetadataExternalRun(c.ExternalID)
		if !ok || claimedRepo != repo {
			return rs, fmt.Errorf("unattributable build metadata check %d", c.ID)
		}
		if runID != r.ID {
			continue
		}
		m, err := model.ParseBuildMetadata([]byte(c.Output.Text))
		if err != nil {
			return rs, fmt.Errorf("malformed build metadata check %d: %w", c.ID, err)
		}
		if err = ValidateMetadataProvenance(c, m, repo, r); err != nil {
			return rs, fmt.Errorf("build metadata check %d: %w", c.ID, err)
		}
		if raw, seen := raws[c.ExternalID]; seen {
			if raw != c.Output.Text {
				return rs, fmt.Errorf("conflicting build metadata replicas for %s", c.ExternalID)
			}
			continue
		}
		raws[c.ExternalID] = c.Output.Text
		if err = rs.Add(m); err != nil {
			return rs, err
		}
	}
	return rs, nil
}

func ValidateMetadataProvenance(c model.GitHubMetadataCheck, m prwire.BuildMetadata, repo string, r MetadataWorkflowRun) error {
	if err := ValidateMetadataTransport(c, m, r); err != nil {
		return err
	}
	if err := ValidateMetadataRevisions(m, repo, r); err != nil {
		return err
	}
	for _, sel := range m.Selected {
		_, image, workload := prwire.ResolveImageTag(repo, sel.Service)
		if workload != sel.Service || image != sel.ImageName {
			return fmt.Errorf("service %q does not map to image %q", sel.Service, sel.ImageName)
		}
	}
	return ValidateMetadataImages(m)
}

// ValidateMetadataTransport binds the check itself: fixed name, exact external
// ID, a completed-neutral record on the run's source commit.
func ValidateMetadataTransport(c model.GitHubMetadataCheck, m prwire.BuildMetadata, r MetadataWorkflowRun) error {
	name := model.BuildMetadataCheckName
	if m.Kind == prwire.BuildMetadataResult {
		name = model.BuildMetadataResultCheckName
	}
	switch {
	case c.ID <= 0 || c.Name != name || c.ExternalID != MetadataExternalID(m):
		return fmt.Errorf("transport identity mismatch")
	case c.Status != statusCompleted || c.Conclusion != "neutral":
		return fmt.Errorf("transport check is not a completed neutral record")
	case c.HeadSHA != r.HeadSHA:
		return fmt.Errorf("source revision mismatch")
	}
	return nil
}

// ValidateMetadataRevisions binds the JSON identity to the authoritative run.
func ValidateMetadataRevisions(m prwire.BuildMetadata, repo string, r MetadataWorkflowRun) error {
	switch {
	case m.SourceSHA != r.HeadSHA:
		return fmt.Errorf("source revision mismatch")
	case m.Repository != repo || m.RunID != r.ID || m.RunNumber != r.RunNumber || m.RunAttempt > r.RunAttempt || m.WorkflowPath != r.Path || m.Event != r.Event:
		return fmt.Errorf("workflow run identity mismatch")
	case r.Event == model.GHEventPullRequest && (m.PRNumber == nil || m.ExecutionSHA == m.SourceSHA):
		return fmt.Errorf("invalid pull request identity")
	case r.Event != model.GHEventPullRequest && (m.PRNumber != nil || m.ExecutionSHA != m.SourceSHA):
		return fmt.Errorf("invalid branch build identity")
	}
	return nil
}

// ValidateMetadataImages checks result subsets, owner-scoped image references and digests.
func ValidateMetadataImages(m prwire.BuildMetadata) error {
	results := m.Results
	if m.Kind == prwire.BuildMetadataResult {
		if !slicesContainsSelection(m.Selected, m.Service, m.ImageName) || m.SourceAttempt != m.RunAttempt {
			return fmt.Errorf("result outside selection or attempt")
		}
		results = []prwire.BuildResult{{Service: m.Service, ImageName: m.ImageName, State: m.State, Pushed: m.Pushed, ImageRefs: m.ImageRefs, Digest: m.Digest}}
	}
	owner := MetadataOwner(m.Repository)
	for _, result := range results {
		if result.Pushed && len(result.ImageRefs) == 0 {
			return fmt.Errorf("pushed result %q has no image reference", result.Service)
		}
		prefix := "ghcr.io/" + owner + "/" + result.ImageName + ":"
		for _, ref := range result.ImageRefs {
			tag, ok := strings.CutPrefix(ref, prefix)
			if !ok || !model.MetadataSHA.MatchString(strings.TrimPrefix(tag, "tree-")) {
				return fmt.Errorf("invalid image reference %q", ref)
			}
		}
		if result.Digest != "" && !MetadataDigest.MatchString(result.Digest) {
			return fmt.Errorf("invalid image digest for %q", result.Service)
		}
	}
	return nil
}

func MetadataOwner(repo string) string {
	owner, _, _ := strings.Cut(repo, "/")
	return strings.ToLower(owner)
}

func slicesContainsSelection(selected []prwire.BuildSelection, service, image string) bool {
	for _, sel := range selected {
		if sel.Service == service && sel.ImageName == image {
			return true
		}
	}
	return false
}

// CurrentMetadataSnapshot projects records onto the run's current attempt. A
// final for that attempt must agree with the newest result record of every
// service. Without one, only current-attempt results are shown so inherited or
// superseded successes never read as current progress.
func CurrentMetadataSnapshot(rs MetadataRecords, attempt int) (prwire.BuildMetadata, bool, error) {
	if rs.Identity == nil {
		return prwire.BuildMetadata{}, false, nil
	}
	if final, ok := rs.Snapshots[attempt]; ok && final.Kind == prwire.BuildMetadataFinal {
		for _, result := range final.Results {
			newest, found := rs.NewestResult(result.Service, attempt)
			if !found || newest.RunAttempt != result.SourceAttempt || newest.State != result.State || newest.Pushed != result.Pushed ||
				!reflect.DeepEqual(newest.ImageRefs, result.ImageRefs) || newest.Digest != result.Digest {
				return final, false, fmt.Errorf("final result for %q disagrees with its transport record", result.Service)
			}
		}
		return final, true, nil
	}
	snapshot := *rs.Identity
	snapshot.Kind = prwire.BuildMetadataManifest
	snapshot.RunAttempt = attempt
	snapshot.Results = nil
	snapshot.Service, snapshot.ImageName, snapshot.State, snapshot.Pushed = "", "", "", false
	snapshot.SourceAttempt, snapshot.ImageRefs, snapshot.Digest = 0, nil, ""
	for _, sel := range snapshot.Selected {
		if r, ok := rs.Results[MetadataExternalID(prwire.BuildMetadata{Kind: prwire.BuildMetadataResult, Repository: snapshot.Repository, RunID: snapshot.RunID, RunAttempt: attempt, Service: sel.Service})]; ok {
			snapshot.Kind = prwire.BuildMetadataProgress
			snapshot.Results = append(snapshot.Results, prwire.BuildResult{Service: r.Service, ImageName: r.ImageName, State: r.State, Pushed: r.Pushed, SourceAttempt: attempt, ImageRefs: r.ImageRefs, Digest: r.Digest})
		}
	}
	return snapshot, true, nil
}

// MetadataPublishable reports whether a snapshot authorizes promotion: the
// current attempt's final aggregate, every selected image pushed, and the whole
// workflow (including any separate promotion job) successful.
func MetadataPublishable(m prwire.BuildMetadata, r MetadataWorkflowRun) bool {
	if m.Kind != prwire.BuildMetadataFinal || m.RunAttempt != r.RunAttempt || r.Status != statusCompleted || r.Conclusion != model.ConclusionSuccess || len(m.Results) != len(m.Selected) {
		return false
	}
	for _, result := range m.Results {
		if result.State != model.ConclusionSuccess || !result.Pushed {
			return false
		}
	}
	return true
}

// PRBuildRevision decides which producer revision a PR's cards follow: the
// current head while open, or the merge commit once merged.
func PRBuildRevision(lifecycle model.PRBuildLifecycle, sha string) (pullRequest, ok bool) {
	switch lifecycle.Status {
	case "open", "ci_failed", "conflict":
		return true, lifecycle.HeadSHA != "" && lifecycle.HeadSHA == sha
	case "merged", "synced":
		return false, lifecycle.MergeSHA != "" && lifecycle.MergeSHA == sha
	default:
		return false, false
	}
}

// PRMetadataRegistry is the PR state store build metadata tracking reads and writes (PRObject, through Restate).
type PRMetadataRegistry interface {
	LoadPRBuildLifecycle(ctx context.Context, repo string, prNumber int) (model.PRBuildLifecycle, error)
	RecordPRBuildMetadata(ctx context.Context, repo string, prNumber int, metadata prwire.BuildMetadata) (bool, error)
	LoadPRBuildMetadata(ctx context.Context, repo string, prNumber int) (prwire.BuildMetadata, bool, error)
}

type Consumer struct {
	gh       GitHubGetter
	registry PRMetadataRegistry
}

func NewConsumer(gh GitHubGetter, registry PRMetadataRegistry) *Consumer {
	return &Consumer{
		gh:       gh,
		registry: registry,
	}
}

func (c *Consumer) fetchMetadataRun(ctx context.Context, repo string, id int64) (MetadataWorkflowRun, error) {
	var r MetadataWorkflowRun
	if id <= 0 || MetadataWorkflowPath(repo) == "" {
		return r, fmt.Errorf("unsupported producer run")
	}
	if c.gh == nil {
		return r, fmt.Errorf("github client not initialized")
	}
	err := c.gh.GetGitHubJSON(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d", repo, id), 2<<20, &r)
	return r, err
}

func (c *Consumer) listCommitCheckRuns(ctx context.Context, repo, sha string) ([]model.GitHubMetadataCheck, error) {
	if c.gh == nil {
		return nil, fmt.Errorf("github client not initialized")
	}
	var all []model.GitHubMetadataCheck
	for page := 1; page <= MaxMetadataListPages; page++ {
		var batch model.GitHubCheckRunsResponse
		path := fmt.Sprintf("/repos/%s/commits/%s/check-runs?filter=all&per_page=%d&page=%d", repo, sha, MetadataListPageSize, page)
		if err := c.gh.GetGitHubJSON(ctx, path, 8<<20, &batch); err != nil {
			return nil, err
		}
		all = append(all, batch.CheckRuns...)
		if len(batch.CheckRuns) < MetadataListPageSize {
			return all, nil
		}
	}
	return nil, fmt.Errorf("check run listing exceeds %d pages", MaxMetadataListPages)
}

func (c *Consumer) newestMetadataRun(ctx context.Context, repo, sha string, pullRequest bool) (MetadataWorkflowRun, bool, error) {
	var newest MetadataWorkflowRun
	if c.gh == nil {
		return newest, false, fmt.Errorf("github client not initialized")
	}
	for page := 1; page <= MaxMetadataListPages; page++ {
		var runs struct {
			Runs []MetadataWorkflowRun `json:"workflow_runs"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs?head_sha=%s&per_page=%d&page=%d", repo, url.QueryEscape(sha), MetadataListPageSize, page)
		if err := c.gh.GetGitHubJSON(ctx, path, 4<<20, &runs); err != nil {
			return newest, false, err
		}
		for _, r := range runs.Runs {
			eligible := r.HeadSHA == sha && r.Path == MetadataWorkflowPath(repo) &&
				((pullRequest && r.Event == model.GHEventPullRequest) || (!pullRequest && model.IsMainBuildEvent(r.Event) && r.HeadBranch == DefaultMainBranch))
			if eligible && (r.RunNumber > newest.RunNumber || (r.RunNumber == newest.RunNumber && r.RunAttempt > newest.RunAttempt)) {
				newest = r
			}
		}
		if len(runs.Runs) < MetadataListPageSize {
			return newest, newest.ID != 0, nil
		}
	}
	return newest, false, fmt.Errorf("workflow run listing exceeds %d pages", MaxMetadataListPages)
}

func (c *Consumer) recordPRBuildSnapshot(ctx context.Context, repo string, prNum int, m prwire.BuildMetadata) {
	if c.registry == nil || prNum <= 0 {
		return
	}
	if _, err := c.registry.RecordPRBuildMetadata(ctx, repo, prNum, m); err != nil {
		slog.WarnContext(ctx, "[synapse] [build_metadata] recording build metadata snapshot", "repo", repo, "pr", prNum, "error", err)
	}
}

// ValidateMetadataExecution binds the execution revision: a branch build runs
// its source commit; a PR build runs GitHub's merge of the source head.
func (c *Consumer) ValidateMetadataExecution(ctx context.Context, repo string, r MetadataWorkflowRun, executionSHA string) error {
	if r.Event != model.GHEventPullRequest {
		if executionSHA != r.HeadSHA {
			return fmt.Errorf("branch execution revision mismatch")
		}
		return nil
	}
	if executionSHA == r.HeadSHA || !model.MetadataSHA.MatchString(executionSHA) {
		return fmt.Errorf("PR execution is not a merge revision")
	}
	if c.gh == nil {
		return fmt.Errorf("github client not initialized")
	}
	var commit struct {
		SHA     string `json:"sha"`
		Parents []struct {
			SHA string `json:"sha"`
		} `json:"parents"`
	}
	if err := c.gh.GetGitHubJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s", repo, executionSHA), 2<<20, &commit); err != nil {
		return err
	}
	if commit.SHA != executionSHA || len(commit.Parents) != 2 || commit.Parents[1].SHA != r.HeadSHA {
		return fmt.Errorf("execution revision is not bound to the PR head")
	}
	return nil
}

// ResolveRunMetadata returns the run's current-attempt snapshot. A completed
// run without structured records lacks required metadata.
func (c *Consumer) ResolveRunMetadata(ctx context.Context, repo string, r MetadataWorkflowRun) (prwire.BuildMetadata, MetadataSource, error) {
	checks, err := c.listCommitCheckRuns(ctx, repo, r.HeadSHA)
	if err != nil {
		return prwire.BuildMetadata{}, MetadataPending, err
	}
	records, err := CollectMetadataRecords(checks, repo, r)
	if err != nil {
		return prwire.BuildMetadata{}, MetadataPending, err
	}
	m, found, err := CurrentMetadataSnapshot(records, r.RunAttempt)
	if err != nil {
		return m, MetadataPending, err
	}
	if found {
		if err = c.ValidateMetadataExecution(ctx, repo, r, m.ExecutionSHA); err != nil {
			return m, MetadataPending, err
		}
		return m, MetadataStructured, nil
	}
	if r.Status != statusCompleted {
		return m, MetadataPending, nil
	}
	return m, MetadataPending, ErrBuildMetadataMissing
}

// ResolveWorkflowRunImages returns the images a successful main-branch producer
// run published, bound to the exact webhook attempt.
func (c *Consumer) ResolveWorkflowRunImages(ctx context.Context, repo string, ev WorkflowRunEvent, prNum int) ([]string, error) {
	repo = model.NormalizeRepo(repo)
	if MetadataWorkflowPath(repo) == "" {
		return nil, nil
	}
	r, err := c.fetchMetadataRun(ctx, repo, ev.ID)
	if err != nil {
		return nil, err
	}
	if r.Path != MetadataWorkflowPath(repo) {
		return nil, ErrNotImageWorkflow
	}
	if err = ValidateMetadataRunIdentity(r, repo, ev.ID); err != nil {
		return nil, err
	}
	switch {
	case r.Event == model.GHEventPullRequest:
		return nil, ErrNotImageWorkflow
	case ev.Attempt < 1:
		return nil, fmt.Errorf("workflow run webhook lacks its attempt")
	case r.RunAttempt > ev.Attempt:
		return nil, ErrStaleWorkflowEvent
	case r.RunAttempt != ev.Attempt || r.HeadSHA != ev.HeadSHA:
		return nil, fmt.Errorf("workflow run API disagrees with webhook (attempt %d/%d)", r.RunAttempt, ev.Attempt)
	case r.Status != statusCompleted || r.Conclusion != model.ConclusionSuccess:
		return nil, fmt.Errorf("workflow run has not succeeded (%s/%s)", r.Status, r.Conclusion)
	}
	m, _, err := c.ResolveRunMetadata(ctx, repo, r)
	if err != nil {
		return nil, err
	}
	if !MetadataPublishable(m, r) {
		return nil, fmt.Errorf("build metadata for run %d attempt %d is not publishable", r.ID, r.RunAttempt)
	}
	c.recordPRBuildSnapshot(ctx, repo, prNum, m)
	owner := MetadataOwner(repo)
	images := make([]string, 0, len(m.Selected))
	for _, sel := range m.Selected {
		images = append(images, fmt.Sprintf("ghcr.io/%s/%s:latest", owner, sel.Service))
	}
	return images, nil
}

// RefreshPRBuildMetadata re-resolves the newest eligible producer run for the
// PR's current revision and atomically records its snapshot. Stale revisions,
// and runs that have not published yet record nothing.
func (c *Consumer) RefreshPRBuildMetadata(ctx context.Context, repo string, pr int, sha string) (bool, error) {
	repo = model.NormalizeRepo(repo)
	if c.registry == nil || pr <= 0 || MetadataWorkflowPath(repo) == "" || !model.MetadataSHA.MatchString(sha) {
		return false, nil
	}
	lifecycle, err := c.registry.LoadPRBuildLifecycle(ctx, repo, pr)
	if err != nil {
		return false, err
	}
	pullRequest, ok := PRBuildRevision(lifecycle, sha)
	if !ok {
		return false, nil
	}
	newest, found, err := c.newestMetadataRun(ctx, repo, sha, pullRequest)
	if err != nil || !found {
		return false, err
	}
	r, err := c.fetchMetadataRun(ctx, repo, newest.ID)
	if err != nil {
		return false, err
	}
	if err = ValidateMetadataRunIdentity(r, repo, newest.ID); err != nil {
		return false, err
	}
	m, source, err := c.ResolveRunMetadata(ctx, repo, r)
	if err != nil || source != MetadataStructured {
		return false, err
	}
	if pullRequest && (m.PRNumber == nil || *m.PRNumber != pr) {
		return false, fmt.Errorf("build metadata PR identity mismatch")
	}
	return c.registry.RecordPRBuildMetadata(ctx, repo, pr, m)
}

// SupersedeStaleBuildAttempt demotes a stored snapshot once a newer attempt of
// the same run starts, without needing the GitHub API, so a prior attempt's
// success is never shown as current.
func (c *Consumer) SupersedeStaleBuildAttempt(ctx context.Context, repo string, pr int, ev WorkflowRunEvent) {
	if c.registry == nil || pr <= 0 || ev.ID <= 0 {
		return
	}
	m, found, err := c.registry.LoadPRBuildMetadata(ctx, repo, pr)
	if err != nil || !found || m.RunID != ev.ID || m.RunAttempt >= ev.Attempt || m.SourceSHA != ev.HeadSHA {
		return
	}
	pending := prwire.BuildMetadata{
		Version:      m.Version,
		Kind:         prwire.BuildMetadataManifest,
		Repository:   m.Repository,
		WorkflowPath: m.WorkflowPath,
		RunID:        m.RunID,
		RunNumber:    m.RunNumber,
		RunAttempt:   ev.Attempt,
		Event:        m.Event,
		SourceSHA:    m.SourceSHA,
		ExecutionSHA: m.ExecutionSHA,
		PRNumber:     m.PRNumber,
		Selected:     m.Selected,
	}
	c.recordPRBuildSnapshot(ctx, repo, pr, pending)
}
