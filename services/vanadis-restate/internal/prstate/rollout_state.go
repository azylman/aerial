package prstate

import (
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
)

// Rollout state on restate-owned facts.
//
// Build VO stores a chunk's rollout_state only from the facts that carry one
// (CompletePRDeployment on deploy_success, RecordPRRolloutState otherwise),
// and the build widget and cross-build sweep read it from there. PRObject
// renders it from committed PRState when it publishes a fact that carries
// none.

// Image card states for a selected service, matching synapse
// structuredServiceState.
const (
	imageStatePending     = "image_pending"
	imageStateBuilding    = "image_building"
	imageStateBuilt       = "image_built"
	imageStateUnpublished = "image_unpublished"
	imageStateFailed      = "image_failed"
)

// Build result states from the build-metadata snapshot.
const (
	buildResultRunning = "running"
	buildResultSuccess = "success"
)

// BuildPRRolloutState renders the rollout state of a PR from its committed
// state with synapse buildPRRolloutState's semantics: expected jobs are the
// row jobs plus every service the fenced build-metadata snapshot selected,
// started and deployed jobs come from the row, and job_states keys are
// lower-cased with selected services filled from their build results
// without overriding a deployment-reported state. Kustomization statuses
// stay empty: PRState does not hold them. It returns nil when nothing
// renders.
func BuildPRRolloutState(st *PRState) *pubsub.PRRolloutState {
	if st == nil {
		return nil
	}
	expected := dedupeTrimmed(st.Jobs)
	started := dedupeTrimmed(st.StartedJobs)
	deployed := dedupeTrimmed(st.DeployedJobs)
	jobStates := normalizedJobStates(st.JobStates)
	if m := st.renderableBuildMetadata(); m != nil {
		expected, jobStates = mergeStructuredBuildState(m, expected, jobStates)
	}
	if len(expected) == 0 && len(started) == 0 && len(deployed) == 0 && len(jobStates) == 0 {
		return nil
	}
	return &pubsub.PRRolloutState{
		ExpectedJobs: expected,
		StartedJobs:  started,
		DeployedJobs: deployed,
		JobStates:    jobStates,
	}
}

// withRolloutState fills a fact's rollout state from st when the fact
// carries none.
func withRolloutState(st *PRState, f pubsub.PRLifecycleEvent) pubsub.PRLifecycleEvent {
	if f.RolloutState == nil {
		f.RolloutState = BuildPRRolloutState(st)
	}
	return f
}

// renderableBuildMetadata is LoadPRBuildMetadata's view: the snapshot only
// while it matches the current revision and names this PR.
func (s *PRState) renderableBuildMetadata() *BuildMetadata {
	m := s.BuildMetadata
	if m == nil || !s.buildMetadataRevisionMatches(m.SourceSHA) {
		return nil
	}
	if strings.TrimSpace(m.Repository) != s.Repo || (m.PRNumber != nil && *m.PRNumber != s.PRNumber) {
		return nil
	}
	return m
}

// normalizedJobStates mirrors synapse extractStringMapFromMetadata: keys
// trimmed and lower-cased (empty keys dropped), values trimmed.
func normalizedJobStates(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if key := strings.ToLower(strings.TrimSpace(k)); key != "" {
			out[key] = strings.TrimSpace(v)
		}
	}
	return out
}

// mergeStructuredBuildState adds every selected service to the expected
// jobs and derives its image card state, never overriding a
// deployment-reported state.
func mergeStructuredBuildState(m *BuildMetadata, expected []string, jobStates map[string]string) ([]string, map[string]string) {
	if len(m.Selected) == 0 {
		return expected, jobStates
	}
	if jobStates == nil {
		jobStates = make(map[string]string, len(m.Selected))
	}
	for _, sel := range m.Selected {
		expected = append(expected, sel.Service)
		key := strings.ToLower(sel.Service)
		if jobStates[key] == "" {
			jobStates[key] = structuredServiceState(m, sel.Service)
		}
	}
	return dedupeTrimmed(expected), jobStates
}

// structuredServiceState maps a service's snapshot result onto its card
// state; a service without a result is pending.
func structuredServiceState(m *BuildMetadata, service string) string {
	for _, r := range m.Results {
		if r.Service != service {
			continue
		}
		switch {
		case r.State == buildResultRunning:
			return imageStateBuilding
		case r.State == buildResultSuccess && r.Pushed:
			return imageStateBuilt
		case r.State == buildResultSuccess:
			return imageStateUnpublished
		default:
			return imageStateFailed
		}
	}
	return imageStatePending
}

// dedupeTrimmed trims, drops empty and deduplicates items, keeping first
// occurrence order (synapse DedupeTrimmedStrings).
func dedupeTrimmed(items []string) []string {
	seen := make(map[string]bool, len(items))
	var out []string
	for _, item := range items {
		if clean := strings.TrimSpace(item); clean != "" && !seen[clean] {
			seen[clean] = true
			out = append(out, clean)
		}
	}
	return out
}
