package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// BuildMetadata is an untrusted transport snapshot until the caller validates
// its provenance against authoritative workflow and PR API evidence.
type BuildMetadata struct {
	Version       int              `json:"version"`
	Kind          string           `json:"kind"`
	Repository    string           `json:"repository"`
	WorkflowPath  string           `json:"workflow_path"`
	RunID         int64            `json:"run_id"`
	RunNumber     int64            `json:"run_number"`
	RunAttempt    int              `json:"run_attempt"`
	Event         string           `json:"event"`
	SourceSHA     string           `json:"source_sha"`
	ExecutionSHA  string           `json:"execution_sha"`
	PRNumber      *int             `json:"pr_number"`
	Selected      []BuildSelection `json:"selected"`
	Results       []BuildResult    `json:"results,omitempty"`
	Service       string           `json:"service,omitempty"`
	ImageName     string           `json:"image_name,omitempty"`
	State         string           `json:"state,omitempty"`
	Pushed        bool             `json:"pushed,omitempty"`
	SourceAttempt int              `json:"source_attempt,omitempty"`
	ImageRefs     []string         `json:"image_refs,omitempty"`
	Digest        string           `json:"digest,omitempty"`
}

type BuildSelection struct {
	Service   string `json:"service"`
	ImageName string `json:"image_name"`
}

type BuildResult struct {
	Service       string   `json:"service"`
	ImageName     string   `json:"image_name"`
	State         string   `json:"state"`
	Pushed        bool     `json:"pushed"`
	SourceAttempt int      `json:"source_attempt"`
	ImageRefs     []string `json:"image_refs,omitempty"`
	Digest        string   `json:"digest,omitempty"`
}

const (
	buildMetadataManifest = "manifest"
	buildMetadataFinal    = "final"
	buildMetadataResult   = "result"
	// buildMetadataProgress is a normalized registry snapshot, never a producer wire kind.
	buildMetadataProgress = "progress"

	buildMetadataCheckName       = "build-metadata"
	buildMetadataResultCheckName = "build-metadata-result"

	buildEventPush     = "push"
	buildEventDispatch = "workflow_dispatch"

	buildResultRunning   = "running"
	buildResultFailure   = "failure"
	buildResultCancelled = "cancelled"
)

// isMainBuildEvent reports whether a producer event builds the default branch
// itself (as opposed to a PR's synthetic merge revision).
func isMainBuildEvent(event string) bool {
	return event == buildEventPush || event == buildEventDispatch
}

var (
	metadataSHA  = regexp.MustCompile(`^[a-f0-9]{40}$`)
	metadataRepo = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	metadataName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
)

func isBuildMetadataCheck(name string) bool {
	return name == buildMetadataCheckName || name == buildMetadataResultCheckName
}

func isDependabotAppCheck(appSlug string) bool {
	return strings.EqualFold(strings.TrimSpace(appSlug), "dependabot")
}

func validWorkflowPath(path string) bool {
	return strings.HasPrefix(path, ".github/workflows/") && !strings.Contains(strings.TrimPrefix(path, ".github/workflows/"), "/") && (strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml")) && !strings.Contains(path, "..")
}

// ParseBuildMetadata validates structure only. It never authorizes promotion.
func ParseBuildMetadata(raw []byte) (BuildMetadata, error) { return parseBuildMetadata(raw, false) }

// parseStoredBuildMetadata additionally accepts normalized progress snapshots.
func parseStoredBuildMetadata(raw []byte) (BuildMetadata, error) {
	return parseBuildMetadata(raw, true)
}

func parseBuildMetadata(raw []byte, stored bool) (BuildMetadata, error) {
	var m BuildMetadata
	if len(raw) == 0 || len(raw) > 65536 {
		return m, fmt.Errorf("invalid metadata size")
	}
	if err := rejectDuplicateMetadataKeys(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return m, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return m, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return m, fmt.Errorf("trailing metadata")
	}
	if !validBuildMetadataIdentity(m) || (!stored && m.Kind == buildMetadataProgress) {
		return m, fmt.Errorf("invalid metadata identity")
	}
	if err := validateBuildMetadataResults(m); err != nil {
		return m, err
	}
	return m, nil
}

func validBuildMetadataIdentity(m BuildMetadata) bool {
	return validBuildMetadataRun(m) && validBuildMetadataSource(m)
}

func validBuildMetadataRun(m BuildMetadata) bool {
	return m.Version == 1 && (m.Kind == buildMetadataManifest || m.Kind == buildMetadataFinal || m.Kind == buildMetadataResult || m.Kind == buildMetadataProgress) && metadataRepo.MatchString(m.Repository) && validWorkflowPath(m.WorkflowPath) && m.RunID > 0 && m.RunNumber > 0 && m.RunAttempt > 0
}

func validBuildMetadataSource(m BuildMetadata) bool {
	return metadataSHA.MatchString(m.SourceSHA) && metadataSHA.MatchString(m.ExecutionSHA) && (m.Event == ghEventPullRequest || isMainBuildEvent(m.Event)) && (m.PRNumber == nil || *m.PRNumber > 0)
}

func validateBuildMetadataResults(m BuildMetadata) error {
	if m.Selected == nil {
		return fmt.Errorf("missing selected services")
	}
	if !validBuildMetadataTransport(m) {
		return fmt.Errorf("invalid transport result")
	}
	if m.Kind == buildMetadataManifest && len(m.Results) > 0 {
		return fmt.Errorf("manifest contains results")
	}
	selected := map[string]string{}
	for _, s := range m.Selected {
		if !metadataName.MatchString(s.Service) || !metadataName.MatchString(s.ImageName) || selected[s.Service] != "" {
			return fmt.Errorf("invalid selection")
		}
		selected[s.Service] = s.ImageName
	}
	seen := map[string]bool{}
	for _, r := range m.Results {
		if seen[r.Service] || selected[r.Service] != r.ImageName || r.ImageName == "" || r.SourceAttempt < 1 || r.SourceAttempt > m.RunAttempt || !validBuildResultState(r.State) || (r.Pushed && r.State != conclusionSuccess) {
			return fmt.Errorf("invalid result")
		}
		seen[r.Service] = true
	}
	if m.Kind == buildMetadataFinal && len(m.Results) != len(m.Selected) {
		return fmt.Errorf("incomplete final results")
	}
	return nil
}

func validBuildResultState(s string) bool {
	return s == buildResultRunning || s == conclusionSuccess || s == buildResultFailure || s == buildResultCancelled
}

// rejectDuplicateMetadataKeys rejects ambiguous objects at every nesting level.
func rejectDuplicateMetadataKeys(dec *json.Decoder) error {
	token, tokenErr := dec.Token()
	if tokenErr != nil {
		return tokenErr
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || name != strings.ToLower(name) || seen[name] {
				return fmt.Errorf("duplicate metadata key")
			}
			seen[name] = true
			if err := rejectDuplicateMetadataKeys(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := rejectDuplicateMetadataKeys(dec); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, closeErr := dec.Token()
	return closeErr
}

type GitHubCheckRunsResponse struct {
	CheckRuns []GitHubMetadataCheck `json:"check_runs"`
}

type GitHubMetadataCheck struct {
	ID         int64  `json:"id"`
	HeadSHA    string `json:"head_sha"`
	ExternalID string `json:"external_id"`
	App        struct {
		ID   int64  `json:"id"`
		Slug string `json:"slug"`
	} `json:"app"`
	CheckSuite struct {
		ID int64 `json:"id"`
	} `json:"check_suite"`
	Output struct {
		Title   string `json:"title"`
		Summary string `json:"summary"`
		Text    string `json:"text"`
	} `json:"output"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

func validBuildMetadataTransport(m BuildMetadata) bool {
	if m.Kind != buildMetadataResult {
		return true
	}
	return metadataName.MatchString(m.Service) && metadataName.MatchString(m.ImageName) && validBuildResultState(m.State) && m.SourceAttempt > 0 && m.SourceAttempt <= m.RunAttempt && (!m.Pushed || m.State == conclusionSuccess)
}
