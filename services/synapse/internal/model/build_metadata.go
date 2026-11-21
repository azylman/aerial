package model

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
	BuildMetadataManifest = "manifest"
	BuildMetadataFinal    = "final"
	BuildMetadataResult   = "result"
	BuildMetadataProgress = "progress"

	BuildMetadataCheckName       = "build-metadata"
	BuildMetadataResultCheckName = "build-metadata-result"

	BuildEventPush     = "push"
	BuildEventDispatch = "workflow_dispatch"

	BuildResultRunning   = "running"
	BuildResultFailure   = "failure"
	BuildResultCancelled = "cancelled"

	ConclusionSuccess  = "success"
	GHEventPullRequest = "pull_request"
)

var (
	MetadataSHA  = regexp.MustCompile(`^[a-f0-9]{40}$`)
	MetadataRepo = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	MetadataName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
)

// IsMainBuildEvent reports whether a producer event builds the default branch
// itself (as opposed to a PR's synthetic merge revision).
func IsMainBuildEvent(event string) bool {
	return event == BuildEventPush || event == BuildEventDispatch
}

func IsBuildMetadataCheck(name string) bool {
	return name == BuildMetadataCheckName || name == BuildMetadataResultCheckName
}

func ValidWorkflowPath(path string) bool {
	return strings.HasPrefix(path, ".github/workflows/") &&
		!strings.Contains(strings.TrimPrefix(path, ".github/workflows/"), "/") &&
		(strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml")) &&
		!strings.Contains(path, "..")
}

// ParseBuildMetadata validates structure only. It never authorizes promotion.
func ParseBuildMetadata(raw []byte) (BuildMetadata, error) { return parseBuildMetadata(raw, false) }

// ParseStoredBuildMetadata additionally accepts normalized progress snapshots.
func ParseStoredBuildMetadata(raw []byte) (BuildMetadata, error) {
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
	if !validBuildMetadataIdentity(m) || (!stored && m.Kind == BuildMetadataProgress) {
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
	return m.Version == 1 &&
		(m.Kind == BuildMetadataManifest || m.Kind == BuildMetadataFinal || m.Kind == BuildMetadataResult || m.Kind == BuildMetadataProgress) &&
		MetadataRepo.MatchString(m.Repository) &&
		ValidWorkflowPath(m.WorkflowPath) &&
		m.RunID > 0 && m.RunNumber > 0 && m.RunAttempt > 0
}

func validBuildMetadataSource(m BuildMetadata) bool {
	return MetadataSHA.MatchString(m.SourceSHA) &&
		MetadataSHA.MatchString(m.ExecutionSHA) &&
		(m.Event == GHEventPullRequest || IsMainBuildEvent(m.Event)) &&
		(m.PRNumber == nil || *m.PRNumber > 0)
}

func validateBuildMetadataResults(m BuildMetadata) error {
	if m.Selected == nil {
		return fmt.Errorf("missing selected services")
	}
	if !validBuildMetadataTransport(m) {
		return fmt.Errorf("invalid transport result")
	}
	if m.Kind == BuildMetadataManifest && len(m.Results) > 0 {
		return fmt.Errorf("manifest contains results")
	}
	selected := map[string]string{}
	for _, s := range m.Selected {
		if !MetadataName.MatchString(s.Service) || !MetadataName.MatchString(s.ImageName) || selected[s.Service] != "" {
			return fmt.Errorf("invalid selection")
		}
		selected[s.Service] = s.ImageName
	}
	seen := map[string]bool{}
	for _, r := range m.Results {
		if seen[r.Service] || selected[r.Service] != r.ImageName || r.ImageName == "" || r.SourceAttempt < 1 || r.SourceAttempt > m.RunAttempt || !validBuildResultState(r.State) || (r.Pushed && r.State != ConclusionSuccess) {
			return fmt.Errorf("invalid result")
		}
		seen[r.Service] = true
	}
	if m.Kind == BuildMetadataFinal && len(m.Results) != len(m.Selected) {
		return fmt.Errorf("incomplete final results")
	}
	return nil
}

func validBuildResultState(s string) bool {
	return s == BuildResultRunning || s == ConclusionSuccess || s == BuildResultFailure || s == BuildResultCancelled
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
	if m.Kind != BuildMetadataResult {
		return true
	}
	return MetadataName.MatchString(m.Service) &&
		MetadataName.MatchString(m.ImageName) &&
		validBuildResultState(m.State) &&
		m.SourceAttempt > 0 &&
		m.SourceAttempt <= m.RunAttempt &&
		(!m.Pushed || m.State == ConclusionSuccess)
}
