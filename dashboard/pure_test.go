package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var testRefTime = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

func mkRuns(runs ...GitHubRun) []GitHubRun {
	return runs
}

func mkRun(id int64, repo, sha, status, conclusion, msg string, createdOff, updatedOff time.Duration) GitHubRun {
	r := GitHubRun{
		ID:         id,
		Repository: repo,
		Name:       "Continuous Delivery",
		HeadSHA:    sha,
		Status:     status,
		Conclusion: conclusion,
		CreatedAt:  testRefTime.Add(createdOff),
		UpdatedAt:  testRefTime.Add(updatedOff),
	}
	if repo == "" {
		r.HTMLURL = fmt.Sprintf("https://github.com/azylman/aerial/actions/runs/%d", id)
	} else {
		r.HTMLURL = fmt.Sprintf("https://github.com/%s/actions/runs/%d", repo, id)
	}
	if msg != "" {
		r.HeadCommit = &struct {
			Message   string    `json:"message"`
			Timestamp time.Time `json:"timestamp"`
		}{Message: msg, Timestamp: testRefTime.Add(-10 * time.Minute)}
	}
	return r
}

func mkPRRun(id int64, repo, sha, status, conclusion string, createdOff, updatedOff time.Duration) GitHubRun {
	r := mkRun(id, repo, sha, status, conclusion, "", createdOff, updatedOff)
	r.Event = "pull_request"
	r.HeadBranch = "feat/sidecars-pr"
	return r
}

func mkJob(id, runID int64, name, status, conclusion string, startOff, endOff time.Duration) GitHubJob {
	j := GitHubJob{ID: id, RunID: runID, Name: name, Status: status, Conclusion: conclusion}
	if startOff != 0 {
		j.StartedAt = testRefTime.Add(startOff)
	}
	if endOff != 0 {
		j.CompletedAt = testRefTime.Add(endOff)
	}
	return j
}

func mkCont(name, service, sha, state, health string, createdOff time.Duration) DockerContainerJSON {
	c := DockerContainerJSON{
		ID:      "c-" + name,
		Names:   []string{"/" + strings.TrimPrefix(name, "/")},
		State:   state,
		Created: testRefTime.Add(createdOff).Unix(),
	}
	if service != "" || sha != "" {
		c.Labels = map[string]string{"com.docker.compose.project": "aerial"}
		if service != "" {
			c.Labels["com.docker.compose.service"] = service
		}
		if sha != "" {
			c.Labels["org.opencontainers.image.revision"] = sha
		}
	}
	if health != "" {
		c.Health = &DockerContainerHealth{Status: health}
	}
	return c
}

func mkLabeledCont(name, state string, labels map[string]string) DockerContainerJSON {
	c := DockerContainerJSON{
		ID:     "c-" + name,
		State:  state,
		Labels: labels,
	}
	if name != "" {
		c.Names = []string{"/" + strings.TrimPrefix(name, "/")}
	}
	return c
}

func mkRecon(active bool, state, sha, errMsg string, targets []string, startOff time.Duration) GitSyncStatusResponse {
	status := "synced"
	if state == "failed" {
		status = "error"
	}
	return GitSyncStatusResponse{
		Status: status,
		Reconciliation: &ReconciliationStatus{
			Active:         active,
			State:          state,
			Stage:          state,
			CommitSHA:      sha,
			Error:          errMsg,
			TargetServices: targets,
			StartedAt:      testRefTime.Add(startOff),
		},
	}
}

func mkNomadJob(id, name, jobType, status, deployStatus string, starting int) NomadJobItem {
	_ = deployStatus
	j := NomadJobItem{ID: id, Name: name, Type: jobType, Status: status}
	if starting > 0 {
		j.JobSummary = struct {
			Summary map[string]NomadJobSummaryGroup `json:"Summary"`
		}{
			Summary: map[string]NomadJobSummaryGroup{
				"web": {Starting: starting},
			},
		}
	}
	return j
}

func mkNomadAlloc(id, jobID, clientStatus string, createOff time.Duration, healthy *bool, tasks map[string]NomadTaskState) NomadAllocItem {
	a := NomadAllocItem{
		ID:           id,
		JobID:        jobID,
		ClientStatus: clientStatus,
		CreateTime:   testRefTime.Add(createOff).UnixNano(),
		TaskStates:   tasks,
	}
	if healthy != nil {
		a.DeploymentStatus = &NomadDeploymentStatus{Healthy: *healthy}
	}
	return a
}

func TestBuildFactsUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		baseURL    string
		query      url.Values
		wantLimit  int
		wantOffset int
		wantErr    bool
		wantQuery  map[string]string
		omitQuery  []string
	}{
		{name: "empty base URL errors", baseURL: "", query: url.Values{}, wantErr: true},
		{name: "control characters in base URL error", baseURL: "http://brain:\x7f8080", query: url.Values{}, wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", query: url.Values{}, wantErr: true},
		{name: "trailing slash normalized", baseURL: "http://brain:8080/", query: url.Values{}, omitQuery: []string{"limit", "offset", "category", "q"}},
		{name: "nil query handled cleanly", baseURL: "http://brain:8080", query: nil},
		{name: "positive limit and offset forwarded", baseURL: "http://brain:8080", query: url.Values{"limit": {"25"}, "offset": {"10"}}, wantLimit: 25, wantOffset: 10, wantQuery: map[string]string{"limit": "25", "offset": "10"}},
		{name: "zero or negative limit and offset omitted", baseURL: "http://brain:8080", query: url.Values{"limit": {"0"}, "offset": {"0"}}, omitQuery: []string{"limit", "offset"}},
		{name: "negative or invalid limit and offset omitted", baseURL: "http://brain:8080", query: url.Values{"limit": {"-5"}, "offset": {"invalid"}}, omitQuery: []string{"limit", "offset"}},
		{name: "category and q trimmed and forwarded", baseURL: "http://brain:8080", query: url.Values{"category": {"  system  "}, "q": {"  docker deploy  "}}, wantQuery: map[string]string{"category": "system", "q": "docker deploy"}},
		{name: "empty category and q omitted", baseURL: "http://brain:8080", query: url.Values{"category": {"   "}, "q": {"   "}}, omitQuery: []string{"category", "q"}},
		{name: "long ASCII q clamped to 64 runes", baseURL: "http://brain:8080", query: url.Values{"q": {strings.Repeat("a", 100)}}, wantQuery: map[string]string{"q": strings.Repeat("a", 64)}},
		{name: "multibyte unicode kanji clamped to 64 runes without UTF-8 corruption", baseURL: "http://brain:8080", query: url.Values{"q": {strings.Repeat("電", 70)}}, wantQuery: map[string]string{"q": strings.Repeat("電", 64)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotLimit, gotOffset, err := BuildFactsUpstreamURL(tt.baseURL, tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildFactsUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotLimit != tt.wantLimit {
				t.Errorf("BuildFactsUpstreamURL() gotLimit = %v, want %v", gotLimit, tt.wantLimit)
			}
			if gotOffset != tt.wantOffset {
				t.Errorf("BuildFactsUpstreamURL() gotOffset = %v, want %v", gotOffset, tt.wantOffset)
			}

			parsed, err := url.Parse(gotURL)
			if err != nil {
				t.Fatalf("failed to parse returned URL: %v", err)
			}
			if strings.Contains(parsed.Path, "//") {
				t.Errorf("expected normalized path without double slashes, got %q", parsed.Path)
			}

			q := parsed.Query()
			for k, v := range tt.wantQuery {
				if got := q.Get(k); got != v {
					t.Errorf("query param %q = %q, want %q", k, got, v)
				}
				if k == "q" {
					if runeLen := utf8.RuneCountInString(q.Get(k)); runeLen > 64 {
						t.Errorf("query param q has rune length %d, want <= 64", runeLen)
					}
				}
			}
			for _, k := range tt.omitQuery {
				if q.Has(k) {
					t.Errorf("expected query param %q to be omitted, got %q", k, q.Get(k))
				}
			}
		})
	}
}

func TestBuildSchedulesUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantURL string
		wantErr bool
	}{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control character returns error", baseURL: "http://brain:\x7f8080", wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", wantErr: true},
		{name: "clean base URL without trailing slash", baseURL: "http://brain:8080", wantURL: "http://brain:8080/schedules"},
		{name: "base URL with trailing slash normalized", baseURL: "http://brain:8080/", wantURL: "http://brain:8080/schedules"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, err := BuildSchedulesUpstreamURL(tt.baseURL)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildSchedulesUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && gotURL != tt.wantURL {
				t.Errorf("BuildSchedulesUpstreamURL() = %q, want %q", gotURL, tt.wantURL)
			}
		})
	}
}

func TestBuildScheduleRunsUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		baseURL    string
		query      url.Values
		wantLimit  int
		wantOffset int
		wantErr    bool
		wantQuery  map[string]string
	}{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control characters in URL return error", baseURL: "http://brain:\x7f8080", wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", wantErr: true},
		{name: "defaults applied when query is empty", baseURL: "http://brain:8080/", query: url.Values{}, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
		{name: "nil query receives defaults", baseURL: "http://brain:8080", query: nil, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
		{name: "valid limit and offset within boundaries", baseURL: "http://brain:8080", query: url.Values{"limit": {"100"}, "offset": {"20"}, "schedule_id": {"  cron-test  "}, "status": {"  success  "}}, wantLimit: 100, wantOffset: 20, wantQuery: map[string]string{"limit": "100", "offset": "20", "schedule_id": "cron-test", "status": "success"}},
		{name: "limit > 100 falls back to default 50", baseURL: "http://brain:8080", query: url.Values{"limit": {"150"}, "offset": {"-5"}}, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
		{name: "invalid non-integer limit and offset fall back to defaults", baseURL: "http://brain:8080", query: url.Values{"limit": {"abc"}, "offset": {"xyz"}}, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotLimit, gotOffset, err := BuildScheduleRunsUpstreamURL(tt.baseURL, tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildScheduleRunsUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotLimit != tt.wantLimit {
				t.Errorf("gotLimit = %d, want %d", gotLimit, tt.wantLimit)
			}
			if gotOffset != tt.wantOffset {
				t.Errorf("gotOffset = %d, want %d", gotOffset, tt.wantOffset)
			}

			parsed, err := url.Parse(gotURL)
			if err != nil {
				t.Fatalf("failed to parse returned URL: %v", err)
			}
			if strings.Contains(parsed.Path, "//") {
				t.Errorf("path contains double slashes: %q", parsed.Path)
			}

			q := parsed.Query()
			for k, v := range tt.wantQuery {
				if got := q.Get(k); got != v {
					t.Errorf("query param %q = %q, want %q", k, got, v)
				}
			}
		})
	}
}

func TestBuildVanadisBuildsUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		query     url.Values
		wantLimit int
		wantErr   bool
		wantQuery map[string]string
	}{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control characters in URL return error", baseURL: "http://vanadis:\x7f8080", wantErr: true},
		{name: "defaults applied when query is empty", baseURL: "http://vanadis.aerial/", query: url.Values{}, wantLimit: 50, wantQuery: map[string]string{"limit": "50"}},
		{name: "nil query receives default limit 50", baseURL: "http://vanadis.aerial", query: nil, wantLimit: 50, wantQuery: map[string]string{"limit": "50"}},
		{name: "valid limit and query params within boundaries", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"25"}, "status": {"  running  "}, "queue_state": {"  running  "}}, wantLimit: 25, wantQuery: map[string]string{"limit": "25", "status": "running", "queue_state": "running"}},
		{name: "limit > 100 clamped to 100", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"200"}}, wantLimit: 100, wantQuery: map[string]string{"limit": "100"}},
		{name: "limit < 1 clamped to 1", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"0"}}, wantLimit: 1, wantQuery: map[string]string{"limit": "1"}},
		{name: "negative limit clamped to 1", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"-10"}}, wantLimit: 1, wantQuery: map[string]string{"limit": "1"}},
		{name: "invalid non-integer limit falls back to default 50", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"invalid"}}, wantLimit: 50, wantQuery: map[string]string{"limit": "50"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotLimit, err := BuildVanadisBuildsUpstreamURL(tt.baseURL, tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildVanadisBuildsUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotLimit != tt.wantLimit {
				t.Errorf("gotLimit = %d, want %d", gotLimit, tt.wantLimit)
			}

			parsed, err := url.Parse(gotURL)
			if err != nil {
				t.Fatalf("failed to parse returned URL: %v", err)
			}
			if !strings.HasSuffix(parsed.Path, "/builds") {
				t.Errorf("path does not end with /builds: %q", parsed.Path)
			}

			q := parsed.Query()
			for k, v := range tt.wantQuery {
				if got := q.Get(k); got != v {
					t.Errorf("query param %q = %q, want %q", k, got, v)
				}
			}
		})
	}
}

func TestBuildVanadisBuildDetailUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		baseURL     string
		requestPath string
		wantID      string
		wantURL     string
		wantErr     bool
	}{
		{name: "empty base URL returns error", baseURL: "", requestPath: "/api/builds/build-123", wantErr: true},
		{name: "control characters in URL return error", baseURL: "http://vanadis:\x7f8080", requestPath: "/api/builds/build-123", wantErr: true},
		{name: "valid /api/builds/ prefix", baseURL: "http://vanadis.aerial", requestPath: "/api/builds/build-1792214230", wantID: "build-1792214230", wantURL: "http://vanadis.aerial/builds/build-1792214230"},
		{name: "valid /dashboard/api/builds/ prefix with trailing slash baseURL", baseURL: "http://vanadis.aerial/", requestPath: "/dashboard/api/builds/build-1792214230", wantID: "build-1792214230", wantURL: "http://vanadis.aerial/builds/build-1792214230"},
		{name: "missing expected prefix returns error", baseURL: "http://vanadis.aerial", requestPath: "/api/other/build-123", wantErr: true},
		{name: "empty build ID on /api/builds/ returns error", baseURL: "http://vanadis.aerial", requestPath: "/api/builds/", wantErr: true},
		{name: "empty build ID on /dashboard/api/builds/ returns error", baseURL: "http://vanadis.aerial", requestPath: "/dashboard/api/builds/", wantErr: true},
		{name: "path traversal with .. returns error", baseURL: "http://vanadis.aerial", requestPath: "/api/builds/../evil", wantErr: true},
		{name: "path traversal with embedded slash returns error", baseURL: "http://vanadis.aerial", requestPath: "/api/builds/build/subpath", wantErr: true},
		{name: "control characters in build ID return error", baseURL: "http://vanadis.aerial", requestPath: "/api/builds/build-\x00bad", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotID, err := BuildVanadisBuildDetailUpstreamURL(tt.baseURL, tt.requestPath)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildVanadisBuildDetailUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotID != tt.wantID {
				t.Errorf("gotID = %q, want %q", gotID, tt.wantID)
			}
			if gotURL != tt.wantURL {
				t.Errorf("gotURL = %q, want %q", gotURL, tt.wantURL)
			}
		})
	}
}

func TestCalculateClusterStatus_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		services   []ServiceStatus
		wantStatus string
	}{
		{
			name:       "nil services returns healthy",
			services:   nil,
			wantStatus: "healthy",
		},
		{
			name:       "empty services returns healthy",
			services:   []ServiceStatus{},
			wantStatus: "healthy",
		},
		{
			name: "all services healthy returns healthy",
			services: []ServiceStatus{
				{Name: "brain", Status: "healthy"},
				{Name: "dashboard", Status: "healthy"},
				{Name: "proxy", Status: "healthy"},
			},
			wantStatus: "healthy",
		},
		{
			name: "at least one unhealthy service returns degraded",
			services: []ServiceStatus{
				{Name: "brain", Status: "healthy"},
				{Name: "dashboard", Status: "unhealthy"},
				{Name: "proxy", Status: "healthy"},
			},
			wantStatus: "degraded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateClusterStatus(tt.services); got != tt.wantStatus {
				t.Errorf("CalculateClusterStatus() = %q, want %q", got, tt.wantStatus)
			}
		})
	}
}

func TestFormatUptimeString_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		sec      int
		expected string
	}{
		{name: "negative seconds clamped to 0s", sec: -10, expected: "0s"},
		{name: "zero seconds", sec: 0, expected: "0s"},
		{name: "sub-minute seconds", sec: 45, expected: "45s"},
		{name: "59 seconds", sec: 59, expected: "59s"},
		{name: "exact 1 minute", sec: 60, expected: "1m"},
		{name: "119 seconds", sec: 119, expected: "1m"},
		{name: "2 minutes", sec: 120, expected: "2m"},
		{name: "59 minutes", sec: 3599, expected: "59m"},
		{name: "exact 1 hour", sec: 3600, expected: "1h"},
		{name: "2 hours", sec: 7200, expected: "2h"},
		{name: "24 hours", sec: 86400, expected: "24h"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatUptimeString(tt.sec); got != tt.expected {
				t.Errorf("FormatUptimeString(%d) = %q, want %q", tt.sec, got, tt.expected)
			}
		})
	}
}

func TestIsCoreAerialContainer_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		container DockerContainerJSON
		wantCore  bool
	}{
		{name: "core aerial service via compose label", container: mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "brain"}), wantCore: true},
		{name: "core aerial service via container name prefix", container: mkCont("aerial-dashboard", "", "", "", "", 0), wantCore: true},
		{name: "ignored auxiliary service watchtower via compose label", container: mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "watchtower"}), wantCore: false},
		{name: "ignored auxiliary service autoheal via name", container: mkCont("aerial-autoheal", "", "", "", "", 0), wantCore: false},
		{name: "ignored auxiliary service agentsview", container: mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "agentsview"}), wantCore: false},
		{name: "ignored auxiliary service ollama", container: mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "ollama"}), wantCore: false},
		{name: "image containing watchtower is excluded", container: DockerContainerJSON{Image: "containrrr/watchtower:latest"}, wantCore: false},
		{name: "image containing autoheal is excluded", container: DockerContainerJSON{Image: "willfarrell/autoheal:latest"}, wantCore: false},
		{name: "image containing ollama is excluded", container: DockerContainerJSON{Image: "ollama/ollama:latest"}, wantCore: false},
		{name: "foreign image source excluded", container: mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "custom-service", "org.opencontainers.image.source": "https://github.com/someone/other-repo"}), wantCore: false},
		{name: "valid aerial image source accepted", container: mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "custom-service", "org.opencontainers.image.source": "https://github.com/azylman/aerial"}), wantCore: true},
		{name: "nil labels and names defaults to true", container: DockerContainerJSON{Image: "alpine:latest"}, wantCore: true},
		{name: "image containing agentsview is excluded", container: DockerContainerJSON{Image: "ghcr.io/azylman/agentsview:latest"}, wantCore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCoreAerialContainer(tt.container); got != tt.wantCore {
				t.Errorf("IsCoreAerialContainer() = %v, want %v", got, tt.wantCore)
			}
		})
	}
}

func TestExtractServiceNameFromJobName_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		jobName  string
		expected string
	}{
		{name: "brain service", jobName: "Build & Push brain (linux/amd64)", expected: "brain"},
		{name: "dashboard service", jobName: "Build & Push dashboard", expected: "dashboard"},
		{name: "proxy service", jobName: "test-proxy-service", expected: "proxy"},
		{name: "scheduler mcp", jobName: "scheduler-mcp CI", expected: "scheduler-mcp"},
		{name: "discord mcp", jobName: "discord-mcp Tests", expected: "discord-mcp"},
		{name: "docker mcp", jobName: "docker-mcp Unit", expected: "docker-mcp"},
		{name: "github mcp", jobName: "github-mcp Deploy", expected: "github-mcp"},
		{name: "nomad mcp", jobName: "nomad-mcp Build", expected: "nomad-mcp"},
		{name: "infisical mcp", jobName: "infisical-mcp Build", expected: "infisical-mcp"},
		{name: "webhooks router", jobName: "webhooks-router Build", expected: "webhooks-router"},
		{name: "ollama service", jobName: "Setup ollama image", expected: "ollama"},
		{name: "agentsview service", jobName: "agentsview container", expected: "agentsview"},
		{name: "unit test generic", jobName: "Unit Test Matrix", expected: "unit-tests"},
		{name: "test generic", jobName: "Service Integration Test", expected: "unit-tests"},
		{name: "lint job", jobName: "GolangCI Lint Run", expected: "lint"},
		{name: "validate-config job", jobName: "validate-config", expected: "config"},
		{name: "Validate Configuration job", jobName: "Validate Configuration", expected: "config"},
		{name: "parentheses sidecar aura-farming", jobName: "Build & Push Sidecar Images to GHCR (aura-farming)", expected: "aura-farming"},
		{name: "parentheses sidecar banana", jobName: "Build & Push Sidecar Images to GHCR (banana)", expected: "banana"},
		{name: "hangar service", jobName: "Build hangar", expected: "hangar"},
		{name: "docs service", jobName: "Build docs", expected: "docs"},
		{name: "unrelated job", jobName: "Cleanup Stale Artifacts", expected: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractServiceNameFromJobName(tt.jobName); got != tt.expected {
				t.Errorf("ExtractServiceNameFromJobName(%q) = %q, want %q", tt.jobName, got, tt.expected)
			}
		})
	}
}

func TestParseMatrixJobChips_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		jobs      []GitHubJob
		now       time.Time
		wantChips []MatrixJobChip
	}{
		{name: "empty jobs returns empty slice", jobs: []GitHubJob{}, now: testRefTime, wantChips: []MatrixJobChip{}},
		{name: "zero-time default fallback", jobs: []GitHubJob{mkJob(1, 10, "Build & Push Images (proxy)", "in_progress", "", -10*time.Second, 0)}, now: time.Time{}, wantChips: []MatrixJobChip{{Name: "proxy"}}},
		{name: "in_progress job computes active status and duration from refTime", jobs: []GitHubJob{mkJob(1, 10, "brain-build", "in_progress", "", -45*time.Second, 0)}, now: testRefTime, wantChips: []MatrixJobChip{{Name: "brain", Status: "active", Duration: "45s"}}},
		{name: "completed success and failure with deduplication", jobs: []GitHubJob{mkJob(1, 10, "dashboard-test", "completed", "success", -120*time.Second, -60*time.Second), mkJob(2, 10, "dashboard-duplicate", "completed", "failure", 0, 0), mkJob(3, 10, "scheduler-mcp", "completed", "failure", 0, 0), mkJob(4, 10, "discord-mcp", "completed", "cancelled", 0, 0)}, now: testRefTime, wantChips: []MatrixJobChip{{Name: "dashboard", Status: "completed", Duration: "60s"}, {Name: "scheduler-mcp", Status: "failed"}, {Name: "discord-mcp", Status: "pending"}}},
		{name: "zero reference time defaults to now", jobs: []GitHubJob{mkJob(1, 10, "brain-build", "in_progress", "", -10*time.Second, 0)}, now: time.Time{}, wantChips: []MatrixJobChip{{Name: "brain", Status: "active"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chips := ParseMatrixJobChips(tt.jobs, tt.now)
			if len(chips) != len(tt.wantChips) {
				t.Fatalf("ParseMatrixJobChips() len = %d, want %d", len(chips), len(tt.wantChips))
			}
			for i, want := range tt.wantChips {
				if want.Name != "" && chips[i].Name != want.Name {
					t.Errorf("chip[%d] name = %q, want %q", i, chips[i].Name, want.Name)
				}
				if want.Status != "" && chips[i].Status != want.Status {
					t.Errorf("chip[%d] status = %q, want %q", i, chips[i].Status, want.Status)
				}
				if want.Duration != "" && chips[i].Duration != want.Duration {
					t.Errorf("chip[%d] duration = %q, want %q", i, chips[i].Duration, want.Duration)
				}
			}
		})
	}
}

func TestBuildContainerChips_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		containers []DockerContainerJSON
		now        time.Time
		wantChips  []MatrixJobChip
	}{
		{name: "empty containers returns empty slice", containers: []DockerContainerJSON{}, now: testRefTime, wantChips: []MatrixJobChip{}},
		{name: "zero-time default fallback", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -60*time.Second)}, now: time.Time{}, wantChips: []MatrixJobChip{{Name: "brain"}}},
		{name: "zero or future creation time defaults to 0s duration", containers: []DockerContainerJSON{mkLabeledCont("aerial-brain", "running", nil), mkCont("aerial-dashboard", "", "", "running", "", 10*time.Minute)}, now: testRefTime, wantChips: []MatrixJobChip{{Duration: "0s"}, {Duration: "0s"}}},
		{name: "health status active for starting, failed for unhealthy or not running", containers: []DockerContainerJSON{mkCont("aerial-brain", "", "", "running", "starting", -120*time.Second), mkCont("aerial-proxy", "", "", "exited", "", -300*time.Second), mkCont("aerial-scheduler-mcp", "", "", "running", "unhealthy", -600*time.Second)}, now: testRefTime, wantChips: []MatrixJobChip{{Status: "active", Duration: "2m"}, {Status: "failed", Conclusion: "failure"}, {Status: "failed", Conclusion: "failure"}}},
		{name: "deduplicates running brain containers with future and current created", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", 1*time.Hour), mkCont("aerial-brain", "brain", "", "running", "", 0)}, now: testRefTime, wantChips: []MatrixJobChip{{Name: "brain", Duration: "0s"}}},
		{name: "nomad containers with alloc_id labels", containers: []DockerContainerJSON{
			mkLabeledCont("brain-2a3a24e5-d6ce-2069-bad1-f328c7a24af0", "running", map[string]string{"aerial.service": "brain", "com.hashicorp.nomad.alloc_id": "2a3a24e5-d6ce-2069-bad1-f328c7a24af0"}),
			mkLabeledCont("victoriametrics-mcp-6ca68da8-1599-0516-4c28-635658025294", "running", map[string]string{"com.hashicorp.nomad.alloc_id": "6ca68da8-1599-0516-4c28-635658025294"}),
			mkLabeledCont("hassio_supervisor", "running", nil),
		}, now: testRefTime, wantChips: []MatrixJobChip{{Name: "brain", Status: "completed", Conclusion: "success"}, {Name: "victoriametrics-mcp", Status: "completed"}}},
		{name: "zero reference time defaults to now", containers: []DockerContainerJSON{mkCont("aerial-brain", "", "", "running", "", -10*time.Second)}, now: time.Time{}, wantChips: []MatrixJobChip{{Status: "completed", Conclusion: "success"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chips := BuildContainerChips(tt.containers, tt.now)
			if len(chips) != len(tt.wantChips) {
				t.Fatalf("BuildContainerChips() len = %d, want %d", len(chips), len(tt.wantChips))
			}
			for i, want := range tt.wantChips {
				if want.Name != "" && chips[i].Name != want.Name {
					t.Errorf("chip[%d] name = %q, want %q", i, chips[i].Name, want.Name)
				}
				if want.Status != "" && chips[i].Status != want.Status {
					t.Errorf("chip[%d] status = %q, want %q", i, chips[i].Status, want.Status)
				}
				if want.Conclusion != "" && chips[i].Conclusion != want.Conclusion {
					t.Errorf("chip[%d] conclusion = %q, want %q", i, chips[i].Conclusion, want.Conclusion)
				}
				if want.Duration != "" && chips[i].Duration != want.Duration {
					t.Errorf("chip[%d] duration = %q, want %q", i, chips[i].Duration, want.Duration)
				}
			}
		})
	}
}

func TestGetContainerCommit_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		containers []DockerContainerJSON
		expected   string
	}{
		{name: "empty containers returns empty", containers: []DockerContainerJSON{}, expected: ""},
		{name: "ignored auxiliary container is skipped", containers: []DockerContainerJSON{mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "watchtower", "org.opencontainers.image.revision": "abcdef123456"})}, expected: ""},
		{name: "org.opencontainers.image.revision prioritized and truncated to 7 chars", containers: []DockerContainerJSON{mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "brain", "org.opencontainers.image.revision": "1234567890abcdef", "aerial.commit_sha": "fedcba0987654321"})}, expected: "1234567"},
		{name: "aerial.commit_sha prioritized over vcs-ref", containers: []DockerContainerJSON{mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "brain", "aerial.commit_sha": "abcdef1", "vcs-ref": "9999999"})}, expected: "abcdef1"},
		{name: "vcs-ref used as fallback", containers: []DockerContainerJSON{mkLabeledCont("", "", map[string]string{"com.docker.compose.service": "brain", "vcs-ref": "short"})}, expected: "short"},
		{name: "core container with nil labels returns empty", containers: []DockerContainerJSON{mkCont("aerial-brain", "", "", "", "", 0)}, expected: ""},
		{name: "short org.opencontainers.image.revision returned untruncated", containers: []DockerContainerJSON{mkLabeledCont("aerial-brain", "", map[string]string{"com.docker.compose.project": "aerial", "org.opencontainers.image.revision": "123456"})}, expected: "123456"},
		{name: "long aerial.commit_sha truncated to 7 chars", containers: []DockerContainerJSON{mkLabeledCont("aerial-brain", "", map[string]string{"com.docker.compose.project": "aerial", "aerial.commit_sha": "abcdef012345"})}, expected: "abcdef0"},
		{name: "long vcs-ref truncated to 7 chars", containers: []DockerContainerJSON{mkLabeledCont("aerial-brain", "", map[string]string{"com.docker.compose.project": "aerial", "vcs-ref": "9876543210"})}, expected: "9876543"},
		{name: "auxiliary sidecars ignored before brain", containers: []DockerContainerJSON{
			{ID: "c-agentsview", Names: []string{"/aerial-agentsview"}, Image: "ghcr.io/azylman/agentsview:latest", Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "agentsview", "org.opencontainers.image.revision": "080b49fb7a7c1d8f206549e8ee4eaf9cf50a5c20"}},
			{ID: "c-watchtower", Names: []string{"/aerial-watchtower"}, Image: "containrrr/watchtower:latest", Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "watchtower", "org.opencontainers.image.revision": "ace93994711edfe47036e2d2ee5f5d531df013ed"}},
			{ID: "c-autoheal", Names: []string{"/aerial-autoheal"}, Image: "willfarrell/autoheal:latest", Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "autoheal", "org.opencontainers.image.revision": "autohealsha1234"}},
			{ID: "c-brain", Names: []string{"/aerial-brain"}, Image: "ghcr.io/azylman/aerial-brain:latest", Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain", "aerial.commit_sha": "e056544d32a5f9d8a4cc50915a20db5eaea7db1e"}},
		}, expected: "e056544"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetContainerCommit(tt.containers); got != tt.expected {
				t.Errorf("GetContainerCommit() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestMatchesETag_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		ifNoneMatch string
		targetETag  string
		targetHash  string
		wantMatch   bool
	}{
		{name: "empty if-none-match returns false", ifNoneMatch: "", targetETag: `"etag1"`, wantMatch: false},
		{name: "wildcard * returns true", ifNoneMatch: "*", targetETag: `"etag1"`, wantMatch: true},
		{name: "exact etag match", ifNoneMatch: `"etag1"`, targetETag: `"etag1"`, wantMatch: true},
		{name: "weak prefix stripped", ifNoneMatch: `W/"etag1"`, targetETag: `"etag1"`, wantMatch: true},
		{name: "target hash match", ifNoneMatch: `"hash123"`, targetETag: `"etag1"`, targetHash: "hash123", wantMatch: true},
		{name: "comma separated list match", ifNoneMatch: `"etag0", "etag1", "etag2"`, targetETag: `"etag1"`, wantMatch: true},
		{name: "mismatched etag returns false", ifNoneMatch: `"other"`, targetETag: `"etag1"`, wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchesETag(tt.ifNoneMatch, tt.targetETag, tt.targetHash); got != tt.wantMatch {
				t.Errorf("MatchesETag(%q, %q, %q) = %v, want %v", tt.ifNoneMatch, tt.targetETag, tt.targetHash, got, tt.wantMatch)
			}
		})
	}
}

func TestGetMimeType_TableDriven(t *testing.T) {
	tests := []struct {
		path       string
		wantSubstr string
	}{
		{path: "index.html", wantSubstr: "text/html"},
		{path: "STYLE.CSS", wantSubstr: "text/css"},
		{path: "app.js", wantSubstr: "javascript"},
		{path: "DATA.JSON", wantSubstr: "application/json"},
		{path: "icon.svg", wantSubstr: "image/svg+xml"},
		{path: "favicon.ico", wantSubstr: "icon"},
		{path: "photo.png", wantSubstr: "image/png"},
		{path: "font.woff2", wantSubstr: "woff2"},
		{path: "font.woff", wantSubstr: "woff"},
		{path: "unknown.xyz123", wantSubstr: "application/octet-stream"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := GetMimeType(tt.path)
			if !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("GetMimeType(%q) = %q, want containing %q", tt.path, got, tt.wantSubstr)
			}
		})
	}
}

func TestCalculateDeployStatus_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		deployments []DeploymentStatus
		wantStatus  string
		wantOngoing bool
	}{
		{
			name:        "empty deployments returns idle and false",
			deployments: []DeploymentStatus{},
			wantStatus:  "idle",
			wantOngoing: false,
		},
		{
			name: "live completed deployment returns idle and false",
			deployments: []DeploymentStatus{
				{Stage: "live", Progress: 100},
			},
			wantStatus:  "idle",
			wantOngoing: false,
		},
		{
			name: "queued deployment returns queued and true",
			deployments: []DeploymentStatus{
				{Stage: "queued", Progress: 15},
			},
			wantStatus:  "queued",
			wantOngoing: true,
		},
		{
			name: "building deployment returns building and true",
			deployments: []DeploymentStatus{
				{Stage: "building", Progress: 45},
			},
			wantStatus:  "building",
			wantOngoing: true,
		},
		{
			name: "awaiting_pull deployment returns awaiting_pull and true",
			deployments: []DeploymentStatus{
				{Stage: "awaiting_pull", Progress: 50},
			},
			wantStatus:  "awaiting_pull",
			wantOngoing: true,
		},
		{
			name: "swapping deployment returns swapping and true",
			deployments: []DeploymentStatus{
				{Stage: "swapping", Progress: 75},
			},
			wantStatus:  "swapping",
			wantOngoing: true,
		},
		{
			name: "pulling deployment returns pulling and true",
			deployments: []DeploymentStatus{
				{Stage: "pulling", Progress: 55},
			},
			wantStatus:  "pulling",
			wantOngoing: true,
		},
		{
			name: "precedence: pulling overrides building",
			deployments: []DeploymentStatus{
				{Stage: "building"},
				{Stage: "pulling"},
			},
			wantStatus:  "pulling",
			wantOngoing: true,
		},
		{
			name: "failed deployment returns failed and false",
			deployments: []DeploymentStatus{
				{Stage: "failed", Progress: 0},
			},
			wantStatus:  "failed",
			wantOngoing: false,
		},
		{
			name: "degraded deployment returns degraded and false",
			deployments: []DeploymentStatus{
				{Stage: "degraded", Progress: 85},
			},
			wantStatus:  "degraded",
			wantOngoing: false,
		},
		{
			name: "precedence: swapping overrides building",
			deployments: []DeploymentStatus{
				{Stage: "building", Progress: 45},
				{Stage: "swapping", Progress: 75},
			},
			wantStatus:  "swapping",
			wantOngoing: true,
		},
		{
			name: "precedence: building overrides queued and failed",
			deployments: []DeploymentStatus{
				{Stage: "queued", Progress: 15},
				{Stage: "failed", Progress: 0},
				{Stage: "building", Progress: 35},
			},
			wantStatus:  "building",
			wantOngoing: true,
		},
		{
			name: "precedence: failed overrides degraded",
			deployments: []DeploymentStatus{
				{Stage: "degraded", Progress: 85},
				{Stage: "failed", Progress: 0},
			},
			wantStatus:  "failed",
			wantOngoing: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStatus := CalculateDeployStatus(tt.deployments)
			if gotStatus != tt.wantStatus {
				t.Errorf("CalculateDeployStatus() = %q, want %q", gotStatus, tt.wantStatus)
			}
			gotOngoing := IsDeployOngoing(tt.deployments)
			if gotOngoing != tt.wantOngoing {
				t.Errorf("IsDeployOngoing() = %v, want %v", gotOngoing, tt.wantOngoing)
			}
			for _, d := range tt.deployments {
				_ = IsDeployOngoing(d.Stage)
			}
		})
	}
}

func TestBuildTargetContainerChips_TableDriven(t *testing.T) {
	now := testRefTime
	startedAt := now.Add(-60 * time.Second)

	rawContainers := []DockerContainerJSON{
		mkCont("aerial-dashboard", "dashboard", "", "running", "", -30*time.Second),
		mkCont("aerial-brain", "brain", "", "running", "", -3*time.Hour),
		mkCont("aerial-proxy", "proxy", "", "running", "starting", -10*time.Second),
		mkCont("aerial-docs", "docs", "", "exited", "", -5*time.Second),
	}

	tests := []struct {
		name             string
		targets          []string
		startedAt        time.Time
		customContainers []DockerContainerJSON
		wantChips        []MatrixJobChip
	}{
		{name: "nil targets falls back to all containers", targets: nil, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "dashboard"}, {Name: "brain"}, {Name: "proxy"}, {Name: "docs"}}},
		{name: "empty targets falls back to all containers", targets: []string{}, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "dashboard"}, {Name: "brain"}, {Name: "proxy"}, {Name: "docs"}}},
		{name: "scoped to single recently created target", targets: []string{"dashboard"}, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "dashboard", Status: "completed", Conclusion: "success", Duration: "30s"}}},
		{name: "stale container reports active and pending duration", targets: []string{"brain"}, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "brain", Status: "active", Duration: "pending"}}},
		{name: "missing container reports active and pending duration", targets: []string{"scheduler-mcp"}, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "scheduler-mcp", Status: "active", Duration: "pending"}}},
		{name: "starting health reports active and non-empty duration", targets: []string{"proxy"}, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "proxy", Status: "active"}}},
		{name: "exited container reports failed and failure conclusion", targets: []string{"docs"}, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "docs", Status: "failed", Conclusion: "failure"}}},
		{name: "multiple targets deduplicated and trimmed", targets: []string{"dashboard", " dashboard ", "docs", ""}, startedAt: startedAt, wantChips: []MatrixJobChip{{Name: "dashboard"}, {Name: "docs"}}},
		{name: "zero reference time defaults now to time.Now().UTC()", targets: []string{"dashboard"}, startedAt: startedAt, customContainers: rawContainers, wantChips: []MatrixJobChip{{Name: "dashboard", Duration: "*"}}},
		{name: "container name prefix fallback and newer container wins", targets: []string{"brain"}, startedAt: startedAt, customContainers: []DockerContainerJSON{
			mkCont("aerial-brain", "", "", "running", "", -55*time.Second),
			mkCont("aerial-brain", "", "", "exited", "", -160*time.Second),
		}, wantChips: []MatrixJobChip{{Name: "brain", Status: "completed"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			containers := rawContainers
			if tt.customContainers != nil {
				containers = tt.customContainers
			}
			refTime := now
			if tt.name == "zero reference time defaults now to time.Now().UTC()" {
				refTime = time.Time{}
			}
			got := BuildTargetContainerChips(containers, tt.targets, tt.startedAt, refTime)
			if len(got) != len(tt.wantChips) {
				t.Fatalf("BuildTargetContainerChips() returned %d chips, want %d", len(got), len(tt.wantChips))
			}
			for i, want := range tt.wantChips {
				if want.Name != "" && got[i].Name != want.Name {
					t.Errorf("chip[%d] name = %q, want %q", i, got[i].Name, want.Name)
				}
				if want.Status != "" && got[i].Status != want.Status {
					t.Errorf("chip[%d] status = %q, want %q", i, got[i].Status, want.Status)
				}
				if want.Conclusion != "" && got[i].Conclusion != want.Conclusion {
					t.Errorf("chip[%d] conclusion = %q, want %q", i, got[i].Conclusion, want.Conclusion)
				}
				if want.Duration == "*" {
					if got[i].Duration == "" {
						t.Errorf("chip[%d] expected non-empty duration", i)
					}
				} else if want.Duration != "" && got[i].Duration != want.Duration {
					t.Errorf("chip[%d] duration = %q, want %q", i, got[i].Duration, want.Duration)
				}
			}
		})
	}
}

func TestMergeClusterDeploymentsWithHangar_PureTableDriven(t *testing.T) {
	t.Parallel()
	commitTime := testRefTime.Add(-10 * time.Minute)

	var cardinalityRuns []GitHubRun
	for i := 1; i <= 8; i++ {
		cardinalityRuns = append(cardinalityRuns, mkRun(int64(600+i), "", fmt.Sprintf("%07d000000000000000000000000000000000", i), "in_progress", "", "", time.Duration(-i)*time.Minute, time.Duration(-i)*time.Minute))
	}

	sortRuns := mkRuns(
		mkRun(100, "", "1111111111111111111111111111111111111111", "completed", "success", "", -10*time.Minute, -2*time.Minute),
		mkRun(101, "", "3333333333333333333333333333333333333333", "in_progress", "", "", -4*time.Minute, -3*time.Minute),
		mkRun(102, "", "5555555555555555555555555555555555555555", "completed", "success", "", -10*time.Minute, -2*time.Minute),
		mkRun(103, "", "2222222222222222222222222222222222222222", "queued", "", "", -1*time.Minute, -1*time.Minute),
	)
	sortGitSync := mkRecon(true, "pulling", "4444444444444444444444444444444444444444", "", nil, -30*time.Second)
	sortContainers := []DockerContainerJSON{
		mkCont("proxy", "proxy", "1111111111111111111111111111111111111111", "running", "", -40*time.Second),
		mkCont("brain", "brain", "6666666666666666666666666666666666666666", "running", "", -300*time.Second),
	}

	type wantDep struct {
		ID               string
		Service          string
		Repository       string
		Commit           string
		CommitMsgSubstr  string
		Stage            string
		Progress         int
		HTMLURL          string
		StepStatuses     []string
		StepStatusMap    map[int]string
		MatrixJobCount   int
		HasContainerChip bool
		HasCIChip        bool
		CheckCommitTime  *time.Time
	}

	tests := []struct {
		name          string
		runs          []GitHubRun
		jobs          map[int64][]GitHubJob
		gitSync       GitSyncStatusResponse
		containers    []DockerContainerJSON
		currentCommit string
		refTime       time.Time
		useZeroTime   bool
		wantCount     int
		wantList      []wantDep
		wantRepeats   int
	}{
		// 1. Concurrent Pipelines
		{name: "ConcurrentPipelines", runs: mkRuns(mkRun(201, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "in_progress", "", "feat: build feature B", -5*time.Minute, -1*time.Minute)), jobs: map[int64][]GitHubJob{201: {mkJob(1, 201, "build (dashboard)", "in_progress", "", -4*time.Minute, 0)}}, gitSync: mkRecon(true, "swapping", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", []string{"proxy"}, -2*time.Minute), currentCommit: "aaaaaaaa", wantList: []wantDep{{Stage: "swapping", Commit: "aaaaaaa"}, {Stage: "building", Commit: "bbbbbbb"}}},

		// 2. AwaitingPullBridge
		{name: "AwaitingPullBridge", runs: mkRuns(mkRun(301, "", "cccccccccccccccccccccccccccccccccccccccc", "completed", "success", "chore: updated docs", -10*time.Minute, -5*time.Minute)), currentCommit: "oldcommit", wantList: []wantDep{{Commit: "ccccccc", Stage: "awaiting_pull", StepStatusMap: map[int]string{1: "completed", 2: "active"}}}},

		// 3. DeterministicSort
		{name: "DeterministicSort", runs: sortRuns, gitSync: sortGitSync, containers: sortContainers, currentCommit: "6666666", wantCount: 5, wantList: []wantDep{{Stage: "swapping"}, {Stage: "pulling"}, {Stage: "building"}, {Stage: "awaiting_pull"}, {Stage: "queued"}}, wantRepeats: 10},

		// 4. MultiCommitContainerGrouping
		{name: "MultiCommitContainerGrouping", runs: mkRuns(mkRun(1, "", "aaaaaaa123456789", "completed", "success", "", -10*time.Minute, -5*time.Minute), mkRun(2, "", "bbbbbbb123456789", "completed", "success", "", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "aaaaaaa123456789", "running", "", -300*time.Second), mkCont("aerial-proxy", "proxy", "bbbbbbb123456789", "running", "", -30*time.Second)}, currentCommit: "aaaaaaa", wantList: []wantDep{{Commit: "bbbbbbb", Stage: "swapping"}, {Commit: "aaaaaaa", Stage: "live"}}},

		// 5. ChipScoping
		{name: "ChipScoping", runs: mkRuns(mkRun(401, "", "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "in_progress", "", "", -2*time.Minute, -1*time.Minute)), jobs: map[int64][]GitHubJob{401: {mkJob(1, 401, "build (brain)", "in_progress", "", -90*time.Second, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "running", "", -30*time.Second)}, currentCommit: "7777777", wantList: []wantDep{{HasCIChip: true, HasContainerChip: true}}},

		// 6. SafeNilHandling
		{name: "SafeNilHandling", runs: mkRuns(mkRun(501, "", "", "queued", "", "", -10*time.Second, 0), mkRun(502, "", "abc", "in_progress", "", "", -20*time.Second, 0), mkRun(503, "", "not-a-valid-hex-sha!@#$%^&*()", "completed", "failure", "", -30*time.Second, -25*time.Second)), wantCount: 3},

		// 7. CardinalityClamp
		{name: "CardinalityClamp", runs: cardinalityRuns, wantCount: 5},

		// 8. PullingNotPromotedToSwapping
		{name: "PullingNotPromotedToSwapping", gitSync: mkRecon(true, "pulling", "7777777777777777777777777777777777777777", "", []string{"aerial-dashboard"}, -15*time.Second), containers: []DockerContainerJSON{mkCont("aerial-dashboard", "", "7777777777777777777777777777777777777777", "running", "", -300*time.Second)}, wantList: []wantDep{{Stage: "pulling", StepStatusMap: map[int]string{2: "active", 3: "pending"}}}},

		// 9. DegradedAndFailedBranches_DegradedContainer
		{name: "DegradedAndFailedBranches_DegradedContainer", runs: mkRuns(mkRun(801, "", "8888888888888888888888888888888888888888", "completed", "success", "", -5*time.Minute, -2*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-dashboard", "", "8888888888888888888888888888888888888888", "exited", "", -60*time.Second)}, wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{4: "failed"}}}},

		// 10. DegradedAndFailedBranches_HangarFailedWithError
		{name: "DegradedAndFailedBranches_HangarFailedWithError", gitSync: mkRecon(false, "failed", "9999999999999999999999999999999999999999", "failed to pull image: auth expired", nil, 0), wantList: []wantDep{{Stage: "failed", CommitMsgSubstr: "auth expired"}}},

		// 11. DegradedAndFailedBranches_HangarFailedWithoutError
		{name: "DegradedAndFailedBranches_HangarFailedWithoutError", gitSync: mkRecon(false, "failed", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", nil, 0), wantList: []wantDep{{Stage: "failed"}}},

		// 12. MultiRunSameCommitAndSortTies_SameCommit
		{name: "MultiRunSameCommitAndSortTies_SameCommit", runs: mkRuns(mkRun(111, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "completed", "success", "", -10*time.Minute, -5*time.Minute), mkRun(112, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "in_progress", "", "", -1*time.Minute, -30*time.Second)), wantList: []wantDep{{Stage: "building"}}},

		// 13. MultiRunSameCommitAndSortTies_SortTiesLexicographical
		{name: "MultiRunSameCommitAndSortTies_SortTiesLexicographical", runs: mkRuns(mkRun(201, "", "ffffffffffffffffffffffffffffffffffffffff", "in_progress", "", "", -2*time.Minute, -1*time.Minute), mkRun(202, "", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "in_progress", "", "", -2*time.Minute, -1*time.Minute)), wantList: []wantDep{{Commit: "eeeeeee"}, {Commit: "fffffff"}}},

		// 14. MultiRunSameCommitAndSortTies_FailedPrecedesDegraded
		{name: "MultiRunSameCommitAndSortTies_FailedPrecedesDegraded", runs: mkRuns(mkRun(301, "", "1111111111111111111111111111111111111111", "completed", "failure", "", -5*time.Minute, -5*time.Minute), mkRun(302, "", "2222222222222222222222222222222222222222", "completed", "success", "", -5*time.Minute, -5*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-proxy", "", "2222222222222222222222222222222222222222", "exited", "", -5*time.Minute)}, wantList: []wantDep{{Stage: "failed"}, {Stage: "degraded"}}},

		// 15. MultiRunSameCommitAndSortTies_ExpiredIgnored
		{name: "MultiRunSameCommitAndSortTies_ExpiredIgnored", runs: mkRuns(mkRun(401, "", "3333333333333333333333333333333333333333", "completed", "success", "", -40*time.Minute, -35*time.Minute)), wantCount: 0},

		// 16. UnparentedContainersDoNotSpawnCards
		{name: "UnparentedContainersDoNotSpawnCards", containers: []DockerContainerJSON{mkLabeledCont("aerial-postgres", "running", map[string]string{"com.docker.compose.service": "postgres"}), mkLabeledCont("aerial-vector", "exited", map[string]string{"com.docker.compose.service": "vector"})}, currentCommit: "livecommit", wantCount: 0},

		// 17. ActiveCIRun
		{name: "ActiveCIRun", runs: mkRuns(mkRun(481, "", "7a9f1b234567", "in_progress", "", "feat(brain): live github actions tracking", -2*time.Minute, -10*time.Second)), jobs: map[int64][]GitHubJob{481: {mkJob(1, 481, "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)", "in_progress", "", -40*time.Second, 0), mkJob(2, 481, "Build & Push Images to GHCR (dashboard, ./dashboard, ./dashboard/Dockerfile, aerial-dashboard)", "completed", "success", -60*time.Second, -10*time.Second)}}, currentCommit: "7a9f1b2", wantList: []wantDep{{Stage: "building", Commit: "7a9f1b2", MatrixJobCount: 2, HTMLURL: "https://github.com/azylman/aerial/actions/runs/481"}}},

		// 18. FailedCIRun
		{name: "FailedCIRun", runs: mkRuns(mkRun(482, "", "abc1234567", "completed", "failure", "", -5*time.Minute, -4*time.Minute)), currentCommit: "abc1234", wantList: []wantDep{{Stage: "failed", StepStatusMap: map[int]string{1: "failed"}}}},

		// 19. AwaitingHangarSync
		{name: "AwaitingHangarSync", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, runs: mkRuns(mkRun(483, "", "def5678901", "completed", "success", "", -4*time.Minute, -45*time.Second)), jobs: map[int64][]GitHubJob{483: {mkJob(0, 483, "Build & Push Images to GHCR (brain)", "completed", "success", 0, 0)}}, currentCommit: "old1234", wantList: []wantDep{{Stage: "awaiting_pull", StepStatusMap: map[int]string{2: "active"}}}},

		// 20. HangarSyncTimeout
		{name: "HangarSyncTimeout", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, runs: mkRuns(mkRun(484, "", "timeout1234", "completed", "success", "", -10*time.Minute, -150*time.Second)), jobs: map[int64][]GitHubJob{484: {mkJob(0, 484, "Build & Push Images to GHCR (brain)", "completed", "success", 0, 0)}}, currentCommit: "old1234", wantList: []wantDep{{Stage: "failed", CommitMsgSubstr: "Hangar reconciliation timed out (>120s)", StepStatusMap: map[int]string{2: "failed"}}}},

		// 21. NoContainerBuilds_ImmediateLive
		{name: "NoContainerBuilds_ImmediateLive", runs: mkRuns(mkRun(485, "", "purecode1234", "completed", "success", "", -10*time.Minute, -150*time.Second)), jobs: map[int64][]GitHubJob{485: {mkJob(0, 485, "Run Service Unit Tests", "completed", "success", 0, 0), mkJob(0, 485, "Lint Microservices & Frontend", "completed", "success", 0, 0), mkJob(0, 485, "Build & Push Images to GHCR (brain)", "completed", "skipped", 0, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -1*time.Hour)}, currentCommit: "old1234", wantList: []wantDep{{Stage: "live", Progress: 100, StepStatusMap: map[int]string{2: "completed"}}}},

		// 22. RollingSwap
		{name: "RollingSwap", runs: mkRuns(mkRun(101, "", "abc9999999", "completed", "success", "", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "abc9999999", "running", "starting", -30*time.Second), mkCont("aerial-dashboard", "dashboard", "", "running", "", -10*time.Second)}, currentCommit: "abc9999", wantList: []wantDep{{Stage: "swapping", Commit: "abc9999", MatrixJobCount: 2}}},

		// 23. SyncedGrace
		{name: "SyncedGrace", runs: mkRuns(mkRun(102, "", "live123", "completed", "success", "", -5*time.Minute, -4*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "healthy", -3*time.Minute), mkCont("aerial-dashboard", "dashboard", "", "running", "healthy", -3*time.Minute)}, currentCommit: "live123", wantList: []wantDep{{Stage: "live", Progress: 100, StepStatuses: []string{"completed", "completed", "completed", "completed", "completed"}}}},

		// 24. DegradedState
		{name: "DegradedState", runs: mkRuns(mkRun(103, "", "deg1234", "completed", "success", "", -5*time.Minute, -3*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "unhealthy", -2*time.Minute), mkCont("aerial-dashboard", "dashboard", "", "running", "healthy", -2*time.Minute)}, currentCommit: "deg1234", wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{4: "failed"}}}},

		// 25. Idle
		{name: "Idle", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "healthy", -2*time.Hour)}, currentCommit: "idle123", wantCount: 0},

		// 26. AdversarialNilLabelsAndNilHealth
		{name: "AdversarialNilLabelsAndNilHealth", runs: mkRuns(mkRun(104, "", "test1234", "completed", "success", "", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "", "", "running", "", -30*time.Second), mkCont("aerial-dashboard", "", "", "running", "", -30*time.Second)}, currentCommit: "test1234", wantList: []wantDep{{Stage: "swapping", MatrixJobCount: 2}}},

		// 27. AdversarialClockSkewFutureTimestamp
		{name: "AdversarialClockSkewFutureTimestamp", runs: mkRuns(mkRun(999, "", "future1234", "completed", "success", "", 10*time.Second, 15*time.Second)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", 2*time.Hour)}, currentCommit: "old1234", wantList: []wantDep{{Stage: "awaiting_pull"}}},

		// 28. CommitTimeAcrossAllStages_Queued
		{name: "CommitTimeAcrossAllStages_Queued", runs: mkRuns(mkRun(501, "", "1111aaa", "queued", "", "feat: queued", -2*time.Minute, 0)), currentCommit: "1111aaa", wantList: []wantDep{{CheckCommitTime: &commitTime}}},

		// 29. CommitTimeAcrossAllStages_Building
		{name: "CommitTimeAcrossAllStages_Building", runs: mkRuns(mkRun(502, "", "2222bbb", "in_progress", "", "feat: building", -1*time.Minute, 0)), currentCommit: "2222bbb", wantList: []wantDep{{CheckCommitTime: &commitTime}}},

		// 30. CommitTimeAcrossAllStages_Failed
		{name: "CommitTimeAcrossAllStages_Failed", runs: mkRuns(mkRun(503, "", "3333ccc", "completed", "failure", "feat: failed", -5*time.Minute, -4*time.Minute)), currentCommit: "3333ccc", wantList: []wantDep{{CheckCommitTime: &commitTime}}},

		// 31. CommitTimeAcrossAllStages_Swapping
		{name: "CommitTimeAcrossAllStages_Swapping", runs: mkRuns(mkRun(504, "", "4444ddd", "completed", "success", "feat: success", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "starting", -30*time.Second)}, currentCommit: "4444ddd", wantList: []wantDep{{CheckCommitTime: &commitTime}}},

		// 32. EmptyAndFuture_Empty
		{name: "EmptyAndFuture_Empty", currentCommit: "commit123", wantCount: 0},

		// 33. EmptyAndFuture_FutureContainers
		{name: "EmptyAndFuture_FutureContainers", runs: mkRuns(mkRun(123, "", "sha1234567890", "completed", "success", "", -10*time.Minute, 0)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", 2*time.Hour)}, wantCount: 1},

		// 34. HangarActiveSuppresses120sTimeout
		{name: "HangarActiveSuppresses120sTimeout", runs: mkRuns(mkRun(101, "", "abc1234567890", "completed", "success", "", -300*time.Second, -200*time.Second)), jobs: map[int64][]GitHubJob{101: {mkJob(0, 101, "Build & Push Images (dashboard)", "completed", "success", 0, 0)}}, gitSync: mkRecon(true, "pulling", "", "", []string{"dashboard"}, 0), currentCommit: "oldcommit", wantList: []wantDep{{StepStatusMap: map[int]string{2: "active"}}}},

		// 35. HangarFailedFastFail
		{name: "HangarFailedFastFail", runs: mkRuns(mkRun(102, "", "def5678901234", "completed", "success", "", -60*time.Second, -30*time.Second)), gitSync: mkRecon(false, "failed", "", "manifest unknown", nil, 0), currentCommit: "oldcommit", wantList: []wantDep{{Stage: "failed", CommitMsgSubstr: "manifest unknown", StepStatusMap: map[int]string{2: "failed"}}}},

		// 36. TargetServicesScoping
		{name: "TargetServicesScoping", containers: []DockerContainerJSON{mkCont("aerial-dashboard", "dashboard", "", "running", "", -30*time.Second), mkCont("aerial-brain", "brain", "", "running", "", -5*3600*time.Second), mkCont("aerial-proxy", "proxy", "", "running", "", -5*3600*time.Second)}, gitSync: mkRecon(true, "swapping", "548c826", "", []string{"dashboard"}, -60*time.Second), currentCommit: "current", wantList: []wantDep{{Stage: "swapping", Commit: "548c826", MatrixJobCount: 1}}},

		// 37. DegradedWithoutHangarFailure
		{name: "DegradedWithoutHangarFailure", runs: mkRuns(mkRun(105, "", "commit123", "completed", "success", "", -15*time.Minute, -12*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "unhealthy", -10*time.Minute)}, gitSync: GitSyncStatusResponse{Status: "synced"}, currentCommit: "commit123", wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{3: "completed", 4: "failed"}}}},

		// 38. MultiRepoActiveFanout
		{name: "MultiRepoActiveFanout", runs: mkRuns(mkRun(101, "azylman/aerial", "1111aaa", "in_progress", "", "", -2*time.Minute, 0), mkRun(201, "azylman/aerial-sidecars", "2222bbb", "in_progress", "", "", -1*time.Minute, 0)), currentCommit: "0000000", wantList: []wantDep{{Repository: "azylman/aerial-sidecars", Service: "aerial-sidecars", Stage: "building"}, {Repository: "azylman/aerial", Service: "aerial-stack", Stage: "building"}}},

		// 39. NonContainerRepoNoTimeout
		{name: "NonContainerRepoNoTimeout", runs: mkRuns(mkRun(301, "azylman/aerial-config", "3333ccc", "completed", "success", "", -5*time.Minute, -150*time.Second)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, currentCommit: "oldcorecommit", wantList: []wantDep{{Service: "aerial-config", Stage: "awaiting_pull"}}},

		// 40. ActivePRRunShowsBuilding
		{name: "ActivePRRunShowsBuilding", runs: mkRuns(mkPRRun(9001, "azylman/aerial-sidecars", "aabbccdd1122", "in_progress", "", -1*time.Minute, -30*time.Second)), jobs: map[int64][]GitHubJob{9001: {mkJob(0, 9001, "Run Sidecar Tests & Lint", "completed", "success", 0, 0), mkJob(0, 9001, "Build & Push Sidecar Images to GHCR (aura-farming)", "in_progress", "", 0, 0)}}, currentCommit: "0000000", wantList: []wantDep{{Repository: "azylman/aerial-sidecars", Stage: "building", MatrixJobCount: 2}}},

		// 41. CompletedPRRunDoesNotTimeout
		{name: "CompletedPRRunDoesNotTimeout", runs: mkRuns(mkPRRun(9002, "azylman/aerial-sidecars", "aabbccdd1122", "completed", "success", -300*time.Second, -200*time.Second)), jobs: map[int64][]GitHubJob{9002: {mkJob(0, 9002, "Build & Push Sidecar Images to GHCR (aura-farming)", "completed", "success", 0, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, currentCommit: "oldcorecommit", wantCount: 0},

		// 42. ReconTargetServices_MatchingContainerWithoutCommitLabel
		{name: "ReconTargetServices_MatchingContainerWithoutCommitLabel", gitSync: mkRecon(true, "pulling", "8888888888888888888888888888888888888888", "", []string{"proxy"}, -15*time.Second), containers: []DockerContainerJSON{mkCont("aerial-proxy", "proxy", "", "running", "", -10*time.Second)}, wantList: []wantDep{{Commit: "8888888", MatrixJobCount: 1, HasContainerChip: true}}},

		// 43. ReconShortSteps_FailedState
		{name: "ReconShortSteps_FailedState", gitSync: mkRecon(false, "failed", "9999999999999999999999999999999999999999", "docker compose build failed", nil, -2*time.Minute), wantList: []wantDep{{Stage: "failed", Progress: 55, CommitMsgSubstr: "docker compose build failed", StepStatuses: []string{"completed", "completed", "failed", "pending", "pending"}}}},

		// 44. ReconEmptyCommitSHA_FallbackToHangar
		{name: "ReconEmptyCommitSHA_FallbackToHangar", gitSync: mkRecon(true, "pulling", "", "", nil, -10*time.Second), wantList: []wantDep{{ID: "dep-aerial-stack-hangar", Commit: "dep-aerial-stack-hangar"}}},

		// 45. ReconEmptyCommitSHA_FallbackToRuns
		{name: "ReconEmptyCommitSHA_FallbackToRuns", runs: mkRuns(mkRun(701, "", "abcdef0123456789012345678901234567890123", "in_progress", "", "", -1*time.Minute, 0)), gitSync: mkRecon(true, "pulling", "", "", nil, -10*time.Second), currentCommit: "ignored", wantList: []wantDep{{Commit: "abcdef0"}}},

		// 46. ReconEmptyCommitSHA_FallbackToCurrentCommit
		{name: "ReconEmptyCommitSHA_FallbackToCurrentCommit", gitSync: mkRecon(true, "pulling", "", "", nil, -10*time.Second), currentCommit: "fedcba987654321", wantList: []wantDep{{Commit: "fedcba9"}}},

		// 47. ReconExistingRun_SwappingState
		{name: "ReconExistingRun_SwappingState", runs: mkRuns(mkRun(777, "", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "in_progress", "", "", -2*time.Minute, 0)), gitSync: mkRecon(true, "swapping", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", []string{"brain"}, -30*time.Second), wantList: []wantDep{{Stage: "swapping", StepStatusMap: map[int]string{3: "active", 4: "active"}}}},

		// 48. ReconExistingRun_FailedState
		{name: "ReconExistingRun_FailedState", runs: mkRuns(mkRun(778, "", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "in_progress", "", "", -2*time.Minute, 0)), gitSync: mkRecon(false, "failed", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "host crash", nil, -10*time.Second), wantList: []wantDep{{Stage: "failed", StepStatusMap: map[int]string{2: "failed"}}}},

		// 49. ReconExistingRun_HealthyState
		{name: "ReconExistingRun_HealthyState", runs: mkRuns(mkRun(779, "", "cccccccccccccccccccccccccccccccccccccccc", "in_progress", "", "", -2*time.Minute, 0)), gitSync: mkRecon(false, "healthy", "cccccccccccccccccccccccccccccccccccccccc", "", nil, -10*time.Second), wantList: []wantDep{{Stage: "building", Progress: 25, StepStatuses: []string{"completed", "completed", "completed", "completed", "completed"}}}},

		// 50. ReconShortSteps_HealthyState
		{name: "ReconShortSteps_HealthyState", gitSync: mkRecon(false, "healthy", "cccccccccccccccccccccccccccccccccccccccc", "", nil, -10*time.Second), wantCount: 0},

		// 51. ReconShortSteps_PullingState
		{name: "ReconShortSteps_PullingState", gitSync: mkRecon(true, "pulling", "dddddddddddddddddddddddddddddddddddddddd", "", nil, -10*time.Second), wantList: []wantDep{{Stage: "pulling", Progress: 55, StepStatuses: []string{"completed", "completed", "active", "pending", "pending"}}}},

		// 52. ReconExistingRun_PullingState
		{name: "ReconExistingRun_PullingState", runs: mkRuns(mkRun(780, "", "dddddddddddddddddddddddddddddddddddddddd", "in_progress", "", "", -2*time.Minute, 0)), gitSync: mkRecon(true, "pulling", "dddddddddddddddddddddddddddddddddddddddd", "", nil, -10*time.Second), wantList: []wantDep{{Stage: "pulling", Progress: 55, StepStatusMap: map[int]string{1: "completed", 2: "active"}}}},

		// 53. ReconShortSteps_SwappingState
		{name: "ReconShortSteps_SwappingState", gitSync: mkRecon(true, "swapping", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "", nil, -10*time.Second), wantList: []wantDep{{Stage: "swapping", Progress: 60, StepStatuses: []string{"completed", "completed", "completed", "active", "active"}}}},

		// 54. Prefix40CharSHA_MatchedBy7CharPrefixContainer
		{name: "Prefix40CharSHA_MatchedBy7CharPrefixContainer", runs: mkRuns(mkRun(888, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "in_progress", "", "", -3*time.Minute, 0)), containers: []DockerContainerJSON{mkCont("aerial-proxy", "proxy", "bbbbbbb", "running", "", -20*time.Second)}, wantList: []wantDep{{Commit: "bbbbbbb"}}},

		// 55. SortTieBreaker_FailedPrecedesDegraded_BothPositions
		{name: "SortTieBreaker_FailedPrecedesDegraded_BothPositions", runs: mkRuns(mkRun(901, "", "1111111111111111111111111111111111111111", "completed", "failure", "", -5*time.Minute, -5*time.Minute), mkRun(902, "", "2222222222222222222222222222222222222222", "completed", "success", "", -5*time.Minute, -5*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "", "2222222222222222222222222222222222222222", "exited", "", -5*time.Minute)}, wantList: []wantDep{{Stage: "failed", Commit: "1111111"}, {Stage: "degraded", Commit: "2222222"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ref := tt.refTime
			if ref.IsZero() && !tt.useZeroTime {
				ref = testRefTime
			}
			deps := MergeClusterDeploymentsWithHangar(tt.runs, tt.jobs, tt.gitSync, tt.containers, tt.currentCommit, ref)

			expectedCount := tt.wantCount
			if expectedCount == 0 && len(tt.wantList) > 0 {
				expectedCount = len(tt.wantList)
			}
			if len(deps) != expectedCount {
				t.Fatalf("expected %d deployments, got %d: %+v", expectedCount, len(deps), deps)
			}
			for i, want := range tt.wantList {
				if i >= len(deps) {
					break
				}
				got := deps[i]
				if want.ID != "" && got.ID != want.ID {
					t.Errorf("dep[%d].ID = %q, want %q", i, got.ID, want.ID)
				}
				if want.Service != "" && got.Service != want.Service {
					t.Errorf("dep[%d].Service = %q, want %q", i, got.Service, want.Service)
				}
				if want.Repository != "" && got.Repository != want.Repository {
					t.Errorf("dep[%d].Repository = %q, want %q", i, got.Repository, want.Repository)
				}
				if want.Commit != "" && got.Commit != want.Commit {
					t.Errorf("dep[%d].Commit = %q, want %q", i, got.Commit, want.Commit)
				}
				if want.Stage != "" && got.Stage != want.Stage {
					t.Errorf("dep[%d].Stage = %q, want %q", i, got.Stage, want.Stage)
				}
				if want.Progress != 0 && got.Progress != want.Progress {
					t.Errorf("dep[%d].Progress = %d, want %d", i, got.Progress, want.Progress)
				}
				if want.CommitMsgSubstr != "" && !strings.Contains(got.CommitMsg, want.CommitMsgSubstr) {
					t.Errorf("dep[%d].CommitMsg = %q, want containing %q", i, got.CommitMsg, want.CommitMsgSubstr)
				}
				if want.HTMLURL != "" && got.HTMLURL != want.HTMLURL {
					t.Errorf("dep[%d].HTMLURL = %q, want %q", i, got.HTMLURL, want.HTMLURL)
				}
				if len(want.StepStatuses) > 0 {
					if len(got.Steps) != len(want.StepStatuses) {
						t.Errorf("dep[%d].Steps len = %d, want %d", i, len(got.Steps), len(want.StepStatuses))
					} else {
						for sIdx, expStatus := range want.StepStatuses {
							if got.Steps[sIdx].Status != expStatus {
								t.Errorf("dep[%d].Steps[%d].Status = %q, want %q", i, sIdx, got.Steps[sIdx].Status, expStatus)
							}
						}
					}
				}
				for sIdx, expStatus := range want.StepStatusMap {
					if sIdx < len(got.Steps) {
						if got.Steps[sIdx].Status != expStatus {
							t.Errorf("dep[%d].Steps[%d].Status = %q, want %q", i, sIdx, got.Steps[sIdx].Status, expStatus)
						}
					} else {
						t.Errorf("dep[%d].Steps has len %d, cannot check index %d", i, len(got.Steps), sIdx)
					}
				}
				if want.MatrixJobCount > 0 && len(got.MatrixJobs) != want.MatrixJobCount {
					t.Errorf("dep[%d].MatrixJobs count = %d, want %d", i, len(got.MatrixJobs), want.MatrixJobCount)
				}
				if want.HasContainerChip {
					found := false
					for _, c := range got.MatrixJobs {
						if c.Type == "container" {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("dep[%d] expected container chip, got %+v", i, got.MatrixJobs)
					}
				}
				if want.HasCIChip {
					found := false
					for _, c := range got.MatrixJobs {
						if c.Type == "ci" {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("dep[%d] expected CI chip, got %+v", i, got.MatrixJobs)
					}
				}
				if want.CheckCommitTime != nil {
					if got.CommitTime == nil || !got.CommitTime.Equal(*want.CheckCommitTime) {
						t.Errorf("dep[%d].CommitTime = %v, want %v", i, got.CommitTime, *want.CheckCommitTime)
					}
				}
			}
			if tt.wantRepeats > 0 {
				for iter := 0; iter < tt.wantRepeats; iter++ {
					iterDeps := MergeClusterDeploymentsWithHangar(tt.runs, tt.jobs, tt.gitSync, tt.containers, tt.currentCommit, ref)
					if len(iterDeps) != len(tt.wantList) {
						t.Fatalf("iteration %d: expected %d deployments, got %d", iter, len(tt.wantList), len(iterDeps))
					}
					for idx, want := range tt.wantList {
						if iterDeps[idx].Stage != want.Stage {
							t.Fatalf("iteration %d at index %d: expected stage %q, got %q", iter, idx, want.Stage, iterDeps[idx].Stage)
						}
					}
				}
			}
		})
	}
}

func TestExtractSingleContainerCommit_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		container DockerContainerJSON
		want      string
	}{
		{name: "nil_labels", container: mkLabeledCont("", "", nil), want: ""},
		{name: "empty_labels", container: mkLabeledCont("", "", map[string]string{}), want: ""},
		{name: "opencontainers_revision_priority", container: mkLabeledCont("", "", map[string]string{"org.opencontainers.image.revision": "1111111111111111111111111111111111111111", "aerial.commit_sha": "2222222222222222222222222222222222222222", "vcs-ref": "3333333333333333333333333333333333333333"}), want: "1111111111111111111111111111111111111111"},
		{name: "aerial_commit_sha_fallback", container: mkLabeledCont("", "", map[string]string{"aerial.commit_sha": "2222222222222222222222222222222222222222", "vcs-ref": "3333333333333333333333333333333333333333"}), want: "2222222222222222222222222222222222222222"},
		{name: "vcs_ref_fallback", container: mkLabeledCont("", "", map[string]string{"vcs-ref": "3333333333333333333333333333333333333333"}), want: "3333333333333333333333333333333333333333"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractSingleContainerCommit(tt.container)
			if got != tt.want {
				t.Errorf("ExtractSingleContainerCommit() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsDeployOngoing_TableDriven(t *testing.T) {
	ongoingDep := DeploymentStatus{Stage: "pulling"}
	idleDep := DeploymentStatus{Stage: "live"}

	tests := []struct {
		name  string
		input any
		want  bool
	}{
		{name: "string_queued", input: "queued", want: true},
		{name: "string_building", input: "building", want: true},
		{name: "string_awaiting_pull", input: "awaiting_pull", want: true},
		{name: "string_pulling", input: "pulling", want: true},
		{name: "string_swapping", input: "swapping", want: true},
		{name: "string_failed", input: "failed", want: false},
		{name: "string_live", input: "live", want: false},
		{name: "string_unknown", input: "idle", want: false},
		{name: "dep_struct_ongoing", input: ongoingDep, want: true},
		{name: "dep_struct_idle", input: idleDep, want: false},
		{name: "dep_ptr_ongoing", input: &ongoingDep, want: true},
		{name: "dep_ptr_idle", input: &idleDep, want: false},
		{name: "dep_ptr_nil", input: (*DeploymentStatus)(nil), want: false},
		{name: "dep_slice_empty", input: []DeploymentStatus{}, want: false},
		{name: "dep_slice_has_ongoing", input: []DeploymentStatus{idleDep, ongoingDep}, want: true},
		{name: "dep_slice_all_idle", input: []DeploymentStatus{idleDep}, want: false},
		{name: "dep_ptr_slice_nil_elem", input: []*DeploymentStatus{nil}, want: false},
		{name: "dep_ptr_slice_has_ongoing", input: []*DeploymentStatus{&idleDep, &ongoingDep}, want: true},
		{name: "dep_ptr_slice_all_idle", input: []*DeploymentStatus{&idleDep}, want: false},
		{name: "unsupported_type", input: 12345, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsDeployOngoing(tt.input)
			if got != tt.want {
				t.Errorf("IsDeployOngoing(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestExtractAerialServiceName(t *testing.T) {
	tests := []struct {
		name        string
		container   DockerContainerJSON
		expectedSvc string
		expectedOk  bool
	}{
		{name: "Docker Compose container", container: mkLabeledCont("aerial-brain-1", "", map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}), expectedSvc: "brain", expectedOk: true},
		{name: "Foreign Docker Compose project container", container: mkLabeledCont("homeassistant-web-1", "", map[string]string{"com.docker.compose.project": "homeassistant", "com.docker.compose.service": "web"}), expectedSvc: "", expectedOk: false},
		{name: "Nomad container with aerial.service label", container: mkLabeledCont("scheduler-mcp-6f0e3a1d-8af5-f530-2ca8-eee83dfac17e", "", map[string]string{"aerial.service": "scheduler-mcp", "com.hashicorp.nomad.alloc_id": "6f0e3a1d-8af5-f530-2ca8-eee83dfac17e"}), expectedSvc: "scheduler-mcp", expectedOk: true},
		{name: "Nomad container with alloc_id in name", container: mkLabeledCont("victoriametrics-mcp-6ca68da8-1599-0516-4c28-635658025294", "", map[string]string{"com.hashicorp.nomad.alloc_id": "6ca68da8-1599-0516-4c28-635658025294"}), expectedSvc: "victoriametrics-mcp", expectedOk: true},
		{name: "Legacy container with aerial- prefix", container: mkCont("aerial-proxy", "", "", "", "", 0), expectedSvc: "proxy", expectedOk: true},
		{name: "Host supervisor container (non-Aerial)", container: mkLabeledCont("hassio_supervisor", "", map[string]string{"io.hass.type": "supervisor"}), expectedSvc: "", expectedOk: false},
		{name: "Host addon container (non-Aerial)", container: mkLabeledCont("app_core_mosquitto", "", map[string]string{"supervisor_managed": ""}), expectedSvc: "", expectedOk: false},
		{name: "Empty container metadata", container: mkLabeledCont("", "running", nil), expectedSvc: "", expectedOk: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, ok := ExtractAerialServiceName(tc.container)
			if ok != tc.expectedOk {
				t.Errorf("expected ok=%v, got %v", tc.expectedOk, ok)
			}
			if svc != tc.expectedSvc {
				t.Errorf("expected svc=%q, got %q", tc.expectedSvc, svc)
			}
		})
	}
}

func TestParseNomadClusterState(t *testing.T) {
	now := testRefTime
	boolPtr := func(b bool) *bool { return &b }
	mustJSON := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("failed to marshal JSON: %v", err)
		}
		return b
	}

	tests := []struct {
		name         string
		jobsData     []byte
		allocsData   []byte
		now          time.Time
		wantErr      bool
		wantCount    int
		wantServices []ServiceStatus
	}{
		{
			name:       "healthy services with deployment",
			jobsData:   mustJSON([]NomadJobItem{mkNomadJob("brain", "", "service", "running", "", 0)}),
			allocsData: mustJSON([]NomadAllocItem{mkNomadAlloc("alloc-brain", "brain", "running", -60*time.Second, boolPtr(true), nil)}),
			now:        now,
			wantCount:  1,
			wantServices: []ServiceStatus{
				{Name: "brain", Status: "healthy", UptimeSeconds: 60},
			},
		},
		{
			name:       "starting service with deployment healthy=false and running task",
			jobsData:   mustJSON([]NomadJobItem{mkNomadJob("hangar", "", "service", "running", "", 0)}),
			allocsData: mustJSON([]NomadAllocItem{mkNomadAlloc("alloc-hangar", "hangar", "running", -10*time.Second, boolPtr(false), map[string]NomadTaskState{"task1": {State: "running"}})}),
			now:        now,
			wantCount:  1,
			wantServices: []ServiceStatus{
				{Status: "starting", UptimeSeconds: 10},
			},
		},
		{
			name:       "starting service with deployment healthy=false and pending task",
			jobsData:   mustJSON([]NomadJobItem{mkNomadJob("hangar", "", "service", "running", "", 0)}),
			allocsData: mustJSON([]NomadAllocItem{mkNomadAlloc("alloc-hangar", "hangar", "running", -10*time.Second, boolPtr(false), map[string]NomadTaskState{"task1": {State: "pending"}})}),
			now:        now,
			wantCount:  1,
			wantServices: []ServiceStatus{
				{Status: "starting"},
			},
		},
		{
			name:       "unhealthy service with dead task",
			jobsData:   mustJSON([]NomadJobItem{mkNomadJob("hangar", "", "service", "running", "", 0)}),
			allocsData: mustJSON([]NomadAllocItem{mkNomadAlloc("alloc-hangar", "hangar", "running", -10*time.Second, boolPtr(false), map[string]NomadTaskState{"task1": {State: "dead"}})}),
			now:        now,
			wantCount:  1,
			wantServices: []ServiceStatus{
				{Status: "unhealthy"},
			},
		},
		{
			name:       "unhealthy service with failed task or no allocs",
			jobsData:   mustJSON([]NomadJobItem{mkNomadJob("svc-failed", "", "service", "running", "", 0), mkNomadJob("svc-no-alloc", "", "service", "dead", "", 0)}),
			allocsData: mustJSON([]NomadAllocItem{mkNomadAlloc("alloc-failed", "svc-failed", "running", -30*time.Second, nil, map[string]NomadTaskState{"task1": {State: "running", Failed: true}})}),
			now:        now,
			wantCount:  2,
			wantServices: []ServiceStatus{
				{Name: "svc-failed", Status: "unhealthy"},
				{Name: "svc-no-alloc", Status: "unhealthy"},
			},
		},
		{
			name:     "nil deployment status with healthy and unhealthy tasks",
			jobsData: mustJSON([]NomadJobItem{mkNomadJob("proxy", "", "service", "running", "", 0), mkNomadJob("gateway", "", "service", "running", "", 0)}),
			allocsData: mustJSON([]NomadAllocItem{
				mkNomadAlloc("alloc-proxy", "proxy", "running", -100*time.Second, nil, map[string]NomadTaskState{"proxy-task": {State: "running"}}),
				mkNomadAlloc("alloc-gateway", "gateway", "running", -100*time.Second, nil, map[string]NomadTaskState{"gateway-task": {State: "pending"}}),
			}),
			now:       now,
			wantCount: 2,
			wantServices: []ServiceStatus{
				{Name: "gateway", Status: "unhealthy"},
				{Name: "proxy", Status: "healthy", UptimeSeconds: 100},
			},
		},
		{
			name: "no running alloc with pending status or starting group",
			jobsData: mustJSON([]NomadJobItem{
				mkNomadJob("pending-svc", "", "service", "pending", "", 0),
				mkNomadJob("starting-svc", "", "service", "running", "", 1),
			}),
			allocsData: []byte("[]"),
			now:        now,
			wantCount:  2,
			wantServices: []ServiceStatus{
				{Name: "pending-svc", Status: "starting"},
				{Name: "starting-svc", Status: "starting"},
			},
		},
		{
			name: "multiple running allocs selects highest CreateTime and filters batch",
			jobsData: mustJSON([]NomadJobItem{
				mkNomadJob("svc-multi", "", "service", "running", "", 0),
				mkNomadJob("batch-ignored", "", "batch", "running", "", 0),
				mkNomadJob("", "named-only", "service", "running", "", 0),
				mkNomadJob("", "", "service", "running", "", 0),
			}),
			allocsData: mustJSON([]NomadAllocItem{
				mkNomadAlloc("alloc-old", "svc-multi", "running", -200*time.Second, boolPtr(false), nil),
				mkNomadAlloc("alloc-new", "svc-multi", "running", -40*time.Second, boolPtr(true), nil),
				mkNomadAlloc("alloc-batch", "batch-ignored", "running", -10*time.Second, nil, nil),
				mkNomadAlloc("alloc-named", "named-only", "running", -15*time.Second, boolPtr(true), nil),
				mkNomadAlloc("alloc-nonrunning", "svc-multi", "complete", -5*time.Second, nil, nil),
			}),
			now:       time.Time{},
			wantCount: 2,
			wantServices: []ServiceStatus{
				{Name: "named-only", Status: "healthy"},
				{Name: "svc-multi", Status: "healthy"},
			},
		},
		{name: "malformed jobs JSON returns error", jobsData: []byte("not valid json"), allocsData: []byte("[]"), now: now, wantErr: true},
		{name: "malformed allocs JSON returns error", jobsData: []byte("[]"), allocsData: []byte("not valid json"), now: now, wantErr: true},
		{name: "empty payloads returns empty slice", jobsData: []byte("[]"), allocsData: []byte("[]"), now: now, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			services, err := ParseNomadClusterState(tt.jobsData, tt.allocsData, tt.now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseNomadClusterState() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(services) != tt.wantCount {
				t.Fatalf("ParseNomadClusterState() count = %d, want %d", len(services), tt.wantCount)
			}
			for i, want := range tt.wantServices {
				if want.Name != "" && services[i].Name != want.Name {
					t.Errorf("service[%d].Name = %q, want %q", i, services[i].Name, want.Name)
				}
				if want.Status != "" && services[i].Status != want.Status {
					t.Errorf("service[%d].Status = %q, want %q", i, services[i].Status, want.Status)
				}
				if want.UptimeSeconds > 0 && services[i].UptimeSeconds != want.UptimeSeconds {
					t.Errorf("service[%d].UptimeSeconds = %d, want %d", i, services[i].UptimeSeconds, want.UptimeSeconds)
				}
			}
		})
	}
}

func TestBuildDeploymentSteps(t *testing.T) {
	steps := buildDeploymentSteps("active", "pending", "failed", "completed", "active")
	if len(steps) != 5 {
		t.Fatalf("expected 5 steps, got %d", len(steps))
	}
	if steps[0].Status != "active" {
		t.Errorf("expected step 0 status active, got %s", steps[0].Status)
	}
}
