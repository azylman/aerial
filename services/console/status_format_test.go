package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var (
	testRefTime    = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	testCommitTime = testRefTime.Add(-10 * time.Minute)
)

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

func mkPRRun(id int64, repo, branch, sha, status, conclusion string, createdOff, updatedOff time.Duration) GitHubRun {
	r := mkRun(id, repo, sha, status, conclusion, "", createdOff, updatedOff)
	r.Event = "pull_request"
	r.HeadBranch = branch
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

// mkCont builds a container in the shape synthesizeContainersFromServices produces
// for a service without image metadata; the service defaults to the name minus "aerial-".
func mkCont(name, service, sha, state, health string, createdOff time.Duration) DockerContainerJSON {
	if service == "" {
		service = strings.TrimPrefix(name, "aerial-")
	}
	c := DockerContainerJSON{
		State:   state,
		Created: testRefTime.Add(createdOff).Unix(),
		Labels:  map[string]string{"aerial.service": service},
	}
	if sha != "" {
		c.Labels["aerial.commit_sha"] = sha
	}
	if health != "" {
		c.Health = &DockerContainerHealth{Status: health}
	}
	return c
}

func mkLabeledCont(state string, labels map[string]string) DockerContainerJSON {
	return DockerContainerJSON{State: state, Labels: labels}
}

func TestBuildFactsUpstreamURL_TableDriven(t *testing.T) {
	tests := []upstreamURLTestCase{
		{name: "empty base URL errors", baseURL: "", query: url.Values{}, wantErr: true},
		{name: "control characters in base URL error", baseURL: "http://brain:\x7f8080", query: url.Values{}, wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", query: url.Values{}, wantErr: true},
		{name: "trailing slash normalized", baseURL: "http://brain:8080/", query: url.Values{}, omitQuery: []string{"limit", "offset", "category", "q"}},
		{name: "nil query handled cleanly", baseURL: "http://brain:8080", query: nil},
		{name: "positive limit and offset forwarded", baseURL: "http://brain:8080", query: url.Values{"limit": {"25"}, "offset": {"10"}}, wantLimit: 25, wantOffset: 10, wantQuery: map[string]string{"limit": "25", "offset": "10"}},
		{name: "zero or negative limit and offset omitted", baseURL: "http://brain:8080", query: url.Values{"limit": {"0"}, "offset": {"0"}}, omitQuery: []string{"limit", "offset"}},
		{name: "negative or invalid limit and offset omitted", baseURL: "http://brain:8080", query: url.Values{"limit": {"-5"}, "offset": {"invalid"}}, omitQuery: []string{"limit", "offset"}},
		{name: "q trimmed and forwarded while category is ignored", baseURL: "http://brain:8080", query: url.Values{"category": {"  system  "}, "q": {"  docker deploy  "}}, wantQuery: map[string]string{"q": "docker deploy"}, omitQuery: []string{"category"}},
		{name: "empty category and q omitted", baseURL: "http://brain:8080", query: url.Values{"category": {"   "}, "q": {"   "}}, omitQuery: []string{"category", "q"}},
		{name: "long ASCII q clamped to 64 runes", baseURL: "http://brain:8080", query: url.Values{"q": {strings.Repeat("a", 100)}}, wantQuery: map[string]string{"q": strings.Repeat("a", 64)}},
		{name: "multibyte unicode kanji clamped to 64 runes without UTF-8 corruption", baseURL: "http://brain:8080", query: url.Values{"q": {strings.Repeat("電", 70)}}, wantQuery: map[string]string{"q": strings.Repeat("電", 64)}},
		{name: "agent forwarded", baseURL: "http://brain:8080", query: url.Values{"agent": {"agent-alpha"}}, wantQuery: map[string]string{"agent": "agent-alpha"}},
		{name: "agent all omitted", baseURL: "http://brain:8080", query: url.Values{"agent": {"all"}}, omitQuery: []string{"agent"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBuildFactsUpstreamURLCase(t, tt)
		})
	}
}

type upstreamURLTestCase struct {
	name       string
	baseURL    string
	query      url.Values
	wantLimit  int
	wantOffset int
	wantErr    bool
	wantQuery  map[string]string
	omitQuery  []string
}

func runBuildFactsUpstreamURLCase(t *testing.T, tt upstreamURLTestCase) {
	t.Helper()
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
}

func TestBuildSchedulesUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		query   url.Values
		wantURL string
		wantErr bool
	}{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control character returns error", baseURL: "http://brain:\x7f8080", wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", wantErr: true},
		{name: "clean base URL without trailing slash", baseURL: "http://brain:8080", wantURL: "http://brain:8080/schedules"},
		{name: "base URL with trailing slash normalized", baseURL: "http://brain:8080/", wantURL: "http://brain:8080/schedules"},
		{name: "agent forwarded", baseURL: "http://brain:8080", query: url.Values{"agent": {"agent-alpha"}}, wantURL: "http://brain:8080/schedules?agent=agent-alpha"},
		{name: "agent all omitted", baseURL: "http://brain:8080", query: url.Values{"agent": {"all"}}, wantURL: "http://brain:8080/schedules"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, err := BuildSchedulesUpstreamURL(tt.baseURL, tt.query)
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
	tests := []upstreamURLTestCase{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control characters in URL return error", baseURL: "http://brain:\x7f8080", wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", wantErr: true},
		{name: "defaults applied when query is empty", baseURL: "http://brain:8080/", query: url.Values{}, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
		{name: "nil query receives defaults", baseURL: "http://brain:8080", query: nil, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
		{name: "valid limit and offset within boundaries", baseURL: "http://brain:8080", query: url.Values{"limit": {"100"}, "offset": {"20"}, "schedule_id": {"  cron-test  "}, "status": {"  success  "}}, wantLimit: 100, wantOffset: 20, wantQuery: map[string]string{"limit": "100", "offset": "20", "schedule_id": "cron-test", "status": "success"}},
		{name: "limit > 100 falls back to default 50", baseURL: "http://brain:8080", query: url.Values{"limit": {"150"}, "offset": {"-5"}}, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
		{name: "invalid non-integer limit and offset fall back to defaults", baseURL: "http://brain:8080", query: url.Values{"limit": {"abc"}, "offset": {"xyz"}}, wantLimit: 50, wantQuery: map[string]string{"limit": "50", "offset": "0"}},
		{name: "agent forwarded", baseURL: "http://brain:8080", query: url.Values{"agent": {"agent-alpha"}, "limit": {"10"}, "offset": {"0"}}, wantLimit: 10, wantQuery: map[string]string{"agent": "agent-alpha", "limit": "10"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBuildScheduleRunsCase(t, tt)
		})
	}
}

func runBuildScheduleRunsCase(t *testing.T, tt upstreamURLTestCase) {
	t.Helper()
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
}

func TestBuildVanadisBuildsUpstreamURL_TableDriven(t *testing.T) {
	tests := []upstreamURLTestCase{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control characters in URL return error", baseURL: "http://vanadis:\x7f8080", wantErr: true},
		{name: "defaults applied when query is empty", baseURL: "http://vanadis.aerial/", query: url.Values{}, wantLimit: 50, wantQuery: map[string]string{"limit": "50"}},
		{name: "nil query receives default limit 50", baseURL: "http://vanadis.aerial", query: nil, wantLimit: 50, wantQuery: map[string]string{"limit": "50"}},
		{name: "valid limit and query params within boundaries", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"25"}, "status": {"  running  "}, "queue_state": {"  running  "}}, wantLimit: 25, wantQuery: map[string]string{"limit": "25", "status": "running", "queue_state": "running"}},
		{name: "limit > 100 clamped to 100", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"200"}}, wantLimit: 100, wantQuery: map[string]string{"limit": "100"}},
		{name: "limit < 1 clamped to 1", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"0"}}, wantLimit: 1, wantQuery: map[string]string{"limit": "1"}},
		{name: "negative limit clamped to 1", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"-10"}}, wantLimit: 1, wantQuery: map[string]string{"limit": "1"}},
		{name: "invalid non-integer limit falls back to default 50", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"invalid"}}, wantLimit: 50, wantQuery: map[string]string{"limit": "50"}},
		{name: "agent forwarded", baseURL: "http://vanadis:4025", query: url.Values{"agent": {"agent-alpha"}, "limit": {"10"}}, wantLimit: 10, wantQuery: map[string]string{"agent": "agent-alpha", "limit": "10"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBuildVanadisBuildsCase(t, tt)
		})
	}
}

func runBuildVanadisBuildsCase(t *testing.T, tt upstreamURLTestCase) {
	t.Helper()
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
		{name: "valid /dashboard/api/builds/ prefix with trailing slash baseURL", baseURL: "http://vanadis.aerial/", requestPath: "/console/api/builds/build-1792214230", wantID: "build-1792214230", wantURL: "http://vanadis.aerial/builds/build-1792214230"},
		{name: "missing expected prefix returns error", baseURL: "http://vanadis.aerial", requestPath: "/api/other/build-123", wantErr: true},
		{name: "empty build ID on /api/builds/ returns error", baseURL: "http://vanadis.aerial", requestPath: "/api/builds/", wantErr: true},
		{name: "empty build ID on /dashboard/api/builds/ returns error", baseURL: "http://vanadis.aerial", requestPath: "/console/api/builds/", wantErr: true},
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
		{"nil services returns healthy", nil, "healthy"},
		{"empty services returns healthy", []ServiceStatus{}, "healthy"},
		{"all services healthy returns healthy", []ServiceStatus{{Name: "brain", Status: "healthy"}, {Name: "dashboard", Status: "healthy"}}, "healthy"},
		{"at least one unhealthy service returns degraded", []ServiceStatus{{Name: "brain", Status: "healthy"}, {Name: "dashboard", Status: "unhealthy"}}, "degraded"},
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

func TestExtractServiceNameFromJobName_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		jobName  string
		expected string
	}{
		{name: "brain service", jobName: "Build & Push brain (linux/amd64)", expected: "brain"},
		{name: "console service", jobName: "Build & Push console", expected: "console"},
		{name: "proxy service", jobName: "test-proxy-service", expected: "proxy"},
		{name: "haro service", jobName: "haro CI", expected: "haro"},
		{name: "coven service", jobName: "Build & Push coven", expected: "coven"},
		{name: "vanadis service", jobName: "Build & Push vanadis", expected: "vanadis"},
		{name: "github-runner service", jobName: "github-runner CI", expected: "github-runner"},
		{name: "eri service build & push", jobName: "Build & Push eri", expected: "eri"},
		{name: "eri service build parentheses", jobName: "build (eri)", expected: "eri"},
		{name: "discord mcp", jobName: "discord-mcp Tests", expected: "discord-mcp"},
		{name: "kernel mcp", jobName: "kernel-mcp Tests", expected: "kernel-mcp"},
		{name: "github mcp", jobName: "github-mcp Deploy", expected: "github-mcp"},
		{name: "infisical mcp", jobName: "infisical-mcp Build", expected: "infisical-mcp"},
		{name: "synapse service", jobName: "Build & Push synapse", expected: "synapse"},
		{name: "ollama service", jobName: "Setup ollama image", expected: "ollama"},
		{name: "agentsview service", jobName: "agentsview container", expected: "agentsview"},
		{name: "unit test generic", jobName: "Unit Test Matrix", expected: "unit-tests"},
		{name: "test generic", jobName: "Service Integration Test", expected: "unit-tests"},
		{name: "lint job", jobName: "GolangCI Lint Run", expected: "lint"},
		{name: "validate-config job", jobName: "validate-config", expected: "config"},
		{name: "Validate Configuration job", jobName: "Validate Configuration", expected: "config"},
		{name: "parentheses sidecar aura-farming", jobName: "Build & Push Sidecar Images to GHCR (aura-farming)", expected: "aura-farming"},
		{name: "parentheses sidecar banana", jobName: "Build & Push Sidecar Images to GHCR (banana)", expected: "banana"},
		{name: "docs service", jobName: "Build docs", expected: "docs"},
		{name: "unrelated job", jobName: "Cleanup Stale Artifacts", expected: ""},
		{name: "verify does not match eri", jobName: "verify", expected: ""},
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
		{name: "completed success and failure with deduplication", jobs: []GitHubJob{mkJob(1, 10, "console-test", "completed", "success", -120*time.Second, -60*time.Second), mkJob(2, 10, "console-duplicate", "completed", "failure", 0, 0), mkJob(3, 10, "haro", "completed", "failure", 0, 0), mkJob(4, 10, "discord-mcp", "completed", "cancelled", 0, 0)}, now: testRefTime, wantChips: []MatrixJobChip{{Name: "console", Status: "completed", Duration: "60s"}, {Name: "haro", Status: "failed"}, {Name: "discord-mcp", Status: "pending"}}},
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
		{name: "zero or future creation time defaults to 0s duration", containers: []DockerContainerJSON{mkLabeledCont("running", map[string]string{"aerial.service": "brain"}), mkCont("aerial-dashboard", "", "", "running", "", 10*time.Minute)}, now: testRefTime, wantChips: []MatrixJobChip{{Duration: "0s"}, {Duration: "0s"}}},
		{name: "health status active for starting, failed for unhealthy or not running", containers: []DockerContainerJSON{mkCont("aerial-brain", "", "", "running", "starting", -120*time.Second), mkCont("aerial-proxy", "", "", "exited", "", -300*time.Second), mkCont("aerial-haro", "", "", "running", "unhealthy", -600*time.Second)}, now: testRefTime, wantChips: []MatrixJobChip{{Status: "active", Duration: "2m"}, {Status: "failed", Conclusion: "failure"}, {Status: "failed", Conclusion: "failure"}}},
		{name: "deduplicates running brain containers with future and current created", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", 1*time.Hour), mkCont("aerial-brain", "brain", "", "running", "", 0)}, now: testRefTime, wantChips: []MatrixJobChip{{Name: "brain", Duration: "0s"}}},
		{name: "containers without an aerial.service label are skipped", containers: []DockerContainerJSON{
			mkLabeledCont("running", map[string]string{"aerial.service": "brain"}),
			mkLabeledCont("running", nil),
		}, now: testRefTime, wantChips: []MatrixJobChip{{Name: "brain", Status: "completed", Conclusion: "success"}}},
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
		{path: "manifest.webmanifest", wantSubstr: "application/manifest+json"},
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
		want        string
	}{
		{"empty deployments returns idle", []DeploymentStatus{}, "idle"},
		{"live completed deployment returns idle", []DeploymentStatus{{Stage: "live", Progress: 100}}, "idle"},
		{"queued deployment returns queued", []DeploymentStatus{{Stage: "queued", Progress: 15}}, "queued"},
		{"building deployment returns building", []DeploymentStatus{{Stage: "building", Progress: 45}}, "building"},
		{"awaiting_pull deployment returns awaiting_pull", []DeploymentStatus{{Stage: "awaiting_pull", Progress: 50}}, "awaiting_pull"},
		{"swapping deployment returns swapping", []DeploymentStatus{{Stage: "swapping", Progress: 75}}, "swapping"},
		{"pulling deployment returns pulling", []DeploymentStatus{{Stage: "pulling", Progress: 55}}, "pulling"},
		{"precedence: pulling overrides building", []DeploymentStatus{{Stage: "building"}, {Stage: "pulling"}}, "pulling"},
		{"failed deployment returns failed", []DeploymentStatus{{Stage: "failed", Progress: 0}}, "failed"},
		{"degraded deployment returns degraded", []DeploymentStatus{{Stage: "degraded", Progress: 85}}, "degraded"},
		{"precedence: swapping overrides building", []DeploymentStatus{{Stage: "building", Progress: 45}, {Stage: "swapping", Progress: 75}}, "swapping"},
		{"precedence: building overrides queued and failed", []DeploymentStatus{{Stage: "queued"}, {Stage: "failed"}, {Stage: "building"}}, "building"},
		{"precedence: failed overrides degraded", []DeploymentStatus{{Stage: "degraded", Progress: 85}, {Stage: "failed", Progress: 0}}, "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateDeployStatus(tt.deployments); got != tt.want {
				t.Errorf("CalculateDeployStatus() = %q, want %q", got, tt.want)
			}
		})
	}
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

func TestMergeClusterDeployments_PureTableDriven(t *testing.T) {
	t.Parallel()
	tests := make([]mergeClusterTestCase, 0, 70)
	tests = append(tests, mergeClusterBatch1Cases()...)
	tests = append(tests, mergeClusterBatch2Cases()...)
	tests = append(tests, mergeClusterBatch3Cases()...)
	tests = append(tests, mergeClusterBatch4Cases()...)
	tests = append(tests, mergeClusterBatch5Cases()...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runMergeClusterDeploymentsCase(t, tt, testRefTime)
		})
	}
}

func mergeClusterBatch1Cases() []mergeClusterTestCase {
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
	sortContainers := []DockerContainerJSON{
		mkCont("proxy", "proxy", "1111111111111111111111111111111111111111", "running", "", -40*time.Second),
		mkCont("brain", "brain", "6666666666666666666666666666666666666666", "running", "", -300*time.Second),
	}

	return []mergeClusterTestCase{
		// 2. AwaitingPullBridge
		{name: "AwaitingPullBridge", runs: mkRuns(mkRun(301, "", "cccccccccccccccccccccccccccccccccccccccc", "completed", "success", "chore: updated docs", -10*time.Minute, -5*time.Minute)), currentCommit: "oldcommit", wantList: []wantDep{{Commit: "ccccccc", Stage: "awaiting_pull", StepStatusMap: map[int]string{1: "completed", 2: "active"}}}},

		// 3. DeterministicSort
		{name: "DeterministicSort", runs: sortRuns, containers: sortContainers, currentCommit: "6666666", wantCount: 4, wantList: []wantDep{{Stage: "swapping"}, {Stage: "building"}, {Stage: "awaiting_pull"}, {Stage: "queued"}}, wantRepeats: 10},

		// 4. MultiCommitContainerGrouping
		{name: "MultiCommitContainerGrouping", runs: mkRuns(mkRun(1, "", "aaaaaaa123456789", "completed", "success", "", -10*time.Minute, -5*time.Minute), mkRun(2, "", "bbbbbbb123456789", "completed", "success", "", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "aaaaaaa123456789", "running", "", -300*time.Second), mkCont("aerial-proxy", "proxy", "bbbbbbb123456789", "running", "", -30*time.Second)}, currentCommit: "aaaaaaa", wantList: []wantDep{{Commit: "bbbbbbb", Stage: "swapping"}, {Commit: "aaaaaaa", Stage: "live"}}},

		// 5. ChipScoping
		{name: "ChipScoping", runs: mkRuns(mkRun(401, "", "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "in_progress", "", "", -2*time.Minute, -1*time.Minute)), jobs: map[int64][]GitHubJob{401: {mkJob(1, 401, "build (brain)", "in_progress", "", -90*time.Second, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "running", "", -30*time.Second)}, currentCommit: "7777777", wantList: []wantDep{{HasCIChip: true, HasContainerChip: true}}},

		// 6. SafeNilHandling
		{name: "SafeNilHandling", runs: mkRuns(mkRun(501, "", "", "queued", "", "", -10*time.Second, 0), mkRun(502, "", "abc", "in_progress", "", "", -20*time.Second, 0), mkRun(503, "", "not-a-valid-hex-sha!@#$%^&*()", "completed", "failure", "", -30*time.Second, -25*time.Second)), wantCount: 3},

		// 7. CardinalityClamp
		{name: "CardinalityClamp", runs: cardinalityRuns, wantCount: 5},

		// 9. DegradedAndFailedBranches_DegradedContainer
		{name: "DegradedAndFailedBranches_DegradedContainer", runs: mkRuns(mkRun(801, "", "8888888888888888888888888888888888888888", "completed", "success", "", -5*time.Minute, -2*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-dashboard", "", "8888888888888888888888888888888888888888", "exited", "", -60*time.Second)}, wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{4: "failed"}}}},

		// 12. MultiRunSameCommitAndSortTies_SameCommit
		{name: "MultiRunSameCommitAndSortTies_SameCommit", runs: mkRuns(mkRun(111, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "completed", "success", "", -10*time.Minute, -5*time.Minute), mkRun(112, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "in_progress", "", "", -1*time.Minute, -30*time.Second)), wantList: []wantDep{{Stage: "building"}}},

		// 13. MultiRunSameCommitAndSortTies_SortTiesLexicographical
		{name: "MultiRunSameCommitAndSortTies_SortTiesLexicographical", runs: mkRuns(mkRun(201, "", "ffffffffffffffffffffffffffffffffffffffff", "in_progress", "", "", -2*time.Minute, -1*time.Minute), mkRun(202, "", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "in_progress", "", "", -2*time.Minute, -1*time.Minute)), wantList: []wantDep{{Commit: "eeeeeee"}, {Commit: "fffffff"}}},

		// 14. MultiRunSameCommitAndSortTies_FailedPrecedesDegraded
		{name: "MultiRunSameCommitAndSortTies_FailedPrecedesDegraded", runs: mkRuns(mkRun(301, "", "1111111111111111111111111111111111111111", "completed", "failure", "", -5*time.Minute, -5*time.Minute), mkRun(302, "", "2222222222222222222222222222222222222222", "completed", "success", "", -5*time.Minute, -5*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-proxy", "", "2222222222222222222222222222222222222222", "exited", "", -5*time.Minute)}, wantList: []wantDep{{Stage: "failed"}, {Stage: "degraded"}}},

		// 15. MultiRunSameCommitAndSortTies_ExpiredIgnored
		{name: "MultiRunSameCommitAndSortTies_ExpiredIgnored", runs: mkRuns(mkRun(401, "", "3333333333333333333333333333333333333333", "completed", "success", "", -40*time.Minute, -35*time.Minute)), wantCount: 0},

		// 16. UnparentedContainersDoNotSpawnCards
		{name: "UnparentedContainersDoNotSpawnCards", containers: []DockerContainerJSON{mkLabeledCont("running", map[string]string{"aerial.service": "postgres"}), mkLabeledCont("exited", map[string]string{"aerial.service": "vector"})}, currentCommit: "livecommit", wantCount: 0},

		// 17. ActiveCIRun
		{name: "ActiveCIRun", runs: mkRuns(mkRun(481, "", "7a9f1b234567", "in_progress", "", "feat(brain): live github actions tracking", -2*time.Minute, -10*time.Second)), jobs: map[int64][]GitHubJob{481: {mkJob(1, 481, "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)", "in_progress", "", -40*time.Second, 0), mkJob(2, 481, "Build & Push Images to GHCR (dashboard, ./dashboard, ./dashboard/Dockerfile, aerial-dashboard)", "completed", "success", -60*time.Second, -10*time.Second)}}, currentCommit: "7a9f1b2", wantList: []wantDep{{Stage: "building", Commit: "7a9f1b2", MatrixJobCount: 2, HTMLURL: "https://github.com/azylman/aerial/actions/runs/481"}}},

		// 18. FailedCIRun
		{name: "FailedCIRun", runs: mkRuns(mkRun(482, "", "abc1234567", "completed", "failure", "", -5*time.Minute, -4*time.Minute)), currentCommit: "abc1234", wantList: []wantDep{{Stage: "failed", StepStatusMap: map[int]string{1: "failed"}}}},

		// 19. AwaitingPullUntilRolloutObserved
		{name: "AwaitingPullUntilRolloutObserved", containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, runs: mkRuns(mkRun(483, "", "def5678901", "completed", "success", "", -4*time.Minute, -45*time.Second)), jobs: map[int64][]GitHubJob{483: {mkJob(0, 483, "Build & Push Images to GHCR (brain)", "completed", "success", 0, 0)}}, currentCommit: "old1234", wantList: []wantDep{{Stage: "awaiting_pull", StepStatusMap: map[int]string{2: "active"}}}},
	}
}

func mergeClusterBatch2Cases() []mergeClusterTestCase {
	return []mergeClusterTestCase{
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
		{name: "CommitTimeAcrossAllStages_Queued", runs: mkRuns(mkRun(501, "", "1111aaa", "queued", "", "feat: queued", -2*time.Minute, 0)), currentCommit: "1111aaa", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 29. CommitTimeAcrossAllStages_Building
		{name: "CommitTimeAcrossAllStages_Building", runs: mkRuns(mkRun(502, "", "2222bbb", "in_progress", "", "feat: building", -1*time.Minute, 0)), currentCommit: "2222bbb", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 30. CommitTimeAcrossAllStages_Failed
		{name: "CommitTimeAcrossAllStages_Failed", runs: mkRuns(mkRun(503, "", "3333ccc", "completed", "failure", "feat: failed", -5*time.Minute, -4*time.Minute)), currentCommit: "3333ccc", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 31. CommitTimeAcrossAllStages_Swapping
		{name: "CommitTimeAcrossAllStages_Swapping", runs: mkRuns(mkRun(504, "", "4444ddd", "completed", "success", "feat: success", -2*time.Minute, -1*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "starting", -30*time.Second)}, currentCommit: "4444ddd", wantList: []wantDep{{CheckCommitTime: &testCommitTime}}},

		// 32. EmptyAndFuture_Empty
		{name: "EmptyAndFuture_Empty", currentCommit: "commit123", wantCount: 0},

		// 33. EmptyAndFuture_FutureContainers
		{name: "EmptyAndFuture_FutureContainers", runs: mkRuns(mkRun(123, "", "sha1234567890", "completed", "success", "", -10*time.Minute, 0)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", 2*time.Hour)}, wantCount: 1},

		// 37. UnhealthyContainerDegradesCard
		{name: "UnhealthyContainerDegradesCard", runs: mkRuns(mkRun(105, "", "commit123", "completed", "success", "", -15*time.Minute, -12*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "unhealthy", -10*time.Minute)}, currentCommit: "commit123", wantList: []wantDep{{Stage: "degraded", StepStatusMap: map[int]string{3: "completed", 4: "failed"}}}},

		// 38. MultiRepoActiveFanout
		{name: "MultiRepoActiveFanout", runs: mkRuns(mkRun(101, "azylman/aerial", "1111aaa", "in_progress", "", "", -2*time.Minute, 0), mkRun(201, "azylman/aerial-sidecars", "2222bbb", "in_progress", "", "", -1*time.Minute, 0)), currentCommit: "0000000", wantList: []wantDep{{Repository: "azylman/aerial-sidecars", Service: "aerial-sidecars", Stage: "building"}, {Repository: "azylman/aerial", Service: "aerial-stack", Stage: "building"}}},
	}
}

func mergeClusterBatch3Cases() []mergeClusterTestCase {
	return []mergeClusterTestCase{
		// 39. NonContainerRepoNoTimeout
		{name: "NonContainerRepoNoTimeout", runs: mkRuns(mkRun(301, "azylman/aerial-config", "3333ccc", "completed", "success", "", -5*time.Minute, -150*time.Second)), containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, currentCommit: "oldcorecommit", wantList: []wantDep{{Service: "aerial-config", Stage: "awaiting_pull"}}},

		// 40. ActivePRRunShowsBuilding
		{name: "ActivePRRunShowsBuilding", runs: mkRuns(mkPRRun(9001, "azylman/aerial-sidecars", "feat/sidecars-pr", "aabbccdd1122", "in_progress", "", -1*time.Minute, -30*time.Second)), jobs: map[int64][]GitHubJob{9001: {mkJob(0, 9001, "Run Sidecar Tests & Lint", "completed", "success", 0, 0), mkJob(0, 9001, "Build & Push Sidecar Images to GHCR (aura-farming)", "in_progress", "", 0, 0)}}, currentCommit: "0000000", wantList: []wantDep{{Repository: "azylman/aerial-sidecars", Stage: "building", MatrixJobCount: 2}}},

		// 41. CompletedPRRunDoesNotTimeout
		{name: "CompletedPRRunDoesNotTimeout", runs: mkRuns(mkPRRun(9002, "azylman/aerial-sidecars", "feat/sidecars-pr", "aabbccdd1122", "completed", "success", -300*time.Second, -200*time.Second)), jobs: map[int64][]GitHubJob{9002: {mkJob(0, 9002, "Build & Push Sidecar Images to GHCR (aura-farming)", "completed", "success", 0, 0)}}, containers: []DockerContainerJSON{mkCont("aerial-brain", "brain", "", "running", "", -10*time.Hour)}, currentCommit: "oldcorecommit", wantCount: 0},

		// 54. Prefix40CharSHA_MatchedBy7CharPrefixContainer
		{name: "Prefix40CharSHA_MatchedBy7CharPrefixContainer", runs: mkRuns(mkRun(888, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "in_progress", "", "", -3*time.Minute, 0)), containers: []DockerContainerJSON{mkCont("aerial-proxy", "proxy", "bbbbbbb", "running", "", -20*time.Second)}, wantList: []wantDep{{Commit: "bbbbbbb"}}},

		// 55. SortTieBreaker_FailedPrecedesDegraded_BothPositions
		{name: "SortTieBreaker_FailedPrecedesDegraded_BothPositions", runs: mkRuns(mkRun(901, "", "1111111111111111111111111111111111111111", "completed", "failure", "", -5*time.Minute, -5*time.Minute), mkRun(902, "", "2222222222222222222222222222222222222222", "completed", "success", "", -5*time.Minute, -5*time.Minute)), containers: []DockerContainerJSON{mkCont("aerial-brain", "", "2222222222222222222222222222222222222222", "exited", "", -5*time.Minute)}, wantList: []wantDep{{Stage: "failed", Commit: "1111111"}, {Stage: "degraded", Commit: "2222222"}}},
	}
}

type mergeClusterTestCase struct {
	name          string
	runs          []GitHubRun
	jobs          map[int64][]GitHubJob
	containers    []DockerContainerJSON
	currentCommit string
	refTime       time.Time
	useZeroTime   bool
	wantCount     int
	wantList      []wantDep
	wantRepeats   int
}

func runMergeClusterDeploymentsCase(t *testing.T, tt mergeClusterTestCase, defaultRefTime time.Time) {
	t.Helper()
	ref := tt.refTime
	if ref.IsZero() && !tt.useZeroTime {
		ref = defaultRefTime
	}
	deps := MergeClusterDeployments(tt.runs, tt.jobs, tt.containers, tt.currentCommit, ref)

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
		assertMergeClusterDeploymentItem(t, i, deps[i], want)
	}
	if tt.wantRepeats > 0 {
		assertMergeClusterDeploymentRepeats(t, tt, ref)
	}
}

func assertMergeClusterDeploymentItem(t *testing.T, i int, got DeploymentStatus, want wantDep) {
	t.Helper()
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
	assertMergeClusterDeploymentSteps(t, i, got, want)
	assertMergeClusterDeploymentChips(t, i, got, want)
	if want.CheckCommitTime != nil {
		if got.CommitTime == nil || !got.CommitTime.Equal(*want.CheckCommitTime) {
			t.Errorf("dep[%d].CommitTime = %v, want %v", i, got.CommitTime, *want.CheckCommitTime)
		}
	}
}

func assertMergeClusterDeploymentSteps(t *testing.T, i int, got DeploymentStatus, want wantDep) {
	t.Helper()
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
}

func assertMergeClusterDeploymentChips(t *testing.T, i int, got DeploymentStatus, want wantDep) {
	t.Helper()
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
}

func assertMergeClusterDeploymentRepeats(t *testing.T, tt mergeClusterTestCase, ref time.Time) {
	t.Helper()
	for iter := range tt.wantRepeats {
		iterDeps := MergeClusterDeployments(tt.runs, tt.jobs, tt.containers, tt.currentCommit, ref)
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

func TestBuildDocketTasksUpstreamURL_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		baseURL string
		query   url.Values
		wantURL string
		wantErr bool
	}{
		{name: "empty base URL", baseURL: "  ", wantErr: true},
		{name: "control character in base URL", baseURL: "http://docket:\x7f80", wantErr: true},
		{name: "nil query omits status", baseURL: "http://docket.local/", query: nil, wantURL: "http://docket.local/api/tasks"},
		{name: "status forwarded and other params dropped", baseURL: "http://docket.local", query: url.Values{"status": {" completed "}, "q": {"x"}, "limit": {"5"}}, wantURL: "http://docket.local/api/tasks?status=completed"},
		{name: "blank status omitted", baseURL: "http://docket.local", query: url.Values{"status": {"  "}}, wantURL: "http://docket.local/api/tasks"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := BuildDocketTasksUpstreamURL(tc.baseURL, tc.query)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.wantURL {
				t.Errorf("url = %q, want %q", got, tc.wantURL)
			}
		})
	}
}

type docketSearchTestCase struct {
	name           string
	baseURL        string
	query          url.Values
	wantURL        string
	wantErr        bool
	wantBadRequest bool
}

func TestBuildDocketSearchUpstreamURL_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []docketSearchTestCase{
		{name: "empty base URL", baseURL: "", query: url.Values{"q": {"x"}}, wantErr: true},
		{name: "missing q is a bad request", baseURL: "http://docket.local", query: url.Values{}, wantErr: true, wantBadRequest: true},
		{name: "whitespace q is a bad request", baseURL: "http://docket.local", query: url.Values{"q": {"   "}}, wantErr: true, wantBadRequest: true},
		{name: "q over 200 characters is a bad request", baseURL: "http://docket.local", query: url.Values{"q": {strings.Repeat("a", 201)}}, wantErr: true, wantBadRequest: true},
		{name: "q of exactly 200 multibyte characters accepted", baseURL: "http://docket.local", query: url.Values{"q": {strings.Repeat("é", 200)}}, wantURL: "http://docket.local/api/search?q=" + strings.Repeat("%C3%A9", 200)},
		{name: "q only", baseURL: "http://docket.local/", query: url.Values{"q": {" flaky test "}}, wantURL: "http://docket.local/api/search?q=flaky+test"},
		{name: "q status and limit forwarded, extras dropped", baseURL: "http://docket.local", query: url.Values{"q": {"auth"}, "status": {"all"}, "limit": {"10"}, "offset": {"3"}}, wantURL: "http://docket.local/api/search?limit=10&q=auth&status=all"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDocketSearchCase(t, tc)
		})
	}
}

func runDocketSearchCase(t *testing.T, tc docketSearchTestCase) {
	t.Helper()
	got, err := BuildDocketSearchUpstreamURL(tc.baseURL, tc.query)
	if (err != nil) != tc.wantErr {
		t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
	}
	if errors.Is(err, errInvalidDocketRequest) != tc.wantBadRequest {
		t.Errorf("bad-request classification = %v, want %v (err %v)", !tc.wantBadRequest, tc.wantBadRequest, err)
	}
	if got != tc.wantURL {
		t.Errorf("url = %q, want %q", got, tc.wantURL)
	}
}

type docketTaskDetailTestCase struct {
	name           string
	baseURL        string
	requestPath    string
	wantURL        string
	wantID         string
	wantErr        bool
	wantBadRequest bool
}

func TestBuildDocketTaskDetailUpstreamURL_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []docketTaskDetailTestCase{
		{name: "empty base URL", baseURL: "", requestPath: "/api/docket/tasks/7", wantErr: true},
		{name: "control character in base URL", baseURL: "http://docket:\x7f80", requestPath: "/api/docket/tasks/7", wantErr: true},
		{name: "bare prefix", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/42", wantURL: "http://docket.local/api/tasks/42", wantID: "42"},
		{name: "console prefix with trailing slash base", baseURL: "http://docket.local/", requestPath: "/console/api/docket/tasks/42", wantURL: "http://docket.local/api/tasks/42", wantID: "42"},
		{name: "leading zeros canonicalized", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/007", wantURL: "http://docket.local/api/tasks/7", wantID: "7"},
		{name: "unexpected prefix", baseURL: "http://docket.local", requestPath: "/api/builds/7", wantErr: true, wantBadRequest: true},
		{name: "empty id", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/", wantErr: true, wantBadRequest: true},
		{name: "zero id", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/0", wantErr: true, wantBadRequest: true},
		{name: "signed id", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/+7", wantErr: true, wantBadRequest: true},
		{name: "non-numeric id", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/abc", wantErr: true, wantBadRequest: true},
		{name: "nested path", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/7/comments", wantErr: true, wantBadRequest: true},
		{name: "overflowing id", baseURL: "http://docket.local", requestPath: "/api/docket/tasks/99999999999999999999", wantErr: true, wantBadRequest: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDocketTaskDetailCase(t, tc)
		})
	}
}

func runDocketTaskDetailCase(t *testing.T, tc docketTaskDetailTestCase) {
	t.Helper()
	gotURL, gotID, err := BuildDocketTaskDetailUpstreamURL(tc.baseURL, tc.requestPath)
	if (err != nil) != tc.wantErr {
		t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
	}
	if errors.Is(err, errInvalidDocketRequest) != tc.wantBadRequest {
		t.Errorf("bad-request classification = %v, want %v (err %v)", !tc.wantBadRequest, tc.wantBadRequest, err)
	}
	if gotURL != tc.wantURL || gotID != tc.wantID {
		t.Errorf("got (%q, %q), want (%q, %q)", gotURL, gotID, tc.wantURL, tc.wantID)
	}
}
