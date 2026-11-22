package config

import (
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBuildFactsUpstreamURL_TableDriven(t *testing.T) {
	const facts = "http://brain:8080/facts"
	tests := []struct {
		name       string
		baseURL    string
		query      url.Values
		wantURL    string
		wantLimit  int
		wantOffset int
		wantErr    bool
	}{
		{name: "empty base URL errors", baseURL: "", query: url.Values{}, wantErr: true},
		{name: "control characters in base URL error", baseURL: "http://brain:\x7f8080", query: url.Values{}, wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", query: url.Values{}, wantErr: true},
		{name: "trailing slash normalized", baseURL: "http://brain:8080/", query: url.Values{}, wantURL: facts},
		{name: "nil query handled cleanly", baseURL: "http://brain:8080", query: nil, wantURL: facts},
		{name: "positive limit and offset forwarded", baseURL: "http://brain:8080", query: url.Values{"limit": {"25"}, "offset": {"10"}}, wantURL: facts + "?limit=25&offset=10", wantLimit: 25, wantOffset: 10},
		{name: "zero or negative limit and offset omitted", baseURL: "http://brain:8080", query: url.Values{"limit": {"0"}, "offset": {"0"}}, wantURL: facts},
		{name: "negative or invalid limit and offset omitted", baseURL: "http://brain:8080", query: url.Values{"limit": {"-5"}, "offset": {"invalid"}}, wantURL: facts},
		{name: "q trimmed and forwarded while category is ignored", baseURL: "http://brain:8080", query: url.Values{"category": {"  system  "}, "q": {"  docker deploy  "}}, wantURL: facts + "?q=docker+deploy"},
		{name: "empty category and q omitted", baseURL: "http://brain:8080", query: url.Values{"category": {"   "}, "q": {"   "}}, wantURL: facts},
		{name: "long ASCII q clamped to 64 runes", baseURL: "http://brain:8080", query: url.Values{"q": {strings.Repeat("a", 100)}}, wantURL: facts + "?q=" + strings.Repeat("a", 64)},
		{name: "multibyte unicode kanji clamped to 64 runes without UTF-8 corruption", baseURL: "http://brain:8080", query: url.Values{"q": {strings.Repeat("電", 70)}}, wantURL: facts + "?q=" + strings.Repeat("%E9%9B%BB", 64)},
		{name: "agent forwarded", baseURL: "http://brain:8080", query: url.Values{"agent": {"agent-alpha"}}, wantURL: facts + "?agent=agent-alpha"},
		{name: "agent all omitted", baseURL: "http://brain:8080", query: url.Values{"agent": {"all"}}, wantURL: facts},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotLimit, gotOffset, err := BuildFactsUpstreamURL(tt.baseURL, tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildFactsUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotURL != tt.wantURL || gotLimit != tt.wantLimit || gotOffset != tt.wantOffset {
				t.Errorf("BuildFactsUpstreamURL() = (%q, %d, %d), want (%q, %d, %d)", gotURL, gotLimit, gotOffset, tt.wantURL, tt.wantLimit, tt.wantOffset)
			}
		})
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
	const runs = "http://brain:8080/schedules/runs"
	tests := []struct {
		name       string
		baseURL    string
		query      url.Values
		wantURL    string
		wantLimit  int
		wantOffset int
		wantErr    bool
	}{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control characters in URL return error", baseURL: "http://brain:\x7f8080", wantErr: true},
		{name: "invalid url parse input error", baseURL: "http://[::1]:namedport", wantErr: true},
		{name: "defaults applied when query is empty", baseURL: "http://brain:8080/", query: url.Values{}, wantURL: runs + "?limit=50&offset=0", wantLimit: 50},
		{name: "nil query receives defaults", baseURL: "http://brain:8080", query: nil, wantURL: runs + "?limit=50&offset=0", wantLimit: 50},
		{
			name: "valid limit and offset within boundaries", baseURL: "http://brain:8080",
			query:   url.Values{"limit": {"100"}, "offset": {"20"}, "schedule_id": {"  cron-test  "}, "status": {"  success  "}},
			wantURL: runs + "?limit=100&offset=20&schedule_id=cron-test&status=success", wantLimit: 100, wantOffset: 20,
		},
		{name: "limit > 100 falls back to default 50", baseURL: "http://brain:8080", query: url.Values{"limit": {"150"}, "offset": {"-5"}}, wantURL: runs + "?limit=50&offset=0", wantLimit: 50},
		{name: "invalid non-integer limit and offset fall back to defaults", baseURL: "http://brain:8080", query: url.Values{"limit": {"abc"}, "offset": {"xyz"}}, wantURL: runs + "?limit=50&offset=0", wantLimit: 50},
		{
			name: "agent forwarded", baseURL: "http://brain:8080", query: url.Values{"agent": {"agent-alpha"}, "limit": {"10"}, "offset": {"0"}},
			wantURL: runs + "?agent=agent-alpha&limit=10&offset=0", wantLimit: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotLimit, gotOffset, err := BuildScheduleRunsUpstreamURL(tt.baseURL, tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildScheduleRunsUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotURL != tt.wantURL || gotLimit != tt.wantLimit || gotOffset != tt.wantOffset {
				t.Errorf("BuildScheduleRunsUpstreamURL() = (%q, %d, %d), want (%q, %d, %d)", gotURL, gotLimit, gotOffset, tt.wantURL, tt.wantLimit, tt.wantOffset)
			}
		})
	}
}

func TestBuildVanadisBuildsUpstreamURL_TableDriven(t *testing.T) {
	const builds = "http://vanadis.aerial/builds"
	tests := []struct {
		name      string
		baseURL   string
		query     url.Values
		wantURL   string
		wantLimit int
		wantErr   bool
	}{
		{name: "empty base URL returns error", baseURL: "", wantErr: true},
		{name: "control characters in URL return error", baseURL: "http://vanadis:\x7f8080", wantErr: true},
		{name: "defaults applied when query is empty", baseURL: "http://vanadis.aerial/", query: url.Values{}, wantURL: builds + "?limit=50", wantLimit: 50},
		{name: "nil query receives default limit 50", baseURL: "http://vanadis.aerial", query: nil, wantURL: builds + "?limit=50", wantLimit: 50},
		{
			name: "valid limit and query params within boundaries", baseURL: "http://vanadis.aerial",
			query:   url.Values{"limit": {"25"}, "status": {"  running  "}, "queue_state": {"  running  "}},
			wantURL: builds + "?limit=25&queue_state=running&status=running", wantLimit: 25,
		},
		{name: "limit > 100 clamped to 100", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"200"}}, wantURL: builds + "?limit=100", wantLimit: 100},
		{name: "limit < 1 clamped to 1", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"0"}}, wantURL: builds + "?limit=1", wantLimit: 1},
		{name: "negative limit clamped to 1", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"-10"}}, wantURL: builds + "?limit=1", wantLimit: 1},
		{name: "invalid non-integer limit falls back to default 50", baseURL: "http://vanadis.aerial", query: url.Values{"limit": {"invalid"}}, wantURL: builds + "?limit=50", wantLimit: 50},
		{
			name: "agent forwarded", baseURL: "http://vanadis:4025", query: url.Values{"agent": {"agent-alpha"}, "limit": {"10"}},
			wantURL: "http://vanadis:4025/builds?agent=agent-alpha&limit=10", wantLimit: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotLimit, err := BuildVanadisBuildsUpstreamURL(tt.baseURL, tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildVanadisBuildsUpstreamURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotURL != tt.wantURL || gotLimit != tt.wantLimit {
				t.Errorf("BuildVanadisBuildsUpstreamURL() = (%q, %d), want (%q, %d)", gotURL, gotLimit, tt.wantURL, tt.wantLimit)
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

func TestNormalizeAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "clean lowercase slug", input: "agent-alpha", want: "agent-alpha"},
		{name: "uppercase and whitespace", input: "  AGENT-Alpha  ", want: "agent-alpha"},
		{name: "disallowed symbols stripped", input: "agent_alpha!@#$", want: "agentalpha"},
		{name: "numbers and hyphens kept", input: "agent-007-beta", want: "agent-007-beta"},
		{name: "empty input", input: "", want: ""},
		{name: "whitespace only", input: "   ", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeAgent(tc.input)
			if got != tc.want {
				t.Errorf("NormalizeAgent(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestMatchAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		itemAgent   string
		filterAgent string
		want        bool
	}{
		{name: "empty filter matches any", itemAgent: "agent-alpha", filterAgent: "", want: true},
		{name: "all filter matches any", itemAgent: "agent-alpha", filterAgent: "all", want: true},
		{name: "ALL uppercase matches any", itemAgent: "agent-alpha", filterAgent: "ALL", want: true},
		{name: "exact match", itemAgent: "agent-alpha", filterAgent: "agent-alpha", want: true},
		{name: "normalized case match", itemAgent: "Agent-Alpha", filterAgent: "agent-alpha", want: true},
		{name: "mismatch", itemAgent: "agent-alpha", filterAgent: "agent-beta", want: false},
		{name: "empty item defaults to aerial - aerial filter matches", itemAgent: "", filterAgent: "aerial", want: true},
		{name: "empty item defaults to aerial - non-aerial filter mismatches", itemAgent: "", filterAgent: "agent-alpha", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := MatchAgent(tc.itemAgent, tc.filterAgent)
			if got != tc.want {
				t.Errorf("MatchAgent(%q, %q) = %v, want %v", tc.itemAgent, tc.filterAgent, got, tc.want)
			}
		})
	}
}

func TestSortConfiguredAgents_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		agents []string
		want   []string
	}{
		{name: "nil returns aerial", agents: nil, want: []string{"aerial"}},
		{name: "empty returns aerial", agents: []string{}, want: []string{"aerial"}},
		{name: "aerial only", agents: []string{"aerial"}, want: []string{"aerial"}},
		{name: "aerial preserved first and others sorted alphabetically", agents: []string{"agent-beta", "aerial", "agent-alpha"}, want: []string{"aerial", "agent-alpha", "agent-beta"}},
		{name: "duplicates and all and empty skipped", agents: []string{"", "all", "ALL", "agent-alpha", "agent-alpha", "agent-beta"}, want: []string{"aerial", "agent-alpha", "agent-beta"}},
		{name: "missing aerial automatically prepended", agents: []string{"agent-beta", "agent-alpha"}, want: []string{"aerial", "agent-alpha", "agent-beta"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SortConfiguredAgents(tc.agents)
			if len(got) != len(tc.want) {
				t.Fatalf("SortConfiguredAgents(%v) len = %d, want %d: %v", tc.agents, len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("SortConfiguredAgents(%v)[%d] = %q, want %q", tc.agents, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestFilterByAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		filter    string
		itemAgent string
		wantKept  bool
	}{
		{name: "matching agent kept by every wrapper", filter: "agent-alpha", itemAgent: "agent-alpha", wantKept: true},
		{name: "other agent dropped by every wrapper", filter: "agent-alpha", itemAgent: "aerial", wantKept: false},
		{name: "all filter short-circuits every wrapper", filter: "all", itemAgent: "agent-alpha", wantKept: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wantLen := 0
			if tc.wantKept {
				wantLen = 1
			}

			if got := len(FilterFactsByAgent([]FactItem{{ID: 1, Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterFactsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterCronsByAgent([]CronSchedule{{ID: "c", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterCronsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterOneShotsByAgent([]OneShotSchedule{{ID: "o", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterOneShotsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterScheduleRunsByAgent([]ScheduleRun{{ID: "r", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterScheduleRunsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterBuildsByAgent([]VanadisBuildDTO{{ID: "b", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterBuildsByAgent len = %d, want %d", got, wantLen)
			}
			if got := len(FilterActiveTasksByAgent([]ActiveTaskStatus{{ID: "t", Agent: tc.itemAgent}}, tc.filter)); got != wantLen {
				t.Errorf("FilterActiveTasksByAgent len = %d, want %d", got, wantLen)
			}
		})
	}
}

func TestFactItemUnmarshalJSON_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		rawJSON   string
		wantAgent string
		wantText  string
	}{
		{name: "agent field present", rawJSON: `{"id":1,"fact_text":"dark mode","agent":"agent-alpha"}`, wantAgent: "agent-alpha", wantText: "dark mode"},
		{name: "source_agent fallback", rawJSON: `{"id":2,"fact_text":"voice setting","source_agent":"agent-beta"}`, wantAgent: "agent-beta", wantText: "voice setting"},
		{name: "agent wins", rawJSON: `{"id":3,"fact_text":"both","agent":"agent-alpha","source_agent":"agent-beta"}`, wantAgent: "agent-alpha", wantText: "both"},
		{name: "neither present leaves agent empty", rawJSON: `{"id":4,"fact_text":"neither"}`, wantText: "neither"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var item FactItem
			if err := json.Unmarshal([]byte(tc.rawJSON), &item); err != nil {
				t.Fatalf("json.Unmarshal error = %v", err)
			}
			if item.Agent != tc.wantAgent {
				t.Errorf("item.Agent = %q, want %q", item.Agent, tc.wantAgent)
			}
			if item.FactText != tc.wantText {
				t.Errorf("item.FactText = %q, want %q", item.FactText, tc.wantText)
			}
		})
	}
}

func TestFactsAPIResponseUnmarshalJSON_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		rawJSON   string
		wantTotal int
		wantAgent string
		wantErr   bool
	}{
		{
			name:      "valid response with facts",
			rawJSON:   `{"total":1,"facts":[{"id":1,"fact_text":"voice","source_agent":"agent-1"}]}`,
			wantTotal: 1,
			wantAgent: "agent-1",
		},
		{
			name:      "empty facts",
			rawJSON:   `{"total":0,"facts":[]}`,
			wantTotal: 0,
		},
		{
			name:    "invalid response json",
			rawJSON: `{"total":`,
			wantErr: true,
		},
		{
			name:    "invalid item json",
			rawJSON: `{"total":1,"facts":[{"id":"not-an-int"}]}`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var resp FactsAPIResponse
			err := json.Unmarshal([]byte(tc.rawJSON), &resp)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error = %v", err)
			}
			if resp.Total != tc.wantTotal {
				t.Errorf("Total = %d, want %d", resp.Total, tc.wantTotal)
			}
			if tc.wantAgent != "" {
				if len(resp.Facts) != 1 || resp.Facts[0].Agent != tc.wantAgent {
					t.Errorf("Facts[0].Agent = %q, want %q", resp.Facts[0].Agent, tc.wantAgent)
				}
			}
		})
	}
}

func TestAgentBrainURL_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		defaultURL   string
		defaultAgent string
		agent        string
		want         string
	}{
		{name: "default agent keeps configured host", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "aerial", want: "http://eri.aerial"},
		{name: "default agent match is normalized", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: " Aerial ", want: "http://eri.aerial"},
		{name: "sibling replaces first label", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "agent-alpha", want: "http://eri-agent-alpha.aerial"},
		{name: "sibling agent normalized", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "  Agent-Beta! ", want: "http://eri-agent-beta.aerial"},
		{name: "port and scheme preserved", defaultURL: "https://eri.default.svc.cluster.local:8080", defaultAgent: "aerial", agent: "agent-alpha", want: "https://eri-agent-alpha.default.svc.cluster.local:8080"},
		{name: "bare host with port", defaultURL: "http://eri:80", defaultAgent: "aerial", agent: "agent-alpha", want: "http://eri-agent-alpha:80"},
		{name: "default stack not named aerial", defaultURL: "http://eri.aerial", defaultAgent: "agent-alpha", agent: "aerial", want: "http://eri-aerial.aerial"},
		{name: "first label not eri is underivable", defaultURL: "http://brain.aerial", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
		{name: "eri only in later label is underivable", defaultURL: "http://x.eri.aerial", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
		{name: "empty configured url", defaultURL: "", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
		{name: "empty agent", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agent: "  ", want: ""},
		{name: "unparseable url", defaultURL: "http://eri.aerial:bad port", defaultAgent: "aerial", agent: "agent-alpha", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AgentBrainURL(tc.defaultURL, tc.defaultAgent, tc.agent); got != tc.want {
				t.Errorf("AgentBrainURL(%q, %q, %q) = %q, want %q", tc.defaultURL, tc.defaultAgent, tc.agent, got, tc.want)
			}
		})
	}
}

func TestSiblingBrainTargets_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		defaultURL   string
		defaultAgent string
		agents       []string
		want         []AgentBrainTarget
	}{
		{
			name: "default excluded, duplicates collapsed", defaultURL: "http://eri.aerial", defaultAgent: "aerial",
			agents: []string{"aerial", "agent-alpha", "Agent-Alpha", "agent-beta", ""},
			want:   []AgentBrainTarget{{Agent: "agent-alpha", URL: "http://eri-agent-alpha.aerial"}, {Agent: "agent-beta", URL: "http://eri-agent-beta.aerial"}},
		},
		{name: "underivable host yields none", defaultURL: "http://127.0.0.1:9999", defaultAgent: "aerial", agents: []string{"agent-alpha"}, want: []AgentBrainTarget{}},
		{name: "no agents", defaultURL: "http://eri.aerial", defaultAgent: "aerial", agents: nil, want: []AgentBrainTarget{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SiblingBrainTargets(tc.defaultURL, tc.defaultAgent, tc.agents); !slices.Equal(got, tc.want) {
				t.Errorf("SiblingBrainTargets() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDefaultAgentFromTasks_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		tasks []ActiveTaskStatus
		want  string
	}{
		{name: "no tasks falls back to default tenant", tasks: nil, want: "aerial"},
		{name: "rows without agent fall back", tasks: []ActiveTaskStatus{{ID: "1"}}, want: "aerial"},
		{name: "first reported agent wins", tasks: []ActiveTaskStatus{{ID: "1"}, {ID: "2", Agent: " Agent-Alpha "}}, want: "agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := DefaultAgentFromTasks(tc.tasks); got != tc.want {
				t.Errorf("DefaultAgentFromTasks() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStampTaskAgent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		tasks []ActiveTaskStatus
		agent string
		want  []ActiveTaskStatus
	}{
		{name: "nil yields empty", tasks: nil, agent: "aerial", want: []ActiveTaskStatus{}},
		{name: "fills empty, keeps reported", tasks: []ActiveTaskStatus{{ID: "1"}, {ID: "2", Agent: "agent-alpha"}}, agent: "aerial", want: []ActiveTaskStatus{{ID: "1", Agent: "aerial"}, {ID: "2", Agent: "agent-alpha"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := StampTaskAgent(tc.tasks, tc.agent)
			if got == nil || !slices.Equal(got, tc.want) {
				t.Errorf("StampTaskAgent() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
