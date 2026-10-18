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
		{
			name: "core aerial service via compose label",
			container: DockerContainerJSON{
				Labels: map[string]string{"com.docker.compose.service": "brain"},
			},
			wantCore: true,
		},
		{
			name: "core aerial service via container name prefix",
			container: DockerContainerJSON{
				Names: []string{"/aerial-dashboard"},
			},
			wantCore: true,
		},
		{
			name: "ignored auxiliary service watchtower via compose label",
			container: DockerContainerJSON{
				Labels: map[string]string{"com.docker.compose.service": "watchtower"},
			},
			wantCore: false,
		},
		{
			name: "ignored auxiliary service autoheal via name",
			container: DockerContainerJSON{
				Names: []string{"/aerial-autoheal"},
			},
			wantCore: false,
		},
		{
			name: "ignored auxiliary service agentsview",
			container: DockerContainerJSON{
				Labels: map[string]string{"com.docker.compose.service": "agentsview"},
			},
			wantCore: false,
		},
		{
			name: "ignored auxiliary service ollama",
			container: DockerContainerJSON{
				Labels: map[string]string{"com.docker.compose.service": "ollama"},
			},
			wantCore: false,
		},
		{
			name: "image containing watchtower is excluded",
			container: DockerContainerJSON{
				Image: "containrrr/watchtower:latest",
			},
			wantCore: false,
		},
		{
			name: "image containing autoheal is excluded",
			container: DockerContainerJSON{
				Image: "willfarrell/autoheal:latest",
			},
			wantCore: false,
		},
		{
			name: "image containing ollama is excluded",
			container: DockerContainerJSON{
				Image: "ollama/ollama:latest",
			},
			wantCore: false,
		},
		{
			name: "foreign image source excluded",
			container: DockerContainerJSON{
				Labels: map[string]string{
					"com.docker.compose.service":      "custom-service",
					"org.opencontainers.image.source": "https://github.com/someone/other-repo",
				},
			},
			wantCore: false,
		},
		{
			name: "valid aerial image source accepted",
			container: DockerContainerJSON{
				Labels: map[string]string{
					"com.docker.compose.service":      "custom-service",
					"org.opencontainers.image.source": "https://github.com/azylman/aerial",
				},
			},
			wantCore: true,
		},
		{
			name: "nil labels and names defaults to true",
			container: DockerContainerJSON{
				Image: "alpine:latest",
			},
			wantCore: true,
		},
		{
			name:      "image containing agentsview is excluded",
			container: DockerContainerJSON{Image: "ghcr.io/azylman/agentsview:latest"},
			wantCore:  false,
		},
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
	refTime := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		jobs     []GitHubJob
		now      time.Time
		wantLen  int
		validate func(t *testing.T, chips []MatrixJobChip)
	}{
		{
			name:    "empty jobs returns empty slice",
			jobs:    []GitHubJob{},
			now:     refTime,
			wantLen: 0,
		},
		{
			name: "zero-time default fallback",
			jobs: []GitHubJob{
				{
					Name:      "Build & Push Images (proxy)",
					Status:    "in_progress",
					StartedAt: time.Now().Add(-10 * time.Second),
				},
			},
			now:     time.Time{},
			wantLen: 1,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Name != "proxy" {
					t.Errorf("expected chip name 'proxy', got %q", chips[0].Name)
				}
			},
		},
		{
			name: "in_progress job computes active status and duration from refTime",
			jobs: []GitHubJob{
				{
					Name:      "brain-build",
					Status:    "in_progress",
					StartedAt: refTime.Add(-45 * time.Second),
				},
			},
			now:     refTime,
			wantLen: 1,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Name != "brain" {
					t.Errorf("expected chip name 'brain', got %q", chips[0].Name)
				}
				if chips[0].Status != "active" {
					t.Errorf("expected chip status 'active', got %q", chips[0].Status)
				}
				if chips[0].Duration != "45s" {
					t.Errorf("expected chip duration '45s', got %q", chips[0].Duration)
				}
			},
		},
		{
			name: "completed success and failure with deduplication",
			jobs: []GitHubJob{
				{
					Name:        "dashboard-test",
					Status:      "completed",
					Conclusion:  "success",
					StartedAt:   refTime.Add(-120 * time.Second),
					CompletedAt: refTime.Add(-60 * time.Second),
				},
				{
					Name:       "dashboard-duplicate",
					Status:     "completed",
					Conclusion: "failure",
				},
				{
					Name:       "scheduler-mcp",
					Status:     "completed",
					Conclusion: "failure",
				},
				{
					Name:       "discord-mcp",
					Status:     "completed",
					Conclusion: "cancelled",
				},
			},
			now:     refTime,
			wantLen: 3, // dashboard, scheduler-mcp, discord-mcp (deduplicated dashboard)
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Name != "dashboard" || chips[0].Status != "completed" || chips[0].Duration != "60s" {
					t.Errorf("unexpected first chip: %+v", chips[0])
				}
				if chips[1].Name != "scheduler-mcp" || chips[1].Status != "failed" {
					t.Errorf("unexpected second chip: %+v", chips[1])
				}
				if chips[2].Name != "discord-mcp" || chips[2].Status != "pending" {
					t.Errorf("unexpected third chip: %+v", chips[2])
				}
			},
		},
		{
			name: "zero reference time defaults to now",
			jobs: []GitHubJob{
				{
					Name:      "brain-build",
					Status:    "in_progress",
					StartedAt: time.Now().Add(-10 * time.Second),
				},
			},
			now:     time.Time{},
			wantLen: 1,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Name != "brain" || chips[0].Status != "active" {
					t.Errorf("unexpected chip: %+v", chips[0])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chips := ParseMatrixJobChips(tt.jobs, tt.now)
			if len(chips) != tt.wantLen {
				t.Fatalf("ParseMatrixJobChips() len = %d, want %d", len(chips), tt.wantLen)
			}
			if tt.validate != nil {
				tt.validate(t, chips)
			}
		})
	}
}

func TestBuildContainerChips_TableDriven(t *testing.T) {
	refTime := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		containers []DockerContainerJSON
		now        time.Time
		wantLen    int
		validate   func(t *testing.T, chips []MatrixJobChip)
	}{
		{
			name:       "empty containers returns empty slice",
			containers: []DockerContainerJSON{},
			now:        refTime,
			wantLen:    0,
		},
		{
			name: "zero-time default fallback",
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					Created: time.Now().Add(-60 * time.Second).Unix(),
					State:   "running",
					Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
				},
			},
			now:     time.Time{},
			wantLen: 1,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Name != "brain" {
					t.Errorf("expected chip name 'brain', got %q", chips[0].Name)
				}
			},
		},
		{
			name: "zero or future creation time defaults to 0s duration",
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					Created: 0,
					State:   "running",
				},
				{
					Names:   []string{"/aerial-dashboard"},
					Created: refTime.Add(10 * time.Minute).Unix(),
					State:   "running",
				},
			},
			now:     refTime,
			wantLen: 2,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Duration != "0s" {
					t.Errorf("expected 0s duration for Created=0, got %q", chips[0].Duration)
				}
				if chips[1].Duration != "0s" {
					t.Errorf("expected 0s duration for future Created, got %q", chips[1].Duration)
				}
			},
		},
		{
			name: "health status active for starting, failed for unhealthy or not running",
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					Created: refTime.Add(-120 * time.Second).Unix(),
					State:   "running",
					Health: &struct {
						Status string `json:"Status"`
					}{Status: "starting"},
				},
				{
					Names:   []string{"/aerial-proxy"},
					Created: refTime.Add(-300 * time.Second).Unix(),
					State:   "exited",
				},
				{
					Names:   []string{"/aerial-scheduler-mcp"},
					Created: refTime.Add(-600 * time.Second).Unix(),
					State:   "running",
					Health: &struct {
						Status string `json:"Status"`
					}{Status: "unhealthy"},
				},
			},
			now:     refTime,
			wantLen: 3,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Status != "active" || chips[0].Duration != "2m" {
					t.Errorf("unexpected starting chip: %+v", chips[0])
				}
				if chips[1].Status != "failed" || chips[1].Conclusion != "failure" {
					t.Errorf("unexpected exited chip: %+v", chips[1])
				}
				if chips[2].Status != "failed" || chips[2].Conclusion != "failure" {
					t.Errorf("unexpected unhealthy chip: %+v", chips[2])
				}
			},
		},
		{
			name: "deduplicates running brain containers with future and current created",
			containers: []DockerContainerJSON{
				{
					ID:      "c1",
					Names:   []string{"/aerial-brain"},
					State:   "running",
					Created: refTime.Add(1 * time.Hour).Unix(),
					Labels:  map[string]string{"com.docker.compose.service": "brain"},
				},
				{
					ID:      "c2",
					Names:   []string{"/aerial-brain"},
					State:   "running",
					Created: refTime.Unix(),
					Labels:  map[string]string{"com.docker.compose.service": "brain"},
				},
			},
			now:     refTime,
			wantLen: 1,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Duration != "0s" {
					t.Errorf("expected Duration '0s', got %q", chips[0].Duration)
				}
			},
		},
		{
			name: "nomad containers with alloc_id labels",
			containers: []DockerContainerJSON{
				{
					ID:      "brain-nomad",
					Names:   []string{"/brain-2a3a24e5-d6ce-2069-bad1-f328c7a24af0"},
					Created: refTime.Add(-120 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"aerial.service":               "brain",
						"com.hashicorp.nomad.alloc_id": "2a3a24e5-d6ce-2069-bad1-f328c7a24af0",
					},
				},
				{
					ID:      "vm-nomad",
					Names:   []string{"/victoriametrics-mcp-6ca68da8-1599-0516-4c28-635658025294"},
					Created: refTime.Add(-300 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"com.hashicorp.nomad.alloc_id": "6ca68da8-1599-0516-4c28-635658025294",
					},
				},
				{
					ID:    "hassio-supervisor",
					Names: []string{"/hassio_supervisor"},
					State: "running",
				},
			},
			now:     refTime,
			wantLen: 2,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				chipMap := make(map[string]MatrixJobChip)
				for _, chip := range chips {
					chipMap[chip.Name] = chip
				}
				if brainChip, ok := chipMap["brain"]; !ok {
					t.Errorf("missing chip for brain")
				} else if brainChip.Status != "completed" || brainChip.Conclusion != "success" {
					t.Errorf("unexpected brain chip status/conclusion: %+v", brainChip)
				}
				if vmChip, ok := chipMap["victoriametrics-mcp"]; !ok {
					t.Errorf("missing chip for victoriametrics-mcp")
				} else if vmChip.Status != "completed" {
					t.Errorf("unexpected victoriametrics chip status: %+v", vmChip)
				}
			},
		},
		{
			name: "zero reference time defaults to now",
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					Created: time.Now().Unix() - 10,
					State:   "running",
				},
			},
			now:     time.Time{},
			wantLen: 1,
			validate: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Status != "completed" || chips[0].Conclusion != "success" {
					t.Errorf("unexpected chip status/conclusion: %+v", chips[0])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chips := BuildContainerChips(tt.containers, tt.now)
			if len(chips) != tt.wantLen {
				t.Fatalf("BuildContainerChips() len = %d, want %d", len(chips), tt.wantLen)
			}
			if tt.validate != nil {
				tt.validate(t, chips)
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
		{
			name:       "empty containers returns empty",
			containers: []DockerContainerJSON{},
			expected:   "",
		},
		{
			name: "ignored auxiliary container is skipped",
			containers: []DockerContainerJSON{
				{
					Labels: map[string]string{
						"com.docker.compose.service":        "watchtower",
						"org.opencontainers.image.revision": "abcdef123456",
					},
				},
			},
			expected: "",
		},
		{
			name: "org.opencontainers.image.revision prioritized and truncated to 7 chars",
			containers: []DockerContainerJSON{
				{
					Labels: map[string]string{
						"com.docker.compose.service":        "brain",
						"org.opencontainers.image.revision": "1234567890abcdef",
						"aerial.commit_sha":                 "fedcba0987654321",
					},
				},
			},
			expected: "1234567",
		},
		{
			name: "aerial.commit_sha prioritized over vcs-ref",
			containers: []DockerContainerJSON{
				{
					Labels: map[string]string{
						"com.docker.compose.service": "brain",
						"aerial.commit_sha":          "abcdef1",
						"vcs-ref":                    "9999999",
					},
				},
			},
			expected: "abcdef1",
		},
		{
			name: "vcs-ref used as fallback",
			containers: []DockerContainerJSON{
				{
					Labels: map[string]string{
						"com.docker.compose.service": "brain",
						"vcs-ref":                    "short",
					},
				},
			},
			expected: "short",
		},
		{
			name: "core container with nil labels returns empty",
			containers: []DockerContainerJSON{
				{Names: []string{"/aerial-brain"}, Labels: nil},
			},
			expected: "",
		},
		{
			name: "short org.opencontainers.image.revision returned untruncated",
			containers: []DockerContainerJSON{
				{
					Names:  []string{"/aerial-brain"},
					Labels: map[string]string{"com.docker.compose.project": "aerial", "org.opencontainers.image.revision": "123456"},
				},
			},
			expected: "123456",
		},
		{
			name: "long aerial.commit_sha truncated to 7 chars",
			containers: []DockerContainerJSON{
				{
					Names:  []string{"/aerial-brain"},
					Labels: map[string]string{"com.docker.compose.project": "aerial", "aerial.commit_sha": "abcdef012345"},
				},
			},
			expected: "abcdef0",
		},
		{
			name: "long vcs-ref truncated to 7 chars",
			containers: []DockerContainerJSON{
				{
					Names:  []string{"/aerial-brain"},
					Labels: map[string]string{"com.docker.compose.project": "aerial", "vcs-ref": "9876543210"},
				},
			},
			expected: "9876543",
		},
		{
			name: "auxiliary sidecars ignored before brain",
			containers: []DockerContainerJSON{
				{
					ID:    "c-agentsview",
					Names: []string{"/aerial-agentsview"},
					Image: "ghcr.io/azylman/agentsview:latest",
					Labels: map[string]string{
						"com.docker.compose.project":        "aerial",
						"com.docker.compose.service":        "agentsview",
						"org.opencontainers.image.revision": "080b49fb7a7c1d8f206549e8ee4eaf9cf50a5c20",
					},
				},
				{
					ID:    "c-watchtower",
					Names: []string{"/aerial-watchtower"},
					Image: "containrrr/watchtower:latest",
					Labels: map[string]string{
						"com.docker.compose.project":        "aerial",
						"com.docker.compose.service":        "watchtower",
						"org.opencontainers.image.revision": "ace93994711edfe47036e2d2ee5f5d531df013ed",
					},
				},
				{
					ID:    "c-autoheal",
					Names: []string{"/aerial-autoheal"},
					Image: "willfarrell/autoheal:latest",
					Labels: map[string]string{
						"com.docker.compose.project":        "aerial",
						"com.docker.compose.service":        "autoheal",
						"org.opencontainers.image.revision": "autohealsha1234",
					},
				},
				{
					ID:    "c-brain",
					Names: []string{"/aerial-brain"},
					Image: "ghcr.io/azylman/aerial-brain:latest",
					Labels: map[string]string{
						"com.docker.compose.project": "aerial",
						"com.docker.compose.service": "brain",
						"aerial.commit_sha":          "e056544d32a5f9d8a4cc50915a20db5eaea7db1e",
					},
				},
			},
			expected: "e056544",
		},
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
		path      string
		validator func(t *testing.T, got string)
	}{
		{
			path: "index.html",
			validator: func(t *testing.T, got string) {
				if !strings.HasPrefix(got, "text/html") {
					t.Errorf("expected text/html prefix, got %q", got)
				}
			},
		},
		{
			path: "STYLE.CSS",
			validator: func(t *testing.T, got string) {
				if !strings.HasPrefix(got, "text/css") {
					t.Errorf("expected text/css prefix, got %q", got)
				}
			},
		},
		{
			path: "app.js",
			validator: func(t *testing.T, got string) {
				if !strings.Contains(got, "javascript") {
					t.Errorf("expected javascript in MIME type, got %q", got)
				}
			},
		},
		{
			path: "DATA.JSON",
			validator: func(t *testing.T, got string) {
				if !strings.HasPrefix(got, "application/json") {
					t.Errorf("expected application/json prefix, got %q", got)
				}
			},
		},
		{
			path: "icon.svg",
			validator: func(t *testing.T, got string) {
				if got != "image/svg+xml" {
					t.Errorf("expected image/svg+xml, got %q", got)
				}
			},
		},
		{
			path: "favicon.ico",
			validator: func(t *testing.T, got string) {
				if got != "image/x-icon" && got != "image/vnd.microsoft.icon" {
					t.Errorf("expected image/x-icon or image/vnd.microsoft.icon, got %q", got)
				}
			},
		},
		{
			path: "photo.png",
			validator: func(t *testing.T, got string) {
				if got != "image/png" {
					t.Errorf("expected image/png, got %q", got)
				}
			},
		},
		{
			path: "font.woff2",
			validator: func(t *testing.T, got string) {
				if !strings.Contains(got, "woff2") {
					t.Errorf("expected woff2 in MIME type, got %q", got)
				}
			},
		},
		{
			path: "font.woff",
			validator: func(t *testing.T, got string) {
				if !strings.Contains(got, "woff") {
					t.Errorf("expected woff in MIME type, got %q", got)
				}
			},
		},
		{
			path: "unknown.xyz123",
			validator: func(t *testing.T, got string) {
				if got != "application/octet-stream" {
					t.Errorf("expected application/octet-stream, got %q", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := GetMimeType(tt.path)
			tt.validator(t, got)
		})
	}
}

func TestSanitizeEnvVars_TableDriven(t *testing.T) {
	input := []string{
		"NORMAL_VAR=hello",
		"HA_TOKEN=supersecret123",
		"DISCORD_BOT_TOKEN=token456",
		"GITHUB_PAT=pat789",
		"DB_PASSWORD=mypassword",
		"API_KEY=keyvalue",
		"SESSION_SECRET=secretvalue",
		"INVALID_FORMAT_WITHOUT_EQUALS",
	}

	sanitized := SanitizeEnvVars(input)

	if len(sanitized) != 7 {
		t.Fatalf("expected 7 sanitized entries (skipping invalid format), got %d", len(sanitized))
	}

	for _, env := range sanitized {
		if strings.HasPrefix(env, "NORMAL_VAR=") && env != "NORMAL_VAR=hello" {
			t.Errorf("expected NORMAL_VAR to be preserved, got %q", env)
		}
		if (strings.Contains(env, "TOKEN") || strings.Contains(env, "PASSWORD") || strings.Contains(env, "KEY") || strings.Contains(env, "SECRET") || strings.Contains(env, "PAT")) && !strings.Contains(env, "[REDACTED]") {
			t.Errorf("expected sensitive key to be redacted, got %q", env)
		}
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
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	startedAt := now.Add(-60 * time.Second) // 1 minute ago

	rawContainers := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-dashboard"},
			Created: now.Add(-30 * time.Second).Unix(), // recently created (after startedAt)
			State:   "running",
			Labels: map[string]string{
				"com.docker.compose.project": "aerial",
				"com.docker.compose.service": "dashboard",
			},
		},
		{
			Names:   []string{"/aerial-brain"},
			Created: now.Add(-3 * 3600 * time.Second).Unix(), // 3 hours ago (stale, before startedAt)
			State:   "running",
			Labels: map[string]string{
				"com.docker.compose.project": "aerial",
				"com.docker.compose.service": "brain",
			},
		},
		{
			Names:   []string{"/aerial-proxy"},
			Created: now.Add(-10 * time.Second).Unix(),
			State:   "running",
			Health:  &DockerContainerHealth{Status: "starting"},
			Labels: map[string]string{
				"com.docker.compose.project": "aerial",
				"com.docker.compose.service": "proxy",
			},
		},
		{
			Names:   []string{"/aerial-docs"},
			Created: now.Add(-5 * time.Second).Unix(),
			State:   "exited",
			Labels: map[string]string{
				"com.docker.compose.project": "aerial",
				"com.docker.compose.service": "docs",
			},
		},
	}

	tests := []struct {
		name             string
		targets          []string
		startedAt        time.Time
		wantLen          int
		customContainers []DockerContainerJSON
		check            func(t *testing.T, chips []MatrixJobChip)
	}{
		{
			name:      "nil targets falls back to all containers",
			targets:   nil,
			startedAt: startedAt,
			wantLen:   4,
			check: func(t *testing.T, chips []MatrixJobChip) {
				names := make(map[string]bool)
				for _, c := range chips {
					names[c.Name] = true
				}
				if !names["dashboard"] || !names["brain"] || !names["proxy"] || !names["docs"] {
					t.Errorf("expected all containers in fallback, got %v", names)
				}
			},
		},
		{
			name:      "empty targets falls back to all containers",
			targets:   []string{},
			startedAt: startedAt,
			wantLen:   4,
		},
		{
			name:      "scoped to single recently created target",
			targets:   []string{"dashboard"},
			startedAt: startedAt,
			wantLen:   1,
			check: func(t *testing.T, chips []MatrixJobChip) {
				chip := chips[0]
				if chip.Name != "dashboard" {
					t.Errorf("expected dashboard chip, got %s", chip.Name)
				}
				if chip.Status != "completed" || chip.Conclusion != "success" {
					t.Errorf("expected completed/success, got status=%s conclusion=%s", chip.Status, chip.Conclusion)
				}
				if chip.Duration != "30s" {
					t.Errorf("expected 30s uptime, got %s", chip.Duration)
				}
			},
		},
		{
			name:      "stale container reports active and pending duration",
			targets:   []string{"brain"},
			startedAt: startedAt,
			wantLen:   1,
			check: func(t *testing.T, chips []MatrixJobChip) {
				chip := chips[0]
				if chip.Name != "brain" {
					t.Errorf("expected brain chip, got %s", chip.Name)
				}
				if chip.Status != "active" || chip.Duration != "pending" {
					t.Errorf("expected active/pending for stale container, got status=%s duration=%s", chip.Status, chip.Duration)
				}
			},
		},
		{
			name:      "missing container reports active and pending duration",
			targets:   []string{"scheduler-mcp"},
			startedAt: startedAt,
			wantLen:   1,
			check: func(t *testing.T, chips []MatrixJobChip) {
				chip := chips[0]
				if chip.Name != "scheduler-mcp" {
					t.Errorf("expected scheduler-mcp chip, got %s", chip.Name)
				}
				if chip.Status != "active" || chip.Duration != "pending" {
					t.Errorf("expected active/pending for missing container, got status=%s duration=%s", chip.Status, chip.Duration)
				}
			},
		},
		{
			name:      "starting health reports active and non-empty duration",
			targets:   []string{"proxy"},
			startedAt: startedAt,
			wantLen:   1,
			check: func(t *testing.T, chips []MatrixJobChip) {
				chip := chips[0]
				if chip.Status != "active" || chip.Conclusion != "" {
					t.Errorf("expected active/empty conclusion for starting container, got status=%s", chip.Status)
				}
			},
		},
		{
			name:      "exited container reports failed and failure conclusion",
			targets:   []string{"docs"},
			startedAt: startedAt,
			wantLen:   1,
			check: func(t *testing.T, chips []MatrixJobChip) {
				chip := chips[0]
				if chip.Status != "failed" || chip.Conclusion != "failure" {
					t.Errorf("expected failed/failure for exited container, got status=%s", chip.Status)
				}
			},
		},
		{
			name:      "multiple targets deduplicated and trimmed",
			targets:   []string{"dashboard", " dashboard ", "docs", ""},
			startedAt: startedAt,
			wantLen:   2,
		},
		{
			name:             "zero reference time defaults now to time.Now().UTC()",
			targets:          []string{"dashboard"},
			startedAt:        startedAt,
			wantLen:          1,
			customContainers: rawContainers,
			check: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Duration == "" {
					t.Errorf("expected non-empty duration with zero time fallback")
				}
			},
		},
		{
			name:      "container name prefix fallback and newer container wins",
			targets:   []string{"brain"},
			startedAt: startedAt,
			wantLen:   1,
			customContainers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					State:   "running",
					Created: startedAt.Add(5 * time.Second).Unix(),
				},
				{
					Names:   []string{"/aerial-brain"},
					State:   "exited",
					Created: startedAt.Add(-100 * time.Second).Unix(),
				},
			},
			check: func(t *testing.T, chips []MatrixJobChip) {
				if chips[0].Status != "completed" {
					t.Errorf("expected newest running container to win, got %s", chips[0].Status)
				}
			},
		},
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
			if len(got) != tt.wantLen {
				t.Fatalf("BuildTargetContainerChips() returned %d chips, want %d", len(got), tt.wantLen)
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}


func TestMergeClusterDeploymentsWithHangar_PureTableDriven(t *testing.T) {
	t.Parallel()
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	commitTime := refTime.Add(-10 * time.Minute)

	headCommit := func(msg string, ts time.Time) *struct {
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	} {
		return &struct {
			Message   string    `json:"message"`
			Timestamp time.Time `json:"timestamp"`
		}{Message: msg, Timestamp: ts}
	}

	var cardinalityRuns []GitHubRun
	for i := 1; i <= 8; i++ {
		cardinalityRuns = append(cardinalityRuns, GitHubRun{
			ID:        int64(600 + i),
			HeadSHA:   fmt.Sprintf("%07d000000000000000000000000000000000", i),
			Status:    "in_progress",
			CreatedAt: refTime.Add(time.Duration(-i) * time.Minute),
			UpdatedAt: refTime.Add(time.Duration(-i) * time.Minute),
		})
	}

	sortRuns := []GitHubRun{
		{
			ID:         100,
			HeadSHA:    "1111111111111111111111111111111111111111",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  refTime.Add(-10 * time.Minute),
			UpdatedAt:  refTime.Add(-2 * time.Minute),
		},
		{
			ID:        101,
			HeadSHA:   "3333333333333333333333333333333333333333",
			Status:    "in_progress",
			CreatedAt: refTime.Add(-4 * time.Minute),
			UpdatedAt: refTime.Add(-3 * time.Minute),
		},
		{
			ID:         102,
			HeadSHA:    "5555555555555555555555555555555555555555",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  refTime.Add(-10 * time.Minute),
			UpdatedAt:  refTime.Add(-2 * time.Minute),
		},
		{
			ID:        103,
			HeadSHA:   "2222222222222222222222222222222222222222",
			Status:    "queued",
			CreatedAt: refTime.Add(-1 * time.Minute),
			UpdatedAt: refTime.Add(-1 * time.Minute),
		},
	}
	sortGitSync := GitSyncStatusResponse{
		Status: "synced",
		Reconciliation: &ReconciliationStatus{
			Active:    true,
			State:     "pulling",
			CommitSHA: "4444444444444444444444444444444444444444",
			StartedAt: refTime.Add(-30 * time.Second),
		},
	}
	sortContainers := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-proxy"},
			Created: refTime.Add(-40 * time.Second).Unix(),
			State:   "running",
			Labels: map[string]string{
				"com.docker.compose.project":        "aerial",
				"com.docker.compose.service":        "proxy",
				"org.opencontainers.image.revision": "1111111111111111111111111111111111111111",
			},
		},
		{
			Names:   []string{"/aerial-brain"},
			Created: refTime.Add(-300 * time.Second).Unix(),
			State:   "running",
			Labels: map[string]string{
				"com.docker.compose.project":        "aerial",
				"com.docker.compose.service":        "brain",
				"org.opencontainers.image.revision": "6666666666666666666666666666666666666666",
			},
		},
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
		wantCount     int
		wantList      []wantDep
		extraCheck    func(t *testing.T, deps []DeploymentStatus)
	}{
		// 1. Concurrent Pipelines
		{
			name: "ConcurrentPipelines",
			runs: []GitHubRun{
				{
					ID:        201,
					HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-5 * time.Minute),
					UpdatedAt: refTime.Add(-1 * time.Minute),
					HTMLURL:   "https://github.com/azylman/aerial/actions/runs/201",
					HeadCommit: headCommit("feat: build feature B", refTime.Add(-5*time.Minute)),
				},
			},
			jobs: map[int64][]GitHubJob{
				201: {
					{
						ID:        1,
						RunID:     201,
						Name:      "build (dashboard)",
						Status:    "in_progress",
						StartedAt: refTime.Add(-4 * time.Minute),
					},
				},
			},
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:         true,
					State:          "swapping",
					Stage:          "swapping",
					CommitSHA:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					TargetServices: []string{"proxy"},
					StartedAt:      refTime.Add(-2 * time.Minute),
				},
			},
			currentCommit: "aaaaaaaa",
			refTime:       refTime,
			wantList: []wantDep{
				{Stage: "swapping", Commit: "aaaaaaa"},
				{Stage: "building", Commit: "bbbbbbb"},
			},
		},
		// 2. AwaitingPullBridge
		{
			name: "AwaitingPullBridge",
			runs: []GitHubRun{
				{
					ID:         301,
					HeadSHA:    "cccccccccccccccccccccccccccccccccccccccc",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-10 * time.Minute),
					UpdatedAt:  refTime.Add(-5 * time.Minute),
					HTMLURL:    "https://github.com/azylman/aerial/actions/runs/301",
					HeadCommit: headCommit("chore: updated docs", refTime.Add(-10*time.Minute)),
				},
			},
			currentCommit: "oldcommit",
			refTime:       refTime,
			wantList: []wantDep{{
				Commit: "ccccccc",
				Stage:  "awaiting_pull",
				StepStatusMap: map[int]string{
					1: "completed",
					2: "active",
				},
			}},
		},
		// 3. DeterministicSort
		{
			name:          "DeterministicSort",
			runs:          sortRuns,
			gitSync:       sortGitSync,
			containers:    sortContainers,
			currentCommit: "6666666",
			refTime:       refTime,
			wantCount:     5,
			extraCheck: func(t *testing.T, _ []DeploymentStatus) {
				expectedStages := []string{"swapping", "pulling", "building", "awaiting_pull", "queued"}
				for i := 0; i < 10; i++ {
					deps := MergeClusterDeploymentsWithHangar(sortRuns, nil, sortGitSync, sortContainers, "6666666", refTime)
					if len(deps) != len(expectedStages) {
						t.Fatalf("iteration %d: expected %d deployments, got %d", i, len(expectedStages), len(deps))
					}
					for idx, expStage := range expectedStages {
						if deps[idx].Stage != expStage {
							t.Fatalf("iteration %d at index %d: expected stage %q, got %q", i, idx, expStage, deps[idx].Stage)
						}
					}
				}
			},
		},
		// 4. MultiCommitContainerGrouping
		{
			name: "MultiCommitContainerGrouping",
			runs: []GitHubRun{
				{
					ID:         1,
					HeadSHA:    "aaaaaaa123456789",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-10 * time.Minute),
					UpdatedAt:  refTime.Add(-5 * time.Minute),
				},
				{
					ID:         2,
					HeadSHA:    "bbbbbbb123456789",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-2 * time.Minute),
					UpdatedAt:  refTime.Add(-1 * time.Minute),
				},
			},
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					Created: refTime.Add(-300 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"com.docker.compose.project":        "aerial",
						"com.docker.compose.service":        "brain",
						"org.opencontainers.image.revision": "aaaaaaa123456789",
					},
				},
				{
					Names:   []string{"/aerial-proxy"},
					Created: refTime.Add(-30 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"com.docker.compose.project":        "aerial",
						"com.docker.compose.service":        "proxy",
						"org.opencontainers.image.revision": "bbbbbbb123456789",
					},
				},
			},
			currentCommit: "aaaaaaa",
			refTime:       refTime,
			wantList: []wantDep{
				{Commit: "bbbbbbb", Stage: "swapping"},
				{Commit: "aaaaaaa", Stage: "live"},
			},
		},
		// 5. ChipScoping
		{
			name: "ChipScoping",
			runs: []GitHubRun{
				{
					ID:        401,
					HeadSHA:   "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-2 * time.Minute),
					UpdatedAt: refTime.Add(-1 * time.Minute),
				},
			},
			jobs: map[int64][]GitHubJob{
				401: {
					{
						ID:        1,
						RunID:     401,
						Name:      "build (brain)",
						Status:    "in_progress",
						StartedAt: refTime.Add(-90 * time.Second),
					},
				},
			},
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					Created: refTime.Add(-30 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"com.docker.compose.project":        "aerial",
						"com.docker.compose.service":        "brain",
						"org.opencontainers.image.revision": "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					},
				},
			},
			currentCommit: "7777777",
			refTime:       refTime,
			wantList: []wantDep{{
				HasCIChip:        true,
				HasContainerChip: true,
			}},
		},
		// 6. SafeNilHandling
		{
			name: "SafeNilHandling",
			runs: []GitHubRun{
				{
					ID:        501,
					HeadSHA:   "",
					Status:    "queued",
					CreatedAt: refTime.Add(-10 * time.Second),
				},
				{
					ID:        502,
					HeadSHA:   "abc",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-20 * time.Second),
				},
				{
					ID:         503,
					HeadSHA:    "not-a-valid-hex-sha!@#$%^&*()",
					Status:     "completed",
					Conclusion: "failure",
					CreatedAt:  refTime.Add(-30 * time.Second),
					UpdatedAt:  refTime.Add(-25 * time.Second),
				},
			},
			refTime:   refTime,
			wantCount: 3,
		},
		// 7. CardinalityClamp
		{
			name:      "CardinalityClamp",
			runs:      cardinalityRuns,
			refTime:   refTime,
			wantCount: 5,
		},
		// 8. PullingNotPromotedToSwapping
		{
			name: "PullingNotPromotedToSwapping",
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:         true,
					State:          "pulling",
					CommitSHA:      "7777777777777777777777777777777777777777",
					StartedAt:      refTime.Add(-15 * time.Second),
					TargetServices: []string{"aerial-dashboard"},
				},
			},
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-dashboard"},
					Created: refTime.Add(-300 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"org.opencontainers.image.revision": "7777777777777777777777777777777777777777",
					},
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage: "pulling",
				StepStatusMap: map[int]string{
					2: "active",
					3: "pending",
				},
			}},
		},
		// 9. DegradedAndFailedBranches_DegradedContainer
		{
			name: "DegradedAndFailedBranches_DegradedContainer",
			runs: []GitHubRun{
				{
					ID:         801,
					HeadSHA:    "8888888888888888888888888888888888888888",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-5 * time.Minute),
					UpdatedAt:  refTime.Add(-2 * time.Minute),
				},
			},
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-dashboard"},
					Created: refTime.Add(-60 * time.Second).Unix(),
					State:   "exited",
					Labels: map[string]string{
						"org.opencontainers.image.revision": "8888888888888888888888888888888888888888",
					},
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage: "degraded",
				StepStatusMap: map[int]string{
					4: "failed",
				},
			}},
		},
		// 10. DegradedAndFailedBranches_HangarFailedWithError
		{
			name: "DegradedAndFailedBranches_HangarFailedWithError",
			gitSync: GitSyncStatusResponse{
				Reconciliation: &ReconciliationStatus{
					State:     "failed",
					CommitSHA: "9999999999999999999999999999999999999999",
					Error:     "failed to pull image: auth expired",
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage:           "failed",
				CommitMsgSubstr: "auth expired",
			}},
		},
		// 11. DegradedAndFailedBranches_HangarFailedWithoutError
		{
			name: "DegradedAndFailedBranches_HangarFailedWithoutError",
			gitSync: GitSyncStatusResponse{
				Reconciliation: &ReconciliationStatus{
					State:     "failed",
					CommitSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage: "failed",
			}},
		},
		// 12. MultiRunSameCommitAndSortTies_SameCommit
		{
			name: "MultiRunSameCommitAndSortTies_SameCommit",
			runs: []GitHubRun{
				{
					ID:         111,
					HeadSHA:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-10 * time.Minute),
					UpdatedAt:  refTime.Add(-5 * time.Minute),
				},
				{
					ID:        112,
					HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-1 * time.Minute),
					UpdatedAt: refTime.Add(-30 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage: "building",
			}},
		},
		// 13. MultiRunSameCommitAndSortTies_SortTiesLexicographical
		{
			name: "MultiRunSameCommitAndSortTies_SortTiesLexicographical",
			runs: []GitHubRun{
				{
					ID:        201,
					HeadSHA:   "ffffffffffffffffffffffffffffffffffffffff",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-2 * time.Minute),
					UpdatedAt: refTime.Add(-1 * time.Minute),
				},
				{
					ID:        202,
					HeadSHA:   "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-2 * time.Minute),
					UpdatedAt: refTime.Add(-1 * time.Minute),
				},
			},
			refTime: refTime,
			wantList: []wantDep{
				{Commit: "eeeeeee"},
				{Commit: "fffffff"},
			},
		},
		// 14. MultiRunSameCommitAndSortTies_FailedPrecedesDegraded
		{
			name: "MultiRunSameCommitAndSortTies_FailedPrecedesDegraded",
			runs: []GitHubRun{
				{
					ID:         301,
					HeadSHA:    "1111111111111111111111111111111111111111",
					Status:     "completed",
					Conclusion: "failure",
					CreatedAt:  refTime.Add(-5 * time.Minute),
					UpdatedAt:  refTime.Add(-5 * time.Minute),
				},
				{
					ID:         302,
					HeadSHA:    "2222222222222222222222222222222222222222",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-5 * time.Minute),
					UpdatedAt:  refTime.Add(-5 * time.Minute),
				},
			},
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-proxy"},
					Created: refTime.Add(-5 * time.Minute).Unix(),
					State:   "exited",
					Labels: map[string]string{
						"org.opencontainers.image.revision": "2222222222222222222222222222222222222222",
					},
				},
			},
			refTime: refTime,
			wantList: []wantDep{
				{Stage: "failed"},
				{Stage: "degraded"},
			},
		},
		// 15. MultiRunSameCommitAndSortTies_ExpiredIgnored
		{
			name: "MultiRunSameCommitAndSortTies_ExpiredIgnored",
			runs: []GitHubRun{
				{
					ID:         401,
					HeadSHA:    "3333333333333333333333333333333333333333",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-40 * time.Minute),
					UpdatedAt:  refTime.Add(-35 * time.Minute),
				},
			},
			refTime:   refTime,
			wantCount: 0,
		},
		// 16. UnparentedContainersDoNotSpawnCards
		{
			name: "UnparentedContainersDoNotSpawnCards",
			containers: []DockerContainerJSON{
				{
					ID:      "c-postgres",
					Names:   []string{"/aerial-postgres"},
					Created: refTime.Add(-30 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"com.docker.compose.service": "postgres",
					},
				},
				{
					ID:      "c-vector",
					Names:   []string{"/aerial-vector"},
					Created: refTime.Add(-10 * time.Second).Unix(),
					State:   "exited",
					Labels: map[string]string{
						"com.docker.compose.service": "vector",
					},
				},
			},
			currentCommit: "livecommit",
			refTime:       refTime,
			wantCount:     0,
		},
		// 17. ActiveCIRun
		{
			name: "ActiveCIRun",
			runs: []GitHubRun{
				{
					ID:         481,
					Name:       "Continuous Delivery",
					HeadSHA:    "7a9f1b234567",
					Status:     "in_progress",
					CreatedAt:  refTime.Add(-2 * time.Minute),
					UpdatedAt:  refTime.Add(-10 * time.Second),
					HTMLURL:    "https://github.com/azylman/aerial/actions/runs/481",
					HeadCommit: headCommit("feat(brain): live github actions tracking", refTime.Add(-2*time.Minute)),
				},
			},
			jobs: map[int64][]GitHubJob{
				481: {
					{ID: 1, Name: "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)", Status: "in_progress", StartedAt: refTime.Add(-40 * time.Second)},
					{ID: 2, Name: "Build & Push Images to GHCR (dashboard, ./dashboard, ./dashboard/Dockerfile, aerial-dashboard)", Status: "completed", Conclusion: "success", StartedAt: refTime.Add(-60 * time.Second), CompletedAt: refTime.Add(-10 * time.Second)},
				},
			},
			currentCommit: "7a9f1b2",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:          "building",
				Commit:         "7a9f1b2",
				MatrixJobCount: 2,
				HTMLURL:        "https://github.com/azylman/aerial/actions/runs/481",
			}},
		},
		// 18. FailedCIRun
		{
			name: "FailedCIRun",
			runs: []GitHubRun{
				{
					ID:         482,
					Name:       "Continuous Delivery",
					HeadSHA:    "abc1234567",
					Status:     "completed",
					Conclusion: "failure",
					CreatedAt:  refTime.Add(-5 * time.Minute),
					UpdatedAt:  refTime.Add(-4 * time.Minute),
					HTMLURL:    "https://github.com/azylman/aerial/actions/runs/482",
				},
			},
			currentCommit: "abc1234",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage: "failed",
				StepStatusMap: map[int]string{
					1: "failed",
				},
			}},
		},
		// 19. AwaitingHangarSync
		{
			name: "AwaitingHangarSync",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-10 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
			},
			runs: []GitHubRun{
				{ID: 483, Name: "Continuous Delivery", HeadSHA: "def5678901", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-4 * time.Minute), UpdatedAt: refTime.Add(-45 * time.Second), HTMLURL: "https://github.com/azylman/aerial/actions/runs/483"},
			},
			jobs: map[int64][]GitHubJob{
				483: {{Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"}},
			},
			currentCommit: "old1234",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage: "awaiting_pull",
				StepStatusMap: map[int]string{
					2: "active",
				},
			}},
		},
		// 20. HangarSyncTimeout
		{
			name: "HangarSyncTimeout",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-10 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
			},
			runs: []GitHubRun{
				{ID: 484, Name: "Continuous Delivery", HeadSHA: "timeout1234", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-10 * time.Minute), UpdatedAt: refTime.Add(-150 * time.Second), HTMLURL: "https://github.com/azylman/aerial/actions/runs/484"},
			},
			jobs: map[int64][]GitHubJob{
				484: {{Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"}},
			},
			currentCommit: "old1234",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:           "failed",
				CommitMsgSubstr: "Hangar reconciliation timed out (>120s)",
				StepStatusMap: map[int]string{
					2: "failed",
				},
			}},
		},
		// 21. NoContainerBuilds_ImmediateLive
		{
			name: "NoContainerBuilds_ImmediateLive",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-1 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
			},
			runs: []GitHubRun{
				{ID: 485, Name: "Continuous Delivery", HeadSHA: "purecode1234", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-10 * time.Minute), UpdatedAt: refTime.Add(-150 * time.Second), HTMLURL: "https://github.com/azylman/aerial/actions/runs/485"},
			},
			jobs: map[int64][]GitHubJob{
				485: {
					{Name: "Run Service Unit Tests", Status: "completed", Conclusion: "success"},
					{Name: "Lint Microservices & Frontend", Status: "completed", Conclusion: "success"},
					{Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "skipped"},
				},
			},
			currentCommit: "old1234",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:    "live",
				Progress: 100,
				StepStatusMap: map[int]string{
					2: "completed",
				},
			}},
		},
		// 22. RollingSwap
		{
			name: "RollingSwap",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-30 * time.Second).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain", "org.opencontainers.image.revision": "abc9999999"}, Health: &DockerContainerHealth{Status: "starting"}},
				{ID: "c2", Names: []string{"/aerial-dashboard"}, State: "running", Created: refTime.Add(-10 * time.Second).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"}},
			},
			runs: []GitHubRun{
				{ID: 101, HeadSHA: "abc9999999", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-2 * time.Minute), UpdatedAt: refTime.Add(-1 * time.Minute)},
			},
			currentCommit: "abc9999",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:          "swapping",
				Commit:         "abc9999",
				MatrixJobCount: 2,
			}},
		},
		// 23. SyncedGrace
		{
			name: "SyncedGrace",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-3 * time.Minute).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}, Health: &DockerContainerHealth{Status: "healthy"}},
				{ID: "c2", Names: []string{"/aerial-dashboard"}, State: "running", Created: refTime.Add(-3 * time.Minute).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"}, Health: &DockerContainerHealth{Status: "healthy"}},
			},
			runs: []GitHubRun{
				{ID: 102, HeadSHA: "live123", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-5 * time.Minute), UpdatedAt: refTime.Add(-4 * time.Minute)},
			},
			currentCommit: "live123",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:        "live",
				Progress:     100,
				StepStatuses: []string{"completed", "completed", "completed", "completed", "completed"},
			}},
		},
		// 24. DegradedState
		{
			name: "DegradedState",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-2 * time.Minute).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}, Health: &DockerContainerHealth{Status: "unhealthy"}},
				{ID: "c2", Names: []string{"/aerial-dashboard"}, State: "running", Created: refTime.Add(-2 * time.Minute).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"}, Health: &DockerContainerHealth{Status: "healthy"}},
			},
			runs: []GitHubRun{
				{ID: 103, HeadSHA: "deg1234", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-5 * time.Minute), UpdatedAt: refTime.Add(-3 * time.Minute)},
			},
			currentCommit: "deg1234",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage: "degraded",
				StepStatusMap: map[int]string{
					4: "failed",
				},
			}},
		},
		// 25. Idle
		{
			name: "Idle",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-2 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}, Health: &DockerContainerHealth{Status: "healthy"}},
			},
			currentCommit: "idle123",
			refTime:       refTime,
			wantCount:     0,
		},
		// 26. AdversarialNilLabelsAndNilHealth
		{
			name: "AdversarialNilLabelsAndNilHealth",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-30 * time.Second).Unix()},
				{ID: "c2", Names: []string{"/aerial-dashboard"}, State: "running", Created: refTime.Add(-30 * time.Second).Unix()},
			},
			runs: []GitHubRun{
				{ID: 104, HeadSHA: "test1234", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-2 * time.Minute), UpdatedAt: refTime.Add(-1 * time.Minute)},
			},
			currentCommit: "test1234",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:          "swapping",
				MatrixJobCount: 2,
			}},
		},
		// 27. AdversarialClockSkewFutureTimestamp
		{
			name: "AdversarialClockSkewFutureTimestamp",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(2 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
			},
			runs: []GitHubRun{
				{ID: 999, Name: "Continuous Delivery", HeadSHA: "future1234", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(10 * time.Second), UpdatedAt: refTime.Add(15 * time.Second), HTMLURL: "https://github.com/azylman/aerial/actions/runs/999"},
			},
			currentCommit: "old1234",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage: "awaiting_pull",
			}},
		},
		// 28. CommitTimeAcrossAllStages_Queued
		{
			name: "CommitTimeAcrossAllStages_Queued",
			runs: []GitHubRun{
				{ID: 501, Name: "Continuous Delivery", HeadSHA: "1111aaa", Status: "queued", CreatedAt: refTime.Add(-2 * time.Minute), HeadCommit: headCommit("feat: queued", commitTime)},
			},
			currentCommit: "1111aaa",
			refTime:       refTime,
			wantList: []wantDep{{
				CheckCommitTime: &commitTime,
			}},
		},
		// 29. CommitTimeAcrossAllStages_Building
		{
			name: "CommitTimeAcrossAllStages_Building",
			runs: []GitHubRun{
				{ID: 502, Name: "Continuous Delivery", HeadSHA: "2222bbb", Status: "in_progress", CreatedAt: refTime.Add(-1 * time.Minute), HeadCommit: headCommit("feat: building", commitTime)},
			},
			currentCommit: "2222bbb",
			refTime:       refTime,
			wantList: []wantDep{{
				CheckCommitTime: &commitTime,
			}},
		},
		// 30. CommitTimeAcrossAllStages_Failed
		{
			name: "CommitTimeAcrossAllStages_Failed",
			runs: []GitHubRun{
				{ID: 503, Name: "Continuous Delivery", HeadSHA: "3333ccc", Status: "completed", Conclusion: "failure", CreatedAt: refTime.Add(-5 * time.Minute), UpdatedAt: refTime.Add(-4 * time.Minute), HeadCommit: headCommit("feat: failed", commitTime)},
			},
			currentCommit: "3333ccc",
			refTime:       refTime,
			wantList: []wantDep{{
				CheckCommitTime: &commitTime,
			}},
		},
		// 31. CommitTimeAcrossAllStages_Swapping
		{
			name: "CommitTimeAcrossAllStages_Swapping",
			containers: []DockerContainerJSON{
				{ID: "c-brain", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-30 * time.Second).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}, Health: &DockerContainerHealth{Status: "starting"}},
			},
			runs: []GitHubRun{
				{ID: 504, Name: "Continuous Delivery", HeadSHA: "4444ddd", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-2 * time.Minute), UpdatedAt: refTime.Add(-1 * time.Minute), HeadCommit: headCommit("feat: success", commitTime)},
			},
			currentCommit: "4444ddd",
			refTime:       refTime,
			wantList: []wantDep{{
				Commit:          "4444ddd",
				CheckCommitTime: &commitTime,
			}},
		},
		// 32. EmptyAndFuture_Empty
		{
			name:          "EmptyAndFuture_Empty",
			currentCommit: "commit123",
			refTime:       refTime,
			wantCount:     0,
		},
		// 33. EmptyAndFuture_FutureContainers
		{
			name: "EmptyAndFuture_FutureContainers",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(2 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
			},
			runs: []GitHubRun{
				{ID: 123, Status: "completed", Conclusion: "success", HeadSHA: "sha1234567890", CreatedAt: refTime.Add(-10 * time.Minute)},
			},
			refTime:   refTime,
			wantCount: 1,
		},
		// 34. HangarActiveSuppresses120sTimeout
		{
			name: "HangarActiveSuppresses120sTimeout",
			runs: []GitHubRun{
				{ID: 101, HeadSHA: "abc1234567890", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-300 * time.Second), UpdatedAt: refTime.Add(-200 * time.Second), HTMLURL: "https://github.com/azylman/aerial/actions/runs/101"},
			},
			jobs: map[int64][]GitHubJob{
				101: {{Name: "Build & Push Images (dashboard)", Status: "completed", Conclusion: "success"}},
			},
			gitSync: GitSyncStatusResponse{
				Status:         "synced",
				Reconciliation: &ReconciliationStatus{Active: true, State: "pulling", Stage: "pulling", TargetServices: []string{"dashboard"}},
			},
			currentCommit: "oldcommit",
			refTime:       refTime,
			wantList: []wantDep{{
				StepStatusMap: map[int]string{
					2: "active",
				},
			}},
		},
		// 35. HangarFailedFastFail
		{
			name: "HangarFailedFastFail",
			runs: []GitHubRun{
				{ID: 102, HeadSHA: "def5678901234", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-60 * time.Second), UpdatedAt: refTime.Add(-30 * time.Second), HTMLURL: "https://github.com/azylman/aerial/actions/runs/102"},
			},
			gitSync: GitSyncStatusResponse{
				Status:         "error",
				Reconciliation: &ReconciliationStatus{Active: false, State: "failed", Stage: "failed", Error: "manifest unknown"},
			},
			currentCommit: "oldcommit",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:           "failed",
				CommitMsgSubstr: "manifest unknown",
				StepStatusMap: map[int]string{
					2: "failed",
				},
			}},
		},
		// 36. TargetServicesScoping
		{
			name: "TargetServicesScoping",
			containers: []DockerContainerJSON{
				{Names: []string{"/aerial-dashboard"}, Created: refTime.Add(-30 * time.Second).Unix(), State: "running", Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"}},
				{Names: []string{"/aerial-brain"}, Created: refTime.Add(-5 * 3600 * time.Second).Unix(), State: "running", Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
				{Names: []string{"/aerial-proxy"}, Created: refTime.Add(-5 * 3600 * time.Second).Unix(), State: "running", Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "proxy"}},
			},
			gitSync: GitSyncStatusResponse{
				Status:         "synced",
				Reconciliation: &ReconciliationStatus{Active: true, State: "swapping", Stage: "swapping", TargetServices: []string{"dashboard"}, CommitSHA: "548c826", StartedAt: refTime.Add(-60 * time.Second)},
			},
			currentCommit: "current",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage:          "swapping",
				Commit:         "548c826",
				MatrixJobCount: 1,
			}},
		},
		// 37. DegradedWithoutHangarFailure
		{
			name: "DegradedWithoutHangarFailure",
			containers: []DockerContainerJSON{
				{ID: "c-unhealthy", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-10 * time.Minute).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}, Health: &DockerContainerHealth{Status: "unhealthy"}},
			},
			runs: []GitHubRun{
				{ID: 105, HeadSHA: "commit123", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-15 * time.Minute), UpdatedAt: refTime.Add(-12 * time.Minute)},
			},
			gitSync:       GitSyncStatusResponse{Status: "synced"},
			currentCommit: "commit123",
			refTime:       refTime,
			wantList: []wantDep{{
				Stage: "degraded",
				StepStatusMap: map[int]string{
					3: "completed",
					4: "failed",
				},
			}},
		},
		// 38. MultiRepoActiveFanout
		{
			name: "MultiRepoActiveFanout",
			runs: []GitHubRun{
				{ID: 101, Repository: "azylman/aerial", Name: "CI", HeadSHA: "1111aaa", Status: "in_progress", CreatedAt: refTime.Add(-2 * time.Minute), HTMLURL: "https://github.com/azylman/aerial/actions/runs/101"},
				{ID: 201, Repository: "azylman/aerial-sidecars", Name: "Build Sidecars", HeadSHA: "2222bbb", Status: "in_progress", CreatedAt: refTime.Add(-1 * time.Minute), HTMLURL: "https://github.com/azylman/aerial-sidecars/actions/runs/201"},
			},
			currentCommit: "0000000",
			refTime:       refTime,
			wantList: []wantDep{
				{Repository: "azylman/aerial-sidecars", Service: "aerial-sidecars", Stage: "building"},
				{Repository: "azylman/aerial", Service: "aerial-stack", Stage: "building"},
			},
		},
		// 39. NonContainerRepoNoTimeout
		{
			name: "NonContainerRepoNoTimeout",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-10 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
			},
			runs: []GitHubRun{
				{ID: 301, Repository: "azylman/aerial-config", Name: "Config Validation", HeadSHA: "3333ccc", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-5 * time.Minute), UpdatedAt: refTime.Add(-150 * time.Second), HTMLURL: "https://github.com/azylman/aerial-config/actions/runs/301"},
			},
			currentCommit: "oldcorecommit",
			refTime:       refTime,
			wantList: []wantDep{{
				Service: "aerial-config",
				Stage:   "awaiting_pull",
			}},
		},
		// 40. ActivePRRunShowsBuilding
		{
			name: "ActivePRRunShowsBuilding",
			runs: []GitHubRun{
				{ID: 9001, Repository: "azylman/aerial-sidecars", Name: "Continuous Delivery", Event: "pull_request", HeadBranch: "feat/sidecars-pr", HeadSHA: "aabbccdd1122", Status: "in_progress", CreatedAt: refTime.Add(-1 * time.Minute), UpdatedAt: refTime.Add(-30 * time.Second), HTMLURL: "https://github.com/azylman/aerial-sidecars/actions/runs/9001"},
			},
			jobs: map[int64][]GitHubJob{
				9001: {
					{Name: "Run Sidecar Tests & Lint", Status: "completed", Conclusion: "success"},
					{Name: "Build & Push Sidecar Images to GHCR (aura-farming)", Status: "in_progress"},
				},
			},
			currentCommit: "0000000",
			refTime:       refTime,
			wantList: []wantDep{{
				Repository:     "azylman/aerial-sidecars",
				Stage:          "building",
				MatrixJobCount: 2,
			}},
		},
		// 41. CompletedPRRunDoesNotTimeout
		{
			name: "CompletedPRRunDoesNotTimeout",
			containers: []DockerContainerJSON{
				{ID: "c1", Names: []string{"/aerial-brain"}, State: "running", Created: refTime.Add(-10 * time.Hour).Unix(), Labels: map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"}},
			},
			runs: []GitHubRun{
				{ID: 9002, Repository: "azylman/aerial-sidecars", Name: "Continuous Delivery", Event: "pull_request", HeadBranch: "feat/sidecars-pr", HeadSHA: "aabbccdd1122", Status: "completed", Conclusion: "success", CreatedAt: refTime.Add(-300 * time.Second), UpdatedAt: refTime.Add(-200 * time.Second), HTMLURL: "https://github.com/azylman/aerial-sidecars/actions/runs/9002"},
			},
			jobs: map[int64][]GitHubJob{
				9002: {{Name: "Build & Push Sidecar Images to GHCR (aura-farming)", Status: "completed", Conclusion: "success"}},
			},
			currentCommit: "oldcorecommit",
			refTime:       refTime,
			wantCount:     0,
		},
		// 42. ReconTargetServices_MatchingContainerWithoutCommitLabel
		{
			name: "ReconTargetServices_MatchingContainerWithoutCommitLabel",
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:         true,
					State:          "pulling",
					CommitSHA:      "8888888888888888888888888888888888888888",
					TargetServices: []string{"proxy"},
					StartedAt:      refTime.Add(-15 * time.Second),
				},
			},
			containers: []DockerContainerJSON{
				{
					ID:      "p1",
					Names:   []string{"/aerial-proxy"},
					State:   "running",
					Created: refTime.Add(-10 * time.Second).Unix(),
					Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "proxy"},
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Commit:           "8888888",
				MatrixJobCount:   1,
				HasContainerChip: true,
			}},
		},
		// 43. ReconShortSteps_FailedState
		{
			name: "ReconShortSteps_FailedState",
			gitSync: GitSyncStatusResponse{
				Status: "error",
				Reconciliation: &ReconciliationStatus{
					Active:    false,
					State:     "failed",
					Error:     "docker compose build failed",
					CommitSHA: "9999999999999999999999999999999999999999",
					StartedAt: refTime.Add(-2 * time.Minute),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage:           "failed",
				Progress:        55,
				CommitMsgSubstr: "docker compose build failed",
				StepStatuses:    []string{"completed", "completed", "failed", "pending", "pending"},
			}},
		},
		// 44. ReconEmptyCommitSHA_FallbackToHangar
		{
			name: "ReconEmptyCommitSHA_FallbackToHangar",
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    true,
					State:     "pulling",
					CommitSHA: "",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				ID:     "dep-aerial-stack-hangar",
				Commit: "dep-aerial-stack-hangar",
			}},
		},
		// 45. ReconEmptyCommitSHA_FallbackToRuns
		{
			name: "ReconEmptyCommitSHA_FallbackToRuns",
			runs: []GitHubRun{
				{
					ID:        701,
					HeadSHA:   "abcdef0123456789012345678901234567890123",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-1 * time.Minute),
				},
			},
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    true,
					State:     "pulling",
					CommitSHA: "",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			currentCommit: "ignored",
			refTime:       refTime,
			wantList: []wantDep{{
				Commit: "abcdef0",
			}},
		},
		// 46. ReconEmptyCommitSHA_FallbackToCurrentCommit
		{
			name: "ReconEmptyCommitSHA_FallbackToCurrentCommit",
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    true,
					State:     "pulling",
					CommitSHA: "",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			currentCommit: "fedcba987654321",
			refTime:       refTime,
			wantList: []wantDep{{
				Commit: "fedcba9",
			}},
		},
		// 47. ReconExistingRun_SwappingState
		{
			name: "ReconExistingRun_SwappingState",
			runs: []GitHubRun{
				{
					ID:        777,
					HeadSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-2 * time.Minute),
				},
			},
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:         true,
					State:          "swapping",
					CommitSHA:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					TargetServices: []string{"brain"},
					StartedAt:      refTime.Add(-30 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage: "swapping",
				StepStatusMap: map[int]string{
					3: "active",
					4: "active",
				},
			}},
		},
		// 48. ReconExistingRun_FailedState
		{
			name: "ReconExistingRun_FailedState",
			runs: []GitHubRun{
				{
					ID:        778,
					HeadSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-2 * time.Minute),
				},
			},
			gitSync: GitSyncStatusResponse{
				Status: "error",
				Reconciliation: &ReconciliationStatus{
					Active:    false,
					State:     "failed",
					Error:     "host crash",
					CommitSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage: "failed",
				StepStatusMap: map[int]string{
					2: "failed",
				},
			}},
		},
		// 49. ReconExistingRun_HealthyState
		{
			name: "ReconExistingRun_HealthyState",
			runs: []GitHubRun{
				{
					ID:        779,
					HeadSHA:   "cccccccccccccccccccccccccccccccccccccccc",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-2 * time.Minute),
				},
			},
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    false,
					State:     "healthy",
					CommitSHA: "cccccccccccccccccccccccccccccccccccccccc",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage:        "building",
				Progress:     25,
				StepStatuses: []string{"completed", "completed", "completed", "completed", "completed"},
			}},
		},
		// 50. ReconShortSteps_HealthyState
		{
			name: "ReconShortSteps_HealthyState",
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    false,
					State:     "healthy",
					CommitSHA: "cccccccccccccccccccccccccccccccccccccccc",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			refTime:   refTime,
			wantCount: 0,
		},
		// 51. ReconShortSteps_PullingState
		{
			name: "ReconShortSteps_PullingState",
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    true,
					State:     "pulling",
					CommitSHA: "dddddddddddddddddddddddddddddddddddddddd",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage:        "pulling",
				Progress:     55,
				StepStatuses: []string{"completed", "completed", "active", "pending", "pending"},
			}},
		},
		// 52. ReconExistingRun_PullingState
		{
			name: "ReconExistingRun_PullingState",
			runs: []GitHubRun{
				{
					ID:        780,
					HeadSHA:   "dddddddddddddddddddddddddddddddddddddddd",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-2 * time.Minute),
				},
			},
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    true,
					State:     "pulling",
					CommitSHA: "dddddddddddddddddddddddddddddddddddddddd",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage:    "pulling",
				Progress: 55,
				StepStatusMap: map[int]string{
					1: "completed",
					2: "active",
				},
			}},
		},
		// 53. ReconShortSteps_SwappingState
		{
			name: "ReconShortSteps_SwappingState",
			gitSync: GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:    true,
					State:     "swapping",
					CommitSHA: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
					StartedAt: refTime.Add(-10 * time.Second),
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Stage:        "swapping",
				Progress:     60,
				StepStatuses: []string{"completed", "completed", "completed", "active", "active"},
			}},
		},
		// 54. 40CharSHARun_MatchedBy7CharPrefixContainer
		{
			name: "Prefix40CharSHA_MatchedBy7CharPrefixContainer",
			runs: []GitHubRun{
				{
					ID:        888,
					HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					Status:    "in_progress",
					CreatedAt: refTime.Add(-3 * time.Minute),
				},
			},
			containers: []DockerContainerJSON{
				{
					ID:      "c2",
					Names:   []string{"/aerial-proxy"},
					Created: refTime.Add(-20 * time.Second).Unix(),
					State:   "running",
					Labels: map[string]string{
						"com.docker.compose.project":        "aerial",
						"com.docker.compose.service":        "proxy",
						"org.opencontainers.image.revision": "bbbbbbb",
					},
				},
			},
			refTime: refTime,
			wantList: []wantDep{{
				Commit: "bbbbbbb",
			}},
		},
		// 55. SortTieBreaker_FailedPrecedesDegraded_BothPositions
		{
			name: "SortTieBreaker_FailedPrecedesDegraded_BothPositions",
			runs: []GitHubRun{
				{
					ID:         901,
					HeadSHA:    "1111111111111111111111111111111111111111",
					Status:     "completed",
					Conclusion: "failure",
					CreatedAt:  refTime.Add(-5 * time.Minute),
					UpdatedAt:  refTime.Add(-5 * time.Minute),
				},
				{
					ID:         902,
					HeadSHA:    "2222222222222222222222222222222222222222",
					Status:     "completed",
					Conclusion: "success",
					CreatedAt:  refTime.Add(-5 * time.Minute),
					UpdatedAt:  refTime.Add(-5 * time.Minute),
				},
			},
			containers: []DockerContainerJSON{
				{
					Names:   []string{"/aerial-brain"},
					Created: refTime.Add(-5 * time.Minute).Unix(),
					State:   "exited",
					Labels: map[string]string{
						"org.opencontainers.image.revision": "2222222222222222222222222222222222222222",
					},
				},
			},
			refTime: refTime,
			wantList: []wantDep{
				{Stage: "failed", Commit: "1111111"},
				{Stage: "degraded", Commit: "2222222"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			deps := MergeClusterDeploymentsWithHangar(tt.runs, tt.jobs, tt.gitSync, tt.containers, tt.currentCommit, tt.refTime)
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
			if tt.extraCheck != nil {
				tt.extraCheck(t, deps)
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
		{
			name:      "nil_labels",
			container: DockerContainerJSON{Labels: nil},
			want:      "",
		},
		{
			name:      "empty_labels",
			container: DockerContainerJSON{Labels: map[string]string{}},
			want:      "",
		},
		{
			name: "opencontainers_revision_priority",
			container: DockerContainerJSON{
				Labels: map[string]string{
					"org.opencontainers.image.revision": "1111111111111111111111111111111111111111",
					"aerial.commit_sha":                 "2222222222222222222222222222222222222222",
					"vcs-ref":                           "3333333333333333333333333333333333333333",
				},
			},
			want: "1111111111111111111111111111111111111111",
		},
		{
			name: "aerial_commit_sha_fallback",
			container: DockerContainerJSON{
				Labels: map[string]string{
					"aerial.commit_sha": "2222222222222222222222222222222222222222",
					"vcs-ref":           "3333333333333333333333333333333333333333",
				},
			},
			want: "2222222222222222222222222222222222222222",
		},
		{
			name: "vcs_ref_fallback",
			container: DockerContainerJSON{
				Labels: map[string]string{
					"vcs-ref": "3333333333333333333333333333333333333333",
				},
			},
			want: "3333333333333333333333333333333333333333",
		},
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
		{
			name: "Docker Compose container",
			container: DockerContainerJSON{
				Names: []string{"/aerial-brain-1"},
				Labels: map[string]string{
					"com.docker.compose.project": "aerial",
					"com.docker.compose.service": "brain",
				},
			},
			expectedSvc: "brain",
			expectedOk:  true,
		},
		{
			name: "Foreign Docker Compose project container",
			container: DockerContainerJSON{
				Names: []string{"/homeassistant-web-1"},
				Labels: map[string]string{
					"com.docker.compose.project": "homeassistant",
					"com.docker.compose.service": "web",
				},
			},
			expectedSvc: "",
			expectedOk:  false,
		},
		{
			name: "Nomad container with aerial.service label",
			container: DockerContainerJSON{
				Names: []string{"/scheduler-mcp-6f0e3a1d-8af5-f530-2ca8-eee83dfac17e"},
				Labels: map[string]string{
					"aerial.service":               "scheduler-mcp",
					"com.hashicorp.nomad.alloc_id": "6f0e3a1d-8af5-f530-2ca8-eee83dfac17e",
				},
			},
			expectedSvc: "scheduler-mcp",
			expectedOk:  true,
		},
		{
			name: "Nomad container with alloc_id in name",
			container: DockerContainerJSON{
				Names: []string{"/victoriametrics-mcp-6ca68da8-1599-0516-4c28-635658025294"},
				Labels: map[string]string{
					"com.hashicorp.nomad.alloc_id": "6ca68da8-1599-0516-4c28-635658025294",
				},
			},
			expectedSvc: "victoriametrics-mcp",
			expectedOk:  true,
		},
		{
			name: "Legacy container with aerial- prefix",
			container: DockerContainerJSON{
				Names: []string{"/aerial-proxy"},
			},
			expectedSvc: "proxy",
			expectedOk:  true,
		},
		{
			name: "Host supervisor container (non-Aerial)",
			container: DockerContainerJSON{
				Names: []string{"/hassio_supervisor"},
				Labels: map[string]string{
					"io.hass.type": "supervisor",
				},
			},
			expectedSvc: "",
			expectedOk:  false,
		},
		{
			name: "Host addon container (non-Aerial)",
			container: DockerContainerJSON{
				Names: []string{"/app_core_mosquitto"},
				Labels: map[string]string{
					"supervisor_managed": "",
				},
			},
			expectedSvc: "",
			expectedOk:  false,
		},
		{
			name: "Empty container metadata",
			container: DockerContainerJSON{
				Names:  nil,
				Labels: nil,
				State:  "running",
			},
			expectedSvc: "",
			expectedOk:  false,
		},
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
	now := time.Date(2026, 10, 3, 20, 0, 0, 0, time.UTC)
	mustJSON := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("failed to marshal JSON: %v", err)
		}
		return b
	}

	tests := []struct {
		name       string
		jobsData   []byte
		allocsData []byte
		now        time.Time
		wantErr    bool
		wantCount  int
		validate   func(t *testing.T, services []ServiceStatus)
	}{
		{
			name: "healthy services with deployment",
			jobsData: mustJSON([]NomadJobItem{
				{ID: "brain", Type: "service", Status: "running"},
			}),
			allocsData: mustJSON([]NomadAllocItem{
				{
					ID:           "alloc-brain",
					JobID:        "brain",
					ClientStatus: "running",
					CreateTime:   now.Add(-60 * time.Second).UnixNano(),
					DeploymentStatus: &NomadDeploymentStatus{
						Healthy: true,
					},
				},
			}),
			now:       now,
			wantCount: 1,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Name != "brain" || services[0].Status != "healthy" || services[0].UptimeSeconds != 60 {
					t.Errorf("unexpected service status: %+v", services[0])
				}
			},
		},
		{
			name: "starting service with deployment healthy=false and running task",
			jobsData: mustJSON([]NomadJobItem{
				{ID: "hangar", Type: "service", Status: "running"},
			}),
			allocsData: mustJSON([]NomadAllocItem{
				{
					ID:           "alloc-hangar",
					JobID:        "hangar",
					ClientStatus: "running",
					CreateTime:   now.Add(-10 * time.Second).UnixNano(),
					DeploymentStatus: &NomadDeploymentStatus{
						Healthy: false,
					},
					TaskStates: map[string]NomadTaskState{
						"task1": {State: "running"},
					},
				},
			}),
			now:       now,
			wantCount: 1,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Status != "starting" || services[0].UptimeSeconds != 10 {
					t.Errorf("expected starting service with 10s uptime, got: %+v", services)
				}
			},
		},
		{
			name: "starting service with deployment healthy=false and pending task",
			jobsData: mustJSON([]NomadJobItem{
				{ID: "hangar", Type: "service", Status: "running"},
			}),
			allocsData: mustJSON([]NomadAllocItem{
				{
					ID:           "alloc-hangar",
					JobID:        "hangar",
					ClientStatus: "running",
					CreateTime:   now.Add(-10 * time.Second).UnixNano(),
					DeploymentStatus: &NomadDeploymentStatus{
						Healthy: false,
					},
					TaskStates: map[string]NomadTaskState{
						"task1": {State: "pending"},
					},
				},
			}),
			now:       now,
			wantCount: 1,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Status != "starting" {
					t.Errorf("expected starting for pending task, got: %+v", services)
				}
			},
		},
		{
			name: "unhealthy service with dead task",
			jobsData: mustJSON([]NomadJobItem{
				{ID: "hangar", Type: "service", Status: "running"},
			}),
			allocsData: mustJSON([]NomadAllocItem{
				{
					ID:           "alloc-hangar",
					JobID:        "hangar",
					ClientStatus: "running",
					CreateTime:   now.Add(-10 * time.Second).UnixNano(),
					DeploymentStatus: &NomadDeploymentStatus{
						Healthy: false,
					},
					TaskStates: map[string]NomadTaskState{
						"task1": {State: "dead"},
					},
				},
			}),
			now:       now,
			wantCount: 1,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Status != "unhealthy" {
					t.Errorf("expected unhealthy for dead task, got: %+v", services)
				}
			},
		},
		{
			name: "unhealthy service with failed task or no allocs",
			jobsData: mustJSON([]NomadJobItem{
				{ID: "svc-failed", Type: "service", Status: "running"},
				{ID: "svc-no-alloc", Type: "service", Status: "dead"},
			}),
			allocsData: mustJSON([]NomadAllocItem{
				{
					ID:           "alloc-failed",
					JobID:        "svc-failed",
					ClientStatus: "running",
					CreateTime:   now.Add(-30 * time.Second).UnixNano(),
					TaskStates: map[string]NomadTaskState{
						"task1": {State: "running", Failed: true},
					},
				},
			}),
			now:       now,
			wantCount: 2,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Name != "svc-failed" || services[0].Status != "unhealthy" {
					t.Errorf("unexpected svc-failed status: %+v", services[0])
				}
				if services[1].Name != "svc-no-alloc" || services[1].Status != "unhealthy" || services[1].UptimeSeconds != 0 {
					t.Errorf("unexpected svc-no-alloc status: %+v", services[1])
				}
			},
		},
		{
			name: "nil deployment status with healthy and unhealthy tasks",
			jobsData: mustJSON([]NomadJobItem{
				{ID: "proxy", Type: "service", Status: "running"},
				{ID: "gateway", Type: "service", Status: "running"},
			}),
			allocsData: mustJSON([]NomadAllocItem{
				{
					ID:               "alloc-proxy",
					JobID:            "proxy",
					ClientStatus:     "running",
					CreateTime:       now.Add(-100 * time.Second).UnixNano(),
					DeploymentStatus: nil,
					TaskStates: map[string]NomadTaskState{
						"proxy-task": {State: "running", Failed: false},
					},
				},
				{
					ID:               "alloc-gateway",
					JobID:            "gateway",
					ClientStatus:     "running",
					CreateTime:       now.Add(-100 * time.Second).UnixNano(),
					DeploymentStatus: nil,
					TaskStates: map[string]NomadTaskState{
						"gateway-task": {State: "pending", Failed: false},
					},
				},
			}),
			now:       now,
			wantCount: 2,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Name != "gateway" || services[0].Status != "unhealthy" {
					t.Errorf("expected gateway unhealthy, got: %+v", services[0])
				}
				if services[1].Name != "proxy" || services[1].Status != "healthy" || services[1].UptimeSeconds != 100 {
					t.Errorf("expected proxy healthy, got: %+v", services[1])
				}
			},
		},
		{
			name: "no running alloc with pending status or starting group",
			jobsData: mustJSON([]NomadJobItem{
				{
					ID:     "pending-svc",
					Type:   "service",
					Status: "pending",
				},
				{
					ID:     "starting-svc",
					Type:   "service",
					Status: "running",
					JobSummary: struct {
						Summary map[string]NomadJobSummaryGroup `json:"Summary"`
					}{
						Summary: map[string]NomadJobSummaryGroup{
							"web": {Starting: 1},
						},
					},
				},
			}),
			allocsData: []byte("[]"),
			now:        now,
			wantCount:  2,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Name != "pending-svc" || services[0].Status != "starting" {
					t.Errorf("expected pending-svc starting, got: %+v", services[0])
				}
				if services[1].Name != "starting-svc" || services[1].Status != "starting" {
					t.Errorf("expected starting-svc starting, got: %+v", services[1])
				}
			},
		},
		{
			name: "multiple running allocs selects highest CreateTime and filters batch",
			jobsData: mustJSON([]NomadJobItem{
				{ID: "svc-multi", Type: "service", Status: "running"},
				{ID: "batch-ignored", Type: "batch", Status: "running"},
				{ID: "", Name: "named-only", Type: "service", Status: "running"},
				{ID: "", Name: "", Type: "service", Status: "running"},
			}),
			allocsData: mustJSON([]NomadAllocItem{
				{
					ID:           "alloc-old",
					JobID:        "svc-multi",
					ClientStatus: "running",
					CreateTime:   now.Add(-200 * time.Second).UnixNano(),
					DeploymentStatus: &NomadDeploymentStatus{
						Healthy: false,
					},
				},
				{
					ID:           "alloc-new",
					JobID:        "svc-multi",
					ClientStatus: "running",
					CreateTime:   now.Add(-40 * time.Second).UnixNano(),
					DeploymentStatus: &NomadDeploymentStatus{
						Healthy: true,
					},
				},
				{
					ID:           "alloc-batch",
					JobID:        "batch-ignored",
					ClientStatus: "running",
					CreateTime:   now.Add(-10 * time.Second).UnixNano(),
				},
				{
					ID:           "alloc-named",
					JobID:        "named-only",
					ClientStatus: "running",
					CreateTime:   now.Add(-15 * time.Second).UnixNano(),
					DeploymentStatus: &NomadDeploymentStatus{
						Healthy: true,
					},
				},
				{
					ID:           "alloc-nonrunning",
					JobID:        "svc-multi",
					ClientStatus: "complete",
					CreateTime:   now.Add(-5 * time.Second).UnixNano(),
				},
			}),
			now:       time.Time{},
			wantCount: 2,
			validate: func(t *testing.T, services []ServiceStatus) {
				if services[0].Name != "named-only" || services[0].Status != "healthy" {
					t.Errorf("unexpected named-only: %+v", services[0])
				}
				if services[1].Name != "svc-multi" || services[1].Status != "healthy" {
					t.Errorf("unexpected svc-multi: %+v", services[1])
				}
			},
		},
		{
			name:       "malformed jobs JSON returns error",
			jobsData:   []byte("not valid json"),
			allocsData: []byte("[]"),
			now:        now,
			wantErr:    true,
		},
		{
			name:       "malformed allocs JSON returns error",
			jobsData:   []byte("[]"),
			allocsData: []byte("not valid json"),
			now:        now,
			wantErr:    true,
		},
		{
			name:       "empty payloads returns empty slice",
			jobsData:   []byte("[]"),
			allocsData: []byte("[]"),
			now:        now,
			wantCount:  0,
		},
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
			if tt.validate != nil {
				tt.validate(t, services)
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
