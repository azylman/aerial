package main

import (
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
		{
			name:    "empty base URL errors",
			baseURL: "",
			query:   url.Values{},
			wantErr: true,
		},
		{
			name:    "control characters in base URL error",
			baseURL: "http://brain:\x7f8080",
			query:   url.Values{},
			wantErr: true,
		},
		{
			name:       "trailing slash normalized",
			baseURL:    "http://brain:8080/",
			query:      url.Values{},
			wantLimit:  0,
			wantOffset: 0,
			wantErr:    false,
			omitQuery:  []string{"limit", "offset", "category", "q"},
		},
		{
			name:       "nil query handled cleanly",
			baseURL:    "http://brain:8080",
			query:      nil,
			wantLimit:  0,
			wantOffset: 0,
			wantErr:    false,
		},
		{
			name:    "positive limit and offset forwarded",
			baseURL: "http://brain:8080",
			query: url.Values{
				"limit":  []string{"25"},
				"offset": []string{"10"},
			},
			wantLimit:  25,
			wantOffset: 10,
			wantErr:    false,
			wantQuery: map[string]string{
				"limit":  "25",
				"offset": "10",
			},
		},
		{
			name:    "zero or negative limit and offset omitted",
			baseURL: "http://brain:8080",
			query: url.Values{
				"limit":  []string{"0"},
				"offset": []string{"0"},
			},
			wantLimit:  0,
			wantOffset: 0,
			wantErr:    false,
			omitQuery:  []string{"limit", "offset"},
		},
		{
			name:    "negative or invalid limit and offset omitted",
			baseURL: "http://brain:8080",
			query: url.Values{
				"limit":  []string{"-5"},
				"offset": []string{"invalid"},
			},
			wantLimit:  0,
			wantOffset: 0,
			wantErr:    false,
			omitQuery:  []string{"limit", "offset"},
		},
		{
			name:    "category and q trimmed and forwarded",
			baseURL: "http://brain:8080",
			query: url.Values{
				"category": []string{"  system  "},
				"q":        []string{"  docker deploy  "},
			},
			wantErr: false,
			wantQuery: map[string]string{
				"category": "system",
				"q":        "docker deploy",
			},
		},
		{
			name:    "empty category and q omitted",
			baseURL: "http://brain:8080",
			query: url.Values{
				"category": []string{"   "},
				"q":        []string{"   "},
			},
			wantErr:   false,
			omitQuery: []string{"category", "q"},
		},
		{
			name:    "long ASCII q clamped to 64 runes",
			baseURL: "http://brain:8080",
			query: url.Values{
				"q": []string{strings.Repeat("a", 100)},
			},
			wantErr: false,
			wantQuery: map[string]string{
				"q": strings.Repeat("a", 64),
			},
		},
		{
			name:    "multibyte unicode kanji clamped to 64 runes without UTF-8 corruption",
			baseURL: "http://brain:8080",
			query: url.Values{
				// 70 kanji characters (each 3 bytes in UTF-8)
				"q": []string{strings.Repeat("電", 70)},
			},
			wantErr: false,
			wantQuery: map[string]string{
				"q": strings.Repeat("電", 64),
			},
		},
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
		{
			name:    "empty base URL returns error",
			baseURL: "",
			wantErr: true,
		},
		{
			name:    "control character returns error",
			baseURL: "http://brain:\x7f8080",
			wantErr: true,
		},
		{
			name:    "clean base URL without trailing slash",
			baseURL: "http://brain:8080",
			wantURL: "http://brain:8080/schedules",
			wantErr: false,
		},
		{
			name:    "base URL with trailing slash normalized",
			baseURL: "http://brain:8080/",
			wantURL: "http://brain:8080/schedules",
			wantErr: false,
		},
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
		{
			name:    "empty base URL returns error",
			baseURL: "",
			wantErr: true,
		},
		{
			name:    "control characters in URL return error",
			baseURL: "http://brain:\x7f8080",
			wantErr: true,
		},
		{
			name:       "defaults applied when query is empty",
			baseURL:    "http://brain:8080/",
			query:      url.Values{},
			wantLimit:  50,
			wantOffset: 0,
			wantErr:    false,
			wantQuery: map[string]string{
				"limit":  "50",
				"offset": "0",
			},
		},
		{
			name:       "nil query receives defaults",
			baseURL:    "http://brain:8080",
			query:      nil,
			wantLimit:  50,
			wantOffset: 0,
			wantErr:    false,
			wantQuery: map[string]string{
				"limit":  "50",
				"offset": "0",
			},
		},
		{
			name:    "valid limit and offset within boundaries",
			baseURL: "http://brain:8080",
			query: url.Values{
				"limit":       []string{"100"},
				"offset":      []string{"20"},
				"schedule_id": []string{"  cron-test  "},
				"status":      []string{"  success  "},
			},
			wantLimit:  100,
			wantOffset: 20,
			wantErr:    false,
			wantQuery: map[string]string{
				"limit":       "100",
				"offset":      "20",
				"schedule_id": "cron-test",
				"status":      "success",
			},
		},
		{
			name:    "limit > 100 falls back to default 50",
			baseURL: "http://brain:8080",
			query: url.Values{
				"limit":  []string{"150"},
				"offset": []string{"-5"},
			},
			wantLimit:  50,
			wantOffset: 0,
			wantErr:    false,
			wantQuery: map[string]string{
				"limit":  "50",
				"offset": "0",
			},
		},
		{
			name:    "invalid non-integer limit and offset fall back to defaults",
			baseURL: "http://brain:8080",
			query: url.Values{
				"limit":  []string{"abc"},
				"offset": []string{"xyz"},
			},
			wantLimit:  50,
			wantOffset: 0,
			wantErr:    false,
			wantQuery: map[string]string{
				"limit":  "50",
				"offset": "0",
			},
		},
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
		{name: "ollama service", jobName: "Setup ollama image", expected: "ollama"},
		{name: "agentsview service", jobName: "agentsview container", expected: "agentsview"},
		{name: "unit test generic", jobName: "Unit Test Matrix", expected: "unit-tests"},
		{name: "test generic", jobName: "Service Integration Test", expected: "unit-tests"},
		{name: "lint job", jobName: "GolangCI Lint Run", expected: "lint"},
		{name: "validate-config job", jobName: "validate-config", expected: "config"},
		{name: "Validate Configuration job", jobName: "Validate Configuration", expected: "config"},
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

func TestCalculateDeployStatus_Pulling(t *testing.T) {
	if got := IsDeployOngoing("pulling"); !got {
		t.Fatalf("expected IsDeployOngoing(\"pulling\") to be true, got false")
	}
	deps := []DeploymentStatus{
		{Stage: "building"},
		{Stage: "pulling"},
	}
	if got := CalculateDeployStatus(deps); got != "pulling" {
		t.Fatalf("expected CalculateDeployStatus to return pulling, got %s", got)
	}
}

func TestMergeClusterDeployments_ConcurrentPipelines(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	runs := []GitHubRun{
		{
			ID:         201,
			HeadSHA:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Status:     "in_progress",
			CreatedAt:  refTime.Add(-5 * time.Minute),
			UpdatedAt:  refTime.Add(-1 * time.Minute),
			HTMLURL:    "https://github.com/azylman/aerial/actions/runs/201",
			HeadCommit: &struct {
				Message   string    `json:"message"`
				Timestamp time.Time `json:"timestamp"`
			}{
				Message:   "feat: build feature B",
				Timestamp: refTime.Add(-5 * time.Minute),
			},
		},
	}
	jobs := map[int64][]GitHubJob{
		201: {
			{
				ID:        1,
				RunID:     201,
				Name:      "build (dashboard)",
				Status:    "in_progress",
				StartedAt: refTime.Add(-4 * time.Minute),
			},
		},
	}
	gitSync := GitSyncStatusResponse{
		Status: "synced",
		Reconciliation: &ReconciliationStatus{
			Active:         true,
			State:          "swapping",
			Stage:          "swapping",
			CommitSHA:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			TargetServices: []string{"proxy"},
			StartedAt:      refTime.Add(-2 * time.Minute),
		},
	}

	deps := MergeClusterDeploymentsWithHangar(runs, jobs, gitSync, nil, "aaaaaaaa", refTime)
	if len(deps) != 2 {
		t.Fatalf("expected 2 concurrent deployments, got %d", len(deps))
	}

	// Deterministic precedence: swapping (rank 6) > building (rank 4)
	if deps[0].Stage != "swapping" || deps[0].Commit != "aaaaaaa" {
		t.Errorf("expected deps[0] to be swapping/aaaaaaa, got stage=%q commit=%q", deps[0].Stage, deps[0].Commit)
	}
	if deps[1].Stage != "building" || deps[1].Commit != "bbbbbbb" {
		t.Errorf("expected deps[1] to be building/bbbbbbb, got stage=%q commit=%q", deps[1].Stage, deps[1].Commit)
	}
}

func TestMergeClusterDeployments_AwaitingPullBridge(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	runs := []GitHubRun{
		{
			ID:         301,
			HeadSHA:    "cccccccccccccccccccccccccccccccccccccccc",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  refTime.Add(-10 * time.Minute),
			UpdatedAt:  refTime.Add(-5 * time.Minute),
			HTMLURL:    "https://github.com/azylman/aerial/actions/runs/301",
			HeadCommit: &struct {
				Message   string    `json:"message"`
				Timestamp time.Time `json:"timestamp"`
			}{
				Message:   "chore: updated docs",
				Timestamp: refTime.Add(-10 * time.Minute),
			},
		},
	}

	deps := MergeClusterDeploymentsWithHangar(runs, nil, GitSyncStatusResponse{}, nil, "oldcommit", refTime)
	if len(deps) != 1 {
		t.Fatalf("expected 1 deployment in bridge state, got %d", len(deps))
	}
	dep := deps[0]
	if dep.Commit != "ccccccc" {
		t.Errorf("expected commit ccccccc, got %q", dep.Commit)
	}
	if dep.Stage != "awaiting_pull" {
		t.Errorf("expected stage awaiting_pull, got %q", dep.Stage)
	}
	if len(dep.Steps) < 5 {
		t.Fatalf("expected 5 steps, got %d", len(dep.Steps))
	}
	if dep.Steps[1].Status != "completed" {
		t.Errorf("expected Step 2 (CI Build) completed, got %q", dep.Steps[1].Status)
	}
	if dep.Steps[2].Status != "active" {
		t.Errorf("expected Step 3 (Hangar Sync) active, got %q", dep.Steps[2].Status)
	}
}

func TestMergeClusterDeployments_DeterministicSort(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	runs := []GitHubRun{
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

	gitSync := GitSyncStatusResponse{
		Status: "synced",
		Reconciliation: &ReconciliationStatus{
			Active:    true,
			State:     "pulling",
			CommitSHA: "4444444444444444444444444444444444444444",
			StartedAt: refTime.Add(-30 * time.Second),
		},
	}

	containers := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-proxy"},
			Created: refTime.Add(-40 * time.Second).Unix(), // uptime 40s < 120s -> swapping
			State:   "running",
			Labels: map[string]string{
				"com.docker.compose.project":        "aerial",
				"com.docker.compose.service":        "proxy",
				"org.opencontainers.image.revision": "1111111111111111111111111111111111111111",
			},
		},
		{
			Names:   []string{"/aerial-brain"},
			Created: refTime.Add(-300 * time.Second).Unix(), // uptime 300s -> live grace
			State:   "running",
			Labels: map[string]string{
				"com.docker.compose.project":        "aerial",
				"com.docker.compose.service":        "brain",
				"org.opencontainers.image.revision": "6666666666666666666666666666666666666666",
			},
		},
	}

	expectedStages := []string{"swapping", "pulling", "building", "awaiting_pull", "queued"}
	for i := 0; i < 10; i++ {
		deps := MergeClusterDeploymentsWithHangar(runs, nil, gitSync, containers, "6666666", refTime)
		if len(deps) != len(expectedStages) {
			t.Fatalf("iteration %d: expected %d deployments, got %d", i, len(expectedStages), len(deps))
		}
		for idx, expStage := range expectedStages {
			if deps[idx].Stage != expStage {
				t.Fatalf("iteration %d at index %d: expected stage %q, got %q (commit %s)", i, idx, expStage, deps[idx].Stage, deps[idx].Commit)
			}
		}
	}
}

func TestMergeClusterDeployments_MultiCommitContainerGrouping(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	containers := []DockerContainerJSON{
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
	}

	deps := MergeClusterDeploymentsWithHangar(nil, nil, GitSyncStatusResponse{}, containers, "aaaaaaa", refTime)
	if len(deps) != 2 {
		t.Fatalf("expected 2 deployments, got %d", len(deps))
	}
	// Newer commit B is swapping (rank 6)
	if deps[0].Commit != "bbbbbbb" || deps[0].Stage != "swapping" {
		t.Errorf("expected deps[0] to be bbbbbbb in swapping, got %s in %s", deps[0].Commit, deps[0].Stage)
	}
	// Earlier commit A maintains live grace (rank 0)
	if deps[1].Commit != "aaaaaaa" || deps[1].Stage != "live" {
		t.Errorf("expected deps[1] to be aaaaaaa in live, got %s in %s", deps[1].Commit, deps[1].Stage)
	}
}

func TestMergeClusterDeployments_ChipScoping(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	runs := []GitHubRun{
		{
			ID:        401,
			HeadSHA:   "7777777aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Status:    "in_progress",
			CreatedAt: refTime.Add(-2 * time.Minute),
			UpdatedAt: refTime.Add(-1 * time.Minute),
		},
	}
	jobs := map[int64][]GitHubJob{
		401: {
			{
				ID:        1,
				RunID:     401,
				Name:      "build (brain)",
				Status:    "in_progress",
				StartedAt: refTime.Add(-90 * time.Second),
			},
		},
	}
	containers := []DockerContainerJSON{
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
	}

	deps := MergeClusterDeploymentsWithHangar(runs, jobs, GitSyncStatusResponse{}, containers, "7777777", refTime)
	if len(deps) != 1 {
		t.Fatalf("expected 1 combined deployment, got %d", len(deps))
	}
	dep := deps[0]
	hasCI := false
	hasContainer := false
	for _, chip := range dep.MatrixJobs {
		if chip.Type == "ci" {
			hasCI = true
		}
		if chip.Type == "container" {
			hasContainer = true
		}
	}
	if !hasCI {
		t.Errorf("expected CI matrix chip with Type == 'ci', but none found: %+v", dep.MatrixJobs)
	}
	if !hasContainer {
		t.Errorf("expected container chip with Type == 'container', but none found: %+v", dep.MatrixJobs)
	}
}

func TestMergeClusterDeployments_SafeNilHandling(t *testing.T) {
	now := time.Now().UTC()
	runs := []GitHubRun{
		{
			ID:         501,
			HeadSHA:    "",
			HeadCommit: nil,
			Status:     "queued",
			CreatedAt:  now.Add(-10 * time.Second),
		},
		{
			ID:         502,
			HeadSHA:    "abc", // < 7 chars
			HeadCommit: nil,
			Status:     "in_progress",
			CreatedAt:  now.Add(-20 * time.Second),
		},
		{
			ID:         503,
			HeadSHA:    "not-a-valid-hex-sha!@#$%^&*()",
			HeadCommit: nil,
			Status:     "completed",
			Conclusion: "failure",
			CreatedAt:  now.Add(-30 * time.Second),
			UpdatedAt:  now.Add(-25 * time.Second),
		},
	}

	deps := MergeClusterDeploymentsWithHangar(runs, nil, GitSyncStatusResponse{}, nil, "", time.Time{})
	if len(deps) != 3 {
		t.Fatalf("expected 3 deployments from safe nil handling test, got %d", len(deps))
	}
}

func TestMergeClusterDeployments_CardinalityClamp(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	var runs []GitHubRun
	for i := 1; i <= 8; i++ {
		runs = append(runs, GitHubRun{
			ID:        int64(600 + i),
			HeadSHA:   fmt.Sprintf("%07d000000000000000000000000000000000", i),
			Status:    "in_progress",
			CreatedAt: refTime.Add(time.Duration(-i) * time.Minute),
			UpdatedAt: refTime.Add(time.Duration(-i) * time.Minute),
		})
	}

	deps := MergeClusterDeploymentsWithHangar(runs, nil, GitSyncStatusResponse{}, nil, "", refTime)
	if len(deps) != 5 {
		t.Fatalf("expected cardinality clamped to 5, got %d", len(deps))
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

func TestMergeClusterDeployments_PullingNotPromotedToSwapping(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	gitSync := GitSyncStatusResponse{
		Status: "synced",
		Reconciliation: &ReconciliationStatus{
			Active:         true,
			State:          "pulling",
			CommitSHA:      "7777777777777777777777777777777777777777",
			StartedAt:      refTime.Add(-15 * time.Second),
			TargetServices: []string{"aerial-dashboard"},
		},
	}

	// Existing containers with uptime > 120s and healthy status (not starting)
	containers := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-dashboard"},
			Created: refTime.Add(-300 * time.Second).Unix(),
			State:   "running",
			Labels: map[string]string{
				"org.opencontainers.image.revision": "7777777777777777777777777777777777777777",
			},
		},
	}

	deps := MergeClusterDeploymentsWithHangar(nil, nil, gitSync, containers, "", refTime)
	if len(deps) == 0 {
		t.Fatalf("expected 1 deployment, got 0")
	}
	dep := deps[0]
	if dep.Stage != "pulling" {
		t.Fatalf("expected stage pulling (not promoted to swapping), got %q", dep.Stage)
	}
	if dep.Steps[2].Status != "active" {
		t.Errorf("expected Step 3 (Hangar Sync) active, got %q", dep.Steps[2].Status)
	}
	if dep.Steps[3].Status != "pending" {
		t.Errorf("expected Step 4 (Container Swap) pending, got %q", dep.Steps[3].Status)
	}
}

func TestMergeClusterDeployments_DegradedAndFailedBranches(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	// Test degraded container
	degradedContainers := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-dashboard"},
			Created: refTime.Add(-60 * time.Second).Unix(),
			State:   "exited",
			Labels: map[string]string{
				"org.opencontainers.image.revision": "8888888888888888888888888888888888888888",
			},
		},
	}
	deps := MergeClusterDeploymentsWithHangar(nil, nil, GitSyncStatusResponse{}, degradedContainers, "", refTime)
	if len(deps) == 0 || deps[0].Stage != "degraded" {
		t.Fatalf("expected degraded stage for exited container, got %+v", deps)
	}
	if deps[0].Steps[4].Status != "failed" {
		t.Errorf("expected health check failed for degraded, got %q", deps[0].Steps[4].Status)
	}

	// Test Hangar reconciliation failed with error
	gitSyncFailed := GitSyncStatusResponse{
		Reconciliation: &ReconciliationStatus{
			State:     "failed",
			CommitSHA: "9999999999999999999999999999999999999999",
			Error:     "failed to pull image: auth expired",
		},
	}
	depsFailed := MergeClusterDeploymentsWithHangar(nil, nil, gitSyncFailed, nil, "", refTime)
	if len(depsFailed) == 0 || depsFailed[0].Stage != "failed" {
		t.Fatalf("expected failed stage for Hangar error, got %+v", depsFailed)
	}
	if !strings.Contains(depsFailed[0].CommitMsg, "auth expired") {
		t.Errorf("expected error in CommitMsg, got %q", depsFailed[0].CommitMsg)
	}

	// Test Hangar reconciliation failed without error message
	gitSyncFailedNoErr := GitSyncStatusResponse{
		Reconciliation: &ReconciliationStatus{
			State:     "failed",
			CommitSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	depsFailedNoErr := MergeClusterDeploymentsWithHangar(nil, nil, gitSyncFailedNoErr, nil, "", refTime)
	if len(depsFailedNoErr) == 0 || depsFailedNoErr[0].Stage != "failed" {
		t.Fatalf("expected failed stage, got %+v", depsFailedNoErr)
	}
}

func TestMergeClusterDeployments_MultiRunSameCommitAndSortTies(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	// Two runs for same commit: older completed/success vs newer in_progress
	sameCommitRuns := []GitHubRun{
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
	}
	depsSame := MergeClusterDeploymentsWithHangar(sameCommitRuns, nil, GitSyncStatusResponse{}, nil, "", refTime)
	if len(depsSame) != 1 {
		t.Fatalf("expected 1 deduplicated deployment, got %d", len(depsSame))
	}
	// in_progress (building, rank 4) > awaiting_pull (rank 3)
	if depsSame[0].Stage != "building" {
		t.Errorf("expected higher stage building to win, got %q", depsSame[0].Stage)
	}

	// Test sort ties: same stage, same StartedAt, different commits -> lexicographical sort
	sameStageRuns := []GitHubRun{
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
	}
	depsTies := MergeClusterDeploymentsWithHangar(sameStageRuns, nil, GitSyncStatusResponse{}, nil, "", refTime)
	if len(depsTies) != 2 {
		t.Fatalf("expected 2 deployments, got %d", len(depsTies))
	}
	if depsTies[0].Commit != "eeeeeee" || depsTies[1].Commit != "fffffff" {
		t.Errorf("expected lexicographical sort order eeeeeee < fffffff, got %s, %s", depsTies[0].Commit, depsTies[1].Commit)
	}

	// Test sort tie between failed and degraded
	failedDegradedRuns := []GitHubRun{
		{
			ID:         301,
			HeadSHA:    "1111111111111111111111111111111111111111",
			Status:     "completed",
			Conclusion: "failure",
			CreatedAt:  refTime.Add(-5 * time.Minute),
			UpdatedAt:  refTime.Add(-5 * time.Minute),
		},
	}
	degradedCont := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-proxy"},
			Created: refTime.Add(-5 * time.Minute).Unix(),
			State:   "exited",
			Labels: map[string]string{
				"org.opencontainers.image.revision": "2222222222222222222222222222222222222222",
			},
		},
	}
	depsTieRanks := MergeClusterDeploymentsWithHangar(failedDegradedRuns, nil, GitSyncStatusResponse{}, degradedCont, "", refTime)
	if len(depsTieRanks) != 2 {
		t.Fatalf("expected 2 deployments, got %d", len(depsTieRanks))
	}
	if depsTieRanks[0].Stage != "failed" || depsTieRanks[1].Stage != "degraded" {
		t.Errorf("expected failed to precede degraded, got %s, %s", depsTieRanks[0].Stage, depsTieRanks[1].Stage)
	}

	// Test expired runs (>30m) ignored
	expiredRun := []GitHubRun{
		{
			ID:         401,
			HeadSHA:    "3333333333333333333333333333333333333333",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  refTime.Add(-40 * time.Minute),
			UpdatedAt:  refTime.Add(-35 * time.Minute),
		},
	}
	depsExpired := MergeClusterDeploymentsWithHangar(expiredRun, nil, GitSyncStatusResponse{}, nil, "", refTime)
	if len(depsExpired) != 0 {
		t.Errorf("expected expired run to be ignored, got %d deployments", len(depsExpired))
	}
}

func TestPure_TargetedCoverageBoost(t *testing.T) {
	refTime := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	// 1. Zero-time defaults for ParseMatrixJobChips and BuildContainerChips
	jobs := []GitHubJob{
		{
			ID:        1,
			Name:      "Build & Push Images (proxy)",
			Status:    "in_progress",
			StartedAt: time.Now().Add(-10 * time.Second),
		},
	}
	chips := ParseMatrixJobChips(jobs, time.Time{})
	if len(chips) != 1 || chips[0].Name != "proxy" {
		t.Errorf("expected 1 chip for proxy, got %+v", chips)
	}

	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: time.Now().Add(-60 * time.Second).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
		},
	}
	containerChips := BuildContainerChips(containers, time.Time{})
	if len(containerChips) != 1 || containerChips[0].Name != "brain" {
		t.Errorf("expected 1 container chip for brain, got %+v", containerChips)
	}

	// 2. url.Parse error branches using invalid ports / characters
	invalidURL := "http://[::1]:namedport"
	if _, _, _, err := BuildFactsUpstreamURL(invalidURL, nil); err == nil {
		t.Errorf("expected url.Parse error for invalid facts URL")
	}
	if _, err := BuildSchedulesUpstreamURL(invalidURL); err == nil {
		t.Errorf("expected url.Parse error for invalid schedules URL")
	}
	if _, _, _, err := BuildScheduleRunsUpstreamURL(invalidURL, nil); err == nil {
		t.Errorf("expected url.Parse error for invalid schedule runs URL")
	}

	// 3. Cardinality clamping to max 5 deployments
	var manyRuns []GitHubRun
	for i := 1; i <= 8; i++ {
		sha := fmt.Sprintf("%040d", i)
		manyRuns = append(manyRuns, GitHubRun{
			ID:        int64(1000 + i),
			HeadSHA:   sha,
			Status:    "in_progress",
			CreatedAt: refTime.Add(-time.Duration(i) * time.Minute),
		})
	}
	clampedDeps := MergeClusterDeploymentsWithHangar(manyRuns, nil, GitSyncStatusResponse{}, nil, "", refTime)
	if len(clampedDeps) != 5 {
		t.Errorf("expected exactly 5 clamped deployments, got %d", len(clampedDeps))
	}

	// 4. TargetServices matching container without commit label
	syncWithTarget := GitSyncStatusResponse{
		Status: "synced",
		Reconciliation: &ReconciliationStatus{
			Active:         true,
			State:          "pulling",
			CommitSHA:      "8888888888888888888888888888888888888888",
			TargetServices: []string{"proxy"},
			StartedAt:      refTime.Add(-15 * time.Second),
		},
	}
	proxyWithoutLabel := []DockerContainerJSON{
		{
			ID:      "p1",
			Names:   []string{"/aerial-proxy"},
			State:   "running",
			Created: refTime.Add(-10 * time.Second).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "proxy"},
		},
	}
	targetDeps := MergeClusterDeploymentsWithHangar(nil, nil, syncWithTarget, proxyWithoutLabel, "", refTime)
	if len(targetDeps) == 0 || targetDeps[0].Commit != "8888888" || len(targetDeps[0].MatrixJobs) == 0 {
		t.Errorf("expected deployment with target container chip, got %+v", targetDeps)
	}

	// 5. Short steps (< 5) coverage on Hangar reconciliation failure
	failedSync := GitSyncStatusResponse{
		Status: "error",
		Reconciliation: &ReconciliationStatus{
			Active:    false,
			State:     "failed",
			Error:     "docker compose build failed",
			CommitSHA: "9999999999999999999999999999999999999999",
			StartedAt: refTime.Add(-2 * time.Minute),
		},
	}
	failedDeps := MergeClusterDeploymentsWithHangar(nil, nil, failedSync, nil, "", refTime)
	if len(failedDeps) == 0 || failedDeps[0].Stage != "failed" || len(failedDeps[0].Steps) != 5 {
		t.Errorf("expected failed deployment with 5 steps, got %+v", failedDeps)
	}

	// 6. Reconciliation with empty CommitSHA and empty currentCommit -> fallback to "hangar"
	emptyReconSync := GitSyncStatusResponse{
		Status: "synced",
		Reconciliation: &ReconciliationStatus{
			Active:    true,
			State:     "pulling",
			CommitSHA: "",
			StartedAt: refTime.Add(-10 * time.Second),
		},
	}
	emptyReconDeps := MergeClusterDeploymentsWithHangar(nil, nil, emptyReconSync, nil, "", refTime)
	if len(emptyReconDeps) == 0 || emptyReconDeps[0].Commit != "dep-aerial-stack-hangar" {
		t.Errorf("expected fallback to commit dep-aerial-stack-hangar, got %+v", emptyReconDeps)
	}

	// 7. Pre-existing run (with 5 steps) updated by reconciliation swapping and failure
	sameSHA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	preRun := []GitHubRun{
		{
			ID:        777,
			HeadSHA:   sameSHA,
			Status:    "in_progress",
			CreatedAt: refTime.Add(-2 * time.Minute),
		},
	}
	swapSync := GitSyncStatusResponse{
		Status: "synced",
		Reconciliation: &ReconciliationStatus{
			Active:         true,
			State:          "swapping",
			CommitSHA:      sameSHA,
			TargetServices: []string{"brain"},
			StartedAt:      refTime.Add(-30 * time.Second),
		},
	}
	swappedDeps := MergeClusterDeploymentsWithHangar(preRun, nil, swapSync, nil, "", refTime)
	if len(swappedDeps) == 0 || swappedDeps[0].Stage != "swapping" || swappedDeps[0].Steps[3].Status != "active" {
		t.Errorf("expected existing steps updated to swapping, got %+v", swappedDeps)
	}

	// 8. Pre-existing run updated by reconciliation failure
	failedExistingSync := GitSyncStatusResponse{
		Status: "error",
		Reconciliation: &ReconciliationStatus{
			Active:    false,
			State:     "failed",
			Error:     "host crash",
			CommitSHA: sameSHA,
			StartedAt: refTime.Add(-10 * time.Second),
		},
	}
	failedExistingDeps := MergeClusterDeploymentsWithHangar(preRun, nil, failedExistingSync, nil, "", refTime)
	if len(failedExistingDeps) == 0 || failedExistingDeps[0].Steps[2].Status != "failed" {
		t.Errorf("expected existing step 2 failed, got %+v", failedExistingDeps)
	}

	// 9. 40-char SHA run matched by 7-char prefix container
	prefixRun := []GitHubRun{
		{
			ID:        888,
			HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Status:    "in_progress",
			CreatedAt: refTime.Add(-3 * time.Minute),
		},
	}
	prefixCont := []DockerContainerJSON{
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
	}
	prefixDeps := MergeClusterDeploymentsWithHangar(prefixRun, nil, GitSyncStatusResponse{}, prefixCont, "", refTime)
	if len(prefixDeps) == 0 || prefixDeps[0].Commit != "bbbbbbb" {
		t.Errorf("expected prefix match for container, got %+v", prefixDeps)
	}

	// 10. Degraded vs Failed tie-breaking in sort where degraded is element i and failed is element j
	degradedFirstRuns := []GitHubRun{
		{
			ID:         901,
			HeadSHA:    "1111111111111111111111111111111111111111",
			Status:     "completed",
			Conclusion: "failure",
			CreatedAt:  refTime.Add(-5 * time.Minute),
			UpdatedAt:  refTime.Add(-5 * time.Minute),
		},
	}
	degradedContOnly := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-brain"},
			Created: refTime.Add(-5 * time.Minute).Unix(),
			State:   "exited",
			Labels: map[string]string{
				"org.opencontainers.image.revision": "2222222222222222222222222222222222222222",
			},
		},
	}
	tieDeps := MergeClusterDeploymentsWithHangar(degradedFirstRuns, nil, GitSyncStatusResponse{}, degradedContOnly, "", refTime)
	if len(tieDeps) != 2 || tieDeps[0].Stage != "failed" || tieDeps[1].Stage != "degraded" {
		t.Errorf("expected failed to sort before degraded, got %+v", tieDeps)
	}
}

