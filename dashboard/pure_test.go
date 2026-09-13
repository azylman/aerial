package main

import (
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
				if got != "image/x-icon" {
					t.Errorf("expected image/x-icon, got %q", got)
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
				if got != "font/woff2" {
					t.Errorf("expected font/woff2, got %q", got)
				}
			},
		},
		{
			path: "font.woff",
			validator: func(t *testing.T, got string) {
				if got != "font/woff" {
					t.Errorf("expected font/woff, got %q", got)
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
