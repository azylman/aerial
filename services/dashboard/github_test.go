package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMergeClusterDeployments_WrapperSmoke(t *testing.T) {
	t.Parallel()
	deploys := mergeClusterDeploymentsWithHangar(
		[]DockerContainerJSON{{ID: "c1", Names: []string{"/aerial-brain"}, State: "running"}},
		[]GitHubRun{{ID: 1, HeadSHA: "abc1234567", Status: "in_progress"}},
		nil,
		GitSyncStatusResponse{},
		"abc1234",
	)
	if len(deploys) != 1 || deploys[0].Stage != "building" {
		t.Fatalf("unexpected wrapper deployment output: %+v", deploys)
	}
}

func TestGitHubPoller_PollAndDecode(t *testing.T) {
	runID := int64(987654)
	prRunJSON := `{
		"total_count": 1,
		"workflow_runs": [
			{
				"id": 9001,
				"name": "Continuous Delivery",
				"head_branch": "feat/sidecars-pr",
				"head_sha": "aabbccdd1122",
				"event": "pull_request",
				"status": "in_progress",
				"conclusion": null,
				"created_at": "2026-09-19T02:48:54Z",
				"updated_at": "2026-09-19T02:49:34Z",
				"html_url": "https://github.com/azylman/aerial-sidecars/actions/runs/9001",
				"repository": {
					"id": 1350916489,
					"name": "aerial-sidecars",
					"full_name": "azylman/aerial-sidecars"
				}
			}
		]
	}`

	tests := []struct {
		name             string
		pollerKind       string
		repos            []string
		runsStatus       int
		runsBody         string
		runsETag         string
		jobsStatus       int
		jobsBody         string
		jobsETag         string
		initialCachedJob int64
		initialOldRepo   string
		cancelCtx        bool
		testSecondPoll   bool
		testFetchJobs    bool
		wantActive       bool
		wantRunCount     int
		wantRunID        int64
		wantJobCount     int
		wantRepoName     string
		wantEvent        string
	}{
		{
			name:     "StandardPollAndETagJobsCaching",
			repos:    []string{"azylman/aerial"},
			runsETag: "runs-etag-1",
			jobsETag: "job-etag-1",
			runsBody: fmt.Sprintf(`{
				"total_count": 1,
				"workflow_runs": [
					{
						"id": %d,
						"name": "CI",
						"head_branch": "main",
						"head_sha": "abcdef012345",
						"status": "in_progress",
						"html_url": "https://github.com/azylman/aerial/actions/runs/%d",
						"created_at": "%s",
						"updated_at": "%s",
						"head_commit": {
							"message": "%s",
							"timestamp": "%s"
						}
					}
				]
			}`, runID, runID, time.Now().Add(-10*time.Minute).Format(time.RFC3339), time.Now().Format(time.RFC3339), strings.Repeat("Very long commit message that exceeds seventy-two runes in length so it gets truncated", 2), time.Now().Format(time.RFC3339)),
			jobsBody: fmt.Sprintf(`{
				"total_count": 1,
				"jobs": [
					{
						"id": 111,
						"run_id": %d,
						"name": "build-backend (brain)",
						"status": "completed",
						"conclusion": "success",
						"started_at": "%s"
					}
				]
			}`, runID, time.Now().Add(-5*time.Minute).Format(time.RFC3339)),
			initialCachedJob: 999,
			testSecondPoll:   true,
			wantActive:       true,
			wantRunCount:     1,
			wantRunID:        runID,
			wantJobCount:     1,
		},
		{
			name:         "PullRequestRunsAndRepositoryObjectDecoding",
			pollerKind:   "multi",
			repos:        []string{"azylman/aerial-sidecars"},
			runsBody:     prRunJSON,
			jobsBody:     `{"total_count": 1, "jobs": [{"id": 10, "run_id": 9001, "name": "build", "status": "in_progress"}]}`,
			wantActive:   true,
			wantRunCount: 1,
			wantRunID:    9001,
			wantJobCount: 1,
			wantRepoName: "azylman/aerial-sidecars",
			wantEvent:    "pull_request",
		},
		{
			name:         "DecodeError",
			pollerKind:   "multi",
			repos:        []string{"repo1", "repo2"},
			runsBody:     `{not valid json`,
			wantActive:   false,
			wantRunCount: 0,
		},
		{
			name:       "RateLimit429",
			pollerKind: "multi",
			repos:      []string{"repo1", "repo2"},
			runsStatus: http.StatusTooManyRequests,
			runsBody:   `{"message": "rate limit exceeded"}`,
			wantActive: false,
		},
		{
			name:       "InternalServerError500",
			pollerKind: "multi",
			repos:      []string{"repo1", "repo2"},
			runsStatus: http.StatusInternalServerError,
			runsBody:   `{"message": "internal server error"}`,
			wantActive: false,
		},
		{
			name:          "NetworkConnectionFailure",
			pollerKind:    "unreachable",
			testFetchJobs: true,
			wantActive:    false,
		},
		{
			name:          "FetchJobs_EmptyRepo",
			pollerKind:    "empty_repo",
			testFetchJobs: true,
		},
		{
			name:          "FetchJobs_304NotModified",
			repos:         []string{"azylman/aerial"},
			jobsETag:      "test-etag",
			testFetchJobs: true,
		},
		{
			name:          "ContextCancellationBranches",
			repos:         []string{"azylman/aerial"},
			cancelCtx:     true,
			testFetchJobs: true,
			wantActive:    false,
		},
		{
			name:       "NilReceiverBranches",
			pollerKind: "nil",
			wantActive: false,
		},
		{
			name:       "EmptyPollerBranches",
			pollerKind: "empty",
			wantActive: false,
		},
		{
			name:           "RepoPruningBranches",
			pollerKind:     "prune_old",
			repos:          []string{"azylman/aerial"},
			initialOldRepo: "old/removed",
			runsETag:       "legacy-etag",
			wantActive:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var queriedQuery url.Values
			mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/actions/runs/") && strings.HasSuffix(r.URL.Path, "/jobs") {
					if tc.jobsETag != "" && r.Header.Get("If-None-Match") == tc.jobsETag {
						w.WriteHeader(http.StatusNotModified)
						return
					}
					if tc.jobsETag != "" {
						w.Header().Set("ETag", tc.jobsETag)
					}
					if tc.jobsStatus != 0 {
						w.WriteHeader(tc.jobsStatus)
					}
					if tc.jobsBody != "" {
						_, _ = w.Write([]byte(tc.jobsBody))
					} else {
						_ = json.NewEncoder(w).Encode(GitHubJobsResponse{Jobs: []GitHubJob{}})
					}
					return
				}
				if strings.HasSuffix(r.URL.Path, "/actions/runs") {
					queriedQuery = r.URL.Query()
					if tc.runsETag != "" && r.Header.Get("If-None-Match") == tc.runsETag {
						w.WriteHeader(http.StatusNotModified)
						return
					}
					if tc.runsETag != "" {
						w.Header().Set("ETag", tc.runsETag)
					}
					if tc.runsStatus != 0 {
						w.WriteHeader(tc.runsStatus)
					}
					if tc.runsBody != "" {
						_, _ = w.Write([]byte(tc.runsBody))
					} else {
						_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
					}
					return
				}
				http.NotFound(w, r)
			}))
			defer mockGH.Close()

			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			var poller *GitHubPoller
			switch tc.pollerKind {
			case "nil":
				var nilPoller *GitHubPoller
				nilPoller.Start(ctx)
				if nilPoller.pollOnce(ctx) {
					t.Errorf("expected nil poller pollOnce to return false")
				}
				nilRuns, nilJobs := nilPoller.GetSnapshot()
				if nilRuns != nil || nilJobs != nil {
					t.Errorf("expected nil snapshot from nil poller")
				}
				return
			case "empty":
				emptyPoller := &GitHubPoller{}
				if emptyPoller.pollOnce(ctx) {
					t.Errorf("expected empty poller pollOnce to return false")
				}
				return
			case "empty_repo":
				emptyRepoPoller := &GitHubPoller{client: &http.Client{}}
				emptyRepoPoller.fetchJobsForRun(ctx, 123)
				return
			case "unreachable":
				badPoller := NewMultiGitHubPoller([]string{"azylman/aerial"}, "")
				badPoller.apiBaseURL = "http://127.0.0.1:54321"
				if badPoller.pollOnce(ctx) {
					t.Errorf("expected false on network failure")
				}
				badPoller.fetchJobsForRun(ctx, 123)
				return
			case "prune_old":
				prunePoller := &GitHubPoller{
					repo:             "azylman/aerial",
					runsETag:         tc.runsETag,
					runsETagMap:      map[string]string{tc.initialOldRepo: "etag-old"},
					cachedRunsByRepo: map[string][]GitHubRun{tc.initialOldRepo: {{ID: 999}}},
					client:           &http.Client{Timeout: 2 * time.Second},
					apiBaseURL:       mockGH.URL,
				}
				prunePoller.pollOnce(ctx)
				prunePoller.mu.RLock()
				if _, exists := prunePoller.cachedRunsByRepo[tc.initialOldRepo]; exists {
					t.Errorf("expected removed repository to be pruned from cache")
				}
				prunePoller.mu.RUnlock()
				return
			case "multi":
				poller = NewMultiGitHubPoller(tc.repos, "test-token")
				poller.apiBaseURL = mockGH.URL
			default:
				poller = NewMultiGitHubPoller([]string{tc.repos[0]}, "token123")
				poller.apiBaseURL = mockGH.URL
			}

			if tc.initialCachedJob > 0 {
				poller.cachedJobs[tc.initialCachedJob] = []GitHubJob{{ID: 1}}
				poller.jobsETagMap[tc.initialCachedJob] = "stale-etag"
			}

			if tc.testFetchJobs {
				if tc.jobsETag != "" {
					poller.jobsETagMap[100] = tc.jobsETag
				}
				poller.fetchJobsForRun(ctx, 100)
			}

			hasActive := poller.pollOnce(ctx)
			if hasActive != tc.wantActive {
				t.Errorf("pollOnce() = %v, want %v", hasActive, tc.wantActive)
			}

			if tc.wantEvent != "" && (queriedQuery.Get("event") != "" || queriedQuery.Get("branch") != "") {
				t.Errorf("expected no event or branch filters in query, got: %+v", queriedQuery)
			}

			runs, jobs := poller.GetSnapshot()
			if tc.wantRunCount > 0 {
				if len(runs) != tc.wantRunCount {
					t.Fatalf("expected %d runs, got %d", tc.wantRunCount, len(runs))
				}
				if tc.wantRunID > 0 && runs[0].ID != tc.wantRunID {
					t.Errorf("expected run ID %d, got %d", tc.wantRunID, runs[0].ID)
				}
				if tc.wantRepoName != "" && runs[0].Repository != tc.wantRepoName {
					t.Errorf("expected repository %q, got %q", tc.wantRepoName, runs[0].Repository)
				}
				if tc.wantEvent != "" && runs[0].Event != tc.wantEvent {
					t.Errorf("expected event %q, got %q", tc.wantEvent, runs[0].Event)
				}
			} else if tc.name == "DecodeError" && len(runs) != 0 {
				t.Errorf("expected 0 runs on decode error, got %d", len(runs))
			}

			if tc.wantJobCount > 0 && tc.wantRunID > 0 {
				if len(jobs[tc.wantRunID]) != tc.wantJobCount {
					t.Errorf("expected %d job for run %d, got %d", tc.wantJobCount, tc.wantRunID, len(jobs[tc.wantRunID]))
				}
			}

			if tc.initialCachedJob > 0 {
				poller.mu.RLock()
				if _, exists := poller.cachedJobs[tc.initialCachedJob]; exists {
					t.Errorf("expected stale cachedJobs to be pruned")
				}
				if _, exists := poller.jobsETagMap[tc.initialCachedJob]; exists {
					t.Errorf("expected stale jobsETagMap to be pruned")
				}
				poller.mu.RUnlock()
			}

			if tc.testSecondPoll {
				hasActive2 := poller.pollOnce(ctx)
				if hasActive2 != tc.wantActive {
					t.Errorf("expected hasActive2=%v on 304, got %v", tc.wantActive, hasActive2)
				}
			}
		})
	}
}

func TestGitHubPoller_StartStopTickerLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pollCount atomic.Int32
	secondPollCh := make(chan struct{}, 4)

	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		count := pollCount.Add(1)
		if count >= 2 {
			select {
			case secondPollCh <- struct{}{}:
			default:
			}
		}

		resp := GitHubRunsResponse{
			WorkflowRuns: []GitHubRun{
				{
					ID:        100,
					Status:    "in_progress",
					UpdatedAt: time.Now().UTC(),
					HeadCommit: &struct {
						Message   string    `json:"message"`
						Timestamp time.Time `json:"timestamp"`
					}{
						Message: "feat: adaptive timing test",
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ghMock.Close()

	p := NewMultiGitHubPoller([]string{"azylman/aerial"}, "token")
	p.apiBaseURL = ghMock.URL
	p.pollInterval = time.Millisecond

	// Start background loop
	p.Start(ctx)

	// Wait deterministically for the second poll request via buffered channel without arbitrary sleep
	select {
	case <-secondPollCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second poll tick in Start(ctx)")
	}

	close(p.stopCh)

	// Empty repo poller Start returns early
	emptyPoller := NewMultiGitHubPoller([]string{}, "")
	emptyPoller.Start(ctx)
}

func TestGitHubPoller_MultiRepoAndDiscovery_TableDriven(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "MultiRepoFanoutAndSnapshotPrioritization"},
		{name: "DynamicConfigReload"},
		{name: "GetActiveReposEdgeCases"},
		{name: "FetchHangarRepos"},
		{name: "HangarDiscoveryAndLKGCRetention"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			switch tc.name {
			case "MultiRepoFanoutAndSnapshotPrioritization":
				runIDAerial := int64(1001)
				runIDSidecars := int64(2001)
				now := time.Now().UTC()

				mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.URL.Path == "/repos/azylman/aerial/actions/runs":
						if r.Header.Get("If-None-Match") == "etag-aerial-1" {
							w.WriteHeader(http.StatusNotModified)
							return
						}
						w.Header().Set("ETag", "etag-aerial-1")
						_ = json.NewEncoder(w).Encode(GitHubRunsResponse{
							TotalCount: 1,
							WorkflowRuns: []GitHubRun{
								{
									ID:         runIDAerial,
									Name:       "CI",
									HeadSHA:    "1111aaa",
									Status:     "completed",
									Conclusion: "success",
									CreatedAt:  now.Add(-10 * time.Minute),
									UpdatedAt:  now.Add(-8 * time.Minute),
									HTMLURL:    "https://github.com/azylman/aerial/actions/runs/1001",
								},
							},
						})
					case r.URL.Path == "/repos/azylman/aerial-sidecars/actions/runs":
						if r.Header.Get("If-None-Match") == "etag-sidecars-1" {
							w.WriteHeader(http.StatusNotModified)
							return
						}
						w.Header().Set("ETag", "etag-sidecars-1")
						_ = json.NewEncoder(w).Encode(GitHubRunsResponse{
							TotalCount: 1,
							WorkflowRuns: []GitHubRun{
								{
									ID:         runIDSidecars,
									Name:       "Build Sidecars",
									HeadSHA:    "2222bbb",
									Status:     "in_progress",
									Conclusion: "",
									CreatedAt:  now.Add(-2 * time.Minute),
									UpdatedAt:  now.Add(-10 * time.Second),
									HTMLURL:    "https://github.com/azylman/aerial-sidecars/actions/runs/2001",
								},
							},
						})
					case r.URL.Path == fmt.Sprintf("/repos/azylman/aerial/actions/runs/%d/jobs", runIDAerial):
						_ = json.NewEncoder(w).Encode(GitHubJobsResponse{
							TotalCount: 1,
							Jobs: []GitHubJob{
								{
									ID:         11,
									RunID:      runIDAerial,
									Name:       "test (brain)",
									Status:     "completed",
									Conclusion: "success",
								},
							},
						})
					case r.URL.Path == fmt.Sprintf("/repos/azylman/aerial-sidecars/actions/runs/%d/jobs", runIDSidecars):
						_ = json.NewEncoder(w).Encode(GitHubJobsResponse{
							TotalCount: 1,
							Jobs: []GitHubJob{
								{
									ID:        22,
									RunID:     runIDSidecars,
									Name:      "Build & Push Images to GHCR (aura-farming)",
									Status:    "in_progress",
									StartedAt: now.Add(-60 * time.Second),
								},
							},
						})
					default:
						http.NotFound(w, r)
					}
				}))
				defer mockGH.Close()

				repos := []string{"azylman/aerial", "azylman/aerial-sidecars"}
				poller := NewMultiGitHubPoller(repos, "test-token")
				poller.apiBaseURL = mockGH.URL

				hasActive := poller.pollOnce(ctx)
				if !hasActive {
					t.Errorf("expected hasActive=true because aerial-sidecars is in_progress")
				}

				runs, jobs := poller.GetSnapshot()
				if len(runs) != 2 {
					t.Fatalf("expected 2 runs across both repos, got %d: %+v", len(runs), runs)
				}

				if runs[0].ID != runIDSidecars || runs[0].Repository != "azylman/aerial-sidecars" {
					t.Errorf("expected first run to be active sidecars run %d with repo azylman/aerial-sidecars, got %+v", runIDSidecars, runs[0])
				}
				if runs[1].ID != runIDAerial || runs[1].Repository != "azylman/aerial" {
					t.Errorf("expected second run to be aerial run %d with repo azylman/aerial, got %+v", runIDAerial, runs[1])
				}

				if len(jobs[runIDAerial]) != 1 || len(jobs[runIDSidecars]) != 1 {
					t.Errorf("expected jobs for both repos to be preserved")
				}

				hasActive2 := poller.pollOnce(ctx)
				if !hasActive2 {
					t.Errorf("expected hasActive2=true on 304 because sidecars is still in_progress")
				}

				runs2, jobs2 := poller.GetSnapshot()
				if len(runs2) != 2 || len(jobs2[runIDAerial]) != 1 || len(jobs2[runIDSidecars]) != 1 {
					t.Errorf("expected runs and jobs to persist after 304 poll")
				}

			case "DynamicConfigReload":
				tempDir := t.TempDir()
				cfgFile := filepath.Join(tempDir, "config.yaml")

				initialYaml := `
dashboard:
  github_repos:
    - "azylman/aerial"
`
				if err := os.WriteFile(cfgFile, []byte(initialYaml), 0o644); err != nil {
					t.Fatal(err)
				}

				callCountAerial := 0
				callCountSidecars := 0
				mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch r.URL.Path {
					case "/repos/azylman/aerial/actions/runs":
						callCountAerial++
						_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
					case "/repos/azylman/aerial-sidecars/actions/runs":
						callCountSidecars++
						_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
					default:
						http.NotFound(w, r)
					}
				}))
				defer mockGH.Close()

				poller := NewMultiGitHubPoller([]string{"azylman/aerial"}, "token")
				poller.apiBaseURL = mockGH.URL
				poller.configPath = cfgFile

				_ = poller.pollOnce(ctx)
				if callCountAerial != 1 || callCountSidecars != 0 {
					t.Fatalf("expected 1 aerial call, 0 sidecars calls; got aerial=%d, sidecars=%d", callCountAerial, callCountSidecars)
				}

				updatedYaml := `
dashboard:
  github_repos:
    - "azylman/aerial"
    - "azylman/aerial-sidecars"
`
				if err := os.WriteFile(cfgFile, []byte(updatedYaml), 0o644); err != nil {
					t.Fatal(err)
				}

				_ = poller.pollOnce(ctx)
				if callCountAerial != 2 || callCountSidecars != 1 {
					t.Fatalf("expected 2 aerial calls, 1 sidecars call; got aerial=%d, sidecars=%d", callCountAerial, callCountSidecars)
				}

			case "GetActiveReposEdgeCases":
				var nilPoller *GitHubPoller
				if got := nilPoller.getActiveRepos(); got != nil {
					t.Errorf("expected nil for nil poller, got %v", got)
				}

				singleRepoPoller := &GitHubPoller{repo: "azylman/aerial"}
				if got := singleRepoPoller.getActiveRepos(); len(got) != 1 || got[0] != "azylman/aerial" {
					t.Errorf("expected [azylman/aerial], got %v", got)
				}

				emptyPoller := &GitHubPoller{}
				if got := emptyPoller.getActiveRepos(); got != nil {
					t.Errorf("expected nil for empty poller, got %v", got)
				}

			case "FetchHangarRepos":
				if res := fetchHangarRepos(ctx, "", nil); res != nil {
					t.Errorf("expected nil for empty URL, got %v", res)
				}
				if res := fetchHangarRepos(ctx, "http://127.0.0.1:54320/unreachable", &http.Client{Timeout: 50 * time.Millisecond}); res != nil {
					t.Errorf("expected nil for unreachable URL, got %v", res)
				}

				srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
				defer srv500.Close()
				if res := fetchHangarRepos(ctx, srv500.URL, srv500.Client()); res != nil {
					t.Errorf("expected nil for 500 response, got %v", res)
				}

				srvBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("{invalid-json"))
				}))
				defer srvBadJSON.Close()
				if res := fetchHangarRepos(ctx, srvBadJSON.URL, srvBadJSON.Client()); res != nil {
					t.Errorf("expected nil for bad JSON, got %v", res)
				}

				srvOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					resp := GitSyncStatusResponse{
						Status: "synced",
						Repos: map[string]RepoStatus{
							"/share/aerial":       {GitHubRepo: "azylman/aerial"},
							"/share/mirrormere":   {GitHubRepo: "Azylman/MirrorMere"},
							"/share/aerial-alias": {GitHubRepo: "azylman/aerial"},
							"/share/no-github":    {GitHubRepo: ""},
						},
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(resp)
				}))
				defer srvOK.Close()

				res := fetchHangarRepos(ctx, srvOK.URL, srvOK.Client())
				expected := []string{"azylman/aerial", "azylman/mirrormere"}
				if len(res) != len(expected) {
					t.Fatalf("expected %d repos, got %d: %v", len(expected), len(res), res)
				}
				for i, exp := range expected {
					if res[i] != exp {
						t.Errorf("res[%d] = %q, want %q", i, res[i], exp)
					}
				}

			case "HangarDiscoveryAndLKGCRetention":
				hangarShouldFail := false
				srvHangar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if hangarShouldFail {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					resp := GitSyncStatusResponse{
						Status: "synced",
						Repos: map[string]RepoStatus{
							"/share/mirrormere": {GitHubRepo: "azylman/mirrormere"},
						},
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(resp)
				}))
				defer srvHangar.Close()

				srvGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
				}))
				defer srvGH.Close()

				discoveryPoller := NewMultiGitHubPoller([]string{"azylman/aerial"}, "token")
				discoveryPoller.apiBaseURL = srvGH.URL
				discoveryPoller.hangarURL = srvHangar.URL

				discoveryPoller.pollOnce(ctx)
				activeRepos := discoveryPoller.getActiveRepos()
				if len(activeRepos) != 2 || activeRepos[0] != "azylman/aerial" || activeRepos[1] != "azylman/mirrormere" {
					t.Fatalf("expected 2 repos discovered, got %d: %v", len(activeRepos), activeRepos)
				}

				hangarShouldFail = true
				discoveryPoller.pollOnce(ctx)
				retainedRepos := discoveryPoller.getActiveRepos()
				if len(retainedRepos) != 2 {
					t.Fatalf("expected 2 repos retained under LKGC on Hangar failure, got %d: %v", len(retainedRepos), retainedRepos)
				}
			}
		})
	}
}
