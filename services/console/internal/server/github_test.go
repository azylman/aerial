package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pollerEndpoint is one canned fake GitHub endpoint: status 0 means 200, and a non-empty
// etag is returned as ETag and answered with 304 when the request sends it back.
type pollerEndpoint struct {
	status int
	body   string
	etag   string
}

// pollerFakeGitHub serves the runs, jobs and commit-pulls endpoints the poller calls and
// records every exchange as "<request URI> <status>".
type pollerFakeGitHub struct {
	runs, jobs, pulls pollerEndpoint

	mu    sync.Mutex
	calls []string
}

func (f *pollerFakeGitHub) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ep := pollerEndpoint{status: http.StatusNotFound, body: `{"message":"Not Found"}`}
		switch {
		case strings.HasSuffix(r.URL.Path, "/jobs"):
			ep = f.jobs
		case strings.HasSuffix(r.URL.Path, "/actions/runs"):
			ep = f.runs
		case strings.HasSuffix(r.URL.Path, "/pulls"):
			ep = f.pulls
		}
		status := ep.status
		switch {
		case ep.etag != "" && r.Header.Get("If-None-Match") == ep.etag:
			status = http.StatusNotModified
		case status == 0:
			status = http.StatusOK
		}
		f.mu.Lock()
		f.calls = append(f.calls, fmt.Sprintf("%s %d", r.URL.RequestURI(), status))
		f.mu.Unlock()
		if ep.etag != "" {
			w.Header().Set("ETag", ep.etag)
		}
		w.WriteHeader(status)
		if status != http.StatusNotModified {
			_, _ = io.WriteString(w, ep.body)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func (f *pollerFakeGitHub) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// newPollerForFake builds a poller for repos aimed at gh, or at a closed port when apiDown.
func newPollerForFake(t *testing.T, gh *pollerFakeGitHub, repos []string, apiDown bool) *GitHubPoller {
	t.Helper()
	p := NewMultiGitHubPoller(repos, "token123")
	p.apiBaseURL = gh.start(t)
	if apiDown {
		p.apiBaseURL = downServerURL(t)
	}
	return p
}

// pollerTestContext returns the test's context, already cancelled when cancelled is set.
func pollerTestContext(t *testing.T, cancelled bool) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	if cancelled {
		cancel()
	}
	return ctx
}

// pollerRun is the part of a snapshot run the poller tests assert on.
type pollerRun struct {
	ID               int64
	Repository       string
	Event            string
	MergedFromBranch string
}

func pollerSnapshot(p *GitHubPoller) ([]pollerRun, map[int64]int) {
	runs, jobs := p.GetSnapshot()
	got := make([]pollerRun, 0, len(runs))
	for _, r := range runs {
		got = append(got, pollerRun{r.ID, r.Repository, r.Event, r.MergedFromBranch})
	}
	counts := make(map[int64]int, len(jobs))
	for id, js := range jobs {
		counts[id] = len(js)
	}
	return got, counts
}

// seedPollerStaleCaches caches a job (and its ETag) for jobID and runs (and a runs ETag) for
// repo, so a poll can prove it prunes both; zero values seed nothing.
func seedPollerStaleCaches(p *GitHubPoller, jobID int64, repo string) {
	if jobID != 0 {
		p.cachedJobs[jobID] = []GitHubJob{{ID: 1}}
		p.jobsETagMap[jobID] = "stale-etag"
	}
	if repo != "" {
		p.cachedRunsByRepo[repo] = []GitHubRun{{ID: 998}}
		p.runsETagMap[repo] = "etag-old"
	}
}

// pollerCacheState returns a copy of the job ETags and whether repo still has cached runs or a runs ETag.
func pollerCacheState(p *GitHubPoller, repo string) (map[int64]string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, runs := p.cachedRunsByRepo[repo]
	_, etag := p.runsETagMap[repo]
	return maps.Clone(p.jobsETagMap), runs || etag
}

func TestGitHubPoller_pollOnce(t *testing.T) {
	now := time.Now().UTC()
	activeRunsBody := fmt.Sprintf(`{"total_count":1,"workflow_runs":[{"id":987654,"name":"CI","head_branch":"main","head_sha":"abcdef012345","status":"in_progress",`+
		`"created_at":%q,"updated_at":%q,"head_commit":{"message":%q,"timestamp":%q}}]}`,
		now.Add(-10*time.Minute).Format(time.RFC3339), now.Format(time.RFC3339), strings.Repeat("Very long commit message that exceeds seventy-two runes ", 2), now.Format(time.RFC3339))
	prRunsBody := `{"total_count":1,"workflow_runs":[{"id":9001,"name":"Continuous Delivery","head_branch":"feat/pr","head_sha":"aabbccdd1122","event":"pull_request",` +
		`"status":"in_progress","conclusion":null,"repository":{"id":1,"name":"aerial-sidecars","full_name":"acme/aerial-sidecars"}}]}`
	mainPushBody := fmt.Sprintf(`{"total_count":1,"workflow_runs":[{"id":9100,"name":"Continuous Delivery","head_branch":"main","head_sha":"de22ef4a238e",`+
		`"event":"push","status":"completed","conclusion":"success","created_at":%q,"updated_at":%q}]}`,
		now.Add(-3*time.Minute).Format(time.RFC3339), now.Add(-time.Minute).Format(time.RFC3339))
	const (
		aerialRuns = "/repos/acme/aerial/actions/runs?per_page=10"
		mainPulls  = "/repos/acme/aerial/commits/de22ef4a238e/pulls"
		mainJobs   = "/repos/acme/aerial/actions/runs/9100/jobs"
	)

	tests := []struct {
		name       string
		repos      []string
		runs       pollerEndpoint
		jobs       pollerEndpoint
		pulls      pollerEndpoint
		apiDown    bool
		cancelCtx  bool
		staleJobID int64  // seeded into the job cache and job ETags; pruned when its run is gone
		staleRepo  string // seeded into the per-repo run cache and ETags; pruned when no longer configured
		legacyETag string // seeded as the primary repo's single-repo runs ETag
		wantActive bool
		wantRuns   []pollerRun
		wantJobs   map[int64]int
		wantETags  map[int64]string
		wantCalls  []string // both polls
	}{
		{
			name:       "active run fetches jobs, prunes stale job cache, second poll is 304",
			repos:      []string{"acme/aerial"},
			runs:       pollerEndpoint{body: activeRunsBody, etag: "runs-etag-1"},
			jobs:       pollerEndpoint{body: `{"jobs":[{"id":111,"run_id":987654,"status":"completed"}]}`, etag: "job-etag-1"},
			staleJobID: 999,
			wantActive: true,
			wantRuns:   []pollerRun{{987654, "acme/aerial", "", ""}},
			wantJobs:   map[int64]int{987654: 1},
			wantETags:  map[int64]string{987654: "job-etag-1"},
			wantCalls:  []string{aerialRuns + " 200", "/repos/acme/aerial/actions/runs/987654/jobs 200", aerialRuns + " 304"},
		},
		{
			name:       "pull request run with repository object, no event or branch filter",
			repos:      []string{"acme/aerial-sidecars"},
			runs:       pollerEndpoint{body: prRunsBody},
			jobs:       pollerEndpoint{body: `{"jobs":[{"id":10,"run_id":9001,"status":"in_progress"}]}`},
			wantActive: true,
			wantRuns:   []pollerRun{{9001, "acme/aerial-sidecars", "pull_request", ""}},
			wantJobs:   map[int64]int{9001: 1},
			wantCalls: []string{
				"/repos/acme/aerial-sidecars/actions/runs?per_page=10 200", "/repos/acme/aerial-sidecars/actions/runs/9001/jobs 200",
				"/repos/acme/aerial-sidecars/actions/runs?per_page=10 200", "/repos/acme/aerial-sidecars/actions/runs/9001/jobs 200",
			},
		},
		{
			name:      "main push resolves merged PR branch once",
			repos:     []string{"acme/aerial"},
			runs:      pollerEndpoint{body: mainPushBody},
			jobs:      pollerEndpoint{body: `{"jobs":[]}`},
			pulls:     pollerEndpoint{body: `[{"merged_at":"2026-11-20T10:00:00Z","head":{"ref":"feat/pr-cards"}}]`},
			wantRuns:  []pollerRun{{9100, "acme/aerial", "push", "feat/pr-cards"}},
			wantJobs:  map[int64]int{9100: 0},
			wantCalls: []string{aerialRuns + " 200", mainPulls + " 200", mainJobs + " 200", aerialRuns + " 200", mainJobs + " 200"},
		},
		{
			name:      "main push lookup error falls back to SHA and retries",
			repos:     []string{"acme/aerial"},
			runs:      pollerEndpoint{body: mainPushBody},
			jobs:      pollerEndpoint{body: `{"jobs":[]}`},
			pulls:     pollerEndpoint{status: http.StatusInternalServerError, body: `{"message":"boom"}`},
			wantRuns:  []pollerRun{{9100, "acme/aerial", "push", ""}},
			wantJobs:  map[int64]int{9100: 0},
			wantCalls: []string{aerialRuns + " 200", mainPulls + " 500", mainJobs + " 200", aerialRuns + " 200", mainPulls + " 500", mainJobs + " 200"},
		},
		{
			name:      "decode error",
			repos:     []string{"acme/aerial"},
			runs:      pollerEndpoint{body: `{not valid json`},
			wantCalls: []string{aerialRuns + " 200", aerialRuns + " 200"},
		},
		{
			name:      "rate limit 429",
			repos:     []string{"acme/aerial"},
			runs:      pollerEndpoint{status: http.StatusTooManyRequests, body: `{"message":"rate limit"}`},
			wantCalls: []string{aerialRuns + " 429", aerialRuns + " 429"},
		},
		{
			name:      "server error 500",
			repos:     []string{"acme/aerial"},
			runs:      pollerEndpoint{status: http.StatusInternalServerError, body: `{"message":"oops"}`},
			wantCalls: []string{aerialRuns + " 500", aerialRuns + " 500"},
		},
		{name: "cancelled context sends nothing", repos: []string{"acme/aerial"}, cancelCtx: true},
		{name: "unreachable API", repos: []string{"acme/aerial"}, apiDown: true},
		{
			name:       "removed repo pruned while legacy ETag gets 304",
			repos:      []string{"acme/aerial"},
			runs:       pollerEndpoint{body: `{}`, etag: "legacy-etag"},
			staleRepo:  "old/removed",
			legacyETag: "legacy-etag",
			wantCalls:  []string{aerialRuns + " 304", aerialRuns + " 304"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gh := &pollerFakeGitHub{runs: tc.runs, jobs: tc.jobs, pulls: tc.pulls}
			p := newPollerForFake(t, gh, tc.repos, tc.apiDown)
			p.runsETag = tc.legacyETag
			seedPollerStaleCaches(p, tc.staleJobID, tc.staleRepo)
			ctx := pollerTestContext(t, tc.cancelCtx)

			for poll := 1; poll <= 2; poll++ {
				if got := p.pollOnce(ctx); got != tc.wantActive {
					t.Errorf("poll %d: pollOnce() = %v, want %v", poll, got, tc.wantActive)
				}
				runs, jobs := pollerSnapshot(p)
				if !slices.Equal(runs, tc.wantRuns) {
					t.Errorf("poll %d: runs = %+v, want %+v", poll, runs, tc.wantRuns)
				}
				if !maps.Equal(jobs, tc.wantJobs) {
					t.Errorf("poll %d: job counts = %v, want %v", poll, jobs, tc.wantJobs)
				}
			}
			gotETags, staleCached := pollerCacheState(p, tc.staleRepo)
			if !maps.Equal(gotETags, tc.wantETags) {
				t.Errorf("job ETags = %v, want %v", gotETags, tc.wantETags)
			}
			if staleCached {
				t.Errorf("removed repo %q still has cached runs or a runs ETag", tc.staleRepo)
			}
			if got := gh.recorded(); !slices.Equal(got, tc.wantCalls) {
				t.Errorf("calls = %q, want %q", got, tc.wantCalls)
			}
		})
	}
}

func TestGitHubPoller_fetchJobsForRun(t *testing.T) {
	tests := []struct {
		name       string
		repos      []string
		repoArg    string
		presetETag string
		jobs       pollerEndpoint
		apiDown    bool
		cancelCtx  bool
		wantJobs   map[int64]int
		wantETag   string
		wantCalls  []string
	}{
		{name: "no repo configured sends nothing"},
		{
			name:      "explicit repo argument",
			repoArg:   "acme/other",
			jobs:      pollerEndpoint{body: `{"jobs":[{"id":1}]}`, etag: "e1"},
			wantJobs:  map[int64]int{100: 1},
			wantETag:  "e1",
			wantCalls: []string{"/repos/acme/other/actions/runs/100/jobs 200"},
		},
		{
			name:      "first active repo without argument",
			repos:     []string{"acme/aerial"},
			jobs:      pollerEndpoint{body: `{"jobs":[{"id":1},{"id":2}]}`},
			wantJobs:  map[int64]int{100: 2},
			wantCalls: []string{"/repos/acme/aerial/actions/runs/100/jobs 200"},
		},
		{
			name:       "304 leaves cache empty",
			repos:      []string{"acme/aerial"},
			presetETag: "test-etag",
			jobs:       pollerEndpoint{etag: "test-etag"},
			wantETag:   "test-etag",
			wantCalls:  []string{"/repos/acme/aerial/actions/runs/100/jobs 304"},
		},
		{
			name:      "decode error ignored",
			repos:     []string{"acme/aerial"},
			jobs:      pollerEndpoint{body: `{bad`, etag: "e2"},
			wantCalls: []string{"/repos/acme/aerial/actions/runs/100/jobs 200"},
		},
		{name: "cancelled context sends nothing", repos: []string{"acme/aerial"}, cancelCtx: true},
		{name: "unreachable API", repos: []string{"acme/aerial"}, apiDown: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gh := &pollerFakeGitHub{jobs: tc.jobs}
			p := newPollerForFake(t, gh, tc.repos, tc.apiDown)
			if tc.presetETag != "" {
				p.jobsETagMap[100] = tc.presetETag
			}

			p.fetchJobsForRun(pollerTestContext(t, tc.cancelCtx), 100, tc.repoArg)

			if _, jobs := pollerSnapshot(p); !maps.Equal(jobs, tc.wantJobs) {
				t.Errorf("job counts = %v, want %v", jobs, tc.wantJobs)
			}
			p.mu.RLock()
			gotETag := p.jobsETagMap[100]
			p.mu.RUnlock()
			if gotETag != tc.wantETag {
				t.Errorf("job ETag = %q, want %q", gotETag, tc.wantETag)
			}
			if got := gh.recorded(); !slices.Equal(got, tc.wantCalls) {
				t.Errorf("calls = %q, want %q", got, tc.wantCalls)
			}
		})
	}
}

func TestGitHubPoller_IdleWithoutRepos(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		poller          *GitHubPoller
		wantNilSnapshot bool
	}{
		{name: "nil poller", wantNilSnapshot: true},
		{name: "zero-value poller", poller: &GitHubPoller{}},
		{name: "only blank repos", poller: NewMultiGitHubPoller([]string{" ", ""}, "")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.poller.Start(t.Context()) // returns without starting a loop; goleak in TestMain enforces it
			if tc.poller.pollOnce(t.Context()) {
				t.Error("pollOnce() = true, want false")
			}
			runs, jobs := tc.poller.GetSnapshot()
			if len(runs) != 0 || len(jobs) != 0 || (runs == nil) != tc.wantNilSnapshot || (jobs == nil) != tc.wantNilSnapshot {
				t.Errorf("GetSnapshot() = %v, %v; want empty (nil: %v)", runs, jobs, tc.wantNilSnapshot)
			}
		})
	}
}

func TestGitHubPoller_StartStopTickerLoop(t *testing.T) {
	tests := []struct {
		name         string
		repos        []string
		pollInterval time.Duration
		wantPolls    int32
	}{
		{name: "ticker repolls until stopped", repos: []string{"example/repo"}, pollInterval: time.Millisecond, wantPolls: 2},
		{name: "stop with idle ticker returns via stopCh", repos: []string{"example/repo"}, pollInterval: time.Hour, wantPolls: 1},
		{name: "no repos returns without polling", repos: []string{}, pollInterval: time.Millisecond, wantPolls: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var pollCount atomic.Int32
			polled := make(chan struct{}, 1)
			ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if pollCount.Add(1) >= tc.wantPolls {
					select {
					case polled <- struct{}{}:
					default:
					}
				}
				_, _ = w.Write([]byte(`{"workflow_runs":[{"id":100,"status":"in_progress"}]}`))
			}))
			defer ghMock.Close()

			p := NewMultiGitHubPoller(tc.repos, "token")
			p.apiBaseURL = ghMock.URL
			p.pollInterval = tc.pollInterval

			// The context is never canceled, so once stopCh closes it is the loop's only exit.
			p.Start(context.WithoutCancel(t.Context()))
			if tc.wantPolls > 0 {
				select {
				case <-polled:
				case <-time.After(2 * time.Second):
					t.Fatalf("timed out waiting for %d polls, got %d", tc.wantPolls, pollCount.Load())
				}
			}
			close(p.stopCh)
			if tc.wantPolls == 0 && pollCount.Load() != 0 {
				t.Errorf("polls = %d, want 0", pollCount.Load())
			}
		})
	}
}

func TestGitHubPoller_MultiRepoFanout(t *testing.T) {
	ctx := context.Background()
	runIDAerial := int64(1001)
	runIDSidecars := int64(2001)
	now := time.Now().UTC()

	mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/repos/acme/aerial/actions/runs":
			handleMockRunsEndpoint(w, r, "etag-aerial-1", GitHubRun{
				ID:         runIDAerial,
				Name:       "CI",
				HeadSHA:    "1111aaa",
				Status:     "completed",
				Conclusion: "success",
				CreatedAt:  now.Add(-10 * time.Minute),
				UpdatedAt:  now.Add(-8 * time.Minute),
				HTMLURL:    "https://github.com/acme/aerial/actions/runs/1001",
			})
		case r.URL.Path == "/repos/acme/aerial-sidecars/actions/runs":
			handleMockRunsEndpoint(w, r, "etag-sidecars-1", GitHubRun{
				ID:         runIDSidecars,
				Name:       "Build Sidecars",
				HeadSHA:    "2222bbb",
				Status:     "in_progress",
				Conclusion: "",
				CreatedAt:  now.Add(-2 * time.Minute),
				UpdatedAt:  now.Add(-10 * time.Second),
				HTMLURL:    "https://github.com/acme/aerial-sidecars/actions/runs/2001",
			})
		case r.URL.Path == fmt.Sprintf("/repos/acme/aerial/actions/runs/%d/jobs", runIDAerial):
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
		case r.URL.Path == fmt.Sprintf("/repos/acme/aerial-sidecars/actions/runs/%d/jobs", runIDSidecars):
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

	repos := []string{"acme/aerial", "acme/aerial-sidecars"}
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

	if runs[0].ID != runIDSidecars || runs[0].Repository != "acme/aerial-sidecars" {
		t.Errorf("expected first run to be active sidecars run %d with repo acme/aerial-sidecars, got %+v", runIDSidecars, runs[0])
	}
	if runs[1].ID != runIDAerial || runs[1].Repository != "acme/aerial" {
		t.Errorf("expected second run to be aerial run %d with repo acme/aerial, got %+v", runIDAerial, runs[1])
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
}

func handleMockRunsEndpoint(w http.ResponseWriter, r *http.Request, etag string, run GitHubRun) {
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", etag)
	_ = json.NewEncoder(w).Encode(GitHubRunsResponse{
		TotalCount:   1,
		WorkflowRuns: []GitHubRun{run},
	})
}

func TestGitHubPoller_ReloadsReposFromConfig(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	cfgFile := filepath.Join(tempDir, "config.yaml")

	initialYaml := `
console:
  github_repos:
    - "acme/aerial"
`
	if err := os.WriteFile(cfgFile, []byte(initialYaml), 0o644); err != nil {
		t.Fatal(err)
	}

	callCountAerial := 0
	callCountSidecars := 0
	mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("Accept") != "application/vnd.github+json" || r.Header.Get("X-Github-Api-Version") != "2022-11-28" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/repos/acme/aerial/actions/runs":
			callCountAerial++
			_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
		case "/repos/acme/aerial-sidecars/actions/runs":
			callCountSidecars++
			_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockGH.Close()

	poller := NewMultiGitHubPoller([]string{"acme/aerial"}, "token")
	poller.apiBaseURL = mockGH.URL
	poller.configPath = cfgFile

	poller.pollOnce(ctx)
	if callCountAerial != 1 || callCountSidecars != 0 {
		t.Fatalf("expected 1 aerial call, 0 sidecars calls; got aerial=%d, sidecars=%d", callCountAerial, callCountSidecars)
	}

	updatedYaml := `
console:
  github_repos:
    - "acme/aerial"
    - "acme/aerial-sidecars"
`
	if err := os.WriteFile(cfgFile, []byte(updatedYaml), 0o644); err != nil {
		t.Fatal(err)
	}

	poller.pollOnce(ctx)
	if callCountAerial != 2 || callCountSidecars != 1 {
		t.Fatalf("expected 2 aerial calls, 1 sidecars call; got aerial=%d, sidecars=%d", callCountAerial, callCountSidecars)
	}
}

func TestGitHubPoller_getActiveRepos(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		poller *GitHubPoller
		want   []string
	}{
		{name: "nil poller"},
		{name: "empty poller", poller: &GitHubPoller{}},
		{name: "single primary repo", poller: &GitHubPoller{repo: "acme/aerial"}, want: []string{"acme/aerial"}},
		{
			name:   "repo list trimmed and case-insensitively deduped",
			poller: &GitHubPoller{repo: "acme/x", repos: []string{"acme/A", " acme/a ", "", "acme/b"}},
			want:   []string{"acme/A", "acme/b"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.poller.getActiveRepos(); !slices.Equal(got, tc.want) {
				t.Errorf("getActiveRepos() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseMergedPullHeadRef_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		body    string
		want    string
		wantErr bool
	}{
		{
			name: "MergedPRPresent",
			body: `[{"merged_at":null,"head":{"ref":"feat/open"}},{"merged_at":"2026-11-20T10:00:00Z","head":{"ref":"feat/merged"}}]`,
			want: "feat/merged",
		},
		{name: "UnmergedOnly", body: `[{"merged_at":null,"head":{"ref":"feat/open"}}]`, want: ""},
		{name: "EmptyList", body: `[]`, want: ""},
		{name: "MalformedJSON", body: `{not json`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseMergedPullHeadRef([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseMergedPullHeadRef() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("parseMergedPullHeadRef() = %q, want %q", got, tc.want)
			}
		})
	}
}
