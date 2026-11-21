package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- 6. Workflow Run Image Resolution & Dispatch ---

type resolveWorkflowRunCase struct {
	name    string
	repo    string
	runID   int64
	want    []string
	wantErr bool
}

func newWorkflowRunJobsMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 8,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 8, Name: "Build & Push Images to GHCR (${{ matrix.service }})", Status: "completed", Conclusion: "success", Steps: []GitHubWorkflowJobStep{{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "success", Number: 1}}},
			{ID: 1, Name: "Detect Changed Microservices", Status: "completed", Conclusion: "success"},
			{ID: 2, Name: "Run Service Unit Tests", Status: "completed", Conclusion: "success"},
			{ID: 3, Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"},
			{ID: 4, Name: "Build & Push Images to GHCR (synapse)", Status: "completed", Conclusion: "success"},
			{ID: 5, Name: "Build & Push Images to GHCR (custom-microservice)", Status: "completed", Conclusion: "success"},
			{ID: 6, Name: "Build & Push Images to GHCR (failed-service)", Status: "completed", Conclusion: "failure"},
			{ID: 7, Name: "Build & Push Images to GHCR (running-service)", Status: "in_progress", Conclusion: ""},
		},
	}
	mockSidecarJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 10, Name: "Build & Push Sidecar Images to GHCR (banana)", Status: "completed", Conclusion: "success"},
			{ID: 11, Name: "Build & Push Sidecar Images to GHCR (voice-cuda)", Status: "completed", Conclusion: "success"},
		},
	}
	mockMirrormereJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 20, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success"},
			{ID: 21, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success"},
		},
	}
	mockSkippedStepJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{
				ID: 101, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "skipped", Number: 2},
				},
			},
			{
				ID: 102, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "success", Number: 2},
				},
			},
		},
	}
	mockSkippedPromotionJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 201, Name: "Build & Push Images to GHCR (eri)", Status: "completed", Conclusion: "success"},
			{ID: 202, Name: "Promote Images to :latest", Status: "completed", Conclusion: "skipped"},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/actions/runs/12345/jobs"):
			_ = json.NewEncoder(w).Encode(mockJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/54321/jobs"):
			_ = json.NewEncoder(w).Encode(mockSidecarJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/67890/jobs"):
			_ = json.NewEncoder(w).Encode(mockMirrormereJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/77777/jobs"):
			_ = json.NewEncoder(w).Encode(mockSkippedStepJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/88888/jobs"):
			_ = json.NewEncoder(w).Encode(mockSkippedPromotionJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/403/jobs"):
			w.Header().Set("X-Ratelimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
		case strings.Contains(r.URL.Path, "/actions/runs/500/jobs"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/actions/runs/999/jobs"):
			_, _ = w.Write([]byte("malformed json"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func resolveWorkflowRunTestCases() []resolveWorkflowRunCase {
	return []resolveWorkflowRunCase{
		{
			name:  "aerial monorepo images",
			repo:  "azylman/aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/brain:latest",
				"ghcr.io/azylman/synapse:latest",
				"ghcr.io/azylman/custom-microservice:latest",
			},
		},
		{
			name:  "unprefixed repo normalization",
			repo:  "aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/brain:latest",
				"ghcr.io/azylman/synapse:latest",
				"ghcr.io/azylman/custom-microservice:latest",
			},
		},
		{
			name:  "aerial-sidecars images",
			repo:  "azylman/aerial-sidecars",
			runID: 54321,
			want: []string{
				"ghcr.io/azylman/banana:latest",
				"ghcr.io/azylman/voice-cuda:latest",
			},
		},
		{
			name:  "mirrormere images",
			repo:  "azylman/mirrormere",
			runID: 67890,
			want: []string{
				"ghcr.io/azylman/mirrormere:latest",
				"ghcr.io/azylman/mirrormere-cast-watcher:latest",
			},
		},
		{
			name:  "skipped build step filtering",
			repo:  "azylman/mirrormere",
			runID: 77777,
			want:  []string{"ghcr.io/azylman/mirrormere-cast-watcher:latest"},
		},
		{
			name:  "skipped image promotion returns nil",
			repo:  "azylman/aerial",
			runID: 88888,
			want:  nil,
		},
		{name: "empty repo", repo: "", runID: 12345, wantErr: true},
		{name: "zero runID", repo: "aerial", runID: 0, wantErr: true},
		{name: "403 rate limit", repo: "aerial", runID: 403, wantErr: true},
		{name: "500 server error", repo: "aerial", runID: 500, wantErr: true},
		{name: "malformed json", repo: "aerial", runID: 999, wantErr: true},
	}
}

func runResolveWorkflowRunImagesCase(ctx context.Context, t *testing.T, srv *RouterServer, tc resolveWorkflowRunCase) {
	t.Helper()
	t.Parallel()
	images, err := srv.resolveLegacyWorkflowRunImages(ctx, tc.repo, tc.runID)
	if (err != nil) != tc.wantErr {
		t.Fatalf("resolveWorkflowRunImages() err = %v, wantErr %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}
	if len(images) != len(tc.want) {
		t.Fatalf("expected %v, got %v", tc.want, images)
	}
	for i := range tc.want {
		if images[i] != tc.want[i] {
			t.Errorf("image[%d] = %q, want %q", i, images[i], tc.want[i])
		}
	}
}

func TestResolveWorkflowRunImages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := newWorkflowRunJobsMockServer(t)
	srv := NewRouterServer(fixedConfig(Config{GitHubAPIURL: ts.URL}), ts.Client())

	for _, tc := range resolveWorkflowRunTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runResolveWorkflowRunImagesCase(ctx, t, srv, tc)
		})
	}
}
