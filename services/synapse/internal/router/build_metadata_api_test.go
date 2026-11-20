package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
)

// metadataAPI is a declarative GitHub API serving one repository's producer runs.
type metadataAPI struct {
	runs                                     []metadataWorkflowRun
	checks                                   []GitHubMetadataCheck
	paginate                                 bool
	parents                                  []string
	blob                                     string
	logs                                     []byte
	jobs                                     []GitHubWorkflowJobItem
	runStatus, checksStatus, blobStatus, cmt int
	listStatus, logsStatus                   int
}

func (f metadataAPI) serve(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var body any
	status := http.StatusOK
	switch {
	case len(parts) == 5 && parts[3] == "actions" && parts[4] == "runs":
		body, status = map[string]any{"workflow_runs": f.runs}, firstStatus(f.listStatus, status)
	case len(parts) == 6 && parts[3] == "actions" && parts[4] == "runs":
		body, status = f.runByID(parts[5])
	case len(parts) == 7 && parts[6] == "jobs":
		body = GitHubWorkflowJobsResponse{Jobs: f.jobs}
	case len(parts) == 9 && parts[8] == "logs":
		w.WriteHeader(firstStatus(f.logsStatus, http.StatusOK))
		_, _ = w.Write(f.logs)
		return
	case len(parts) == 6 && parts[3] == "commits" && parts[5] == "check-runs":
		body, status = GitHubCheckRunsResponse{CheckRuns: f.checkPage(r.URL.Query().Get("page"))}, firstStatus(f.checksStatus, status)
	case len(parts) == 5 && parts[3] == "commits":
		parents := []map[string]string{}
		for _, sha := range f.parents {
			parents = append(parents, map[string]string{"sha": sha})
		}
		body, status = map[string]any{"sha": parts[4], "parents": parents}, firstStatus(f.cmt, status)
	case len(parts) >= 5 && parts[3] == "contents":
		body, status = map[string]string{"sha": f.blob}, firstStatus(f.blobStatus, status)
	default:
		status = http.StatusNotFound
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (f metadataAPI) runByID(id string) (any, int) {
	for _, run := range f.runs {
		if strconv.FormatInt(run.ID, 10) == id {
			return run, firstStatus(f.runStatus, http.StatusOK)
		}
	}
	return nil, http.StatusNotFound
}

// checkPage fills page 1 with native checks when paginating, so metadata only
// appears on page 2.
func (f metadataAPI) checkPage(page string) []GitHubMetadataCheck {
	if !f.paginate || page != "1" {
		return f.checks
	}
	natives := make([]GitHubMetadataCheck, buildmeta.MetadataListPageSize)
	for i := range natives {
		natives[i] = GitHubMetadataCheck{ID: int64(i + 1), Name: fmt.Sprintf("native-%d", i)}
	}
	return natives
}

func firstStatus(override, status int) int {
	if override != 0 {
		return override
	}
	return status
}

func runs(rs ...metadataWorkflowRun) []metadataWorkflowRun { return rs }

func TestBuildMetadataEventIngestion(t *testing.T) {
	t.Parallel()
	pr := tPRMeta(1, "engine")
	prChecks := checks(tCheck(pr), tCheck(tResultOf(pr, "engine", buildResultRunning, false)))
	api := metadataAPI{runs: runs(tPRRun(1, "in_progress", "")), checks: prChecks, parents: []string{tBase, tHead}}
	checkRun := `{"action":"completed","repository":{"full_name":"example/aerial"},"check_run":{"name":"build-metadata-result","head_sha":"` + tHead + `","status":"completed","conclusion":"neutral","pull_requests":[{"number":42,"head":{"sha":"` + tHead + `","ref":"BRANCH"}}]}}`
	workflowRun := `{"action":"in_progress","repository":{"full_name":"example/aerial"},"workflow_run":{"id":77,"run_attempt":1,"path":".github/workflows/docker-publish.yml","event":"pull_request","name":"CD","head_sha":"` + tHead + `","head_branch":"BRANCH","status":"in_progress","pull_requests":[{"number":42,"head":{"sha":"` + tHead + `","ref":"BRANCH"}}]}}`
	tests := []struct {
		name, event, payload, branch string
		owner                        bool
		checksStatus                 int
		wantWrites                   int
		wantPublished                int
		wantErr                      bool
		busErr                       error
	}{
		{"Dependabot transport check ingests", ghEventCheckRun, checkRun, "dependabot/go_modules/x", true, 0, 1, 1, false, nil},
		{"feature transport check ingests", ghEventCheckRun, checkRun, "feature", true, 0, 1, 1, false, nil},
		{"transport check publish error handled", ghEventCheckRun, checkRun, "feature", true, 0, 1, 0, false, errors.New("bus fail")},
		{"foreign stack owner skips", ghEventCheckRun, checkRun, "", false, 0, 0, 0, false, nil},
		{"transport API failure retries", ghEventCheckRun, checkRun, "feature", true, http.StatusBadGateway, 0, 0, true, nil},
		{"workflow run refreshes progress", ghEventWorkflowRun, workflowRun, "feature", true, 0, 1, 0, false, nil},
		{"workflow run refresh failure never blocks CI handling", ghEventWorkflowRun, workflowRun, "feature", true, http.StatusBadGateway, 0, 0, false, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := api
			fixture.checksStatus = tc.checksStatus
			server := httptest.NewServer(http.HandlerFunc(fixture.serve))
			defer server.Close()
			reg := &mockPRRegistry{resolveBranch: tc.branch, resolveHeadSHA: tHead, buildLifecycle: PRBuildLifecycle{Status: prOpen, HeadSHA: tHead}}
			if tc.branch != "" {
				reg.resolveTargetID = "100000000000000001"
			}
			bus := &mockEventBus{err: tc.busErr}
			srv := newTestRouter(Config{GitHubAPIURL: server.URL, DefaultOwner: tc.owner}, server.Client(), reg, nil, bus)
			_, err := srv.ProcessGitHubEvent(context.Background(), tc.event, "delivery", []byte(strings.ReplaceAll(tc.payload, "BRANCH", tc.branch)))
			if (err != nil) != tc.wantErr || len(reg.recordedBuildMetadata) != tc.wantWrites {
				t.Fatalf("err=%v writes=%d, want err=%v writes=%d", err, len(reg.recordedBuildMetadata), tc.wantErr, tc.wantWrites)
			}
			published := 0
			for _, ev := range bus.publishedPREvents {
				if ev.Event == "check_updated" && ev.PRNumber == 42 && ev.CheckName == "" {
					published++
				}
			}
			if published != tc.wantPublished {
				t.Fatalf("published=%d want %d", published, tc.wantPublished)
			}
		})
	}
}
