package router

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
)

// metadataAPI is a declarative GitHub API serving one repository's producer runs.
type metadataAPI struct {
	runs                                     []metadataWorkflowRun
	checks                                   []GitHubMetadataCheck
	paginate                                 bool
	parents                                  []string
	runStatus, checksStatus, cmt, listStatus int
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
	case len(parts) == 6 && parts[3] == "commits" && parts[5] == "check-runs":
		body, status = GitHubCheckRunsResponse{CheckRuns: f.checkPage(r.URL.Query().Get("page"))}, firstStatus(f.checksStatus, status)
	case len(parts) == 5 && parts[3] == "commits":
		parents := make([]map[string]string, 0, len(f.parents))
		for _, sha := range f.parents {
			parents = append(parents, map[string]string{"sha": sha})
		}
		body, status = map[string]any{"sha": parts[4], "parents": parents}, firstStatus(f.cmt, status)
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
