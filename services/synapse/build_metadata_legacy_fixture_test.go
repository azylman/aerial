package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// legacyFixtureHead is the main-branch revision every deployment-plumbing
// fixture's producer run executed.
const legacyFixtureHead = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// legacyFixtureBlob stands in for an inventoried pre-producer workflow blob.
const legacyFixtureBlob = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// legacyFixtureConfig pins legacyFixtureBlob in the immutable allowlist so the
// deployment-plumbing fixtures, which serve historical job names, exercise the
// bounded legacy adapter; structured transport has its own fixtures.
func legacyFixtureConfig(cfg Config) Config {
	for _, repo := range []string{normalizeRepo("aerial"), normalizeRepo("aerial-sidecars"), normalizeRepo("mirrormere")} {
		cfg.LegacyBuildWorkflows = append(cfg.LegacyBuildWorkflows, LegacyBuildWorkflow{Repository: repo, WorkflowPath: metadataWorkflowPath(repo), BlobSHA: legacyFixtureBlob})
	}
	return cfg
}

// legacyMetadataTransport answers the authoritative run, transport-check and
// workflow-blob lookups for a historical attempt-1 main-branch run, delegating
// every other request (jobs, PRs, Kubernetes) to the test's own server.
type legacyMetadataTransport struct{ base http.RoundTripper }

func (l legacyMetadataTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	body := ""
	switch {
	case len(parts) == 6 && parts[0] == "repos" && parts[3] == "actions" && parts[4] == "runs":
		id, err := strconv.ParseInt(parts[5], 10, 64)
		if err != nil {
			return nil, err
		}
		repo := parts[1] + "/" + parts[2]
		body = fmt.Sprintf(`{"id":%d,"run_number":1,"run_attempt":1,"repository":{"full_name":%q},"path":%q,"event":"push","head_branch":"main","head_sha":%q,"status":"completed","conclusion":"success"}`, id, repo, metadataWorkflowPath(repo), legacyFixtureHead)
	case len(parts) == 6 && parts[3] == "commits" && parts[5] == "check-runs":
		body = `{"check_runs":[]}`
	case len(parts) >= 5 && parts[3] == "contents":
		body = fmt.Sprintf(`{"sha":%q}`, legacyFixtureBlob)
	}
	if body != "" {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	}
	return l.base.RoundTrip(r)
}

func legacyFixtureClient(client *http.Client) *http.Client {
	cloned := *client
	cloned.Transport = legacyMetadataTransport{base: client.Transport}
	return &cloned
}
