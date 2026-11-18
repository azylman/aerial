package main

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// legacyWorkflowRevision returns the revision whose workflow blob actually ran.
// A PR run's head_sha is its source revision, not its checkout merge revision,
// and historical producers publish no metadata, so the immutable checkout SHA
// is recovered from the exact attempt's archived checkout output and bound to
// the PR head through the merge commit's parents before its blob is looked up.
func (s *RouterServer) legacyWorkflowRevision(ctx context.Context, repo string, r metadataWorkflowRun) (string, error) {
	if r.Event != ghEventPullRequest {
		return r.HeadSHA, nil
	}
	path := fmt.Sprintf("/repos/%s/actions/runs/%d/attempts/%d/logs", repo, r.ID, r.RunAttempt)
	req, err := s.newGitHubRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	resp, err := s.githubClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("historical execution logs unavailable: status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20+1))
	if err != nil {
		return "", err
	}
	if len(raw) > 16<<20 {
		return "", fmt.Errorf("historical log archive too large")
	}
	revision, err := checkoutRevisionFromArchive(raw)
	if err != nil {
		return "", err
	}
	if err = s.validateMetadataExecution(ctx, repo, r, revision); err != nil {
		return "", err
	}
	return revision, nil
}

func checkoutRevisionFromArchive(raw []byte) (string, error) {
	archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", err
	}
	revision := ""
	var total int64
	for _, file := range archive.File {
		if file.FileInfo().IsDir() {
			continue
		}
		reader, openErr := file.Open()
		if openErr != nil {
			return "", openErr
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, 8<<20+1))
		closeErr := reader.Close()
		if readErr != nil {
			return "", readErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		total += int64(len(data))
		if len(data) > 8<<20 || total > 64<<20 {
			return "", fmt.Errorf("historical logs exceed limit")
		}
		found, parseErr := checkoutRevisionFromLog(string(data))
		if parseErr != nil {
			return "", parseErr
		}
		if found == "" {
			continue
		}
		if revision != "" && revision != found {
			return "", fmt.Errorf("conflicting checkout revisions")
		}
		revision = found
	}
	if revision == "" {
		return "", fmt.Errorf("historical checkout revision missing")
	}
	return revision, nil
}

func checkoutRevisionFromLog(log string) (string, error) {
	revision := ""
	expectSHA := false
	for line := range strings.SplitSeq(log, "\n") {
		fields := strings.Fields(line)
		if expectSHA && len(fields) == 2 && metadataSHA.MatchString(fields[1]) {
			if revision != "" && revision != fields[1] {
				return "", fmt.Errorf("conflicting checkout revisions")
			}
			revision = fields[1]
		}
		expectSHA = strings.Contains(line, "[command]/") && strings.HasSuffix(strings.TrimSpace(line), "/git log -1 --format=%H")
	}
	return revision, nil
}
