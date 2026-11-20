package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/guard"
)

var proxyHTTPClient = &http.Client{
	Timeout: 0,
}

func copyAndFlush(w http.ResponseWriter, f http.Flusher, r io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
			f.Flush()
		}
		if err != nil {
			return
		}
	}
}

func streamProxyResponse(w http.ResponseWriter, resp *http.Response) {
	for k, vv := range resp.Header {
		if strings.EqualFold(k, "Transfer-Encoding") || strings.EqualFold(k, "Connection") {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if f, ok := w.(http.Flusher); ok {
		copyAndFlush(w, f, resp.Body)
		return
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		return
	}
}

func resolveConfigPAT(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	cur := cfg.Current()
	if cur == nil || cur.GitHubPAT == "" {
		return "", false
	}
	return cur.GitHubPAT, true
}

func executeGitHubProxy(w http.ResponseWriter, r *http.Request, pathPrefix, defaultBase, envBaseVar, authHeader string) {
	subpath := strings.TrimPrefix(r.URL.Path, pathPrefix)
	subpath = strings.TrimPrefix(subpath, "/")

	upstreamBase := ""
	if envBaseVar != "" {
		upstreamBase = os.Getenv(envBaseVar)
	}
	if upstreamBase == "" {
		upstreamBase = defaultBase
	}
	targetURL := strings.TrimSuffix(upstreamBase, "/") + "/" + subpath
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Failed to create proxy request: %v"}`, err), http.StatusInternalServerError)
		return
	}

	for k, vv := range r.Header {
		if strings.EqualFold(k, "Authorization") || strings.EqualFold(k, "Connection") || strings.EqualFold(k, "Transfer-Encoding") {
			continue
		}
		for _, v := range vv {
			proxyReq.Header.Add(k, v)
		}
	}
	proxyReq.Header.Set("Authorization", authHeader)

	resp, err := proxyHTTPClient.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Upstream proxy error: %v"}`, err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	streamProxyResponse(w, resp)
}

func handleInternalGitHubAPIProxy(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pat, ok := resolveConfigPAT(cfg)
		if !ok {
			http.Error(w, `{"error":"GitHub PAT not configured"}`, http.StatusServiceUnavailable)
			return
		}
		executeGitHubProxy(w, r, "/internal/github-api/", "https://api.github.com", "GITHUB_API_URL", "token "+pat)
	}
}

func handleInternalGitHubGitProxy(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pat, ok := resolveConfigPAT(cfg)
		if !ok {
			http.Error(w, `{"error":"GitHub PAT not configured"}`, http.StatusServiceUnavailable)
			return
		}
		authVal := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + pat))
		executeGitHubProxy(w, r, "/internal/github-git/", "https://github.com", "AERIAL_GITHUB_GIT_UPSTREAM", "Basic "+authVal)
	}
}

// GuardScanRequest represents the payload for POST /internal/guard/scan.
type GuardScanRequest struct {
	Diff                 string `json:"diff"`
	Text                 string `json:"text"`
	SourceLabel          string `json:"source_label"`
	CheckTestFiles       bool   `json:"check_test_files"`
	IgnoreInternalDomain bool   `json:"ignore_internal_domain"`
}

// GuardScanResponse represents the outcome of a guard scan request.
type GuardScanResponse struct {
	Decision   string            `json:"decision"`
	Reason     string            `json:"reason,omitempty"`
	Violations []guard.Violation `json:"violations,omitempty"`
}

func handleInternalGuardScan() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req GuardScanRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{jsonKeyError: "Invalid request payload"})
			return
		}

		var violations []guard.Violation
		patterns := guard.GetActivePatterns()

		if req.Diff != "" {
			diffPatterns := patterns
			if req.IgnoreInternalDomain {
				filtered := make([]guard.PatternRule, 0, len(patterns))
				for _, p := range patterns {
					if p.Category != "InternalClusterDomain" {
						filtered = append(filtered, p)
					}
				}
				diffPatterns = filtered
			}
			violations = append(violations, guard.ScanDiffForSecrets(req.Diff, req.CheckTestFiles, diffPatterns)...)
		}

		if req.Text != "" {
			violations = append(violations, guard.ScanTextForSecrets(req.Text, req.SourceLabel, patterns)...)
		}

		resp := GuardScanResponse{
			Decision: "allow",
		}
		if len(violations) > 0 {
			resp.Decision = "deny"
			resp.Reason = guard.FormatCommitDenyReason(violations)
			resp.Violations = violations
		}

		httpkit.WriteJSON(w, http.StatusOK, resp)
	}
}
