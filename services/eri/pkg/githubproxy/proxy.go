// Package githubproxy serves the PAT-injecting GitHub egress proxy used by
// agent turns: /internal/github-api/ forwards to the GitHub REST API and
// /internal/github-git/ forwards git smart-HTTP traffic. The PAT is read JIT
// from the live config snapshot and never leaves process memory except as the
// upstream Authorization header.
package githubproxy

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

// Route prefixes served by the proxy.
const (
	APIPrefix = "/internal/github-api/"
	GitPrefix = "/internal/github-git/"
)

const (
	defaultAPIBase = "https://api.github.com"
	defaultGitBase = "https://github.com"
	apiBaseEnv     = "GITHUB_API_URL"
	gitBaseEnv     = "AERIAL_GITHUB_GIT_UPSTREAM"
	patMissingBody = `{"error":"GitHub PAT not configured"}`
)

var httpClient = &http.Client{
	Timeout: 0,
}

// Register mounts both proxy routes on mux behind httpkit.LoopbackOnly, so the
// PAT-bearing proxy is reachable only from inside the pod.
func Register(mux *http.ServeMux, cfg *config.Config) {
	mux.Handle(APIPrefix, httpkit.LoopbackOnly(APIHandler(cfg)))
	mux.Handle(GitPrefix, httpkit.LoopbackOnly(GitHandler(cfg)))
}

// APIHandler proxies APIPrefix requests to the GitHub REST API with
// "token <PAT>" auth. It applies no caller restriction of its own.
func APIHandler(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pat, ok := resolvePAT(cfg)
		if !ok {
			http.Error(w, patMissingBody, http.StatusServiceUnavailable)
			return
		}
		forward(w, r, APIPrefix, defaultAPIBase, apiBaseEnv, "token "+pat)
	}
}

// GitHandler proxies GitPrefix requests to GitHub smart HTTP with
// x-access-token basic auth. It applies no caller restriction of its own.
func GitHandler(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pat, ok := resolvePAT(cfg)
		if !ok {
			http.Error(w, patMissingBody, http.StatusServiceUnavailable)
			return
		}
		authVal := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + pat))
		forward(w, r, GitPrefix, defaultGitBase, gitBaseEnv, "Basic "+authVal)
	}
}

// StreamResponse copies resp to w, dropping hop-by-hop headers and flushing
// after every chunk when w supports it.
func StreamResponse(w http.ResponseWriter, resp *http.Response) {
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

func resolvePAT(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	cur := cfg.Current()
	if cur == nil || cur.GitHubPAT == "" {
		return "", false
	}
	return cur.GitHubPAT, true
}

func forward(w http.ResponseWriter, r *http.Request, pathPrefix, defaultBase, envBaseVar, authHeader string) {
	subpath := strings.TrimPrefix(r.URL.Path, pathPrefix)
	subpath = strings.TrimPrefix(subpath, "/")

	upstreamBase := os.Getenv(envBaseVar)
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

	resp, err := httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Upstream proxy error: %v"}`, err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	StreamResponse(w, resp)
}
