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
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

// Route prefixes served by the proxy.
const (
	APIPrefix       = "/internal/github-api/"
	GitPrefix       = "/internal/github-git/"
	GHCRTokenPrefix = "/internal/github-ghcr-token"
)

const (
	defaultAPIBase      = "https://api.github.com"
	defaultGitBase      = "https://github.com"
	defaultGHCRTokenURL = "https://ghcr.io/token"
	apiBaseEnv          = "GITHUB_API_URL"
	gitBaseEnv          = "AERIAL_GITHUB_GIT_UPSTREAM"
	ghcrTokenURLEnv     = "AERIAL_GHCR_TOKEN_UPSTREAM"
	patMissingBody      = `{"error":"GitHub PAT not configured"}`
)

var httpClient = &http.Client{
	Timeout: 0,
}

// Register mounts proxy routes on mux behind httpkit.LoopbackOnly, so the
// PAT-bearing proxy is reachable only from inside the pod.
func Register(mux *http.ServeMux, cfg *config.Config) {
	mux.Handle(APIPrefix, httpkit.LoopbackOnly(APIHandler(cfg)))
	mux.Handle(GitPrefix, httpkit.LoopbackOnly(GitHandler(cfg)))
	mux.Handle(GHCRTokenPrefix, httpkit.LoopbackOnly(GHCRTokenHandler(cfg)))
	mux.Handle(GHCRTokenPrefix+"/", httpkit.LoopbackOnly(GHCRTokenHandler(cfg)))
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
		body, ok := scanAPIWrite(w, r, cfg, pat)
		if !ok {
			return
		}
		forward(w, r, body, APIPrefix, defaultAPIBase, apiBaseEnv, "token "+pat)
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
		body, ok := scanGitPush(w, r, cfg, pat)
		if !ok {
			return
		}
		forward(w, r, body, GitPrefix, defaultGitBase, gitBaseEnv, "Basic "+authVal)
	}
}

var pullScopePattern = regexp.MustCompile(`^repository:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+:pull$`)

func validateGHCRTokenRequest(r *http.Request) (service string, scopes []string, errStatus int, errMsg string) {
	if r.Method != http.MethodGet {
		return "", nil, http.StatusMethodNotAllowed, `{"error":"Method not allowed"}`
	}
	if r.URL.Path != GHCRTokenPrefix {
		return "", nil, http.StatusForbidden, `{"error":"Forbidden path"}`
	}

	service = r.URL.Query().Get("service")
	if service != "ghcr.io" && service != "ghcr.io/token" {
		return "", nil, http.StatusForbidden, `{"error":"Invalid or unauthorized service"}`
	}

	scopes = r.URL.Query()["scope"]
	if len(scopes) == 0 {
		return "", nil, http.StatusForbidden, `{"error":"Scope is required"}`
	}
	for _, scope := range scopes {
		if !pullScopePattern.MatchString(scope) {
			return "", nil, http.StatusForbidden, `{"error":"Unauthorized scope"}`
		}
	}
	return service, scopes, 0, ""
}

func buildGHCRTargetURL(service string, scopes []string) string {
	targetBase := upstreamBase(ghcrTokenURLEnv, defaultGHCRTokenURL)
	q := url.Values{}
	q.Set("service", service)
	for _, scope := range scopes {
		q.Add("scope", scope)
	}
	if strings.Contains(targetBase, "?") {
		return targetBase + "&" + q.Encode()
	}
	return targetBase + "?" + q.Encode()
}

// GHCRTokenHandler proxies GHCRTokenPrefix requests to ghcr.io/token with
// x-access-token basic auth for read-only repository pull scopes.
func GHCRTokenHandler(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		service, scopes, status, errMsg := validateGHCRTokenRequest(r)
		if status != 0 {
			http.Error(w, errMsg, status)
			return
		}

		pat, ok := resolvePAT(cfg)
		if !ok {
			http.Error(w, patMissingBody, http.StatusServiceUnavailable)
			return
		}

		targetURL := buildGHCRTargetURL(service, scopes)
		proxyReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL, http.NoBody)
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

		authVal := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + pat))
		proxyReq.Header.Set("Authorization", "Basic "+authVal)

		resp, err := httpClient.Do(proxyReq)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"Upstream proxy error: %v"}`, err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		StreamResponse(w, resp)
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

func upstreamBase(envBaseVar, defaultBase string) string {
	if base := os.Getenv(envBaseVar); base != "" {
		return strings.TrimSuffix(base, "/")
	}
	return defaultBase
}

// forward relays r upstream with body (r.Body when the request was not scanned).
func forward(w http.ResponseWriter, r *http.Request, body io.Reader, pathPrefix, defaultBase, envBaseVar, authHeader string) {
	subpath := strings.TrimPrefix(r.URL.Path, pathPrefix)
	subpath = strings.TrimPrefix(subpath, "/")

	targetURL := upstreamBase(envBaseVar, defaultBase) + "/" + subpath
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, body)
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
