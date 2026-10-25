package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/hangar/pkg/metrics"
)

var sha256DigestRegex = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

const acceptManifestHeaders = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json"

// GetRemoteImageDigest performs an HTTP HEAD query against the target container registry
// to obtain the current manifest digest, authenticating via OAuth2 / Bearer challenge if required.
func (d *SyncDaemon) GetRemoteImageDigest(ctx context.Context, imageRef string) (digest string, err error) {
	start := time.Now()
	var registry string
	defer func() {
		metrics.RecordRegistryManifest(registry, metrics.SanitizeStatus(err), time.Since(start))
	}()

	var repository, tag string
	registry, repository, tag, err = ParseImageReference(imageRef)
	if err != nil {
		return "", fmt.Errorf("invalid image reference %q: %w", imageRef, err)
	}

	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, tag)
	resp, _, err := d.doRegistryRequestWithAuth(ctx, http.MethodHead, manifestURL, registry, repository, acceptManifestHeaders, "") //nolint:bodyclose // closed by httpkit.CloseWarn
	if err != nil {
		return "", err
	}
	defer httpkit.CloseWarn(resp.Body, "manifest response body")

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("manifest query for %s returned HTTP %d", imageRef, resp.StatusCode)
	}

	digest = strings.TrimSpace(resp.Header.Get("Docker-Content-Digest"))
	if digest == "" {
		etag := strings.TrimSpace(resp.Header.Get("ETag"))
		etag = strings.TrimPrefix(etag, "W/")
		etag = strings.Trim(etag, "\"")
		if sha256DigestRegex.MatchString(etag) {
			digest = etag
		}
	}

	if digest == "" || !sha256DigestRegex.MatchString(digest) {
		return "", fmt.Errorf("no valid digest returned for %s (Docker-Content-Digest: %q, ETag: %q)",
			imageRef, resp.Header.Get("Docker-Content-Digest"), resp.Header.Get("ETag"))
	}

	return digest, nil
}

func (d *SyncDaemon) doRegistryRequestWithAuth(ctx context.Context, method, targetURL, registry, repository, acceptHeader, existingBearer string) (*http.Response, string, error) {
	req, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed creating request for %s: %w", targetURL, err)
	}
	if acceptHeader != "" {
		req.Header.Set("Accept", acceptHeader)
	}
	if existingBearer != "" {
		req.Header.Set("Authorization", "Bearer "+existingBearer)
	}

	resp, err := d.getRegistryClient().Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("request failed for %s: %w", targetURL, err)
	}

	if resp.StatusCode != http.StatusUnauthorized {
		return resp, existingBearer, nil
	}

	if existingBearer != "" {
		httpkit.CloseWarn(resp.Body, "unauthorized response body with existing bearer")
	} else {
		httpkit.CloseWarn(resp.Body, "unauthorized probe response body")
	}

	authHeader := resp.Header.Get("Www-Authenticate")
	token, err := d.fetchRegistryBearerToken(ctx, targetURL, registry, repository, authHeader)
	if err != nil {
		return nil, "", err
	}

	authReq, aErr := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if aErr != nil {
		return nil, "", fmt.Errorf("failed creating authenticated request for %s: %w", targetURL, aErr)
	}
	if acceptHeader != "" {
		authReq.Header.Set("Accept", acceptHeader)
	}
	authReq.Header.Set("Authorization", "Bearer "+token)

	finalResp, fErr := d.getRegistryClient().Do(authReq)
	if fErr != nil {
		return nil, "", fmt.Errorf("authenticated request failed for %s: %w", targetURL, fErr)
	}

	return finalResp, token, nil
}

func (d *SyncDaemon) fetchRegistryBearerToken(ctx context.Context, targetURL, registry, repository, authHeader string) (string, error) {
	realm, service, scope, pErr := ParseWwwAuthenticate(authHeader)
	if pErr != nil {
		return "", fmt.Errorf("failed parsing Www-Authenticate header for %s: %w", targetURL, pErr)
	}
	if scope == "" {
		scope = fmt.Sprintf("repository:%s:pull", repository)
	}

	tokURL, uErr := url.Parse(realm)
	if uErr != nil {
		return "", fmt.Errorf("failed parsing token realm %q: %w", realm, uErr)
	}
	q := tokURL.Query()
	if service != "" {
		q.Set("service", service)
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	tokURL.RawQuery = q.Encode()

	tokReq, tErr := http.NewRequestWithContext(ctx, http.MethodGet, tokURL.String(), nil)
	if tErr != nil {
		return "", fmt.Errorf("failed creating token request for %s: %w", targetURL, tErr)
	}

	if registry == "ghcr.io" && d != nil && strings.TrimSpace(d.pat) != "" {
		username := ResolveRegistryUser("", os.Getenv)
		tokReq.SetBasicAuth(username, d.pat)
	}

	tokResp, dErr := d.getRegistryClient().Do(tokReq) //nolint:bodyclose // closed by httpkit.CloseWarn
	if dErr != nil {
		return "", fmt.Errorf("token request failed for %s: %w", targetURL, dErr)
	}
	defer httpkit.CloseWarn(tokResp.Body, "token response body")

	if tokResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request for %s returned HTTP %d", targetURL, tokResp.StatusCode)
	}

	var tokPayload struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	decErr := json.NewDecoder(tokResp.Body).Decode(&tokPayload)
	if decErr != nil {
		return "", fmt.Errorf("failed decoding token for %s: %w", targetURL, decErr)
	}

	token := tokPayload.Token
	if token == "" {
		token = tokPayload.AccessToken
	}
	if token == "" {
		return "", fmt.Errorf("empty bearer token returned for %s", targetURL)
	}

	return token, nil
}

func (d *SyncDaemon) fetchRegistryBytes(ctx context.Context, targetURL, registry, repository, acceptHeader, existingBearer, label string) ([]byte, string, error) {
	resp, token, err := d.doRegistryRequestWithAuth(ctx, http.MethodGet, targetURL, registry, repository, acceptHeader, existingBearer) //nolint:bodyclose // closed by httpkit.CloseWarn
	if err != nil {
		return nil, "", err
	}
	defer httpkit.CloseWarn(resp.Body, label+" response body")

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("%s request returned HTTP %d", label, resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, "", fmt.Errorf("failed to read %s body: %w", label, err)
	}
	return bodyBytes, token, nil
}

// GetRemoteImageRevision retrieves the git commit revision associated with an OCI/Docker container image
// by inspecting manifest annotations or configuration blob labels.
func (d *SyncDaemon) GetRemoteImageRevision(ctx context.Context, imageRef string) (string, error) {
	registry, repository, tagOrDigest, err := ParseImageReference(imageRef)
	if err != nil {
		return "", fmt.Errorf("invalid image reference %q: %w", imageRef, err)
	}

	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, tagOrDigest)
	bodyBytes, bearerToken, err := d.fetchRegistryBytes(ctx, manifestURL, registry, repository, acceptManifestHeaders, "", fmt.Sprintf("manifest for %s", imageRef))
	if err != nil {
		return "", err
	}

	// Check if this is an OCI Image Index or Docker Manifest List
	childDigest := ExtractPlatformManifestDigest(bodyBytes, "linux", "amd64")
	if childDigest != "" {
		if rev := ExtractRevisionFromAnnotations(ExtractManifestAnnotations(bodyBytes)); rev != "" {
			return rev, nil
		}

		childURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, childDigest)
		childBytes, childToken, childErr := d.fetchRegistryBytes(ctx, childURL, registry, repository, acceptManifestHeaders, bearerToken, fmt.Sprintf("child manifest for %s (%s)", imageRef, childDigest))
		if childErr != nil {
			return "", childErr
		}

		bodyBytes = childBytes
		if childToken != "" {
			bearerToken = childToken
		}
	}

	// Single manifest processing:
	// 1. Check annotations on manifest
	if rev := ExtractRevisionFromAnnotations(ExtractManifestAnnotations(bodyBytes)); rev != "" {
		return rev, nil
	}

	// 2. Extract config blob digest and fetch config blob
	configDigest := ExtractConfigBlobDigest(bodyBytes)
	if configDigest == "" {
		return "", fmt.Errorf("no config digest or revision annotations found for %s", imageRef)
	}

	blobURL := fmt.Sprintf("https://%s/v2/%s/blobs/%s", registry, repository, configDigest)
	const acceptBlobHeaders = "application/vnd.oci.image.config.v1+json, application/vnd.docker.container.image.v1+json, application/octet-stream"
	blobBytes, _, blobErr := d.fetchRegistryBytes(ctx, blobURL, registry, repository, acceptBlobHeaders, bearerToken, fmt.Sprintf("config blob for %s (%s)", imageRef, configDigest))
	if blobErr != nil {
		return "", blobErr
	}

	rev := ExtractRevisionFromConfigJSON(blobBytes)
	if rev == "" {
		return "", fmt.Errorf("no valid revision label found in config blob for %s", imageRef)
	}

	return rev, nil
}

// EnsureDockerAuth writes or updates the Docker config.json file with authentication credentials
// for ghcr.io using the daemon's configured PAT, enabling Docker Compose to pull private sidecar images.
func (d *SyncDaemon) EnsureDockerAuth() error {
	if d == nil || strings.TrimSpace(d.pat) == "" {
		return nil
	}

	repoURL := ""
	if d.repoUrls != nil {
		repoURL = d.repoUrls[d.configDir]
		if repoURL == "" {
			repoURL = d.repoUrls[d.composeDir]
		}
	}

	return EnsureDockerAuthPath(d.pat, repoURL, os.Getenv, os.ReadFile, os.WriteFile, os.MkdirAll)
}

// EnsureDockerAuthPath is the parameterized implementation of EnsureDockerAuth, enabling
// hermetic unit testing without filesystem or ambient environment side-effects.
func EnsureDockerAuthPath(
	pat string,
	repoURL string,
	lookup func(string) string,
	readFile func(string) ([]byte, error),
	writeFile func(string, []byte, os.FileMode) error,
	mkdirAll func(string, os.FileMode) error,
) error {
	pat = strings.TrimSpace(pat)
	if pat == "" {
		return nil
	}

	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	if readFile == nil {
		readFile = os.ReadFile
	}
	if writeFile == nil {
		writeFile = os.WriteFile
	}
	if mkdirAll == nil {
		mkdirAll = os.MkdirAll
	}

	configPath := ResolveDockerConfigPath(lookup)
	configDir := filepath.Dir(configPath)

	if err := mkdirAll(configDir, 0o700); err != nil {
		return fmt.Errorf("failed to create docker config directory %s: %w", configDir, err)
	}

	var existingContent []byte
	if data, err := readFile(configPath); err == nil {
		existingContent = data
	} else if !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read existing docker config %s: %w", configPath, err)
	}

	username := ResolveRegistryUser(repoURL, lookup)
	newContent, err := GenerateDockerConfig(existingContent, "ghcr.io", username, pat)
	if err != nil {
		return fmt.Errorf("failed to generate docker config: %w", err)
	}

	if err := writeFile(configPath, newContent, 0o600); err != nil {
		return fmt.Errorf("failed to write docker config to %s: %w", configPath, err)
	}

	return nil
}

// CheckGitHubRepoBuildInProgress checks if any GitHub Actions workflow runs are queued or in progress for a repo.
func (d *SyncDaemon) CheckGitHubRepoBuildInProgress(ctx context.Context, repoSlug string) bool {
	if d == nil {
		return false
	}
	slug := NormalizeGitHubSlug(repoSlug)
	if slug == "" {
		return false
	}

	client := d.getRegistryClient()
	checkStatus := func(status string) bool {
		apiURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs?status=%s&per_page=5", slug, status)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			log.Printf("[Hangar:GitHub] Warning: failed to create actions request for %s: %v", slug, err)
			return false
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		if strings.TrimSpace(d.pat) != "" {
			req.Header.Set("Authorization", "Bearer "+d.pat)
		}

		resp, err := client.Do(req) //nolint:bodyclose // closed by httpkit.CloseWarn
		if err != nil {
			log.Printf("[Hangar:GitHub] Warning: actions API request failed for %s: %v", slug, err)
			return false
		}
		defer httpkit.CloseWarn(resp.Body, "github actions response body")

		if resp.StatusCode == http.StatusOK {
			var data struct {
				TotalCount int `json:"total_count"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&data); err == nil && data.TotalCount > 0 {
				log.Printf("[Hangar:GitHub] %d %s workflow run(s) for %s", data.TotalCount, status, slug)
				return true
			}
		}
		return false
	}

	for _, status := range []string{"in_progress", "queued"} {
		if checkStatus(status) {
			return true
		}
	}

	return false
}
