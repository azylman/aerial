package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	resp, err := d.doRegistryRequestWithAuth(ctx, http.MethodHead, manifestURL, registry, repository, acceptManifestHeaders) //nolint:bodyclose // closed by httpkit.CloseWarn
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

func (d *SyncDaemon) doRegistryRequestWithAuth(ctx context.Context, method, targetURL, registry, repository, acceptHeader string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed creating request for %s: %w", targetURL, err)
	}
	if acceptHeader != "" {
		req.Header.Set("Accept", acceptHeader)
	}

	resp, err := d.getRegistryClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed for %s: %w", targetURL, err)
	}

	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	httpkit.CloseWarn(resp.Body, "unauthorized probe response body")

	authHeader := resp.Header.Get("WWW-Authenticate")
	token, err := d.fetchRegistryBearerToken(ctx, targetURL, registry, repository, authHeader)
	if err != nil {
		return nil, err
	}

	authReq, aErr := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if aErr != nil {
		return nil, fmt.Errorf("failed creating authenticated request for %s: %w", targetURL, aErr)
	}
	if acceptHeader != "" {
		authReq.Header.Set("Accept", acceptHeader)
	}
	authReq.Header.Set("Authorization", "Bearer "+token)

	finalResp, fErr := d.getRegistryClient().Do(authReq)
	if fErr != nil {
		return nil, fmt.Errorf("authenticated request failed for %s: %w", targetURL, fErr)
	}

	return finalResp, nil
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
