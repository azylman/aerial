package githubproxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/guard"
)

const (
	maxScanBodyBytes = 256 << 20
	visibilityTTL    = 10 * time.Minute
	receivePackPath  = "/git-receive-pack"
	sideBandMaxData  = 65515
	sideBandSmallMax = 995
)

// visibilityCache remembers whether owner/repo is public per API base.
// Lookup failures are not cached and count as public (fail closed).
type visibilityCache struct {
	mu      sync.Mutex
	entries map[string]visibilityEntry
}

type visibilityEntry struct {
	public  bool
	expires time.Time
}

var repoVisibility = &visibilityCache{entries: map[string]visibilityEntry{}}

func (c *visibilityCache) isPublic(ctx context.Context, base, owner, repo, pat string) bool {
	key := strings.ToLower(base + "|" + owner + "/" + repo)
	now := time.Now()
	c.mu.Lock()
	e, ok := c.entries[key]
	c.mu.Unlock()
	if ok && now.Before(e.expires) {
		return e.public
	}
	var meta struct {
		Private *bool `json:"private"`
	}
	if err := getJSON(ctx, base+"/repos/"+owner+"/"+repo, pat, &meta); err != nil || meta.Private == nil {
		return true
	}
	public := !*meta.Private
	c.mu.Lock()
	c.entries[key] = visibilityEntry{public: public, expires: now.Add(visibilityTTL)}
	c.mu.Unlock()
	return public
}

type httpStatusError struct{ code int }

func (e *httpStatusError) Error() string { return fmt.Sprintf("GitHub API returned %d", e.code) }

func getJSON(ctx context.Context, url, pat string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "token "+pat)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &httpStatusError{code: resp.StatusCode}
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxScanBodyBytes)).Decode(dst)
}

// apiFetcher implements guard.ObjectFetcher over the GitHub git data API.
type apiFetcher struct {
	base string
	pat  string
}

// Fetch returns a blob's raw bytes, or rebuilds a raw git tree from the trees
// API when sha is not a blob. Commits and tags are not fetchable, so deltas
// against them stay unresolved and their hits keep failing closed.
func (f apiFetcher) Fetch(ctx context.Context, owner, repo, sha string) (string, []byte, error) {
	prefix := f.base + "/repos/" + owner + "/" + repo + "/git/"
	var blob struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	err := getJSON(ctx, prefix+"blobs/"+sha, f.pat, &blob)
	if err == nil {
		if blob.Encoding != "base64" {
			return "blob", []byte(blob.Content), nil
		}
		data, decErr := base64.StdEncoding.DecodeString(strings.ReplaceAll(blob.Content, "\n", ""))
		return "blob", data, decErr
	}
	if _, isStatus := errors.AsType[*httpStatusError](err); !isStatus {
		return "", nil, err
	}
	var tree struct {
		Tree []struct {
			Mode string `json:"mode"`
			Path string `json:"path"`
			SHA  string `json:"sha"`
		} `json:"tree"`
	}
	if err := getJSON(ctx, prefix+"trees/"+sha, f.pat, &tree); err != nil {
		return "", nil, err
	}
	var raw bytes.Buffer
	for _, e := range tree.Tree {
		id, err := hex.DecodeString(e.SHA)
		if err != nil || len(id) != 20 {
			return "", nil, fmt.Errorf("tree entry %q has invalid sha", e.Path)
		}
		raw.WriteString(strings.TrimPrefix(e.Mode, "0") + " " + e.Path + "\x00")
		raw.Write(id)
	}
	return "tree", raw.Bytes(), nil
}

// readScanBody buffers the request body; decoded strips gzip Content-Encoding.
func readScanBody(r *http.Request) (original, decoded []byte, err error) {
	original, err = io.ReadAll(io.LimitReader(r.Body, maxScanBodyBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if len(original) > maxScanBodyBytes {
		return nil, nil, errors.New("request body exceeds the secret scan limit")
	}
	if !strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		return original, original, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(original))
	if err != nil {
		return nil, nil, fmt.Errorf("decode gzip body: %w", err)
	}
	decoded, err = io.ReadAll(io.LimitReader(zr, maxScanBodyBytes+1))
	if closeErr := zr.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, nil, fmt.Errorf("decode gzip body: %w", err)
	}
	if len(decoded) > maxScanBodyBytes {
		return nil, nil, errors.New("request body exceeds the secret scan limit")
	}
	return original, decoded, nil
}

func scanPolicy(ctx context.Context, cfg *config.Config, owner, repo, pat string) guard.PushScanPolicy {
	var allow []string
	if cur := cfg.Current(); cur != nil {
		allow = cur.PushScan.PrefixesFor(owner, repo)
	}
	return guard.PushScanPolicy{
		PublicRepo:     repoVisibility.isPublic(ctx, upstreamBase(apiBaseEnv, defaultAPIBase), owner, repo, pat),
		RegexAllowlist: allow,
		BlockMainPush:  true,
		Owner:          owner,
		Repo:           repo,
	}
}

// gitRepoFromPath extracts owner/repo from "owner/repo(.git)/git-receive-pack".
func gitRepoFromPath(subpath string) (owner, repo string, ok bool) {
	parts := strings.Split(strings.Trim(subpath, "/"), "/")
	if len(parts) < 3 {
		return "", "", false
	}
	return parts[0], strings.TrimSuffix(parts[1], ".git"), parts[0] != "" && parts[1] != ""
}

// scanGitPush scans POST .../git-receive-pack bodies. It returns the body to
// forward (r.Body untouched for every other git request) or ok=false after
// writing the rejection.
func scanGitPush(w http.ResponseWriter, r *http.Request, cfg *config.Config, pat string) (io.Reader, bool) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, receivePackPath) {
		return r.Body, true
	}
	owner, repo, ok := gitRepoFromPath(strings.TrimPrefix(r.URL.Path, GitPrefix))
	if !ok {
		http.Error(w, "secret scan: cannot determine repository from push path", http.StatusBadRequest)
		return nil, false
	}
	original, decoded, err := readScanBody(r)
	if err != nil {
		http.Error(w, "secret scan: "+err.Error(), http.StatusRequestEntityTooLarge)
		return nil, false
	}
	req, err := guard.ParseReceivePackRequest(decoded)
	if err != nil {
		http.Error(w, "secret scan: malformed receive-pack request: "+err.Error(), http.StatusBadRequest)
		return nil, false
	}
	policy := scanPolicy(r.Context(), cfg, owner, repo, pat)
	fetcher := apiFetcher{base: upstreamBase(apiBaseEnv, defaultAPIBase), pat: pat}
	violations, err := guard.ScanReceivePackContext(r.Context(), bytes.NewReader(decoded), policy, fetcher)
	switch {
	case err != nil:
		writeReceivePackRejection(w, req, "secret scan failed: "+oneLine(err.Error()), nil)
		return nil, false
	case len(violations) > 0:
		writeReceivePackRejection(w, req, rejectionSummary(violations), violationLines(violations))
		return nil, false
	}
	return bytes.NewReader(original), true
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(sanitizer.SanitizeString(s)), " ")
}

func violationLabel(v guard.Violation) string {
	if v.LineNum > 0 {
		return fmt.Sprintf("%s:%d", v.File, v.LineNum)
	}
	return v.File
}

func rejectionSummary(violations []guard.Violation) string {
	first := violations[0]
	msg := fmt.Sprintf("secret scan blocked push: %s [%s]", violationLabel(first), first.Category)
	if n := len(violations) - 1; n > 0 {
		msg += fmt.Sprintf(" (+%d more)", n)
	}
	return oneLine(msg)
}

func violationLines(violations []guard.Violation) []string {
	lines := make([]string, 0, len(violations))
	for _, v := range violations {
		lines = append(lines, oneLine(fmt.Sprintf("%s [%s] sample %s", violationLabel(v), v.Category, v.Sample)))
	}
	return lines
}

func pktLine(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }

// writeReceivePackRejection answers git-receive-pack with "unpack ok" and an
// "ng" status for every ref, multiplexed over side-band when negotiated, so the
// git client prints the reason instead of failing the RPC.
func writeReceivePackRejection(w http.ResponseWriter, req *guard.ReceivePackRequest, reason string, details []string) {
	var status strings.Builder
	status.WriteString(pktLine("unpack ok\n"))
	for _, c := range req.Commands {
		status.WriteString(pktLine("ng " + c.Ref + " " + reason + "\n"))
	}
	status.WriteString("0000")

	var out strings.Builder
	maxData := 0
	switch {
	case req.HasCapability("side-band-64k"):
		maxData = sideBandMaxData
	case req.HasCapability("side-band"):
		maxData = sideBandSmallMax
	}
	if maxData == 0 {
		out.WriteString(status.String())
	} else {
		for _, d := range append([]string{reason}, details...) {
			writeSideBand(&out, 2, d+"\n", maxData)
		}
		writeSideBand(&out, 1, status.String(), maxData)
		out.WriteString("0000")
	}
	w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, out.String()); err != nil {
		log.Printf("githubproxy: write receive-pack rejection: %v", err)
	}
}

func writeSideBand(out *strings.Builder, band byte, data string, maxData int) {
	for data != "" {
		n := min(len(data), maxData)
		out.WriteString(pktLine(string([]byte{band}) + data[:n]))
		data = data[n:]
	}
}

func isWriteMethod(m string) bool {
	return m == http.MethodPost || m == http.MethodPut || m == http.MethodPatch
}

// apiPayloads returns the decoded file payloads of a contents or blobs write
// along with the repository path used for allowlisting ("" for blobs).
func apiPayloads(rest string, body []byte) (filePath string, payload []byte, ok bool) {
	var doc struct {
		Content  *string `json:"content"`
		Encoding string  `json:"encoding"`
	}
	isContents := strings.HasPrefix(rest, "contents/")
	if !isContents && rest != "git/blobs" {
		return "", nil, false
	}
	if json.Unmarshal(body, &doc) != nil || doc.Content == nil {
		return "", nil, false
	}
	if isContents {
		filePath = strings.TrimPrefix(rest, "contents/")
	}
	if !isContents && doc.Encoding != "base64" {
		return filePath, []byte(*doc.Content), true
	}
	data, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(*doc.Content, "\n", ""))
	if err != nil {
		return filePath, []byte(*doc.Content), true
	}
	return filePath, data, true
}

// scanAPIWrite scans POST/PUT/PATCH bodies: registered values everywhere,
// regex rules on public /repos/{owner}/{repo}/ writes. It returns the body to
// forward or ok=false after writing a 422.
func scanAPIWrite(w http.ResponseWriter, r *http.Request, cfg *config.Config, pat string) (io.Reader, bool) {
	if !isWriteMethod(r.Method) {
		return r.Body, true
	}
	original, decoded, err := readScanBody(r)
	if err != nil {
		writeAPIRejection(w, http.StatusRequestEntityTooLarge, "secret scan: "+err.Error(), nil)
		return nil, false
	}
	subpath := strings.Trim(strings.TrimPrefix(r.URL.Path, APIPrefix), "/")
	var violations []guard.Violation
	parts := strings.SplitN(subpath, "/", 4)
	if len(parts) >= 3 && parts[0] == "repos" && parts[1] != "" && parts[2] != "" {
		policy := scanPolicy(r.Context(), cfg, parts[1], parts[2], pat)
		rest := ""
		if len(parts) == 4 {
			rest = parts[3]
		}
		if filePath, payload, ok := apiPayloads(rest, decoded); ok {
			violations = append(violations, guard.ScanPayload(payload, filePath, policy)...)
		}
		violations = append(violations, guard.ScanPayload(decoded, "", policy)...)
	} else {
		violations = guard.ScanPayload(decoded, "", guard.PushScanPolicy{})
	}
	if len(violations) > 0 {
		writeAPIRejection(w, http.StatusUnprocessableEntity, apiRejectionSummary(violations), violations)
		return nil, false
	}
	return bytes.NewReader(original), true
}

func apiRejectionSummary(violations []guard.Violation) string {
	first := violations[0]
	msg := fmt.Sprintf("secret scan blocked request: %s [%s] sample %s", violationLabel(first), first.Category, first.Sample)
	if n := len(violations) - 1; n > 0 {
		msg += fmt.Sprintf(" (+%d more)", n)
	}
	return oneLine(msg)
}

func writeAPIRejection(w http.ResponseWriter, code int, message string, violations []guard.Violation) {
	httpkit.WriteJSON(w, code, struct {
		Message    string            `json:"message"`
		Violations []guard.Violation `json:"violations,omitempty"`
	}{Message: message, Violations: violations})
}
