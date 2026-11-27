package githubproxy

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

type upstreamEcho struct {
	Auth       string `json:"auth"`
	Path       string `json:"path"`
	Query      string `json:"query"`
	APIVersion string `json:"api_version"`
	Method     string `json:"method"`
	Body       string `json:"body"`
}

func newEchoUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/"); r.Method == http.MethodGet && len(parts) == 3 && parts[0] == "repos" {
			if _, err := fmt.Fprintf(w, `{"private":%t}`, strings.HasPrefix(parts[2], "private")); err != nil {
				return
			}
			return
		}
		w.Header().Set("X-Upstream", "echo")
		_ = json.NewEncoder(w).Encode(upstreamEcho{
			Auth:       r.Header.Get("Authorization"),
			Path:       r.URL.Path,
			Query:      r.URL.RawQuery,
			APIVersion: r.Header.Get("X-Github-Api-Version"),
			Method:     r.Method,
			Body:       string(body),
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func cfgWithPAT(pat string) *config.Config {
	return config.NewFromData(&config.ConfigData{
		GitHubPAT: pat,
		PushScan: config.PushScanConfig{
			SecretAllowlist: []string{
				"acme/private-repo:hooks:GITHUB_WEBHOOK_SECRET",
				"org/*:hooks:GITHUB_WEBHOOK_SECRET",
			},
		},
	})
}

func TestRegister(t *testing.T) {
	upstream := newEchoUpstream(t)
	t.Setenv("GITHUB_API_URL", upstream.URL)
	t.Setenv("AERIAL_GITHUB_GIT_UPSTREAM", upstream.URL+"/")
	t.Setenv("AERIAL_GHCR_TOKEN_UPSTREAM", upstream.URL+"/token")

	const pat = "dummy-proxy-pat"
	// base64("x-access-token:dummy-proxy-pat"), derived by hand.
	const gitBasic = "Basic eC1hY2Nlc3MtdG9rZW46ZHVtbXktcHJveHktcGF0"

	tests := []struct {
		name       string
		cfg        *config.Config
		method     string
		target     string
		remoteAddr string
		headers    map[string]string
		body       string
		wantStatus int
		wantSubstr string
		wantEcho   *upstreamEcho
	}{
		{
			name: "api injects token auth and passes query", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-api/repos/org/repo/pulls?state=open&per_page=5", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: "token dummy-proxy-pat", Path: "/repos/org/repo/pulls", Query: "state=open&per_page=5", Method: http.MethodGet},
		},
		{
			name: "api strips client authorization and forwards other headers", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-api/user", remoteAddr: "[::1]:5000",
			headers:    map[string]string{"Authorization": "token client-supplied", "X-Github-Api-Version": "2022-11-28"},
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: "token dummy-proxy-pat", Path: "/user", APIVersion: "2022-11-28", Method: http.MethodGet},
		},
		{
			name: "git injects basic auth and forwards body", cfg: cfgWithPAT(pat), method: http.MethodPost,
			target: "/internal/github-git/org/repo.git/git-upload-pack", remoteAddr: "127.0.0.1:5000",
			headers:    map[string]string{"Authorization": "Basic Y2xpZW50OmNsaWVudA=="},
			body:       "0000PACK",
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: gitBasic, Path: "/org/repo.git/git-upload-pack", Method: http.MethodPost, Body: "0000PACK"},
		},
		{
			name: "git passes service query", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-git/org/repo.git/info/refs?service=git-receive-pack", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: gitBasic, Path: "/org/repo.git/info/refs", Query: "service=git-receive-pack", Method: http.MethodGet},
		},
		{
			name: "ghcr token forwards read-only pull scope", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-ghcr-token?scope=repository:azylman/aerial-sidecar-vision:pull&service=ghcr.io/token", remoteAddr: "127.0.0.1:5000",
			headers:    map[string]string{"Authorization": "token client-token", "X-Custom": "val"},
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: gitBasic, Path: "/token", Query: "scope=repository%3Aazylman%2Faerial-sidecar-vision%3Apull&service=ghcr.io%2Ftoken", Method: http.MethodGet},
		},
		{
			name: "ghcr token supports ghcr.io service", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-ghcr-token?scope=repository:azylman/aerial:pull&service=ghcr.io", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: gitBasic, Path: "/token", Query: "scope=repository%3Aazylman%2Faerial%3Apull&service=ghcr.io", Method: http.MethodGet},
		},
		{
			name: "ghcr token missing scope rejected", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-ghcr-token?service=ghcr.io/token", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusForbidden, wantSubstr: "Scope is required",
		},
		{
			name: "ghcr token wrong subpath rejected", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-ghcr-token/extra?scope=repository:azylman/aerial:pull&service=ghcr.io/token", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusForbidden, wantSubstr: "Forbidden path",
		},
		{
			name: "ghcr token non-pull scope rejected", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-ghcr-token?scope=repository:azylman/aerial:push&service=ghcr.io/token", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusForbidden, wantSubstr: "Unauthorized scope",
		},
		{
			name: "ghcr token invalid service rejected", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-ghcr-token?scope=repository:azylman/aerial:pull&service=docker.io", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusForbidden, wantSubstr: "Invalid or unauthorized service",
		},
		{
			name: "ghcr token method not allowed for post", cfg: cfgWithPAT(pat), method: http.MethodPost,
			target: "/internal/github-ghcr-token?scope=repository:azylman/aerial:pull&service=ghcr.io/token", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusMethodNotAllowed, wantSubstr: "Method not allowed",
		},
		{
			name: "ghcr token missing PAT returns 503", cfg: cfgWithPAT(""), method: http.MethodGet,
			target: "/internal/github-ghcr-token?scope=repository:azylman/aerial:pull&service=ghcr.io/token", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusServiceUnavailable, wantSubstr: "GitHub PAT not configured",
		},
		{
			name: "ghcr token non-loopback caller gets 403", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-ghcr-token?scope=repository:azylman/aerial:pull&service=ghcr.io/token", remoteAddr: "192.0.2.10:5000",
			wantStatus: http.StatusForbidden, wantSubstr: "forbidden",
		},
		{
			name: "api missing PAT returns 503", cfg: cfgWithPAT(""), method: http.MethodGet,
			target: "/internal/github-api/user", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusServiceUnavailable, wantSubstr: "GitHub PAT not configured",
		},
		{
			name: "git nil config returns 503", cfg: nil, method: http.MethodGet,
			target: "/internal/github-git/org/repo.git/info/refs", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusServiceUnavailable, wantSubstr: "GitHub PAT not configured",
		},
		{
			name: "git config without snapshot returns 503", cfg: &config.Config{}, method: http.MethodGet,
			target: "/internal/github-git/org/repo.git/info/refs", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusServiceUnavailable, wantSubstr: "GitHub PAT not configured",
		},
		{
			name: "api non-loopback caller gets 403", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-api/user", remoteAddr: "192.0.2.10:5000",
			wantStatus: http.StatusForbidden, wantSubstr: "forbidden",
		},
		{
			name: "git non-loopback caller gets 403", cfg: cfgWithPAT(pat), method: http.MethodPost,
			target: "/internal/github-git/org/repo.git/git-receive-pack", remoteAddr: "192.0.2.10:5000",
			wantStatus: http.StatusForbidden, wantSubstr: "forbidden",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			Register(mux, tc.cfg)
			checkProxyResponse(t, mux, tc.method, tc.target, tc.remoteAddr, tc.body, tc.headers, tc.wantStatus, tc.wantSubstr, tc.wantEcho)
		})
	}
}

func checkProxyResponse(t *testing.T, handler http.Handler, method, target, remoteAddr, body string, headers map[string]string, wantStatus int, wantSubstr string, wantEcho *upstreamEcho) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.RemoteAddr = remoteAddr
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, wantStatus, rec.Body.String())
	}
	if wantSubstr != "" && !strings.Contains(rec.Body.String(), wantSubstr) {
		t.Fatalf("body %q missing %q", rec.Body.String(), wantSubstr)
	}
	if wantEcho == nil {
		return
	}
	if got := rec.Header().Get("X-Upstream"); got != "echo" {
		t.Errorf("X-Upstream header = %q, want echo", got)
	}
	var got upstreamEcho
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode upstream echo: %v (body: %s)", err, rec.Body.String())
	}
	if got != *wantEcho {
		t.Errorf("upstream saw %+v, want %+v", got, *wantEcho)
	}
}

func TestAPIHandler(t *testing.T) {
	runProxyHandlerErrors(t, "GITHUB_API_URL", "/internal/github-api/user", APIHandler)
}

func TestGHCRTokenHandler(t *testing.T) {
	runProxyHandlerErrors(t, "AERIAL_GHCR_TOKEN_UPSTREAM", "/internal/github-ghcr-token?scope=repository:azylman/aerial:pull&service=ghcr.io/token", GHCRTokenHandler)
}

func runProxyHandlerErrors(t *testing.T, envVar, reqPath string, getHandler func(cfg *config.Config) http.HandlerFunc) {
	t.Helper()
	tests := []struct {
		name       string
		upstream   string
		wantStatus int
		wantSubstr string
	}{
		{name: "unreachable upstream returns 502", upstream: "http://127.0.0.1:0", wantStatus: http.StatusBadGateway, wantSubstr: "Upstream proxy error"},
		{name: "unparseable upstream returns 500", upstream: "http://bad host", wantStatus: http.StatusInternalServerError, wantSubstr: "Failed to create proxy request"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envVar, tc.upstream)
			req := httptest.NewRequest(http.MethodGet, reqPath, http.NoBody)
			rec := httptest.NewRecorder()
			getHandler(cfgWithPAT("dummy-proxy-pat"))(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

type nonFlusherWriter struct {
	header http.Header
	code   int
	buf    bytes.Buffer
}

func (n *nonFlusherWriter) Header() http.Header {
	if n.header == nil {
		n.header = make(http.Header)
	}
	return n.header
}
func (n *nonFlusherWriter) Write(b []byte) (int, error) { return n.buf.Write(b) }
func (n *nonFlusherWriter) WriteHeader(code int)        { n.code = code }

type failingWriter struct{ nonFlusherWriter }

func (*failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (*failingWriter) Flush()                    {}

func TestStreamResponse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		w          http.ResponseWriter
		status     int
		body       string
		wantBody   string
		wantCustom string
	}{
		{name: "flusher copies body and drops hop headers", w: httptest.NewRecorder(), status: http.StatusCreated, body: "hello proxy", wantBody: "hello proxy", wantCustom: "val"},
		{name: "non-flusher copies body", w: &nonFlusherWriter{}, status: http.StatusOK, body: "plain copy", wantBody: "plain copy", wantCustom: "val"},
		{name: "write failure stops copy", w: &failingWriter{}, status: http.StatusOK, body: "lost", wantBody: "", wantCustom: "val"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := &http.Response{
				StatusCode: tc.status,
				Header:     http.Header{"X-Custom": []string{"val"}, "Connection": []string{"close"}, "Transfer-Encoding": []string{"chunked"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			StreamResponse(tc.w, resp)

			var gotCode int
			var gotBody string
			switch w := tc.w.(type) {
			case *httptest.ResponseRecorder:
				gotCode, gotBody = w.Code, w.Body.String()
			case *failingWriter:
				gotCode, gotBody = w.code, w.buf.String()
			case *nonFlusherWriter:
				gotCode, gotBody = w.code, w.buf.String()
			}
			if gotCode != tc.status || gotBody != tc.wantBody {
				t.Fatalf("got code=%d body=%q, want code=%d body=%q", gotCode, gotBody, tc.status, tc.wantBody)
			}
			h := tc.w.Header()
			if h.Get("X-Custom") != tc.wantCustom || h.Get("Connection") != "" || h.Get("Transfer-Encoding") != "" {
				t.Fatalf("unexpected headers: %v", h)
			}
		})
	}
}

const proxySecret = "dummy-proxy-secret-value"

func pktFixture(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }

// receivePackBody builds a one-ref push whose pack holds one object per blob.
func receivePackBody(caps string, blobs ...string) []byte {
	return receivePackRefBody("refs/heads/feat/test", caps, blobs...)
}

func receivePackRefBody(ref, caps string, blobs ...string) []byte {
	var buf bytes.Buffer
	buf.WriteString(pktFixture(strings.Repeat("0", 40) + " " + strings.Repeat("a", 40) + " " + ref + "\x00" + caps + "\n"))
	buf.WriteString("0000PACK")
	buf.Write([]byte{0, 0, 0, 2, 0, 0, 0, byte(len(blobs))})
	for _, blob := range blobs {
		size := len(blob)
		c := byte(3<<4) | byte(size&0x0f)
		for size >>= 4; size > 0; size >>= 7 {
			buf.WriteByte(c | 0x80)
			c = byte(size & 0x7f)
		}
		buf.WriteByte(c)
		zw := zlib.NewWriter(&buf)
		if _, err := zw.Write([]byte(blob)); err != nil {
			panic(err)
		}
		if err := zw.Close(); err != nil {
			panic(err)
		}
	}
	buf.Write(make([]byte, 20))
	return buf.Bytes()
}

func jsonLossy(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	var out string
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

func gzipped(b []byte) string {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(b)
	_ = zw.Close()
	return buf.String()
}

type scanWriteTestCase struct {
	name        string
	method      string
	target      string
	encoding    string
	body        string
	wantStatus  int
	wantEcho    bool
	wantBody    string
	wantSubstrs []string
}

func runScanWriteCase(t *testing.T, tc scanWriteTestCase) {
	t.Helper()
	mux := http.NewServeMux()
	Register(mux, cfgWithPAT("dummy-proxy-pat"))
	req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
	req.RemoteAddr = "127.0.0.1:5000"
	if tc.encoding != "" {
		req.Header.Set("Content-Encoding", tc.encoding)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d (body: %q)", rec.Code, tc.wantStatus, rec.Body.String())
	}
	if got := rec.Header().Get("X-Upstream") == "echo"; got != tc.wantEcho {
		t.Fatalf("forwarded = %v, want %v (body: %q)", got, tc.wantEcho, rec.Body.String())
	}
	if tc.wantEcho {
		var echo upstreamEcho
		// The echo upstream JSON-encodes the body, which maps invalid UTF-8
		// bytes to U+FFFD; apply the same mapping to the expectation.
		if err := json.Unmarshal(rec.Body.Bytes(), &echo); err != nil || echo.Body != jsonLossy(tc.wantBody) {
			t.Fatalf("upstream body = %q (err %v), want %q", echo.Body, err, tc.wantBody)
		}
	}
	for _, s := range tc.wantSubstrs {
		if !strings.Contains(rec.Body.String(), s) {
			t.Errorf("body %q missing %q", rec.Body.String(), s)
		}
	}
	if strings.Contains(rec.Body.String(), proxySecret) && !tc.wantEcho {
		t.Errorf("rejection leaked the raw secret: %q", rec.Body.String())
	}
}

func TestScanWrites(t *testing.T) {
	sanitizer.ResetSensitiveTokensForTest()
	sameMaskSecret := "du-different-token-secret-value"
	shortSecret := "shrt"
	nestedPass := "dbpass1234"
	connURL := "postgres://user:dbpass1234@localhost:5432/db"
	sanitizer.RegisterSensitiveTokens(proxySecret, sameMaskSecret, shortSecret, connURL, nestedPass)
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)

	origSharedSecrets := config.SharedSecretsPath
	tmpSecrets := t.TempDir()
	config.SharedSecretsPath = tmpSecrets
	t.Cleanup(func() {
		config.SharedSecretsPath = origSharedSecrets
	})
	if err := os.WriteFile(filepath.Join(tmpSecrets, "GITHUB_WEBHOOK_SECRET"), []byte(proxySecret), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}

	upstream := newEchoUpstream(t)
	t.Setenv("GITHUB_API_URL", upstream.URL)
	t.Setenv("AERIAL_GITHUB_GIT_UPSTREAM", upstream.URL)

	cleanPush := string(receivePackBody("report-status side-band-64k", "hello\n"))
	secretBlob := "key: " + proxySecret + "\n"
	contentsBody := `{"message":"add","content":"` + base64.StdEncoding.EncodeToString([]byte(secretBlob)) + `"}`

	tests := []scanWriteTestCase{
		{
			name: "clean push forwarded with original bytes", method: http.MethodPost, target: "/internal/github-git/acme/private-repo.git/git-receive-pack",
			body: cleanPush, wantStatus: http.StatusOK, wantEcho: true, wantBody: cleanPush,
		},
		{
			name: "direct push to refs/heads/main rejected", method: http.MethodPost, target: "/internal/github-git/acme/private-repo.git/git-receive-pack",
			body: string(receivePackRefBody("refs/heads/main", "report-status side-band-64k", "hello\n")), wantStatus: http.StatusOK,
			wantSubstrs: []string{"ng refs/heads/main secret scan blocked push: refs/heads/main [DirectMainPush]", "direct push to refs/heads/main is prohibited"},
		},
		{
			name: "secret push rejected over side-band", method: http.MethodPost, target: "/internal/github-git/acme/private-repo.git/git-receive-pack",
			body: string(receivePackBody("report-status side-band-64k", secretBlob)), wantStatus: http.StatusOK,
			wantSubstrs: []string{"\x02secret scan blocked push: blob ", "[InfisicalSecret]", "sample du***ue", "\x01000eunpack ok\n", "ng refs/heads/feat/test secret scan blocked push"},
		},
		{
			name: "secret push rejected over small side-band", method: http.MethodPost, target: "/internal/github-git/acme/repo/git-receive-pack",
			body: string(receivePackBody("report-status side-band", secretBlob)), wantStatus: http.StatusOK,
			wantSubstrs: []string{"\x02secret scan blocked push", "\x01000eunpack ok\n"},
		},
		{
			name: "secret push rejected without side-band", method: http.MethodPost, target: "/internal/github-git/acme/private-repo.git/git-receive-pack",
			body: string(receivePackBody("report-status", secretBlob)), wantStatus: http.StatusOK,
			wantSubstrs: []string{"000eunpack ok\n", "ng refs/heads/feat/test secret scan blocked push: blob "},
		},
		{
			name: "gzip secret push decoded and rejected", method: http.MethodPost, target: "/internal/github-git/acme/private-repo.git/git-receive-pack",
			encoding: "gzip", body: gzipped(receivePackBody("report-status", secretBlob)), wantStatus: http.StatusOK,
			wantSubstrs: []string{"ng refs/heads/feat/test secret scan blocked push"},
		},
		{
			name: "corrupt pack fails closed", method: http.MethodPost, target: "/internal/github-git/acme/private-repo.git/git-receive-pack",
			body: pktFixture(strings.Repeat("0", 40)+" "+strings.Repeat("a", 40)+" refs/heads/feat/test\x00report-status\n") + "0000PACKjunk", wantStatus: http.StatusOK,
			wantSubstrs: []string{"ng refs/heads/feat/test secret scan failed: missing PACK header"},
		},
		{
			name: "malformed receive-pack is 400", method: http.MethodPost, target: "/internal/github-git/acme/repo.git/git-receive-pack",
			body: "zz", wantStatus: http.StatusBadRequest, wantSubstrs: []string{"malformed receive-pack request"},
		},
		{
			name: "push path without repo is 400", method: http.MethodPost, target: "/internal/github-git/git-receive-pack",
			body: cleanPush, wantStatus: http.StatusBadRequest, wantSubstrs: []string{"cannot determine repository"},
		},
		{
			name: "bad gzip is rejected", method: http.MethodPost, target: "/internal/github-git/acme/repo.git/git-receive-pack",
			encoding: "gzip", body: "not gzip", wantStatus: http.StatusRequestEntityTooLarge, wantSubstrs: []string{"decode gzip body"},
		},
		{
			name: "issue comment secret rejected", method: http.MethodPost, target: "/internal/github-api/repos/acme/private-repo/issues/1/comments",
			body: `{"body":"` + proxySecret + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{`"category":"InfisicalSecret"`, "du***ue"},
		},
		{
			name: "contents put decodes base64", method: http.MethodPut, target: "/internal/github-api/repos/acme/private-repo/contents/docs/key.md",
			body: contentsBody, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"docs/key.md:1 [InfisicalSecret]"},
		},
		{
			name: "utf-8 blob create rejected", method: http.MethodPost, target: "/internal/github-api/repos/acme/repo/git/blobs",
			body: `{"content":"` + proxySecret + `","encoding":"utf-8"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
		{
			name: "graphql gets value match", method: http.MethodPost, target: "/internal/github-api/graphql",
			body: `{"query":"` + proxySecret + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"request body:1 [InfisicalSecret]"},
		},
		{
			name: "gzip api body decoded and rejected", method: http.MethodPatch, target: "/internal/github-api/repos/acme/repo/pulls/2",
			encoding: "gzip", body: gzipped([]byte(`{"body":"` + proxySecret + `"}`)), wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
		{
			name: "bad gzip api body rejected", method: http.MethodPatch, target: "/internal/github-api/repos/acme/repo/pulls/2",
			encoding: "gzip", body: "nope", wantStatus: http.StatusRequestEntityTooLarge, wantSubstrs: []string{"decode gzip body"},
		},
		{
			name: "clean api write forwarded", method: http.MethodPatch, target: "/internal/github-api/repos/acme/repo/pulls/2",
			body: `{"title":"hi"}`, wantStatus: http.StatusOK, wantEcho: true, wantBody: `{"title":"hi"}`,
		},
		{
			name: "webhook config patch with allowlisted secret forwarded", method: http.MethodPatch, target: "/internal/github-api/repos/acme/private-repo/hooks/123/config",
			body: `{"secret":"` + proxySecret + `"}`, wantStatus: http.StatusOK, wantEcho: true, wantBody: `{"secret":"` + proxySecret + `"}`,
		},
		{
			name: "secret with same mask as allowlisted secret is rejected", method: http.MethodPatch, target: "/internal/github-api/repos/acme/private-repo/hooks/123/config",
			body: `{"secret":"` + sameMaskSecret + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
		{
			name: "same mask secret alongside allowlisted secret is rejected", method: http.MethodPatch, target: "/internal/github-api/repos/acme/private-repo/hooks/123/config",
			body: `{"secret":"` + proxySecret + `","other":"` + sameMaskSecret + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
		{
			name: "unrelated short secret on allowlisted route is rejected", method: http.MethodPatch, target: "/internal/github-api/repos/acme/private-repo/hooks/123/config",
			body: `{"secret":"` + shortSecret + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
		{
			name: "nested secret on allowlisted route is rejected", method: http.MethodPatch, target: "/internal/github-api/repos/acme/private-repo/hooks/123/config",
			body: `{"password":"` + nestedPass + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
		{
			name: "wildcard pattern webhook config patch forwarded", method: http.MethodPatch, target: "/internal/github-api/repos/org/repo/hooks/123/config",
			body: `{"secret":"` + proxySecret + `"}`, wantStatus: http.StatusOK, wantEcho: true, wantBody: `{"secret":"` + proxySecret + `"}`,
		},
		{
			name: "repo with hash rejected from secret exemption", method: http.MethodPatch, target: "/internal/github-api/repos/org/repo%23/hooks",
			body: `{"description":"` + proxySecret + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
		{
			name: "repo with question mark rejected from secret exemption", method: http.MethodPatch, target: "/internal/github-api/repos/org/repo%3F/hooks",
			body: `{"description":"` + proxySecret + `"}`, wantStatus: http.StatusUnprocessableEntity, wantSubstrs: []string{"InfisicalSecret"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runScanWriteCase(t, tc)
		})
	}
}

func TestAllowedSecretValues(t *testing.T) {
	origShared := config.SharedSecretsPath
	tmp := t.TempDir()
	config.SharedSecretsPath = tmp
	t.Cleanup(func() { config.SharedSecretsPath = origShared })

	if err := os.WriteFile(filepath.Join(tmp, "CASE_INSENSITIVE_KEY"), []byte("case-secret-value-9999"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	cfg := config.NewFromData(&config.ConfigData{
		PushScan: config.PushScanConfig{
			SecretAllowlist: []string{
				"acme/widgets:hooks:case_insensitive_key",
				"acme/widgets:hooks:MISSING_KEY",
			},
		},
	})

	got := allowedSecretValues(cfg, "acme", "widgets", "hooks")
	want := []string{"case-secret-value-9999"}
	if !slices.Equal(got, want) {
		t.Fatalf("allowedSecretValues() = %v, want %v", got, want)
	}
}

func TestAPIFetcher(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/acme/widgets/git/blobs/b64":
			_, _ = io.WriteString(w, `{"content":"aGVs\nbG8K","encoding":"base64"}`)
		case "/repos/acme/widgets/git/blobs/utf8":
			_, _ = io.WriteString(w, `{"content":"plain","encoding":"utf-8"}`)
		case "/repos/acme/widgets/git/trees/t1":
			_, _ = io.WriteString(w, `{"tree":[{"mode":"100644","path":"a.txt","sha":"1111111111111111111111111111111111111111"},{"mode":"040000","path":"d","sha":"2222222222222222222222222222222222222222"}]}`)
		case "/repos/acme/widgets/git/trees/bad":
			_, _ = io.WriteString(w, `{"tree":[{"mode":"100644","path":"a.txt","sha":"zz"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	tests := []struct {
		name     string
		base     string
		sha      string
		wantType string
		wantData string
		wantErr  string
	}{
		{name: "base64 blob", base: srv.URL, sha: "b64", wantType: "blob", wantData: "hello\n"},
		{name: "utf-8 blob", base: srv.URL, sha: "utf8", wantType: "blob", wantData: "plain"},
		{
			name: "tree rebuilt from json", base: srv.URL, sha: "t1", wantType: "tree",
			wantData: "100644 a.txt\x00" + strings.Repeat("\x11", 20) + "40000 d\x00" + strings.Repeat("\x22", 20),
		},
		{name: "invalid tree sha", base: srv.URL, sha: "bad", wantErr: "invalid sha"},
		{name: "unknown object", base: srv.URL, sha: "missing", wantErr: "GitHub API returned 404"},
		{name: "transport error", base: "http://127.0.0.1:0", sha: "b64", wantErr: "connect"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			typ, data, err := apiFetcher{base: tc.base, pat: "p"}.Fetch(t.Context(), "acme", "widgets", tc.sha)
			if (err == nil) != (tc.wantErr == "") || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if typ != tc.wantType || string(data) != tc.wantData {
				t.Fatalf("got %q %q, want %q %q", typ, data, tc.wantType, tc.wantData)
			}
		})
	}
}

func TestVisibilityCacheIsPublic(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/acme/open":
			_, _ = io.WriteString(w, `{"private":false}`)
		case "/repos/acme/closed":
			_, _ = io.WriteString(w, `{"private":true}`)
		case "/repos/acme/nofield":
			_, _ = io.WriteString(w, `{}`)
		default:
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)

	tests := []struct {
		name string
		repo string
		want bool
	}{
		{name: "public repo", repo: "open", want: true},
		{name: "private repo", repo: "closed", want: false},
		{name: "private repo served from cache", repo: "closed", want: false},
		{name: "missing field treated as public", repo: "nofield", want: true},
		{name: "lookup error treated as public", repo: "broken", want: true},
	}
	cache := &visibilityCache{entries: map[string]visibilityEntry{}}
	for _, tc := range tests {
		if got := cache.isPublic(t.Context(), srv.URL, "acme", tc.repo, "p"); got != tc.want {
			t.Errorf("%s: isPublic = %v, want %v", tc.name, got, tc.want)
		}
	}
}
