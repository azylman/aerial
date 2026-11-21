package githubproxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
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
	return config.NewFromData(&config.ConfigData{GitHubPAT: pat})
}

func TestRegister(t *testing.T) {
	upstream := newEchoUpstream(t)
	t.Setenv("GITHUB_API_URL", upstream.URL)
	t.Setenv("AERIAL_GITHUB_GIT_UPSTREAM", upstream.URL+"/")

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
			target: "/internal/github-git/org/repo.git/git-receive-pack", remoteAddr: "127.0.0.1:5000",
			headers:    map[string]string{"Authorization": "Basic Y2xpZW50OmNsaWVudA=="},
			body:       "0000PACK",
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: gitBasic, Path: "/org/repo.git/git-receive-pack", Method: http.MethodPost, Body: "0000PACK"},
		},
		{
			name: "git passes service query", cfg: cfgWithPAT(pat), method: http.MethodGet,
			target: "/internal/github-git/org/repo.git/info/refs?service=git-receive-pack", remoteAddr: "127.0.0.1:5000",
			wantStatus: http.StatusOK,
			wantEcho:   &upstreamEcho{Auth: gitBasic, Path: "/org/repo.git/info/refs", Query: "service=git-receive-pack", Method: http.MethodGet},
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

			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			req.RemoteAddr = tc.remoteAddr
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
			if tc.wantEcho == nil {
				return
			}
			if got := rec.Header().Get("X-Upstream"); got != "echo" {
				t.Errorf("X-Upstream header = %q, want echo", got)
			}
			var got upstreamEcho
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode upstream echo: %v (body: %s)", err, rec.Body.String())
			}
			if got != *tc.wantEcho {
				t.Errorf("upstream saw %+v, want %+v", got, *tc.wantEcho)
			}
		})
	}
}

func TestAPIHandler(t *testing.T) {
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
			t.Setenv("GITHUB_API_URL", tc.upstream)
			req := httptest.NewRequest(http.MethodGet, "/internal/github-api/user", http.NoBody)
			rec := httptest.NewRecorder()
			APIHandler(cfgWithPAT("dummy-proxy-pat"))(rec, req)
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
