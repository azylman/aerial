package app

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestParseGitHubCredential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		data        string
		readErr     error
		last        string
		want        string
		wantProblem string
	}{
		{name: "missing file selects app", readErr: fs.ErrNotExist, last: "pat", want: "app"},
		{name: "unreadable file keeps last", readErr: fs.ErrPermission, last: "pat", want: "pat", wantProblem: "read config: permission denied"},
		{name: "unset key selects app", data: "deploy_ui_url: x\n", last: "pat", want: "app"},
		{name: "empty section selects app", data: "vanadis_restate: {}\n", last: "pat", want: "app"},
		{name: "blank value selects app", data: "vanadis_restate:\n  github_credential: \" \"\n", last: "pat", want: "app"},
		{name: "app", data: "vanadis_restate:\n  github_credential: app\n", last: "pat", want: "app"},
		{name: "pat", data: "vanadis_restate:\n  github_credential: pat\n", last: "app", want: "pat"},
		{name: "value is case insensitive", data: "vanadis_restate:\n  github_credential: \" PAT \"\n", last: "app", want: "pat"},
		{name: "invalid value keeps last", data: "vanadis_restate:\n  github_credential: oauth\n", last: "pat", want: "pat", wantProblem: `github_credential "oauth" is not app or pat`},
		{name: "bad yaml keeps last", data: "vanadis_restate: [\n", last: "pat", want: "pat", wantProblem: "parse config: yaml: line 1: did not find expected node content"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, problem := parseGitHubCredential([]byte(tc.data), tc.readErr, tc.last)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantProblem, problem)
		})
	}
}

// staticConfigFile serves one fixed read result.
type staticConfigFile struct {
	data string
	err  error
}

func (f staticConfigFile) read(string) ([]byte, error) { return []byte(f.data), f.err }

func TestReadGitHubOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		file    staticConfigFile
		want    string
		wantErr string
	}{
		{name: "owner set", file: staticConfigFile{data: "vanadis_restate:\n  github_owner: acme\n"}, want: "acme"},
		{name: "owner trimmed", file: staticConfigFile{data: "vanadis_restate:\n  github_owner: \" acme/ \"\n"}, want: "acme"},
		{name: "owner beside credential", file: staticConfigFile{data: "vanadis_restate:\n  github_credential: pat\n  github_owner: agent-alpha\n"}, want: "agent-alpha"},
		{name: "owner unset", file: staticConfigFile{data: "vanadis_restate:\n  github_credential: app\n"}, wantErr: "vanadis_restate.github_owner is not set in /cfg/vanadis.yaml"},
		{name: "owner blank", file: staticConfigFile{data: "vanadis_restate:\n  github_owner: \" / \"\n"}, wantErr: "vanadis_restate.github_owner is not set in /cfg/vanadis.yaml"},
		{name: "missing file", file: staticConfigFile{err: fs.ErrNotExist}, wantErr: "read github_owner config /cfg/vanadis.yaml: file does not exist"},
		{name: "bad yaml", file: staticConfigFile{data: "vanadis_restate: [\n"}, wantErr: "parse github_owner config /cfg/vanadis.yaml: yaml: line 1: did not find expected node content"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ReadGitHubOwner("/cfg/vanadis.yaml", tc.file.read)
			require.Equal(t, tc.want, got)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

// fakeConfigFile serves a sequence of file reads; the last one repeats.
type fakeConfigFile struct {
	mu    sync.Mutex
	reads []string
	calls int
}

func (f *fakeConfigFile) read(string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data := f.reads[min(f.calls, len(f.reads)-1)]
	f.calls++
	if data == "<missing>" {
		return nil, fs.ErrNotExist
	}
	return []byte(data), nil
}

func TestCredentialSettingCurrent(t *testing.T) {
	t.Parallel()
	const patYAML = "vanadis_restate:\n  github_credential: pat\n"
	const appYAML = "vanadis_restate:\n  github_credential: app\n"
	const badYAML = "vanadis_restate:\n  github_credential: nope\n"
	tests := []struct {
		name      string
		reads     []string
		advance   time.Duration
		want      []string
		wantCalls int
	}{
		{name: "missing file defaults to app", reads: []string{"<missing>"}, advance: time.Second, want: []string{"app", "app"}, wantCalls: 1},
		{name: "cached within the reload interval", reads: []string{patYAML, appYAML}, advance: 29 * time.Second, want: []string{"pat", "pat"}, wantCalls: 1},
		{name: "hot reload after the interval", reads: []string{patYAML, appYAML}, advance: 30 * time.Second, want: []string{"pat", "app"}, wantCalls: 2},
		{name: "invalid reload keeps last good", reads: []string{patYAML, badYAML}, advance: time.Minute, want: []string{"pat", "pat"}, wantCalls: 2},
		{name: "invalid first read keeps the app default", reads: []string{badYAML}, advance: time.Minute, want: []string{"app", "app"}, wantCalls: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			file := &fakeConfigFile{reads: tc.reads}
			now := time.Date(2026, 11, 21, 20, 0, 0, 0, time.UTC)
			setting := newCredentialSetting("/cfg/vanadis.yaml", file.read, func() time.Time { return now }, slog.New(slog.DiscardHandler))
			first := setting.Current()
			now = now.Add(tc.advance)
			got := []string{first, setting.Current()}
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantCalls, file.calls)
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newTestAppKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "RSA " + "PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
}

func TestGitHubHTTPClientCredential(t *testing.T) {
	t.Parallel()
	keyPEM := newTestAppKeyPEM(t)
	tests := []struct {
		name       string
		appEnv     bool
		pat        string
		mintStatus int
		choice     string
		target     string
		wantAuth   string
		wantMints  int
	}{
		{name: "app env unset with app setting uses the pat", pat: "pat-1", choice: "app", target: "https://api.github.com/repos/o/r", wantAuth: "Bearer pat-1"},
		{name: "app env unset with pat setting uses the pat", pat: "pat-1", choice: "pat", target: "https://api.github.com/repos/o/r", wantAuth: "Bearer pat-1"},
		{name: "app env unset without a pat is unauthenticated", choice: "app", target: "https://api.github.com/repos/o/r"},
		{name: "app setting uses the minted installation token", appEnv: true, pat: "pat-1", mintStatus: 201, choice: "app", target: "https://api.github.com/repos/o/r", wantAuth: "Bearer ghs_minted", wantMints: 1},
		{name: "pat setting skips the app", appEnv: true, pat: "pat-1", mintStatus: 201, choice: "pat", target: "https://api.github.com/repos/o/r", wantAuth: "Bearer pat-1"},
		{name: "failed mint falls back to the pat", appEnv: true, pat: "pat-1", mintStatus: 500, choice: "app", target: "https://api.github.com/repos/o/r", wantAuth: "Bearer pat-1", wantMints: 1},
		{name: "other hosts never carry a credential", appEnv: true, pat: "pat-1", mintStatus: 201, choice: "app", target: "https://blob.example.com/log"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			mints := 0
			mintClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				mu.Lock()
				mints++
				mu.Unlock()
				body := `{"token":"ghs_minted","expires_at":"2099-01-01T00:00:00Z"}`
				if tc.mintStatus != http.StatusCreated {
					body = `{"message":"boom"}`
				}
				return &http.Response{StatusCode: tc.mintStatus, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}, Request: r}, nil
			})}
			vals := gitHubAuthValues{PAT: tc.pat}
			if tc.appEnv {
				vals.AppID, vals.InstallationID, vals.PrivateKeyPEM = "12345", "678", keyPEM
			}
			choice := tc.choice
			src := newGitHubTokenSource(vals, func() string { return choice }, mintClient, slog.New(slog.DiscardHandler))
			var gotAuth string
			base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotAuth = r.Header.Get("Authorization")
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}")), Header: http.Header{}, Request: r}, nil
			})
			client := newGitHubHTTPClient(src, base, 5*time.Second)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, tc.target, http.NoBody)
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, tc.wantAuth, gotAuth)
			require.Equal(t, tc.wantMints, mints)
		})
	}
}

func TestPromGitHubMetrics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		source string
	}{
		{name: "app source", source: "app"},
		{name: "pat source", source: "pat"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := promGitHubMetrics{}
			before := metricValue(t, githubRequestsBySource.WithLabelValues(tc.source))
			m.IncRequest(tc.source)
			m.SetRateLimitRemaining(tc.source, 42)
			m.IncTokenMint("success")
			require.GreaterOrEqual(t, metricValue(t, githubRequestsBySource.WithLabelValues(tc.source)), before+1)
			require.Equal(t, 42.0, metricValue(t, githubRateLimitRemaining.WithLabelValues(tc.source)))
			require.GreaterOrEqual(t, metricValue(t, githubTokenMints.WithLabelValues("success")), 1.0)
		})
	}
}

// metricValue reads one counter or gauge child's current value.
func metricValue(t *testing.T, c prometheus.Metric) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}
