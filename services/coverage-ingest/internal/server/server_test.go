package server

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/services/coverage-ingest/internal/config"
)

func TestMain(m *testing.M) {
	// Run() opens a pool from the libpq PG* environment and the pull loop prunes through
	// it, so point it at a socket directory that cannot exist: tests never reach a real
	// database even when the shell exports one.
	for _, k := range []string{"PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGPORT"} {
		_ = os.Unsetenv(k)
	}
	_ = os.Setenv("PGHOST", "/nonexistent/coverage-ingest-test")
	_ = os.Setenv("PGPASSFILE", "/nonexistent/coverage-ingest-test/pgpass")
	_ = os.Setenv("PGCONNECT_TIMEOUT", "1")
	goleak.VerifyTestMain(m)
}

type tarEntry struct {
	name     string
	typeflag byte
	body     string
	size     int64
}

func buildTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		size := e.size
		if size == 0 {
			size = int64(len(e.body))
		}
		typ := e.typeflag
		if typ == 0 {
			typ = tar.TypeReg
		}
		if err := tw.WriteHeader(&tar.Header{Name: e.name, Typeflag: typ, Mode: 0o600, Size: size, Linkname: "x"}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if e.size == 0 && e.body != "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("write body: %v", err)
			}
		}
	}
	_ = tw.Close()
	return buf.Bytes()
}

func reg(name, body string) tarEntry { return tarEntry{name: name, body: body} }

type fakeRunner struct {
	out []byte
	err error

	mu        sync.Mutex
	dirCounts []int
	fileSets  [][]string
}

func (r *fakeRunner) Func(_ context.Context, dirs []string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dirCounts = append(r.dirCounts, len(dirs))
	var files []string
	for _, d := range dirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			files = append(files, e.Name())
		}
	}
	slices.Sort(files)
	r.fileSets = append(r.fileSets, files)
	return r.out, r.err
}

type fakeStore struct {
	errs     map[string]error
	pruneErr error

	mu      sync.Mutex
	upserts map[string][]config.CoverageRow
}

func (s *fakeStore) UpsertService(_ context.Context, service string, rows []config.CoverageRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.errs[service]; err != nil {
		return err
	}
	if s.upserts == nil {
		s.upserts = map[string][]config.CoverageRow{}
	}
	s.upserts[service] = rows
	return nil
}

func (s *fakeStore) PruneServices(_ context.Context, keep []string) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pruneErr != nil {
		return nil, s.pruneErr
	}
	pruned := map[string]int64{}
	for service, rows := range s.upserts {
		if !slices.Contains(keep, service) {
			pruned[service] = int64(len(rows))
			delete(s.upserts, service)
		}
	}
	return pruned, nil
}

func (s *fakeStore) services() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.upserts))
}

type flushResponse struct {
	status int
	body   []byte
}

func newFlushServer(t *testing.T, responses map[string]flushResponse) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, ok := responses[r.URL.Path]
		if !ok || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(resp.status)
		_, _ = w.Write(resp.body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

var sampleRows = []config.CoverageRow{
	{File: "github.com/x/svc/a.go", Function: "Run", Percent: 75},
	{File: "github.com/x/svc/a.go", Function: "Stop", Percent: 0},
}

const sampleReport = "github.com/x/svc/a.go:3:\t\tRun\t\t75.0%\ngithub.com/x/svc/a.go:9:\t\t(*S).Stop\t\t0.0%\ntotal\t(statements)\t50.0%\n"

const refusedPath = "<refused>"

func closedURL(t *testing.T) string {
	t.Helper()
	return "http://" + freeAddr(t) + "/debug/coverage/flush"
}

var errRefused = &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}

type scriptedTransport struct {
	mu   sync.Mutex
	errs []error
	body []byte
}

func scripted(body []byte, errs ...error) *scriptedTransport {
	return &scriptedTransport{errs: errs, body: body}
}

func (s *scriptedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(s.body))}, nil
}

func pushFixtures(t *testing.T) map[string][]byte {
	t.Helper()
	good := buildTar(t, reg("covmeta.p", "m"), reg("covcounters.p.1.1", "c"))
	return map[string][]byte{
		"good":      good,
		"trailing":  append(slices.Clone(good), strings.Repeat("x", 4096)...),
		"unsafe":    buildTar(t, reg("../covcounters.x", "c")),
		"meta-only": buildTar(t, reg("covmeta.p", "m")),
		"big":       buildTar(t, reg("covcounters.big", strings.Repeat("x", 4096))),
	}
}

func writeConfigAtomic(t *testing.T, path, body string) {
	t.Helper()
	tmp := path + ".tmp"
	writeFile(t, tmp, body)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename config: %v", err)
	}
}

func waitHits(t *testing.T, hits <-chan string, path string, n int) {
	t.Helper()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for n > 0 {
		select {
		case got := <-hits:
			if got == path {
				n--
			}
		case <-timeout.C:
			t.Fatalf("timed out waiting for %d more pulls of %s", n, path)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

func assertErrContains(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want it to contain %q", err, want)
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestIngest(t *testing.T) {
	t.Parallel()
	good := func(id string) flushResponse {
		return flushResponse{status: http.StatusOK, body: buildTar(t, reg("covmeta."+id, "m"), reg("covcounters."+id+".1.1", "c"))}
	}
	tests := []struct {
		name          string
		paths         []config.Target
		responses     map[string]flushResponse
		runnerOut     string
		runnerErr     error
		storeErrs     map[string]error
		wantErrs      []string
		wantDirCounts []int
		wantFileSets  [][]string
		wantUpserts   map[string][]config.CoverageRow
	}{
		{
			name:          "multi-source service merges every stack",
			paths:         []config.Target{{Service: "eri", URL: "/eri"}, {Service: "eri", URL: "/eri-agent-alpha"}, {Service: "haro", URL: "/haro"}},
			responses:     map[string]flushResponse{"/eri": good("e1"), "/eri-agent-alpha": good("e2"), "/haro": good("h1")},
			runnerOut:     sampleReport,
			wantDirCounts: []int{2, 1},
			wantFileSets: [][]string{
				{"covcounters.e1.1.1", "covcounters.e2.1.1", "covmeta.e1", "covmeta.e2"},
				{"covcounters.h1.1.1", "covmeta.h1"},
			},
			wantUpserts: map[string][]config.CoverageRow{"eri": sampleRows, "haro": sampleRows},
		},
		{
			name:          "failing targets are skipped and named",
			paths:         []config.Target{{Service: "eri", URL: "/eri"}, {Service: "eri", URL: "/eri-agent-beta"}, {Service: "coven", URL: "/coven"}, {Service: "vanadis", URL: "/vanadis"}},
			responses:     map[string]flushResponse{"/eri": good("e1"), "/eri-agent-beta": {status: http.StatusInternalServerError, body: []byte("no meta")}, "/vanadis": {status: http.StatusOK, body: buildTar(t, reg("covmeta.v", "m"))}},
			runnerOut:     sampleReport,
			wantErrs:      []string{"target eri (", "/eri-agent-beta): flush: HTTP 500: no meta", "target coven (", "HTTP 404", "target vanadis (", "no covcounters files"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}},
			wantUpserts:   map[string][]config.CoverageRow{"eri": sampleRows},
		},
		{
			name:          "unsafe archive fails the target",
			paths:         []config.Target{{Service: "eri", URL: "/eri"}},
			responses:     map[string]flushResponse{"/eri": {status: http.StatusOK, body: buildTar(t, reg("../covcounters.x", "c"))}},
			wantErrs:      []string{"unsafe archive entry"},
			wantDirCounts: nil,
		},
		{
			name:          "covdata failure names the service",
			paths:         []config.Target{{Service: "eri", URL: "/eri"}},
			responses:     map[string]flushResponse{"/eri": good("e1")},
			runnerErr:     errors.New("covdata func: exit status 1"),
			wantErrs:      []string{"service eri: covdata func: exit status 1"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}},
		},
		{
			name:          "empty report never reaches the store",
			paths:         []config.Target{{Service: "eri", URL: "/eri"}},
			responses:     map[string]flushResponse{"/eri": good("e1")},
			runnerOut:     "total\t(statements)\t0.0%\n",
			wantErrs:      []string{"service eri: covdata report has no functions"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}},
		},
		{
			name:          "store failure does not stop later services",
			paths:         []config.Target{{Service: "eri", URL: "/eri"}, {Service: "haro", URL: "/haro"}},
			responses:     map[string]flushResponse{"/eri": good("e1"), "/haro": good("h1")},
			runnerOut:     sampleReport,
			storeErrs:     map[string]error{"eri": errors.New("commit eri: deadlock")},
			wantErrs:      []string{"service eri: commit eri: deadlock"},
			wantDirCounts: []int{1, 1},
			wantFileSets:  [][]string{{"covcounters.e1.1.1", "covmeta.e1"}, {"covcounters.h1.1.1", "covmeta.h1"}},
			wantUpserts:   map[string][]config.CoverageRow{"haro": sampleRows},
		},
		{
			name:          "refused target gives up after its window and later services still ingest",
			paths:         []config.Target{{Service: "eri", URL: refusedPath}, {Service: "haro", URL: "/haro"}},
			responses:     map[string]flushResponse{"/haro": good("h1")},
			runnerOut:     sampleReport,
			wantErrs:      []string{"target eri (", "connection refused"},
			wantDirCounts: []int{1},
			wantFileSets:  [][]string{{"covcounters.h1.1.1", "covmeta.h1"}},
			wantUpserts:   map[string][]config.CoverageRow{"haro": sampleRows},
		},
		{
			name:     "malformed url fails the target",
			paths:    []config.Target{{Service: "eri", URL: "\x7f"}},
			wantErrs: []string{"build request"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newFlushServer(t, tc.responses)
			cfg := config.Config{FlushTimeout: 5 * time.Second, ConnectRetryWindow: 20 * time.Millisecond, ConnectRetryInterval: time.Millisecond}
			for _, p := range tc.paths {
				url := srv.URL + p.URL
				if p.URL == refusedPath {
					url = closedURL(t)
				}
				cfg.Targets = append(cfg.Targets, config.Target{Service: p.Service, URL: url})
			}
			runner := &fakeRunner{out: []byte(tc.runnerOut), err: tc.runnerErr}
			store := &fakeStore{errs: tc.storeErrs}
			err := NewServer(cfg, srv.Client(), runner, store).Ingest(context.Background(), cfg)
			if len(tc.wantErrs) == 0 && err != nil {
				t.Fatalf("Ingest error = %v, want nil", err)
			}
			for _, want := range tc.wantErrs {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("Ingest error = %v, want it to contain %q", err, want)
				}
			}
			if !slices.Equal(runner.dirCounts, tc.wantDirCounts) {
				t.Fatalf("covdata dir counts = %v, want %v", runner.dirCounts, tc.wantDirCounts)
			}
			if !reflect.DeepEqual(runner.fileSets, tc.wantFileSets) {
				t.Fatalf("covdata inputs = %v, want %v", runner.fileSets, tc.wantFileSets)
			}
			if !reflect.DeepEqual(store.upserts, tc.wantUpserts) {
				t.Fatalf("upserts = %+v, want %+v", store.upserts, tc.wantUpserts)
			}
		})
	}
}

func TestFetchTargetErrors(t *testing.T) {
	t.Parallel()
	archive := buildTar(t, reg("covmeta.e1", "m"), reg("covcounters.e1.1.1", "c"))
	const eriURL = "http://eri.aerial/debug/coverage/flush"
	tests := []struct {
		name        string
		transport   http.RoundTripper
		url         string
		timeout     time.Duration
		window      time.Duration
		interval    time.Duration
		dir         string
		wantErr     string
		wantRefused bool
		wantFiles   []string
	}{
		{name: "non-refused transport error fails without retry", transport: scripted(archive, errors.New("tls handshake failure")), url: eriURL, timeout: time.Second, window: time.Second, interval: time.Millisecond, dir: "fresh", wantErr: "flush: Post \"http://eri.aerial/debug/coverage/flush\": tls handshake failure"},
		{name: "refused dials recover within the window", transport: scripted(archive, errRefused, errRefused), url: eriURL, timeout: time.Second, window: time.Second, interval: time.Millisecond, dir: "fresh", wantFiles: []string{"covcounters.e1.1.1", "covmeta.e1"}},
		{name: "refused without a retry window fails at once", transport: scripted(archive, errRefused), url: eriURL, timeout: time.Second, dir: "fresh", wantErr: "connection refused", wantRefused: true},
		{name: "closed port gives up after the window", timeout: 5 * time.Second, window: 20 * time.Millisecond, interval: time.Millisecond, dir: "fresh", wantErr: "connection refused", wantRefused: true},
		{name: "flush timeout ends the retry wait", timeout: 20 * time.Millisecond, window: time.Minute, interval: 10 * time.Second, dir: "fresh", wantErr: "connection refused", wantRefused: true},
		{name: "existing target dir", transport: scripted(archive), url: eriURL, timeout: time.Second, dir: "", wantErr: "create target dir"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &http.Client{Transport: tc.transport}
			url := tc.url
			if url == "" {
				url = closedURL(t)
			}
			cfg := config.Config{FlushTimeout: tc.timeout, ConnectRetryWindow: tc.window, ConnectRetryInterval: tc.interval}
			dir := filepath.Join(t.TempDir(), tc.dir)
			err := fetchTarget(context.Background(), client, cfg, config.Target{Service: "eri", URL: url}, dir)
			assertErrContains(t, err, tc.wantErr)
			if got := errors.Is(err, syscall.ECONNREFUSED); got != tc.wantRefused {
				t.Fatalf("errors.Is(%v, ECONNREFUSED) = %v, want %v", err, got, tc.wantRefused)
			}
			if tc.wantFiles != nil && !slices.Equal(listDir(t, dir), tc.wantFiles) {
				t.Fatalf("extracted = %v, want %v", listDir(t, dir), tc.wantFiles)
			}
		})
	}
}

func TestIngestEnvFailures(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	tests := []struct {
		name    string
		envKey  string
		envVal  string
		wantErr string
	}{
		{name: "unusable temp dir", envKey: "TMPDIR", envVal: missing, wantErr: "create work dir"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envKey, tc.envVal)
			cfg := config.Config{Targets: []config.Target{{Service: "eri", URL: "http://127.0.0.1:1/debug/coverage/flush"}}}
			err := NewServer(cfg, nil, &fakeRunner{}, &fakeStore{}).Ingest(context.Background(), cfg)
			assertErrContains(t, err, tc.wantErr)
		})
	}
}

func TestHandler(t *testing.T) {
	t.Parallel()
	fixtures := pushFixtures(t)
	tests := []struct {
		name         string
		method       string
		path         string
		fixture      string
		chunked      bool
		storeErrs    map[string]error
		wantStatus   int
		wantFileSets [][]string
		wantUpserts  map[string][]config.CoverageRow
	}{
		{name: "allowlisted push upserts", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "good", wantStatus: http.StatusOK, wantFileSets: [][]string{{"covcounters.p.1.1", "covmeta.p"}}, wantUpserts: map[string][]config.CoverageRow{"photos-sync": sampleRows}},
		{name: "unknown service", method: http.MethodPost, path: "/v1/coverage/eri", fixture: "good", wantStatus: http.StatusNotFound},
		{name: "unsafe archive", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "unsafe", wantStatus: http.StatusBadRequest},
		{name: "no counters", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "meta-only", wantStatus: http.StatusBadRequest},
		{name: "empty body", method: http.MethodPost, path: "/v1/coverage/photos-sync", wantStatus: http.StatusBadRequest},
		{name: "oversized by content length", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "big", wantStatus: http.StatusRequestEntityTooLarge},
		{name: "oversized chunked body", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "big", chunked: true, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "valid archive with oversized trailing data", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "trailing", chunked: true, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "store failure", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "good", storeErrs: map[string]error{"photos-sync": errors.New("commit: deadlock")}, wantStatus: http.StatusInternalServerError, wantFileSets: [][]string{{"covcounters.p.1.1", "covmeta.p"}}},
		{name: "health", method: http.MethodGet, path: "/health", wantStatus: http.StatusOK},
		{name: "wrong method on push path", method: http.MethodGet, path: "/v1/coverage/photos-sync", wantStatus: http.StatusMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runner := &fakeRunner{out: []byte(sampleReport)}
			store := &fakeStore{errs: tc.storeErrs}
			cfg := config.Config{PushServices: []string{"photos-sync"}, MaxPushBytes: 4096}
			srv := httptest.NewServer(NewServer(cfg, nil, runner, store).Handler())
			t.Cleanup(srv.Close)
			var body io.Reader = bytes.NewReader(fixtures[tc.fixture])
			if tc.chunked {
				body = io.MultiReader(body)
			}
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, body)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if !reflect.DeepEqual(runner.fileSets, tc.wantFileSets) {
				t.Fatalf("covdata inputs = %v, want %v", runner.fileSets, tc.wantFileSets)
			}
			if !reflect.DeepEqual(store.upserts, tc.wantUpserts) {
				t.Fatalf("upserts = %+v, want %+v", store.upserts, tc.wantUpserts)
			}
		})
	}
}

func TestHandlerEnvFailures(t *testing.T) {
	archive := pushFixtures(t)["good"]
	tests := []struct {
		name       string
		envKey     string
		envVal     string
		wantStatus int
	}{
		{name: "unusable temp dir", envKey: "TMPDIR", envVal: filepath.Join(t.TempDir(), "missing"), wantStatus: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envKey, tc.envVal)
			cfg := config.Config{PushServices: []string{"photos-sync"}, MaxPushBytes: 4096}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/coverage/photos-sync", bytes.NewReader(archive))
			NewServer(cfg, nil, &fakeRunner{out: []byte(sampleReport)}, &fakeStore{}).Handler().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestPullLoop(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		reload       string
		wantPath     string
		wantServices []string
	}{
		{name: "broken reload keeps last good targets and skips prune", reload: "targets: [\n", wantPath: "/a", wantServices: []string{"a", "retired"}},
		{name: "valid reload switches targets and prunes the rest", reload: "targets:\n  - service: b\n    url: {srv}/b\npull_interval: 6ms\n", wantPath: "/b", wantServices: []string{"b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			archive := buildTar(t, reg("covmeta.p", "m"), reg("covcounters.p.1.1", "c"))
			hits := make(chan string)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case hits <- r.URL.Path:
				case <-r.Context().Done():
					return
				}
				_, _ = w.Write(archive)
			}))
			t.Cleanup(srv.Close)
			path := filepath.Join(t.TempDir(), "config.yaml")
			initial := "targets:\n  - service: a\n    url: " + srv.URL + "/a\npull_interval: 5ms\n"
			writeConfigAtomic(t, path, initial)
			cfg, err := config.ParseConfig([]byte(initial))
			if err != nil {
				t.Fatalf("parse initial config: %v", err)
			}
			store := &fakeStore{upserts: map[string][]config.CoverageRow{"dashboard": sampleRows}}
			s := NewServer(cfg, srv.Client(), &fakeRunner{out: []byte(sampleReport)}, store)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.PullLoop(ctx, path)
			}()
			waitHits(t, hits, "/a", 1)
			writeConfigAtomic(t, path, strings.ReplaceAll(tc.reload, "{srv}", srv.URL))
			waitHits(t, hits, tc.wantPath, 2)
			if err := store.UpsertService(ctx, "retired", sampleRows); err != nil {
				t.Fatalf("seed retired: %v", err)
			}
			waitHits(t, hits, tc.wantPath, 2)
			cancel()
			<-done
			if got := store.services(); !reflect.DeepEqual(got, tc.wantServices) {
				t.Fatalf("services = %v, want %v", got, tc.wantServices)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			for _, svc := range tc.wantServices {
				if !reflect.DeepEqual(store.upserts[svc], sampleRows) {
					t.Fatalf("upserts[%s] = %+v, want %+v", svc, store.upserts[svc], sampleRows)
				}
			}
		})
	}
}

func TestPruneRetired(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		cfg          config.Config
		pruneErr     error
		wantServices []string
	}{
		{name: "drops services outside the config", cfg: config.Config{Targets: []config.Target{{Service: "eri", URL: "http://eri"}}, PushServices: []string{"photos-sync"}}, wantServices: []string{"eri", "eri-restate", "photos-sync"}},
		{name: "eri-restate survives without a target", cfg: config.Config{Targets: []config.Target{{Service: "eri", URL: "http://eri"}}}, wantServices: []string{"eri", "eri-restate"}},
		{name: "empty keep set refuses", cfg: config.Config{}, wantServices: []string{"dashboard", "eri", "eri-restate", "hangar", "photos-sync"}},
		{name: "store failure keeps rows", cfg: config.Config{Targets: []config.Target{{Service: "eri", URL: "http://eri"}}}, pruneErr: errors.New("conn refused"), wantServices: []string{"dashboard", "eri", "eri-restate", "hangar", "photos-sync"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			row := []config.CoverageRow{{File: "a.go", Function: "F", Percent: 50}}
			store := &fakeStore{pruneErr: tc.pruneErr, upserts: map[string][]config.CoverageRow{"dashboard": row, "eri": row, "eri-restate": row, "hangar": row, "photos-sync": row}}
			NewServer(tc.cfg, nil, &fakeRunner{}, store).pruneRetired(context.Background())
			if got := store.services(); !reflect.DeepEqual(got, tc.wantServices) {
				t.Fatalf("services = %v, want %v", got, tc.wantServices)
			}
		})
	}
}

func TestIngestService(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		slotHeld    bool
		cancelled   bool
		wantErr     string
		wantUpserts map[string][]config.CoverageRow
	}{
		{name: "free slot ingests", wantUpserts: map[string][]config.CoverageRow{"photos-sync": {{File: "github.com/x/svc/a.go", Function: "Run", Percent: 75}, {File: "github.com/x/svc/a.go", Function: "Stop", Percent: 0}}}},
		{name: "held slot gives up when ctx ends", slotHeld: true, cancelled: true, wantErr: "wait for ingest slot: context canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeStore{}
			s := NewServer(config.Config{}, nil, &fakeRunner{out: []byte(sampleReport)}, store)
			if tc.slotHeld {
				s.sem <- struct{}{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			err := s.ingestService(ctx, "photos-sync", []string{t.TempDir()})
			assertErrContains(t, err, tc.wantErr)
			if !reflect.DeepEqual(store.upserts, tc.wantUpserts) {
				t.Fatalf("upserts = %+v, want %+v", store.upserts, tc.wantUpserts)
			}
		})
	}
}

func TestRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "bad.yaml"), "targets: [\n")
	writeFile(t, filepath.Join(dir, "badlisten.yaml"), "push_services: [photos-sync]\nlisten: '127.0.0.1:-1'\n")
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
	}{
		{name: "unknown flag", args: []string{"-nope"}, wantCode: 2, wantStderr: "flag provided but not defined"},
		{name: "missing config flag", args: nil, wantCode: 2, wantStderr: "-config is required"},
		{name: "unreadable config", args: []string{"-config", filepath.Join(dir, "absent.yaml")}, wantCode: 1, wantStderr: "read config"},
		{name: "invalid config", args: []string{"-config", filepath.Join(dir, "bad.yaml")}, wantCode: 1, wantStderr: "parse config"},
		{name: "unusable listen address", args: []string{"-config", filepath.Join(dir, "badlisten.yaml")}, wantCode: 1, wantStderr: "listen:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stderr bytes.Buffer
			if code := Run(context.Background(), tc.args, &stderr); code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d (stderr %q)", code, tc.wantCode, stderr.String())
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestRunEnvFailures(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	writeFile(t, cfgPath, "targets:\n  - service: eri\n    url: http://127.0.0.1:1/debug/coverage/flush\n")
	tests := []struct {
		name       string
		envKey     string
		envVal     string
		wantCode   int
		wantStderr string
	}{
		{name: "invalid postgres env", envKey: "PGPORT", envVal: "not-a-port", wantCode: 1, wantStderr: "postgres config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envKey, tc.envVal)
			var stderr bytes.Buffer
			if code := Run(context.Background(), []string{"-config", cfgPath}, &stderr); code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d (stderr %q)", code, tc.wantCode, stderr.String())
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestRunServes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		flushStatus  int
		wantPulls    int
		wantHealth   int
		wantExitCode int
	}{
		{name: "failing target does not stop serving or pulling", flushStatus: http.StatusInternalServerError, wantPulls: 2, wantHealth: http.StatusOK, wantExitCode: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hits := make(chan string)
			flush := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case hits <- r.URL.Path:
				case <-r.Context().Done():
					return
				}
				w.WriteHeader(tc.flushStatus)
			}))
			t.Cleanup(flush.Close)
			addr := freeAddr(t)
			cfgPath := filepath.Join(t.TempDir(), "cfg.yaml")
			writeFile(t, cfgPath, "targets:\n  - service: eri\n    url: "+flush.URL+"/missing\npush_services: [photos-sync]\npull_interval: 5ms\nlisten: '"+addr+"'\n")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			code := make(chan int, 1)
			go func() {
				var stderr bytes.Buffer
				code <- Run(ctx, []string{"-config", cfgPath}, &stderr)
			}()
			waitHits(t, hits, "/missing", tc.wantPulls)
			resp, err := http.Get("http://" + addr + "/health")
			if err != nil {
				t.Fatalf("health: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.wantHealth {
				t.Fatalf("health status = %d, want %d", resp.StatusCode, tc.wantHealth)
			}
			cancel()
			if got := <-code; got != tc.wantExitCode {
				t.Fatalf("exit code = %d, want %d", got, tc.wantExitCode)
			}
		})
	}
}
