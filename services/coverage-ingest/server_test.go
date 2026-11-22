package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// pushFixtures are the archive bodies TestHandler rows refer to by name.
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

func TestHandler(t *testing.T) {
	t.Parallel()
	fixtures := pushFixtures(t)
	tests := []struct {
		name         string
		method       string
		path         string
		fixture      string
		chunked      bool // hide the body length so only MaxBytesReader can enforce the cap
		storeErrs    map[string]error
		wantStatus   int
		wantFileSets [][]string
		wantUpserts  map[string][]CoverageRow
	}{
		{name: "allowlisted push upserts", method: http.MethodPost, path: "/v1/coverage/photos-sync", fixture: "good", wantStatus: http.StatusOK, wantFileSets: [][]string{{"covcounters.p.1.1", "covmeta.p"}}, wantUpserts: map[string][]CoverageRow{"photos-sync": sampleRows}},
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
			cfg := Config{PushServices: []string{"photos-sync"}, MaxPushBytes: 4096}
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

// TestHandlerEnvFailures cannot run in parallel because it rewrites process environment.
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
			cfg := Config{PushServices: []string{"photos-sync"}, MaxPushBytes: 4096}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/coverage/photos-sync", bytes.NewReader(archive))
			NewServer(cfg, nil, &fakeRunner{out: []byte(sampleReport)}, &fakeStore{}).Handler().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}

// writeConfigAtomic replaces path in one rename so a reload never reads a partial file.
func writeConfigAtomic(t *testing.T, path, body string) {
	t.Helper()
	tmp := path + ".tmp"
	writeFile(t, tmp, body)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename config: %v", err)
	}
}

// waitHits blocks until n flush requests for path arrive, ignoring other paths.
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

func TestPullLoop(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		reload       string   // config written after the first pull; "{srv}" becomes the flush server URL
		wantPath     string   // path pulled on rounds after the reload
		wantServices []string // services holding rows at the end; "retired" is seeded after the reload took effect
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
			cfg, err := ParseConfig([]byte(initial))
			if err != nil {
				t.Fatalf("parse initial config: %v", err)
			}
			store := &fakeStore{upserts: map[string][]CoverageRow{"dashboard": sampleRows}}
			s := NewServer(cfg, srv.Client(), &fakeRunner{out: []byte(sampleReport)}, store)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.PullLoop(ctx, path)
			}()
			waitHits(t, hits, "/a", 1)
			writeConfigAtomic(t, path, strings.ReplaceAll(tc.reload, "{srv}", srv.URL))
			// The second post-write pull belongs to a round that reloaded after the write.
			waitHits(t, hits, tc.wantPath, 2)
			// Every round from here on reloads the written file; only a successful load
			// may prune this seeded service. Two more pulls guarantee a full round ran.
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
		cfg          Config
		pruneErr     error
		wantServices []string
	}{
		{name: "drops services outside the config", cfg: Config{Targets: []Target{{Service: "eri", URL: "http://eri"}}, PushServices: []string{"photos-sync"}}, wantServices: []string{"eri", "eri-restate", "photos-sync"}},
		{name: "eri-restate survives without a target", cfg: Config{Targets: []Target{{Service: "eri", URL: "http://eri"}}}, wantServices: []string{"eri", "eri-restate"}},
		{name: "empty keep set refuses", cfg: Config{}, wantServices: []string{"dashboard", "eri", "eri-restate", "hangar", "photos-sync"}},
		{name: "store failure keeps rows", cfg: Config{Targets: []Target{{Service: "eri", URL: "http://eri"}}}, pruneErr: errors.New("conn refused"), wantServices: []string{"dashboard", "eri", "eri-restate", "hangar", "photos-sync"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			row := []CoverageRow{{File: "a.go", Function: "F", Percent: 50}}
			store := &fakeStore{pruneErr: tc.pruneErr, upserts: map[string][]CoverageRow{"dashboard": row, "eri": row, "eri-restate": row, "hangar": row, "photos-sync": row}}
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
		wantUpserts map[string][]CoverageRow
	}{
		{name: "free slot ingests", wantUpserts: map[string][]CoverageRow{"photos-sync": {{File: "github.com/x/svc/a.go", Function: "Run", Percent: 75}, {File: "github.com/x/svc/a.go", Function: "Stop", Percent: 0}}}},
		{name: "held slot gives up when ctx ends", slotHeld: true, cancelled: true, wantErr: "wait for ingest slot: context canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeStore{}
			s := NewServer(Config{}, nil, &fakeRunner{out: []byte(sampleReport)}, store)
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

// freeAddr returns a loopback address nothing currently listens on.
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

// TestRunServes drives run with a live context: it must keep pulling after a failing
// target, answer health checks, and exit cleanly once cancelled.
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
				code <- run(ctx, []string{"-config", cfgPath}, &stderr)
			}()
			// The second pull can only start after the first failing round completed.
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
