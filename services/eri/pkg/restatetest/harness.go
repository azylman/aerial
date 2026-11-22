//go:build restateintegration

package restatetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
)

var restateServerBin = flag.String("restate-server-bin", "", "path to the restate-server binary (set by scripts/restate-integration-test.sh)")

const (
	readyTimeout   = 60 * time.Second
	requestTimeout = 30 * time.Second
	pollInterval   = 100 * time.Millisecond
	logTailBytes   = 4000
)

// Harness is one restate-server child process plus one SDK endpoint serving
// the bound service definitions.
type Harness struct {
	t          testing.TB
	IngressURL string
	AdminURL   string
	client     *http.Client

	endpointAddr string
	mu           sync.Mutex
	endpoint     *http.Server
	endpointDone chan struct{}
}

func serverConfig(dataDir string, ports []int) string {
	return fmt.Sprintf(`cluster-name = "restatetest"
node-name = "n1"
base-dir = %q
default-num-partitions = 2
rocksdb-total-memory-size = "256 MiB"
listen-mode = "tcp"
bind-address = "127.0.0.1:%d"
advertised-address = "http://127.0.0.1:%d/"
[ingress]
bind-address = "127.0.0.1:%d"
[admin]
bind-address = "127.0.0.1:%d"
[admin.query-engine]
pgsql-bind-address = "127.0.0.1:%d"
`, dataDir, ports[0], ports[0], ports[1], ports[2], ports[3])
}

// Start launches restate-server, serves defs on a local h2c endpoint and
// registers that endpoint as a deployment. The child is killed and waited in
// t.Cleanup.
func Start(t testing.TB, defs ...restate.ServiceDefinition) *Harness {
	t.Helper()
	bin := *restateServerBin
	if bin == "" {
		t.Fatal("restatetest: -restate-server-bin is required under the restateintegration tag (run scripts/restate-integration-test.sh)")
	}
	ports := freePorts(t, 5)
	base := t.TempDir()
	cfgPath := filepath.Join(base, "restate.toml")
	if err := os.WriteFile(cfgPath, []byte(serverConfig(filepath.Join(base, "data"), ports)), 0o600); err != nil {
		t.Fatalf("restatetest: write config: %v", err)
	}
	logPath := filepath.Join(base, "restate-server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("restatetest: create log: %v", err)
	}
	procCtx, killProc := context.WithCancel(context.Background())
	cmd := exec.CommandContext(procCtx, bin, "-c", cfgPath, "--no-logo", "--log-disable-ansi-codes", "true")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		killProc()
		t.Fatalf("restatetest: start restate-server: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	h := &Harness{
		t:            t,
		IngressURL:   fmt.Sprintf("http://127.0.0.1:%d", ports[1]),
		AdminURL:     fmt.Sprintf("http://127.0.0.1:%d", ports[2]),
		client:       &http.Client{Timeout: requestTimeout},
		endpointAddr: fmt.Sprintf("127.0.0.1:%d", ports[4]),
	}
	t.Cleanup(func() {
		h.stopEndpoint()
		killProc()
		waitErr := <-exited
		if err := logFile.Close(); err != nil {
			t.Logf("restatetest: close server log: %v", err)
		}
		if t.Failed() {
			t.Logf("restate-server exit: %v", waitErr)
			if out, err := os.ReadFile(logPath); err == nil {
				t.Logf("restate-server log tail:\n%s", tail(string(out), logTailBytes))
			}
		}
	})

	h.waitReady(exited)
	h.serveEndpoint(defs...)
	h.register()
	return h
}

func (h *Harness) waitReady(exited <-chan error) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if h.healthy(ctx, h.AdminURL+"/health") && h.healthy(ctx, h.IngressURL+"/restate/health") {
			return
		}
		select {
		case err := <-exited:
			h.t.Fatalf("restatetest: restate-server exited before becoming ready: %v", err)
		case <-ctx.Done():
			h.t.Fatal("restatetest: restate-server not ready before deadline")
		case <-ticker.C:
		}
	}
}

func (h *Harness) healthy(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return false
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return false
	}
	defer h.closeBody(resp)
	return resp.StatusCode == http.StatusOK
}

func (h *Harness) closeBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		h.t.Logf("restatetest: close response body: %v", err)
	}
}

func (h *Harness) serveEndpoint(defs ...restate.ServiceDefinition) {
	h.t.Helper()
	rs := server.NewRestate()
	for _, d := range defs {
		rs = rs.Bind(d)
	}
	handler, err := rs.Handler()
	if err != nil {
		h.t.Fatalf("restatetest: endpoint handler: %v", err)
	}
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: handler, Protocols: &protocols, ReadHeaderTimeout: 5 * time.Second}
	ln := h.listenEndpoint()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.t.Errorf("restatetest: endpoint serve: %v", err)
		}
	}()
	h.mu.Lock()
	h.endpoint = srv
	h.endpointDone = done
	h.mu.Unlock()
}

// listenEndpoint binds the fixed endpoint address, retrying until a previous
// endpoint on the same address has released it.
func (h *Harness) listenEndpoint() net.Listener {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	var lc net.ListenConfig
	for {
		ln, err := lc.Listen(ctx, "tcp", h.endpointAddr)
		if err == nil {
			return ln
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("restatetest: listen on %s: %v", h.endpointAddr, err)
		case <-ticker.C:
		}
	}
}

func (h *Harness) stopEndpoint() {
	h.mu.Lock()
	srv, done := h.endpoint, h.endpointDone
	h.endpoint, h.endpointDone = nil, nil
	h.mu.Unlock()
	if srv == nil {
		return
	}
	// Close, not Shutdown: drop in-flight invocation streams like a pod crash.
	if err := srv.Close(); err != nil {
		h.t.Logf("restatetest: close endpoint: %v", err)
	}
	<-done
}

// RestartEndpoint simulates a pod crash: it closes the endpoint server,
// dropping every in-flight invocation stream, and serves fresh service
// instances on the same address. Restate retries the dropped invocations
// against the new instances (same deployment, same address).
func (h *Harness) RestartEndpoint(defs ...restate.ServiceDefinition) {
	h.t.Helper()
	h.stopEndpoint()
	h.serveEndpoint(defs...)
}

func (h *Harness) register() {
	h.t.Helper()
	body := h.mustJSON(map[string]any{"uri": "http://" + h.endpointAddr, "use_http_11": false})
	status, out := h.post(h.AdminURL+"/deployments", body, nil)
	if status/100 != 2 {
		h.t.Fatalf("restatetest: register deployment: status %d: %s", status, out)
	}
}

// mustJSON marshals v; nil yields an empty body (no-input handlers).
func (h *Harness) mustJSON(v any) []byte {
	h.t.Helper()
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		h.t.Fatalf("restatetest: marshal %T: %v", v, err)
	}
	return b
}

func (h *Harness) post(url string, body []byte, headers map[string]string) (int, []byte) {
	h.t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		h.t.Fatalf("restatetest: build request: %v", err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return h.roundTrip(h.client, req)
}

func (h *Harness) roundTrip(client *http.Client, req *http.Request) (int, []byte) {
	h.t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		h.t.Fatalf("restatetest: %s %s: %v", req.Method, req.URL, err)
	}
	defer h.closeBody(resp)
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("restatetest: read %s %s: %v", req.Method, req.URL, err)
	}
	return resp.StatusCode, out
}

func handlerPath(service, key, handler string) string {
	if key == "" {
		return service + "/" + handler
	}
	return service + "/" + key + "/" + handler
}

// Send enqueues a one-way invocation (key empty for plain services) and
// returns its invocation ID.
func (h *Harness) Send(service, key, handler string, input any) string {
	h.t.Helper()
	path := handlerPath(service, key, handler)
	status, out := h.post(h.IngressURL+"/"+path+"/send", h.mustJSON(input), nil)
	if status/100 != 2 {
		h.t.Fatalf("restatetest: send %s: status %d: %s", path, status, out)
	}
	var res struct {
		InvocationID string `json:"invocationId"`
	}
	if err := json.Unmarshal(out, &res); err != nil || res.InvocationID == "" {
		h.t.Fatalf("restatetest: send %s: bad response %s", path, out)
	}
	return res.InvocationID
}

// Call invokes a handler synchronously and decodes a 2xx response into out
// (nil to discard). A nil input sends an empty body, for handlers without
// input. It returns the HTTP status so callers can assert failures.
func (h *Harness) Call(service, key, handler string, input, out any) int {
	h.t.Helper()
	path := handlerPath(service, key, handler)
	status, resp := h.post(h.IngressURL+"/"+path, h.mustJSON(input), nil)
	if status/100 != 2 {
		h.t.Logf("restatetest: call %s: status %d: %s", path, status, resp)
		return status
	}
	if out != nil {
		if err := json.Unmarshal(resp, out); err != nil {
			h.t.Fatalf("restatetest: call %s: decode %s: %v", path, resp, err)
		}
	}
	return status
}

// AttachResult is the outcome of an attached invocation.
type AttachResult struct {
	Status int
	Body   string
}

// Attach blocks until the invocation completes and returns its HTTP status
// and body (a terminal error surfaces as its error code, e.g. 409 cancelled).
func (h *Harness) Attach(invocationID string) AttachResult {
	h.t.Helper()
	url := h.IngressURL + "/restate/invocation/" + invocationID + "/attach"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	if err != nil {
		h.t.Fatalf("restatetest: build attach request: %v", err)
	}
	status, out := h.roundTrip(&http.Client{Timeout: readyTimeout}, req)
	return AttachResult{Status: status, Body: string(out)}
}

// Query runs SQL against the admin introspection API and returns the rows.
func (h *Harness) Query(sql string) []map[string]any {
	h.t.Helper()
	status, out := h.post(h.AdminURL+"/query", h.mustJSON(map[string]string{"query": sql}), map[string]string{"Accept": "application/json"})
	if status/100 != 2 {
		h.t.Fatalf("restatetest: query %q: status %d: %s", sql, status, out)
	}
	var res struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		h.t.Fatalf("restatetest: query %q: decode %s: %v", sql, out, err)
	}
	return res.Rows
}

// Eventually polls cond with a context-deadline ticker until it returns true.
func (h *Harness) Eventually(what string, timeout time.Duration, cond func() bool) {
	h.t.Helper()
	if !WaitFor(timeout, cond) {
		h.t.Fatalf("restatetest: timed out after %s waiting for %s", timeout, what)
	}
}

// WaitFor polls cond until it returns true or timeout elapses.
func WaitFor(timeout time.Duration, cond func() bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if cond() {
			return true
		}
		select {
		case <-ctx.Done():
			return cond()
		case <-ticker.C:
		}
	}
}

func freePorts(t testing.TB, n int) []int {
	t.Helper()
	ports := make([]int, 0, n)
	listeners := make([]net.Listener, 0, n)
	var lc net.ListenConfig
	for range n {
		ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("restatetest: free port: %v", err)
		}
		listeners = append(listeners, ln)
		addr, ok := ln.Addr().(*net.TCPAddr)
		if !ok {
			t.Fatalf("restatetest: unexpected listener address %v", ln.Addr())
		}
		ports = append(ports, addr.Port)
	}
	for _, ln := range listeners {
		if err := ln.Close(); err != nil {
			t.Fatalf("restatetest: release free port: %v", err)
		}
	}
	return ports
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
