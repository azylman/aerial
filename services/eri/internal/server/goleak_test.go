package server

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pgtest"
)

type hermeticTestTransport struct {
	base http.RoundTripper
}

func (h *hermeticTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, errors.New("hermetic test guard: nil request or URL")
	}
	host := req.URL.Hostname()
	if host != "127.0.0.1" && host != "::1" && host != "localhost" {
		return nil, fmt.Errorf("hermetic test guard: blocked non-loopback HTTP call to %s", req.URL.Host)
	}
	switch req.URL.Port() {
	case "8080", "8088", "4020", "8081", "8082", "8083", "8090", "8070", "9070":
		return nil, fmt.Errorf("hermetic test guard: blocked dial to live daemon port %s", req.URL.Port())
	}
	base := h.base
	if base == nil {
		base = http.DefaultTransport
		if base == h {
			base = &http.Transport{}
		}
	}
	return base.RoundTrip(req)
}

func (h *hermeticTestTransport) CloseIdleConnections() {
	if cl, ok := h.base.(interface{ CloseIdleConnections() }); ok {
		cl.CloseIdleConnections()
	}
}

func wrapHermeticTransport() {
	if _, ok := http.DefaultTransport.(*hermeticTestTransport); !ok {
		http.DefaultTransport = &hermeticTestTransport{base: http.DefaultTransport}
	}
}

func init() {
	wrapHermeticTransport()
	// Always use a temp token: a pod-provided AERIAL_SA_TOKEN_PATH must never
	// leak the real ServiceAccount token into test requests.
	if f, err := os.CreateTemp("", "eri-test-sa-token-*"); err == nil {
		_, _ = f.WriteString("test-sa-token")
		_ = f.Close()
		_ = os.Setenv("AERIAL_SA_TOKEN_PATH", f.Name())
		transcriptsProxyHTTPClient.Transport = k8sauth.NewServiceAccountTransport(http.DefaultTransport, f.Name())
	}
}

// TestMain starts one throwaway Postgres for the package (pgtest.Main) inside
// the hermetic transport guard and goleak check.
func TestMain(m *testing.M) {
	wrapHermeticTransport()
	goleak.VerifyTestMain(pgtestRunner{m: m}, goleak.Cleanup(func(exitCode int) {
		http.DefaultClient.CloseIdleConnections()
		if cl, ok := http.DefaultTransport.(interface{ CloseIdleConnections() }); ok {
			cl.CloseIdleConnections()
		}
	}))
}

// pgtestRunner runs the tests under pgtest.Main, then closes idle HTTP
// connections so goleak only sees goroutines the tests really leaked.
type pgtestRunner struct{ m *testing.M }

func (r pgtestRunner) Run() int {
	code := pgtest.Main(r.m)
	http.DefaultClient.CloseIdleConnections()
	if cl, ok := http.DefaultTransport.(interface{ CloseIdleConnections() }); ok {
		cl.CloseIdleConnections()
	}
	return code
}

func TestHermeticTransport_BlocksNonLoopbackAndReservedPorts(t *testing.T) {
	tr, ok := http.DefaultTransport.(*hermeticTestTransport)
	if !ok {
		t.Fatalf("expected http.DefaultTransport to be *hermeticTestTransport, got %T", http.DefaultTransport)
	}

	resp, err := tr.RoundTrip(nil)
	if err == nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		t.Fatal("expected error on nil request")
	}

	reqExternal, err := http.NewRequest(http.MethodGet, "http://example.com/test", http.NoBody)
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	resp, err = tr.RoundTrip(reqExternal)
	if err == nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		t.Fatal("expected error on non-loopback request")
	}
	if !strings.Contains(err.Error(), "blocked non-loopback HTTP call") {
		t.Fatalf("expected non-loopback block error, got %v", err)
	}

	for _, port := range []string{"8080", "8088", "4020", "8081", "8082", "8083", "8090", "8070", "9070"} {
		reqReserved, err := http.NewRequest(http.MethodGet, "http://localhost:"+port+"/test", http.NoBody)
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		resp, err = tr.RoundTrip(reqReserved)
		if err == nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			t.Fatalf("expected error on reserved port %s", port)
		}
		if !strings.Contains(err.Error(), "blocked dial to live daemon port") {
			t.Fatalf("expected port %s block error, got %v", port, err)
		}
	}
}
