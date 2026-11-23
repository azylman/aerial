package interactions

import (
	"net/http"
	"os"
	"testing"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/pgtest"
)

func init() {
	// Always use a temp token so a pod-provided AERIAL_SA_TOKEN_PATH is never read.
	if f, err := os.CreateTemp("", "eri-interactions-test-sa-token-*"); err == nil {
		_, _ = f.WriteString("test-sa-token")
		_ = f.Close()
		_ = os.Setenv("AERIAL_SA_TOKEN_PATH", f.Name())
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(pgtestRunner{m: m})
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
