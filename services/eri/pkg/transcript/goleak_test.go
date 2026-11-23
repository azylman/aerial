package transcript

import (
	"net/http"
	"testing"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/pgtest"
)

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
