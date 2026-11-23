package restateclient

import (
	"net/http"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		http.DefaultClient.CloseIdleConnections()
		if cl, ok := http.DefaultTransport.(interface{ CloseIdleConnections() }); ok {
			cl.CloseIdleConnections()
		}
	}))
}
