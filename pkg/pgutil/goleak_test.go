package pgutil

import (
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/pgtest"
)

func TestMain(m *testing.M) {
	hermetictest.Install()
	for _, kv := range os.Environ() {
		if key, _, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(key, "OTEL_") {
			_ = os.Unsetenv(key)
		}
	}
	code := pgtest.Main(m)
	http.DefaultClient.CloseIdleConnections()
	if code == 0 {
		if err := goleak.Find(); err != nil {
			log.Printf("goleak: %v", err)
			code = 1
		}
	}
	os.Exit(code)
}
