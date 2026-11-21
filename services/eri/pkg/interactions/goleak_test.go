package interactions

import (
	"os"
	"testing"

	"go.uber.org/goleak"
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
	goleak.VerifyTestMain(m)
}
