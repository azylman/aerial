package interactions

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

func init() {
	if os.Getenv("AERIAL_SA_TOKEN_PATH") == "" {
		if f, err := os.CreateTemp("", "eri-interactions-test-sa-token-*"); err == nil {
			_, _ = f.WriteString("test-sa-token")
			_ = f.Close()
			_ = os.Setenv("AERIAL_SA_TOKEN_PATH", f.Name())
		}
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
