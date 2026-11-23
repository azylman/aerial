package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/goleak"
)

// saTokenSrcEnv is the env var session.SyncTurnSATokenFile reads its source path from. When it
// is unset the copy falls back to the pod's projected ServiceAccount token, so TestMain points it
// at a missing file and tests that exercise the copy override it with setTurnSATokenSrc.
const saTokenSrcEnv = "AERIAL_K8S_SA_TOKEN_SRC"

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "runner-sa-token-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner TestMain: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv(saTokenSrcEnv, filepath.Join(dir, "missing")); err != nil {
		fmt.Fprintf(os.Stderr, "runner TestMain: %v\n", err)
		os.Exit(1)
	}
	// goleak's Cleanup replaces its os.Exit, so exit with the test code after removing dir.
	goleak.VerifyTestMain(m, goleak.Cleanup(func(code int) {
		_ = os.RemoveAll(dir)
		os.Exit(code)
	}))
}

// setTurnSATokenSrc points the SA-token copy at src inside a fresh temp dir holding a "token"
// file and a "token-dir" directory (reading a directory fails, which drives the warning branch).
// It uses t.Setenv, so callers must not run in parallel.
func setTurnSATokenSrc(t *testing.T, src string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte("test-sa-token\n"), 0o600); err != nil {
		t.Fatalf("write SA token fixture: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "token-dir"), 0o755); err != nil {
		t.Fatalf("create SA token dir fixture: %v", err)
	}
	t.Setenv(saTokenSrcEnv, filepath.Join(dir, src))
}
