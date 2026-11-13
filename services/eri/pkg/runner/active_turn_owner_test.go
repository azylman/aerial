package runner

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func envValue(env []string, key string) string {
	for _, e := range env {
		if v, ok := strings.CutPrefix(e, key+"="); ok {
			return v
		}
	}
	return ""
}

// TestUnifiedProcessPool_LeaseActiveTurnMatchesDaemonEnv proves the key the worker registers
// (the lease's ActiveTurn) is the ERI_DAEMON_ID the daemon's MCP calls and hooks carry.
func TestUnifiedProcessPool_LeaseActiveTurnMatchesDaemonEnv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		proxyPort    string
		interchanged bool
		wantHookEnv  string
	}{
		{name: "tracing enabled exports daemon id and hook endpoint", proxyPort: "8080", wantHookEnv: "http://127.0.0.1:8080/internal/mcp-proxy/announce"},
		{name: "interchangeable lease forwards the worker daemon", proxyPort: "9090", interchanged: true, wantHookEnv: "http://127.0.0.1:9090/internal/mcp-proxy/announce"},
		{name: "tracing disabled exports neither"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var firstEnv []string
			home := filepath.Join(t.TempDir(), "runtimes", "discord")
			first := NewUnifiedProcessPool(PoolConfig{GeminiHomeDir: home, MCPProxyPort: tc.proxyPort}, newPipeMockSpawner(pipeMockSpawnerOpts{onSpawn: func(cfg DaemonConfig) {
				mu.Lock()
				defer mu.Unlock()
				firstEnv = cfg.Env
			}}))
			second := NewUnifiedProcessPool(PoolConfig{GeminiHomeDir: home, MCPProxyPort: tc.proxyPort}, newPipeMockSpawner(pipeMockSpawnerOpts{}))
			defer first.Close()
			defer second.Close()

			var pool AgentPool = first
			if tc.interchanged {
				pool = NewInterchangeablePool(first, InterchangeablePoolConfig{WorkerCount: 1, KeyPrefix: "discord:worker"})
			}
			lease, err := pool.AcquireLease(context.Background(), "discord-1")
			if err != nil {
				t.Fatalf("AcquireLease: %v", err)
			}
			turn := lease.(ActiveTurnOwner).ActiveTurn()
			_ = lease.Release()
			other, err := second.AcquireLease(context.Background(), "discord-1")
			if err != nil {
				t.Fatalf("AcquireLease second pool: %v", err)
			}
			otherTurn := other.(ActiveTurnOwner).ActiveTurn()
			_ = other.Release()

			if turn.Target != "discord" || turn.DaemonID == "" || turn.DaemonID == otherTurn.DaemonID {
				t.Errorf("turns = %+v, %+v; want discord target and distinct daemon ids", turn, otherTurn)
			}
			mu.Lock()
			defer mu.Unlock()
			wantID := ""
			if tc.proxyPort != "" {
				wantID = turn.DaemonID
			}
			if got := envValue(firstEnv, "ERI_DAEMON_ID"); got != wantID {
				t.Errorf("ERI_DAEMON_ID = %q, want %q", got, wantID)
			}
			if got := envValue(firstEnv, "ERI_HOOK_ENDPOINT"); got != tc.wantHookEnv {
				t.Errorf("ERI_HOOK_ENDPOINT = %q, want %q", got, tc.wantHookEnv)
			}
		})
	}
}
