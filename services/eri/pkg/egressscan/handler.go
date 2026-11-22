// Package egressscan serves the loopback-only MCP egress secret scan used by
// the in-pod hook-guard. Agent turns cannot see registered secret values, so
// the hook posts MCP tool arguments here, where the sanitizer pool is loaded.
// Only destinations on the config egress_scan.blocked_mcp list are scanned.
package egressscan

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/guard"
)

// Path is the route the egress scan is served on.
const Path = "/internal/guard/egress-scan"

// maxRequestBytes caps the request body; larger requests get 413 (the hook fails closed).
const maxRequestBytes = 32 << 20

// Register mounts the egress scan on mux behind httpkit.LoopbackOnly, so it is
// reachable only from inside the pod.
func Register(mux *http.ServeMux, cfg *config.Config) {
	mux.Handle("POST "+Path, httpkit.LoopbackOnly(Handler(cfg, os.Getenv)))
}

// Handler scans MCP arguments bound for blocklisted destinations. getenv
// resolves the custom pattern sources read by guard.GetActivePatterns. It
// never writes arguments or samples to its log line.
func Handler(cfg *config.Config, getenv func(string) string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req guard.EgressScanRequest
		status, blocked := serve(w, r, cfg, getenv, &req)
		log.Printf("[egress-scan] status=%d server=%q tool=%q blocked=%t", status, req.Server, req.Tool, blocked)
	})
}

func serve(w http.ResponseWriter, r *http.Request, cfg *config.Config, getenv func(string) string, req *guard.EgressScanRequest) (int, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return writeError(w, http.StatusRequestEntityTooLarge, "request body too large"), false
		}
		return writeError(w, http.StatusBadRequest, "failed to read body"), false
	}
	if json.Unmarshal(body, req) != nil {
		*req = guard.EgressScanRequest{}
		return writeError(w, http.StatusBadRequest, "invalid json payload"), false
	}
	req.Server = strings.TrimSpace(req.Server)
	req.Tool = strings.TrimSpace(req.Tool)
	if req.Server == "" || req.Tool == "" {
		return writeError(w, http.StatusBadRequest, "server and tool are required"), false
	}
	blocklisted := cfg.Current().EgressScan.Blocks(req.Server, req.Tool)
	if blocklisted && sanitizer.SensitiveTokenCount() == 0 {
		return writeError(w, http.StatusServiceUnavailable, "no registered secret values loaded"), false
	}
	result := guard.EvaluateEgress(*req, blocklisted, guard.GetActivePatterns(getenv))
	httpkit.WriteJSON(w, http.StatusOK, result)
	return http.StatusOK, result.Blocked
}

func writeError(w http.ResponseWriter, status int, msg string) int {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
	return status
}
