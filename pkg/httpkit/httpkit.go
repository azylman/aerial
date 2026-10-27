package httpkit

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/coverage"
	"strings"
	"syscall"
)

var (
	WriteCountersDirFn = coverage.WriteCountersDir
	WriteMetaDirFn     = coverage.WriteMetaDir
)

// HandleCoverageFlush returns an HTTP handler for /debug/coverage/flush that writes
// runtime coverage metadata and counters to an ephemeral directory and streams them
// back as an application/x-tar archive.
func HandleCoverageFlush(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tmpDir, err := os.MkdirTemp("", "covflush-*")
		if err != nil {
			log.Printf("[%s] coverage flush mkdir error: %v", service, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = os.RemoveAll(tmpDir) }() //nolint:errcheck

		if WriteMetaDirFn != nil {
			if err := WriteMetaDirFn(tmpDir); err != nil {
				log.Printf("[%s] coverage flush meta error: %v", service, err)
			}
		}
		if WriteCountersDirFn != nil {
			if err := WriteCountersDirFn(tmpDir); err != nil {
				log.Printf("[%s] coverage flush error: %v", service, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		entries, _ := os.ReadDir(tmpDir) //nolint:errcheck
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		for _, entry := range entries {
			if data, readErr := os.ReadFile(filepath.Join(tmpDir, entry.Name())); readErr == nil {
				_ = tw.WriteHeader(&tar.Header{ //nolint:errcheck
					Name: entry.Name(),
					Mode: 0o600,
					Size: int64(len(data)),
				})
				_, _ = tw.Write(data) //nolint:errcheck
			}
		}
		CloseWarn(tw, "coverage tar writer")

		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(http.StatusOK)
		WriteResponse(w, r, service, buf.Bytes())
	}
}

// IsClientDisconnect reports whether err or r.Context() indicates an aborted client connection
// (EPIPE, ECONNRESET, net.ErrClosed, or canceled request context).
func IsClientDisconnect(r *http.Request, err error) bool {
	if err == nil {
		return false
	}
	if r != nil && r.Context().Err() != nil {
		return true
	}
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, net.ErrClosed)
}

// WriteResponse writes data to w and logs non-disconnect write errors labeled with service.
func WriteResponse(w http.ResponseWriter, r *http.Request, service string, data []byte) {
	if _, err := w.Write(data); err != nil {
		if !IsClientDisconnect(r, err) {
			log.Printf("[%s] response write error: %v", service, err)
		}
	}
}

// WriteJSON writes a JSON response with the given status code, setting Content-Type: application/json if unset.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	b, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to marshal JSON response: %v", err)
		http.Error(w, `{"error":"Internal error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	if _, err := w.Write(b); err != nil {
		if !IsClientDisconnect(nil, err) {
			log.Printf("Failed to write JSON response: %v", err)
		}
	}
}

// CloseWarn closes c and logs a warning if Close returns a non-nil error.
func CloseWarn(c io.Closer, resource string) {
	if c != nil {
		if err := c.Close(); err != nil {
			log.Printf("Warning closing %s: %v", resource, err)
		}
	}
}

// ResolveConfigPathWithLocal resolves the configuration file path.
// 1. Checks strings.TrimSpace(os.Getenv(envKey)) (defaults envKey to "CONFIG_PATH" if empty).
// 2. If localPath != "", checks os.Stat(localPath) and returns localPath if err == nil && fi.Size() > 0.
// 3. Iterates fallbackPaths and returns the first path where os.Stat(p) succeeds.
// 4. If none exist on disk, returns fallbackPaths[0] (or "" if fallbackPaths is empty).
func ResolveConfigPathWithLocal(envKey, localPath string, fallbackPaths ...string) string {
	key := strings.TrimSpace(envKey)
	if key == "" {
		key = "CONFIG_PATH"
	}
	if env := strings.TrimSpace(os.Getenv(key)); env != "" {
		return env
	}
	if localPath != "" {
		if fi, err := os.Stat(localPath); err == nil && fi.Size() > 0 {
			return localPath
		}
	}
	for _, p := range fallbackPaths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if len(fallbackPaths) > 0 {
		return fallbackPaths[0]
	}
	return ""
}

// NormalizeMCPToolPrefix rewrites JSON-RPC tool invocation payloads where tool names carry a redundant prefix
// (e.g. oldPrefix="memory_memory_", newPrefix="memory_" for coven; oldPrefix="scheduler_", newPrefix="" for haro),
// matching `"name":"<oldPrefix>`, `"name": "<oldPrefix>`, and `"name":  "<oldPrefix>`.
func NormalizeMCPToolPrefix(bodyBytes []byte, oldPrefix, newPrefix string) []byte {
	if oldPrefix == "" || !bytes.Contains(bodyBytes, []byte(oldPrefix)) {
		return bodyBytes
	}
	bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name":"`+oldPrefix), []byte(`"name":"`+newPrefix))
	bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name": "`+oldPrefix), []byte(`"name": "`+newPrefix))
	bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name":  "`+oldPrefix), []byte(`"name": "`+newPrefix))
	return bodyBytes
}

// ServeMCPStream wraps an mcp.StreamableHTTPHandler with GET readiness probe handling (200 OK text/plain with readyBanner),
// POST enforcement (405 Method Not Allowed with Allow: GET, POST and JSON error body), empty/unreadable body rejection (400),
// tool alias prefix normalization via NormalizeMCPToolPrefix, and Accept/Content-Type header normalization.
func ServeMCPStream(w http.ResponseWriter, r *http.Request, streamHandler http.Handler, readyBanner, oldToolPrefix, newToolPrefix string) {
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		WriteResponse(w, r, "mcp", []byte(readyBanner))
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	defer CloseWarn(r.Body, "request body")
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
		return
	}

	trimmed := strings.TrimSpace(string(bodyBytes))
	if trimmed == "" {
		http.Error(w, `{"error":"Empty request body"}`, http.StatusBadRequest)
		return
	}

	if oldToolPrefix != "" {
		bodyBytes = NormalizeMCPToolPrefix(bodyBytes, oldToolPrefix, newToolPrefix)
	}

	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	r.ContentLength = int64(len(bodyBytes))

	r.Header.Set("Accept", "application/json, text/event-stream")
	if r.Header.Get("Content-Type") == "" {
		r.Header.Set("Content-Type", "application/json")
	}

	if streamHandler != nil {
		streamHandler.ServeHTTP(w, r)
	}
}
