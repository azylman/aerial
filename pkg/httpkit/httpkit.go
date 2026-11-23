package httpkit

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"syscall"
)

// SetupHealthMux builds the shared ServeMux for health, metrics, and coverage flush.
// It mounts GET /health and GET /healthz with the standard ok JSON, GET /metrics
// only when metrics is non-nil, and GET plus POST /debug/coverage/flush via
// HandleCoverageFlush(service).
func SetupHealthMux(service string, metrics http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	health := func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /healthz", health)
	if metrics != nil {
		mux.Handle("GET /metrics", metrics)
	}

	coverageFlush := HandleCoverageFlush(service)
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
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
