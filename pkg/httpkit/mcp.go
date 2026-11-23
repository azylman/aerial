package httpkit

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

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
	AnnotateMCPSpan(r.Context(), bodyBytes)

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

// AnnotateMCPSpan names the span active in ctx "mcp <method>" and tags it with the JSON-RPC
// method and, for tools/call, the tool name. Arguments never reach the span; a missing or
// non-recording span and unparseable bodies are no-ops.
func AnnotateMCPSpan(ctx context.Context, body []byte) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := tracing.MCPRequestAttrs(body)
	if len(attrs) == 0 {
		return
	}
	span.SetName("mcp " + attrs[0].Value.AsString())
	span.SetAttributes(attrs...)
}
