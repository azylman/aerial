package mcp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

type capturedRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
	SpanCtx trace.SpanContext
}

type recordCaptureHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

func (*recordCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordCaptureHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	attrs := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.records = append(h.records, capturedRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   attrs,
		SpanCtx: trace.SpanContextFromContext(ctx),
	})
	return nil
}

func (h *recordCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordCaptureHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordCaptureHandler) snapshot() []capturedRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	res := make([]capturedRecord, len(h.records))
	copy(res, h.records)
	return res
}

func makeTestSpanContext(tidHex, sidHex string) trace.SpanContext {
	tid, _ := trace.TraceIDFromHex(tidHex)
	sid, _ := trace.SpanIDFromHex(sidHex)
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
}

type otelLogProxyTestCase struct {
	name            string
	requestJSON     string
	gzipContent     bool
	daemonHeader    string
	targetSegment   string
	setupTurns      func(r *runner.ActiveTurnRegistry)
	wantStatus      int
	wantRecordCount int
	wantLevel       slog.Level
	wantMsgContains string
	wantAttrs       map[string]any
	omittedAttrs    []string
	wantSpanHex     string // traceId
}

func buildOTelTestRequestBody(tc otelLogProxyTestCase) io.Reader {
	switch {
	case tc.name == "invalid gzip returns 400":
		return strings.NewReader("corrupted-not-gzip")
	case tc.gzipContent:
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, _ = gw.Write([]byte(tc.requestJSON))
		_ = gw.Close()
		return &buf
	default:
		return strings.NewReader(tc.requestJSON)
	}
}

func assertCapturedRecord(t *testing.T, rec capturedRecord, tc otelLogProxyTestCase) {
	t.Helper()
	if rec.Level != tc.wantLevel {
		t.Errorf("level = %v, want %v", rec.Level, tc.wantLevel)
	}
	if tc.wantMsgContains != "" && !strings.Contains(rec.Message, tc.wantMsgContains) {
		t.Errorf("msg = %q, want containing %q", rec.Message, tc.wantMsgContains)
	}
	for k, wantVal := range tc.wantAttrs {
		gotVal, ok := rec.Attrs[k]
		if !ok {
			t.Errorf("missing attr %q", k)
			continue
		}
		if fmt.Sprint(gotVal) != fmt.Sprint(wantVal) {
			t.Errorf("attr %q = %v, want %v", k, gotVal, wantVal)
		}
	}
	for _, k := range tc.omittedAttrs {
		if v, ok := rec.Attrs[k]; ok {
			t.Errorf("expected attr %q to be omitted, but found %v", k, v)
		}
	}
	if tc.wantSpanHex != "" && rec.SpanCtx.TraceID().String() != tc.wantSpanHex {
		t.Errorf("span traceId = %q, want %q", rec.SpanCtx.TraceID().String(), tc.wantSpanHex)
	}
}

func otelLogProxyTestCases() []otelLogProxyTestCase {
	all := append(otelLogProxyFilteringCases(), otelLogProxyDecompressionCases()...)
	all = append(all, otelLogProxyTurnCases()...)
	return append(all, otelLogProxyErrorCases()...)
}

func otelLogProxyFilteringCases() []otelLogProxyTestCase {
	return []otelLogProxyTestCase{
		{
			name: "drops noisy user_prompt event",
			requestJSON: `{
				"resourceLogs": [{
					"scopeLogs": [{
						"logRecords": [{
							"body": {"stringValue": "claude_code.user_prompt"},
							"attributes": [{"key": "event.name", "value": {"stringValue": "claude_code.user_prompt"}}]
						}]
					}]
				}]
			}`,
			wantStatus:      http.StatusOK,
			wantRecordCount: 0,
		},
		{
			name: "drops noisy tool_decision event without prefix",
			requestJSON: `{
				"resourceLogs": [{
					"scopeLogs": [{
						"logRecords": [{
							"body": {"stringValue": "tool_decision"}
						}]
					}]
				}]
			}`,
			wantStatus:      http.StatusOK,
			wantRecordCount: 0,
		},
		{
			name: "drops codex noisy agent_response event",
			requestJSON: `{
				"resourceLogs": [{
					"scopeLogs": [{
						"logRecords": [{
							"body": {"stringValue": "codex.agent_response"}
						}]
					}]
				}]
			}`,
			wantStatus:      http.StatusOK,
			wantRecordCount: 0,
		},
		{
			name: "kept event redacts secrets and drops sensitive keys",
			requestJSON: `{
				"resourceLogs": [{
					"resource": {
						"attributes": [{"key": "service.name", "value": {"stringValue": "claude-code"}}]
					},
					"scopeLogs": [{
						"scope": {"name": "claude-code"},
						"logRecords": [{
							"severityNumber": 9,
							"severityText": "INFO",
							"body": {"stringValue": "Connecting with ghp_1234567890abcdefghijklmnopqrstuvwxyz"},
							"attributes": [
								{"key": "event.name", "value": {"stringValue": "claude_code.api_request"}},
								{"key": "prompt", "value": {"stringValue": "secret-prompt"}},
								{"key": "prompt_text", "value": {"stringValue": "secret-prompt-text"}},
								{"key": "response", "value": {"stringValue": "secret-response"}},
								{"key": "system_prompt", "value": {"stringValue": "secret-sys-prompt"}},
								{"key": "tool_parameters", "value": {"stringValue": "secret-params"}},
								{"key": "tool_input", "value": {"stringValue": "secret-input"}},
								{"key": "tool_result", "value": {"stringValue": "secret-result"}},
								{"key": "raw_body", "value": {"stringValue": "secret-raw"}},
								{"key": "settings", "value": {"stringValue": "secret-settings"}},
								{"key": "managed_settings.settings", "value": {"stringValue": "secret-managed"}},
								{"key": "http.status_code", "value": {"intValue": 200}},
								{"key": "duration_ms", "value": {"doubleValue": 123.45}},
								{"key": "success", "value": {"boolValue": true}},
								{"key": "auth_header", "value": {"stringValue": "Bearer ghp_1234567890abcdefghijklmnopqrstuvwxyz"}}
							]
						}]
					}]
				}]
			}`,
			daemonHeader: "d-claude-test",
			setupTurns: func(r *runner.ActiveTurnRegistry) {
				r.Set(runner.ActiveTurn{DaemonID: "d-claude-test", Target: "discord", ConversationID: "sess-abc"}, makeTestSpanContext("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"))
			},
			wantStatus:      http.StatusOK,
			wantRecordCount: 1,
			wantLevel:       slog.LevelInfo,
			wantMsgContains: "[REDACTED]",
			wantAttrs: map[string]any{
				"source":           "agent_otel",
				"engine":           "claude",
				"stack":            "test-stack",
				"target":           "discord",
				"session_id":       "sess-abc",
				"event_name":       "api_request",
				"http.status_code": int64(200),
				"duration_ms":      123.45,
				"success":          true,
				"auth_header":      "Bearer [REDACTED]",
			},
			omittedAttrs: []string{
				"prompt", "prompt_text", "response", "system_prompt", "tool_parameters",
				"tool_input", "tool_result", "raw_body", "settings", "managed_settings.settings",
			},
			wantSpanHex: "4bf92f3577b34da6a3ce929d0e0e4736",
		},
	}
}

func otelLogProxyDecompressionCases() []otelLogProxyTestCase {
	return []otelLogProxyTestCase{
		{
			name: "gzip decompression succeeds",
			requestJSON: `{
				"resourceLogs": [{
					"scopeLogs": [{
						"logRecords": [{
							"severityNumber": 13,
							"severityText": "WARN",
							"body": {"stringValue": "Warning: high latency detected"},
							"attributes": [{"key": "event.name", "value": {"stringValue": "codex.latency_warning"}}]
						}]
					}]
				}]
			}`,
			gzipContent:     true,
			targetSegment:   "voice",
			wantStatus:      http.StatusOK,
			wantRecordCount: 1,
			wantLevel:       slog.LevelWarn,
			wantMsgContains: "high latency",
			wantAttrs: map[string]any{
				"source":     "agent_otel",
				"engine":     "codex",
				"stack":      "test-stack",
				"target":     "voice",
				"event_name": "latency_warning",
			},
		},
	}
}

func otelLogProxyTurnCases() []otelLogProxyTestCase {
	return []otelLogProxyTestCase{
		{
			name: "turn resolution via session.id attribute fallback",
			requestJSON: `{
				"resourceLogs": [{
					"scopeLogs": [{
						"logRecords": [{
							"severityNumber": 17,
							"severityText": "ERROR",
							"body": {"stringValue": "Process exited unexpectedly"},
							"attributes": [
								{"key": "session.id", "value": {"stringValue": "conv-fallback-42"}},
								{"key": "exit_code", "value": {"intValue": 1}}
							]
						}]
					}]
				}]
			}`,
			setupTurns: func(r *runner.ActiveTurnRegistry) {
				r.Set(runner.ActiveTurn{DaemonID: "daemon-conv-42", Target: "ephemeral", ConversationID: "conv-fallback-42"}, makeTestSpanContext("11111111111111111111111111111111", "2222222222222222"))
			},
			wantStatus:      http.StatusOK,
			wantRecordCount: 1,
			wantLevel:       slog.LevelError,
			wantMsgContains: "Process exited unexpectedly",
			wantAttrs: map[string]any{
				"source":     "agent_otel",
				"stack":      "test-stack",
				"target":     "ephemeral",
				"session_id": "conv-fallback-42",
				"exit_code":  int64(1),
			},
			wantSpanHex: "11111111111111111111111111111111",
		},
		{
			name: "fallback to record traceId and spanId when no turn registered",
			requestJSON: `{
				"resourceLogs": [{
					"scopeLogs": [{
						"logRecords": [{
							"severityNumber": 9,
							"severityText": "INFO",
							"traceId": "abcdef0123456789abcdef0123456789",
							"spanId": "0123456789abcdef",
							"body": {"stringValue": "Untracked session record"}
						}]
					}]
				}]
			}`,
			wantStatus:      http.StatusOK,
			wantRecordCount: 1,
			wantLevel:       slog.LevelInfo,
			wantMsgContains: "Untracked session record",
			wantSpanHex:     "abcdef0123456789abcdef0123456789",
		},
		{
			name: "turn resolution via SoleDaemonInTarget",
			requestJSON: `{
				"resourceLogs": [{
					"resource": {
						"attributes": [{"key": "engine", "value": {"stringValue": "codex"}}]
					},
					"scopeLogs": [{
						"scope": {"name": "claude-scope"},
						"logRecords": [{
							"severityNumber": 5,
							"severityText": "DEBUG",
							"body": "direct-string-body",
							"attributes": [
								{"key": "event.name", "value": "custom_event"},
								{"key": "str_int", "value": {"intValue": "42"}},
								{"key": "flt_int", "value": {"intValue": 99.0}},
								{"key": "dbl", "value": {"doubleValue": 3.14}},
								{"key": "flag", "value": {"boolValue": false}}
							]
						}]
					}]
				}]
			}`,
			targetSegment: "kiosk",
			setupTurns: func(r *runner.ActiveTurnRegistry) {
				r.Set(runner.ActiveTurn{DaemonID: "daemon-kiosk-1", Target: "kiosk", ConversationID: "kiosk-sess"}, makeTestSpanContext("33333333333333333333333333333333", "4444444444444444"))
			},
			wantStatus:      http.StatusOK,
			wantRecordCount: 1,
			wantLevel:       slog.LevelDebug,
			wantMsgContains: "direct-string-body",
			wantAttrs: map[string]any{
				"source":     "agent_otel",
				"engine":     "codex",
				"stack":      "test-stack",
				"target":     "kiosk",
				"session_id": "kiosk-sess",
				"event_name": "custom_event",
				"str_int":    int64(42),
				"flt_int":    int64(99),
				"dbl":        3.14,
				"flag":       false,
			},
			wantSpanHex: "33333333333333333333333333333333",
		},
		{
			name: "scope and event engine inference fallback",
			requestJSON: `{
				"resourceLogs": [{
					"resource": {
						"attributes": [{"key": "service.name", "value": {"stringValue": "codex-runner"}}]
					},
					"scopeLogs": [{
						"scope": {"name": "claude-code-sub"},
						"logRecords": [{
							"severityNumber": 9,
							"severityText": "TRACE",
							"body": {"stringValue": "Trace level test"},
							"attributes": [{"key": "event.name", "value": {"stringValue": "claude_code.agent_step"}}]
						}]
					}]
				}]
			}`,
			wantStatus:      http.StatusOK,
			wantRecordCount: 1,
			wantLevel:       slog.LevelDebug,
			wantMsgContains: "Trace level test",
			wantAttrs: map[string]any{
				"source":     "agent_otel",
				"engine":     "codex",
				"stack":      "test-stack",
				"event_name": "agent_step",
			},
		},
	}
}

func otelLogProxyErrorCases() []otelLogProxyTestCase {
	return []otelLogProxyTestCase{
		{
			name:        "invalid json returns 400",
			requestJSON: `{not-valid-json`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid gzip returns 400",
			requestJSON: `not gzip bytes`,
			gzipContent: true, // We'll send raw uncompressed bytes with Content-Encoding: gzip
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "body exceeding 4MB is rejected",
			requestJSON: `{"resourceLogs": [{"scopeLogs": [{"logRecords": [{"body": {"stringValue": "` + strings.Repeat("A", (4<<20)+1024) + `"}}]}]}]}`,
			wantStatus:  http.StatusBadRequest,
		},
	}
}

func TestOTelLogProxy_FilteringAndScrubbing(t *testing.T) {
	t.Parallel()

	tests := otelLogProxyTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			turns := runner.NewActiveTurnRegistry()
			if tc.setupTurns != nil {
				tc.setupTurns(turns)
			}

			handler := &recordCaptureHandler{}
			logger := slog.New(handler)

			proxy := mcp.NewOTelLogProxy(mcp.OTelLogProxyConfig{
				Turns:  turns,
				Stack:  "test-stack",
				Logger: logger,
			})

			mux := http.NewServeMux()
			proxy.Register(mux)

			reqBody := buildOTelTestRequestBody(tc)
			path := env.OTelLogsPathPrefix
			if tc.targetSegment != "" {
				path += "/" + tc.targetSegment
			}

			req := httptest.NewRequest(http.MethodPost, path, reqBody)
			req.RemoteAddr = "127.0.0.1:4000"
			req.Header.Set("Content-Type", "application/json")
			if tc.gzipContent {
				req.Header.Set("Content-Encoding", "gzip")
			}
			if tc.daemonHeader != "" {
				req.Header.Set(env.DaemonHeader, tc.daemonHeader)
			}

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantStatus != http.StatusOK {
				return
			}

			records := handler.snapshot()
			if len(records) != tc.wantRecordCount {
				t.Fatalf("record count = %d, want %d", len(records), tc.wantRecordCount)
			}
			if tc.wantRecordCount > 0 {
				assertCapturedRecord(t, records[0], tc)
			}
		})
	}
}

func TestOTelLogProxy_DefaultsAndNilTurns(t *testing.T) {
	t.Parallel()
	proxy := mcp.NewOTelLogProxy(mcp.OTelLogProxyConfig{Stack: "default-stack"})
	mux := http.NewServeMux()
	proxy.Register(mux)

	req := httptest.NewRequest(http.MethodPost, env.OTelLogsPathPrefix, strings.NewReader(`{"resourceLogs": [{"scopeLogs": [{"logRecords": [{"body": {"stringValue": "log with nil turns"}}]}]}]}`))
	req.RemoteAddr = "127.0.0.1:4000"
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
