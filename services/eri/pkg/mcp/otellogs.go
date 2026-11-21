package mcp

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

var droppedEvents = map[string]bool{
	"user_prompt":             true,
	"assistant_response":      true,
	"agent_response":          true,
	"system_prompt":           true,
	"tool":                    true,
	"tool_result":             true,
	"tool_decision":           true,
	"tool_call_received":      true,
	"tool_result_ready":       true,
	"skill_activated":         true,
	"skill_invocation":        true,
	"hook_execution_start":    true,
	"hook_execution_complete": true,
	"hook_registered":         true,
	"hook_plugin_metrics":     true,
	"sse_event":               true,
	"output_item_added":       true,
}

var sensitiveAttributeKeys = map[string]bool{
	"prompt":                    true,
	"prompt_text":               true,
	"response":                  true,
	"system_prompt":             true,
	"user_system_prompt":        true,
	"tool_parameters":           true,
	"tool_input":                true,
	"tool_result":               true,
	"raw_body":                  true,
	"settings":                  true,
	"managed_settings.settings": true,
}

// OTelLogProxyConfig configures the OTelLogProxy receiver.
type OTelLogProxyConfig struct {
	Turns  ActiveTurns
	Stack  string
	Logger *slog.Logger
}

// OTelLogProxy receives OTLP HTTP/JSON log records from agent subprocesses (Claude Code, Codex)
// and emits structured, trace-correlated slog events.
type OTelLogProxy struct {
	turns  ActiveTurns
	stack  string
	logger *slog.Logger
}

// NewOTelLogProxy returns a new OTelLogProxy.
func NewOTelLogProxy(cfg OTelLogProxyConfig) *OTelLogProxy {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &OTelLogProxy{
		turns:  cfg.Turns,
		stack:  cfg.Stack,
		logger: logger,
	}
}

// Register mounts the loopback OTel log endpoints on mux.
func (p *OTelLogProxy) Register(mux *http.ServeMux) {
	handler := httpkit.LoopbackOnly(http.HandlerFunc(p.serveLogs))
	mux.Handle("POST "+env.OTelLogsPathPrefix, handler)
	mux.Handle("POST "+env.OTelLogsPathPrefix+"/{target}", handler)
}

type otlpAnyValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	IntValue    any      `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
}

func (v *otlpAnyValue) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err == nil {
			v.StringValue = &s
			return nil
		}
	}
	type plain otlpAnyValue
	return json.Unmarshal(data, (*plain)(v))
}

func (v *otlpAnyValue) AsString() string {
	if v == nil {
		return ""
	}
	if v.StringValue != nil {
		return *v.StringValue
	}
	if v.IntValue != nil {
		return fmt.Sprint(v.IntValue)
	}
	if v.DoubleValue != nil {
		return strconv.FormatFloat(*v.DoubleValue, 'f', -1, 64)
	}
	if v.BoolValue != nil {
		return strconv.FormatBool(*v.BoolValue)
	}
	return ""
}

const (
	typeInt      = "int"
	engineClaude = "claude"
	engineCodex  = "codex"
)

func (v *otlpAnyValue) AsScalar() (any, string) {
	if v == nil {
		return nil, ""
	}
	if v.StringValue != nil {
		return *v.StringValue, "string"
	}
	if v.IntValue != nil {
		switch iv := v.IntValue.(type) {
		case float64:
			return int64(iv), typeInt
		case string:
			if n, err := strconv.ParseInt(iv, 10, 64); err == nil {
				return n, typeInt
			}
		case json.Number:
			if n, err := iv.Int64(); err == nil {
				return n, typeInt
			}
		}
		return v.IntValue, typeInt
	}
	if v.DoubleValue != nil {
		return *v.DoubleValue, "double"
	}
	if v.BoolValue != nil {
		return *v.BoolValue, "bool"
	}
	return nil, ""
}

type otlpKeyValue struct {
	Key   string        `json:"key"`
	Value *otlpAnyValue `json:"value"`
}

type otlpLogRecord struct {
	TimeUnixNano         any            `json:"timeUnixNano"`
	ObservedTimeUnixNano any            `json:"observedTimeUnixNano"`
	SeverityNumber       int            `json:"severityNumber"`
	SeverityText         string         `json:"severityText"`
	Body                 *otlpAnyValue  `json:"body"`
	Attributes           []otlpKeyValue `json:"attributes"`
	Flags                int            `json:"flags"`
	TraceID              string         `json:"traceId"`
	SpanID               string         `json:"spanId"`
}

type otlpScope struct {
	Name       string         `json:"name"`
	Version    string         `json:"version"`
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpScopeLogs struct {
	Scope      otlpScope       `json:"scope"`
	LogRecords []otlpLogRecord `json:"logRecords"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpResourceLogs struct {
	Resource  otlpResource    `json:"resource"`
	ScopeLogs []otlpScopeLogs `json:"scopeLogs"`
}

type otlpExportLogsServiceRequest struct {
	ResourceLogs []otlpResourceLogs `json:"resourceLogs"`
}

func resolveLogLevel(sevNum int, sevText string) slog.Level {
	if sevNum >= 17 || strings.EqualFold(sevText, "error") || strings.EqualFold(sevText, "fatal") {
		return slog.LevelError
	}
	if sevNum >= 13 || strings.EqualFold(sevText, "warn") || strings.EqualFold(sevText, "warning") {
		return slog.LevelWarn
	}
	if (sevNum >= 5 && sevNum < 9) || strings.EqualFold(sevText, "debug") || strings.EqualFold(sevText, "trace") {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

func findAttributeValue(attrs []otlpKeyValue, keys ...string) string {
	for _, k := range keys {
		for _, a := range attrs {
			if strings.EqualFold(a.Key, k) {
				return a.Value.AsString()
			}
		}
	}
	return ""
}

func (p *OTelLogProxy) resolveTurn(r *http.Request, record *otlpLogRecord, resAttrs []otlpKeyValue) (runner.ActiveTurn, trace.SpanContext, bool) {
	if p.turns == nil {
		return runner.ActiveTurn{}, trace.SpanContext{}, false
	}

	// 1. check r.Header.Get(env.DaemonHeader) via p.turns.Turn(daemonID)
	if daemonID := strings.TrimSpace(r.Header.Get(env.DaemonHeader)); daemonID != "" {
		if turn, sc, ok := p.turns.Turn(daemonID); ok {
			return turn, sc, true
		}
	}

	// 2. check session.id / conversation.id / thread.id attribute via p.turns.DaemonByConversation(convID) then p.turns.Turn(id)
	convID := findAttributeValue(record.Attributes, "session.id", "conversation.id", "thread.id")
	if convID == "" {
		convID = findAttributeValue(resAttrs, "session.id", "conversation.id", "thread.id")
	}
	if convID != "" {
		if daemonID, _, ok := p.turns.DaemonByConversation(convID); ok && daemonID != "" {
			if turn, sc, ok := p.turns.Turn(daemonID); ok {
				return turn, sc, true
			}
		}
	}

	// 3. check r.PathValue("target") via p.turns.SoleDaemonInTarget(target) then p.turns.Turn(id)
	if target := strings.TrimSpace(r.PathValue("target")); target != "" {
		if daemonID, ok := p.turns.SoleDaemonInTarget(target); ok && daemonID != "" {
			if turn, sc, ok := p.turns.Turn(daemonID); ok {
				return turn, sc, true
			}
		}
	}

	return runner.ActiveTurn{}, trace.SpanContext{}, false
}

func inferEngine(record *otlpLogRecord, resAttrs []otlpKeyValue, scopeName, rawEventName string) string {
	if e := findAttributeValue(record.Attributes, "engine", "agent.engine"); e != "" {
		return e
	}
	if e := findAttributeValue(resAttrs, "engine", "agent.engine"); e != "" {
		return e
	}
	svc := findAttributeValue(resAttrs, "service.name")
	if strings.Contains(svc, engineClaude) {
		return engineClaude
	}
	if strings.Contains(svc, engineCodex) {
		return engineCodex
	}
	if strings.Contains(scopeName, engineClaude) {
		return engineClaude
	}
	if strings.Contains(scopeName, engineCodex) {
		return engineCodex
	}
	if strings.HasPrefix(rawEventName, "claude_code.") {
		return engineClaude
	}
	if strings.HasPrefix(rawEventName, "codex.") {
		return engineCodex
	}
	return ""
}

func (p *OTelLogProxy) serveLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
	var bodyReader io.Reader = r.Body

	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, `{"error":"invalid gzip"}`, http.StatusBadRequest)
			return
		}
		defer func() {
			if closeErr := gz.Close(); closeErr != nil {
				log.Printf("eri: [otellogs] failed closing gzip reader: %v", closeErr)
			}
		}()
		bodyReader = io.LimitReader(gz, 16<<20)
	}

	var req otlpExportLogsServiceRequest
	if err := json.NewDecoder(bodyReader).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	for _, rl := range req.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				p.processRecord(r.Context(), r, &lr, rl.Resource.Attributes, sl.Scope.Name)
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("{}")); err != nil {
		log.Printf("eri: [otellogs] failed writing response: %v", err)
	}
}

func normalizeEvent(lr *otlpLogRecord) (rawName, normName string, hasEventAttr, drop bool) {
	rawName = findAttributeValue(lr.Attributes, "event.name")
	hasEventAttr = rawName != ""
	if rawName == "" && lr.Body != nil {
		rawName = lr.Body.AsString()
	}

	normName = rawName
	if trimmed, ok := strings.CutPrefix(normName, "claude_code."); ok {
		normName = trimmed
	} else if trimmed, ok := strings.CutPrefix(normName, "codex."); ok {
		normName = trimmed
	}

	return rawName, normName, hasEventAttr, droppedEvents[strings.ToLower(normName)]
}

func resolveRecordSpanContext(lr *otlpLogRecord, turnSC trace.SpanContext) trace.SpanContext {
	if turnSC.IsValid() {
		return turnSC
	}
	if len(lr.TraceID) == 32 && len(lr.SpanID) == 16 {
		tid, err1 := trace.TraceIDFromHex(lr.TraceID)
		sid, err2 := trace.SpanIDFromHex(lr.SpanID)
		if err1 == nil && err2 == nil && tid.IsValid() && sid.IsValid() {
			return trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    tid,
				SpanID:     sid,
				TraceFlags: trace.FlagsSampled,
			})
		}
	}
	return trace.SpanContext{}
}

func isReservedAttrKey(k string) bool {
	return k == "event.name" || k == "session.id" || k == "conversation.id" || k == "thread.id" || k == "target" || k == "engine" || k == "stack"
}

func extractScalarAttrs(recordAttrs []otlpKeyValue) []slog.Attr {
	var attrs []slog.Attr
	for _, attr := range recordAttrs {
		lowerKey := strings.ToLower(attr.Key)
		if sensitiveAttributeKeys[lowerKey] || isReservedAttrKey(lowerKey) {
			continue
		}
		val, valType := attr.Value.AsScalar()
		switch valType {
		case "string":
			if s, ok := val.(string); ok {
				attrs = append(attrs, slog.String(attr.Key, s))
			}
		case typeInt:
			if n, ok := val.(int64); ok {
				attrs = append(attrs, slog.Int64(attr.Key, n))
			}
		case "double":
			if f, ok := val.(float64); ok {
				attrs = append(attrs, slog.Float64(attr.Key, f))
			}
		case "bool":
			if b, ok := val.(bool); ok {
				attrs = append(attrs, slog.Bool(attr.Key, b))
			}
		}
	}
	return attrs
}

func extractRecordMessage(lr *otlpLogRecord, normEventName string) string {
	msg := ""
	if lr.Body != nil {
		msg = lr.Body.AsString()
	}
	if msg == "" && normEventName != "" {
		msg = normEventName
	}
	if msg == "" {
		msg = "agent log"
	}
	return msg
}

func (*OTelLogProxy) resolveSessionAndTarget(resolvedTurn runner.ActiveTurn, r *http.Request, lr *otlpLogRecord, resAttrs []otlpKeyValue) (string, string) {
	sessionID := resolvedTurn.ConversationID
	if sessionID == "" {
		sessionID = findAttributeValue(lr.Attributes, "session.id", "conversation.id", "thread.id")
	}
	if sessionID == "" {
		sessionID = findAttributeValue(resAttrs, "session.id", "conversation.id", "thread.id")
	}

	target := resolvedTurn.Target
	if target == "" {
		target = strings.TrimSpace(r.PathValue("target"))
	}
	if target == "" {
		target = findAttributeValue(lr.Attributes, "target")
	}
	return sessionID, target
}

func (p *OTelLogProxy) processRecord(ctx context.Context, r *http.Request, lr *otlpLogRecord, resAttrs []otlpKeyValue, scopeName string) {
	rawEventName, normEventName, hasEventAttr, drop := normalizeEvent(lr)
	if drop {
		return
	}

	resolvedTurn, turnSC, _ := p.resolveTurn(r, lr, resAttrs)
	sc := resolveRecordSpanContext(lr, turnSC)
	if sc.IsValid() {
		ctx = trace.ContextWithSpanContext(ctx, sc)
	}

	level := resolveLogLevel(lr.SeverityNumber, lr.SeverityText)
	msg := extractRecordMessage(lr, normEventName)

	attrs := []slog.Attr{slog.String("source", "agent_otel")}
	if engine := inferEngine(lr, resAttrs, scopeName, rawEventName); engine != "" {
		attrs = append(attrs, slog.String("engine", engine))
	}
	if p.stack != "" {
		attrs = append(attrs, slog.String("stack", p.stack))
	}

	sessionID, target := p.resolveSessionAndTarget(resolvedTurn, r, lr, resAttrs)
	if sessionID != "" {
		attrs = append(attrs, slog.String("session_id", sessionID))
	}
	if target != "" {
		attrs = append(attrs, slog.String("target", target))
	}

	if hasEventAttr || (rawEventName != "" && (strings.HasPrefix(rawEventName, "claude_code.") || strings.HasPrefix(rawEventName, "codex."))) {
		if normEventName != "" {
			attrs = append(attrs, slog.String("event_name", normEventName))
		}
	}

	attrs = append(attrs, extractScalarAttrs(lr.Attributes)...)

	logger := p.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.LogAttrs(ctx, level, msg, attrs...)
}
