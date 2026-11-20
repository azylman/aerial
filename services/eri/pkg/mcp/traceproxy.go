package mcp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/guard"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

const (
	// AnnouncementTTL is how long an agy hook announcement can wait for its MCP request.
	AnnouncementTTL = 30 * time.Second
	// AnnouncementCap bounds the announcement store; the oldest entries are dropped past it.
	AnnouncementCap = 1024

	maxProxyRequestBytes   = 32 << 20
	maxAnnouncementBytes   = 64 << 10
	attrMCPServer          = attribute.Key("aerial.mcp.server")
	attrGenAIToolCallID    = attribute.Key("gen_ai.tool.call.id")
	claudeToolUseIDMetaKey = "claudecode/toolUseId"
	attrHTTPResponseStatus = attribute.Key("http.response.status_code")
	mcpMethodToolsCall     = "tools/call"
	traceProxyTracerScope  = "github.com/azylman/aerial/services/eri/pkg/mcp"
	traceProxyRoutePattern = env.MCPProxyPathPrefix + "{target}/{server}"
	traceAnnounceRoute     = "POST " + env.MCPProxyPathPrefix + "announce"

	defaultServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	tokenCacheTTL                  = 60 * time.Second
)

// UpstreamResolver returns the original URL of a target's MCP server (env.MCPProxyRoutes).
type UpstreamResolver interface {
	Upstream(target, server string) (string, bool)
}

// ActiveTurns resolves daemons to the span of the turn they are executing and to the
// execute_tool span behind an MCP tool call (runner.ActiveTurnRegistry).
type ActiveTurns interface {
	Get(daemonID string) (trace.SpanContext, bool)
	Target(daemonID string) (string, bool)
	DaemonByConversation(conversationID string) (daemonID, target string, ok bool)
	SoleDaemonInTarget(target string) (daemonID string, ok bool)
	ToolSpan(ctx context.Context, daemonID, server, tool, callID string) (trace.SpanContext, bool)
	Turn(daemonID string) (runner.ActiveTurn, trace.SpanContext, bool)
}

type announcement struct {
	target, server, tool, daemonID string
	at                             time.Time
}

// AnnouncementStore holds agy hook announcements ("daemon D is about to call server/tool")
// until the matching proxied request consumes them. Entries expire after ttl and the store
// keeps at most capacity entries. It is safe for concurrent use.
type AnnouncementStore struct {
	ttl      time.Duration
	capacity int

	mu      sync.Mutex
	entries []announcement // oldest first
}

// NewAnnouncementStore returns an empty store.
func NewAnnouncementStore(ttl time.Duration, capacity int) *AnnouncementStore {
	return &AnnouncementStore{ttl: ttl, capacity: max(capacity, 1)}
}

// Record stores an announcement stamped now.
func (s *AnnouncementStore) Record(target, server, tool, daemonID string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	s.entries = append(s.entries, announcement{target: target, server: server, tool: tool, daemonID: daemonID, at: now})
	if over := len(s.entries) - s.capacity; over > 0 {
		s.entries = append(s.entries[:0:0], s.entries[over:]...)
	}
}

// Take consumes the oldest unexpired announcement for (target, server, tool) and returns its daemon.
func (s *AnnouncementStore) Take(target, server, tool string, now time.Time) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	for i, a := range s.entries {
		if a.target == target && a.server == server && a.tool == tool {
			s.entries = append(s.entries[:i], s.entries[i+1:]...)
			return a.daemonID, true
		}
	}
	return "", false
}

func (s *AnnouncementStore) pruneLocked(now time.Time) {
	keep := 0
	for keep < len(s.entries) && now.Sub(s.entries[keep].at) >= s.ttl {
		keep++
	}
	if keep > 0 {
		s.entries = append(s.entries[:0:0], s.entries[keep:]...)
	}
}

// TraceProxyConfig wires a TraceProxy.
type TraceProxyConfig struct {
	Upstreams      UpstreamResolver
	Turns          ActiveTurns
	Announcements  *AnnouncementStore
	Clock          func() time.Time     // nil uses time.Now
	Transport      http.RoundTripper    // nil uses http.DefaultTransport
	TracerProvider trace.TracerProvider // nil uses the global provider
	// AnnouncementOutcomes counts announcements by outcome and via; nil uses
	// metrics.MCPTraceAnnouncementsTotal.
	AnnouncementOutcomes *prometheus.CounterVec
	TokenPath            string
}

// TraceProxy is eri's loopback MCP proxy. Agents' HTTP MCP servers point at it when tracing
// is enabled; it parents each agent-initiated JSON-RPC request (one carrying a method) under
// the calling daemon's active turn span and injects traceparent. Methodless traffic (ping
// replies, empty bodies, the SSE GET stream) gets no span. Every request is
// forwarded unchanged (headers, body, streamed response) to the upstream URL held in memory.
type TraceProxy struct {
	upstreams     UpstreamResolver
	turns         ActiveTurns
	announcements *AnnouncementStore
	now           func() time.Time
	transport     http.RoundTripper
	tracer        trace.Tracer
	outcomes      *prometheus.CounterVec
	tokenPath     string
	tokenMu       sync.Mutex
	cachedToken   string
	tokenExpiry   time.Time
}

// NewTraceProxy returns a proxy over cfg.
func NewTraceProxy(cfg TraceProxyConfig) *TraceProxy {
	tokenPath := strings.TrimSpace(cfg.TokenPath)
	if tokenPath == "" {
		tokenPath = defaultServiceAccountTokenPath
	}
	p := &TraceProxy{
		upstreams:     cfg.Upstreams,
		turns:         cfg.Turns,
		announcements: cfg.Announcements,
		now:           cfg.Clock,
		transport:     cfg.Transport,
		outcomes:      cfg.AnnouncementOutcomes,
		tokenPath:     tokenPath,
	}
	if p.outcomes == nil {
		p.outcomes = metrics.MCPTraceAnnouncementsTotal
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.transport == nil {
		p.transport = http.DefaultTransport
	}
	if p.announcements == nil {
		p.announcements = NewAnnouncementStore(AnnouncementTTL, AnnouncementCap)
	}
	tp := cfg.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	p.tracer = tp.Tracer(traceProxyTracerScope)
	return p
}

// Register mounts the loopback-only proxy and announce routes on mux.
func (p *TraceProxy) Register(mux *http.ServeMux) {
	mux.Handle(traceAnnounceRoute, httpkit.LoopbackOnly(http.HandlerFunc(p.serveAnnounce)))
	mux.Handle(traceProxyRoutePattern, httpkit.LoopbackOnly(http.HandlerFunc(p.serveProxy)))
}

func (p *TraceProxy) token() string {
	if p.tokenPath == "" {
		return ""
	}
	p.tokenMu.Lock()
	defer p.tokenMu.Unlock()
	now := p.now()
	if !p.tokenExpiry.IsZero() && now.Before(p.tokenExpiry) {
		return p.cachedToken
	}
	p.tokenExpiry = now.Add(tokenCacheTTL)
	data, err := os.ReadFile(p.tokenPath)
	if err != nil {
		p.cachedToken = ""
		return ""
	}
	p.cachedToken = strings.TrimSpace(string(data))
	return p.cachedToken
}

// Announcement outcomes and daemon resolutions counted by serveAnnounce.
const (
	announceRecorded  = "recorded"
	announceDropped   = "dropped"
	announceMalformed = "malformed"

	announceViaDaemon       = "daemon"
	announceViaConversation = "conversation"
	announceViaNone         = "none"
)

// serveAnnounce records an agy hook announcement under the announcing daemon's target. The
// daemon comes from daemon_id or, when that is empty or unknown, from conversation_id; an
// announcement no active turn claims is dropped. Every outcome is counted, so whether agy
// passes its env to hook subprocesses (and which ID resolves the daemon) is observable.
func (p *TraceProxy) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	var a guard.TraceAnnouncement
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAnnouncementBytes)).Decode(&a); err != nil {
		p.outcomes.WithLabelValues(announceMalformed, announceViaNone).Inc()
		http.Error(w, "malformed announcement", http.StatusBadRequest)
		return
	}
	if a.Server == "" || a.Tool == "" {
		p.outcomes.WithLabelValues(announceMalformed, announceViaNone).Inc()
		w.WriteHeader(http.StatusAccepted)
		return
	}
	daemonID, via := a.DaemonID, announceViaDaemon
	target, ok := p.turns.Target(daemonID)
	if !ok {
		daemonID, target, ok = p.turns.DaemonByConversation(a.ConversationID)
		via = announceViaConversation
	}
	if !ok {
		p.outcomes.WithLabelValues(announceDropped, announceViaNone).Inc()
		w.WriteHeader(http.StatusAccepted)
		return
	}
	p.announcements.Record(target, a.Server, a.Tool, daemonID, p.now())
	p.outcomes.WithLabelValues(announceRecorded, via).Inc()
	w.WriteHeader(http.StatusNoContent)
}

func (*TraceProxy) shapeResponse(ctx context.Context, resp *http.Response, rpcMethod, tool string, reqBody []byte, policy env.ServerPolicy) error {
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		upstreamBody := resp.Body
		pr, pw := io.Pipe()
		resp.Body = pr
		resp.ContentLength = -1
		resp.Header.Del("Content-Length")
		resp.Header.Del("Transfer-Encoding")
		go func() {
			defer upstreamBody.Close()
			if err := ShapeSSEStream(ctx, upstreamBody, pw, rpcMethod, tool, reqBody, policy); err != nil {
				if closeErr := pw.CloseWithError(err); closeErr != nil {
					return
				}
				return
			}
			if closeErr := pw.Close(); closeErr != nil {
				return
			}
		}()
		return nil
	}

	limited := io.LimitReader(resp.Body, maxProxyRequestBytes+1)
	respBytes, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if len(respBytes) > maxProxyRequestBytes {
		resp.Body = io.NopCloser(bytes.NewReader(respBytes))
		return nil
	}
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, gzErr := gzip.NewReader(bytes.NewReader(respBytes))
		if gzErr == nil {
			decompressed, readErr := io.ReadAll(io.LimitReader(gz, maxProxyRequestBytes))
			if closeErr := gz.Close(); closeErr == nil && readErr == nil {
				respBytes = decompressed
				resp.Header.Del("Content-Encoding")
			}
		}
	}
	// Runtime filter errors (e.g. unexpected schema variation or iterator error)
	// deliberately fail open to avoid hard-failing an agent turn over a cosmetic shaping issue.
	out, shapeErr := ShapeResponseBody(ctx, respBytes, resp.Header.Get("Content-Type"), rpcMethod, tool, reqBody, policy)
	if shapeErr != nil || len(out) == 0 {
		out = respBytes
	}
	resp.Body = io.NopCloser(bytes.NewReader(out))
	resp.ContentLength = int64(len(out))
	resp.Header.Set("Content-Length", strconv.Itoa(len(out)))
	resp.Header.Del("Transfer-Encoding")
	return nil
}

func (p *TraceProxy) clientSpanParent(ctx context.Context, target, server, method, tool string, headers http.Header, body []byte, attrs []attribute.KeyValue) (trace.SpanContext, []attribute.KeyValue, bool) {
	if method == "" {
		return trace.SpanContext{}, nil, false
	}
	parent, callID, ok := p.parentSpan(ctx, target, server, headers.Get(env.DaemonHeader), method, tool, body)
	if !ok {
		parent, ok = inboundSpanContext(ctx, headers)
	}
	if !ok {
		return trace.SpanContext{}, nil, false
	}
	attrs = append(attrs, attrMCPServer.String(server))
	if callID != "" {
		attrs = append(attrs, attrGenAIToolCallID.String(callID))
	}
	return parent, attrs, true
}

func (p *TraceProxy) serveProxy(w http.ResponseWriter, r *http.Request) {
	target, server := r.PathValue("target"), r.PathValue("server")
	upstreamURL, ok := p.upstreams.Upstream(target, server)
	if !ok {
		http.Error(w, "unknown MCP server", http.StatusNotFound)
		return
	}
	upstream, err := url.Parse(upstreamURL)
	if err != nil {
		http.Error(w, "invalid MCP upstream", http.StatusBadGateway)
		return
	}

	var policy env.ServerPolicy
	var hasPolicy bool
	if pr, ok := p.upstreams.(PolicyResolver); ok {
		policy, hasPolicy = pr.Policy(target, server)
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProxyRequestBytes))
	if err != nil {
		http.Error(w, "unreadable request body", http.StatusBadRequest)
		return
	}

	if checkBlockedToolCall(w, body, policy, hasPolicy, r.Method) {
		return
	}

	attrs := tracing.MCPRequestAttrs(body)
	method, tool := mcpMethodAndTool(attrs)

	ctx := r.Context()
	var span trace.Span
	if parent, spanAttrs, ok := p.clientSpanParent(ctx, target, server, method, tool, r.Header, body, attrs); ok {
		ctx, span = p.tracer.Start(trace.ContextWithSpanContext(ctx, parent), "mcp "+server+" "+method,
			trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(spanAttrs...))
		defer span.End()
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) { //nolint:contextcheck // pr.Out.Context() is the inbound request's context
			p.rewriteRequest(pr, upstream, policy, span)
		},
		Transport:     p.transport,
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			if span != nil {
				span.SetAttributes(attrHTTPResponseStatus.Int(resp.StatusCode))
				if resp.StatusCode >= http.StatusInternalServerError {
					span.SetStatus(codes.Error, "")
				}
			}
			if resp.StatusCode == http.StatusOK && needsResponseShaping(r.Method, method, tool, policy) {
				return p.shapeResponse(ctx, resp, method, tool, body, policy)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			if span != nil && !errors.Is(err, r.Context().Err()) {
				span.SetStatus(codes.Error, "upstream request failed")
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	out := r.WithContext(ctx)
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	rp.ServeHTTP(w, out)
}

func checkBlockedToolCall(w http.ResponseWriter, body []byte, policy env.ServerPolicy, hasPolicy bool, method string) bool {
	if !hasPolicy || method != http.MethodPost {
		return false
	}
	blocked, reqID, toolName := IsBlockedToolCall(body, policy)
	if !blocked {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(BuildJSONRPCErrorResponse(reqID, -32601, fmt.Sprintf("Tool %q is disabled.", toolName))); err != nil {
		return true
	}
	return true
}

func (p *TraceProxy) rewriteRequest(pr *httputil.ProxyRequest, upstream *url.URL, policy env.ServerPolicy, span trace.Span) {
	u := *upstream
	pr.Out.URL = &u
	pr.Out.Host = ""
	pr.Out.Header.Del(env.DaemonHeader)
	if hasActivePolicy(policy) {
		pr.Out.Header.Del("Accept-Encoding")
	}
	if tok := p.token(); tok != "" && pr.Out.Header.Get("Authorization") == "" && IsClusterInternalUpstream(upstream) {
		pr.Out.Header.Set("Authorization", "Bearer "+tok)
	}
	if span != nil {
		propagation.TraceContext{}.Inject(pr.Out.Context(), propagation.HeaderCarrier(pr.Out.Header))
	}
}

// parentSpan resolves the span a proxied request's client span starts under: for tools/call,
// the execute_tool span of the tool call that made it (claimed from the owning daemon's turn,
// waiting briefly when the engine has not announced the tool yet), otherwise or when no span
// matches, the owning turn's root span. callID is Claude Code's tool_use ID, when sent.
func (p *TraceProxy) parentSpan(ctx context.Context, target, server, daemonHeader, method, tool string, body []byte) (parent trace.SpanContext, callID string, ok bool) {
	daemonID, turnSpan, ok := p.owner(target, server, daemonHeader, method, tool)
	if !ok {
		return trace.SpanContext{}, "", false
	}
	if method != mcpMethodToolsCall {
		return turnSpan, "", true
	}
	callID = mcpToolCallID(body)
	if sc, ok := p.turns.ToolSpan(ctx, daemonID, server, tool, callID); ok {
		return sc, callID, true
	}
	return turnSpan, callID, true
}

// inboundSpanContext returns the valid W3C traceparent (with tracestate) an MCP client sent
// in h, if any; ctx is the proxy route's request context, which carries no span of its own. Long-lived daemons carry their daemon.spawn context, so this parents a call
// under the spawn trace when no turn claims it.
func inboundSpanContext(ctx context.Context, h http.Header) (trace.SpanContext, bool) {
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(ctx, propagation.HeaderCarrier(h)))
	return sc, sc.IsValid()
}

// owner resolves the daemon behind a proxied request and its turn span. Claude Code names its
// daemon in X-Eri-Daemon. agy sends no identity, so a tools/call consumes the oldest
// announcement its hook made for (target, server, tool); otherwise the request belongs to the
// target's turn only when exactly one daemon in the target is executing one.
func (p *TraceProxy) owner(target, server, daemonID, method, tool string) (string, trace.SpanContext, bool) {
	if daemonID == "" && method == mcpMethodToolsCall && tool != "" {
		if announced, ok := p.announcements.Take(target, server, tool, p.now()); ok {
			if sc, ok := p.turns.Get(announced); ok {
				return announced, sc, true
			}
		}
	}
	if daemonID == "" {
		sole, ok := p.turns.SoleDaemonInTarget(target)
		if !ok {
			return "", trace.SpanContext{}, false
		}
		daemonID = sole
	}
	sc, ok := p.turns.Get(daemonID)
	return daemonID, sc, ok
}

// mcpToolCallID returns the tool_use ID Claude Code sends in a tools/call request's
// params._meta, or "" when absent.
func mcpToolCallID(body []byte) string {
	var req struct {
		Params struct {
			Meta map[string]any `json:"_meta"`
		} `json:"params"`
	}
	if json.Unmarshal(body, &req) != nil {
		return ""
	}
	if id, ok := req.Params.Meta[claudeToolUseIDMetaKey].(string); ok {
		return id
	}
	return ""
}

func mcpMethodAndTool(attrs []attribute.KeyValue) (method, tool string) {
	for _, kv := range attrs {
		switch kv.Key {
		case tracing.AttrMCPMethodName:
			method = kv.Value.AsString()
		case tracing.AttrGenAIToolName:
			tool = kv.Value.AsString()
		}
	}
	return method, tool
}

// IsClusterInternalUpstream reports whether an upstream URL points to a cluster-internal
// service (e.g. .aerial, .lan, .local, .cluster.local, localhost, or loopback IP) that
// may receive the Kubernetes ServiceAccount token. External or third-party upstreams return false.
func IsClusterInternalUpstream(u *url.URL) bool {
	if u == nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	if host == "aerial" || strings.HasSuffix(host, ".aerial") ||
		host == "lan" || strings.HasSuffix(host, ".lan") ||
		host == "local" || strings.HasSuffix(host, ".local") ||
		host == "cluster.local" || strings.HasSuffix(host, ".cluster.local") {
		return true
	}
	return false
}
