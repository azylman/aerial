package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/guard"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	// AnnouncementTTL is how long an agy hook announcement can wait for its MCP request.
	AnnouncementTTL = 30 * time.Second
	// AnnouncementCap bounds the announcement store; the oldest entries are dropped past it.
	AnnouncementCap = 1024

	maxProxyRequestBytes   = 32 << 20
	maxAnnouncementBytes   = 64 << 10
	attrMCPServer          = attribute.Key("aerial.mcp.server")
	attrHTTPResponseStatus = attribute.Key("http.response.status_code")
	mcpMethodToolsCall     = "tools/call"
	traceProxyTracerScope  = "github.com/azylman/aerial/services/eri/pkg/mcp"
	traceProxyRoutePattern = env.MCPProxyPathPrefix + "{target}/{server}"
	traceAnnounceRoute     = "POST " + env.MCPProxyPathPrefix + "announce"
)

// UpstreamResolver returns the original URL of a target's MCP server (env.MCPProxyRoutes).
type UpstreamResolver interface {
	Upstream(target, server string) (string, bool)
}

// ActiveTurns resolves daemons to the span of the turn they are executing
// (runner.ActiveTurnRegistry).
type ActiveTurns interface {
	Get(daemonID string) (trace.SpanContext, bool)
	Target(daemonID string) (string, bool)
	DaemonByConversation(conversationID string) (daemonID, target string, ok bool)
	SoleTurnInTarget(target string) (trace.SpanContext, bool)
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
}

// TraceProxy is eri's loopback MCP proxy. Agents' HTTP MCP servers point at it when tracing
// is enabled; it parents each request under the calling daemon's active turn span, injects
// traceparent and forwards the request unchanged (headers, body, streamed response) to the
// upstream URL held in memory.
type TraceProxy struct {
	upstreams     UpstreamResolver
	turns         ActiveTurns
	announcements *AnnouncementStore
	now           func() time.Time
	transport     http.RoundTripper
	tracer        trace.Tracer
}

// NewTraceProxy returns a proxy over cfg.
func NewTraceProxy(cfg TraceProxyConfig) *TraceProxy {
	p := &TraceProxy{
		upstreams:     cfg.Upstreams,
		turns:         cfg.Turns,
		announcements: cfg.Announcements,
		now:           cfg.Clock,
		transport:     cfg.Transport,
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

// serveAnnounce records an agy hook announcement under the announcing daemon's target. The
// daemon comes from daemon_id or, when that is empty or unknown, from conversation_id; an
// announcement no active turn claims is dropped.
func (p *TraceProxy) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	var a guard.TraceAnnouncement
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAnnouncementBytes)).Decode(&a); err != nil {
		http.Error(w, "malformed announcement", http.StatusBadRequest)
		return
	}
	daemonID := a.DaemonID
	target, ok := p.turns.Target(daemonID)
	if !ok {
		daemonID, target, ok = p.turns.DaemonByConversation(a.ConversationID)
	}
	if !ok || a.Server == "" || a.Tool == "" {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	p.announcements.Record(target, a.Server, a.Tool, daemonID, p.now())
	w.WriteHeader(http.StatusNoContent)
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
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProxyRequestBytes))
	if err != nil {
		http.Error(w, "unreadable request body", http.StatusBadRequest)
		return
	}
	attrs := tracing.MCPRequestAttrs(body)
	method, tool := mcpMethodAndTool(attrs)

	ctx := r.Context()
	var span trace.Span
	if sc, ok := p.owner(target, server, r.Header.Get(env.DaemonHeader), method, tool); ok {
		name := method
		if name == "" {
			name = r.Method
		}
		ctx, span = p.tracer.Start(trace.ContextWithSpanContext(ctx, sc), "mcp "+server+" "+name,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(append(attrs, attrMCPServer.String(server))...))
		defer span.End()
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			u := *upstream
			pr.Out.URL = &u
			pr.Out.Host = ""
			pr.Out.Header.Del(env.DaemonHeader)
			if span != nil {
				propagation.TraceContext{}.Inject(pr.Out.Context(), propagation.HeaderCarrier(pr.Out.Header))
			}
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

// owner resolves the span of the turn behind a proxied request. Claude Code names its daemon
// in X-Eri-Daemon. agy sends no identity, so a tools/call consumes the oldest announcement its
// hook made for (target, server, tool); otherwise the request belongs to the target's turn
// only when exactly one daemon in the target is executing one.
func (p *TraceProxy) owner(target, server, daemonID, method, tool string) (trace.SpanContext, bool) {
	if daemonID != "" {
		return p.turns.Get(daemonID)
	}
	if method == mcpMethodToolsCall && tool != "" {
		if announced, ok := p.announcements.Take(target, server, tool, p.now()); ok {
			if sc, ok := p.turns.Get(announced); ok {
				return sc, true
			}
		}
	}
	return p.turns.SoleTurnInTarget(target)
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
