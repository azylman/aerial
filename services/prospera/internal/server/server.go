// Package server owns the Prospera HTTP router, webhook handlers, NATS job
// consumers, and RunApp lifecycle. It sits at L4 and wires L0-L3 packages.
package server

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/delivery"
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/tools"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

const (
	serviceName = "prospera"
	fieldStatus = "status"
)

const (
	callStateFailed     = "failed"
	callStateInProgress = "in-progress"
	directionInbound    = "inbound"
	directionOutbound   = "outbound"
	reportStatusPending = "pending"
)

// Server encapsulates the Prospera HTTP router, webhook handlers, and Streamable HTTP MCP server.
type Server struct {
	cfgSrc        config.ReloadableConfigSource
	store         store.Store
	twilio        twilio.TwilioClient
	tokenMgr      *twilio.StreamTokenManager
	sessionMgr    *mediastream.CallSessionManager
	reporter      *delivery.Reporter
	toolHandler   *tools.ToolHandler
	mcpServer     *mcp.Server
	streamHandler http.Handler
	busMu         sync.RWMutex
	bus           pubsub.Bus
	bgWg          sync.WaitGroup
}

// SetEventBus sets the pubsub.Bus used for durable job queueing.
func (s *Server) SetEventBus(b pubsub.Bus) {
	if s == nil {
		return
	}
	s.busMu.Lock()
	defer s.busMu.Unlock()
	s.bus = b
}

// EventBus returns the current pubsub.Bus instance.
func (s *Server) EventBus() pubsub.Bus {
	if s == nil {
		return nil
	}
	s.busMu.RLock()
	defer s.busMu.RUnlock()
	return s.bus
}

// NewServer initializes a new Server.
func NewServer(
	cfgSrc config.ReloadableConfigSource,
	st store.Store,
	tw twilio.TwilioClient,
	tokenMgr *twilio.StreamTokenManager,
	sessionMgr *mediastream.CallSessionManager,
	reporter *delivery.Reporter,
) *Server {
	if cfgSrc == nil {
		panic("server: config source cannot be nil")
	}

	toolHandler := tools.NewToolHandler(cfgSrc, st, tw, sessionMgr, reporter)
	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    serviceName,
		Version: "1.0.0",
	}, &mcp.ServerOptions{Instructions: tools.ServerInstructions})
	tools.RegisterProsperaMCPTools(mcpServer, toolHandler)

	streamHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	})

	return &Server{
		cfgSrc:        cfgSrc,
		store:         st,
		twilio:        tw,
		tokenMgr:      tokenMgr,
		sessionMgr:    sessionMgr,
		reporter:      reporter,
		toolHandler:   toolHandler,
		mcpServer:     mcpServer,
		streamHandler: streamHandler,
	}
}

// Routes builds and returns the HTTP serve mux for Prospera.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("POST /-/reload", s.cfgSrc.Handler("prospera"))
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("prospera"))

	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		httpkit.ServeMCPStream(w, r, s.streamHandler, "Prospera Voice Subsystem MCP Server Ready\n", "", "")
	})

	mux.HandleFunc("POST /api/webhooks/twilio/voice", s.handleTwilioVoice)
	mux.HandleFunc("POST /twilio/voice", s.handleTwilioVoice)
	mux.HandleFunc("GET /api/webhooks/twilio/stream/{token}", s.handleTwilioStream)
	mux.HandleFunc("GET /api/webhooks/twilio/stream", s.handleTwilioStream)
	mux.HandleFunc("/twilio/media-stream", s.handleTwilioStream)
	mux.HandleFunc("POST /api/webhooks/twilio/amd", s.handleTwilioAMD)
	mux.HandleFunc("POST /twilio/amd", s.handleTwilioAMD)
	mux.HandleFunc("POST /api/webhooks/twilio/status", s.handleTwilioStatus)
	mux.HandleFunc("POST /twilio/status", s.handleTwilioStatus)
	mux.HandleFunc("POST /api/webhooks/twilio/voicemail", s.handleTwilioVoicemail)
	mux.HandleFunc("POST /twilio/voicemail", s.handleTwilioVoicemail)
	mux.HandleFunc("POST /api/webhooks/twilio/sms", s.handleTwilioSMS)
	mux.HandleFunc("POST /twilio/sms", s.handleTwilioSMS)

	// One server span per request for every route; /mcp is not wrapped again.
	return tracing.Middleware("prospera", mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		fieldStatus: "ok",
		"service":   serviceName,
		"port":      s.cfgSrc.Current().Port,
	})
}

// Wait blocks until all in-flight background webhook tasks (reports, voicemails) finish.
func (s *Server) Wait() {
	s.bgWg.Wait()
}

// Drain waits until no live Twilio-Gemini bridge sessions remain. It polls
// the session registry ActiveCount on the 20ms pacer cadence (preserving
// pacer/WAV ring timing) and respects ctx cancellation and deadline. It only
// observes natural call ends; shutdownApp deliberately hangs up leftovers via
// HangupActiveSessions after the drain window expires.
func (s *Server) Drain(ctx context.Context) {
	if s == nil || s.sessionMgr == nil {
		return
	}
	if ctx == nil {
		return
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.sessionMgr.ActiveCount() == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// HangupActiveSessions initiates graceful operator-requested hangup of every
// still-registered bridge session. Shutdown calls it after the natural-drain
// window expires so leftover calls end deliberately instead of dropping
// silently at process exit.
func (s *Server) HangupActiveSessions() {
	if s == nil || s.sessionMgr == nil {
		return
	}
	for _, id := range s.sessionMgr.ActiveCallIDs() {
		if err := s.sessionMgr.EndCall(id); err != nil {
			log.Printf("prospera: shutdown hangup of %s failed: %v", id, err)
		}
	}
}
