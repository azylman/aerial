package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server encapsulates the Prospera HTTP router, webhook handlers, and Streamable HTTP MCP server.
type Server struct {
	cfg           *Config
	store         Store
	twilio        TwilioClient
	tokenMgr      *StreamTokenManager
	sessionMgr    *CallSessionManager
	reporter      *Reporter
	toolHandler   *ToolHandler
	mcpServer     *mcp.Server
	streamHandler http.Handler
	bgWg          sync.WaitGroup
}

// Wait blocks until all in-flight background webhook tasks (reports, voicemails) finish.
func (s *Server) Wait() {
	s.bgWg.Wait()
}

// NewServer initializes a new Server.
func NewServer(
	cfg *Config,
	store Store,
	twilio TwilioClient,
	tokenMgr *StreamTokenManager,
	sessionMgr *CallSessionManager,
	reporter *Reporter,
) *Server {
	if cfg == nil {
		panic("server: config cannot be nil")
	}

	toolHandler := NewToolHandler(cfg, store, twilio, sessionMgr, reporter)
	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "prospera",
		Version: "1.0.0",
	}, nil)
	RegisterProsperaMCPTools(mcpServer, toolHandler)

	streamHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	})

	return &Server{
		cfg:           cfg,
		store:         store,
		twilio:        twilio,
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
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("prospera", false))

	mcpBridge := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpkit.ServeMCPStream(w, r, s.streamHandler, "Prospera Voice Subsystem MCP Server Ready\n", "", "")
	})
	mux.Handle("/mcp", mcpBridge)

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

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "prospera",
		"port":    s.cfg.Port,
	})
}

func (s *Server) validateTwilioRequest(w http.ResponseWriter, r *http.Request) bool {
	fullURL := strings.TrimRight(s.cfg.PublicWebhookBaseURL, "/") + r.URL.RequestURI()
	sig := r.Header.Get("X-Twilio-Signature")
	if !ValidateTwilioSignature(s.cfg.TwilioAuthToken, fullURL, r.PostForm, sig) {
		http.Error(w, "invalid twilio signature", http.StatusForbidden)
		return false
	}
	return true
}

func writeXML(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(content)); err != nil {
		log.Printf("server: write xml response: %v", err)
	}
}

// resolveAndBindCall resolves a VoiceCall by provider CallSid, falling back to call_id query or form param.
// If found via call_id and ProviderCallSID is empty, it binds callSid.
func (s *Server) resolveAndBindCall(ctx context.Context, r *http.Request, callSid string) *VoiceCall {
	if s.store == nil {
		return nil
	}
	if callSid != "" {
		call, err := s.store.GetCallByProviderSID(ctx, callSid)
		if err == nil && call != nil {
			return call
		}
	}
	callID := r.URL.Query().Get("call_id")
	if callID == "" {
		callID = r.PostForm.Get("call_id")
	}
	if callID == "" {
		return nil
	}
	call, err := s.store.GetCall(ctx, callID)
	if err != nil || call == nil {
		return nil
	}
	if call.ProviderCallSID == "" && callSid != "" {
		if bindErr := s.store.BindProviderCallSID(ctx, call.CallID, callSid); bindErr != nil {
			log.Printf("server: bind provider sid: %v", bindErr)
		}
	}
	return call
}

func (s *Server) handleTwilioVoice(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if !s.validateTwilioRequest(w, r) {
		return
	}

	callSid := r.PostForm.Get("CallSid")
	direction := r.PostForm.Get("Direction")

	existingCall := s.resolveAndBindCall(r.Context(), r, callSid)

	if direction == "outbound-api" || (existingCall != nil && existingCall.Direction == "outbound") {
		s.handleOutboundVoiceConnect(w, existingCall)
		return
	}

	s.handleInboundVoiceRouting(w, r, callSid)
}

func (s *Server) handleOutboundVoiceConnect(w http.ResponseWriter, existingCall *VoiceCall) {
	if existingCall == nil || existingCall.CallID == "" {
		http.Error(w, "outbound call not found", http.StatusNotFound)
		return
	}
	token, err := s.tokenMgr.Issue(existingCall.CallID, 30*time.Second)
	if err != nil {
		http.Error(w, "failed to issue stream token", http.StatusInternalServerError)
		return
	}
	wsURL := fmt.Sprintf("%s/api/webhooks/twilio/stream/%s", toWSS(s.cfg.PublicWebhookBaseURL), token)
	twiml := GenerateConnectStreamTwiML(wsURL)
	writeXML(w, twiml)
}

func (s *Server) handleInboundVoiceRouting(w http.ResponseWriter, r *http.Request, callSid string) {
	fromRaw := r.PostForm.Get("From")
	toRaw := r.PostForm.Get("To")
	normFrom, errFrom := NormalizeE164(fromRaw)
	normTo, errTo := NormalizeE164(toRaw)
	if errFrom != nil || errTo != nil {
		http.Error(w, "invalid phone numbers", http.StatusBadRequest)
		return
	}

	// Tier 1: AllowFrom allowlist
	for _, a := range s.cfg.AllowFrom {
		if normA, err := NormalizeE164(a); err == nil && normA == normFrom {
			s.connectInboundStream(w, r.Context(), callSid, normFrom, normTo, nil)
			return
		}
	}

	// Tier 2: 7-day callback window
	window := time.Duration(s.cfg.CallbackWindowMinutes) * time.Minute
	if window <= 0 {
		window = 10080 * time.Minute
	}
	var recentCall *VoiceCall
	if s.store != nil {
		var findErr error
		recentCall, findErr = s.store.FindRecentOutboundCall(r.Context(), normFrom, window)
		if findErr != nil {
			log.Printf("server: find recent outbound call: %v", findErr)
		}
	}
	if recentCall != nil {
		s.connectInboundStream(w, r.Context(), callSid, normFrom, normTo, recentCall)
		return
	}

	// Tier 3: Unknown caller -> Voicemail recording
	s.routeVoicemailRecording(w, r.Context(), callSid, normFrom, normTo)
}

func (s *Server) connectInboundStream(w http.ResponseWriter, ctx context.Context, callSid, normFrom, normTo string, callbackOf *VoiceCall) {
	callID := "call-in-" + uuid.NewString()
	c := VoiceCall{
		CallID:           callID,
		ProviderCallSID:  callSid,
		Direction:        "inbound",
		State:            "in-progress",
		FromNumber:       normFrom,
		ToNumber:         normTo,
		DiscordChannelID: s.cfg.DefaultDiscordChannel,
		ReportStatus:     "pending",
		StartedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	if callbackOf != nil {
		c.CallbackOfCallID = callbackOf.CallID
		c.Brief = CallBrief{
			Task: fmt.Sprintf("Take a message or update regarding prior call %s (%s). Do not commit to new terms beyond original approvals.",
				callbackOf.CallID, callbackOf.Brief.Task),
			Context:   fmt.Sprintf("Prior call context: %s", callbackOf.Brief.Context),
			Approvals: callbackOf.Brief.Approvals,
		}
	} else {
		c.Brief = CallBrief{
			Task:      "Greet the caller professionally as Prospera, take their message, and offer to have the account owner return their call.",
			Context:   fmt.Sprintf("Inbound call from allowlisted contact %s.", normFrom),
			Approvals: defaultApprovalFirewall,
		}
	}

	if s.store != nil {
		if createErr := s.store.CreateInboundCall(ctx, c); createErr != nil {
			log.Printf("server: create inbound call: %v", createErr)
		}
	}
	token, issueErr := s.tokenMgr.Issue(callID, 30*time.Second)
	if issueErr != nil {
		http.Error(w, "failed to issue stream token", http.StatusInternalServerError)
		return
	}
	wsURL := fmt.Sprintf("%s/api/webhooks/twilio/stream/%s", toWSS(s.cfg.PublicWebhookBaseURL), token)
	twiml := GenerateConnectStreamTwiML(wsURL)
	writeXML(w, twiml)
}

func (s *Server) routeVoicemailRecording(w http.ResponseWriter, ctx context.Context, callSid, normFrom, normTo string) {
	callID := "call-in-" + uuid.NewString()
	c := VoiceCall{
		CallID:           callID,
		ProviderCallSID:  callSid,
		Direction:        "inbound",
		State:            "in-progress",
		FromNumber:       normFrom,
		ToNumber:         normTo,
		DiscordChannelID: s.cfg.DefaultDiscordChannel,
		ReportStatus:     "awaiting_recording",
		StartedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	if s.store != nil {
		if createErr := s.store.CreateInboundCall(ctx, c); createErr != nil {
			log.Printf("server: create inbound voicemail call: %v", createErr)
		}
	}
	greeting := "Please leave a message after the tone."
	vmCallbackURL := fmt.Sprintf("%s/api/webhooks/twilio/voicemail?call_id=%s", strings.TrimRight(s.cfg.PublicWebhookBaseURL, "/"), callID)
	twiml := GenerateVoicemailTwiML(greeting, vmCallbackURL)
	writeXML(w, twiml)
}

func (s *Server) handleTwilioStream(w http.ResponseWriter, r *http.Request) {
	if s.sessionMgr != nil {
		s.sessionMgr.HandleMediaWebSocket(w, r)
		return
	}
	http.Error(w, "session manager not available", http.StatusServiceUnavailable)
}

func (s *Server) handleTwilioAMD(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if !s.validateTwilioRequest(w, r) {
		return
	}

	callSid := r.PostForm.Get("CallSid")
	answeredBy := r.PostForm.Get("AnsweredBy")

	if s.sessionMgr != nil && callSid != "" {
		if amdErr := s.sessionMgr.HandleAMDWebhook(callSid, answeredBy); amdErr != nil {
			log.Printf("server: handle amd webhook: %v", amdErr)
		}
	}

	if call := s.resolveAndBindCall(r.Context(), r, callSid); call != nil {
		if updateErr := s.store.UpdateCallState(r.Context(), call.CallID, call.State, "", answeredBy); updateErr != nil {
			log.Printf("server: update amd call state: %v", updateErr)
		}
	}

	writeXML(w, "<Response/>")
}

func isTerminalState(status string) bool {
	switch strings.ToLower(status) {
	case "completed", "busy", "no-answer", "failed", "canceled":
		return true
	default:
		return false
	}
}

func (s *Server) handleTwilioStatus(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if !s.validateTwilioRequest(w, r) {
		return
	}

	callSid := r.PostForm.Get("CallSid")
	status := r.PostForm.Get("CallStatus")

	call := s.resolveAndBindCall(r.Context(), r, callSid)
	if call == nil {
		writeXML(w, "<Response/>")
		return
	}

	if updateErr := s.store.UpdateCallState(r.Context(), call.CallID, status, "", ""); updateErr != nil {
		log.Printf("server: update call state on status webhook: %v", updateErr)
	}

	if isTerminalState(status) && s.reporter != nil {
		s.triggerPostCallReportIfPending(r.Context(), call.CallID)
	}

	writeXML(w, "<Response/>")
}

// triggerPostCallReportIfPending asynchronously dispatches post-call report delivery
// when a terminal Twilio status webhook arrives. The atomic CAS ("pending" -> "processing")
// is performed inside Reporter.DeliverCallReport so the report is not left permanently stuck.
func (s *Server) triggerPostCallReportIfPending(_ context.Context, callID string) {
	s.bgWg.Add(1)
	go func(id string) {
		defer s.bgWg.Done()
		if delivErr := s.reporter.DeliverCallReport(context.Background(), id); delivErr != nil {
			log.Printf("server: deliver call report: %v", delivErr)
		}
	}(callID)
}

func (s *Server) handleTwilioVoicemail(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if !s.validateTwilioRequest(w, r) {
		return
	}

	recURL := r.PostForm.Get("RecordingUrl")
	callSid := r.PostForm.Get("CallSid")
	call := s.resolveAndBindCall(r.Context(), r, callSid)

	if call != nil && recURL != "" && s.reporter != nil {
		s.bgWg.Add(1)
		go func(id, url string) {
			defer s.bgWg.Done()
			if procErr := s.reporter.ProcessVoicemailRecording(context.Background(), id, url); procErr != nil {
				log.Printf("server: process voicemail recording: %v", procErr)
			}
		}(call.CallID, recURL)
	}

	writeXML(w, "<Response/>")
}

func (s *Server) handleTwilioSMS(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if !s.validateTwilioRequest(w, r) {
		return
	}

	msgSid := r.PostForm.Get("MessageSid")
	from := r.PostForm.Get("From")
	to := r.PostForm.Get("To")
	body := r.PostForm.Get("Body")

	normFrom, errFrom := NormalizeE164(from)
	if errFrom != nil {
		normFrom = from
	}
	normTo, errTo := NormalizeE164(to)
	if errTo != nil {
		normTo = to
	}

	numMedia := 0
	if rawMedia := r.PostForm.Get("NumMedia"); rawMedia != "" {
		if n, err := strconv.Atoi(rawMedia); err == nil {
			numMedia = n
		}
	}
	var mediaURLs []string
	for i := 0; i < numMedia; i++ {
		u := r.PostForm.Get(fmt.Sprintf("MediaUrl%d", i))
		if u != "" {
			mediaURLs = append(mediaURLs, u)
		}
	}

	relatedID := ""
	if s.store != nil && normFrom != "" {
		recentCall, findErr := s.store.FindRecentOutboundCall(r.Context(), normFrom, 7*24*time.Hour)
		if findErr == nil && recentCall != nil {
			relatedID = recentCall.CallID
		}
	}

	msg := VoiceMessage{
		MessageSID:       msgSid,
		Direction:        "inbound",
		FromNumber:       normFrom,
		ToNumber:         normTo,
		Body:             body,
		MediaURLs:        mediaURLs,
		RelatedCallID:    relatedID,
		DiscordDelivered: false,
		CreatedAt:        time.Now().UTC(),
	}

	if s.store != nil {
		if _, saveErr := s.store.SaveVoiceMessage(r.Context(), msg); saveErr != nil {
			log.Printf("server: save voice message: %v", saveErr)
		}
	}

	if s.reporter != nil {
		if delivErr := s.reporter.DeliverInboundSMS(r.Context(), msg); delivErr != nil {
			log.Printf("server: deliver inbound sms: %v", delivErr)
		}
	}

	writeXML(w, "<Response/>")
}

func toWSS(rawURL string) string {
	trimmed := strings.TrimSpace(rawURL)
	if strings.HasPrefix(trimmed, "https://") {
		return "wss://" + strings.TrimPrefix(trimmed, "https://")
	}
	if strings.HasPrefix(trimmed, "http://") {
		return "ws://" + strings.TrimPrefix(trimmed, "http://")
	}
	return "wss://" + trimmed
}
