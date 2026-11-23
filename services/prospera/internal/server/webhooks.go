package server

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/delivery"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

func validateTwilioRequest(cfg *config.Config, w http.ResponseWriter, r *http.Request) bool {
	fullURL := strings.TrimRight(cfg.PublicWebhookBaseURL, "/") + r.URL.RequestURI()
	sig := r.Header.Get("X-Twilio-Signature")
	if !twilio.ValidateTwilioSignature(cfg.TwilioAuthToken, fullURL, r.PostForm, sig) {
		http.Error(w, "invalid twilio signature", http.StatusForbidden)
		return false
	}
	return true
}

func writeXML(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	//nolint:gosec // G705 false positive: content is server-generated TwiML (literals plus Generate* builders over a fresh random stream token); request-derived IDs never appear verbatim.
	if _, err := w.Write([]byte(content)); err != nil {
		log.Printf("server: write xml response: %v", err)
	}
}

// resolveAndBindCall resolves a VoiceCall by provider CallSid, falling back to call_id query or form param.
// If found via call_id and ProviderCallSID is empty, it binds callSid.
func (s *Server) resolveAndBindCall(ctx context.Context, r *http.Request, callSid string) *model.VoiceCall {
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
			slog.WarnContext(ctx, "server: bind provider sid failed", "call_id", call.CallID, "error", bindErr)
		}
	}
	return call
}

func (s *Server) handleTwilioVoice(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	cfg := s.cfgSrc.Current()
	if !validateTwilioRequest(cfg, w, r) {
		return
	}

	callSid := r.PostForm.Get("CallSid")
	direction := r.PostForm.Get("Direction")

	existingCall := s.resolveAndBindCall(r.Context(), r, callSid)

	if direction == "outbound-api" || (existingCall != nil && existingCall.Direction == directionOutbound) {
		s.handleOutboundVoiceConnect(cfg, w, existingCall)
		return
	}

	s.handleInboundVoiceRouting(cfg, w, r, callSid)
}

func (s *Server) handleOutboundVoiceConnect(cfg *config.Config, w http.ResponseWriter, existingCall *model.VoiceCall) {
	if existingCall == nil || existingCall.CallID == "" {
		http.Error(w, "outbound call not found", http.StatusNotFound)
		return
	}
	token, err := s.tokenMgr.Issue(existingCall.CallID, 30*time.Second)
	if err != nil {
		http.Error(w, "failed to issue stream token", http.StatusInternalServerError)
		return
	}
	wsURL := fmt.Sprintf("%s/api/webhooks/twilio/stream/%s", toWSS(cfg.PublicWebhookBaseURL), token)
	twiml := twilio.GenerateConnectStreamTwiML(wsURL)
	writeXML(w, twiml)
}

func (s *Server) handleInboundVoiceRouting(cfg *config.Config, w http.ResponseWriter, r *http.Request, callSid string) {
	fromRaw := r.PostForm.Get("From")
	toRaw := r.PostForm.Get("To")
	normFrom, errFrom := config.NormalizeE164(fromRaw)
	normTo, errTo := config.NormalizeE164(toRaw)
	if errFrom != nil || errTo != nil {
		http.Error(w, "invalid phone numbers", http.StatusBadRequest)
		return
	}

	// Tier 1: AllowFrom allowlist
	for _, a := range cfg.AllowFrom {
		if normA, err := config.NormalizeE164(a); err == nil && normA == normFrom {
			s.connectInboundStream(r.Context(), cfg, w, callSid, normFrom, normTo, nil)
			return
		}
	}

	// Tier 2: 7-day callback window
	window := time.Duration(cfg.CallbackWindowMinutes) * time.Minute
	if window <= 0 {
		window = 10080 * time.Minute
	}
	var recentCall *model.VoiceCall
	if s.store != nil {
		var findErr error
		recentCall, findErr = s.store.FindRecentOutboundCall(r.Context(), normFrom, window)
		if findErr != nil {
			slog.WarnContext(r.Context(), "server: find recent outbound call failed", "error", findErr)
		}
	}
	if recentCall != nil {
		s.connectInboundStream(r.Context(), cfg, w, callSid, normFrom, normTo, recentCall)
		return
	}

	// Tier 3: Unknown caller -> Voicemail recording
	s.routeVoicemailRecording(r.Context(), cfg, w, callSid, normFrom, normTo)
}

func (s *Server) connectInboundStream(ctx context.Context, cfg *config.Config, w http.ResponseWriter, callSid, normFrom, normTo string, callbackOf *model.VoiceCall) {
	callID := "call-in-" + uuid.NewString()
	c := model.VoiceCall{
		CallID:           callID,
		ProviderCallSID:  callSid,
		Direction:        directionInbound,
		State:            callStateInProgress,
		FromNumber:       normFrom,
		ToNumber:         normTo,
		DiscordChannelID: cfg.DefaultDiscordChannel,
		ReportStatus:     reportStatusPending,
		StartedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	if callbackOf != nil {
		c.CallbackOfCallID = callbackOf.CallID
		c.Brief = model.CallBrief{
			Task:      "Answer the inbound call naturally, listen to what the caller needs, and take a message or update without volunteering prior call details upfront before the caller brings them up. Do not commit to new terms beyond original approvals.",
			Context:   fmt.Sprintf("Inbound callback regarding prior call %s (Prior task: %s; Prior context: %s)", callbackOf.CallID, callbackOf.Brief.Task, callbackOf.Brief.Context),
			Approvals: callbackOf.Brief.Approvals,
		}
	} else {
		c.Brief = model.CallBrief{
			Task:      "Greet the caller professionally, take their message, and offer to have the person they are trying to reach return their call. If asked for your name, say you are Aerial and phrase your role naturally as their assistant (using their name if known from context); only disclose that you are an AI if explicitly asked.",
			Context:   fmt.Sprintf("Inbound call from allowlisted contact %s.", normFrom),
			Approvals: model.DefaultApprovalFirewall,
		}
	}

	if s.store != nil {
		if createErr := s.store.CreateInboundCall(ctx, c); createErr != nil {
			slog.ErrorContext(ctx, "server: create inbound call failed", "call_id", c.CallID, "error", createErr)
		}
	}
	token, issueErr := s.tokenMgr.Issue(callID, 30*time.Second)
	if issueErr != nil {
		http.Error(w, "failed to issue stream token", http.StatusInternalServerError)
		return
	}
	wsURL := fmt.Sprintf("%s/api/webhooks/twilio/stream/%s", toWSS(cfg.PublicWebhookBaseURL), token)
	twiml := twilio.GenerateConnectStreamTwiML(wsURL)
	writeXML(w, twiml)
}

func (s *Server) routeVoicemailRecording(ctx context.Context, cfg *config.Config, w http.ResponseWriter, callSid, normFrom, normTo string) {
	callID := "call-in-" + uuid.NewString()
	c := model.VoiceCall{
		CallID:           callID,
		ProviderCallSID:  callSid,
		Direction:        directionInbound,
		State:            callStateInProgress,
		FromNumber:       normFrom,
		ToNumber:         normTo,
		DiscordChannelID: cfg.DefaultDiscordChannel,
		ReportStatus:     "awaiting_recording",
		StartedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	if s.store != nil {
		if createErr := s.store.CreateInboundCall(ctx, c); createErr != nil {
			slog.ErrorContext(ctx, "server: create inbound voicemail call failed", "call_id", c.CallID, "error", createErr)
		}
	}
	greeting := "Please leave a message after the tone."
	vmCallbackURL := fmt.Sprintf("%s/api/webhooks/twilio/voicemail?call_id=%s", strings.TrimRight(cfg.PublicWebhookBaseURL, "/"), callID)
	twiml := twilio.GenerateVoicemailTwiML(greeting, vmCallbackURL)
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
	cfg := s.cfgSrc.Current()
	if !validateTwilioRequest(cfg, w, r) {
		return
	}

	callSid := r.PostForm.Get("CallSid")
	answeredBy := r.PostForm.Get("AnsweredBy")

	call := s.resolveAndBindCall(r.Context(), r, callSid)

	if s.sessionMgr != nil && callSid != "" {
		if amdErr := s.sessionMgr.HandleAMDWebhook(r.Context(), callSid, answeredBy); amdErr != nil {
			slog.WarnContext(r.Context(), "server: handle amd webhook failed", "call_sid", callSid, "error", amdErr)
		}
	}

	if call != nil {
		if updateErr := s.store.UpdateCallState(r.Context(), call.CallID, call.State, "", answeredBy); updateErr != nil {
			slog.ErrorContext(r.Context(), "server: update amd call state failed", "call_id", call.CallID, "error", updateErr)
		}
	}

	writeXML(w, "<Response/>")
}

func isTerminalState(status string) bool {
	switch strings.ToLower(status) {
	case "completed", "busy", "no-answer", callStateFailed, "canceled":
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
	cfg := s.cfgSrc.Current()
	if !validateTwilioRequest(cfg, w, r) {
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
		slog.ErrorContext(r.Context(), "server: update call state on status webhook failed", "call_id", call.CallID, "error", updateErr)
	}

	if isTerminalState(status) && s.reporter != nil {
		s.triggerPostCallReportIfPending(r.Context(), call.CallID)
	}

	writeXML(w, "<Response/>")
}

// triggerPostCallReport enqueues post-call report delivery into Restate ingress.
// If Restate ingress is unreachable or not configured, it falls back to pubsub or synchronous delivery.
func (s *Server) triggerPostCallReport(ctx context.Context, callID string) error {
	cleanID := strings.TrimSpace(callID)
	if cleanID == "" {
		return nil
	}
	cfg := s.cfgSrc.Current()
	if cfg != nil && strings.TrimSpace(cfg.RestateIngressURL) != "" {
		endpoint := fmt.Sprintf("%s/ProsperaReport/%s/DeliverReport/send", strings.TrimRight(cfg.RestateIngressURL, "/"), cleanID)
		idempotencyKey := "prospera:report:" + cleanID
		_, err := restateingress.Post(ctx, s.httpClient, endpoint, idempotencyKey, delivery.DeliverReportRequest{CallID: cleanID}, 1024)
		if err == nil {
			return nil
		}
		slog.WarnContext(ctx, "server: post to restate ingress failed, falling back to legacy delivery", "call_id", cleanID, "error", err)
	}

	if bus := s.EventBus(); bus != nil {
		msgID := "prospera:report:" + cleanID
		args := pubsub.ProsperaReportJobArgs{CallID: cleanID}
		pubErr := bus.Publish(ctx, pubsub.SubjectProsperaReportJob, msgID, args)
		if pubErr == nil {
			return nil
		}
		slog.WarnContext(ctx, "server: publish report job failed, falling back to goroutine", "call_id", cleanID, "error", pubErr)
	}

	if s.reporter != nil {
		s.bgWg.Add(1)
		go func(id string) {
			defer s.bgWg.Done()
			// The report outlives the request that triggered it.
			if delivErr := s.reporter.DeliverCallReport(context.WithoutCancel(ctx), id); delivErr != nil {
				slog.ErrorContext(ctx, "server: deliver call report failed", "call_id", id, "error", delivErr)
			}
		}(cleanID)
		return nil
	}
	return fmt.Errorf("no report delivery mechanism available for %s", cleanID)
}

// triggerPostCallReportIfPending asynchronously dispatches post-call report delivery
// when a terminal Twilio status webhook arrives. The atomic CAS ("pending" -> "processing")
// is performed inside Reporter.DeliverCallReport so the report is not left permanently stuck.
// When an event bus is configured, the job is published to AERIAL_JOBS with fallback to bgWg.
func (s *Server) triggerPostCallReportIfPending(ctx context.Context, callID string) {
	if err := s.triggerPostCallReport(ctx, callID); err != nil {
		slog.ErrorContext(ctx, "server: trigger post call report failed", "call_id", callID, "error", err)
	}
}

func (s *Server) handleTwilioVoicemail(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	cfg := s.cfgSrc.Current()
	if !validateTwilioRequest(cfg, w, r) {
		return
	}

	recURL := r.PostForm.Get("RecordingUrl")
	callSid := r.PostForm.Get("CallSid")
	call := s.resolveAndBindCall(r.Context(), r, callSid)
	if call == nil || recURL == "" || s.reporter == nil {
		writeXML(w, "<Response/>")
		return
	}

	recSID := r.PostForm.Get("RecordingSid")
	if bus := s.EventBus(); bus != nil {
		msgID := "prospera:vm:" + call.CallID + ":" + recSID
		args := pubsub.ProsperaVoicemailJobArgs{
			CallID:       call.CallID,
			RecordingSID: recSID,
			RecordingURL: recURL,
		}
		pubErr := bus.Publish(r.Context(), pubsub.SubjectProsperaVoicemailJob, msgID, args)
		if pubErr == nil {
			writeXML(w, "<Response/>")
			return
		}
		slog.WarnContext(r.Context(), "server: publish voicemail job failed, falling back to goroutine", "call_id", call.CallID, "error", pubErr)
	}

	// Processing outlives the webhook request that triggered it.
	bgCtx := context.WithoutCancel(r.Context())
	s.bgWg.Add(1)
	go func(id, url string) {
		defer s.bgWg.Done()
		if procErr := s.reporter.ProcessVoicemailRecording(bgCtx, id, url); procErr != nil {
			slog.ErrorContext(bgCtx, "server: process voicemail recording failed", "call_id", id, "error", procErr)
		}
	}(call.CallID, recURL)

	writeXML(w, "<Response/>")
}

func (s *Server) handleTwilioSMS(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	cfg := s.cfgSrc.Current()
	if !validateTwilioRequest(cfg, w, r) {
		return
	}

	msgSid := r.PostForm.Get("MessageSid")
	from := r.PostForm.Get("From")
	to := r.PostForm.Get("To")
	body := r.PostForm.Get("Body")

	normFrom, errFrom := config.NormalizeE164(from)
	if errFrom != nil {
		normFrom = from
	}
	normTo, errTo := config.NormalizeE164(to)
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
	for i := range numMedia {
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

	msg := model.VoiceMessage{
		MessageSID:       msgSid,
		Direction:        directionInbound,
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
			slog.ErrorContext(r.Context(), "server: save voice message failed", "message_sid", msg.MessageSID, "error", saveErr)
		}
	}

	if s.reporter == nil {
		writeXML(w, "<Response/>")
		return
	}

	if bus := s.EventBus(); bus != nil {
		msgID := "prospera:sms:" + strutil.FirstNonEmpty(msgSid, strconv.FormatInt(time.Now().UnixNano(), 10))
		args := pubsub.ProsperaSMSJobArgs{
			MessageSID:    msgSid,
			FromNumber:    normFrom,
			ToNumber:      normTo,
			Body:          body,
			MediaURLs:     mediaURLs,
			RelatedCallID: relatedID,
		}
		pubErr := bus.Publish(r.Context(), pubsub.SubjectProsperaSMSJob, msgID, args)
		if pubErr == nil {
			writeXML(w, "<Response/>")
			return
		}
		slog.WarnContext(r.Context(), "server: publish sms job failed, falling back to goroutine", "message_sid", msgSid, "error", pubErr)
	}

	// Delivery outlives the webhook request that triggered it.
	bgCtx := context.WithoutCancel(r.Context())
	s.bgWg.Add(1)
	go func(vMsg model.VoiceMessage) {
		defer s.bgWg.Done()
		if delivErr := s.reporter.DeliverInboundSMS(bgCtx, vMsg); delivErr != nil {
			slog.ErrorContext(bgCtx, "server: deliver inbound sms failed", "message_sid", vMsg.MessageSID, "error", delivErr)
		}
	}(msg)

	writeXML(w, "<Response/>")
}

func toWSS(rawURL string) string {
	trimmed := strings.TrimSpace(rawURL)
	if after, ok := strings.CutPrefix(trimmed, "https://"); ok {
		return "wss://" + after
	}
	if after, ok := strings.CutPrefix(trimmed, "http://"); ok {
		return "ws://" + after
	}
	return "wss://" + trimmed
}
