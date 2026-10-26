package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

// AppDeps encapsulates subsystem dependencies for Prospera.
type AppDeps struct {
	TwilioClient  TwilioClient
	TokenMgr      *StreamTokenManager
	ConsultClient ContextConsultClient
	SessionMgr    *CallSessionManager
}

func setupRoutes(cfg *Config, store Store, optionalDeps ...AppDeps) http.Handler {
	var deps AppDeps
	if len(optionalDeps) > 0 {
		deps = optionalDeps[0]
	}
	if deps.TwilioClient == nil {
		deps.TwilioClient = NewHTTPTwilioClient(cfg.TwilioAccountSID, cfg.TwilioAuthToken)
	}
	if deps.TokenMgr == nil {
		deps.TokenMgr = NewStreamTokenManager(time.Now)
	}
	if deps.ConsultClient == nil {
		deps.ConsultClient = NewHTTPContextConsultClient(cfg.GoogleMCPURL, cfg.CovenURL)
	}
	if deps.SessionMgr == nil {
		deps.SessionMgr = NewCallSessionManager(cfg, store, deps.TwilioClient, deps.ConsultClient, deps.TokenMgr)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"service": "prospera",
			"port":    cfg.Port,
		})
	})

	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("prospera", false))

	mux.HandleFunc("POST /internal/dsp-preview", func(w http.ResponseWriter, r *http.Request) {
		handleDSPPreview(w, r)
	})

	mux.HandleFunc("POST /internal/store-preview", func(w http.ResponseWriter, r *http.Request) {
		handleStorePreview(w, r, store)
	})

	mux.HandleFunc("/twilio/media-stream", deps.SessionMgr.HandleMediaWebSocket)

	mux.HandleFunc("POST /internal/bridge-preview", func(w http.ResponseWriter, r *http.Request) {
		handleBridgePreview(w, r, deps)
	})

	return mux
}

type dspPreviewRequest struct {
	Sequence string `json:"sequence"`
}

type dspPreviewResponse struct {
	LengthBytes int     `json:"length_bytes"`
	RMS         float64 `json:"rms"`
}

func handleDSPPreview(w http.ResponseWriter, r *http.Request) {
	var req dspPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ulawBytes, err := SynthesizeDTMFMuLaw(req.Sequence)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pcm := MuLawToPCM16(ulawBytes)
	pcm16k := Resample8kTo16k(pcm)
	_ = Resample24kTo8k(pcm16k)
	_ = PCM16ToMuLaw(pcm)
	leBytes := PCM16ToLEBytes(pcm)
	_ = LEBytesToPCM16(leBytes)
	rms := ComputeRMS(pcm)

	httpkit.WriteJSON(w, http.StatusOK, dspPreviewResponse{
		LengthBytes: len(ulawBytes),
		RMS:         rms,
	})
}

type storePreviewRequest struct {
	Action      string       `json:"action"`
	ToRaw       string       `json:"to_raw"`
	FromNumber  string       `json:"from_number"`
	Message     string       `json:"message"`
	Brief       CallBrief    `json:"brief"`
	CallID      string       `json:"call_id"`
	ProviderSID string       `json:"provider_sid"`
	MessageSID  string       `json:"message_sid"`
	VoiceMsg    VoiceMessage `json:"voice_msg"`
}

func handleStorePreview(w http.ResponseWriter, r *http.Request, store Store) {
	var req storePreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	switch req.Action {
	case "validate_and_build":
		normTo, sanitizedBrief, err := ValidateInitiateCallArgs(req.ToRaw, req.FromNumber, req.Message, req.Brief)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		prompt := BuildSystemPrompt(sanitizedBrief, ModeConversation, nil)
		vm := ResolveVoicemailMessage(sanitizedBrief, req.Message)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			"to":        normTo,
			"prompt":    prompt,
			"voicemail": vm,
		})
	case "list_calls":
		calls, err := store.ListCalls(ctx, 10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, calls)
	case "list_messages":
		msgs, err := store.ListVoiceMessages(ctx, 10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, msgs)
	case "recent_outbound":
		call, err := store.FindRecentOutboundCall(ctx, req.ToRaw, 7*24*time.Hour)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, call)
	case "demo_lifecycle":
		c := VoiceCall{
			CallID:     req.CallID,
			Direction:  "outbound",
			State:      "initiated",
			FromNumber: "+15105550100",
			ToNumber:   "+15105550199",
			StartedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		}
		var opErr error
		record := func(err error) {
			if err != nil && opErr == nil {
				opErr = err
			}
		}
		record(store.CreateOutboundCall(ctx, c, 5))
		cIn := c
		cIn.CallID = req.CallID + "-inbound"
		cIn.Direction = "inbound"
		record(store.CreateInboundCall(ctx, cIn))
		record(store.BindProviderCallSID(ctx, req.CallID, req.ProviderSID))
		_, err := store.GetCall(ctx, req.CallID)
		record(err)
		_, err = store.GetCallByProviderSID(ctx, req.ProviderSID)
		record(err)
		record(store.UpdateCallState(ctx, req.CallID, "in-progress", "", "human"))
		record(store.AppendTranscriptTurn(ctx, req.CallID, TranscriptTurn{Role: "user", Text: "hi"}))
		_, err = store.AdvanceLiveTranscriptCursor(ctx, req.CallID, 0, 1)
		record(err)
		record(store.AppendOwnerInstruction(ctx, req.CallID, OwnerInstruction{Instruction: "wait"}))
		record(store.SetVoicemailRecording(ctx, req.CallID, "https://recording"))
		_, err = store.TransitionReportStatus(ctx, req.CallID, []string{"pending"}, "delivered", "ok")
		record(err)
		_, err = store.ReconcileOrphanedCalls(ctx, nil)
		record(err)
		_, err = store.SaveVoiceMessage(ctx, req.VoiceMsg)
		record(err)
		record(store.MarkVoiceMessageDelivered(ctx, req.MessageSID))

		if opErr != nil {
			http.Error(w, opErr.Error(), http.StatusInternalServerError)
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "exercised"})
	default:
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

type bridgePreviewRequest struct {
	Action       string            `json:"action"`
	AuthToken    string            `json:"auth_token"`
	FullURL      string            `json:"full_url"`
	Form         map[string]string `json:"form"`
	Signature    string            `json:"signature"`
	CallID       string            `json:"call_id"`
	TTLSeconds   int               `json:"ttl_seconds"`
	Token        string            `json:"token"`
	WSURL        string            `json:"ws_url"`
	Greeting     string            `json:"greeting"`
	CallbackURL  string            `json:"callback_url"`
	ToNumber     string            `json:"to_number"`
	FromNumber   string            `json:"from_number"`
	TwiMLURL     string            `json:"twiml_url"`
	ProviderSID  string            `json:"provider_sid"`
	SMSBody      string            `json:"sms_body"`
	AreaCode     string            `json:"area_code"`
	RecordingURL string            `json:"recording_url"`
	RMS          float64           `json:"rms"`
	ElapsedMs    int               `json:"elapsed_ms"`
	AnsweredBy   string            `json:"answered_by"`
	Query        string            `json:"query"`
	SteerMessage string            `json:"steer_message"`
	SteerMode    string            `json:"steer_mode"`
}

func handleBridgePreview(w http.ResponseWriter, r *http.Request, deps AppDeps) {
	var req bridgePreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch req.Action {
	case "validate_signature", "stream_token", "twiml", "twilio_rest":
		handlePreviewTelephony(w, r, req, deps)
	case "amd_eval", "consult", "session_mgr":
		handlePreviewLive(w, r, req, deps)
	default:
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

func handlePreviewTelephony(w http.ResponseWriter, r *http.Request, req bridgePreviewRequest, deps AppDeps) {
	ctx := r.Context()
	switch req.Action {
	case "validate_signature":
		uv := url.Values{}
		for k, v := range req.Form {
			uv.Set(k, v)
		}
		valid := ValidateTwilioSignature(req.AuthToken, req.FullURL, uv, req.Signature)
		httpkit.WriteJSON(w, http.StatusOK, map[string]bool{"valid": valid})
	case "stream_token":
		issued, err := deps.TokenMgr.Issue(req.CallID, time.Duration(req.TTLSeconds)*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tok := req.Token
		if tok == "" {
			tok = issued
		}
		consumedID, ok := deps.TokenMgr.Consume(tok)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			"issued":           issued,
			"consumed_call_id": consumedID,
			"consumed_ok":      ok,
		})
	case "twiml":
		sXML := GenerateConnectStreamTwiML(req.WSURL)
		vXML := GenerateVoicemailTwiML(req.Greeting, req.CallbackURL)
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			"stream_twiml":    sXML,
			"voicemail_twiml": vXML,
		})
	case "twilio_rest":
		if _, err := deps.TwilioClient.CreateCall(ctx, CreateCallRequest{To: req.ToNumber, From: req.FromNumber, TwiMLURL: req.TwiMLURL}); err != nil {
			log.Printf("preview create call err: %v", err)
		}
		if err := deps.TwilioClient.HangupCall(ctx, req.ProviderSID); err != nil {
			log.Printf("preview hangup err: %v", err)
		}
		if _, err := deps.TwilioClient.IsCallActive(ctx, req.ProviderSID); err != nil {
			log.Printf("preview is call active err: %v", err)
		}
		if _, err := deps.TwilioClient.SendSMS(ctx, req.ToNumber, req.FromNumber, req.SMSBody); err != nil {
			log.Printf("preview send sms err: %v", err)
		}
		if _, err := deps.TwilioClient.EnsurePhoneNumber(ctx, req.AreaCode, req.CallbackURL); err != nil {
			log.Printf("preview ensure phone err: %v", err)
		}
		if _, _, err := deps.TwilioClient.DownloadRecording(ctx, req.RecordingURL); err != nil {
			log.Printf("preview download recording err: %v", err)
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "exercised"})
	}
}

func handlePreviewLive(w http.ResponseWriter, r *http.Request, req bridgePreviewRequest, deps AppDeps) {
	ctx := r.Context()
	switch req.Action {
	case "amd_eval":
		ctrl := NewAMDGreetingController(0.015)
		act1 := ctrl.OnInboundFrame(req.RMS, req.ElapsedMs)
		act2 := ctrl.OnAMDWebhook(req.AnsweredBy)
		_ = ctrl.State()
		_ = ctrl.IsReleased()
		httpkit.WriteJSON(w, http.StatusOK, map[string]int{
			"frame_action":   int(act1),
			"webhook_action": int(act2),
		})
	case "consult":
		cal, calErr := deps.ConsultClient.ConsultCalendar(ctx, req.Query, "", "")
		if calErr != nil {
			log.Printf("preview consult cal err: %v", calErr)
		}
		mem, memErr := deps.ConsultClient.ConsultMemory(ctx, req.Query)
		if memErr != nil {
			log.Printf("preview consult mem err: %v", memErr)
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			"calendar": cal,
			"memory":   mem,
		})
	case "session_mgr":
		_ = deps.SessionMgr.ActiveCount()
		if err := deps.SessionMgr.SteerCall(req.CallID, req.SteerMessage, req.SteerMode); err != nil {
			log.Printf("preview steer err: %v", err)
		}
		if err := deps.SessionMgr.HandleAMDWebhook(req.ProviderSID, req.AnsweredBy); err != nil {
			log.Printf("preview amd err: %v", err)
		}
		if err := deps.SessionMgr.EndCall(req.CallID); err != nil {
			log.Printf("preview end call err: %v", err)
		}
		_, _ = deps.SessionMgr.GetSession(req.CallID)
		_, _ = deps.SessionMgr.GetSessionByProviderSID(req.ProviderSID)
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "exercised"})
	}
}

// RunApp bootstraps and runs the Prospera service HTTP server with graceful shutdown.
func RunApp(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return fmt.Errorf("app: port cannot be empty")
	}

	db, err := OpenDB(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("app: open db: %w", err)
	}
	store := NewPGStore(db)
	defer httpkit.CloseWarn(store, "prospera store on shutdown")

	// Initial reconciliation of unended orphan calls
	if _, err := store.ReconcileOrphanedCalls(ctx, nil); err != nil {
		log.Printf("prospera: initial orphan reconciliation notice: %v", err)
	}

	twilioClient := NewHTTPTwilioClient(cfg.TwilioAccountSID, cfg.TwilioAuthToken)
	tokenMgr := NewStreamTokenManager(time.Now)
	consultClient := NewHTTPContextConsultClient(cfg.GoogleMCPURL, cfg.CovenURL)
	sessionMgr := NewCallSessionManager(cfg, store, twilioClient, consultClient, tokenMgr)

	mux := setupRoutes(cfg, store, AppDeps{
		TwilioClient:  twilioClient,
		TokenMgr:      tokenMgr,
		ConsultClient: consultClient,
		SessionMgr:    sessionMgr,
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("app: server shutdown: %w", err)
		}
		return nil
	case err := <-serverErr:
		return err
	}
}

var (
	loadConfigFn = LoadConfig
	runAppFn     = RunApp
	exitFn       = log.Fatalf
)

func main() {
	cfg, err := loadConfigFn(nil)
	if err != nil {
		exitFn("failed to load config: %v", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("prospera exited with error: %v", err)
	}
}
