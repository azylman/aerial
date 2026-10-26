package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

func setupRoutes(cfg *Config, store Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"service": "prospera",
			"port":    cfg.Port,
		})
	})

	mux.Handle("GET /debug/coverage/flush", httpkit.HandleCoverageFlush("prospera", false))

	mux.HandleFunc("POST /internal/dsp-preview", func(w http.ResponseWriter, r *http.Request) {
		handleDSPPreview(w, r)
	})

	mux.HandleFunc("POST /internal/store-preview", func(w http.ResponseWriter, r *http.Request) {
		handleStorePreview(w, r, store)
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

	mux := setupRoutes(cfg, store)

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
