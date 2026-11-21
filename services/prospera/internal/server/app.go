package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/delivery"
	"github.com/azylman/aerial/services/prospera/internal/gemini"
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

var newProsperaBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url)
}

var transcriptBatchInterval = 5 * time.Second

// shutdownTimeout bounds graceful HTTP shutdown; drainTimeout bounds the
// live-bridge drain window for calls that end naturally. Separate windows
// (not one shared deadline) so a Shutdown consumed or failed by lingering
// HTTP cannot starve Drain. teardownTimeout bounds the graceful-hangup
// handshake after leftovers are deliberately ended; waitTimeout bounds the
// background-task wait. Worst case stays under the default 30s pod
// termination grace: 5+10+5+5.
var (
	shutdownTimeout = 5 * time.Second
	drainTimeout    = 10 * time.Second
	teardownTimeout = 5 * time.Second
	waitTimeout     = 5 * time.Second
)

// shutdownApp stops the HTTP listener, waits for live Twilio-Gemini bridges
// to end naturally, then deliberately hangs up leftovers (operator-requested
// graceful teardown, never a silent socket cut) and waits for background
// tasks. Every phase runs on its own fresh window, even when Shutdown fails,
// so the total shutdown stays bounded by
// shutdownTimeout+drainTimeout+teardownTimeout+waitTimeout.
func shutdownApp(ctx context.Context, httpServer *http.Server, server *Server) error {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	shutdownErr := httpServer.Shutdown(shutdownCtx)

	drainCtx, drainCancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer drainCancel()
	server.Drain(drainCtx)

	server.HangupActiveSessions()
	teardownCtx, teardownCancel := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
	defer teardownCancel()
	server.Drain(teardownCtx)

	waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(ctx), waitTimeout)
	defer waitCancel()
	waitDone := make(chan struct{})
	go func() {
		server.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-waitCtx.Done():
		log.Printf("prospera: shutdown timed out waiting for background tasks, exiting")
	}

	if shutdownErr != nil {
		return fmt.Errorf("app: server shutdown: %w", shutdownErr)
	}
	return nil
}

func runTranscriptBatcher(ctx context.Context, sessionMgr *mediastream.CallSessionManager, reporter *delivery.Reporter) {
	ticker := time.NewTicker(transcriptBatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushActiveSessions(ctx, sessionMgr, reporter)
		}
	}
}

func flushActiveSessions(ctx context.Context, sessionMgr *mediastream.CallSessionManager, reporter *delivery.Reporter) {
	if sessionMgr == nil || reporter == nil {
		return
	}

	for _, id := range sessionMgr.ActiveCallIDs() {
		if err := reporter.FlushLiveTranscript(ctx, id); err != nil {
			slog.ErrorContext(ctx, "prospera: flush transcript failed", "call_id", id, "error", err)
		}
	}
}

// newProsperaMediaSession adapts a mediastream session request to the concrete
// MediaStreamSession bridge implementation.
func newProsperaMediaSession(cfg mediastream.SessionConfig) mediastream.Session {
	return mediastream.NewMediaStreamSession(mediastream.MediaStreamSessionConfig{
		CallID:           cfg.CallID,
		TwilioConn:       cfg.Conn,
		Config:           cfg.Config,
		Store:            cfg.Store,
		TwilioClient:     cfg.TwilioClient,
		ConsultClient:    cfg.ConsultClient,
		OpeningGreeting:  cfg.OpeningGreeting,
		VoicemailMessage: cfg.VoicemailMessage,
		SystemPrompt:     cfg.SystemPrompt,
		Language:         cfg.Language,
		PacerInterval:    cfg.PacerInterval,
		GeminiDialer:     cfg.GeminiDialer,
	})
}

func setupProsperaBus(ctx context.Context, server *Server, rawNATSURL string) pubsub.Bus {
	natsURL := strings.TrimSpace(rawNATSURL)
	if natsURL == "" {
		return nil
	}
	bus, busErr := newProsperaBusFn(ctx, natsURL)
	if busErr != nil {
		log.Printf("prospera: failed to connect to nats at %s: %v", natsURL, busErr)
		return nil
	}
	if streamErr := pubsub.EnsureCoreStreams(ctx, bus); streamErr != nil {
		log.Printf("prospera: failed to ensure core streams: %v", streamErr)
	}
	server.SetEventBus(bus)
	if consErr := server.StartJobConsumers(ctx); consErr != nil {
		log.Printf("prospera: failed to start job consumers: %v", consErr)
	}
	return bus
}

// RunApp bootstraps and runs the Prospera service HTTP server with graceful shutdown.
// Startup-bound settings (listen port, database, Twilio credentials, consult
// endpoints) are read once here; everything else is read per request from cfgSrc.
func RunApp(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
	if cfgSrc == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	cfg := cfgSrc.Current()
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return fmt.Errorf("app: port cannot be empty")
	}

	db, err := store.OpenDB(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("app: open db: %w", err)
	}
	st := store.NewPGStore(db)
	defer httpkit.CloseWarn(st, "prospera store on shutdown")

	twilioClient := twilio.NewHTTPTwilioClient(cfg.TwilioAccountSID, cfg.TwilioAuthToken)

	// Initial reconciliation of unended orphan calls
	if _, err := st.ReconcileOrphanedCalls(ctx, twilioClient.IsCallActive); err != nil {
		log.Printf("prospera: initial orphan reconciliation notice: %v", err)
	}

	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
	consultClient := gemini.NewHTTPContextConsultClient(cfg.GoogleMCPURL, cfg.CovenURL)
	sessionMgr := mediastream.NewCallSessionManager(cfgSrc, st, twilioClient, consultClient, tokenMgr,
		mediastream.WithSessionFactory(newProsperaMediaSession))
	reporter := delivery.NewReporter(cfgSrc, st, twilioClient, delivery.WithReporterRecordingProvider(sessionMgr.GetRecordingWAV))

	server := NewServer(cfgSrc, st, twilioClient, tokenMgr, sessionMgr, reporter)

	if bus := setupProsperaBus(ctx, server, cfg.NATSURL); bus != nil {
		defer httpkit.CloseWarn(bus, "prospera event bus on shutdown")
	}

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		runTranscriptBatcher(workerCtx, sessionMgr, reporter)
	})

	select {
	case <-ctx.Done():
		workerCancel()
		wg.Wait()
		return shutdownApp(ctx, httpServer, server)
	case err := <-serverErr:
		workerCancel()
		wg.Wait()
		server.Wait()
		return err
	}
}
