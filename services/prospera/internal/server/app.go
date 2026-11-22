package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	restateServer "github.com/restatedev/sdk-go/server"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/restateingress"
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
func shutdownApp(ctx context.Context, httpServer *http.Server, server *Server, extraServers ...*http.Server) error {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	for _, extra := range extraServers {
		if extra != nil {
			if extraErr := extra.Shutdown(shutdownCtx); extraErr != nil && shutdownErr == nil {
				shutdownErr = extraErr
			}
		}
	}

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

type restateDeploymentRequest struct {
	URI   string `json:"uri"`
	Force bool   `json:"force"`
}

// RegisterRestateDeployment registers the service deployment with the Restate Admin API.
func RegisterRestateDeployment(ctx context.Context, client *http.Client, adminURL, advertiseURL string) error {
	cleanAdmin := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	cleanAdv := strings.TrimSpace(advertiseURL)
	if cleanAdmin == "" || cleanAdv == "" {
		return nil
	}
	payload, err := json.Marshal(restateDeploymentRequest{
		URI:   cleanAdv,
		Force: true,
	})
	if err != nil {
		return fmt.Errorf("marshal restate deployment payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cleanAdmin+"/deployments", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create restate deployment request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post restate deployment: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var detail string
		if body, err := io.ReadAll(io.LimitReader(resp.Body, 1024)); err == nil && len(body) > 0 {
			detail = ": " + string(body)
		}
		return fmt.Errorf("restate admin /deployments returned %d%s", resp.StatusCode, detail)
	}
	return nil
}

// SweepUnfinishedReports finds calls in pending or processing status and enqueues them into Restate ingress.
func SweepUnfinishedReports(ctx context.Context, client *http.Client, st store.Store, ingressURL string) error {
	cleanIngress := strings.TrimRight(strings.TrimSpace(ingressURL), "/")
	if cleanIngress == "" || st == nil {
		return nil
	}
	calls, err := st.ListUnfinishedReportCalls(ctx, 100)
	if err != nil {
		return fmt.Errorf("list unfinished report calls: %w", err)
	}
	if client == nil {
		client = http.DefaultClient
	}
	var errs []error
	for _, c := range calls {
		endpoint := fmt.Sprintf("%s/ProsperaReport/%s/DeliverReport/send", cleanIngress, c.CallID)
		idempotencyKey := "prospera:report:" + c.CallID
		if _, postErr := restateingress.Post(ctx, client, endpoint, idempotencyKey, delivery.DeliverReportRequest{CallID: c.CallID}, 1024); postErr != nil {
			slog.WarnContext(ctx, "sweep: post unfinished report to restate ingress failed", "call_id", c.CallID, "error", postErr)
			errs = append(errs, postErr)
		}
	}
	return errors.Join(errs...)
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
	if _, recErr := st.ReconcileOrphanedCalls(ctx, twilioClient.IsCallActive); recErr != nil {
		log.Printf("prospera: initial orphan reconciliation notice: %v", recErr)
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

	serverErr := make(chan error, 2)
	go func() {
		if listenErr := httpServer.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
			serverErr <- listenErr
		}
	}()

	restatePort := strings.TrimSpace(cfg.RestatePort)
	if restatePort == "" {
		if port == "0" {
			restatePort = "0"
		} else {
			restatePort = "9080"
		}
	}

	prosperaReport := delivery.NewProsperaReport(reporter)
	restateSrv := restateServer.NewRestate().Bind(prosperaReport.Definition())
	restateHandler, err := restateSrv.Handler()
	if err != nil {
		httpkit.CloseWarn(httpServer, "prospera http server")
		return fmt.Errorf("app: create restate handler: %w", err)
	}

	var restateProtocols http.Protocols
	restateProtocols.SetHTTP1(true)
	restateProtocols.SetUnencryptedHTTP2(true)

	restateHTTPSrv := &http.Server{
		Addr:              ":" + restatePort,
		Handler:           restateHandler,
		Protocols:         &restateProtocols,
		ReadHeaderTimeout: 5 * time.Second,
	}

	var lc net.ListenConfig
	restateLn, err := lc.Listen(ctx, "tcp", ":"+restatePort)
	if err != nil {
		httpkit.CloseWarn(httpServer, "prospera http server")
		return fmt.Errorf("app: listen on restate port %s: %w", restatePort, err)
	}

	go func() {
		log.Printf("[prospera] Restate HTTP/2 server listening on :%s", restatePort)
		if sErr := restateHTTPSrv.Serve(restateLn); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			serverErr <- sErr
		}
	}()

	// Background Restate deployment registration and startup sweep
	go func() {
		regCtx, regCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer regCancel()
		if regErr := RegisterRestateDeployment(regCtx, nil, cfg.RestateAdminURL, cfg.RestateAdvertiseURL); regErr != nil {
			log.Printf("prospera: register restate deployment notice: %v", regErr)
		}
		if sweepErr := SweepUnfinishedReports(regCtx, nil, st, cfg.RestateIngressURL); sweepErr != nil {
			log.Printf("prospera: sweep unfinished reports notice: %v", sweepErr)
		}
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
		return shutdownApp(ctx, httpServer, server, restateHTTPSrv)
	case err := <-serverErr:
		workerCancel()
		wg.Wait()
		server.Wait()
		httpkit.CloseWarn(restateHTTPSrv, "prospera restate http server")
		return err
	}
}
