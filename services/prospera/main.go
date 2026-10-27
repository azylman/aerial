package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

var transcriptBatchInterval = 5 * time.Second

func runTranscriptBatcher(ctx context.Context, sessionMgr *CallSessionManager, reporter *Reporter) {
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

func flushActiveSessions(ctx context.Context, sessionMgr *CallSessionManager, reporter *Reporter) {
	if sessionMgr == nil || reporter == nil {
		return
	}
	sessionMgr.mu.RLock()
	callIDs := make([]string, 0, len(sessionMgr.sessions))
	for id := range sessionMgr.sessions {
		callIDs = append(callIDs, id)
	}
	sessionMgr.mu.RUnlock()

	for _, id := range callIDs {
		if err := reporter.FlushLiveTranscript(ctx, id); err != nil {
			log.Printf("prospera: flush transcript %s: %v", id, err)
		}
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

	twilioClient := NewHTTPTwilioClient(cfg.TwilioAccountSID, cfg.TwilioAuthToken)

	// Initial reconciliation of unended orphan calls
	if _, err := store.ReconcileOrphanedCalls(ctx, twilioClient.IsCallActive); err != nil {
		log.Printf("prospera: initial orphan reconciliation notice: %v", err)
	}

	tokenMgr := NewStreamTokenManager(time.Now)
	consultClient := NewHTTPContextConsultClient(cfg.GoogleMCPURL, cfg.CovenURL)
	sessionMgr := NewCallSessionManager(cfg, store, twilioClient, consultClient, tokenMgr)
	reporter := NewReporter(cfg, store, twilioClient, WithReporterRecordingProvider(sessionMgr.GetRecordingWAV))

	server := NewServer(cfg, store, twilioClient, tokenMgr, sessionMgr, reporter)

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
	wg.Add(1)
	go func() {
		defer wg.Done()
		runTranscriptBatcher(workerCtx, sessionMgr, reporter)
	}()

	select {
	case <-ctx.Done():
		workerCancel()
		wg.Wait()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			server.Wait()
			return fmt.Errorf("app: server shutdown: %w", err)
		}
		server.Wait()
		return nil
	case err := <-serverErr:
		workerCancel()
		wg.Wait()
		server.Wait()
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
