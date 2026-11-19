package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-nats/internal/bridge"
	"github.com/azylman/aerial/services/vanadis-nats/internal/config"
)

// SetupHealthMux creates the ServeMux for health, metrics, and coverage flush.
func SetupHealthMux() *http.ServeMux {
	return httpkit.SetupHealthMux("vanadis-nats", promhttp.Handler())
}

// NewBusFn dials NATS JetStream; replaceable in tests.
var NewBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	if strings.TrimSpace(url) == "" {
		return nil, errors.New("nats_url is required")
	}
	return pubsub.NewJetStreamBus(ctx, url)
}

// listenFn binds the health port; replaceable in tests to simulate serve failures.
var listenFn = func(ctx context.Context, port string) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", ":"+port)
}

// RunApp starts the health server and consumers until context cancellation.
func RunApp(ctx context.Context, cfg config.Config) error {
	healthMux := SetupHealthMux()
	healthServer := &http.Server{
		Handler:      healthMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	healthLn, err := listenFn(ctx, cfg.Port)
	if err != nil {
		return fmt.Errorf("listening on health port %s: %w", cfg.Port, err)
	}

	errChan := make(chan error, 2)
	go func() {
		log.Printf("[vanadis-nats] Health server listening on :%s", cfg.Port)
		if sErr := healthServer.Serve(healthLn); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			errChan <- sErr
		}
	}()

	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer shutdownCancel()
		if sErr := healthServer.Shutdown(shutdownCtx); sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			log.Printf("[vanadis-nats] error shutting down health server: %v", sErr)
		}
	}()

	bus, err := NewBusFn(ctx, cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer httpkit.CloseWarn(bus, "vanadis-nats bus")

	b := bridge.NewNATSBridge(cfg, bus, nil)
	if cErr := b.StartConsumers(ctx); cErr != nil {
		return fmt.Errorf("start consumers: %w", cErr)
	}

	select {
	case <-ctx.Done():
	case rErr := <-errChan:
		err = rErr
	}

	log.Println("[vanadis-nats] Server stopped")
	return err
}
