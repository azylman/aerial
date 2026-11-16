package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

// Config holds runtime configuration for the vanadis-nats bridge service.
type Config struct {
	Port              string
	NATSURL           string
	RestateIngressURL string
	ConfigPath        string
}

// NATSBridgeYAMLConfig defines the declarative file schema for vanadis-nats configuration.
type NATSBridgeYAMLConfig struct {
	Port              string `yaml:"port"`
	NATSURL           string `yaml:"nats_url"`
	RestateIngressURL string `yaml:"restate_ingress_url"`
}

var fallbackConfigPaths = []string{
	"/share/aerial-config/services/vanadis-nats/vanadis-nats.yaml",
	"/config/vanadis-nats.yaml",
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// LoadConfig loads configuration from an optional YAML file with environment variable overrides.
func LoadConfig(configPath string) Config {
	var fileCfg NATSBridgeYAMLConfig
	target := strings.TrimSpace(configPath)
	if target == "" {
		for _, p := range fallbackConfigPaths {
			if fi, err := os.Stat(p); err == nil && fi.Size() > 0 {
				target = p
				break
			}
		}
	}
	if target != "" {
		if data, err := os.ReadFile(target); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis-nats] Warning: failed to parse yaml config at %s: %v", target, err)
			}
		}
	}

	return Config{
		Port:              firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4027"),
		NATSURL:           firstNonEmpty(os.Getenv("NATS_URL"), fileCfg.NATSURL),
		RestateIngressURL: strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("RESTATE_INGRESS_URL"), fileCfg.RestateIngressURL, "http://restate.restate.svc.cluster.local:8080")), "/"),
		ConfigPath:        strings.TrimSpace(configPath),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config {
	return LoadConfig(os.Getenv("CONFIG_PATH"))
}

// NATSBridge bridges incoming NATS JetStream events to Restate ingress.
type NATSBridge struct {
	cfg        Config
	bus        pubsub.Bus
	httpClient *http.Client
}

// NewNATSBridge constructs a new NATSBridge, wrapping the HTTP client with OpenTelemetry tracing.
func NewNATSBridge(cfg Config, bus pubsub.Bus, httpClient *http.Client) *NATSBridge {
	cfg.RestateIngressURL = strings.TrimRight(strings.TrimSpace(cfg.RestateIngressURL), "/")
	if cfg.RestateIngressURL == "" {
		cfg.RestateIngressURL = "http://restate.restate.svc.cluster.local:8080"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &NATSBridge{
		cfg:        cfg,
		bus:        bus,
		httpClient: tracing.Client(httpClient),
	}
}

// StartConsumers verifies JetStream streams and binds durable queue consumers for dispatch and PR events.
func (b *NATSBridge) StartConsumers(ctx context.Context) error {
	if b.bus == nil {
		return errors.New("pubsub bus is nil")
	}
	if err := pubsub.EnsureCoreStreams(ctx, b.bus); err != nil {
		return fmt.Errorf("ensure core streams: %w", err)
	}

	if _, err := b.bus.ConsumeQueue(ctx, "aerial.dispatch.*", "vanadis-dispatch", b.HandleDispatchOutcomeEvent); err != nil {
		return fmt.Errorf("subscribe vanadis-dispatch queue: %w", err)
	}
	log.Println("[vanadis-nats] Subscribed vanadis-dispatch queue consumer to aerial.dispatch.*")

	if _, err := b.bus.ConsumeQueue(ctx, "aerial.pr.>", "vanadis-pr-events", b.HandlePRLifecycleEvent); err != nil {
		return fmt.Errorf("subscribe vanadis-pr-events queue: %w", err)
	}
	log.Println("[vanadis-nats] Subscribed vanadis-pr-events queue consumer to aerial.pr.>")

	if _, err := b.bus.ConsumeQueue(ctx, "aerial.coalesced.widget_sync.>", "vanadis-widget", b.HandleWidgetSyncEvent); err != nil {
		return fmt.Errorf("subscribe vanadis-widget queue: %w", err)
	}
	log.Println("[vanadis-nats] Subscribed vanadis-widget queue consumer to aerial.coalesced.widget_sync.>")

	return nil
}

func nakBackoffDelay(delivered uint64) time.Duration {
	if delivered <= 1 {
		return 1 * time.Second
	}
	shift := min(delivered-1, 4)
	return time.Duration(1<<shift) * time.Second
}

func settleFailedDelivery(msg pubsub.Message) {
	if msg.NumDelivered() >= 3 {
		if err := msg.Term(); err != nil {
			log.Printf("[vanadis-nats] error terming message after %d deliveries: %v", msg.NumDelivered(), err)
		}
		return
	}
	delay := nakBackoffDelay(msg.NumDelivered())
	if err := msg.NakWithDelay(delay); err != nil {
		log.Printf("[vanadis-nats] error naking message with delay %v: %v", delay, err)
	}
}

func prIdempotencyKey(msg pubsub.Message, ev pubsub.PRLifecycleEvent) string {
	if msg != nil && msg.Header() != nil {
		if k := strings.TrimSpace(msg.Header().Get(pubsub.HeaderMsgID)); k != "" {
			return k
		}
		if k := strings.TrimSpace(msg.Header().Get("Idempotency-Key")); k != "" {
			return k
		}
	}
	if ev.OccurredAt.IsZero() {
		return fmt.Sprintf("pr:%s:%d:%s:%s:%s", ev.Repo, ev.PRNumber, ev.Event, ev.HeadSHA, ev.CheckName)
	}
	return fmt.Sprintf("pr:%s:%d:%s:%s:%s:%d", ev.Repo, ev.PRNumber, ev.Event, ev.HeadSHA, ev.CheckName, ev.OccurredAt.UnixNano())
}

func dispatchIdempotencyKey(msg pubsub.Message, ev pubsub.DispatchOutcomeEvent) string {
	if msg != nil && msg.Header() != nil {
		if k := strings.TrimSpace(msg.Header().Get(pubsub.HeaderMsgID)); k != "" {
			return k
		}
		if k := strings.TrimSpace(msg.Header().Get("Idempotency-Key")); k != "" {
			return k
		}
	}
	return fmt.Sprintf("dispatch:%s:%s:%s", ev.DispatchID, ev.VanadisChunkID, ev.Status)
}

func widgetSyncIdempotencyKey(msg pubsub.Message, args pubsub.WidgetSyncJobArgs) string {
	if msg != nil && msg.Header() != nil {
		if k := strings.TrimSpace(msg.Header().Get(pubsub.HeaderMsgID)); k != "" {
			return k
		}
		if k := strings.TrimSpace(msg.Header().Get("Idempotency-Key")); k != "" {
			return k
		}
	}
	if strings.TrimSpace(args.BuildID) != "" {
		return "widget:build:" + strings.TrimSpace(args.BuildID)
	}
	return fmt.Sprintf("widget:pr:%s:%d", strings.TrimSpace(args.Repo), args.PRNumber)
}

func (b *NATSBridge) postRestateIngress(ctx context.Context, endpoint, idempotencyKey string, body []byte, msg pubsub.Message) error {
	targetURL := fmt.Sprintf("%s%s", b.cfg.RestateIngressURL, endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		settleFailedDelivery(msg)
		return fmt.Errorf("create request to %s: %w", targetURL, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := b.httpClient.Do(req)
	if err != nil {
		settleFailedDelivery(msg)
		return fmt.Errorf("post to restate ingress %s: %w", targetURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 500 {
		settleFailedDelivery(msg)
		return fmt.Errorf("restate ingress error (status %d)", resp.StatusCode)
	}
	return nil
}

// HandlePRLifecycleEvent processes an incoming PRLifecycleEvent on aerial.pr.>.
func (b *NATSBridge) HandlePRLifecycleEvent(ctx context.Context, msg pubsub.Message) error {
	var ev pubsub.PRLifecycleEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[vanadis-nats] warning: failed to unmarshal pr lifecycle event: %v", err)
		return nil
	}
	ev.Event = strings.ToLower(strings.TrimSpace(ev.Event))
	ev.Repo = strings.TrimSpace(ev.Repo)
	if ev.Repo == "" || ev.PRNumber <= 0 {
		return nil
	}

	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal pr lifecycle event: %w", err)
	}
	key := prIdempotencyKey(msg, ev)
	return b.postRestateIngress(ctx, "/BuildRouter/RoutePREvent/send", key, body, msg)
}

// HandleDispatchOutcomeEvent processes an incoming DispatchOutcomeEvent on aerial.dispatch.*.
func (b *NATSBridge) HandleDispatchOutcomeEvent(ctx context.Context, msg pubsub.Message) error {
	var ev pubsub.DispatchOutcomeEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[vanadis-nats] warning: failed to unmarshal dispatch outcome event: %v", err)
		return nil
	}

	ev.DispatchID = strings.TrimSpace(ev.DispatchID)
	ev.VanadisChunkID = strings.TrimSpace(ev.VanadisChunkID)
	status := strings.ToLower(strings.TrimSpace(ev.Status))
	if ev.DispatchID == "healthcheck-ping" || ev.VanadisChunkID == "" {
		return nil
	}
	if status != "completed" && status != "failed" && status != "timeout" {
		return nil
	}
	ev.Status = status

	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal dispatch outcome event: %w", err)
	}
	key := dispatchIdempotencyKey(msg, ev)
	return b.postRestateIngress(ctx, "/BuildRouter/RouteDispatchOutcome/send", key, body, msg)
}

// HandleWidgetSyncEvent processes an incoming WidgetSyncJobArgs on aerial.coalesced.widget_sync.>.
func (b *NATSBridge) HandleWidgetSyncEvent(ctx context.Context, msg pubsub.Message) error {
	var args pubsub.WidgetSyncJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[vanadis-nats] warning: failed to unmarshal widget sync event: %v", err)
		return nil
	}

	if buildID := strings.TrimSpace(args.BuildID); buildID != "" {
		body, err := json.Marshal(map[string]string{"reason": "widget_sync"})
		if err != nil {
			return fmt.Errorf("marshal build widget sync body: %w", err)
		}
		return b.postRestateIngress(ctx, "/Build/"+buildID+"/Reconcile/send", widgetSyncIdempotencyKey(msg, args), body, msg)
	}

	if repo := strings.TrimSpace(args.Repo); repo != "" && args.PRNumber > 0 {
		body, err := json.Marshal(pubsub.PRLifecycleEvent{
			Event:    "widget_sync",
			Repo:     repo,
			PRNumber: args.PRNumber,
		})
		if err != nil {
			return fmt.Errorf("marshal pr widget sync body: %w", err)
		}
		return b.postRestateIngress(ctx, "/BuildRouter/RoutePREvent/send", widgetSyncIdempotencyKey(msg, args), body, msg)
	}

	return nil
}

// SetupHealthMux creates the ServeMux for health, metrics, and coverage flush.
func SetupHealthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /healthz", handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	coverageFlush := httpkit.HandleCoverageFlush("vanadis-nats")
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	if strings.TrimSpace(url) == "" {
		return nil, errors.New("nats_url is required")
	}
	return pubsub.NewJetStreamBus(ctx, url)
}

// RunApp starts the health server and consumers until context cancellation.
func RunApp(ctx context.Context, cfg Config) error {
	healthMux := SetupHealthMux()
	healthServer := &http.Server{
		Handler:      healthMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	var lc net.ListenConfig
	healthLn, err := lc.Listen(ctx, "tcp", ":"+cfg.Port)
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

	bus, err := newBusFn(ctx, cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer httpkit.CloseWarn(bus, "vanadis-nats bus")

	bridge := NewNATSBridge(cfg, bus, nil)
	if cErr := bridge.StartConsumers(ctx); cErr != nil {
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

var (
	loadConfigFn  = LoadConfigFromEnv
	runAppFn      = RunApp
	exitFn        = log.Fatalf
	initTracingFn = tracing.Init
)

func runMain() error {
	cfg := loadConfigFn()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, flushTracing := initTracingFn(ctx, "vanadis-nats", tracing.DefaultConfigPath)
	defer flushTracing()

	if err := runAppFn(ctx, cfg); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := runMain(); err != nil {
		exitFn("[vanadis-nats] Error: %v", err)
	}
}
