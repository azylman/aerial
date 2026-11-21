package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis-nats/internal/config"
)

// NATSBridge bridges incoming NATS JetStream events to Restate ingress.
type NATSBridge struct {
	cfg        config.Config
	bus        pubsub.Bus
	httpClient *http.Client
}

// NewNATSBridge constructs a new NATSBridge, wrapping the HTTP client with OpenTelemetry tracing.
func NewNATSBridge(cfg config.Config, bus pubsub.Bus, httpClient *http.Client) *NATSBridge {
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

func (b *NATSBridge) buildRouterServiceName() string {
	return config.BuildRouterServiceName(b.cfg.AgentName)
}

func (b *NATSBridge) restateRouterSendPath(handler string) string {
	return "/" + b.buildRouterServiceName() + "/" + handler + "/send"
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

func isRetryableIngressStatus(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= http.StatusInternalServerError
}

func settleFailedDelivery(ctx context.Context, msg pubsub.Message) {
	delay := pubsub.NakBackoff(msg.NumDelivered(), 16*time.Second)
	if err := msg.NakWithDelay(delay); err != nil {
		slog.ErrorContext(ctx, "[vanadis-nats] error naking message", "delay", delay, "error", err)
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
		if termErr := msg.Term(); termErr != nil {
			slog.ErrorContext(ctx, "[vanadis-nats] error terming message on request creation failure", "error", termErr)
		}
		return fmt.Errorf("create request to %s: %w", targetURL, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := b.httpClient.Do(req)
	if err != nil {
		settleFailedDelivery(ctx, msg)
		return fmt.Errorf("post to restate ingress %s: %w", targetURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || isRetryableIngressStatus(resp.StatusCode) {
		settleFailedDelivery(ctx, msg)
		return fmt.Errorf("restate ingress error (status %d)", resp.StatusCode)
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		if termErr := msg.Term(); termErr != nil {
			slog.ErrorContext(ctx, "[vanadis-nats] error terming message on non-retryable restate status", "status", resp.StatusCode, "error", termErr)
		}
		return nil
	}
	return nil
}

// HandlePRLifecycleEvent processes an incoming PRLifecycleEvent on aerial.pr.>.
func (b *NATSBridge) HandlePRLifecycleEvent(ctx context.Context, msg pubsub.Message) error {
	var ev pubsub.PRLifecycleEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		slog.WarnContext(ctx, "[vanadis-nats] failed to unmarshal pr lifecycle event", "error", err)
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
	return b.postRestateIngress(ctx, b.restateRouterSendPath("RoutePREvent"), key, body, msg)
}

// HandleDispatchOutcomeEvent processes an incoming DispatchOutcomeEvent on aerial.dispatch.*.
func (b *NATSBridge) HandleDispatchOutcomeEvent(ctx context.Context, msg pubsub.Message) error {
	var ev pubsub.DispatchOutcomeEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		slog.WarnContext(ctx, "[vanadis-nats] failed to unmarshal dispatch outcome event", "error", err)
		return nil
	}

	ev.DispatchID = strings.TrimSpace(ev.DispatchID)
	ev.VanadisChunkID = strings.TrimSpace(ev.VanadisChunkID)
	status := strings.ToLower(strings.TrimSpace(ev.Status))
	if ev.DispatchID == "healthcheck-ping" || ev.VanadisChunkID == "" {
		return nil
	}
	if status != "completed" && status != "failed" && status != "timeout" && status != "crashed" {
		return nil
	}
	ev.Status = status

	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal dispatch outcome event: %w", err)
	}
	key := dispatchIdempotencyKey(msg, ev)
	return b.postRestateIngress(ctx, b.restateRouterSendPath("RouteDispatchOutcome"), key, body, msg)
}

// HandleWidgetSyncEvent processes an incoming WidgetSyncJobArgs on aerial.coalesced.widget_sync.>.
func (b *NATSBridge) HandleWidgetSyncEvent(ctx context.Context, msg pubsub.Message) error {
	var args pubsub.WidgetSyncJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		slog.WarnContext(ctx, "[vanadis-nats] failed to unmarshal widget sync event", "error", err)
		return nil
	}

	if buildID := strings.TrimSpace(args.BuildID); buildID != "" {
		body, err := json.Marshal(buildID)
		if err != nil {
			return fmt.Errorf("marshal build widget sync body: %w", err)
		}
		return b.postRestateIngress(ctx, b.restateRouterSendPath("RouteBuildReconcile"), widgetSyncIdempotencyKey(msg, args), body, msg)
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
		return b.postRestateIngress(ctx, b.restateRouterSendPath("RoutePREvent"), widgetSyncIdempotencyKey(msg, args), body, msg)
	}

	return nil
}
