package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/google/uuid"
)

var newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url)
}

func initEriJetStreamWithFactory(ctx context.Context, natsURL string, factory func(ctx context.Context, url string) (pubsub.Bus, error)) pubsub.Bus {
	natsURL = strings.TrimSpace(natsURL)
	if natsURL == "" {
		return nil
	}
	b, err := factory(ctx, natsURL)
	if err != nil {
		log.Printf("[WorkerPool] Warning: failed to connect to nats at %s: %v", natsURL, err)
		return nil
	}
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		log.Printf("[WorkerPool] Warning: failed to ensure core streams: %v", err)
	}
	return b
}

func attachEriEventBus(ctx context.Context, pool *queue.WorkerPool) func() {
	fn := attachEriEventBusWithFactory(ctx, pool, newBusFn)
	if fn == nil {
		return func() {}
	}
	return fn
}

func attachEriEventBusWithFactory(ctx context.Context, pool *queue.WorkerPool, factory func(ctx context.Context, url string) (pubsub.Bus, error)) func() {
	if pool == nil {
		return nil
	}
	natsURL := strings.TrimSpace(pool.NATSURL())
	if natsURL == "" {
		log.Printf("[WorkerPool] Warning: NATS_URL is not configured; event bus disabled")
		return nil
	}
	bus := initEriJetStreamWithFactory(ctx, natsURL, factory)
	if bus == nil {
		return func() {}
	}
	pool.SetEventBus(bus)
	registerEriJobConsumers(ctx, bus, pool)
	return func() {
		if closeErr := bus.Close(); closeErr != nil {
			log.Printf("[WorkerPool] Warning closing event bus: %v", closeErr)
		}
	}
}

func registerEriJobConsumers(ctx context.Context, bus pubsub.Bus, pool *queue.WorkerPool) {
	if bus == nil || pool == nil {
		return
	}
	store := pool.Store()
	if _, err := bus.ConsumeQueue(ctx, pubsub.SubjectEriPromptJob, "eri-prompt", func(msgCtx context.Context, msg pubsub.Message) error {
		return handleEriPromptJob(msgCtx, store, pool, msg)
	}); err != nil {
		log.Printf("[WorkerPool] Warning subscribing eri-prompt consumer: %v", err)
	} else {
		log.Println("[WorkerPool] Subscribed eri-prompt queue consumer to " + pubsub.SubjectEriPromptJob)
	}

	if _, err := bus.ConsumeQueue(ctx, pubsub.SubjectEriDispatchJob, "eri-dispatch", func(msgCtx context.Context, msg pubsub.Message) error {
		return handleEriDispatchJob(msgCtx, store, pool, msg)
	}); err != nil {
		log.Printf("[WorkerPool] Warning subscribing eri-dispatch consumer: %v", err)
	} else {
		log.Println("[WorkerPool] Subscribed eri-dispatch queue consumer to " + pubsub.SubjectEriDispatchJob)
	}

	if _, err := bus.ConsumeQueue(ctx, pubsub.SubjectEriDiscordMessageJob, "eri-discord-message", func(msgCtx context.Context, msg pubsub.Message) error {
		return handleEriDiscordMessageJob(msgCtx, pool, msg)
	}); err != nil {
		log.Printf("[WorkerPool] Warning subscribing eri-discord-message consumer: %v", err)
	} else {
		log.Println("[WorkerPool] Subscribed eri-discord-message queue consumer to " + pubsub.SubjectEriDiscordMessageJob)
	}
}

func handleEriPromptJob(ctx context.Context, store db.Store, pool *queue.WorkerPool, msg pubsub.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	if store == nil && pool != nil {
		store = pool.Store()
	}
	var args pubsub.EriPromptJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[WorkerPool] Warning unmarshaling prompt job: %v", err)
		return nil
	}
	prompt := strings.TrimSpace(args.Prompt)
	if prompt == "" {
		log.Printf("[WorkerPool] Warning empty prompt in prompt job")
		return nil
	}
	channelID := resolvePromptChannelID(args.ChannelID, pool)
	if !queue.IsNumericSnowflake(channelID) {
		log.Printf("[WorkerPool] Warning invalid prompt job channel_id: %q", channelID)
		return nil
	}
	if pool != nil {
		channelID = pool.RedirectBuildPrompt(channelID, prompt)
	}

	effort := resolvePromptEffort("", strings.TrimSpace(args.Effort))

	active, err := checkPromptActiveTurn(ctx, store, args.SkipIfActive, channelID)
	if err != nil {
		return fmt.Errorf("failed checking prompt active turn: %w", err)
	}
	if active {
		return nil
	}

	if pool != nil {
		channelID = pool.ResolveTargetThread(channelID, "", prompt, time.Now().UTC())
	}

	msgID := strings.TrimSpace(args.DispatchID)
	if msgID == "" {
		msgID = uuid.New().String()
	}

	record := db.Message{
		ID:          msgID,
		ThreadID:    channelID,
		GuildID:     "",
		AuthorID:    "http-client",
		AuthorName:  "HTTP Client",
		IsAdmin:     true,
		Content:     prompt,
		Summary:     db.CleanTaskSummary(prompt),
		Status:      db.StatusPending,
		Effort:      effort,
		ChannelID:   channelID,
		Traceparent: tracing.Traceparent(ctx),
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	if store == nil {
		return errors.New("database store not configured")
	}

	insertCtx, insertCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer insertCancel()
	if err := store.InsertMessage(insertCtx, record); err != nil {
		return fmt.Errorf("failed inserting prompt message: %w", err)
	}

	if pool != nil {
		pool.Enqueue(record)
	}
	return nil
}

func publishDispatchDropOutcome(ctx context.Context, bus pubsub.Bus, dispatchID, source, sessionID, targetID, status, errMsg string) {
	if bus == nil || strings.TrimSpace(dispatchID) == "" {
		return
	}
	now := time.Now().UTC()
	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:  dispatchID,
		Source:      source,
		SessionID:   sessionID,
		TargetID:    targetID,
		Status:      status,
		Error:       errMsg,
		StartedAt:   now,
		CompletedAt: now,
		DurationMs:  0,
	}
	subject := pubsub.SubjectAerialDispatchPrefix + status
	msgID := fmt.Sprintf("dispatch:%s:%s", dispatchID, status)
	if err := bus.Publish(ctx, subject, msgID, ev); err != nil {
		log.Printf("[WorkerPool] Warning publishing dispatch drop outcome (%s) for %s: %v", status, dispatchID, err)
	}
}

func handleEriDispatchJob(ctx context.Context, store db.Store, pool *queue.WorkerPool, msg pubsub.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	if store == nil && pool != nil {
		store = pool.Store()
	}
	var args pubsub.EriDispatchJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[WorkerPool] Warning unmarshaling dispatch job: %v", err)
		return nil
	}
	req := DispatchRequest{
		ID:             args.ID,
		Source:         args.Source,
		SessionID:      args.SessionID,
		TargetID:       args.TargetID,
		VanadisChunkID: args.VanadisChunkID,
		Prompt:         args.Prompt,
		Effort:         args.Effort,
		SkipIfActive:   args.SkipIfActive,
	}
	var bus pubsub.Bus
	if pool != nil {
		bus = pool.EventBus()
	}

	if err := validateDispatchRequest(&req); err != nil {
		log.Printf("[WorkerPool] Warning invalid dispatch job: %v", err)
		publishDispatchDropOutcome(ctx, bus, req.ID, req.Source, req.SessionID, req.TargetID, "failed", err.Error())
		return nil
	}

	dispatchID := strings.TrimSpace(req.ID)
	if dispatchID == "" {
		dispatchID = uuid.New().String()
	}
	// Invariant: empty SessionID represents a stateless (ephemeral) dispatch turn and must
	// not default to dispatchID, matching HTTP POST /dispatch and burst pool routing.
	sessionID := strings.TrimSpace(req.SessionID)

	if store == nil {
		publishDispatchDropOutcome(ctx, bus, dispatchID, req.Source, sessionID, req.TargetID, "failed", "database store not configured")
		return errors.New("database store not configured")
	}

	if req.SkipIfActive {
		active, err := store.HasActiveDispatchTurn(ctx, sessionID, dispatchID)
		if err != nil {
			return fmt.Errorf("failed checking dispatch session active: %w", err)
		}
		if active {
			publishDispatchDropOutcome(ctx, bus, dispatchID, req.Source, sessionID, req.TargetID, "skipped_active", "")
			return nil
		}
	}

	isDup, err := checkDuplicateDispatch(ctx, store, dispatchID)
	if err != nil {
		return fmt.Errorf("failed checking duplicate dispatch: %w", err)
	}
	if isDup {
		return nil
	}

	d := buildDispatchRecord(req, dispatchID, sessionID, tracing.Traceparent(ctx))
	insertCtx, insertCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer insertCancel()
	inserted, err := store.InsertDispatch(insertCtx, d)
	if err != nil {
		return fmt.Errorf("failed inserting dispatch: %w", err)
	}
	if !inserted {
		return nil
	}
	if pool != nil {
		pool.EnqueueDispatch(d)
	}
	return nil
}

func handleEriDiscordMessageJob(_ context.Context, pool *queue.WorkerPool, msg pubsub.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	if pool == nil {
		return errors.New("worker pool is nil")
	}
	var args pubsub.EriDiscordMessageJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[WorkerPool] Warning unmarshaling discord message job: %v", err)
		return nil
	}
	targetID := resolveDirectMessageTarget(pool, DirectMessageRequest{ChannelID: args.ChannelID})
	if !queue.IsNumericSnowflake(targetID) {
		log.Printf("[WorkerPool] Warning invalid discord message job channel_id: %q", args.ChannelID)
		return nil
	}
	content := strings.TrimSpace(args.Content)
	if content == "" {
		log.Printf("[WorkerPool] Warning empty discord message job content")
		return nil
	}
	return pool.DeliverDirect(targetID, content)
}
