package ticker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/metrics"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

// EventWatchFireRequest is the input payload for HaroEventWatch.Fire.
type EventWatchFireRequest struct {
	TriggerID     string `json:"trigger_id,omitempty"`
	ContextHeader string `json:"context_header,omitempty"`
	EventID       string `json:"event_id"`
}

// HaroEventWatch is a Restate Virtual Object keyed by trigger_id managing durable event watch firing.
type HaroEventWatch struct {
	db         *sql.DB
	dispatcher dispatch.PromptDispatcher
	nowFn      func() time.Time
}

func (w *HaroEventWatch) now() time.Time {
	if w.nowFn != nil {
		return w.nowFn()
	}
	return time.Now().UTC()
}

// NewHaroEventWatch constructs a HaroEventWatch Virtual Object handler.
func NewHaroEventWatch(db *sql.DB, dispatcher dispatch.PromptDispatcher) *HaroEventWatch {
	return &HaroEventWatch{
		db:         db,
		dispatcher: dispatcher,
	}
}

// eventWatchLoad is the load-trigger step result: the trigger fields the
// dispatch needs, or Suppressed when a persistent watch is inside its cooldown.
type eventWatchLoad struct {
	Suppressed bool   `json:"suppressed"`
	ID         string `json:"id"`
	ChannelID  string `json:"channel_id"`
	Prompt     string `json:"prompt"`
	Effort     string `json:"effort"`
	Agent      string `json:"agent"`
	Persistent bool   `json:"persistent"`
}

// Fire executes the event watch trigger in three durable Restate steps:
// load the trigger, dispatch the prompt, then consume the trigger. A deleted
// watch clears the object's state and ends terminally (404); a dispatch eri
// permanently rejects also ends terminally. A consumed one-shot watch clears
// the object's state after its last step.
func (w *HaroEventWatch) Fire(ctx restate.ObjectContext, req EventWatchFireRequest) (restate.Void, error) {
	triggerID := restate.Key(ctx)

	// Step 1: load-trigger
	loaded, err := restate.Run(ctx, func(rc restate.RunContext) (eventWatchLoad, error) {
		trig, getErr := withStoreTimeout(rc, func(c context.Context) (*model.EventTrigger, error) {
			return store.GetEventTrigger(c, w.db, triggerID)
		})
		if getErr != nil {
			return eventWatchLoad{}, getErr
		}
		now := w.now()
		if trig.Persistent && trig.UpdatedAt != nil && now.Sub(*trig.UpdatedAt) < 5*time.Second {
			log.Printf("[Haro EventWatch] Suppressing persistent event watch %s (%s:%s) within 5s cooldown (elapsed: %v)", triggerID, trig.Source, trig.Target, now.Sub(*trig.UpdatedAt))
			metrics.RecordScheduleRun(model.ScheduleTypeEventWatch, model.StatusSkippedActive)
			return eventWatchLoad{Suppressed: true}, nil
		}
		return eventWatchLoad{
			ID:         trig.ID,
			ChannelID:  trig.ChannelID,
			Prompt:     trig.Prompt,
			Effort:     trig.Effort,
			Agent:      trig.Agent,
			Persistent: trig.Persistent,
		}, nil
	}, restate.WithName("load-trigger"))
	if restate.IsTerminalError(err) && restate.AsTerminalError(err).Code() == http.StatusNotFound {
		// The watch was cancelled (or already consumed): nothing is left to track.
		restate.ClearAll(ctx)
		return restate.Void{}, err
	}
	if err != nil || loaded.Suppressed {
		return restate.Void{}, err
	}

	// Step 2: dispatch-event
	if _, err = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		if w.dispatcher == nil {
			return restate.Void{}, errors.New("dispatcher not initialized")
		}
		prompt := model.FormatEventTriggerPrompt(req.ContextHeader, loaded.Prompt)
		msgID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("event_watch:"+strings.TrimSpace(loaded.ID)+":"+strings.TrimSpace(req.EventID))).String()
		start := time.Now()
		_, dispErr := dispatch.TraceFire(rc, model.ScheduleTypeEventWatch, loaded.ID, func(dispCtx context.Context) (dispatch.PromptDispatchResponse, error) {
			return w.dispatcher.DispatchPrompt(dispCtx, dispatch.PromptDispatchRequest{
				ChannelID: loaded.ChannelID,
				MessageID: msgID,
				Prompt:    prompt,
				Effort:    loaded.Effort,
				Agent:     loaded.Agent,
			})
		})
		metrics.RecordScheduleDispatchDuration(model.ScheduleTypeEventWatch, time.Since(start).Seconds())
		if dispErr != nil {
			metrics.RecordScheduleRun(model.ScheduleTypeEventWatch, model.StatusFailed)
			return restate.Void{}, dispErr
		}
		metrics.RecordScheduleRun(model.ScheduleTypeEventWatch, model.StatusCompleted)
		return restate.Void{}, nil
	}, restate.WithName("dispatch-event")); err != nil {
		return restate.Void{}, err
	}

	// Step 3: consume-trigger
	_, err = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		_, consumeErr := withStoreTimeout(rc, func(c context.Context) (bool, error) {
			return store.ConsumeEventTrigger(c, w.db, triggerID, w.now())
		})
		return restate.Void{}, consumeErr
	}, restate.WithName("consume-trigger"))
	if err != nil {
		return restate.Void{}, err
	}
	if !loaded.Persistent {
		// A one-shot watch is deleted once consumed; its object is done.
		restate.ClearAll(ctx)
	}
	return restate.Void{}, nil
}

// eventIngressClient bounds the one-way /send posts the external-event
// consumer makes; Restate acknowledges a send without running the handler.
var eventIngressClient = &http.Client{Timeout: 10 * time.Second}

// HandleExternalEvent queries matching triggers and enqueues HaroEventWatch/<triggerID>/Fire
// one-way via Restate ingress /send, keyed by trigger ID + event ID, so the single
// event consumer never blocks on a dispatch.
func HandleExternalEvent(ctx context.Context, db *sql.DB, restateIngressURL string, ev pubsub.ExternalEvent, client ...*http.Client) error {
	if ev.Source == "healthcheck" || len(ev.Targets) == 0 {
		return nil
	}
	if db == nil {
		return errors.New("database not initialized")
	}
	triggers, err := store.QueryMatchingEventTriggers(ctx, db, ev.Source, ev.Targets, ev.EventType)
	if err != nil {
		return err
	}
	if len(triggers) > 0 && strings.TrimSpace(restateIngressURL) == "" {
		return errors.New("restate ingress URL not configured")
	}
	httpClient := eventIngressClient
	if len(client) > 0 && client[0] != nil {
		httpClient = client[0]
	}
	var errs []error
	for _, trig := range triggers {
		idempotencyKey := fmt.Sprintf("haro:event:%s:%s", trig.ID, ev.EventID)
		endpoint := fmt.Sprintf("%s/HaroEventWatch/%s/Fire/send", strings.TrimRight(restateIngressURL, "/"), url.PathEscape(trig.ID))
		payload := EventWatchFireRequest{
			TriggerID:     trig.ID,
			ContextHeader: ev.ContextHeader,
			EventID:       ev.EventID,
		}
		if _, postErr := restateingress.Post(ctx, httpClient, endpoint, idempotencyKey, payload, 0); postErr != nil {
			log.Printf("[Haro] error firing event watch %s (%s): %v", trig.ID, trig.Source, postErr)
			errs = append(errs, postErr)
		}
	}
	return errors.Join(errs...)
}
