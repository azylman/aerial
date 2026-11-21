package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/prospera/internal/model"
)

func nakOrDropJob(ctx context.Context, msg pubsub.Message, jobName, entityID string, err error) error {
	if msg == nil {
		return err
	}
	delivered := msg.NumDelivered()
	if delivered >= 5 {
		slog.ErrorContext(ctx, "prospera: dropping job after max deliveries", "job", jobName, "entity_id", entityID, "delivered", delivered, "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "prospera: term job after max deliveries failed", "job", jobName, "error", termErr)
		}
		return nil
	}
	if nakErr := msg.NakWithDelay(pubsub.NakBackoff(delivered, 16*time.Second)); nakErr != nil {
		slog.WarnContext(ctx, "prospera: nak job error", "job", jobName, "error", nakErr)
	}
	return err
}

// StartJobConsumers subscribes durable competing-consumer queue subscriptions on AERIAL_JOBS.
func (s *Server) StartJobConsumers(ctx context.Context) error {
	bus := s.EventBus()
	if bus == nil {
		return nil
	}

	if _, err := bus.ConsumeQueue(ctx, pubsub.SubjectProsperaReportJob, "prospera-report", func(msgCtx context.Context, msg pubsub.Message) error {
		return s.HandleReportJob(msgCtx, msg)
	}); err != nil {
		return fmt.Errorf("subscribe prospera-report consumer: %w", err)
	}

	if _, err := bus.ConsumeQueue(ctx, pubsub.SubjectProsperaVoicemailJob, "prospera-voicemail", func(msgCtx context.Context, msg pubsub.Message) error {
		return s.HandleVoicemailJob(msgCtx, msg)
	}); err != nil {
		return fmt.Errorf("subscribe prospera-voicemail consumer: %w", err)
	}

	if _, err := bus.ConsumeQueue(ctx, pubsub.SubjectProsperaSMSJob, "prospera-sms", func(msgCtx context.Context, msg pubsub.Message) error {
		return s.HandleSMSJob(msgCtx, msg)
	}); err != nil {
		return fmt.Errorf("subscribe prospera-sms consumer: %w", err)
	}

	return nil
}

// HandleReportJob handles asynchronous post-call report delivery jobs from AERIAL_JOBS.
func (s *Server) HandleReportJob(ctx context.Context, msg pubsub.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	if s.reporter == nil {
		return errors.New("reporter is nil")
	}
	var args pubsub.ProsperaReportJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		slog.ErrorContext(ctx, "prospera: unmarshal report job error", "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "prospera: term invalid report job failed", "error", termErr)
		}
		return nil
	}
	callID := strings.TrimSpace(args.CallID)
	if callID == "" {
		slog.ErrorContext(ctx, "prospera: empty call_id in report job")
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "prospera: term empty call_id report job failed", "error", termErr)
		}
		return nil
	}

	if err := s.reporter.DeliverCallReport(ctx, callID); err != nil {
		return nakOrDropJob(ctx, msg, "report", callID, err)
	}
	return nil
}

// HandleVoicemailJob handles asynchronous voicemail processing jobs from AERIAL_JOBS.
func (s *Server) HandleVoicemailJob(ctx context.Context, msg pubsub.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	var args pubsub.ProsperaVoicemailJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		slog.ErrorContext(ctx, "prospera: unmarshal voicemail job error", "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "prospera: term invalid voicemail job failed", "error", termErr)
		}
		return nil
	}
	callID := strings.TrimSpace(args.CallID)
	recURL := strings.TrimSpace(args.RecordingURL)
	if callID == "" || recURL == "" {
		slog.ErrorContext(ctx, "prospera: missing call_id or recording_url in voicemail job")
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "prospera: term incomplete voicemail job failed", "error", termErr)
		}
		return nil
	}

	if s.reporter == nil {
		return errors.New("reporter is nil")
	}

	if err := s.reporter.ProcessVoicemailRecording(ctx, callID, recURL); err != nil {
		return nakOrDropJob(ctx, msg, "voicemail", callID, err)
	}
	return nil
}

// HandleSMSJob handles asynchronous inbound SMS processing jobs from AERIAL_JOBS.
func (s *Server) HandleSMSJob(ctx context.Context, msg pubsub.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	if s.reporter == nil {
		return errors.New("reporter is nil")
	}
	var args pubsub.ProsperaSMSJobArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		slog.ErrorContext(ctx, "prospera: unmarshal sms job error", "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "prospera: term invalid sms job failed", "error", termErr)
		}
		return nil
	}

	voiceMsg := model.VoiceMessage{
		MessageSID:    args.MessageSID,
		Direction:     directionInbound,
		FromNumber:    args.FromNumber,
		ToNumber:      args.ToNumber,
		Body:          args.Body,
		MediaURLs:     args.MediaURLs,
		RelatedCallID: args.RelatedCallID,
	}

	if err := s.reporter.DeliverInboundSMS(ctx, voiceMsg); err != nil {
		return nakOrDropJob(ctx, msg, "sms", args.MessageSID, err)
	}
	return nil
}
