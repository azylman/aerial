// Package delivery owns post-call reporting, live transcript batching,
// voicemail transcription, and inbound SMS delivery to Discord.
package delivery

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

const (
	reportStatusPending    = "pending"
	reportStatusFailed     = "failed"
	reportStatusProcessing = "processing"
	reportStatusDelivered  = "delivered"
)

const (
	serviceName         = "prospera"
	roleUser            = "user"
	directionInbound    = "inbound"
	contentTypeAudioWAV = "audio/wav"
)

// Reporter coordinates live transcript batching, post-call reports, voicemail transcription, and SMS delivery.
// internalClient calls Aerial services (eri) and propagates trace context; externalClient calls
// third-party APIs (Gemini, Whisper) with client spans only, never trace headers.
type Reporter struct {
	cfgSrc            config.ConfigSource
	store             store.Store
	twilio            twilio.TwilioClient
	internalClient    *http.Client
	externalClient    *http.Client
	geminiBaseURL     string
	recordingProvider func(callID string) []byte
	recordingDeleter  func(callID string)
}

// ReporterOption configures Reporter.
type ReporterOption func(*Reporter)

// WithReporterRecordingProvider configures an optional callback to fetch an in-memory WAV recording for a call.
func WithReporterRecordingProvider(fn func(callID string) []byte) ReporterOption {
	return func(r *Reporter) {
		r.recordingProvider = fn
	}
}

// WithReporterRecordingDeleter configures the callback that drops a call's
// in-memory recording once its report is delivered.
func WithReporterRecordingDeleter(fn func(callID string)) ReporterOption {
	return func(r *Reporter) {
		r.recordingDeleter = fn
	}
}

// NewReporter creates a new Reporter.
func NewReporter(cfgSrc config.ConfigSource, st store.Store, tw twilio.TwilioClient, opts ...ReporterOption) *Reporter {
	r := &Reporter{
		cfgSrc:         cfgSrc,
		store:          st,
		twilio:         tw,
		internalClient: tracing.Client(&http.Client{Timeout: 30 * time.Second}),
		externalClient: tracing.ExternalClient(&http.Client{Timeout: 30 * time.Second}),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type discordAttachmentPayload struct {
	Filename      string `json:"filename"`
	ContentType   string `json:"content_type,omitempty"`
	ContentBase64 string `json:"content_base64"`
}

type discordMessageRequest struct {
	ChannelID   string                     `json:"channel_id"`
	ThreadID    string                     `json:"thread_id,omitempty"`
	Content     string                     `json:"content"`
	Attachments []discordAttachmentPayload `json:"attachments,omitempty"`
}

// ResolveDiscordChannel returns channelID, falling back to the configured default channel.
func ResolveDiscordChannel(cfg *config.Config, channelID string) string {
	if channelID != "" {
		return channelID
	}
	if cfg != nil && cfg.DefaultDiscordChannel != "" {
		return cfg.DefaultDiscordChannel
	}
	return serviceName
}

func (r *Reporter) postDiscordMessage(ctx context.Context, cfg *config.Config, channelID, threadID, content string, attachments []discordAttachmentPayload) error {
	if cfg == nil || strings.TrimSpace(cfg.BrainURL) == "" {
		return fmt.Errorf("reporter: brain url is not configured")
	}

	reqPayload := discordMessageRequest{
		ChannelID:   channelID,
		ThreadID:    threadID,
		Content:     content,
		Attachments: attachments,
	}

	data, err := json.Marshal(reqPayload)
	if err != nil {
		return fmt.Errorf("reporter: marshal discord payload: %w", err)
	}

	endpoint := strings.TrimRight(cfg.BrainURL, "/") + "/discord/message"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("reporter: new discord request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.internalClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("reporter: post discord message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return fmt.Errorf("reporter: discord returned status %d", resp.StatusCode)
		}
		return fmt.Errorf("reporter: discord returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	return nil
}

// transcriptBatch is the slice of live transcript turns not yet posted.
type transcriptBatch struct {
	CallID    string `json:"call_id"`
	ChannelID string `json:"channel_id,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
	Content   string `json:"content,omitempty"`
	From      int    `json:"from"`
	To        int    `json:"to"`
}

// pendingTranscript loads the unsent live transcript turns for callID.
func (r *Reporter) pendingTranscript(ctx context.Context, callID string) (transcriptBatch, error) {
	if r.store == nil {
		return transcriptBatch{}, fmt.Errorf("reporter: store is nil")
	}
	call, err := r.store.GetCall(ctx, callID)
	if err != nil {
		return transcriptBatch{}, fmt.Errorf("reporter: get call %q: %w", callID, err)
	}
	if call == nil {
		return transcriptBatch{}, fmt.Errorf("reporter: call %q: %w", callID, store.ErrCallNotFound)
	}
	total := len(call.Transcript)
	batch := transcriptBatch{CallID: callID, From: call.LiveTranscriptCursor, To: total}
	if batch.From >= total {
		batch.To = batch.From
		return batch, nil
	}
	batch.Content = formatTranscriptTurns(call.Transcript[batch.From:total])
	if batch.Content != "" {
		batch.ChannelID = ResolveDiscordChannel(r.cfgSrc.Current(), call.DiscordChannelID)
		batch.ThreadID = call.DiscordThreadID
	}
	return batch, nil
}

func formatTranscriptTurns(turns []model.TranscriptTurn) string {
	var sb strings.Builder
	for _, turn := range turns {
		speaker := "Prospera"
		switch strings.ToLower(turn.Role) {
		case roleUser, "caller", "human":
			speaker = "Caller"
		case "system":
			speaker = "System"
		}
		if text := strings.TrimSpace(turn.Text); text != "" {
			fmt.Fprintf(&sb, "**%s**: %s\n", speaker, text)
		}
	}
	return strings.TrimSpace(sb.String())
}

func (r *Reporter) postTranscriptBatch(ctx context.Context, batch transcriptBatch) error {
	return r.postDiscordMessage(ctx, r.cfgSrc.Current(), batch.ChannelID, batch.ThreadID, batch.Content, nil)
}

// FlushLiveTranscript batches unsent transcript turns and delivers them to Discord.
func (r *Reporter) FlushLiveTranscript(ctx context.Context, callID string) error {
	batch, err := r.pendingTranscript(ctx, callID)
	if err != nil {
		return err
	}
	if batch.To <= batch.From {
		return nil
	}
	if batch.Content != "" {
		if err := r.postTranscriptBatch(ctx, batch); err != nil {
			return err
		}
	}
	if _, err := r.store.AdvanceLiveTranscriptCursor(ctx, callID, batch.From, batch.To); err != nil {
		return fmt.Errorf("reporter: advance transcript cursor: %w", err)
	}
	return nil
}

// storeStepTimeout bounds every Postgres call made inside a restate.Run step
// so a hung query becomes a retryable error instead of a stuck step.
const storeStepTimeout = 5 * time.Second

// reportIdempotencyKey names the single logical "deliver this call's report"
// operation so every trigger (end_call tool, Twilio status webhook, NATS job,
// startup sweep) collapses into one ProsperaReport invocation.
func reportIdempotencyKey(callID string) string {
	return "prospera:report:" + callID
}

// classifyStoreError makes deterministic store failures terminal and leaves
// transient ones (connection, timeout, serialization, deadlock) retryable.
func classifyStoreError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrCallNotFound) {
		return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusNotFound))
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && len(pgErr.Code) >= 2 {
		switch strings.ToUpper(pgErr.Code[:2]) {
		case "22", "23", "42", "0A", "2B", "2D", "25":
			return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusBadRequest))
		}
	}
	return err
}

// withStoreTimeout runs fn under storeStepTimeout and classifies its error.
func withStoreTimeout[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	tctx, cancel := context.WithTimeout(ctx, storeStepTimeout)
	defer cancel()
	out, err := fn(tctx)
	return out, classifyStoreError(err)
}

// TriggerReport asks Restate to deliver callID's report. It is safe to call
// from every trigger: the idempotency key and the per-call Virtual Object
// guarantee a single delivery.
func (r *Reporter) TriggerReport(ctx context.Context, callID string) error {
	cleanID := strings.TrimSpace(callID)
	if cleanID == "" {
		return fmt.Errorf("reporter: call id is required")
	}
	cfg := r.cfgSrc.Current()
	if cfg == nil || strings.TrimSpace(cfg.RestateIngressURL) == "" {
		return fmt.Errorf("reporter: restate ingress url is not configured")
	}
	endpoint := fmt.Sprintf("%s/ProsperaReport/%s/DeliverReport/send", strings.TrimRight(cfg.RestateIngressURL, "/"), url.PathEscape(cleanID))
	if _, err := restateingress.Post(ctx, r.internalClient, endpoint, reportIdempotencyKey(cleanID), DeliverReportRequest{CallID: cleanID}, 1024); err != nil {
		return fmt.Errorf("reporter: enqueue report for %s: %w", cleanID, err)
	}
	return nil
}

// DeliverReportRequest represents the input payload to ProsperaReport.DeliverReport.
type DeliverReportRequest struct {
	CallID string `json:"call_id,omitempty"`
}

// claimOutput is the journaled result of the claim-report step.
type claimOutput struct {
	Skip   bool   `json:"skip"`
	Status string `json:"status,omitempty"`
}

// ProsperaReport is a Restate Virtual Object keyed by call_id. It is the only
// code path that posts a Call Summary.
type ProsperaReport struct {
	reporter *Reporter
}

// NewProsperaReport constructs a ProsperaReport Virtual Object handler.
func NewProsperaReport(reporter *Reporter) *ProsperaReport {
	return &ProsperaReport{reporter: reporter}
}

// Definition returns the Restate service definition for ProsperaReport.
func (p *ProsperaReport) Definition() restate.ServiceDefinition {
	return restate.Reflect(p,
		restate.WithInactivityTimeout(5*time.Minute),
		restate.WithAbortTimeout(5*time.Minute),
	)
}

// claim moves the report row to processing. Rows already delivered, or in a
// status this object does not own (voicemail's awaiting_recording), are skipped.
func (p *ProsperaReport) claim(rc restate.RunContext, callID string) (claimOutput, error) {
	st := p.reporter.store
	call, err := withStoreTimeout(rc, func(ctx context.Context) (*model.VoiceCall, error) { return st.GetCall(ctx, callID) })
	if err != nil {
		return claimOutput{}, err
	}
	if call == nil {
		return claimOutput{}, restate.ToTerminalError(fmt.Errorf("reporter: call %q not found", callID), restate.WithErrorCode(http.StatusNotFound))
	}
	if call.ReportStatus == reportStatusDelivered {
		return claimOutput{Skip: true, Status: call.ReportStatus}, nil
	}
	ok, err := withStoreTimeout(rc, func(ctx context.Context) (bool, error) {
		return st.TransitionReportStatus(ctx, callID, []string{reportStatusPending, reportStatusFailed, reportStatusProcessing}, reportStatusProcessing, "")
	})
	if err != nil {
		return claimOutput{}, err
	}
	if !ok {
		return claimOutput{Skip: true, Status: call.ReportStatus}, nil
	}
	return claimOutput{Status: reportStatusProcessing}, nil
}

func (p *ProsperaReport) loadCall(rc restate.RunContext, callID string) (*model.VoiceCall, error) {
	st := p.reporter.store
	call, err := withStoreTimeout(rc, func(ctx context.Context) (*model.VoiceCall, error) { return st.GetCall(ctx, callID) })
	if err != nil {
		return nil, err
	}
	if call == nil {
		return nil, restate.ToTerminalError(fmt.Errorf("reporter: call %q not found", callID), restate.WithErrorCode(http.StatusNotFound))
	}
	return call, nil
}

func (p *ProsperaReport) generateSummary(rc restate.RunContext, call *model.VoiceCall) (string, error) {
	cfg := p.reporter.cfgSrc.Current()
	summary, err := p.reporter.generateGeminiSummary(rc, cfg, model.BuildPostCallSummaryPrompt(call))
	if err != nil || strings.TrimSpace(summary) == "" {
		if err != nil {
			slog.WarnContext(rc, "reporter: gemini summary failed, using fallback", "call_id", call.CallID, "error", err)
		}
		summary = fmt.Sprintf("Call %s ended with state %q (reason: %q). Total turns: %d.", call.CallID, call.State, call.EndReason, len(call.Transcript))
	}
	return summary, nil
}

// buildCallSummary renders the Call Summary message body.
func buildCallSummary(call *model.VoiceCall, summary string) string {
	withNumber := call.ToNumber
	if call.Direction == directionInbound {
		withNumber = call.FromNumber
	}
	return fmt.Sprintf("📋 **Call Summary (%s)**\n**Direction**: %s | **With**: %s\n**State**: %s | **End Reason**: %s\n\n%s",
		call.CallID, call.Direction, withNumber, call.State, call.EndReason, summary)
}

// postSummary posts the Call Summary with the call recording attached. The
// recording is read without consuming it so a retried post still carries audio.
func (p *ProsperaReport) postSummary(ctx context.Context, call *model.VoiceCall, summary string) error {
	cfg := p.reporter.cfgSrc.Current()
	var attachments []discordAttachmentPayload
	if p.reporter.recordingProvider != nil {
		if wav := p.reporter.recordingProvider(call.CallID); len(wav) > 0 {
			attachments = []discordAttachmentPayload{{
				Filename:      call.CallID + ".wav",
				ContentType:   contentTypeAudioWAV,
				ContentBase64: base64.StdEncoding.EncodeToString(wav),
			}}
		}
	}
	channel := ResolveDiscordChannel(cfg, call.DiscordChannelID)
	return p.reporter.postDiscordMessage(ctx, cfg, channel, call.DiscordThreadID, buildCallSummary(call, summary), attachments)
}

func (p *ProsperaReport) markDelivered(ctx context.Context, callID, summary string) error {
	st := p.reporter.store
	_, err := withStoreTimeout(ctx, func(c context.Context) (bool, error) {
		return st.TransitionReportStatus(c, callID, []string{reportStatusProcessing, reportStatusFailed, reportStatusPending}, reportStatusDelivered, summary)
	})
	return err
}

// DeliverReport delivers one call's report as journaled steps, one side effect
// each: claim, flush the live transcript (load, post, advance), load the call,
// generate the summary, post it, mark it delivered, then drop the recording.
func (p *ProsperaReport) DeliverReport(ctx restate.ObjectContext, req DeliverReportRequest) error {
	callID := restate.Key(ctx)
	if callID == "" {
		callID = strings.TrimSpace(req.CallID)
	}
	if callID == "" {
		return restate.ToTerminalError(errors.New("reporter: call id is required"), restate.WithErrorCode(http.StatusBadRequest))
	}
	if p.reporter == nil || p.reporter.store == nil {
		return restate.TerminalErrorf("reporter: store is nil")
	}

	claim, err := restate.Run(ctx, func(rc restate.RunContext) (claimOutput, error) {
		return p.claim(rc, callID)
	}, restate.WithName("claim-report"))
	if err != nil {
		return err
	}
	if claim.Skip {
		ctx.Log().Info("reporter: report not claimable, skipping", "call_id", callID, "status", claim.Status)
		return nil
	}

	batch, err := restate.Run(ctx, func(rc restate.RunContext) (transcriptBatch, error) {
		return withStoreTimeout(rc, func(c context.Context) (transcriptBatch, error) { return p.reporter.pendingTranscript(c, callID) })
	}, restate.WithName("load-live-transcript"))
	if err != nil {
		return err
	}
	if batch.Content != "" {
		if _, stepErr := runStep(ctx, "post-live-transcript", func(c context.Context) (struct{}, error) {
			return struct{}{}, p.reporter.postTranscriptBatch(c, batch)
		}); stepErr != nil {
			return stepErr
		}
	}
	if batch.To > batch.From {
		if _, stepErr := runStep(ctx, "advance-transcript-cursor", func(c context.Context) (struct{}, error) {
			_, advErr := withStoreTimeout(c, func(c context.Context) (bool, error) {
				return p.reporter.store.AdvanceLiveTranscriptCursor(c, callID, batch.From, batch.To)
			})
			return struct{}{}, advErr
		}); stepErr != nil {
			return stepErr
		}
	}

	call, err := restate.Run(ctx, func(rc restate.RunContext) (*model.VoiceCall, error) {
		return p.loadCall(rc, callID)
	}, restate.WithName("load-report-call"))
	if err != nil {
		return err
	}
	summary, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
		return p.generateSummary(rc, call)
	}, restate.WithName("generate-summary"))
	if err != nil {
		return err
	}

	// Post first, record delivery second, drop the recording last: a failed
	// mark replays the journaled post instead of posting again, and a failed
	// post retries with the recording still available.
	if _, stepErr := runStep(ctx, "post-call-summary", func(c context.Context) (struct{}, error) {
		return struct{}{}, p.postSummary(c, call, summary)
	}); stepErr != nil {
		return stepErr
	}
	if _, stepErr := restateingress.RunInSpan(ctx, "prospera.report.mark_reported", func(stepCtx context.Context, _ restate.RunContext) (restate.Void, error) {
		return restate.Void{}, p.markDelivered(stepCtx, callID, summary)
	}, restate.WithName("mark-delivered")); stepErr != nil {
		return stepErr
	}
	_, err = runStep(ctx, "delete-recording", func(context.Context) (struct{}, error) {
		p.reporter.deleteRecording(callID)
		return struct{}{}, nil
	})
	return err
}

// runStep journals one side-effecting step that has no result.
func runStep[T any](rc restate.ObjectContext, spanName string, fn func(context.Context) (T, error)) (T, restate.TerminalError) {
	return restateingress.RunInSpan(rc, spanName, func(ctx context.Context, _ restate.RunContext) (T, error) {
		return fn(ctx)
	}, restate.WithName(spanName))
}

func (r *Reporter) deleteRecording(callID string) {
	if r.recordingDeleter != nil {
		r.recordingDeleter(callID)
	}
}

// ProcessVoicemailRecording handles a completed Tier 3 voicemail recording.
func (r *Reporter) ProcessVoicemailRecording(ctx context.Context, callID, recordingURL string) error {
	cfg := r.cfgSrc.Current()
	if r.store == nil {
		return fmt.Errorf("reporter: store is nil")
	}

	call, err := r.store.GetCall(ctx, callID)
	if err != nil {
		return fmt.Errorf("reporter: get call %q: %w", callID, err)
	}
	if call == nil {
		return fmt.Errorf("reporter: call %q not found", callID)
	}

	// Allow transition from awaiting_recording or previously failed attempts to re-enter processing on retry
	ok, err := r.store.TransitionReportStatus(ctx, callID, []string{"awaiting_recording", reportStatusFailed}, reportStatusProcessing, "")
	if err != nil {
		return fmt.Errorf("reporter: transition status to processing: %w", err)
	}
	if !ok {
		return nil
	}

	if setErr := r.store.SetVoicemailRecording(ctx, callID, recordingURL); setErr != nil {
		slog.ErrorContext(ctx, "reporter: set voicemail recording", "error", setErr)
	}

	audioBytes, contentType, err := r.twilio.DownloadRecording(ctx, recordingURL)
	if err != nil {
		if _, transErr := r.store.TransitionReportStatus(ctx, callID, []string{reportStatusProcessing}, reportStatusFailed, "download failed: "+err.Error()); transErr != nil {
			slog.ErrorContext(ctx, "reporter: transition status to failed", "error", transErr)
		}
		return fmt.Errorf("reporter: download recording: %w", err)
	}
	if contentType == "" {
		contentType = contentTypeAudioWAV
	}

	transcript, tErr := r.transcribeAudio(ctx, cfg, audioBytes, contentType)
	if tErr != nil || strings.TrimSpace(transcript) == "" {
		transcript = "(Voicemail transcription unavailable)"
	}

	channel := ResolveDiscordChannel(cfg, call.DiscordChannelID)
	att := discordAttachmentPayload{
		Filename:      "voicemail.wav",
		ContentType:   contentType,
		ContentBase64: base64.StdEncoding.EncodeToString(audioBytes),
	}

	msgContent := fmt.Sprintf("📼 **Voicemail Received**\n**From**: %s | **Call ID**: %s\n**Transcript**:\n> %s",
		call.FromNumber, call.CallID, transcript)

	if err := r.postDiscordMessage(ctx, cfg, channel, call.DiscordThreadID, msgContent, []discordAttachmentPayload{att}); err != nil {
		if _, transErr := r.store.TransitionReportStatus(ctx, callID, []string{reportStatusProcessing}, reportStatusFailed, "discord post failed: "+err.Error()); transErr != nil {
			slog.ErrorContext(ctx, "reporter: transition status to failed", "error", transErr)
		}
		return err
	}

	if _, transErr := r.store.TransitionReportStatus(ctx, callID, []string{reportStatusProcessing}, reportStatusDelivered, transcript); transErr != nil {
		slog.ErrorContext(ctx, "reporter: transition status to delivered", "error", transErr)
	}
	return nil
}

// DeliverInboundSMS formats an inbound SMS/MMS and delivers it to Discord.
func (r *Reporter) DeliverInboundSMS(ctx context.Context, msg model.VoiceMessage) error {
	cfg := r.cfgSrc.Current()
	channel := ResolveDiscordChannel(cfg, "")

	var sb strings.Builder
	fmt.Fprintf(&sb, "💬 **Inbound SMS Received**\n**From**: %s | **To**: %s\n\n%s", msg.FromNumber, msg.ToNumber, msg.Body)
	if len(msg.MediaURLs) > 0 {
		sb.WriteString("\n\n**Media URLs**:\n")
		for _, u := range msg.MediaURLs {
			fmt.Fprintf(&sb, "- %s\n", u)
		}
	}

	if err := r.postDiscordMessage(ctx, cfg, channel, "", sb.String(), nil); err != nil {
		return err
	}

	if r.store != nil {
		if markErr := r.store.MarkVoiceMessageDelivered(ctx, msg.MessageSID); markErr != nil {
			slog.ErrorContext(ctx, "reporter: mark voice message delivered", "error", markErr)
		}
	}
	return nil
}
