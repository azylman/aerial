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
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

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
}

// ReporterOption configures Reporter.
type ReporterOption func(*Reporter)

// WithReporterRecordingProvider configures an optional callback to fetch an in-memory WAV recording for a call.
func WithReporterRecordingProvider(fn func(callID string) []byte) ReporterOption {
	return func(r *Reporter) {
		r.recordingProvider = fn
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

// FlushLiveTranscript batches unsent transcript turns and delivers them to Discord.
func (r *Reporter) FlushLiveTranscript(ctx context.Context, callID string) error {
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

	totalTurns := len(call.Transcript)
	cursor := call.LiveTranscriptCursor
	if cursor >= totalTurns {
		return nil
	}

	newTurns := call.Transcript[cursor:totalTurns]

	var sb strings.Builder
	for _, turn := range newTurns {
		speaker := "Prospera"
		switch strings.ToLower(turn.Role) {
		case roleUser, "caller", "human":
			speaker = "Caller"
		case "system":
			speaker = "System"
		}
		text := strings.TrimSpace(turn.Text)
		if text != "" {
			fmt.Fprintf(&sb, "**%s**: %s\n", speaker, text)
		}
	}

	content := strings.TrimSpace(sb.String())
	if content == "" {
		if _, advErr := r.store.AdvanceLiveTranscriptCursor(ctx, callID, cursor, totalTurns); advErr != nil {
			slog.ErrorContext(ctx, "reporter: advance empty transcript cursor", "error", advErr)
		}
		return nil
	}

	channel := ResolveDiscordChannel(cfg, call.DiscordChannelID)
	if err := r.postDiscordMessage(ctx, cfg, channel, call.DiscordThreadID, content, nil); err != nil {
		return err
	}

	if _, advErr := r.store.AdvanceLiveTranscriptCursor(ctx, callID, cursor, totalTurns); advErr != nil {
		slog.ErrorContext(ctx, "reporter: advance transcript cursor", "error", advErr)
	}
	return nil
}

// DeliverReportRequest represents the input payload to ProsperaReport.DeliverReport.
type DeliverReportRequest struct {
	CallID string `json:"call_id,omitempty"`
}

// DeliverReportStep1Output holds the result of load-and-mark-processing.
type DeliverReportStep1Output struct {
	AlreadyDelivered bool             `json:"already_delivered"`
	Call             *model.VoiceCall `json:"call,omitempty"`
}

// ProsperaReport is a Restate Virtual Object keyed by call_id that delivers call reports durably.
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

func (p *ProsperaReport) step1LoadAndMarkProcessing(rc restate.RunContext, callID string) (DeliverReportStep1Output, error) {
	if p.reporter == nil || p.reporter.store == nil {
		return DeliverReportStep1Output{}, restate.TerminalErrorf("reporter: store is nil")
	}

	call, err := p.reporter.store.GetCall(rc, callID)
	if err != nil {
		if errors.Is(err, store.ErrCallNotFound) {
			return DeliverReportStep1Output{}, restate.ToTerminalError(err, restate.WithErrorCode(http.StatusNotFound))
		}
		return DeliverReportStep1Output{}, err
	}
	if call == nil {
		return DeliverReportStep1Output{}, restate.ToTerminalError(fmt.Errorf("reporter: call %q not found", callID), restate.WithErrorCode(http.StatusNotFound))
	}

	if call.ReportStatus == reportStatusDelivered {
		return DeliverReportStep1Output{AlreadyDelivered: true, Call: call}, nil
	}

	// Accept pending, failed, or processing on crash replay
	ok, err := p.reporter.store.TransitionReportStatus(rc, callID, []string{reportStatusPending, reportStatusFailed, reportStatusProcessing}, reportStatusProcessing, "")
	if err != nil {
		return DeliverReportStep1Output{}, err
	}
	if !ok {
		// Row could not be transitioned to processing (e.g. awaiting_recording, or concurrently delivered)
		reloaded, reloadErr := p.reporter.store.GetCall(rc, callID)
		if reloadErr == nil && reloaded != nil && reloaded.ReportStatus == reportStatusDelivered {
			return DeliverReportStep1Output{AlreadyDelivered: true, Call: reloaded}, nil
		}
		return DeliverReportStep1Output{AlreadyDelivered: true, Call: call}, nil
	}

	// Flush any unsent live transcript turns before report
	if flushErr := p.reporter.FlushLiveTranscript(rc, callID); flushErr != nil {
		slog.ErrorContext(rc, "reporter: flush live transcript before report", "call_id", callID, "error", flushErr)
	}

	updatedCall, err := p.reporter.store.GetCall(rc, callID)
	if err == nil && updatedCall != nil {
		call = updatedCall
	}

	return DeliverReportStep1Output{AlreadyDelivered: false, Call: call}, nil
}

func (p *ProsperaReport) step2GenerateSummary(rc restate.RunContext, call *model.VoiceCall) (string, error) {
	cfg := p.reporter.cfgSrc.Current()
	prompt := model.BuildPostCallSummaryPrompt(call)
	geminiSummary, genErr := p.reporter.generateGeminiSummary(rc, cfg, prompt)
	if genErr != nil || strings.TrimSpace(geminiSummary) == "" {
		geminiSummary = fmt.Sprintf("Call %s ended with state %q (reason: %q). Total turns: %d.", call.CallID, call.State, call.EndReason, len(call.Transcript))
	}
	return geminiSummary, nil
}

func (p *ProsperaReport) step3DeliverDiscordAndMarkDelivered(rc restate.RunContext, call *model.VoiceCall, summary string) (struct{}, error) {
	cfg := p.reporter.cfgSrc.Current()
	withNumber := call.ToNumber
	if call.Direction == directionInbound {
		withNumber = call.FromNumber
	}

	channel := ResolveDiscordChannel(cfg, call.DiscordChannelID)
	msgContent := fmt.Sprintf("📋 **Call Summary (%s)**\n**Direction**: %s | **With**: %s\n**State**: %s | **End Reason**: %s\n\n%s",
		call.CallID, call.Direction, withNumber, call.State, call.EndReason, summary)

	var attachments []discordAttachmentPayload
	if p.reporter.recordingProvider != nil {
		if wavBytes := p.reporter.recordingProvider(call.CallID); len(wavBytes) > 0 {
			attachments = []discordAttachmentPayload{
				{
					Filename:      call.CallID + ".wav",
					ContentType:   contentTypeAudioWAV,
					ContentBase64: base64.StdEncoding.EncodeToString(wavBytes),
				},
			}
		}
	}

	if postErr := p.reporter.postDiscordMessage(rc, cfg, channel, call.DiscordThreadID, msgContent, attachments); postErr != nil {
		if _, transErr := p.reporter.store.TransitionReportStatus(rc, call.CallID, []string{reportStatusProcessing}, reportStatusFailed, "discord post failed: "+postErr.Error()); transErr != nil {
			slog.ErrorContext(rc, "reporter: transition report status to failed", "error", transErr)
		}
		return struct{}{}, postErr
	}

	if _, transErr := p.reporter.store.TransitionReportStatus(rc, call.CallID, []string{reportStatusProcessing, reportStatusFailed, reportStatusPending}, reportStatusDelivered, summary); transErr != nil {
		slog.ErrorContext(rc, "reporter: transition report status to delivered", "error", transErr)
		return struct{}{}, transErr
	}
	return struct{}{}, nil
}

// DeliverReport runs a 3-step journaled post-call report delivery chain.
func (p *ProsperaReport) DeliverReport(ctx restate.ObjectContext, req DeliverReportRequest) error {
	callID := restate.Key(ctx)
	if callID == "" {
		callID = strings.TrimSpace(req.CallID)
	}

	// Step 1: load-and-mark-processing
	step1, err := restate.Run(ctx, func(rc restate.RunContext) (DeliverReportStep1Output, error) {
		return p.step1LoadAndMarkProcessing(rc, callID)
	}, restate.WithName("load-and-mark-processing"))
	if err != nil {
		return err
	}
	if step1.AlreadyDelivered {
		return nil
	}

	call := step1.Call

	// Step 2: generate-summary
	summary, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
		return p.step2GenerateSummary(rc, call)
	}, restate.WithName("generate-summary"))
	if err != nil {
		return err
	}

	// Step 3: deliver-discord-and-mark-delivered
	_, err = restate.Run(ctx, func(rc restate.RunContext) (struct{}, error) {
		return p.step3DeliverDiscordAndMarkDelivered(rc, call, summary)
	}, restate.WithName("deliver-discord-and-mark-delivered"))
	if err != nil {
		return err
	}

	restate.ClearAll(ctx)
	return nil
}

// DeliverCallReport generates a post-call summary using Gemini with tools: [] and posts it to Discord.
func (r *Reporter) DeliverCallReport(ctx context.Context, callID string) error {
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

	// Allow transition from pending or previously failed attempts to re-enter processing on retry
	ok, err := r.store.TransitionReportStatus(ctx, callID, []string{reportStatusPending, reportStatusFailed}, reportStatusProcessing, "")
	if err != nil {
		return fmt.Errorf("reporter: transition status to processing: %w", err)
	}
	if !ok {
		return nil
	}

	// Flush any unsent live transcript turns before posting summary.
	if flushErr := r.FlushLiveTranscript(ctx, callID); flushErr != nil {
		slog.ErrorContext(ctx, "reporter: flush live transcript before report", "error", flushErr)
	}

	prompt := model.BuildPostCallSummaryPrompt(call)
	summary, err := r.generateGeminiSummary(ctx, cfg, prompt)
	if err != nil || strings.TrimSpace(summary) == "" {
		summary = fmt.Sprintf("Call %s ended with state %q (reason: %q). Total turns: %d.", call.CallID, call.State, call.EndReason, len(call.Transcript))
	}

	withNumber := call.ToNumber
	if call.Direction == directionInbound {
		withNumber = call.FromNumber
	}

	channel := ResolveDiscordChannel(cfg, call.DiscordChannelID)
	msgContent := fmt.Sprintf("📋 **Call Summary (%s)**\n**Direction**: %s | **With**: %s\n**State**: %s | **End Reason**: %s\n\n%s",
		call.CallID, call.Direction, withNumber, call.State, call.EndReason, summary)

	var attachments []discordAttachmentPayload
	if r.recordingProvider != nil {
		if wavBytes := r.recordingProvider(callID); len(wavBytes) > 0 {
			attachments = []discordAttachmentPayload{
				{
					Filename:      call.CallID + ".wav",
					ContentType:   contentTypeAudioWAV,
					ContentBase64: base64.StdEncoding.EncodeToString(wavBytes),
				},
			}
		}
	}

	if postErr := r.postDiscordMessage(ctx, cfg, channel, call.DiscordThreadID, msgContent, attachments); postErr != nil {
		if _, transErr := r.store.TransitionReportStatus(ctx, callID, []string{reportStatusProcessing}, reportStatusFailed, "discord post failed: "+postErr.Error()); transErr != nil {
			slog.ErrorContext(ctx, "reporter: transition report status to failed", "error", transErr)
		}
		return postErr
	}
	if _, transErr := r.store.TransitionReportStatus(ctx, callID, []string{reportStatusProcessing}, reportStatusDelivered, summary); transErr != nil {
		slog.ErrorContext(ctx, "reporter: transition report status to delivered", "error", transErr)
	}
	return nil
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
