package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ToolHandler coordinates execution of Prospera MCP tools.
type ToolHandler struct {
	cfg        *Config
	store      Store
	twilio     TwilioClient
	sessionMgr *CallSessionManager
	reporter   *Reporter
}

// NewToolHandler creates a new ToolHandler.
func NewToolHandler(cfg *Config, store Store, twilio TwilioClient, sessionMgr *CallSessionManager, reporter *Reporter) *ToolHandler {
	return &ToolHandler{
		cfg:        cfg,
		store:      store,
		twilio:     twilio,
		sessionMgr: sessionMgr,
		reporter:   reporter,
	}
}

// InitiateCallArgs defines input arguments for initiate_call.
type InitiateCallArgs struct {
	To               string    `json:"to" jsonschema:"Destination phone number in E.164 format"`
	Message          string    `json:"message,omitempty" jsonschema:"Opening spoken greeting or task instruction"`
	Mode             string    `json:"mode,omitempty" jsonschema:"Operating mode: conversation or notify"`
	DTMFSequence     string    `json:"dtmf_sequence,omitempty" jsonschema:"Optional pre-connect DTMF sequence"`
	DiscordChannelID string    `json:"discord_channel_id,omitempty" jsonschema:"Target Discord channel for transcripts and updates"`
	DiscordThreadID  string    `json:"discord_thread_id,omitempty" jsonschema:"Target Discord thread for updates"`
	Brief            CallBrief `json:"brief,omitempty" jsonschema:"Call brief defining intent and approval firewall"`
}

// InitiateCallOutput defines return payload for initiate_call.
type InitiateCallOutput struct {
	CallID          string `json:"call_id"`
	ProviderCallSID string `json:"provider_call_sid"`
	Status          string `json:"status"`
}

// SteerCallArgs defines input arguments for steer_call.
type SteerCallArgs struct {
	CallID  string `json:"call_id" jsonschema:"Active call ID to steer"`
	Message string `json:"message" jsonschema:"Guidance or message text to inject (max 500 chars)"`
	Mode    string `json:"mode,omitempty" jsonschema:"Steer mode: guidance or say"`
}

// SteerCallOutput defines return payload for steer_call.
type SteerCallOutput struct {
	CallID string `json:"call_id"`
	Status string `json:"status"`
}

// EndCallArgs defines input arguments for end_call.
type EndCallArgs struct {
	CallID string `json:"call_id" jsonschema:"Call ID to hang up and terminate"`
}

// EndCallOutput defines return payload for end_call.
type EndCallOutput struct {
	CallID string `json:"call_id"`
	Status string `json:"status"`
}

// GetCallStatusArgs defines input arguments for get_call_status.
type GetCallStatusArgs struct {
	CallID string `json:"call_id" jsonschema:"Call ID to query"`
}

// GetCallStatusOutput defines return payload for get_call_status.
type GetCallStatusOutput struct {
	Call *VoiceCall `json:"call,omitempty"`
}

// ListCallsArgs defines input arguments for list_calls.
type ListCallsArgs struct {
	Limit int `json:"limit,omitempty" jsonschema:"Maximum calls to return (default 20)"`
}

// ListCallsOutput defines return payload for list_calls.
type ListCallsOutput struct {
	Calls []VoiceCall `json:"calls"`
}

// SendSMSArgs defines input arguments for send_sms.
type SendSMSArgs struct {
	To            string `json:"to" jsonschema:"Destination phone number in E.164 format"`
	Body          string `json:"body" jsonschema:"Message body text"`
	From          string `json:"from,omitempty" jsonschema:"Optional sender phone number in E.164 format"`
	RelatedCallID string `json:"related_call_id,omitempty" jsonschema:"Optional associated call ID"`
}

// SendSMSOutput defines return payload for send_sms.
type SendSMSOutput struct {
	MessageSID string `json:"message_sid"`
	Status     string `json:"status"`
}

// ListSMSArgs defines input arguments for list_sms.
type ListSMSArgs struct {
	Limit int `json:"limit,omitempty" jsonschema:"Maximum messages to return (default 20)"`
}

// ListSMSOutput defines return payload for list_sms.
type ListSMSOutput struct {
	Messages []VoiceMessage `json:"messages"`
}

// EnsurePhoneNumberArgs defines input arguments for ensure_phone_number.
type EnsurePhoneNumberArgs struct {
	AreaCode string `json:"area_code,omitempty" jsonschema:"Preferred 3-digit US area code (default 510)"`
}

// EnsurePhoneNumberOutput defines return payload for ensure_phone_number.
type EnsurePhoneNumberOutput struct {
	PhoneNumber string `json:"phone_number"`
	Status      string `json:"status"`
}

// InitiateCall executes the initiate_call tool.
func (h *ToolHandler) InitiateCall(ctx context.Context, args InitiateCallArgs) (InitiateCallOutput, error) {
	fromNumber := ""
	if h.cfg != nil {
		fromNumber = h.cfg.TwilioFromNumber
	}

	if strings.TrimSpace(args.Mode) != "" {
		args.Brief.Mode = strings.TrimSpace(args.Mode)
	}

	normTo, sanitizedBrief, err := ValidateInitiateCallArgs(args.To, fromNumber, args.Message, args.Brief)
	if err != nil {
		return InitiateCallOutput{}, fmt.Errorf("initiate_call: validation error: %w", err)
	}

	channelID := args.DiscordChannelID
	if channelID == "" && h.cfg != nil {
		channelID = h.cfg.DefaultDiscordChannel
	}
	if channelID == "" {
		channelID = "prospera"
	}

	callID := "call-" + uuid.NewString()
	call := VoiceCall{
		CallID:           callID,
		Direction:        "outbound",
		State:            "initiated",
		FromNumber:       fromNumber,
		ToNumber:         normTo,
		DiscordChannelID: channelID,
		DiscordThreadID:  args.DiscordThreadID,
		Brief:            sanitizedBrief,
		ReportStatus:     "pending",
		StartedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	maxConcurrent := 5
	if h.cfg != nil && h.cfg.MaxConcurrentCalls > 0 {
		maxConcurrent = h.cfg.MaxConcurrentCalls
	}

	if err := h.store.CreateOutboundCall(ctx, call, maxConcurrent); err != nil {
		return InitiateCallOutput{}, fmt.Errorf("initiate_call: store create call: %w", err)
	}

	twimlURL := ""
	statusURL := ""
	amdURL := ""
	if h.cfg != nil && h.cfg.PublicWebhookBaseURL != "" {
		baseURL := strings.TrimRight(h.cfg.PublicWebhookBaseURL, "/")
		twimlURL = fmt.Sprintf("%s/api/webhooks/twilio/voice?call_id=%s", baseURL, url.QueryEscape(callID))
		statusURL = fmt.Sprintf("%s/api/webhooks/twilio/status?call_id=%s", baseURL, url.QueryEscape(callID))
		amdURL = fmt.Sprintf("%s/api/webhooks/twilio/amd?call_id=%s", baseURL, url.QueryEscape(callID))
	}

	providerSID, err := h.twilio.CreateCall(ctx, CreateCallRequest{
		To:                     normTo,
		From:                   fromNumber,
		TwiMLURL:               twimlURL,
		StatusCallback:         statusURL,
		AsyncAmdStatusCallback: amdURL,
		SendDigits:             args.DTMFSequence,
		MachineDetection:       "DetectMessageEnd",
		AsyncAmd:               "true",
	})
	if err != nil {
		if updateErr := h.store.UpdateCallState(ctx, callID, "failed", err.Error(), ""); updateErr != nil {
			log.Printf("prospera: update call state failed: %v", updateErr)
		}
		return InitiateCallOutput{}, fmt.Errorf("initiate_call: twilio create call: %w", err)
	}

	if bindErr := h.store.BindProviderCallSID(ctx, callID, providerSID); bindErr != nil {
		log.Printf("prospera: bind provider sid failed: %v", bindErr)
	}
	return InitiateCallOutput{
		CallID:          callID,
		ProviderCallSID: providerSID,
		Status:          "initiated",
	}, nil
}

// SteerCall executes the steer_call tool.
func (h *ToolHandler) SteerCall(_ context.Context, args SteerCallArgs) (SteerCallOutput, error) {
	callID := strings.TrimSpace(args.CallID)
	if callID == "" {
		return SteerCallOutput{}, fmt.Errorf("steer_call: call_id is required")
	}

	msg := strings.TrimSpace(args.Message)
	if msg == "" {
		return SteerCallOutput{}, fmt.Errorf("steer_call: message is required")
	}
	if len(msg) > 500 {
		return SteerCallOutput{}, fmt.Errorf("steer_call: message exceeds 500 characters limit")
	}

	mode := strings.TrimSpace(args.Mode)
	if mode == "" {
		mode = "guidance"
	}

	if h.sessionMgr == nil {
		return SteerCallOutput{}, fmt.Errorf("steer_call: session manager unavailable")
	}

	if err := h.sessionMgr.SteerCall(callID, msg, mode); err != nil {
		return SteerCallOutput{}, fmt.Errorf("steer_call: %w", err)
	}

	return SteerCallOutput{
		CallID: callID,
		Status: "steered",
	}, nil
}

// EndCall executes the end_call tool.
func (h *ToolHandler) EndCall(ctx context.Context, args EndCallArgs) (EndCallOutput, error) {
	callID := strings.TrimSpace(args.CallID)
	if callID == "" {
		return EndCallOutput{}, fmt.Errorf("end_call: call_id is required")
	}

	call, err := h.store.GetCall(ctx, callID)
	if err != nil {
		return EndCallOutput{}, fmt.Errorf("end_call: get call: %w", err)
	}
	if call == nil {
		return EndCallOutput{}, fmt.Errorf("end_call: call %q not found", callID)
	}

	if h.sessionMgr != nil {
		if endErr := h.sessionMgr.EndCall(callID); endErr != nil {
			log.Printf("prospera: session end call: %v", endErr)
		}
	}

	if call.ProviderCallSID != "" && h.twilio != nil {
		if hangupErr := h.twilio.HangupCall(ctx, call.ProviderCallSID); hangupErr != nil {
			log.Printf("prospera: twilio hangup: %v", hangupErr)
		}
	}

	if updateErr := h.store.UpdateCallState(ctx, callID, "completed", "operator_requested", ""); updateErr != nil {
		log.Printf("prospera: update call state: %v", updateErr)
	}

	if h.reporter != nil {
		if repErr := h.reporter.DeliverCallReport(ctx, callID); repErr != nil {
			log.Printf("prospera: deliver call report: %v", repErr)
		}
	}

	return EndCallOutput{
		CallID: callID,
		Status: "ended",
	}, nil
}

// GetCallStatus executes the get_call_status tool.
func (h *ToolHandler) GetCallStatus(ctx context.Context, args GetCallStatusArgs) (GetCallStatusOutput, error) {
	callID := strings.TrimSpace(args.CallID)
	if callID == "" {
		return GetCallStatusOutput{}, fmt.Errorf("get_call_status: call_id is required")
	}

	call, err := h.store.GetCall(ctx, callID)
	if err != nil {
		return GetCallStatusOutput{}, fmt.Errorf("get_call_status: %w", err)
	}
	if call == nil {
		return GetCallStatusOutput{}, fmt.Errorf("get_call_status: call %q not found", callID)
	}

	return GetCallStatusOutput{Call: call}, nil
}

// ListCalls executes the list_calls tool.
func (h *ToolHandler) ListCalls(ctx context.Context, args ListCallsArgs) (ListCallsOutput, error) {
	limit := args.Limit
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	calls, err := h.store.ListCalls(ctx, limit)
	if err != nil {
		return ListCallsOutput{}, fmt.Errorf("list_calls: %w", err)
	}
	if calls == nil {
		calls = []VoiceCall{}
	}

	return ListCallsOutput{Calls: calls}, nil
}

// SendSMS executes the send_sms tool.
func (h *ToolHandler) SendSMS(ctx context.Context, args SendSMSArgs) (SendSMSOutput, error) {
	normTo, err := NormalizeE164(args.To)
	if err != nil {
		return SendSMSOutput{}, fmt.Errorf("send_sms: invalid to number: %w", err)
	}

	from := strings.TrimSpace(args.From)
	if from == "" && h.cfg != nil {
		from = h.cfg.TwilioFromNumber
	}
	if from == "" {
		return SendSMSOutput{}, fmt.Errorf("send_sms: from number is required")
	}
	normFrom, err := NormalizeE164(from)
	if err != nil {
		return SendSMSOutput{}, fmt.Errorf("send_sms: invalid from number: %w", err)
	}

	body := strings.TrimSpace(args.Body)
	if body == "" {
		return SendSMSOutput{}, fmt.Errorf("send_sms: body cannot be empty")
	}

	msgSID, err := h.twilio.SendSMS(ctx, normTo, normFrom, body)
	if err != nil {
		return SendSMSOutput{}, fmt.Errorf("send_sms: twilio send error: %w", err)
	}

	msg := VoiceMessage{
		MessageSID:       msgSID,
		Direction:        "outbound",
		FromNumber:       normFrom,
		ToNumber:         normTo,
		Body:             body,
		RelatedCallID:    args.RelatedCallID,
		DiscordDelivered: true,
		CreatedAt:        time.Now().UTC(),
	}
	if _, saveErr := h.store.SaveVoiceMessage(ctx, msg); saveErr != nil {
		log.Printf("prospera: save voice message: %v", saveErr)
	}

	return SendSMSOutput{
		MessageSID: msgSID,
		Status:     "sent",
	}, nil
}

// ListSMS executes the list_sms tool.
func (h *ToolHandler) ListSMS(ctx context.Context, args ListSMSArgs) (ListSMSOutput, error) {
	limit := args.Limit
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	msgs, err := h.store.ListVoiceMessages(ctx, limit)
	if err != nil {
		return ListSMSOutput{}, fmt.Errorf("list_sms: %w", err)
	}
	if msgs == nil {
		msgs = []VoiceMessage{}
	}

	return ListSMSOutput{Messages: msgs}, nil
}

// EnsurePhoneNumber executes the ensure_phone_number tool.
func (h *ToolHandler) EnsurePhoneNumber(ctx context.Context, args EnsurePhoneNumberArgs) (EnsurePhoneNumberOutput, error) {
	if h.twilio == nil {
		return EnsurePhoneNumberOutput{}, fmt.Errorf("ensure_phone_number: twilio client unavailable")
	}

	areaCode := strings.TrimSpace(args.AreaCode)
	if areaCode == "" {
		areaCode = "510"
	}

	webhookBaseURL := ""
	if h.cfg != nil && h.cfg.PublicWebhookBaseURL != "" {
		webhookBaseURL = strings.TrimRight(h.cfg.PublicWebhookBaseURL, "/") + "/api/webhooks"
	}

	number, err := h.twilio.EnsurePhoneNumber(ctx, areaCode, webhookBaseURL)
	if err != nil {
		return EnsurePhoneNumberOutput{}, fmt.Errorf("ensure_phone_number: %w", err)
	}

	return EnsurePhoneNumberOutput{
		PhoneNumber: number,
		Status:      "configured",
	}, nil
}

// RegisterProsperaMCPTools registers all 8 Prospera control tools onto the given MCP server.
func RegisterProsperaMCPTools(s *mcp.Server, h *ToolHandler) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "initiate_call",
		Description: "Initiate an outbound voice call governed by a CallBrief approval firewall and bridged to Gemini Live.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args InitiateCallArgs) (*mcp.CallToolResult, InitiateCallOutput, error) {
		out, err := h.InitiateCall(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "steer_call",
		Description: "Inject guidance or direct spoken sentences into an active voice call session.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args SteerCallArgs) (*mcp.CallToolResult, SteerCallOutput, error) {
		out, err := h.SteerCall(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "end_call",
		Description: "Terminate an active phone call, hang up the provider leg, and trigger post-call summarization.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args EndCallArgs) (*mcp.CallToolResult, EndCallOutput, error) {
		out, err := h.EndCall(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_call_status",
		Description: "Retrieve current state, live transcript, and summary report for a call by ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args GetCallStatusArgs) (*mcp.CallToolResult, GetCallStatusOutput, error) {
		out, err := h.GetCallStatus(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_calls",
		Description: "List recent voice calls and their current execution state.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ListCallsArgs) (*mcp.CallToolResult, ListCallsOutput, error) {
		out, err := h.ListCalls(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "send_sms",
		Description: "Send an outbound SMS message via Twilio and record it in Prospera storage.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args SendSMSArgs) (*mcp.CallToolResult, SendSMSOutput, error) {
		out, err := h.SendSMS(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_sms",
		Description: "List recent inbound and outbound SMS messages.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ListSMSArgs) (*mcp.CallToolResult, ListSMSOutput, error) {
		out, err := h.ListSMS(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "ensure_phone_number",
		Description: "Ensure an active Twilio phone number is provisioned and configured with Prospera webhook URLs.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args EnsurePhoneNumberArgs) (*mcp.CallToolResult, EnsurePhoneNumberOutput, error) {
		out, err := h.EnsurePhoneNumber(ctx, args)
		return nil, out, err
	})
}
