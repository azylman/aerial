package tools

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/delivery"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

type testToolEnv struct {
	handler *ToolHandler
	store   *fakes.FakeStore
	twilio  *fakes.FakeTwilio
}

func setupTestToolEnv() testToolEnv {
	cfg := &config.Config{
		TwilioFromNumber:      "+15105550100",
		PublicWebhookBaseURL:  "https://webhooks.example.com",
		DefaultDiscordChannel: "prospera",
		MaxConcurrentCalls:    5,
	}
	store := fakes.NewFakeStore()
	twilioMock := &fakes.FakeTwilio{}
	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
	sessionMgr := mediastream.NewCallSessionManager(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, nil, tokenMgr)
	reporter := delivery.NewReporter(fakes.StaticConfig{Cfg: cfg}, store, twilioMock)

	handler := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, sessionMgr, reporter)
	return testToolEnv{
		handler: handler,
		store:   store,
		twilio:  twilioMock,
	}
}

type toolHandlerInitiateCallTestCase struct {
	name               string
	args               InitiateCallArgs
	twilioErr          string
	updateStateErr     string
	storeErr           string
	bindErr            string
	wantErr            bool
	wantErrContains    string
	wantErrNotContains string
	wantMode           string
	wantStatus         string
}

func initCallCase(name string, args InitiateCallArgs, twilioErr, updateStateErr, storeErr, bindErr string, wantErr bool, wantErrContains, wantErrNotContains, wantMode, wantStatus string) toolHandlerInitiateCallTestCase {
	return toolHandlerInitiateCallTestCase{name: name, args: args, twilioErr: twilioErr, updateStateErr: updateStateErr, storeErr: storeErr, bindErr: bindErr, wantErr: wantErr, wantErrContains: wantErrContains, wantErrNotContains: wantErrNotContains, wantMode: wantMode, wantStatus: wantStatus}
}

func TestToolHandler_InitiateCall(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerInitiateCallTestCase{
		initCallCase("valid initiate call", InitiateCallArgs{To: "+15105550199", Message: "Hello, this is an automated confirmation call."}, "", "", "", "", false, "", "", "conversation", "initiated"),
		initCallCase("valid initiate call with partial brief", InitiateCallArgs{To: "+15105550199", Message: "Hello, this is an automated confirmation call.", Brief: model.CallBrief{Context: "Context details"}}, "", "", "", "", false, "", "", "conversation", "initiated"),
		initCallCase("valid initiate call with notify mode", InitiateCallArgs{To: "+15105550199", Message: "Hello, notification only.", Mode: "notify"}, "", "", "", "", false, "", "", "notify", "initiated"),
		initCallCase("invalid phone number", InitiateCallArgs{To: "not-a-number", Message: "Hello"}, "", "", "", "", true, "", "", "", ""),
		initCallCase("empty message and task", InitiateCallArgs{To: "+15105550199"}, "", "", "", "", true, "", "", "", ""),
		initCallCase("twilio error with state update failure", InitiateCallArgs{To: "+15105550199", Message: "Hello, this is an automated confirmation call."}, "twilio network error", "db write failed", "", "", true, "twilio network error", "db write failed", "", ""),
		initCallCase("store error on initiate", InitiateCallArgs{To: "+15105550199", Message: "hello"}, "", "", "db error", "", true, "db error", "", "", ""),
		initCallCase("twilio error alone", InitiateCallArgs{To: "+15105550199", Message: "hello"}, "twilio network error", "", "", "", true, "twilio network error", "", "", ""),
		initCallCase("twilio dial fail and store op error", InitiateCallArgs{To: "+15105550199", Message: "hello"}, "dial fail", "", "db error", "", true, "db error", "", "", ""),
		initCallCase("bind provider sid error fallback", InitiateCallArgs{To: "+15105550199", Message: "hello"}, "", "", "", "bind failure", false, "", "", "", "initiated"),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runToolHandlerInitiateCallCase(t, tc)
		})
	}
}

func runToolHandlerInitiateCallCase(t *testing.T, tc toolHandlerInitiateCallTestCase) {
	t.Helper()
	env := setupTestToolEnv()
	if tc.twilioErr != "" {
		env.twilio.CreateCallErr = errors.New(tc.twilioErr)
	}
	if tc.updateStateErr != "" {
		env.store.UpdateStateErr = errors.New(tc.updateStateErr)
	}
	if tc.storeErr != "" {
		env.store.OpErr = errors.New(tc.storeErr)
	}
	if tc.bindErr != "" {
		env.store.BindErr = errors.New(tc.bindErr)
	}

	out, err := env.handler.InitiateCall(context.Background(), tc.args)
	if (err != nil) != tc.wantErr {
		t.Fatalf("InitiateCall() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
			t.Fatalf("InitiateCall() err = %v, want substring %q", err, tc.wantErrContains)
		}
		if tc.wantErrNotContains != "" && strings.Contains(err.Error(), tc.wantErrNotContains) {
			t.Fatalf("InitiateCall() err = %v, must not contain %q", err, tc.wantErrNotContains)
		}
		return
	}
	if tc.wantStatus != "" && out.Status != tc.wantStatus {
		t.Fatalf("InitiateCall() out.Status = %q, want %q", out.Status, tc.wantStatus)
	}
	if tc.bindErr == "" {
		assertToolHandlerInitiateCallSuccess(t, tc, out, env.store, env.twilio)
	}
}

func assertToolHandlerInitiateCallSuccess(t *testing.T, tc toolHandlerInitiateCallTestCase, out InitiateCallOutput, store *fakes.FakeStore, twilioMock *fakes.FakeTwilio) {
	t.Helper()
	if out.CallID == "" || out.ProviderCallSID == "" || out.Status != "initiated" {
		t.Fatalf("InitiateCall() unexpected output: %+v", out)
	}
	if !strings.Contains(twilioMock.LastCreateCallReq.TwiMLURL, "?call_id="+out.CallID) {
		t.Fatalf("TwiMLURL %q does not contain call_id", twilioMock.LastCreateCallReq.TwiMLURL)
	}
	if !strings.Contains(twilioMock.LastCreateCallReq.StatusCallback, "?call_id="+out.CallID) {
		t.Fatalf("StatusCallback %q does not contain call_id", twilioMock.LastCreateCallReq.StatusCallback)
	}
	if !strings.Contains(twilioMock.LastCreateCallReq.AsyncAmdStatusCallback, "?call_id="+out.CallID) {
		t.Fatalf("AsyncAmdStatusCallback %q does not contain call_id", twilioMock.LastCreateCallReq.AsyncAmdStatusCallback)
	}
	savedCall, getErr := store.GetCall(context.Background(), out.CallID)
	if getErr != nil || savedCall == nil || savedCall.Brief.Task != tc.args.Message {
		t.Fatalf("savedCall.Brief.Task = %v, want %q", savedCall, tc.args.Message)
	}
	if tc.wantMode != "" && savedCall.Brief.Mode != tc.wantMode {
		t.Fatalf("savedCall.Brief.Mode = %q, want %q", savedCall.Brief.Mode, tc.wantMode)
	}
}

type toolHandlerSteerCallTestCase struct {
	name          string
	callID        string
	msg           string
	mode          string
	nilSessionMgr bool
	wantErr       bool
}

func steerCase(name, callID, msg, mode string, nilSessionMgr, wantErr bool) toolHandlerSteerCallTestCase {
	return toolHandlerSteerCallTestCase{name: name, callID: callID, msg: msg, mode: mode, nilSessionMgr: nilSessionMgr, wantErr: wantErr}
}

func TestToolHandler_SteerCall(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerSteerCallTestCase{
		steerCase("empty call id", "", "say this", "", false, true),
		steerCase("empty message", "call-1", "", "", false, true),
		steerCase("message too long", "call-1", strings.Repeat("a", 501), "", false, true),
		steerCase("call not active returns error", "call-non-existent", "Wrap up the call please.", "guidance", false, true),
		steerCase("active call steers successfully", "call-steer-active", "Wrap up the call please.", "guidance", false, false),
		steerCase("steer call with nil session manager", "c1", "msg", "", true, true),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runToolHandlerSteerCallCase(t, tc)
		})
	}
}

// steerableSession is a minimal live session whose Steer succeeds.
type steerableSession struct{ mediastream.Session }

func (steerableSession) Steer(string, string) error { return nil }

func setupMockGeminiSession(t *testing.T, callID string, handler *ToolHandler) {
	t.Helper()
	handler.sessionMgr.RegisterSession(callID, steerableSession{})
}

func runToolHandlerSteerCallCase(t *testing.T, tc toolHandlerSteerCallTestCase) {
	t.Helper()
	env := setupTestToolEnv()
	if tc.nilSessionMgr {
		env.handler = NewToolHandler(env.handler.cfgSrc, env.store, env.twilio, nil, nil)
	} else if tc.callID == "call-steer-active" {
		setupMockGeminiSession(t, tc.callID, env.handler)
	}

	_, err := env.handler.SteerCall(context.Background(), SteerCallArgs{
		CallID:  tc.callID,
		Message: tc.msg,
		Mode:    tc.mode,
	})
	if (err != nil) != tc.wantErr {
		t.Fatalf("SteerCall() err = %v, wantErr = %v", err, tc.wantErr)
	}
}

type toolHandlerEndCallTestCase struct {
	name             string
	callID           string
	reportStatus     string
	storeErr         string
	hangupErr        string
	updateErr        string
	hasReporter      bool
	wantErr          bool
	wantStatus       string
	wantReportStatus string
}

func endCallCase(name, callID, reportStatus, storeErr, hangupErr, updateErr string, hasReporter, wantErr bool, wantStatus, wantReportStatus string) toolHandlerEndCallTestCase {
	return toolHandlerEndCallTestCase{name: name, callID: callID, reportStatus: reportStatus, storeErr: storeErr, hangupErr: hangupErr, updateErr: updateErr, hasReporter: hasReporter, wantErr: wantErr, wantStatus: wantStatus, wantReportStatus: wantReportStatus}
}

func runToolHandlerEndCallCase(t *testing.T, tc toolHandlerEndCallTestCase) {
	t.Helper()
	env := setupTestToolEnv()

	if strings.HasPrefix(tc.callID, "call-active-") {
		if err := env.store.CreateOutboundCall(context.Background(), model.VoiceCall{
			CallID:          tc.callID,
			ProviderCallSID: "CA999",
			State:           "in-progress",
			FromNumber:      "+15105550100",
			ToNumber:        "+15105550199",
			ReportStatus:    tc.reportStatus,
		}, 5); err != nil {
			t.Fatalf("CreateOutboundCall failed: %v", err)
		}
	}
	if tc.storeErr != "" {
		env.store.OpErr = errors.New(tc.storeErr)
	}
	if tc.hangupErr != "" {
		env.twilio.HangupErr = errors.New(tc.hangupErr)
	}
	if tc.updateErr != "" {
		env.store.UpdateStateErr = errors.New(tc.updateErr)
	}
	if tc.hasReporter {
		rep := delivery.NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: ""}}, env.store, env.twilio)
		env.handler = NewToolHandler(env.handler.cfgSrc, env.store, env.twilio, nil, rep)
	}

	out, err := env.handler.EndCall(context.Background(), EndCallArgs{CallID: tc.callID})
	if (err != nil) != tc.wantErr {
		t.Fatalf("EndCall() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantStatus != "" && out.Status != tc.wantStatus {
		t.Fatalf("EndCall() status = %s, want %s", out.Status, tc.wantStatus)
	}
	if tc.wantReportStatus != "" {
		updated, getErr := env.store.GetCall(context.Background(), tc.callID)
		if getErr != nil || updated == nil {
			t.Fatalf("GetCall() err = %v, call = %+v", getErr, updated)
		}
		if updated.ReportStatus != tc.wantReportStatus {
			t.Fatalf("ReportStatus = %q, want %q", updated.ReportStatus, tc.wantReportStatus)
		}
	}
}

func TestToolHandler_EndCall(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerEndCallTestCase{
		endCallCase("empty call id", "", "", "", "", "", false, true, "", ""),
		endCallCase("call not found", "call-missing", "", "", "", "", false, true, "", ""),
		endCallCase("existing call ends successfully", "call-active-1", "", "", "", "", false, false, "ended", ""),
		endCallCase("existing call with pending report leaves delivery to ProsperaReport", "call-active-1", "pending", "", "", "", false, false, "ended", "pending"),
		endCallCase("end call store error", "c1", "", "db error", "", "", false, true, "", ""),
		endCallCase("end call with twilio hangup error and reporter error fallback", "call-active-1", "", "", "hangup failed", "", true, false, "ended", ""),
		endCallCase("end call update state error fallback", "call-active-1", "", "", "", "update state failure", false, false, "ended", ""),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runToolHandlerEndCallCase(t, tc)
		})
	}
}

type toolHandlerGetCallStatusTestCase struct {
	name     string
	callID   string
	storeErr string
	wantErr  bool
}

func getCallCase(name, callID, storeErr string, wantErr bool) toolHandlerGetCallStatusTestCase {
	return toolHandlerGetCallStatusTestCase{name: name, callID: callID, storeErr: storeErr, wantErr: wantErr}
}

func TestToolHandler_GetCallStatus(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerGetCallStatusTestCase{
		getCallCase("get existing call", "call-test-get", "", false),
		getCallCase("get empty call id fails", "", "", true),
		getCallCase("get non-existent call", "call-does-not-exist", "", true),
		getCallCase("get call status store error", "c1", "db error", true),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := setupTestToolEnv()
			if tc.callID == "call-test-get" {
				if err := env.store.CreateOutboundCall(context.Background(), model.VoiceCall{
					CallID: "call-test-get", State: "in-progress", FromNumber: "+15105550100", ToNumber: "+15105550199",
				}, 5); err != nil {
					t.Fatalf("CreateOutboundCall failed: %v", err)
				}
			}
			if tc.storeErr != "" {
				env.store.OpErr = errors.New(tc.storeErr)
			}
			out, err := env.handler.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: tc.callID})
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetCallStatus() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (out.Call == nil || out.Call.CallID != tc.callID) {
				t.Fatalf("GetCallStatus() unexpected out = %+v", out)
			}
		})
	}
}

type toolHandlerListCallsTestCase struct {
	name       string
	limit      int
	emptyStore bool
	storeErr   string
	wantErr    bool
	wantCalls  bool
	wantEmpty  bool
}

func listCallsCase(name string, limit int, emptyStore bool, storeErr string, wantErr, wantCalls, wantEmpty bool) toolHandlerListCallsTestCase {
	return toolHandlerListCallsTestCase{name: name, limit: limit, emptyStore: emptyStore, storeErr: storeErr, wantErr: wantErr, wantCalls: wantCalls, wantEmpty: wantEmpty}
}

func TestToolHandler_ListCalls(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerListCallsTestCase{
		listCallsCase("list calls limit 10", 10, false, "", false, true, false),
		listCallsCase("empty store returns empty non-nil calls", 0, true, "", false, false, true),
		listCallsCase("list calls store error", 0, false, "db error", true, false, false),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := setupTestToolEnv()
			if tc.emptyStore {
				env.store = fakes.NewFakeStore()
				env.handler = NewToolHandler(env.handler.cfgSrc, env.store, env.twilio, nil, nil)
			} else {
				if err := env.store.CreateOutboundCall(context.Background(), model.VoiceCall{
					CallID: "call-list-1", State: "in-progress", FromNumber: "+15105550100", ToNumber: "+15105550199",
				}, 5); err != nil {
					t.Fatalf("CreateOutboundCall: %v", err)
				}
			}
			if tc.storeErr != "" {
				env.store.OpErr = errors.New(tc.storeErr)
			}
			out, err := env.handler.ListCalls(context.Background(), ListCallsArgs{Limit: tc.limit})
			if (err != nil) != tc.wantErr {
				t.Fatalf("ListCalls() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantCalls && len(out.Calls) == 0 {
				t.Fatalf("ListCalls() expected calls, got 0")
			}
			if tc.wantEmpty && (out.Calls == nil || len(out.Calls) != 0) {
				t.Fatalf("ListCalls() expected empty non-nil calls slice, got %+v", out.Calls)
			}
		})
	}
}

type toolHandlerSendSMSTestCase struct {
	name         string
	to           string
	from         string
	body         string
	noConfigFrom bool
	twilioErr    string
	saveErr      string
	wantErr      bool
	wantStatus   string
}

func sendSMSCase(name, to, from, body string, noConfigFrom bool, twilioErr, saveErr string, wantErr bool, wantStatus string) toolHandlerSendSMSTestCase {
	return toolHandlerSendSMSTestCase{name: name, to: to, from: from, body: body, noConfigFrom: noConfigFrom, twilioErr: twilioErr, saveErr: saveErr, wantErr: wantErr, wantStatus: wantStatus}
}

func TestToolHandler_SendSMS(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerSendSMSTestCase{
		sendSMSCase("valid send", "+15105550199", "", "Test SMS message", false, "", "", false, "sent"),
		sendSMSCase("custom from number", "+15105550199", "+1 (510) 555-0188", "Test custom from", false, "", "", false, "sent"),
		sendSMSCase("invalid phone number", "invalid", "", "Test", false, "", "", true, ""),
		sendSMSCase("invalid source number", "+15105550199", "invalid-source-number", "Test", false, "", "", true, ""),
		sendSMSCase("empty body", "+15105550199", "", "", false, "", "", true, ""),
		sendSMSCase("missing from in config", "+15105550199", "", "hi", true, "", "", true, ""),
		sendSMSCase("twilio sms error", "+15105550199", "", "test", false, "twilio sms fail", "", true, ""),
		sendSMSCase("store save error fallback", "+15105550199", "", "Test SMS message", false, "", "save failure", false, "sent"),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := setupTestToolEnv()
			if tc.noConfigFrom {
				env.handler = NewToolHandler(fakes.StaticConfig{Cfg: &config.Config{}}, nil, nil, nil, nil)
			}
			if tc.twilioErr != "" {
				env.twilio.SendSMSErr = errors.New(tc.twilioErr)
			}
			if tc.saveErr != "" {
				env.store.OpErr = errors.New(tc.saveErr)
				env.store.SaveErr = errors.New(tc.saveErr)
			}
			out, err := env.handler.SendSMS(context.Background(), SendSMSArgs{
				To:   tc.to,
				From: tc.from,
				Body: tc.body,
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("SendSMS() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantStatus != "" && (out.Status != tc.wantStatus || out.MessageSID == "") {
				t.Fatalf("SendSMS() out = %+v, want status %s", out, tc.wantStatus)
			}
		})
	}
}

type toolHandlerListSMSTestCase struct {
	name       string
	limit      int
	emptyStore bool
	storeErr   string
	wantErr    bool
	wantEmpty  bool
}

func listSMSCase(name string, limit int, emptyStore bool, storeErr string, wantErr, wantEmpty bool) toolHandlerListSMSTestCase {
	return toolHandlerListSMSTestCase{name: name, limit: limit, emptyStore: emptyStore, storeErr: storeErr, wantErr: wantErr, wantEmpty: wantEmpty}
}

func TestToolHandler_ListSMS(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerListSMSTestCase{
		listSMSCase("list sms limit 5", 5, false, "", false, false),
		listSMSCase("empty store returns empty non-nil messages", 0, true, "", false, true),
		listSMSCase("list sms store error", 0, false, "db error", true, false),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := setupTestToolEnv()
			if tc.emptyStore {
				env.store = fakes.NewFakeStore()
				env.handler = NewToolHandler(env.handler.cfgSrc, env.store, env.twilio, nil, nil)
			}
			if tc.storeErr != "" {
				env.store.OpErr = errors.New(tc.storeErr)
			}
			out, err := env.handler.ListSMS(context.Background(), ListSMSArgs{Limit: tc.limit})
			if (err != nil) != tc.wantErr {
				t.Fatalf("ListSMS() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantEmpty && (out.Messages == nil || len(out.Messages) != 0) {
				t.Fatalf("ListSMS() expected empty messages, got %+v", out.Messages)
			}
		})
	}
}

type toolHandlerEnsurePhoneTestCase struct {
	name      string
	areaCode  string
	noTwilio  bool
	twilioErr string
	wantErr   bool
	wantWeb   string
}

func ensurePhoneCase(name, areaCode string, noTwilio bool, twilioErr string, wantErr bool, wantWeb string) toolHandlerEnsurePhoneTestCase {
	return toolHandlerEnsurePhoneTestCase{name: name, areaCode: areaCode, noTwilio: noTwilio, twilioErr: twilioErr, wantErr: wantErr, wantWeb: wantWeb}
}

func TestToolHandler_EnsurePhoneNumber(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerEnsurePhoneTestCase{
		ensurePhoneCase("configured area code", "510", false, "", false, "https://webhooks.example.com/api/webhooks"),
		ensurePhoneCase("default area code", "", false, "", false, ""),
		ensurePhoneCase("missing twilio client returns error", "", true, "", true, ""),
		ensurePhoneCase("twilio error returns error", "510", false, "twilio phone fail", true, ""),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := setupTestToolEnv()
			if tc.noTwilio {
				env.handler = NewToolHandler(env.handler.cfgSrc, env.store, nil, nil, nil)
			}
			if tc.twilioErr != "" {
				env.twilio.EnsurePhoneErr = errors.New(tc.twilioErr)
			}
			out, err := env.handler.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: tc.areaCode})
			if (err != nil) != tc.wantErr {
				t.Fatalf("EnsurePhoneNumber() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if out.PhoneNumber == "" || out.Status != "configured" {
					t.Fatalf("EnsurePhoneNumber() out = %+v, want configured", out)
				}
				if tc.wantWeb != "" && env.twilio.LastWebhookBaseURL != tc.wantWeb {
					t.Fatalf("LastWebhookBaseURL = %q, want %q", env.twilio.LastWebhookBaseURL, tc.wantWeb)
				}
			}
		})
	}
}

func TestMCPStreamableHTTPHandler(t *testing.T) {
	t.Parallel()

	env := setupTestToolEnv()
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "prospera", Version: "1.0.0"}, nil)
	RegisterProsperaMCPTools(mcpServer, env.handler)

	streamHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Accept", "application/json, text/event-stream")
		streamHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	client := srv.Client()

	// Prepopulate a call for MCP call tests
	if err := env.store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-mcp-active",
		ProviderCallSID: "CA-mcp-1",
		State:           "in-progress",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550199",
	}, 5); err != nil {
		t.Fatalf("CreateOutboundCall failed: %v", err)
	}

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantSub  string
	}{
		{name: "initialize", body: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test-client","version":"1.0"}}}`, wantCode: http.StatusOK, wantSub: `"protocolVersion"`},
		{name: "tools list", body: `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`, wantCode: http.StatusOK, wantSub: `initiate_call`},
		{name: "tools call initiate_call", body: `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"initiate_call","arguments":{"to":"+15105550199","message":"hello","brief":{"context":"reservation details"}}}}`, wantCode: http.StatusOK, wantSub: `"call_id"`},
		{name: "tools call steer_call", body: `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"steer_call","arguments":{"call_id":"call-nonexistent","message":"steer"}}}`, wantCode: http.StatusOK, wantSub: `"isError":true`},
		{name: "tools call end_call", body: `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"end_call","arguments":{"call_id":"call-mcp-active"}}}`, wantCode: http.StatusOK, wantSub: `"ended"`},
		{name: "tools call get_call_status", body: `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"get_call_status","arguments":{"call_id":"call-mcp-active"}}}`, wantCode: http.StatusOK, wantSub: `call-mcp-active`},
		{name: "tools call list_calls", body: `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"list_calls","arguments":{"limit":5}}}`, wantCode: http.StatusOK, wantSub: `"calls"`},
		{name: "tools call send_sms", body: `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"send_sms","arguments":{"to":"+15105550199","body":"test"}}}`, wantCode: http.StatusOK, wantSub: `"sent"`},
		{name: "tools call list_sms", body: `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"list_sms","arguments":{"limit":5}}}`, wantCode: http.StatusOK, wantSub: `"messages"`},
		{name: "tools call ensure_phone_number", body: `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"ensure_phone_number","arguments":{"area_code":"510"}}}`, wantCode: http.StatusOK, wantSub: `"configured"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("client.Do: %v", err)
			}
			defer resp.Body.Close()

			respBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if resp.StatusCode != tc.wantCode {
				t.Fatalf("status code = %d, want %d, body: %s", resp.StatusCode, tc.wantCode, string(respBytes))
			}
			if !strings.Contains(string(respBytes), tc.wantSub) {
				t.Fatalf("response %s missing %s", string(respBytes), tc.wantSub)
			}
		})
	}
}
