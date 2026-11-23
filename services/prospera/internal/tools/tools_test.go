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

func setupTestToolHandler() (*ToolHandler, *fakes.FakeStore, *fakes.FakeTwilio) {
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
	return handler, store, twilioMock
}

type toolHandlerInitiateCallTestCase struct {
	name               string
	args               InitiateCallArgs
	twilioErr          string
	updateStateErr     string
	wantErr            bool
	wantErrContains    string
	wantErrNotContains string
	wantMode           string
}

func TestToolHandler_InitiateCall(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerInitiateCallTestCase{
		{
			name: "valid initiate call",
			args: InitiateCallArgs{
				To:      "+15105550199",
				Message: "Hello, this is an automated confirmation call.",
			},
			wantErr:  false,
			wantMode: "conversation",
		},
		{
			name: "valid initiate call with partial brief",
			args: InitiateCallArgs{
				To:      "+15105550199",
				Message: "Hello, this is an automated confirmation call.",
				Brief: model.CallBrief{
					Context: "Context details",
				},
			},
			wantErr:  false,
			wantMode: "conversation",
		},
		{
			name: "valid initiate call with notify mode",
			args: InitiateCallArgs{
				To:      "+15105550199",
				Message: "Hello, notification only.",
				Mode:    "notify",
			},
			wantErr:  false,
			wantMode: "notify",
		},
		{
			name: "invalid phone number",
			args: InitiateCallArgs{
				To:      "not-a-number",
				Message: "Hello",
			},
			wantErr: true,
		},
		{
			name: "empty message and task",
			args: InitiateCallArgs{
				To: "+15105550199",
			},
			wantErr: true,
		},
		{
			name: "twilio error with state update failure",
			args: InitiateCallArgs{
				To:      "+15105550199",
				Message: "Hello, this is an automated confirmation call.",
			},
			twilioErr:          "twilio network error",
			updateStateErr:     "db write failed",
			wantErr:            true,
			wantErrContains:    "twilio network error",
			wantErrNotContains: "db write failed",
		},
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
	handler, store, twilioMock := setupTestToolHandler()
	if tc.twilioErr != "" {
		twilioMock.CreateCallErr = errors.New(tc.twilioErr)
	}
	if tc.updateStateErr != "" {
		store.UpdateStateErr = errors.New(tc.updateStateErr)
	}

	out, err := handler.InitiateCall(context.Background(), tc.args)
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
	assertToolHandlerInitiateCallSuccess(t, tc, out, store, twilioMock)
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
	name    string
	callID  string
	msg     string
	mode    string
	wantErr bool
}

func TestToolHandler_SteerCall(t *testing.T) {
	t.Parallel()

	tests := []toolHandlerSteerCallTestCase{
		{
			name:    "empty call id",
			callID:  "",
			msg:     "say this",
			wantErr: true,
		},
		{
			name:    "empty message",
			callID:  "call-1",
			msg:     "",
			wantErr: true,
		},
		{
			name:    "message too long",
			callID:  "call-1",
			msg:     strings.Repeat("a", 501),
			wantErr: true,
		},
		{
			name:    "call not active returns error",
			callID:  "call-non-existent",
			msg:     "Wrap up the call please.",
			mode:    "guidance",
			wantErr: true,
		},
		{
			name:    "active call steers successfully",
			callID:  "call-steer-active",
			msg:     "Wrap up the call please.",
			mode:    "guidance",
			wantErr: false,
		},
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
	handler, _, _ := setupTestToolHandler()
	if tc.callID == "call-steer-active" {
		setupMockGeminiSession(t, tc.callID, handler)
	}

	_, err := handler.SteerCall(context.Background(), SteerCallArgs{
		CallID:  tc.callID,
		Message: tc.msg,
		Mode:    tc.mode,
	})
	if (err != nil) != tc.wantErr {
		t.Fatalf("SteerCall() err = %v, wantErr = %v", err, tc.wantErr)
	}
}

func TestToolHandler_EndCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		callID           string
		reportStatus     string
		wantReportStatus string
		wantErr          bool
	}{
		{
			name:    "empty call id",
			callID:  "",
			wantErr: true,
		},
		{
			name:    "call not found",
			callID:  "call-missing",
			wantErr: true,
		},
		{
			name:    "existing call ends successfully",
			callID:  "call-active-1",
			wantErr: false,
		},
		{
			name:             "existing call with pending report leaves delivery to ProsperaReport",
			callID:           "call-active-1",
			reportStatus:     "pending",
			wantReportStatus: "pending",
			wantErr:          false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler, store, _ := setupTestToolHandler()

			if tc.callID == "call-active-1" {
				_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
					CallID:          "call-active-1",
					ProviderCallSID: "CA999",
					State:           "in-progress",
					FromNumber:      "+15105550100",
					ToNumber:        "+15105550199",
					ReportStatus:    tc.reportStatus,
				}, 5)
			}

			out, err := handler.EndCall(context.Background(), EndCallArgs{CallID: tc.callID})
			if (err != nil) != tc.wantErr {
				t.Fatalf("EndCall() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && out.Status != "ended" {
				t.Fatalf("EndCall() status = %s, want 'ended'", out.Status)
			}
			if tc.wantReportStatus != "" {
				updated, getErr := store.GetCall(context.Background(), tc.callID)
				if getErr != nil || updated == nil {
					t.Fatalf("GetCall() err = %v, call = %+v", getErr, updated)
				}
				if updated.ReportStatus != tc.wantReportStatus {
					t.Fatalf("ReportStatus = %q, want %q", updated.ReportStatus, tc.wantReportStatus)
				}
			}
		})
	}
}

func TestToolHandler_GetAndListCalls(t *testing.T) {
	t.Parallel()

	handler, store, _ := setupTestToolHandler()
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:     "call-test-get",
		State:      "in-progress",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
	}, 5)

	t.Run("get existing call", func(t *testing.T) {
		t.Parallel()
		out, err := handler.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: "call-test-get"})
		if err != nil || out.Call == nil || out.Call.CallID != "call-test-get" {
			t.Fatalf("GetCallStatus() err = %v, out = %+v", err, out)
		}
	})

	t.Run("get empty call id fails", func(t *testing.T) {
		t.Parallel()
		_, err := handler.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: ""})
		if err == nil {
			t.Fatal("expected error for empty call id")
		}
	})

	t.Run("get non-existent call", func(t *testing.T) {
		t.Parallel()
		_, err := handler.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: "call-does-not-exist"})
		if err == nil {
			t.Fatal("expected error for non-existent call")
		}
	})

	t.Run("list calls", func(t *testing.T) {
		t.Parallel()
		out, err := handler.ListCalls(context.Background(), ListCallsArgs{Limit: 10})
		if err != nil || len(out.Calls) == 0 {
			t.Fatalf("ListCalls() err = %v, calls = %d", err, len(out.Calls))
		}
	})
}

func TestToolHandler_SMSAndPhone(t *testing.T) {
	t.Parallel()

	handler, _, _ := setupTestToolHandler()

	t.Run("send sms", func(t *testing.T) {
		t.Parallel()
		testToolHandlerSendSMS(t, handler)
	})
	t.Run("list sms and calls", func(t *testing.T) {
		t.Parallel()
		testToolHandlerListSMSAndCalls(t, handler)
	})
	t.Run("ensure phone number", func(t *testing.T) {
		t.Parallel()
		testToolHandlerEnsurePhone(t, handler)
	})
}

func TestToolHandler_SendSMSStoreError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		saveErr string
		wantErr bool
	}{
		{
			name:    "save error still sends",
			saveErr: "db write failed",
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler, store, _ := setupTestToolHandler()
			if tc.saveErr != "" {
				store.SaveErr = errors.New(tc.saveErr)
			}
			out, err := handler.SendSMS(context.Background(), SendSMSArgs{
				To:   "+15105550199",
				Body: "Test SMS message",
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("SendSMS() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (out.Status != "sent" || out.MessageSID == "") {
				t.Fatalf("SendSMS() out = %+v, want sent status", out)
			}
		})
	}
}

func testToolHandlerSendSMS(t *testing.T, handler *ToolHandler) {
	t.Helper()
	out, err := handler.SendSMS(context.Background(), SendSMSArgs{
		To:   "+15105550199",
		Body: "Test SMS message",
	})
	if err != nil || out.Status != "sent" || out.MessageSID == "" {
		t.Fatalf("SendSMS() err = %v, out = %+v", err, out)
	}

	outCustom, errCustom := handler.SendSMS(context.Background(), SendSMSArgs{
		To:   "+15105550199",
		From: "+1 (510) 555-0188",
		Body: "Test custom from",
	})
	if errCustom != nil || outCustom.Status != "sent" {
		t.Fatalf("SendSMS() with custom from err = %v, out = %+v", errCustom, outCustom)
	}

	if _, err := handler.SendSMS(context.Background(), SendSMSArgs{To: "invalid", Body: "Test"}); err == nil {
		t.Fatal("expected error for invalid phone number")
	}

	if _, err := handler.SendSMS(context.Background(), SendSMSArgs{To: "+15105550199", From: "invalid-source-number", Body: "Test"}); err == nil {
		t.Fatal("expected error for invalid from number")
	}

	if _, err := handler.SendSMS(context.Background(), SendSMSArgs{To: "+15105550199", Body: ""}); err == nil {
		t.Fatal("expected error for empty body")
	}

	handlerNoFrom := NewToolHandler(fakes.StaticConfig{Cfg: &config.Config{}}, nil, nil, nil, nil)
	if _, err := handlerNoFrom.SendSMS(context.Background(), SendSMSArgs{To: "+15105550199", Body: "hi"}); err == nil {
		t.Fatal("expected error for missing from")
	}
}

func testToolHandlerListSMSAndCalls(t *testing.T, handler *ToolHandler) {
	t.Helper()
	if _, err := handler.ListSMS(context.Background(), ListSMSArgs{Limit: 5}); err != nil {
		t.Fatalf("ListSMS() err = %v", err)
	}

	_, _ = handler.ListSMS(context.Background(), ListSMSArgs{Limit: -1})
	_, _ = handler.ListSMS(context.Background(), ListSMSArgs{Limit: 100})
	_, _ = handler.ListCalls(context.Background(), ListCallsArgs{Limit: -1})
	_, _ = handler.ListCalls(context.Background(), ListCallsArgs{Limit: 100})

	emptyStore := fakes.NewFakeStore()
	h := NewToolHandler(handler.cfgSrc, emptyStore, &fakes.FakeTwilio{}, nil, nil)
	outCalls, err := h.ListCalls(context.Background(), ListCallsArgs{})
	if err != nil || outCalls.Calls == nil {
		t.Fatal("expected empty non-nil calls slice")
	}
	outSMS, err := h.ListSMS(context.Background(), ListSMSArgs{})
	if err != nil || outSMS.Messages == nil {
		t.Fatal("expected empty non-nil messages slice")
	}
}

func testToolHandlerEnsurePhone(t *testing.T, handler *ToolHandler) {
	t.Helper()
	h, _, twilioMock := setupTestToolHandler()
	out, err := h.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: "510"})
	if err != nil || out.PhoneNumber == "" || out.Status != "configured" {
		t.Fatalf("EnsurePhoneNumber() err = %v, out = %+v", err, out)
	}
	if twilioMock.LastWebhookBaseURL != "https://webhooks.example.com/api/webhooks" {
		t.Fatalf("lastWebhookBaseURL = %q, want https://webhooks.example.com/api/webhooks", twilioMock.LastWebhookBaseURL)
	}

	outDef, errDef := handler.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: ""})
	if errDef != nil || outDef.Status != "configured" {
		t.Fatalf("EnsurePhoneNumber() default err = %v, out = %+v", errDef, outDef)
	}
}

func TestMCPStreamableHTTPHandler(t *testing.T) {
	t.Parallel()

	handler, store, _ := setupTestToolHandler()
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "prospera", Version: "1.0.0"}, nil)
	RegisterProsperaMCPTools(mcpServer, handler)

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
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-mcp-active",
		ProviderCallSID: "CA-mcp-1",
		State:           "in-progress",
		FromNumber:      "+15105550100",
		ToNumber:        "+15105550199",
	}, 5)

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantSub  string
	}{
		{
			name:     "initialize",
			body:     `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test-client","version":"1.0"}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"protocolVersion"`,
		},
		{
			name:     "tools list",
			body:     `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
			wantCode: http.StatusOK,
			wantSub:  `initiate_call`,
		},
		{
			name:     "tools call initiate_call",
			body:     `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"initiate_call","arguments":{"to":"+15105550199","message":"hello","brief":{"context":"reservation details"}}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"call_id"`,
		},
		{
			name:     "tools call steer_call",
			body:     `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"steer_call","arguments":{"call_id":"call-nonexistent","message":"steer"}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"isError":true`,
		},
		{
			name:     "tools call end_call",
			body:     `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"end_call","arguments":{"call_id":"call-mcp-active"}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"ended"`,
		},
		{
			name:     "tools call get_call_status",
			body:     `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"get_call_status","arguments":{"call_id":"call-mcp-active"}}}`,
			wantCode: http.StatusOK,
			wantSub:  `call-mcp-active`,
		},
		{
			name:     "tools call list_calls",
			body:     `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"list_calls","arguments":{"limit":5}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"calls"`,
		},
		{
			name:     "tools call send_sms",
			body:     `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"send_sms","arguments":{"to":"+15105550199","body":"test"}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"sent"`,
		},
		{
			name:     "tools call list_sms",
			body:     `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"list_sms","arguments":{"limit":5}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"messages"`,
		},
		{
			name:     "tools call ensure_phone_number",
			body:     `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"ensure_phone_number","arguments":{"area_code":"510"}}}`,
			wantCode: http.StatusOK,
			wantSub:  `"configured"`,
		},
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

			respBytes, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.wantCode {
				t.Fatalf("status code = %d, want %d, body: %s", resp.StatusCode, tc.wantCode, string(respBytes))
			}
			if !strings.Contains(string(respBytes), tc.wantSub) {
				t.Fatalf("response %s missing %s", string(respBytes), tc.wantSub)
			}
		})
	}
}

func TestToolHandler_StoreAndTwilioErrors(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		TwilioFromNumber:     "+15105550100",
		PublicWebhookBaseURL: "https://webhooks.example.com",
		MaxConcurrentCalls:   5,
	}

	t.Run("initiate call store error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		store.OpErr = errors.New("db error")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)
		_, err := h.InitiateCall(context.Background(), InitiateCallArgs{
			To:      "+15105550199",
			Message: "hello",
		})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("initiate call twilio error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		twilioMock := &fakes.FakeTwilio{CreateCallErr: errors.New("twilio network error")}
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, nil, nil)
		_, err := h.InitiateCall(context.Background(), InitiateCallArgs{
			To:      "+15105550199",
			Message: "hello",
		})
		if err == nil {
			t.Fatal("expected error on twilio failure")
		}
	})

	t.Run("end call store error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		store.OpErr = errors.New("db error")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)
		_, err := h.EndCall(context.Background(), EndCallArgs{CallID: "c1"})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("get call status store error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		store.OpErr = errors.New("db error")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)
		_, err := h.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: "c1"})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("list calls store error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		store.OpErr = errors.New("db error")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)
		_, err := h.ListCalls(context.Background(), ListCallsArgs{})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("send sms twilio error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		twilioMock := &fakes.FakeTwilio{SendSMSErr: errors.New("twilio sms fail")}
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, nil, nil)
		_, err := h.SendSMS(context.Background(), SendSMSArgs{
			To:   "+15105550199",
			Body: "test",
		})
		if err == nil {
			t.Fatal("expected error on twilio sms fail")
		}
	})

	t.Run("list sms store error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		store.OpErr = errors.New("db error")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)
		_, err := h.ListSMS(context.Background(), ListSMSArgs{})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("ensure phone number twilio error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		twilioMock := &fakes.FakeTwilio{EnsurePhoneErr: errors.New("twilio phone fail")}
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, nil, nil)
		_, err := h.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: "510"})
		if err == nil {
			t.Fatal("expected error on twilio phone fail")
		}
	})

	t.Run("steer call with nil session manager", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)
		_, err := h.SteerCall(context.Background(), SteerCallArgs{
			CallID:  "c1",
			Message: "msg",
		})
		if err == nil {
			t.Fatal("expected error on nil session manager")
		}
	})
}

func TestToolHandler_ValidationErrors(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{TwilioFromNumber: "+15105550100"}
	store := fakes.NewFakeStore()
	h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)

	t.Run("initiate call invalid to", func(t *testing.T) {
		t.Parallel()
		_, err := h.InitiateCall(context.Background(), InitiateCallArgs{To: "invalid"})
		if err == nil {
			t.Fatal("expected err")
		}
	})

	t.Run("steer call validation", func(t *testing.T) {
		t.Parallel()
		if _, err := h.SteerCall(context.Background(), SteerCallArgs{CallID: ""}); err == nil {
			t.Fatal("expected err")
		}
		if _, err := h.SteerCall(context.Background(), SteerCallArgs{CallID: "c1", Message: ""}); err == nil {
			t.Fatal("expected err")
		}
		if _, err := h.SteerCall(context.Background(), SteerCallArgs{CallID: "c1", Message: strings.Repeat("x", 501)}); err == nil {
			t.Fatal("expected err")
		}
	})

	t.Run("end call validation", func(t *testing.T) {
		t.Parallel()
		if _, err := h.EndCall(context.Background(), EndCallArgs{CallID: ""}); err == nil {
			t.Fatal("expected err")
		}
		if _, err := h.EndCall(context.Background(), EndCallArgs{CallID: "missing"}); err == nil {
			t.Fatal("expected err")
		}
	})

	t.Run("get call status validation", func(t *testing.T) {
		t.Parallel()
		if _, err := h.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: ""}); err == nil {
			t.Fatal("expected err")
		}
		if _, err := h.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: "missing"}); err == nil {
			t.Fatal("expected err")
		}
	})

	t.Run("send sms validation", func(t *testing.T) {
		t.Parallel()
		if _, err := h.SendSMS(context.Background(), SendSMSArgs{To: "bad"}); err == nil {
			t.Fatal("expected err")
		}
		if _, err := h.SendSMS(context.Background(), SendSMSArgs{To: "+15105550199", Body: ""}); err == nil {
			t.Fatal("expected err")
		}
	})

	t.Run("ensure phone number validation", func(t *testing.T) {
		t.Parallel()
		hNoTwilio := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, nil, nil, nil)
		if _, err := hNoTwilio.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: ""}); err == nil {
			t.Fatal("expected err")
		}
	})
}

func TestToolHandler_SteerEndAndEnsurePhoneFallbacks(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{TwilioFromNumber: "+15105550100"}

	t.Run("list sms zero limit defaults to 20", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)
		out, err := h.ListSMS(context.Background(), ListSMSArgs{Limit: 0})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(out.Messages) != 0 {
			t.Fatalf("expected empty messages, got %v", out.Messages)
		}
	})

	t.Run("end call with twilio hangup error and reporter error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
			CallID:          "call-hangup-err",
			ProviderCallSID: "CA-hangup-err",
			State:           "in-progress",
		}, 5)
		twilioMock := &fakes.FakeTwilio{HangupErr: errors.New("hangup failed")}
		rep := delivery.NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: ""}}, store, twilioMock)
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, nil, rep)

		out, err := h.EndCall(context.Background(), EndCallArgs{CallID: "call-hangup-err"})
		requireFallbackStatus(t, out.Status, err, "ended")
	})

	t.Run("initiate call with twilio error and state update error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		twilioMock := &fakes.FakeTwilio{CreateCallErr: errors.New("dial fail")}
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, nil, nil)
		store.OpErr = errors.New("db error")

		_, err := h.InitiateCall(context.Background(), InitiateCallArgs{
			To:      "+15105550199",
			Message: "hello",
		})
		if err == nil {
			t.Fatal("expected initiate call error")
		}
	})

	t.Run("initiate call bind provider sid error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		store.BindErr = errors.New("bind failure")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)

		out, err := h.InitiateCall(context.Background(), InitiateCallArgs{
			To:      "+15105550199",
			Message: "hello",
		})
		requireFallbackStatus(t, out.Status, err, "initiated")
	})

	t.Run("end call update state error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
			CallID: "call-upd-err",
			State:  "in-progress",
		}, 5)
		store.UpdateStateErr = errors.New("update state failure")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)

		out, err := h.EndCall(context.Background(), EndCallArgs{CallID: "call-upd-err"})
		requireFallbackStatus(t, out.Status, err, "ended")
	})

	t.Run("send sms save error", func(t *testing.T) {
		t.Parallel()
		store := fakes.NewFakeStore()
		store.OpErr = errors.New("save failure")
		h := NewToolHandler(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, nil, nil)

		out, err := h.SendSMS(context.Background(), SendSMSArgs{
			To:   "+15105550199",
			Body: "test message",
		})
		requireFallbackStatus(t, out.Status, err, "sent")
	})
}

// requireFallbackStatus fails unless a fallback call succeeded with status want.
func requireFallbackStatus(t *testing.T, got string, err error, want string) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != want {
		t.Fatalf("status = %s, want %s", got, want)
	}
}
