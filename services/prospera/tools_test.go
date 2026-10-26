package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func setupTestToolHandler() (*ToolHandler, *mockStoreForDelivery, *mockTwilioForDelivery) {
	cfg := &Config{
		TwilioFromNumber:      "+15105550100",
		PublicWebhookBaseURL:  "https://webhooks.example.com",
		DefaultDiscordChannel: "prospera",
		MaxConcurrentCalls:    5,
	}
	store := newMockStoreForDelivery()
	twilioMock := &mockTwilioForDelivery{}
	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(cfg, store, twilioMock, nil, tokenMgr)
	reporter := NewReporter(cfg, store, twilioMock)

	handler := NewToolHandler(cfg, store, twilioMock, sessionMgr, reporter)
	return handler, store, twilioMock
}

func TestToolHandler_InitiateCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    InitiateCallArgs
		wantErr bool
	}{
		{
			name: "valid initiate call",
			args: InitiateCallArgs{
				To:      "+15105550199",
				Message: "Hello, this is an automated confirmation call.",
			},
			wantErr: false,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler, store, twilioMock := setupTestToolHandler()

			out, err := handler.InitiateCall(context.Background(), tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("InitiateCall() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if out.CallID == "" || out.ProviderCallSID == "" || out.Status != "initiated" {
					t.Fatalf("InitiateCall() unexpected output: %+v", out)
				}
				if !strings.Contains(twilioMock.lastCreateCallReq.TwiMLURL, "?call_id="+out.CallID) {
					t.Fatalf("TwiMLURL %q does not contain call_id", twilioMock.lastCreateCallReq.TwiMLURL)
				}
				if !strings.Contains(twilioMock.lastCreateCallReq.StatusCallback, "?call_id="+out.CallID) {
					t.Fatalf("StatusCallback %q does not contain call_id", twilioMock.lastCreateCallReq.StatusCallback)
				}
				if !strings.Contains(twilioMock.lastCreateCallReq.AsyncAmdStatusCallback, "?call_id="+out.CallID) {
					t.Fatalf("AsyncAmdStatusCallback %q does not contain call_id", twilioMock.lastCreateCallReq.AsyncAmdStatusCallback)
				}
				savedCall, getErr := store.GetCall(context.Background(), out.CallID)
				if getErr != nil || savedCall == nil || savedCall.Brief.Task != tc.args.Message {
					t.Fatalf("savedCall.Brief.Task = %v, want %q", savedCall, tc.args.Message)
				}
			}
		})
	}
}

func TestToolHandler_SteerCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		callID  string
		msg     string
		mode    string
		wantErr bool
	}{
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
			handler, _, _ := setupTestToolHandler()
			if tc.callID == "call-steer-active" {
				upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
				geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					for {
						var msg any
						if err := conn.ReadJSON(&msg); err != nil {
							return
						}
					}
				}))
				defer geminiServer.Close()

				wsURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")
				wsConn, dialResp, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if dialResp != nil && dialResp.Body != nil {
					_ = dialResp.Body.Close()
				}
				if err != nil {
					t.Fatalf("dial mock gemini: %v", err)
				}
				defer func() {
					_ = wsConn.Close()
				}()

				sess := &MediaStreamSession{
					callID:     "call-steer-active",
					geminiConn: wsConn,
				}
				handler.sessionMgr.registerSession("call-steer-active", sess)
			}

			_, err := handler.SteerCall(context.Background(), SteerCallArgs{
				CallID:  tc.callID,
				Message: tc.msg,
				Mode:    tc.mode,
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("SteerCall() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestToolHandler_EndCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		callID  string
		wantErr bool
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler, store, _ := setupTestToolHandler()

			if tc.callID == "call-active-1" {
				_ = store.CreateOutboundCall(context.Background(), VoiceCall{
					CallID:          "call-active-1",
					ProviderCallSID: "CA999",
					State:           "in-progress",
					FromNumber:      "+15105550100",
					ToNumber:        "+15105550199",
				}, 5)
			}

			out, err := handler.EndCall(context.Background(), EndCallArgs{CallID: tc.callID})
			if (err != nil) != tc.wantErr {
				t.Fatalf("EndCall() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && out.Status != "ended" {
				t.Fatalf("EndCall() status = %s, want 'ended'", out.Status)
			}
		})
	}
}

func TestToolHandler_GetAndListCalls(t *testing.T) {
	t.Parallel()

	handler, store, _ := setupTestToolHandler()
	_ = store.CreateOutboundCall(context.Background(), VoiceCall{
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

	t.Run("send sms success", func(t *testing.T) {
		t.Parallel()
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
	})

	t.Run("send sms invalid number", func(t *testing.T) {
		t.Parallel()
		_, err := handler.SendSMS(context.Background(), SendSMSArgs{
			To:   "invalid",
			Body: "Test",
		})
		if err == nil {
			t.Fatal("expected error for invalid phone number")
		}

		_, errFromInv := handler.SendSMS(context.Background(), SendSMSArgs{
			To:   "+15105550199",
			From: "invalid-source-number",
			Body: "Test",
		})
		if errFromInv == nil {
			t.Fatal("expected error for invalid from number")
		}
	})

	t.Run("list sms", func(t *testing.T) {
		t.Parallel()
		out, err := handler.ListSMS(context.Background(), ListSMSArgs{Limit: 5})
		if err != nil {
			t.Fatalf("ListSMS() err = %v", err)
		}
		_ = out
	})

	t.Run("ensure phone number", func(t *testing.T) {
		t.Parallel()
		handler, _, twilioMock := setupTestToolHandler()
		out, err := handler.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: "510"})
		if err != nil || out.PhoneNumber == "" || out.Status != "configured" {
			t.Fatalf("EnsurePhoneNumber() err = %v, out = %+v", err, out)
		}
		if twilioMock.lastWebhookBaseURL != "https://webhooks.example.com/api/webhooks" {
			t.Fatalf("lastWebhookBaseURL = %q, want https://webhooks.example.com/api/webhooks", twilioMock.lastWebhookBaseURL)
		}
	})

	t.Run("send sms empty body or missing from", func(t *testing.T) {
		t.Parallel()
		_, err := handler.SendSMS(context.Background(), SendSMSArgs{
			To:   "+15105550199",
			Body: "",
		})
		if err == nil {
			t.Fatal("expected error for empty body")
		}

		handlerNoFrom := NewToolHandler(&Config{}, nil, nil, nil, nil)
		_, err2 := handlerNoFrom.SendSMS(context.Background(), SendSMSArgs{
			To:   "+15105550199",
			Body: "hi",
		})
		if err2 == nil {
			t.Fatal("expected error for missing from")
		}
	})

	t.Run("list sms and calls limit branches", func(t *testing.T) {
		t.Parallel()
		_, _ = handler.ListSMS(context.Background(), ListSMSArgs{Limit: -1})
		_, _ = handler.ListSMS(context.Background(), ListSMSArgs{Limit: 100})
		_, _ = handler.ListCalls(context.Background(), ListCallsArgs{Limit: -1})
		_, _ = handler.ListCalls(context.Background(), ListCallsArgs{Limit: 100})

		emptyStore := newMockStoreForDelivery()
		h := NewToolHandler(handler.cfg, emptyStore, &mockTwilioForDelivery{}, nil, nil)
		outCalls, err := h.ListCalls(context.Background(), ListCallsArgs{})
		if err != nil || outCalls.Calls == nil {
			t.Fatal("expected empty non-nil calls slice")
		}
		outSMS, err := h.ListSMS(context.Background(), ListSMSArgs{})
		if err != nil || outSMS.Messages == nil {
			t.Fatal("expected empty non-nil messages slice")
		}
	})

	t.Run("ensure phone number default area code", func(t *testing.T) {
		t.Parallel()
		out, err := handler.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: ""})
		if err != nil || out.Status != "configured" {
			t.Fatalf("EnsurePhoneNumber() err = %v, out = %+v", err, out)
		}
	})
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
	_ = store.CreateOutboundCall(context.Background(), VoiceCall{
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
			body:     `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"initiate_call","arguments":{"to":"+15105550199","message":"hello"}}}`,
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

	cfg := &Config{
		TwilioFromNumber:     "+15105550100",
		PublicWebhookBaseURL: "https://webhooks.example.com",
		MaxConcurrentCalls:   5,
	}

	t.Run("initiate call store error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		store.opErr = errors.New("db error")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)
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
		store := newMockStoreForDelivery()
		twilioMock := &mockTwilioForDelivery{createCallErr: errors.New("twilio network error")}
		h := NewToolHandler(cfg, store, twilioMock, nil, nil)
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
		store := newMockStoreForDelivery()
		store.opErr = errors.New("db error")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)
		_, err := h.EndCall(context.Background(), EndCallArgs{CallID: "c1"})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("get call status store error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		store.opErr = errors.New("db error")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)
		_, err := h.GetCallStatus(context.Background(), GetCallStatusArgs{CallID: "c1"})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("list calls store error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		store.opErr = errors.New("db error")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)
		_, err := h.ListCalls(context.Background(), ListCallsArgs{})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("send sms twilio error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		twilioMock := &mockTwilioForDelivery{sendSMSErr: errors.New("twilio sms fail")}
		h := NewToolHandler(cfg, store, twilioMock, nil, nil)
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
		store := newMockStoreForDelivery()
		store.opErr = errors.New("db error")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)
		_, err := h.ListSMS(context.Background(), ListSMSArgs{})
		if err == nil {
			t.Fatal("expected error on store failure")
		}
	})

	t.Run("ensure phone number twilio error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		twilioMock := &mockTwilioForDelivery{ensurePhoneErr: errors.New("twilio phone fail")}
		h := NewToolHandler(cfg, store, twilioMock, nil, nil)
		_, err := h.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: "510"})
		if err == nil {
			t.Fatal("expected error on twilio phone fail")
		}
	})

	t.Run("steer call with nil session manager", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)
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
	cfg := &Config{TwilioFromNumber: "+15105550100"}
	store := newMockStoreForDelivery()
	h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)

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
		hNoTwilio := NewToolHandler(cfg, store, nil, nil, nil)
		if _, err := hNoTwilio.EnsurePhoneNumber(context.Background(), EnsurePhoneNumberArgs{AreaCode: ""}); err == nil {
			t.Fatal("expected err")
		}
	})
}

func TestToolHandler_UncoveredBranches(t *testing.T) {
	t.Parallel()
	cfg := &Config{TwilioFromNumber: "+15105550100"}

	t.Run("list sms zero limit defaults to 20", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)
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
		store := newMockStoreForDelivery()
		_ = store.CreateOutboundCall(context.Background(), VoiceCall{
			CallID:          "call-hangup-err",
			ProviderCallSID: "CA-hangup-err",
			State:           "in-progress",
		}, 5)
		twilioMock := &mockTwilioForDelivery{hangupErr: errors.New("hangup failed")}
		rep := NewReporter(&Config{BrainURL: ""}, store, twilioMock)
		h := NewToolHandler(cfg, store, twilioMock, nil, rep)

		out, err := h.EndCall(context.Background(), EndCallArgs{CallID: "call-hangup-err"})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if out.Status != "ended" {
			t.Fatalf("status = %s, want ended", out.Status)
		}
	})

	t.Run("initiate call with twilio error and state update error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		twilioMock := &mockTwilioForDelivery{createCallErr: errors.New("dial fail")}
		h := NewToolHandler(cfg, store, twilioMock, nil, nil)
		store.opErr = errors.New("db error")

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
		store := newMockStoreForDelivery()
		store.bindErr = errors.New("bind failure")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)

		out, err := h.InitiateCall(context.Background(), InitiateCallArgs{
			To:      "+15105550199",
			Message: "hello",
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if out.Status != "initiated" {
			t.Fatalf("status = %s, want initiated", out.Status)
		}
	})

	t.Run("end call update state error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		_ = store.CreateOutboundCall(context.Background(), VoiceCall{
			CallID: "call-upd-err",
			State:  "in-progress",
		}, 5)
		store.updateStateErr = errors.New("update state failure")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)

		out, err := h.EndCall(context.Background(), EndCallArgs{CallID: "call-upd-err"})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if out.Status != "ended" {
			t.Fatalf("status = %s, want ended", out.Status)
		}
	})

	t.Run("send sms save error", func(t *testing.T) {
		t.Parallel()
		store := newMockStoreForDelivery()
		store.opErr = errors.New("save failure")
		h := NewToolHandler(cfg, store, &mockTwilioForDelivery{}, nil, nil)

		out, err := h.SendSMS(context.Background(), SendSMSArgs{
			To:   "+15105550199",
			Body: "test message",
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if out.Status != "sent" {
			t.Fatalf("status = %s, want sent", out.Status)
		}
	})
}
