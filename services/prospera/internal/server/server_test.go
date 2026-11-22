package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/delivery"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/tools"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

// ---- webhook/route tests (moved from root server_test.go) ----

func setupTestServer() (*Server, *fakes.FakeStore, *config.Config) {
	cfg := &config.Config{
		Port:                  "4019",
		TwilioAuthToken:       "secret-token-123",
		TwilioFromNumber:      "+15105550100",
		PublicWebhookBaseURL:  "https://webhooks.example.com",
		DefaultDiscordChannel: "prospera",
		CallbackWindowMinutes: 10080,
		AllowFrom:             []string{"+15105550111"},
	}

	store := fakes.NewFakeStore()
	twilioMock := &fakes.FakeTwilio{
		AudioData:   []byte("test-audio"),
		ContentType: "audio/wav",
	}
	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
	sessionMgr := mediastream.NewCallSessionManager(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, nil, tokenMgr,
		mediastream.WithSessionFactory(newProsperaMediaSession),
	)
	reporter := delivery.NewReporter(fakes.StaticConfig{Cfg: cfg}, store, twilioMock)

	server := NewServer(newTestConfigSource(cfg), store, twilioMock, tokenMgr, sessionMgr, reporter)
	return server, store, cfg
}

type threeTierInboundTestCase struct {
	name         string
	form         url.Values
	validSig     bool
	wantCode     int
	wantBodySub  string
	wantCallTier string
}

func TestTwilioVoiceWebhook_ThreeTierInbound(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()

	// Pre-populate outbound call dialed 3 days ago for Tier 2 test
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:       "call-out-prior",
		Direction:    "outbound",
		FromNumber:   cfg.TwilioFromNumber,
		ToNumber:     "+15105550222",
		State:        "completed",
		ReportStatus: "delivered",
		StartedAt:    time.Now().UTC().Add(-72 * time.Hour),
		UpdatedAt:    time.Now().UTC().Add(-72 * time.Hour),
		Brief: model.CallBrief{
			Task: "Dentist appointment confirmation",
		},
	}, 5)

	// Pre-populate an outbound call waiting for connect
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-out-active",
		ProviderCallSID: "CA-out-123",
		Direction:       "outbound",
		FromNumber:      cfg.TwilioFromNumber,
		ToNumber:        "+15105550333",
		State:           "initiated",
		ReportStatus:    "pending",
		StartedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}, 5)

	tests := []threeTierInboundTestCase{
		{
			name: "invalid twilio signature returns forbidden",
			form: url.Values{
				"CallSid": []string{"CA-untrusted"},
				"From":    []string{"+15105550111"},
				"To":      []string{cfg.TwilioFromNumber},
			},
			validSig: false,
			wantCode: http.StatusForbidden,
		},
		{
			name: "outbound call answer returns stream twiml",
			form: url.Values{
				"CallSid":   []string{"CA-out-123"},
				"Direction": []string{"outbound-api"},
				"From":      []string{cfg.TwilioFromNumber},
				"To":        []string{"+15105550333"},
			},
			validSig:    true,
			wantCode:    http.StatusOK,
			wantBodySub: "<Connect>",
		},
		{
			name: "tier 1 allowlist caller returns stream twiml",
			form: url.Values{
				"CallSid": []string{"CA-in-tier1"},
				"From":    []string{"+15105550111"},
				"To":      []string{cfg.TwilioFromNumber},
			},
			validSig:     true,
			wantCode:     http.StatusOK,
			wantBodySub:  "<Connect>",
			wantCallTier: "tier1",
		},
		{
			name: "tier 2 7-day callback caller returns stream twiml with callback brief",
			form: url.Values{
				"CallSid": []string{"CA-in-tier2"},
				"From":    []string{"+15105550222"},
				"To":      []string{cfg.TwilioFromNumber},
			},
			validSig:     true,
			wantCode:     http.StatusOK,
			wantBodySub:  "<Connect>",
			wantCallTier: "tier2",
		},
		{
			name: "tier 3 unknown caller returns voicemail record twiml",
			form: url.Values{
				"CallSid": []string{"CA-in-tier3"},
				"From":    []string{"+15105550999"},
				"To":      []string{cfg.TwilioFromNumber},
			},
			validSig:     true,
			wantCode:     http.StatusOK,
			wantBodySub:  "<Record",
			wantCallTier: "tier3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runThreeTierInboundCase(t, tc, cfg, mux, store)
		})
	}
}

func runThreeTierInboundCase(t *testing.T, tc threeTierInboundTestCase, cfg *config.Config, mux http.Handler, store *fakes.FakeStore) {
	t.Helper()
	path := "/api/webhooks/twilio/voice"
	fullURL := cfg.PublicWebhookBaseURL + path

	sig := "invalid-signature"
	if tc.validSig {
		sig = fakes.TwilioSignature(cfg.TwilioAuthToken, fullURL, tc.form)
	}

	req := httptest.NewRequest(http.MethodPost, fullURL, strings.NewReader(tc.form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", sig)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != tc.wantCode {
		t.Fatalf("status code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
	}

	if tc.wantBodySub != "" && !strings.Contains(rec.Body.String(), tc.wantBodySub) {
		t.Fatalf("response body %q does not contain %q", rec.Body.String(), tc.wantBodySub)
	}

	assertThreeTierCall(t, tc, store)
}

func assertThreeTierCall(t *testing.T, tc threeTierInboundTestCase, store *fakes.FakeStore) {
	t.Helper()
	switch tc.wantCallTier {
	case "tier1":
		c, err := store.GetCallByProviderSID(context.Background(), "CA-in-tier1")
		if err != nil || c == nil {
			t.Fatalf("tier1 call not found in store: %v", err)
		}
		if !strings.Contains(c.Brief.Task, "Greet the caller professionally") {
			t.Fatalf("tier1 Task = %q, want default receptionist greeting", c.Brief.Task)
		}
		if c.Brief.Approvals == "" {
			t.Fatal("tier1 Approvals should be set to model.DefaultApprovalFirewall")
		}
	case "tier2":
		c, err := store.GetCallByProviderSID(context.Background(), "CA-in-tier2")
		if err != nil || c == nil {
			t.Fatalf("tier2 call not found in store: %v", err)
		}
		if c.CallbackOfCallID != "call-out-prior" {
			t.Fatalf("tier2 CallbackOfCallID = %q, want 'call-out-prior'", c.CallbackOfCallID)
		}
		if !strings.Contains(c.Brief.Context, "Dentist appointment confirmation") {
			t.Fatalf("tier2 Context = %q, want prior call task in context", c.Brief.Context)
		}
	case "tier3":
		c, err := store.GetCallByProviderSID(context.Background(), "CA-in-tier3")
		if err != nil || c == nil {
			t.Fatalf("tier3 call not found in store: %v", err)
		}
		if c.ReportStatus != "awaiting_recording" {
			t.Fatalf("tier3 ReportStatus = %q, want 'awaiting_recording'", c.ReportStatus)
		}
	}
}

func TestTwilioStatusAndVoicemailOrder(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()

	// Tier 3 inbound call created in awaiting_recording status
	callID := "call-vm-ordering-test"
	providerSID := "CA-ordering-123"
	_ = store.CreateInboundCall(context.Background(), model.VoiceCall{
		CallID:          callID,
		ProviderCallSID: providerSID,
		Direction:       "inbound",
		FromNumber:      "+15105550999",
		ToNumber:        cfg.TwilioFromNumber,
		State:           "in-progress",
		ReportStatus:    "awaiting_recording",
		StartedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	})

	// Step 1: /status webhook arrives first with CallStatus=completed
	statusForm := url.Values{
		"CallSid":    []string{providerSID},
		"CallStatus": []string{"completed"},
	}
	statusURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/status"
	sig := fakes.TwilioSignature(cfg.TwilioAuthToken, statusURL, statusForm)

	req := httptest.NewRequest(http.MethodPost, statusURL, strings.NewReader(statusForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", sig)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/status returned %d, want 200", rec.Code)
	}
	server.Wait()

	callAfterStatus, _ := store.GetCall(context.Background(), callID)
	if callAfterStatus.State != "completed" {
		t.Fatalf("call state = %s, want completed", callAfterStatus.State)
	}
	// Crucial check: ReportStatus MUST STILL BE awaiting_recording!
	if callAfterStatus.ReportStatus != "awaiting_recording" {
		t.Fatalf("ReportStatus was overwritten to %s, should remain awaiting_recording", callAfterStatus.ReportStatus)
	}

	// Step 2: /voicemail webhook arrives with recording URL
	vmForm := url.Values{
		"CallSid":      []string{providerSID},
		"RecordingUrl": []string{"https://api.twilio.com/recordings/RE123.wav"},
	}
	vmURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voicemail?call_id=" + callID
	sigVM := fakes.TwilioSignature(cfg.TwilioAuthToken, vmURL, vmForm)

	reqVM := httptest.NewRequest(http.MethodPost, vmURL, strings.NewReader(vmForm.Encode()))
	reqVM.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqVM.Header.Set("X-Twilio-Signature", sigVM)

	recVM := httptest.NewRecorder()
	mux.ServeHTTP(recVM, reqVM)

	if recVM.Code != http.StatusOK {
		t.Fatalf("/voicemail returned %d, want 200", recVM.Code)
	}
	server.Wait()
}

func TestTwilioWebhook_AMD(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()

	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-amd-test",
		ProviderCallSID: "CA-amd-1",
		Direction:       "outbound",
		FromNumber:      cfg.TwilioFromNumber,
		ToNumber:        "+15105550888",
		State:           "in-progress",
	}, 5)

	form := url.Values{
		"CallSid":    []string{"CA-amd-1"},
		"AnsweredBy": []string{"machine_start"},
	}
	path := "/api/webhooks/twilio/amd"
	fullURL := cfg.PublicWebhookBaseURL + path
	sig := fakes.TwilioSignature(cfg.TwilioAuthToken, fullURL, form)

	req := httptest.NewRequest(http.MethodPost, fullURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", sig)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("amd returned %d, want 200", rec.Code)
	}

	c, _ := store.GetCall(context.Background(), "call-amd-test")
	if c.AnsweredBy != "machine_start" {
		t.Fatalf("answered by = %s, want machine_start", c.AnsweredBy)
	}
}

func TestTwilioWebhook_SMS(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()

	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:     "call-prior-sms",
		Direction:  "outbound",
		FromNumber: cfg.TwilioFromNumber,
		ToNumber:   "+15105550888",
	}, 5)

	form := url.Values{
		"MessageSid": []string{"SM-test-1"},
		"From":       []string{"+15105550888"},
		"To":         []string{cfg.TwilioFromNumber},
		"Body":       []string{"Confirmation code: 4421"},
		"NumMedia":   []string{"1"},
		"MediaUrl0":  []string{"https://media.twilio.com/img1.png"},
	}
	path := "/api/webhooks/twilio/sms"
	fullURL := cfg.PublicWebhookBaseURL + path
	sig := fakes.TwilioSignature(cfg.TwilioAuthToken, fullURL, form)

	req := httptest.NewRequest(http.MethodPost, fullURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", sig)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("sms returned %d, want 200", rec.Code)
	}

	msgs, _ := store.ListVoiceMessages(context.Background(), 10)
	if len(msgs) == 0 || msgs[0].MessageSID != "SM-test-1" {
		t.Fatalf("expected saved message SM-test-1, got %v", msgs)
	}
	if msgs[0].RelatedCallID != "call-prior-sms" {
		t.Fatalf("expected related call ID 'call-prior-sms', got %q", msgs[0].RelatedCallID)
	}
}

func TestServer_ToWSSAndRouteGuards(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()

	t.Run("toWSS helper variations", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			input string
			want  string
		}{
			{"https://webhooks.example.com", "wss://webhooks.example.com"},
			{"http://webhooks.example.com", "ws://webhooks.example.com"},
			{"webhooks.example.com", "wss://webhooks.example.com"},
		}
		for _, tc := range tests {
			if got := toWSS(tc.input); got != tc.want {
				t.Fatalf("toWSS(%q) = %q, want %q", tc.input, got, tc.want)
			}
		}
	})

	t.Run("new server nil config panics", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil config")
			}
		}()
		NewServer(nil, nil, nil, nil, nil, nil)
	})

	t.Run("stream handler without session manager returns 503", func(t *testing.T) {
		t.Parallel()
		srv := &Server{}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/webhooks/twilio/stream/tok", http.NoBody)
		srv.handleTwilioStream(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})

	t.Run("status webhook with non-terminal status or unknown call", func(t *testing.T) {
		t.Parallel()
		form := url.Values{
			"CallSid":    []string{"CA-unknown-sid"},
			"CallStatus": []string{"ringing"},
		}
		path := "/api/webhooks/twilio/status"
		fullURL := cfg.PublicWebhookBaseURL + path
		sig := fakes.TwilioSignature(cfg.TwilioAuthToken, fullURL, form)

		req := httptest.NewRequest(http.MethodPost, fullURL, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Twilio-Signature", sig)

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status webhook returned %d, want 200", rec.Code)
		}
	})

	t.Run("voicemail webhook resolves call id from call sid", func(t *testing.T) {
		t.Parallel()
		_ = store.CreateInboundCall(context.Background(), model.VoiceCall{
			CallID:          "call-vm-by-sid",
			ProviderCallSID: "CA-vm-sid-1",
			ReportStatus:    "awaiting_recording",
		})

		form := url.Values{
			"CallSid":      []string{"CA-vm-sid-1"},
			"RecordingUrl": []string{"https://api.twilio.com/rec.wav"},
		}
		path := "/api/webhooks/twilio/voicemail"
		fullURL := cfg.PublicWebhookBaseURL + path
		sig := fakes.TwilioSignature(cfg.TwilioAuthToken, fullURL, form)

		req := httptest.NewRequest(http.MethodPost, fullURL, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Twilio-Signature", sig)

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("voicemail webhook returned %d, want 200", rec.Code)
		}
		server.Wait()
	})

	t.Run("voice webhook with invalid phone numbers", func(t *testing.T) {
		t.Parallel()
		form := url.Values{
			"CallSid": []string{"CA-invalid-num"},
			"From":    []string{"not-a-phone"},
			"To":      []string{cfg.TwilioFromNumber},
		}
		path := "/api/webhooks/twilio/voice"
		fullURL := cfg.PublicWebhookBaseURL + path
		sig := fakes.TwilioSignature(cfg.TwilioAuthToken, fullURL, form)

		req := httptest.NewRequest(http.MethodPost, fullURL, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("X-Twilio-Signature", sig)

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("voice webhook returned %d, want 400", rec.Code)
		}
	})

	t.Run("mcp server routes POST ping and GET banner", func(t *testing.T) {
		t.Parallel()
		// POST ping
		reqPost := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":10,"method":"ping"}`))
		reqPost.Header.Set("Content-Type", "application/json")
		recPost := httptest.NewRecorder()
		mux.ServeHTTP(recPost, reqPost)
		if recPost.Code != http.StatusOK {
			t.Fatalf("POST /mcp returned %d, want 200", recPost.Code)
		}

		// GET ready banner
		reqGet := httptest.NewRequest(http.MethodGet, "/mcp", http.NoBody)
		recGet := httptest.NewRecorder()
		mux.ServeHTTP(recGet, reqGet)
		if recGet.Code != http.StatusOK || !strings.Contains(recGet.Body.String(), "Prospera") {
			t.Fatalf("GET /mcp returned %d, body: %s", recGet.Code, recGet.Body.String())
		}
	})
}

func TestWebhooks_InvalidRequests(t *testing.T) {
	t.Parallel()
	server, _, cfg := setupTestServer()
	mux := server.Routes()

	tests := []struct {
		name       string
		endpoint   string
		body       string
		sig        string
		wantStatus int
	}{
		{name: "amd bad sig", endpoint: "/api/webhooks/twilio/amd", body: "CallSid=CA123", sig: "bad-sig", wantStatus: http.StatusForbidden},
		{name: "status bad sig", endpoint: "/api/webhooks/twilio/status", body: "CallSid=CA123", sig: "bad-sig", wantStatus: http.StatusForbidden},
		{name: "voicemail bad sig", endpoint: "/api/webhooks/twilio/voicemail", body: "CallSid=CA123", sig: "bad-sig", wantStatus: http.StatusForbidden},
		{name: "sms bad sig", endpoint: "/api/webhooks/twilio/sms", body: "CallSid=CA123", sig: "bad-sig", wantStatus: http.StatusForbidden},
		{name: "voice bad form", endpoint: "/api/webhooks/twilio/voice", body: "%%invalid-url-encoded-form", wantStatus: http.StatusBadRequest},
		{name: "amd bad form", endpoint: "/api/webhooks/twilio/amd", body: "%%invalid-url-encoded-form", wantStatus: http.StatusBadRequest},
		{name: "status bad form", endpoint: "/api/webhooks/twilio/status", body: "%%invalid-url-encoded-form", wantStatus: http.StatusBadRequest},
		{name: "voicemail bad form", endpoint: "/api/webhooks/twilio/voicemail", body: "%%invalid-url-encoded-form", wantStatus: http.StatusBadRequest},
		{name: "sms bad form", endpoint: "/api/webhooks/twilio/sms", body: "%%invalid-url-encoded-form", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fullURL := cfg.PublicWebhookBaseURL + tc.endpoint
			req := httptest.NewRequest(http.MethodPost, fullURL, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tc.sig != "" {
				req.Header.Set("X-Twilio-Signature", tc.sig)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s status = %d, want %d", tc.name, rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestIsTerminalState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state string
		want  bool
	}{
		{"completed", true},
		{"busy", true},
		{"no-answer", true},
		{"failed", true},
		{"canceled", true},
		{"in-progress", false},
		{"ringing", false},
		{"queued", false},
	}
	for _, tc := range tests {
		if got := isTerminalState(tc.state); got != tc.want {
			t.Errorf("isTerminalState(%q) = %v, want %v", tc.state, got, tc.want)
		}
	}
}

func TestServer_VoicemailVariations(t *testing.T) {
	t.Parallel()
	server, _, cfg := setupTestServer()
	mux := server.Routes()

	// 1. call_id in post form
	form := url.Values{
		"call_id":      []string{"call-form-id"},
		"RecordingUrl": []string{"https://api.twilio.com/rec.wav"},
	}
	vmURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voicemail"
	sig := fakes.TwilioSignature(cfg.TwilioAuthToken, vmURL, form)
	req := httptest.NewRequest(http.MethodPost, vmURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", sig)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("voicemail with call_id in form returned %d", rec.Code)
	}
	server.Wait()

	// 2. empty call_id and no CallSid
	formEmpty := url.Values{
		"RecordingUrl": []string{"https://api.twilio.com/rec.wav"},
	}
	sigEmpty := fakes.TwilioSignature(cfg.TwilioAuthToken, vmURL, formEmpty)
	reqEmpty := httptest.NewRequest(http.MethodPost, vmURL, strings.NewReader(formEmpty.Encode()))
	reqEmpty.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqEmpty.Header.Set("X-Twilio-Signature", sigEmpty)
	recEmpty := httptest.NewRecorder()
	mux.ServeHTTP(recEmpty, reqEmpty)
	if recEmpty.Code != http.StatusOK {
		t.Fatalf("voicemail with empty call_id returned %d", recEmpty.Code)
	}
	server.Wait()
}

func TestServer_StoreErrorsInHandlers(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()
	store.OpErr = errors.New("db down")

	// 1. Voice webhook with store error
	form := url.Values{
		"CallSid": []string{"CA-store-err"},
		"From":    []string{"+15105550999"},
		"To":      []string{cfg.TwilioFromNumber},
	}
	vURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voice"
	sig := fakes.TwilioSignature(cfg.TwilioAuthToken, vURL, form)
	req := httptest.NewRequest(http.MethodPost, vURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", sig)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("voice returned %d, want 200", rec.Code)
	}

	// 2. AMD webhook with store error
	amdForm := url.Values{
		"CallSid":    []string{"CA-store-err"},
		"AnsweredBy": []string{"human"},
	}
	amdURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/amd"
	sigAMD := fakes.TwilioSignature(cfg.TwilioAuthToken, amdURL, amdForm)
	reqAMD := httptest.NewRequest(http.MethodPost, amdURL, strings.NewReader(amdForm.Encode()))
	reqAMD.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqAMD.Header.Set("X-Twilio-Signature", sigAMD)
	recAMD := httptest.NewRecorder()
	mux.ServeHTTP(recAMD, reqAMD)
	if recAMD.Code != http.StatusOK {
		t.Fatalf("amd returned %d, want 200", recAMD.Code)
	}

	// 3. Status webhook with store error
	statForm := url.Values{
		"CallSid":    []string{"CA-store-err"},
		"CallStatus": []string{"completed"},
	}
	statURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/status"
	sigStat := fakes.TwilioSignature(cfg.TwilioAuthToken, statURL, statForm)
	reqStat := httptest.NewRequest(http.MethodPost, statURL, strings.NewReader(statForm.Encode()))
	reqStat.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqStat.Header.Set("X-Twilio-Signature", sigStat)
	recStat := httptest.NewRecorder()
	mux.ServeHTTP(recStat, reqStat)
	if recStat.Code != http.StatusOK {
		t.Fatalf("status returned %d, want 200", recStat.Code)
	}
	server.Wait()

	// 4. SMS webhook with store error
	smsForm := url.Values{
		"MessageSid": []string{"SM-err-1"},
		"From":       []string{"+15105550888"},
		"To":         []string{cfg.TwilioFromNumber},
		"Body":       []string{"hello"},
	}
	smsURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/sms"
	sigSMS := fakes.TwilioSignature(cfg.TwilioAuthToken, smsURL, smsForm)
	reqSMS := httptest.NewRequest(http.MethodPost, smsURL, strings.NewReader(smsForm.Encode()))
	reqSMS.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqSMS.Header.Set("X-Twilio-Signature", sigSMS)
	recSMS := httptest.NewRecorder()
	mux.ServeHTTP(recSMS, reqSMS)
	if recSMS.Code != http.StatusOK {
		t.Fatalf("sms returned %d, want 200", recSMS.Code)
	}
}

func TestServer_TokenIssueFailures(t *testing.T) {
	t.Parallel()
	server, store, cfg := setupTestServer()
	tokenMgrFail := twilio.NewStreamTokenManager(nil, func([]byte) (int, error) {
		return 0, errors.New("entropy failure")
	})
	server.tokenMgr = tokenMgrFail
	mux := server.Routes()

	// 1. Outbound connect token issue failure -> 500
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-out-err",
		ProviderCallSID: "CA-out-err",
		Direction:       "outbound",
	}, 5)
	formOut := url.Values{
		"CallSid":   []string{"CA-out-err"},
		"Direction": []string{"outbound-api"},
	}
	vURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voice"
	sigOut := fakes.TwilioSignature(cfg.TwilioAuthToken, vURL, formOut)
	reqOut := httptest.NewRequest(http.MethodPost, vURL, strings.NewReader(formOut.Encode()))
	reqOut.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqOut.Header.Set("X-Twilio-Signature", sigOut)
	recOut := httptest.NewRecorder()
	mux.ServeHTTP(recOut, reqOut)
	if recOut.Code != http.StatusInternalServerError {
		t.Fatalf("outbound token fail returned %d, want 500", recOut.Code)
	}

	// 2. Inbound connect token issue failure -> 500
	formIn := url.Values{
		"CallSid": []string{"CA-in-allow"},
		"From":    []string{"+15105550111"},
		"To":      []string{cfg.TwilioFromNumber},
	}
	sigIn := fakes.TwilioSignature(cfg.TwilioAuthToken, vURL, formIn)
	reqIn := httptest.NewRequest(http.MethodPost, vURL, strings.NewReader(formIn.Encode()))
	reqIn.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqIn.Header.Set("X-Twilio-Signature", sigIn)
	recIn := httptest.NewRecorder()
	mux.ServeHTTP(recIn, reqIn)
	if recIn.Code != http.StatusInternalServerError {
		t.Fatalf("inbound token fail returned %d, want 500", recIn.Code)
	}
}

func TestServer_UnnormalizedPhonesAndReporterErrors(t *testing.T) {
	t.Parallel()
	server, _, cfg := setupTestServer()
	cfg.AllowFrom = []string{"not-a-valid-phone", "+15105550111"}
	mux := server.Routes()

	// 1. SMS with unnormalized from/to and media parsing
	smsForm := url.Values{
		"MessageSid": []string{"SM-unnorm"},
		"From":       []string{"123"},
		"To":         []string{"456"},
		"Body":       []string{"unnormalized test"},
		"NumMedia":   []string{"2"},
		"MediaUrl0":  []string{"https://img1"},
		"MediaUrl1":  []string{"https://img2"},
	}
	smsURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/sms"
	sigSMS := fakes.TwilioSignature(cfg.TwilioAuthToken, smsURL, smsForm)
	reqSMS := httptest.NewRequest(http.MethodPost, smsURL, strings.NewReader(smsForm.Encode()))
	reqSMS.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqSMS.Header.Set("X-Twilio-Signature", sigSMS)
	recSMS := httptest.NewRecorder()
	mux.ServeHTTP(recSMS, reqSMS)
	if recSMS.Code != http.StatusOK {
		t.Fatalf("sms returned %d, want 200", recSMS.Code)
	}

	// 2. Inbound voice call hits unnormalized allow from
	voiceForm := url.Values{
		"CallSid": []string{"CA-allow-test"},
		"From":    []string{"+15105550111"},
		"To":      []string{cfg.TwilioFromNumber},
	}
	vURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voice"
	sigV := fakes.TwilioSignature(cfg.TwilioAuthToken, vURL, voiceForm)
	reqV := httptest.NewRequest(http.MethodPost, vURL, strings.NewReader(voiceForm.Encode()))
	reqV.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqV.Header.Set("X-Twilio-Signature", sigV)
	recV := httptest.NewRecorder()
	mux.ServeHTTP(recV, reqV)
	if recV.Code != http.StatusOK {
		t.Fatalf("voice returned %d, want 200", recV.Code)
	}

	// 3. Status completed with failing reporter
	serverFailRep, storeFail, cfgFail := setupTestServer()
	serverFailRep.reporter = delivery.NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: ""}}, storeFail, nil)
	muxFail := serverFailRep.Routes()
	_ = storeFail.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-stat-failrep",
		ProviderCallSID: "CA-stat-failrep",
		ReportStatus:    "pending",
	}, 5)
	statForm := url.Values{
		"CallSid":    []string{"CA-stat-failrep"},
		"CallStatus": []string{"completed"},
	}
	statURL := cfgFail.PublicWebhookBaseURL + "/api/webhooks/twilio/status"
	sigStat := fakes.TwilioSignature(cfgFail.TwilioAuthToken, statURL, statForm)
	reqStat := httptest.NewRequest(http.MethodPost, statURL, strings.NewReader(statForm.Encode()))
	reqStat.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqStat.Header.Set("X-Twilio-Signature", sigStat)
	recStat := httptest.NewRecorder()
	muxFail.ServeHTTP(recStat, reqStat)
	if recStat.Code != http.StatusOK {
		t.Fatalf("status returned %d, want 200", recStat.Code)
	}
	serverFailRep.Wait()
}

type failResponseWriter struct {
	header http.Header
}

func (f *failResponseWriter) Header() http.Header     { return f.header }
func (*failResponseWriter) WriteHeader(int)           {}
func (*failResponseWriter) Write([]byte) (int, error) { return 0, errors.New("write error") }

func TestWriteXML_Error(t *testing.T) {
	t.Parallel()
	w := &failResponseWriter{header: make(http.Header)}
	writeXML(w, "<Response/>")
}

func TestTwilioVoiceWebhook_CallIDResolutionAndNotFound(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()

	// 1. Unbound outbound call arrives before BindProviderCallSID commits.
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-out-unbound",
		ProviderCallSID: "",
		Direction:       "outbound",
		FromNumber:      cfg.TwilioFromNumber,
		ToNumber:        "+15105550777",
		State:           "initiated",
		StartedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}, 5)

	form := url.Values{
		"CallSid":   []string{"CA-fast-arrival"},
		"Direction": []string{"outbound-api"},
	}
	vURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voice?call_id=call-out-unbound"
	sig := fakes.TwilioSignature(cfg.TwilioAuthToken, vURL, form)

	req := httptest.NewRequest(http.MethodPost, vURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", sig)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unbound voice webhook code = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<Connect>") {
		t.Fatalf("unbound voice response %q missing <Connect>", rec.Body.String())
	}

	boundCall, err := store.GetCall(context.Background(), "call-out-unbound")
	if err != nil || boundCall == nil || boundCall.ProviderCallSID != "CA-fast-arrival" {
		t.Fatalf("call was not bound to CA-fast-arrival: %+v, err: %v", boundCall, err)
	}

	// 1b. Unbound outbound call with call_id in form body
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-out-unbound-form",
		ProviderCallSID: "",
		Direction:       "outbound",
		FromNumber:      cfg.TwilioFromNumber,
		ToNumber:        "+15105550778",
		State:           "initiated",
		StartedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}, 5)

	formB := url.Values{
		"CallSid":   []string{"CA-fast-arrival-form"},
		"Direction": []string{"outbound-api"},
		"call_id":   []string{"call-out-unbound-form"},
	}
	vURLB := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voice"
	sigB := fakes.TwilioSignature(cfg.TwilioAuthToken, vURLB, formB)

	reqB := httptest.NewRequest(http.MethodPost, vURLB, strings.NewReader(formB.Encode()))
	reqB.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqB.Header.Set("X-Twilio-Signature", sigB)

	recB := httptest.NewRecorder()
	mux.ServeHTTP(recB, reqB)

	if recB.Code != http.StatusOK {
		t.Fatalf("unbound form voice webhook code = %d, want 200", recB.Code)
	}

	// 2. Outbound connect with unknown / missing call returns 404
	formNotFound := url.Values{
		"CallSid":   []string{"CA-unknown"},
		"Direction": []string{"outbound-api"},
	}
	vURLNotFound := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voice"
	sigNotFound := fakes.TwilioSignature(cfg.TwilioAuthToken, vURLNotFound, formNotFound)

	reqNotFound := httptest.NewRequest(http.MethodPost, vURLNotFound, strings.NewReader(formNotFound.Encode()))
	reqNotFound.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqNotFound.Header.Set("X-Twilio-Signature", sigNotFound)

	recNotFound := httptest.NewRecorder()
	mux.ServeHTTP(recNotFound, reqNotFound)

	if recNotFound.Code != http.StatusNotFound {
		t.Fatalf("unknown outbound connect code = %d, want 404", recNotFound.Code)
	}
}

func TestTwilioStatusAndAMD_CallIDResolution(t *testing.T) {
	t.Parallel()

	server, store, cfg := setupTestServer()
	mux := server.Routes()

	// 1. Status webhook with unbound SID resolves by call_id query param
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-unbound-stat",
		ProviderCallSID: "",
		Direction:       "outbound",
		State:           "initiated",
		StartedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}, 5)

	statForm := url.Values{
		"CallSid":    []string{"CA-stat-unbound"},
		"CallStatus": []string{"in-progress"},
	}
	statURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/status?call_id=call-unbound-stat"
	sigStat := fakes.TwilioSignature(cfg.TwilioAuthToken, statURL, statForm)

	reqStat := httptest.NewRequest(http.MethodPost, statURL, strings.NewReader(statForm.Encode()))
	reqStat.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqStat.Header.Set("X-Twilio-Signature", sigStat)

	recStat := httptest.NewRecorder()
	mux.ServeHTTP(recStat, reqStat)

	if recStat.Code != http.StatusOK {
		t.Fatalf("status webhook with call_id code = %d, want 200", recStat.Code)
	}

	boundStatCall, err := store.GetCall(context.Background(), "call-unbound-stat")
	if err != nil || boundStatCall == nil || boundStatCall.ProviderCallSID != "CA-stat-unbound" {
		t.Fatalf("status call not bound to CA-stat-unbound: %+v, err: %v", boundStatCall, err)
	}
	if boundStatCall.State != "in-progress" {
		t.Fatalf("status call state = %q, want in-progress", boundStatCall.State)
	}

	// 2. AMD webhook with unbound SID resolves by call_id query param
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          "call-unbound-amd",
		ProviderCallSID: "",
		Direction:       "outbound",
		State:           "in-progress",
		StartedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}, 5)

	sess := mediastream.NewMediaStreamSession(mediastream.MediaStreamSessionConfig{
		CallID: "call-unbound-amd",
	})
	server.sessionMgr.RegisterSession("call-unbound-amd", sess)

	amdForm := url.Values{
		"CallSid":    []string{"CA-amd-unbound"},
		"AnsweredBy": []string{"human"},
	}
	amdURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/amd?call_id=call-unbound-amd"
	sigAMD := fakes.TwilioSignature(cfg.TwilioAuthToken, amdURL, amdForm)

	reqAMD := httptest.NewRequest(http.MethodPost, amdURL, strings.NewReader(amdForm.Encode()))
	reqAMD.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqAMD.Header.Set("X-Twilio-Signature", sigAMD)

	recAMD := httptest.NewRecorder()
	mux.ServeHTTP(recAMD, reqAMD)

	if recAMD.Code != http.StatusOK {
		t.Fatalf("amd webhook with call_id code = %d, want 200", recAMD.Code)
	}

	boundAMDCall, err := store.GetCall(context.Background(), "call-unbound-amd")
	if err != nil || boundAMDCall == nil || boundAMDCall.ProviderCallSID != "CA-amd-unbound" {
		t.Fatalf("amd call not bound to CA-amd-unbound: %+v, err: %v", boundAMDCall, err)
	}
	if boundAMDCall.AnsweredBy != "human" {
		t.Fatalf("amd call AnsweredBy = %q, want human", boundAMDCall.AnsweredBy)
	}
}

func TestServer_ReloadEndpointSwapsLiveConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		reloadedCfg  *config.Config
		wantStatus   string
		wantSMSFrom  string
		wantRestarts bool
	}{
		{
			name:        "live field applied to next tool call",
			reloadedCfg: &config.Config{Port: "4019", TwilioFromNumber: "+15105550222"},
			wantStatus:  `"status":"reloaded"`,
			wantSMSFrom: "+15105550222",
		},
		{
			name:         "restart field keeps serving old snapshot",
			reloadedCfg:  &config.Config{Port: "5000", TwilioFromNumber: "+15105550222"},
			wantStatus:   `"status":"restarting"`,
			wantSMSFrom:  "+15105550100",
			wantRestarts: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfgSrc := httpkit.NewReloadable(&config.Config{Port: "4019", TwilioFromNumber: "+15105550100"},
				func() (*config.Config, error) { return tc.reloadedCfg, nil }, config.RestartRequired)
			twilioMock := &fakes.FakeTwilio{}
			server := NewServer(cfgSrc, fakes.NewFakeStore(), twilioMock, nil, nil, nil)

			req := httptest.NewRequest(http.MethodPost, "/-/reload", http.NoBody)
			req.RemoteAddr = "127.0.0.1:5555"
			rec := httptest.NewRecorder()
			server.Routes().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), tc.wantStatus) {
				t.Fatalf("reload = %d %s, want 200 containing %s", rec.Code, rec.Body.String(), tc.wantStatus)
			}

			if _, err := server.toolHandler.SendSMS(context.Background(), tools.SendSMSArgs{To: "+15105550199", Body: "hi"}); err != nil {
				t.Fatalf("SendSMS() error = %v", err)
			}
			if twilioMock.LastSendSMSFrom != tc.wantSMSFrom {
				t.Errorf("SMS from = %q, want %q", twilioMock.LastSendSMSFrom, tc.wantSMSFrom)
			}
			select {
			case <-cfgSrc.RestartRequested():
				if !tc.wantRestarts {
					t.Error("restart requested unexpectedly")
				}
			default:
				if tc.wantRestarts {
					t.Error("restart not requested")
				}
			}
		})
	}
}

func TestServer_RoutesMediaStreamUpgrade(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		validToken bool
		wantStatus int
	}{
		{name: "traced mux still upgrades the media stream", validToken: true, wantStatus: http.StatusSwitchingProtocols},
		{name: "bad token is rejected before upgrade", wantStatus: http.StatusUnauthorized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &config.Config{GeminiLiveModel: "gemini-3.8-live"}
			store := fakes.NewFakeStore()
			tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
			built := make(chan struct{}, 1)
			sessionMgr := mediastream.NewCallSessionManager(fakes.StaticConfig{Cfg: cfg}, store, nil, nil, tokenMgr,
				mediastream.WithSessionFactory(func(sc mediastream.SessionConfig) mediastream.Session {
					sc.GeminiDialer = func(context.Context, string) (*websocket.Conn, error) {
						return nil, errors.New("gemini unavailable")
					}
					sess := newProsperaMediaSession(sc)
					built <- struct{}{}
					return sess
				}),
			)
			server := NewServer(newTestConfigSource(cfg), store, nil, tokenMgr, sessionMgr, delivery.NewReporter(fakes.StaticConfig{Cfg: cfg}, store, nil))
			srv := httptest.NewServer(server.Routes())
			t.Cleanup(srv.Close)

			token := "bad-token"
			if tc.validToken {
				token, _ = tokenMgr.Issue("call-upgrade", 30*time.Second)
			}
			conn, resp, _ := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/api/webhooks/twilio/stream/"+token, nil)
			if conn != nil {
				_ = conn.Close()
			}
			if resp == nil {
				t.Fatal("no handshake response")
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Errorf("handshake status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if !tc.validToken {
				return
			}
			select {
			case <-built:
			case <-time.After(5 * time.Second):
				t.Fatal("upgraded stream never built a prospera media session")
			}
		})
	}
}

// ---- instructions test (moved from root instructions_test.go) ----

func TestServerInstructions(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{TwilioFromNumber: "+15105550100", MaxConcurrentCalls: 5}
	ts := httptest.NewServer(NewServer(newTestConfigSource(cfg), fakes.NewFakeStore(), &fakes.FakeTwilio{}, nil, nil, nil).Routes())
	t.Cleanup(ts.Close)

	tests := []struct {
		name     string
		path     string
		wantTool string
	}{
		{"initialize advertises workflow instructions", "/mcp", "ensure_phone_number"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := mcp.NewClient(&mcp.Implementation{Name: "instructions-test", Version: "1.0.0"}, nil)
			session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
				Endpoint:             ts.URL + tc.path,
				HTTPClient:           ts.Client(),
				DisableStandaloneSSE: true,
			}, nil)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer session.Close()

			got := session.InitializeResult().Instructions
			if got == "" || len(got) > 1024 {
				t.Fatalf("instructions length = %d, want 1..1024", len(got))
			}
			if !strings.Contains(got, tc.wantTool) {
				t.Errorf("instructions missing tool name %q: %q", tc.wantTool, got)
			}
		})
	}
}

// WithServerHTTPClient configures an HTTP client for Server outbound calls in tests.
func WithServerHTTPClient(client *http.Client) ServerOption {
	return func(s *Server) {
		s.httpClient = client
	}
}

type triggerPostCallReportTestCase struct {
	name          string
	callID        string
	ingressStatus int
	emptyIngress  bool
	noReporter    bool
	wantPath      string
	wantKey       string
	wantDelivered bool
	wantErr       bool
}

func runTriggerPostCallReportTest(t *testing.T, tc triggerPostCallReportTestCase) {
	t.Helper()
	var receivedPath string
	var receivedKey string
	var mu sync.Mutex

	ingressCode := tc.ingressStatus
	if ingressCode == 0 {
		ingressCode = http.StatusOK
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "ProsperaReport") {
			mu.Lock()
			receivedPath = r.URL.Path
			receivedKey = r.Header.Get("Idempotency-Key")
			mu.Unlock()
			w.WriteHeader(ingressCode)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ingressURL := srv.URL
	if tc.emptyIngress {
		ingressURL = ""
	}

	cfg := &config.Config{
		Port:                  "4019",
		RestateIngressURL:     ingressURL,
		DefaultDiscordChannel: "prospera",
		BrainURL:              srv.URL,
	}
	st := fakes.NewFakeStore()
	if strings.TrimSpace(tc.callID) != "" {
		_ = st.CreateOutboundCall(context.Background(), model.VoiceCall{
			CallID:       tc.callID,
			ReportStatus: "pending",
		}, 5)
	}
	var rep *delivery.Reporter
	if !tc.noReporter {
		rep = delivery.NewReporter(fakes.StaticConfig{Cfg: cfg}, st, &fakes.FakeTwilio{})
	}
	var serverOpts []ServerOption
	if tc.name != "successful restate ingress invocation" {
		serverOpts = append(serverOpts, WithServerHTTPClient(srv.Client()))
	}
	server := NewServer(newTestConfigSource(cfg), st, &fakes.FakeTwilio{}, nil, nil, rep, serverOpts...)

	err := server.triggerPostCallReport(context.Background(), tc.callID)
	if (err != nil) != tc.wantErr {
		t.Fatalf("triggerPostCallReport err = %v, wantErr = %v", err, tc.wantErr)
	}
	server.Wait()

	if tc.wantPath != "" {
		mu.Lock()
		path := receivedPath
		key := receivedKey
		mu.Unlock()
		if path != tc.wantPath {
			t.Errorf("path = %q, want %q", path, tc.wantPath)
		}
		if key != tc.wantKey {
			t.Errorf("key = %q, want %q", key, tc.wantKey)
		}
	}

	if tc.wantDelivered {
		call, _ := st.GetCall(context.Background(), tc.callID)
		if call.ReportStatus != "delivered" {
			t.Errorf("call report status = %q, want delivered", call.ReportStatus)
		}
	}
}

func TestServer_TriggerPostCallReport_RestateIngress(t *testing.T) {
	t.Parallel()

	tests := []triggerPostCallReportTestCase{
		{
			name:          "empty call id returns nil",
			callID:        "   ",
			ingressStatus: http.StatusOK,
			wantDelivered: false,
		},
		{
			name:          "successful restate ingress invocation",
			callID:        "call-restate-ok",
			ingressStatus: http.StatusOK,
			wantPath:      "/ProsperaReport/call-restate-ok/DeliverReport/send",
			wantKey:       "prospera:report:call-restate-ok",
		},
		{
			name:          "restate ingress failure falls back to direct delivery",
			callID:        "call-restate-fail",
			ingressStatus: http.StatusInternalServerError,
			wantDelivered: true,
		},
		{
			name:          "empty restate ingress url delivers directly",
			callID:        "call-no-ingress",
			emptyIngress:  true,
			wantDelivered: true,
		},
		{
			name:         "no delivery mechanism available returns error and logs in triggerPostCallReportIfPending",
			callID:       "call-no-mech",
			emptyIngress: true,
			noReporter:   true,
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTriggerPostCallReportTest(t, tc)
		})
	}
}

func TestServer_TriggerPostCallReportIfPending(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Port: "4019"}
	st := fakes.NewFakeStore()

	// 1. Success path: enqueues/delivers report without error
	server, store, _ := setupTestServer()
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:       "call-pending-rep",
		ReportStatus: "pending",
	}, 5)
	server.triggerPostCallReportIfPending(context.Background(), "call-pending-rep")
	server.Wait()

	// 2. Error path: missing reporter logs error without panic
	serverNoRep := NewServer(newTestConfigSource(cfg), st, nil, nil, nil, nil)
	serverNoRep.triggerPostCallReportIfPending(context.Background(), "call-no-rep")
	serverNoRep.Wait()
}

type handleReportJobTestCase struct {
	name          string
	callID        string
	ingressStatus int
	wantIngress   bool
	wantDelivered bool
}

func runHandleReportJobTest(t *testing.T, tc handleReportJobTestCase) {
	t.Helper()
	var calledIngress bool
	var mu sync.Mutex

	ingressCode := tc.ingressStatus
	if ingressCode == 0 {
		ingressCode = http.StatusOK
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "ProsperaReport") {
			mu.Lock()
			calledIngress = true
			mu.Unlock()
			w.WriteHeader(ingressCode)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &config.Config{
		Port:                  "4019",
		RestateIngressURL:     srv.URL,
		BrainURL:              srv.URL,
		DefaultDiscordChannel: "prospera",
	}
	st := fakes.NewFakeStore()
	_ = st.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:       tc.callID,
		ReportStatus: "pending",
	}, 5)

	rep := delivery.NewReporter(fakes.StaticConfig{Cfg: cfg}, st, &fakes.FakeTwilio{})
	server := NewServer(newTestConfigSource(cfg), st, &fakes.FakeTwilio{}, nil, nil, rep, WithServerHTTPClient(srv.Client()))

	msg := &pubsubtest.Message{
		Payload: []byte(`{"call_id":"` + tc.callID + `"}`),
	}
	err := server.HandleReportJob(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleReportJob() err = %v", err)
	}

	mu.Lock()
	gotIngress := calledIngress
	mu.Unlock()
	if gotIngress != tc.wantIngress {
		t.Errorf("calledIngress = %v, want %v", gotIngress, tc.wantIngress)
	}

	call, _ := st.GetCall(context.Background(), tc.callID)
	if (call.ReportStatus == "delivered") != tc.wantDelivered {
		t.Errorf("call report status = %q, wantDelivered = %v", call.ReportStatus, tc.wantDelivered)
	}
}

func TestServer_HandleReportJob_RestateIngress(t *testing.T) {
	t.Parallel()

	tests := []handleReportJobTestCase{
		{
			name:          "restate ingress succeeds without fallback",
			callID:        "call-job-restate-ok",
			ingressStatus: http.StatusOK,
			wantIngress:   true,
			wantDelivered: false,
		},
		{
			name:          "restate ingress failure falls back to direct reporter delivery",
			callID:        "call-job-restate-fallback",
			ingressStatus: http.StatusInternalServerError,
			wantIngress:   true,
			wantDelivered: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandleReportJobTest(t, tc)
		})
	}
}

func TestServer_HangupActiveSessions(t *testing.T) {
	t.Parallel()

	// 1. Nil server or nil session manager returns cleanly
	var nilS *Server
	nilS.HangupActiveSessions()
	nilS.Drain(context.Background())
	emptyS := &Server{}
	emptyS.HangupActiveSessions()
	emptyS.Drain(context.Background())

	// 2. Active sessions have hangup initiated
	server, _, _ := setupTestServer()
	server.Drain(nil)
	server.Drain(context.Background())

	sess := &stubSession{}
	server.sessionMgr.RegisterSession("call-hangup-1", sess)
	if server.sessionMgr.ActiveCount() != 1 {
		t.Fatalf("active count = %d, want 1", server.sessionMgr.ActiveCount())
	}
	server.HangupActiveSessions()
	if sess.hangupCount() != 1 {
		t.Fatalf("hangup count = %d, want 1", sess.hangupCount())
	}
}

func TestServer_ResolveAndBindCall_ErrorBranches(t *testing.T) {
	t.Parallel()

	// 1. Nil store returns nil
	serverNoStore := NewServer(newTestConfigSource(&config.Config{}), nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?call_id=c1", http.NoBody)
	if call := serverNoStore.resolveAndBindCall(context.Background(), req, "CA1"); call != nil {
		t.Fatalf("expected nil call with nil store, got %+v", call)
	}

	// 2. BindProviderCallSID error logs warning
	st := fakes.NewFakeStore()
	st.BindErr = errors.New("bind failure")
	_ = st.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID: "call-bind-err",
	}, 5)
	server := NewServer(newTestConfigSource(&config.Config{}), st, nil, nil, nil, nil)
	reqBind := httptest.NewRequest(http.MethodGet, "/?call_id=call-bind-err", http.NoBody)
	call := server.resolveAndBindCall(context.Background(), reqBind, "CA-bind-err")
	if call == nil || call.CallID != "call-bind-err" {
		t.Fatalf("expected call-bind-err, got %+v", call)
	}
}
