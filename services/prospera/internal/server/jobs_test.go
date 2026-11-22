package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	_ "modernc.org/sqlite"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/delivery"
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

type mockJobMsg struct {
	data      []byte
	delivered uint64
	termed    bool
	naked     bool
	nakDelay  time.Duration
	termErr   error
	nakErr    error
}

func (m *mockJobMsg) Data() []byte         { return m.data }
func (*mockJobMsg) Subject() string        { return "" }
func (*mockJobMsg) Header() nats.Header    { return nil }
func (m *mockJobMsg) NumDelivered() uint64 { return m.delivered }
func (*mockJobMsg) Ack() error             { return nil }
func (m *mockJobMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return m.nakErr
}

func (m *mockJobMsg) Term() error {
	m.termed = true
	return m.termErr
}
func (*mockJobMsg) Snooze(_ time.Duration) error { return nil }
func (*mockJobMsg) InProgress() error            { return nil }

type failConsumeQueueBus struct {
	pubsub.Bus

	failSubject string
}

func (b *failConsumeQueueBus) ConsumeQueue(ctx context.Context, subject string, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if b.failSubject == "" || b.failSubject == subject {
		return nil, errors.New("consume queue failed")
	}
	return b.Bus.ConsumeQueue(ctx, subject, queue, handler)
}

type captureConsumerBus struct {
	pubsub.Bus

	mu       sync.Mutex
	handlers map[string]pubsub.Handler
}

func (c *captureConsumerBus) ConsumeQueue(ctx context.Context, subject string, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers != nil {
		c.handlers[subject] = handler
	}
	return nil, nil
}

func setupMockReporterServer(t *testing.T) (*Server, *mockStoreForDelivery, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server, store, _ := setupTestServer()
	twilioMock := &mockTwilioForDelivery{
		audioData:   []byte("test-audio"),
		contentType: "audio/wav",
	}
	server.reporter = delivery.NewReporter(staticConfig{cfg: &config.Config{BrainURL: srv.URL, DefaultDiscordChannel: "prospera"}}, store, twilioMock)
	return server, store, srv
}

func assertJobMsgOutcome(t *testing.T, pubMsg pubsub.Message, wantTermed, wantNaked bool, wantNakDelay time.Duration) {
	t.Helper()
	msg, ok := pubMsg.(*mockJobMsg)
	if !ok || msg == nil {
		return
	}
	if msg.termed != wantTermed {
		t.Errorf("termed = %v, want %v", msg.termed, wantTermed)
	}
	if msg.naked != wantNaked {
		t.Errorf("naked = %v, want %v", msg.naked, wantNaked)
	}
	if wantNakDelay > 0 && msg.nakDelay != wantNakDelay {
		t.Errorf("nakDelay = %v, want %v", msg.nakDelay, wantNakDelay)
	}
}

func assertConsumerHandlers(t *testing.T, bus pubsub.Bus) {
	t.Helper()
	cBus, _ := bus.(*captureConsumerBus)
	for _, subj := range []string{
		pubsub.SubjectProsperaReportJob,
		pubsub.SubjectProsperaVoicemailJob,
		pubsub.SubjectProsperaSMSJob,
	} {
		cBus.mu.Lock()
		h := cBus.handlers[subj]
		cBus.mu.Unlock()
		if h == nil {
			t.Fatalf("missing handler for %s", subj)
		}
		if hErr := h(context.Background(), nil); hErr == nil {
			t.Errorf("expected error invoking handler with nil msg on %s", subj)
		}
	}
}

func TestServer_StartJobConsumers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		nilServer      bool
		busKind        string
		assertHandlers bool
		wantErr        bool
	}{
		{
			name:      "nil server returns nil bus and ignores set",
			nilServer: true,
			wantErr:   false,
		},
		{
			name:    "nil bus returns nil",
			busKind: "nil",
			wantErr: false,
		},
		{
			name:    "memory bus subscribes all consumers",
			busKind: "memory",
			wantErr: false,
		},
		{
			name:           "bus wires consumers and handlers invoke server methods",
			busKind:        "capture",
			assertHandlers: true,
			wantErr:        false,
		},
		{
			name:    "report consumer error fails",
			busKind: "fail_report",
			wantErr: true,
		},
		{
			name:    "voicemail consumer error fails",
			busKind: "fail_voicemail",
			wantErr: true,
		},
		{
			name:    "sms consumer error fails",
			busKind: "fail_sms",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var server *Server
			if !tc.nilServer {
				server, _, _ = setupTestServer()
			}
			var bus pubsub.Bus
			switch tc.busKind {
			case "", "nil":
				bus = nil
			case "memory":
				bus = pubsub.NewMemoryBus()
			case "capture":
				bus = &captureConsumerBus{handlers: make(map[string]pubsub.Handler)}
			case "fail_report":
				bus = &failConsumeQueueBus{Bus: pubsub.NewMemoryBus(), failSubject: pubsub.SubjectProsperaReportJob}
			case "fail_voicemail":
				bus = &failConsumeQueueBus{Bus: pubsub.NewMemoryBus(), failSubject: pubsub.SubjectProsperaVoicemailJob}
			case "fail_sms":
				bus = &failConsumeQueueBus{Bus: pubsub.NewMemoryBus(), failSubject: pubsub.SubjectProsperaSMSJob}
			default:
				t.Fatalf("unknown busKind %q", tc.busKind)
			}
			server.SetEventBus(bus)
			if tc.nilServer && server.EventBus() != nil {
				t.Fatalf("server.EventBus() = %v, want nil", server.EventBus())
			}

			err := server.StartJobConsumers(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("StartJobConsumers() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.assertHandlers {
				assertConsumerHandlers(t, bus)
			}
		})
	}
}

func TestServer_HandleReportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		msg          pubsub.Message
		nilReporter  bool
		reporterErr  bool
		wantErr      bool
		wantTermed   bool
		wantNaked    bool
		wantNakDelay time.Duration
		checkCallID  string
	}{
		{
			name:    "nil message returns error",
			wantErr: true,
		},
		{
			name:       "invalid json terminates message",
			msg:        &mockJobMsg{data: []byte("invalid json")},
			wantTermed: true,
		},
		{
			name:       "invalid json term failure still drops",
			msg:        &mockJobMsg{data: []byte("invalid json"), termErr: errors.New("term failed")},
			wantTermed: true,
		},
		{
			name:       "empty call id terminates message",
			msg:        &mockJobMsg{data: []byte(`{"call_id":"   "}`)},
			wantTermed: true,
		},
		{
			name:       "empty call id term failure still drops",
			msg:        &mockJobMsg{data: []byte(`{"call_id":"   "}`), termErr: errors.New("term failed")},
			wantTermed: true,
		},
		{
			name:        "nil reporter returns error",
			msg:         &mockJobMsg{data: []byte(`{"call_id":"c1"}`)},
			nilReporter: true,
			wantErr:     true,
		},
		{
			name:         "reporter error naks with backoff delay",
			msg:          &mockJobMsg{data: []byte(`{"call_id":"c1"}`), delivered: 1},
			reporterErr:  true,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:        "reporter error drops after max deliveries",
			msg:         &mockJobMsg{data: []byte(`{"call_id":"c1"}`), delivered: 5},
			reporterErr: true,
			wantErr:     false,
			wantTermed:  true,
		},
		{
			name:        "successful delivery transitions pending call report to delivered",
			msg:         &mockJobMsg{data: []byte(`{"call_id":"call-rep-ok"}`), delivered: 1},
			checkCallID: "call-rep-ok",
		},
		{
			name:        "retry delivery transitions previously failed call report to delivered",
			msg:         &mockJobMsg{data: []byte(`{"call_id":"call-rep-retry"}`), delivered: 2},
			checkCallID: "call-rep-retry",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, store, srv := setupMockReporterServer(t)
			defer srv.Close()

			_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
				CallID:       "call-rep-ok",
				ReportStatus: "pending",
			}, 5)
			_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
				CallID:       "call-rep-retry",
				ReportStatus: "failed",
			}, 5)

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = delivery.NewReporter(staticConfig{cfg: &config.Config{BrainURL: ""}}, store, nil)
			}

			err := server.HandleReportJob(context.Background(), tc.msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleReportJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			assertJobMsgOutcome(t, tc.msg, tc.wantTermed, tc.wantNaked, tc.wantNakDelay)
			if tc.checkCallID != "" {
				call, getErr := store.GetCall(context.Background(), tc.checkCallID)
				if getErr != nil || call.ReportStatus != "delivered" {
					t.Fatalf("call.ReportStatus = %q, want delivered (err=%v)", call.ReportStatus, getErr)
				}
			}
		})
	}
}

func TestServer_HandleVoicemailJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		callID       string
		recordingURL string
		delivered    uint64
		skipMsg      bool
		rawPayload   []byte
		nilReporter  bool
		reporterErr  bool
		termErr      error
		wantErr      bool
		wantTermed   bool
		wantNaked    bool
		wantNakDelay time.Duration
		checkCallID  string
	}{
		{
			name:    "nil message returns error",
			skipMsg: true,
			wantErr: true,
		},
		{
			name:       "corrupted payload terminates message",
			rawPayload: []byte("{not-json"),
			wantTermed: true,
		},
		{
			name:       "corrupted payload term failure still drops",
			rawPayload: []byte("{not-json"),
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
		{
			name:       "missing recording url terminates message",
			callID:     "c1",
			wantTermed: true,
		},
		{
			name:       "missing recording url term failure still drops",
			callID:     "c1",
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
		{
			name:         "nil reporter returns error",
			callID:       "c1",
			recordingURL: "http://rec.wav",
			nilReporter:  true,
			wantErr:      true,
		},
		{
			name:         "reporter error naks with backoff delay",
			callID:       "c1",
			recordingURL: "http://rec.wav",
			delivered:    2,
			reporterErr:  true,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 2 * time.Second,
		},
		{
			name:         "reporter error drops after max deliveries",
			callID:       "c1",
			recordingURL: "http://rec.wav",
			delivered:    5,
			reporterErr:  true,
			wantErr:      false,
			wantTermed:   true,
		},
		{
			name:         "successful voicemail processing transitions call to delivered",
			callID:       "call-vm-ok",
			recordingURL: "http://rec.wav",
			delivered:    1,
			checkCallID:  "call-vm-ok",
		},
		{
			name:         "retry delivery transitions previously failed voicemail to delivered",
			callID:       "call-vm-retry",
			recordingURL: "http://rec.wav",
			delivered:    2,
			checkCallID:  "call-vm-retry",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, store, srv := setupMockReporterServer(t)
			defer srv.Close()

			_ = store.CreateInboundCall(context.Background(), model.VoiceCall{
				CallID:       "call-vm-ok",
				ReportStatus: "awaiting_recording",
			})
			_ = store.CreateInboundCall(context.Background(), model.VoiceCall{
				CallID:       "call-vm-retry",
				ReportStatus: "failed",
			})

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = delivery.NewReporter(staticConfig{cfg: &config.Config{BrainURL: ""}}, store, nil)
			}

			var pubMsg pubsub.Message
			if !tc.skipMsg {
				payload := tc.rawPayload
				if payload == nil {
					payload, _ = json.Marshal(pubsub.ProsperaVoicemailJobArgs{
						CallID:       tc.callID,
						RecordingURL: tc.recordingURL,
					})
				}
				pubMsg = &mockJobMsg{data: payload, delivered: tc.delivered, termErr: tc.termErr}
			}

			err := server.HandleVoicemailJob(context.Background(), pubMsg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleVoicemailJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			assertJobMsgOutcome(t, pubMsg, tc.wantTermed, tc.wantNaked, tc.wantNakDelay)
			if tc.checkCallID != "" {
				call, getErr := store.GetCall(context.Background(), tc.checkCallID)
				if getErr != nil || call.ReportStatus != "delivered" {
					t.Fatalf("call.ReportStatus = %q, want delivered (err=%v)", call.ReportStatus, getErr)
				}
			}
		})
	}
}

func TestServer_HandleSMSJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		msg          pubsub.Message
		nilReporter  bool
		reporterErr  bool
		wantErr      bool
		wantTermed   bool
		wantNaked    bool
		wantNakDelay time.Duration
	}{
		{
			name:    "nil message returns error",
			wantErr: true,
		},
		{
			name:       "invalid json terminates message",
			msg:        &mockJobMsg{data: []byte("invalid json")},
			wantTermed: true,
		},
		{
			name:       "invalid json term failure still drops",
			msg:        &mockJobMsg{data: []byte("invalid json"), termErr: errors.New("term failed")},
			wantTermed: true,
		},
		{
			name:        "nil reporter returns error",
			msg:         &mockJobMsg{data: []byte(`{"message_sid":"SM1","body":"hello"}`)},
			nilReporter: true,
			wantErr:     true,
		},
		{
			name:         "reporter error naks with backoff delay",
			msg:          &mockJobMsg{data: []byte(`{"message_sid":"SM1","body":"hello"}`), delivered: 4},
			reporterErr:  true,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 8 * time.Second,
		},
		{
			name:        "reporter error drops after max deliveries",
			msg:         &mockJobMsg{data: []byte(`{"message_sid":"SM1","body":"hello"}`), delivered: 5},
			reporterErr: true,
			wantErr:     false,
			wantTermed:  true,
		},
		{
			name: "successful sms delivery returns nil",
			msg:  &mockJobMsg{data: []byte(`{"message_sid":"SM-ok","body":"hi"}`), delivered: 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, store, srv := setupMockReporterServer(t)
			defer srv.Close()

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = delivery.NewReporter(staticConfig{cfg: &config.Config{BrainURL: ""}}, store, nil)
			}

			err := server.HandleSMSJob(context.Background(), tc.msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleSMSJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			assertJobMsgOutcome(t, tc.msg, tc.wantTermed, tc.wantNaked, tc.wantNakDelay)
		})
	}
}

type failPublishBus struct {
	pubsub.Bus
}

func (*failPublishBus) Publish(ctx context.Context, subject string, msgID string, data any) error {
	return errors.New("simulated publish failure")
}

type recordPublishBus struct {
	pubsub.Bus

	mu        sync.Mutex
	published map[string][]any
}

func newRecordPublishBus() *recordPublishBus {
	return &recordPublishBus{
		Bus:       pubsub.NewMemoryBus(),
		published: make(map[string][]any),
	}
}

func (r *recordPublishBus) Publish(ctx context.Context, subject string, msgID string, data any) error {
	r.mu.Lock()
	r.published[subject] = append(r.published[subject], data)
	r.mu.Unlock()
	return r.Bus.Publish(ctx, subject, msgID, data)
}

func (r *recordPublishBus) publishedJobs(subject string) []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]any(nil), r.published[subject]...)
}

func triggerTwilioWebhooks(t *testing.T, server *Server, store *mockStoreForDelivery, cfg *config.Config, suffix string) (string, string, string) {
	t.Helper()
	mux := server.Routes()

	// 1. Status webhook trigger post call report
	callID := "call-bus-report-" + suffix
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:          callID,
		ProviderCallSID: "CA-" + callID,
		Direction:       "outbound",
		State:           "in-progress",
		ReportStatus:    "pending",
	}, 5)

	statusForm := url.Values{
		"CallSid":    []string{"CA-" + callID},
		"CallStatus": []string{"completed"},
	}
	statusURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/status"
	sig := generateTwilioSignature(cfg.TwilioAuthToken, statusURL, statusForm)
	reqStat := httptest.NewRequest(http.MethodPost, statusURL, strings.NewReader(statusForm.Encode()))
	reqStat.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqStat.Header.Set("X-Twilio-Signature", sig)
	recStat := httptest.NewRecorder()
	mux.ServeHTTP(recStat, reqStat)
	if recStat.Code != http.StatusOK {
		t.Fatalf("status webhook returned %d", recStat.Code)
	}

	// 2. Voicemail webhook
	vmCallID := "call-bus-vm-" + suffix
	_ = store.CreateInboundCall(context.Background(), model.VoiceCall{
		CallID:          vmCallID,
		ProviderCallSID: "CA-" + vmCallID,
		ReportStatus:    "awaiting_recording",
	})
	vmForm := url.Values{
		"CallSid":      []string{"CA-" + vmCallID},
		"RecordingSid": []string{"RE-" + vmCallID},
		"RecordingUrl": []string{"https://api.twilio.com/rec.wav"},
	}
	vmURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/voicemail"
	sigVM := generateTwilioSignature(cfg.TwilioAuthToken, vmURL, vmForm)
	reqVM := httptest.NewRequest(http.MethodPost, vmURL, strings.NewReader(vmForm.Encode()))
	reqVM.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqVM.Header.Set("X-Twilio-Signature", sigVM)
	recVM := httptest.NewRecorder()
	mux.ServeHTTP(recVM, reqVM)
	if recVM.Code != http.StatusOK {
		t.Fatalf("voicemail webhook returned %d", recVM.Code)
	}

	// 3. SMS webhook
	smsID := "SM-" + suffix
	smsForm := url.Values{
		"MessageSid": []string{smsID},
		"From":       []string{"+15105550888"},
		"To":         []string{cfg.TwilioFromNumber},
		"Body":       []string{"sms bus test"},
	}
	smsURL := cfg.PublicWebhookBaseURL + "/api/webhooks/twilio/sms"
	sigSMS := generateTwilioSignature(cfg.TwilioAuthToken, smsURL, smsForm)
	reqSMS := httptest.NewRequest(http.MethodPost, smsURL, strings.NewReader(smsForm.Encode()))
	reqSMS.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqSMS.Header.Set("X-Twilio-Signature", sigSMS)
	recSMS := httptest.NewRecorder()
	mux.ServeHTTP(recSMS, reqSMS)
	if recSMS.Code != http.StatusOK {
		t.Fatalf("sms webhook returned %d", recSMS.Code)
	}

	server.Wait()
	return callID, vmCallID, smsID
}

func TestServer_Webhooks_PublishToEventBus(t *testing.T) {
	t.Parallel()
	server, store, cfg := setupTestServer()
	recordBus := newRecordPublishBus()
	server.SetEventBus(recordBus)
	_ = pubsub.EnsureCoreStreams(context.Background(), recordBus)

	callID, vmCallID, smsID := triggerTwilioWebhooks(t, server, store, cfg, "pub")

	reportMsgs := recordBus.publishedJobs(pubsub.SubjectProsperaReportJob)
	if len(reportMsgs) != 1 {
		t.Fatalf("got %d report jobs published, want 1", len(reportMsgs))
	}
	if rArgs, ok := reportMsgs[0].(pubsub.ProsperaReportJobArgs); !ok || rArgs.CallID != callID {
		t.Fatalf("published report job = %+v, want callID %s", reportMsgs[0], callID)
	}

	vmMsgs := recordBus.publishedJobs(pubsub.SubjectProsperaVoicemailJob)
	if len(vmMsgs) != 1 {
		t.Fatalf("got %d voicemail jobs published, want 1", len(vmMsgs))
	}
	if vmArgs, ok := vmMsgs[0].(pubsub.ProsperaVoicemailJobArgs); !ok || vmArgs.CallID != vmCallID || vmArgs.RecordingURL != "https://api.twilio.com/rec.wav" {
		t.Fatalf("published voicemail job = %+v, want callID %s", vmMsgs[0], vmCallID)
	}

	smsMsgs := recordBus.publishedJobs(pubsub.SubjectProsperaSMSJob)
	if len(smsMsgs) != 1 {
		t.Fatalf("got %d sms jobs published, want 1", len(smsMsgs))
	}
	if sArgs, ok := smsMsgs[0].(pubsub.ProsperaSMSJobArgs); !ok || sArgs.MessageSID != smsID || sArgs.Body != "sms bus test" {
		t.Fatalf("published sms job = %+v, want MessageSID %s", smsMsgs[0], smsID)
	}
}

func TestServer_Webhooks_FallbackToBackgroundGoroutine(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	server, store, cfg := setupTestServer()
	twilioMock := &mockTwilioForDelivery{
		audioData:   []byte("test-audio"),
		contentType: "audio/wav",
	}
	server.reporter = delivery.NewReporter(staticConfig{cfg: &config.Config{
		BrainURL:              srv.URL,
		DefaultDiscordChannel: "prospera",
		TwilioAuthToken:       cfg.TwilioAuthToken,
		TwilioFromNumber:      cfg.TwilioFromNumber,
		PublicWebhookBaseURL:  cfg.PublicWebhookBaseURL,
	}}, store, twilioMock)
	server.SetEventBus(&failPublishBus{Bus: pubsub.NewMemoryBus()})

	callID, vmCallID, smsID := triggerTwilioWebhooks(t, server, store, cfg, "fallback")

	repCall, repErr := store.GetCall(context.Background(), callID)
	if repErr != nil || repCall.ReportStatus != "delivered" {
		t.Fatalf("fallback report call status = %q, want delivered (err=%v)", repCall.ReportStatus, repErr)
	}

	vmCall, vmErr := store.GetCall(context.Background(), vmCallID)
	if vmErr != nil || vmCall.ReportStatus != "delivered" {
		t.Fatalf("fallback vm call status = %q, want delivered (err=%v)", vmCall.ReportStatus, vmErr)
	}

	msgs, lErr := store.ListVoiceMessages(context.Background(), 10)
	if lErr != nil || len(msgs) == 0 {
		t.Fatalf("fallback voice messages empty or error: %v", lErr)
	}
	var found *model.VoiceMessage
	for i := range msgs {
		if msgs[i].MessageSID == smsID {
			found = &msgs[i]
			break
		}
	}
	if found == nil || !found.DiscordDelivered || found.Body != "sms bus test" {
		t.Fatalf("fallback voice message not delivered: %+v", found)
	}
}

func TestNakOrDropJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		nilMsg       bool
		delivered    uint64
		termErr      error
		nakErr       error
		wantErr      bool
		wantTermed   bool
		wantNaked    bool
		wantNakDelay time.Duration
	}{
		{
			name:    "nil msg returns original error",
			nilMsg:  true,
			wantErr: true,
		},
		{
			name:         "nak retry returns original error",
			delivered:    1,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:         "nak failure still returns original error",
			delivered:    1,
			nakErr:       errors.New("nak failed"),
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:       "max deliveries terms and drops without error",
			delivered:  5,
			wantTermed: true,
		},
		{
			name:       "max deliveries term failure still drops without error",
			delivered:  5,
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var msg *mockJobMsg
			var pubMsg pubsub.Message
			if !tc.nilMsg {
				msg = &mockJobMsg{
					delivered: tc.delivered,
					termErr:   tc.termErr,
					nakErr:    tc.nakErr,
				}
				pubMsg = msg
			}
			err := nakOrDropJob(context.Background(), pubMsg, "test", "id1", errors.New("underlying error"))
			if (err != nil) != tc.wantErr {
				t.Fatalf("nakOrDropJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if msg != nil {
				if msg.termed != tc.wantTermed {
					t.Errorf("termed = %v, want %v", msg.termed, tc.wantTermed)
				}
				if msg.naked != tc.wantNaked {
					t.Errorf("naked = %v, want %v", msg.naked, tc.wantNaked)
				}
				if tc.wantNakDelay > 0 && msg.nakDelay != tc.wantNakDelay {
					t.Errorf("nakDelay = %v, want %v", msg.nakDelay, tc.wantNakDelay)
				}
			}
		})
	}
}

// ---- test helpers (moved from root config/delivery tests) ----

// staticConfig is a fixed ConfigSource for tests.
type staticConfig struct {
	cfg *config.Config
}

func (s staticConfig) Current() *config.Config { return s.cfg }

// newTestConfigSource wraps cfg in a real Reloadable whose loader always
// yields cfg, so reload requests report "unchanged".
func newTestConfigSource(cfg *config.Config) *httpkit.Reloadable[config.Config] {
	return httpkit.NewReloadable(cfg, func() (*config.Config, error) { return cfg, nil }, config.RestartRequired)
}

type mockStoreForDelivery struct {
	mu             sync.Mutex
	calls          map[string]*model.VoiceCall
	messages       map[string]*model.VoiceMessage
	advances       int
	statusTrans    []string
	vmRecordings   map[string]string
	opErr          error
	bindErr        error
	updateStateErr error
	transErr       error
	advanceErr     error
	vmRecErr       error
	markErr        error
	saveErr        error
	failTrans      map[string]bool
}

func newMockStoreForDelivery() *mockStoreForDelivery {
	return &mockStoreForDelivery{
		calls:        make(map[string]*model.VoiceCall),
		messages:     make(map[string]*model.VoiceMessage),
		vmRecordings: make(map[string]string),
	}
}

func (m *mockStoreForDelivery) CreateOutboundCall(ctx context.Context, call model.VoiceCall, maxConcurrent int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return m.opErr
	}
	m.calls[call.CallID] = &call
	return nil
}

func (m *mockStoreForDelivery) CreateInboundCall(ctx context.Context, call model.VoiceCall) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[call.CallID] = &call
	return nil
}

func (m *mockStoreForDelivery) BindProviderCallSID(ctx context.Context, callID, providerSID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.bindErr != nil {
		return m.bindErr
	}
	if c, ok := m.calls[callID]; ok {
		c.ProviderCallSID = providerSID
	}
	return nil
}

func (m *mockStoreForDelivery) GetCall(ctx context.Context, callID string) (*model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return nil, m.opErr
	}
	c, ok := m.calls[callID]
	if !ok {
		return nil, nil
	}
	cpy := *c
	return &cpy, nil
}

func (m *mockStoreForDelivery) GetCallByProviderSID(ctx context.Context, providerSID string) (*model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return nil, m.opErr
	}
	for _, c := range m.calls {
		if c.ProviderCallSID == providerSID {
			cpy := *c
			return &cpy, nil
		}
	}
	return nil, nil
}

func (m *mockStoreForDelivery) ListCalls(ctx context.Context, limit int) ([]model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return nil, m.opErr
	}
	var list []model.VoiceCall
	for _, c := range m.calls {
		list = append(list, *c)
	}
	return list, nil
}

func (m *mockStoreForDelivery) FindRecentOutboundCall(ctx context.Context, fromE164 string, window time.Duration) (*model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.Direction == "outbound" && c.ToNumber == fromE164 {
			cpy := *c
			return &cpy, nil
		}
	}
	return nil, nil
}

func (m *mockStoreForDelivery) UpdateCallState(ctx context.Context, callID, state, endReason, answeredBy string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.updateStateErr != nil {
		return m.updateStateErr
	}
	if c, ok := m.calls[callID]; ok {
		c.State = state
		if endReason != "" {
			c.EndReason = endReason
		}
		if answeredBy != "" {
			c.AnsweredBy = answeredBy
		}
	}
	return nil
}

func (m *mockStoreForDelivery) AppendTranscriptTurn(ctx context.Context, callID string, turn model.TranscriptTurn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.calls[callID]; ok {
		c.Transcript = append(c.Transcript, turn)
	}
	return nil
}

func (m *mockStoreForDelivery) AdvanceLiveTranscriptCursor(ctx context.Context, callID string, fromCursor, toCursor int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.advanceErr != nil {
		return false, m.advanceErr
	}
	if c, ok := m.calls[callID]; ok {
		if c.LiveTranscriptCursor == fromCursor {
			c.LiveTranscriptCursor = toCursor
			m.advances++
			return true, nil
		}
	}
	return false, nil
}

func (m *mockStoreForDelivery) AppendOwnerInstruction(ctx context.Context, callID string, instr model.OwnerInstruction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.calls[callID]; ok {
		c.OwnerInstructions = append(c.OwnerInstructions, instr)
	}
	return nil
}

func (m *mockStoreForDelivery) SetVoicemailRecording(ctx context.Context, callID, recordingURL string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.vmRecErr != nil {
		return m.vmRecErr
	}
	m.vmRecordings[callID] = recordingURL
	if c, ok := m.calls[callID]; ok {
		c.VoicemailRecordingURL = recordingURL
	}
	return nil
}

func (m *mockStoreForDelivery) TransitionReportStatus(ctx context.Context, callID string, allowedFrom []string, toStatus, summary string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.transErr != nil {
		return false, m.transErr
	}
	if m.failTrans[toStatus] {
		return false, errors.New("transition to " + toStatus + " failed")
	}
	c, ok := m.calls[callID]
	if !ok {
		return false, nil
	}
	if len(allowedFrom) > 0 {
		matched := slices.Contains(allowedFrom, c.ReportStatus)
		if !matched {
			return false, nil
		}
	}
	c.ReportStatus = toStatus
	if summary != "" {
		c.ReportSummary = summary
	}
	m.statusTrans = append(m.statusTrans, toStatus)
	return true, nil
}

func (*mockStoreForDelivery) ReconcileOrphanedCalls(ctx context.Context, checkActive func(ctx context.Context, providerSID string) (bool, error)) (int, error) {
	return 0, nil
}

func (m *mockStoreForDelivery) SaveVoiceMessage(ctx context.Context, msg model.VoiceMessage) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return false, m.saveErr
	}
	m.messages[msg.MessageSID] = &msg
	return true, nil
}

func (m *mockStoreForDelivery) MarkVoiceMessageDelivered(ctx context.Context, messageSID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markErr != nil {
		return m.markErr
	}
	if msg, ok := m.messages[messageSID]; ok {
		msg.DiscordDelivered = true
	}
	return nil
}

func (m *mockStoreForDelivery) ListVoiceMessages(ctx context.Context, limit int) ([]model.VoiceMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return nil, m.opErr
	}
	var list []model.VoiceMessage
	for _, msg := range m.messages {
		list = append(list, *msg)
	}
	return list, nil
}

func (*mockStoreForDelivery) Close() error {
	return nil
}

type mockTwilioForDelivery struct {
	audioData          []byte
	contentType        string
	downloadErr        error
	createCallErr      error
	sendSMSErr         error
	ensurePhoneErr     error
	hangupErr          error
	lastWebhookBaseURL string
	lastCreateCallReq  twilio.CreateCallRequest
	lastSendSMSFrom    string
	lastSendSMSTo      string
}

func (m *mockTwilioForDelivery) CreateCall(ctx context.Context, req twilio.CreateCallRequest) (string, error) {
	m.lastCreateCallReq = req
	if m.createCallErr != nil {
		return "", m.createCallErr
	}
	return "CA12345", nil
}

func (m *mockTwilioForDelivery) HangupCall(ctx context.Context, providerCallSID string) error {
	if m.hangupErr != nil {
		return m.hangupErr
	}
	return nil
}

func (*mockTwilioForDelivery) IsCallActive(ctx context.Context, providerCallSID string) (bool, error) {
	return true, nil
}

func (m *mockTwilioForDelivery) SendSMS(ctx context.Context, to, from, body string) (string, error) {
	m.lastSendSMSTo = to
	m.lastSendSMSFrom = from
	if m.sendSMSErr != nil {
		return "", m.sendSMSErr
	}
	return "SM12345", nil
}

func (m *mockTwilioForDelivery) EnsurePhoneNumber(ctx context.Context, areaCode, webhookBaseURL string) (string, error) {
	m.lastWebhookBaseURL = webhookBaseURL
	if m.ensurePhoneErr != nil {
		return "", m.ensurePhoneErr
	}
	return "+15105550100", nil
}

func (m *mockTwilioForDelivery) DownloadRecording(ctx context.Context, recordingURL string) ([]byte, string, error) {
	return m.audioData, m.contentType, m.downloadErr
}

// ---- app lifecycle tests (moved from root main_test.go) ----

func TestRunApp_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "empty port",
			cfg:     &config.Config{Port: "   ", DatabaseURL: "sqlite://:memory:"},
			wantErr: true,
		},
		{
			name:    "invalid database url",
			cfg:     &config.Config{Port: "4019", DatabaseURL: "invalid://url"},
			wantErr: true,
		},
		{
			name:    "server listen error",
			cfg:     &config.Config{Port: "-1", DatabaseURL: "sqlite://:memory:"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfgSrc *httpkit.Reloadable[config.Config]
			if tc.cfg != nil {
				cfgSrc = newTestConfigSource(tc.cfg)
			}
			err := RunApp(context.Background(), cfgSrc)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RunApp() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "prospera_app_test.db")

	origBusFn := newProsperaBusFn
	newProsperaBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
		return pubsub.NewMemoryBus(), nil
	}
	defer func() { newProsperaBusFn = origBusFn }()

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	schema := `
	CREATE TABLE voice_calls (
		call_id TEXT PRIMARY KEY,
		provider_call_sid TEXT UNIQUE,
		direction TEXT NOT NULL,
		state TEXT NOT NULL,
		end_reason TEXT,
		from_number TEXT NOT NULL,
		to_number TEXT NOT NULL,
		callback_of_call_id TEXT,
		discord_channel_id TEXT,
		discord_thread_id TEXT,
		brief TEXT NOT NULL DEFAULT '{}',
		owner_instructions TEXT NOT NULL DEFAULT '[]',
		transcript TEXT NOT NULL DEFAULT '[]',
		live_transcript_cursor INTEGER NOT NULL DEFAULT 0,
		answered_by TEXT,
		voicemail_recording_url TEXT,
		report_status TEXT NOT NULL DEFAULT 'pending',
		report_summary TEXT,
		started_at TIMESTAMP NOT NULL,
		answered_at TIMESTAMP,
		ended_at TIMESTAMP,
		updated_at TIMESTAMP NOT NULL
	);
	CREATE TABLE voice_messages (
		message_sid TEXT PRIMARY KEY,
		direction TEXT NOT NULL,
		from_number TEXT NOT NULL,
		to_number TEXT NOT NULL,
		body TEXT NOT NULL,
		media_urls TEXT NOT NULL DEFAULT '[]',
		related_call_id TEXT,
		discord_delivered BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMP NOT NULL
	);
	`
	if _, err = db.Exec(schema); err != nil {
		db.Close()
		t.Fatalf("failed to create test schema: %v", err)
	}
	_ = db.Close()

	natsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	closedNATSAddr := natsLn.Addr().String()
	_ = natsLn.Close()

	cfg := &config.Config{
		Port:                  "0",
		DatabaseURL:           "sqlite://" + dbPath,
		DefaultDiscordChannel: "prospera",
		NATSURL:               "nats://" + closedNATSAddr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	appDone := make(chan error, 1)

	go func() {
		appDone <- RunApp(ctx, newTestConfigSource(cfg))
	}()

	cancel()

	select {
	case err := <-appDone:
		if err != nil {
			t.Fatalf("RunApp() shutdown error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunApp() did not shut down within timeout")
	}
}

func TestRunApp_TranscriptBatcher(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{DefaultDiscordChannel: "prospera"}
	store := newMockStoreForDelivery()
	sessionMgr := mediastream.NewCallSessionManager(staticConfig{cfg: cfg}, store, &mockTwilioForDelivery{}, nil, nil)
	reporter := delivery.NewReporter(staticConfig{cfg: cfg}, store, &mockTwilioForDelivery{})

	// Test flush with nil sessionMgr or reporter
	flushActiveSessions(context.Background(), nil, reporter)
	flushActiveSessions(context.Background(), sessionMgr, nil)

	// Test flush with no sessions
	flushActiveSessions(context.Background(), sessionMgr, reporter)

	// Test flush with active session
	sess := mediastream.NewMediaStreamSession(mediastream.MediaStreamSessionConfig{CallID: "call-batch-test"})
	sessionMgr.RegisterSession("call-batch-test", sess)
	_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID: "call-batch-test",
		Transcript: []model.TranscriptTurn{
			{Role: "user", Text: "Batch turn"},
		},
	}, 5)

	flushActiveSessions(context.Background(), sessionMgr, reporter)

	// Test runTranscriptBatcher exiting immediately on canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runTranscriptBatcher(ctx, sessionMgr, reporter)
}

func TestRoutes_HealthAndCoverage(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Port: "4019"}
	store := newMockStoreForDelivery()
	server := NewServer(newTestConfigSource(cfg), store, &mockTwilioForDelivery{}, nil, nil, nil)
	handler := server.Routes()
	// Test binaries carry no runtime/coverage meta-data (only `go build -cover`
	// binaries do), so the wired flush handler answers 500 here, never 404.

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{name: "health endpoint", method: http.MethodGet, path: "/health", wantStatus: http.StatusOK, wantBody: `"status":"ok"`},
		{name: "coverage flush get", method: http.MethodGet, path: "/debug/coverage/flush", wantStatus: http.StatusInternalServerError},
		{name: "coverage flush post", method: http.MethodPost, path: "/debug/coverage/flush", wantStatus: http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("%s %s status = %d, want %d", tc.method, tc.path, rec.Code, tc.wantStatus)
			}
			if tc.wantBody != "" && !bytes.Contains(rec.Body.Bytes(), []byte(tc.wantBody)) {
				t.Fatalf("body %s missing %s", rec.Body.String(), tc.wantBody)
			}
		})
	}
}

type shutdownAppTestCase struct {
	name      string
	blockHTTP bool
	stuckTask bool
	wantErr   bool
}

func TestShutdownApp_DrainWindowIndependentOfShutdown(t *testing.T) {
	origShutdown, origDrain := shutdownTimeout, drainTimeout
	origTeardown, origWait := teardownTimeout, waitTimeout
	shutdownTimeout = 100 * time.Millisecond
	drainTimeout = 200 * time.Millisecond
	teardownTimeout = 200 * time.Millisecond
	waitTimeout = 200 * time.Millisecond
	defer func() {
		shutdownTimeout, drainTimeout = origShutdown, origDrain
		teardownTimeout, waitTimeout = origTeardown, origWait
	}()

	tests := []shutdownAppTestCase{
		{name: "shutdown error still drains full window", blockHTTP: true, wantErr: true},
		{name: "clean shutdown drains full window", blockHTTP: false, wantErr: false},
		{name: "stuck session and task still return within bound", stuckTask: true, wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runShutdownAppCase(t, tc)
		})
	}
}

func startShutdownTestServer(t *testing.T, blockHTTP bool) (*http.Server, string, <-chan struct{}, func()) {
	t.Helper()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	httpSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blockHTTP {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
	})}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		_ = httpSrv.Serve(ln)
	}()
	t.Cleanup(func() {
		closeRelease()
		_ = httpSrv.Close()
	})
	return httpSrv, "http://" + ln.Addr().String(), entered, closeRelease
}

func runShutdownAppCase(t *testing.T, tc shutdownAppTestCase) {
	t.Helper()
	cfg := &config.Config{Port: "4019"}
	store := newMockStoreForDelivery()
	mgr := mediastream.NewCallSessionManager(staticConfig{cfg: cfg}, store, nil, nil, nil)
	sess := &stubSession{}
	mgr.RegisterSession("call-drain-shutdown", sess)
	server := NewServer(newTestConfigSource(cfg), store, nil, nil, mgr, nil)
	if tc.stuckTask {
		server.bgWg.Add(1)
		t.Cleanup(func() { server.bgWg.Done() })
	}

	httpSrv, addr, entered, closeRelease := startShutdownTestServer(t, tc.blockHTTP)

	clientDone := make(chan struct{})
	if tc.blockHTTP {
		go func() {
			defer close(clientDone)
			resp, postErr := http.Post(addr, "text/plain", strings.NewReader("x"))
			if postErr == nil && resp != nil && resp.Body != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("handler never went in-flight")
		}
	} else {
		close(clientDone)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := shutdownApp(ctx, httpSrv, server)
	elapsed := time.Since(start)
	if (err != nil) != tc.wantErr {
		t.Fatalf("shutdownApp() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("shutdownApp returned in %v, want at least 150ms of drain window", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("shutdownApp returned in %v, want return within the documented bound", elapsed)
	}
	if got := mgr.ActiveCount(); got != 1 {
		t.Fatalf("ActiveCount = %d, want 1 (session still live after drain deadline)", got)
	}
	if got := sess.hangupCount(); got != 1 {
		t.Fatalf("hangupCount = %d, want 1 (leftover session deliberately hung up)", got)
	}

	closeRelease()
	select {
	case <-clientDone:
	case <-time.After(5 * time.Second):
		t.Fatal("client request goroutine did not finish")
	}
}
