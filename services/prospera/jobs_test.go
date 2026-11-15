package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/nats-io/nats.go"
)

type mockJobMsg struct {
	data      []byte
	delivered uint64
	termed    bool
	naked     bool
	nakDelay  time.Duration
}

func (m *mockJobMsg) Data() []byte         { return m.data }
func (m *mockJobMsg) Subject() string      { return "" }
func (m *mockJobMsg) Header() nats.Header  { return nil }
func (m *mockJobMsg) NumDelivered() uint64 { return m.delivered }
func (m *mockJobMsg) Ack() error           { return nil }
func (m *mockJobMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}

func (m *mockJobMsg) Term() error {
	m.termed = true
	return nil
}
func (m *mockJobMsg) Snooze(_ time.Duration) error { return nil }
func (m *mockJobMsg) InProgress() error            { return nil }

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
	server.reporter = NewReporter(staticConfig{cfg: &Config{BrainURL: srv.URL, DefaultDiscordChannel: "prospera"}}, store, twilioMock, WithReporterHTTPClient(srv.Client()))
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

func TestServer_StartJobConsumers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupBus       func() (pubsub.Bus, error)
		failSubject    string
		assertHandlers bool
		wantErr        bool
	}{
		{
			name: "nil bus returns nil",
			setupBus: func() (pubsub.Bus, error) {
				return nil, nil
			},
			wantErr: false,
		},
		{
			name: "memory bus subscribes all consumers",
			setupBus: func() (pubsub.Bus, error) {
				return pubsub.NewMemoryBus(), nil
			},
			wantErr: false,
		},
		{
			name: "bus wires consumers and handlers invoke server methods",
			setupBus: func() (pubsub.Bus, error) {
				return &captureConsumerBus{handlers: make(map[string]pubsub.Handler)}, nil
			},
			assertHandlers: true,
			wantErr:        false,
		},
		{
			name: "report consumer error fails",
			setupBus: func() (pubsub.Bus, error) {
				return &failConsumeQueueBus{Bus: pubsub.NewMemoryBus(), failSubject: pubsub.SubjectProsperaReportJob}, nil
			},
			wantErr: true,
		},
		{
			name: "voicemail consumer error fails",
			setupBus: func() (pubsub.Bus, error) {
				return &failConsumeQueueBus{Bus: pubsub.NewMemoryBus(), failSubject: pubsub.SubjectProsperaVoicemailJob}, nil
			},
			wantErr: true,
		},
		{
			name: "sms consumer error fails",
			setupBus: func() (pubsub.Bus, error) {
				return &failConsumeQueueBus{Bus: pubsub.NewMemoryBus(), failSubject: pubsub.SubjectProsperaSMSJob}, nil
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, _, _ := setupTestServer()
			bus, _ := tc.setupBus()
			server.SetEventBus(bus)

			err := server.StartJobConsumers(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("StartJobConsumers() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.assertHandlers {
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
			name:       "empty call id terminates message",
			msg:        &mockJobMsg{data: []byte(`{"call_id":"   "}`)},
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

			_ = store.CreateOutboundCall(context.Background(), VoiceCall{
				CallID:       "call-rep-ok",
				ReportStatus: "pending",
			}, 5)
			_ = store.CreateOutboundCall(context.Background(), VoiceCall{
				CallID:       "call-rep-retry",
				ReportStatus: "failed",
			}, 5)

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = NewReporter(staticConfig{cfg: &Config{BrainURL: ""}}, store, nil)
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
			name:       "missing recording url terminates message",
			callID:     "c1",
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

			_ = store.CreateInboundCall(context.Background(), VoiceCall{
				CallID:       "call-vm-ok",
				ReportStatus: "awaiting_recording",
			})
			_ = store.CreateInboundCall(context.Background(), VoiceCall{
				CallID:       "call-vm-retry",
				ReportStatus: "failed",
			})

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = NewReporter(staticConfig{cfg: &Config{BrainURL: ""}}, store, nil)
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
				pubMsg = &mockJobMsg{data: payload, delivered: tc.delivered}
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
				server.reporter = NewReporter(staticConfig{cfg: &Config{BrainURL: ""}}, store, nil)
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

func (f *failPublishBus) Publish(ctx context.Context, subject string, msgID string, data any) error {
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

func triggerTwilioWebhooks(t *testing.T, server *Server, store *mockStoreForDelivery, cfg *Config, suffix string) (string, string, string) {
	t.Helper()
	mux := server.Routes()

	// 1. Status webhook trigger post call report
	callID := "call-bus-report-" + suffix
	_ = store.CreateOutboundCall(context.Background(), VoiceCall{
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
	_ = store.CreateInboundCall(context.Background(), VoiceCall{
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
	server.reporter = NewReporter(staticConfig{cfg: &Config{
		BrainURL:              srv.URL,
		DefaultDiscordChannel: "prospera",
		TwilioAuthToken:       cfg.TwilioAuthToken,
		TwilioFromNumber:      cfg.TwilioFromNumber,
		PublicWebhookBaseURL:  cfg.PublicWebhookBaseURL,
	}}, store, twilioMock, WithReporterHTTPClient(srv.Client()))
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
	var found *VoiceMessage
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

func TestFirstNonEmptyAndNakBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input []string
		want  string
	}{
		{input: []string{"", "  ", "first", "second"}, want: "first"},
		{input: []string{"", "   "}, want: ""},
		{input: []string{"only"}, want: "only"},
	}

	for _, tc := range tests {
		if got := firstNonEmpty(tc.input...); got != tc.want {
			t.Errorf("firstNonEmpty(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}

	delayTests := []struct {
		delivered uint64
		want      time.Duration
	}{
		{delivered: 0, want: 1 * time.Second},
		{delivered: 1, want: 1 * time.Second},
		{delivered: 2, want: 2 * time.Second},
		{delivered: 3, want: 4 * time.Second},
		{delivered: 4, want: 8 * time.Second},
		{delivered: 5, want: 16 * time.Second},
		{delivered: 10, want: 16 * time.Second},
	}

	for _, dt := range delayTests {
		if got := nakBackoffDelay(dt.delivered); got != dt.want {
			t.Errorf("nakBackoffDelay(%d) = %v, want %v", dt.delivered, got, dt.want)
		}
	}
}

func TestNakOrDropJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		nilMsg       bool
		delivered    uint64
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
			name:         "delivered 1 naks with 1s",
			delivered:    1,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:         "delivered 4 naks with 8s",
			delivered:    4,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 8 * time.Second,
		},
		{
			name:       "delivered 5 terms and drops",
			delivered:  5,
			wantErr:    false,
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
				}
				pubMsg = msg
			}
			err := nakOrDropJob(pubMsg, "test", "id1", errors.New("underlying error"))
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
