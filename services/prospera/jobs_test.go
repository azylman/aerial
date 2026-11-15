package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	termErr   error
	nakErr    error
}

func (m *mockJobMsg) Data() []byte         { return m.data }
func (m *mockJobMsg) Subject() string      { return "" }
func (m *mockJobMsg) Header() nats.Header  { return nil }
func (m *mockJobMsg) NumDelivered() uint64 { return m.delivered }
func (m *mockJobMsg) Ack() error           { return nil }
func (m *mockJobMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return m.nakErr
}
func (m *mockJobMsg) Term() error                  { m.termed = true; return m.termErr }
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

func TestServer_StartJobConsumers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupBus    func() (pubsub.Bus, error)
		failSubject string
		wantErr     bool
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
				b := pubsub.NewMemoryBus()
				return b, nil
			},
			wantErr: false,
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
		})
	}
}

func TestServer_HandleReportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		nilMsg       bool
		data         []byte
		delivered    uint64
		nilReporter  bool
		reporterErr  bool
		termErr      error
		nakErr       error
		wantErr      bool
		wantTermed   bool
		wantNaked    bool
		wantNakDelay time.Duration
	}{
		{
			name:    "nil message error",
			nilMsg:  true,
			wantErr: true,
		},
		{
			name:       "unmarshal error terms",
			data:       []byte("invalid json"),
			wantTermed: true,
		},
		{
			name:       "unmarshal error with term error",
			data:       []byte("invalid json"),
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
		{
			name:       "empty call id terms",
			data:       []byte(`{"call_id":"   "}`),
			wantTermed: true,
		},
		{
			name:       "empty call id with term error",
			data:       []byte(`{"call_id":"   "}`),
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
		{
			name:        "nil reporter error",
			data:        []byte(`{"call_id":"c1"}`),
			nilReporter: true,
			wantErr:     true,
		},
		{
			name:         "reporter error attempt 1 naks with 1s",
			data:         []byte(`{"call_id":"c1"}`),
			delivered:    1,
			reporterErr:  true,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:         "reporter error attempt 1 with nak error",
			data:         []byte(`{"call_id":"c1"}`),
			delivered:    1,
			reporterErr:  true,
			nakErr:       errors.New("nak failed"),
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:         "reporter error attempt 3 naks with 4s",
			data:         []byte(`{"call_id":"c1"}`),
			delivered:    3,
			reporterErr:  true,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 4 * time.Second,
		},
		{
			name:        "reporter error attempt 5 terms",
			data:        []byte(`{"call_id":"c1"}`),
			delivered:   5,
			reporterErr: true,
			wantTermed:  true,
		},
		{
			name:        "reporter error attempt 5 with term error",
			data:        []byte(`{"call_id":"c1"}`),
			delivered:   5,
			reporterErr: true,
			termErr:     errors.New("term failed"),
			wantTermed:  true,
		},
		{
			name:      "success",
			data:      []byte(`{"call_id":"call-rep-ok"}`),
			delivered: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, store, _ := setupTestServer()
			_ = store.CreateOutboundCall(context.Background(), VoiceCall{
				CallID:       "call-rep-ok",
				ReportStatus: "delivered",
			}, 5)

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = NewReporter(staticConfig{cfg: &Config{}}, store, nil)
			}

			var msg *mockJobMsg
			if !tc.nilMsg {
				msg = &mockJobMsg{data: tc.data, delivered: tc.delivered, termErr: tc.termErr, nakErr: tc.nakErr}
			}

			var pubMsg pubsub.Message
			if msg != nil {
				pubMsg = msg
			}

			err := server.HandleReportJob(context.Background(), pubMsg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleReportJob() err = %v, wantErr = %v", err, tc.wantErr)
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

func TestServer_HandleVoicemailJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		nilMsg       bool
		data         []byte
		delivered    uint64
		nilReporter  bool
		reporterErr  bool
		termErr      error
		nakErr       error
		wantErr      bool
		wantTermed   bool
		wantNaked    bool
		wantNakDelay time.Duration
	}{
		{
			name:    "nil message error",
			nilMsg:  true,
			wantErr: true,
		},
		{
			name:       "unmarshal error terms",
			data:       []byte("invalid json"),
			wantTermed: true,
		},
		{
			name:       "unmarshal error with term error",
			data:       []byte("invalid json"),
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
		{
			name:       "missing call_id or url terms",
			data:       []byte(`{"call_id":"c1","recording_url":""}`),
			wantTermed: true,
		},
		{
			name:       "missing call_id or url with term error",
			data:       []byte(`{"call_id":"c1","recording_url":""}`),
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
		{
			name:        "nil reporter error",
			data:        []byte(`{"call_id":"c1","recording_url":"http://rec.wav"}`),
			nilReporter: true,
			wantErr:     true,
		},
		{
			name:         "reporter error attempt 2 naks with 2s",
			data:         []byte(`{"call_id":"c1","recording_url":"http://rec.wav"}`),
			delivered:    2,
			reporterErr:  true,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 2 * time.Second,
		},
		{
			name:         "reporter error attempt 2 with nak error",
			data:         []byte(`{"call_id":"c1","recording_url":"http://rec.wav"}`),
			delivered:    2,
			reporterErr:  true,
			nakErr:       errors.New("nak failed"),
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 2 * time.Second,
		},
		{
			name:        "reporter error attempt 5 terms",
			data:        []byte(`{"call_id":"c1","recording_url":"http://rec.wav"}`),
			delivered:   5,
			reporterErr: true,
			wantTermed:  true,
		},
		{
			name:        "reporter error attempt 5 with term error",
			data:        []byte(`{"call_id":"c1","recording_url":"http://rec.wav"}`),
			delivered:   5,
			reporterErr: true,
			termErr:     errors.New("term failed"),
			wantTermed:  true,
		},
		{
			name:      "success",
			data:      []byte(`{"call_id":"call-vm-ok","recording_url":"http://rec.wav"}`),
			delivered: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, store, _ := setupTestServer()
			_ = store.CreateInboundCall(context.Background(), VoiceCall{
				CallID:       "call-vm-ok",
				ReportStatus: "delivered",
			})

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = NewReporter(staticConfig{cfg: &Config{}}, store, nil)
			}

			var msg *mockJobMsg
			if !tc.nilMsg {
				msg = &mockJobMsg{data: tc.data, delivered: tc.delivered, termErr: tc.termErr, nakErr: tc.nakErr}
			}

			var pubMsg pubsub.Message
			if msg != nil {
				pubMsg = msg
			}

			err := server.HandleVoicemailJob(context.Background(), pubMsg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleVoicemailJob() err = %v, wantErr = %v", err, tc.wantErr)
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

func TestServer_HandleSMSJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		nilMsg       bool
		data         []byte
		delivered    uint64
		nilReporter  bool
		reporterErr  bool
		termErr      error
		nakErr       error
		wantErr      bool
		wantTermed   bool
		wantNaked    bool
		wantNakDelay time.Duration
	}{
		{
			name:    "nil message error",
			nilMsg:  true,
			wantErr: true,
		},
		{
			name:       "unmarshal error terms",
			data:       []byte("invalid json"),
			wantTermed: true,
		},
		{
			name:       "unmarshal error with term error",
			data:       []byte("invalid json"),
			termErr:    errors.New("term failed"),
			wantTermed: true,
		},
		{
			name:        "nil reporter error",
			data:        []byte(`{"message_sid":"SM1","body":"hello"}`),
			nilReporter: true,
			wantErr:     true,
		},
		{
			name:         "reporter error attempt 4 naks with 8s",
			data:         []byte(`{"message_sid":"SM1","body":"hello"}`),
			delivered:    4,
			reporterErr:  true,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 8 * time.Second,
		},
		{
			name:         "reporter error attempt 4 with nak error",
			data:         []byte(`{"message_sid":"SM1","body":"hello"}`),
			delivered:    4,
			reporterErr:  true,
			nakErr:       errors.New("nak failed"),
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 8 * time.Second,
		},
		{
			name:        "reporter error attempt 6 terms",
			data:        []byte(`{"message_sid":"SM1","body":"hello"}`),
			delivered:   6,
			reporterErr: true,
			wantTermed:  true,
		},
		{
			name:        "reporter error attempt 6 with term error",
			data:        []byte(`{"message_sid":"SM1","body":"hello"}`),
			delivered:   6,
			reporterErr: true,
			termErr:     errors.New("term failed"),
			wantTermed:  true,
		},
		{
			name:      "success",
			data:      []byte(`{"message_sid":"SM-ok","body":"hi"}`),
			delivered: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			server, store, _ := setupTestServer()
			server.reporter = NewReporter(staticConfig{cfg: &Config{BrainURL: srv.URL, DefaultDiscordChannel: "prospera"}}, store, nil, WithReporterHTTPClient(srv.Client()))

			if tc.nilReporter {
				server.reporter = nil
			} else if tc.reporterErr {
				server.reporter = NewReporter(staticConfig{cfg: &Config{BrainURL: ""}}, store, nil)
			}

			var msg *mockJobMsg
			if !tc.nilMsg {
				msg = &mockJobMsg{data: tc.data, delivered: tc.delivered, termErr: tc.termErr, nakErr: tc.nakErr}
			}

			var pubMsg pubsub.Message
			if msg != nil {
				pubMsg = msg
			}

			err := server.HandleSMSJob(context.Background(), pubMsg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleSMSJob() err = %v, wantErr = %v", err, tc.wantErr)
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

type failPublishBus struct {
	pubsub.Bus
}

func (f *failPublishBus) Publish(ctx context.Context, subject string, msgID string, data any) error {
	return errors.New("simulated publish failure")
}

func TestServer_WebhooksWithEventBusPublishAndFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bus        pubsub.Bus
		wantMsgSub string
	}{
		{
			name:       "success publish to memory bus",
			bus:        pubsub.NewMemoryBus(),
			wantMsgSub: "published",
		},
		{
			name:       "failing bus falls back to goroutine",
			bus:        &failPublishBus{Bus: pubsub.NewMemoryBus()},
			wantMsgSub: "fallback",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, store, cfg := setupTestServer()
			server.SetEventBus(tc.bus)
			if tc.bus != nil {
				_ = pubsub.EnsureCoreStreams(context.Background(), tc.bus)
			}
			mux := server.Routes()

			// 1. Status webhook trigger post call report
			callID := "call-bus-report-" + tc.name
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
			vmCallID := "call-bus-vm-" + tc.name
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
			smsForm := url.Values{
				"MessageSid": []string{"SM-" + tc.name},
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
		})
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
		name       string
		nilMsg     bool
		delivered  uint64
		termErr    error
		nakErr     error
		wantErr    bool
		wantTermed bool
		wantNaked  bool
	}{
		{
			name:    "nil msg returns original error",
			nilMsg:  true,
			wantErr: true,
		},
		{
			name:      "delivered 1 naks",
			delivered: 1,
			wantErr:   true,
			wantNaked: true,
		},
		{
			name:      "delivered 1 with nak error",
			delivered: 1,
			nakErr:    errors.New("nak fail"),
			wantErr:   true,
			wantNaked: true,
		},
		{
			name:       "delivered 5 terms and drops",
			delivered:  5,
			wantErr:    false,
			wantTermed: true,
		},
		{
			name:       "delivered 5 with term error",
			delivered:  5,
			termErr:    errors.New("term fail"),
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
				msg = &mockJobMsg{delivered: tc.delivered, termErr: tc.termErr, nakErr: tc.nakErr}
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
			}
		})
	}
}

func TestServer_EventBus_NilReceiverAndEdgeCases(t *testing.T) {
	t.Parallel()

	var nilServer *Server
	nilServer.SetEventBus(nil)
	if got := nilServer.EventBus(); got != nil {
		t.Errorf("nilServer.EventBus() = %v, want nil", got)
	}

	server, _, _ := setupTestServer()
	server.triggerPostCallReportIfPending(context.Background(), "")
	server.triggerPostCallReportIfPending(context.Background(), "   ")
}
