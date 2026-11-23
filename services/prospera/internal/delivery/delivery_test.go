package delivery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/model"
)

func TestMain(m *testing.M) {
	fakes.InstallTestTracing()
	os.Exit(m.Run())
}

func TestFlushLiveTranscript(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		call        *model.VoiceCall
		discordResp int
		advanceErr  error
		wantErr     bool
		wantAdvance bool
	}{
		{
			name: "flushes unsent turns",
			call: &model.VoiceCall{
				CallID:               "call-10",
				LiveTranscriptCursor: 0,
				Transcript: []model.TranscriptTurn{
					{Role: "user", Text: "Hello"},
					{Role: "model", Text: "Hi there"},
				},
			},
			discordResp: http.StatusOK,
			wantAdvance: true,
		},
		{
			name: "empty turns with advance error returns error",
			call: &model.VoiceCall{
				CallID:               "call-10-empty-err",
				LiveTranscriptCursor: 0,
				Transcript: []model.TranscriptTurn{
					{Role: "user", Text: "   "},
				},
			},
			discordResp: http.StatusOK,
			advanceErr:  errors.New("advance failed"),
			wantErr:     true,
			wantAdvance: false,
		},
		{
			name: "posted turns with advance error returns error",
			call: &model.VoiceCall{
				CallID:               "call-10-posted-err",
				LiveTranscriptCursor: 0,
				Transcript: []model.TranscriptTurn{
					{Role: "user", Text: "Hello"},
				},
			},
			discordResp: http.StatusOK,
			advanceErr:  errors.New("advance failed"),
			wantErr:     true,
			wantAdvance: false,
		},
		{
			name: "no new turns",
			call: &model.VoiceCall{
				CallID:               "call-11",
				LiveTranscriptCursor: 2,
				Transcript: []model.TranscriptTurn{
					{Role: "user", Text: "Hello"},
					{Role: "model", Text: "Hi there"},
				},
			},
			discordResp: http.StatusOK,
			wantAdvance: false,
		},
		{
			name: "discord server error",
			call: &model.VoiceCall{
				CallID:               "call-12",
				LiveTranscriptCursor: 0,
				Transcript: []model.TranscriptTurn{
					{Role: "user", Text: "Hello"},
				},
			},
			discordResp: http.StatusInternalServerError,
			wantErr:     true,
			wantAdvance: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := fakes.NewFakeStore()
			store.AdvanceErr = tc.advanceErr
			_ = store.CreateOutboundCall(context.Background(), *tc.call, 5)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.discordResp)
			}))
			defer srv.Close()

			cfg := &config.Config{
				BrainURL:              srv.URL,
				DefaultDiscordChannel: "prospera",
			}

			reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, WithReporterHTTPClient(srv.Client()))
			err := reporter.FlushLiveTranscript(context.Background(), tc.call.CallID)

			if (err != nil) != tc.wantErr {
				t.Fatalf("FlushLiveTranscript() err = %v, wantErr = %v", err, tc.wantErr)
			}

			updated, _ := store.GetCall(context.Background(), tc.call.CallID)
			advanced := updated.LiveTranscriptCursor > tc.call.LiveTranscriptCursor
			if advanced != tc.wantAdvance {
				t.Fatalf("cursor advanced = %v, want = %v", advanced, tc.wantAdvance)
			}
		})
	}
}

func TestProcessVoicemailRecording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		initialStatus   string
		whisperCode     int
		whisperResp     string
		geminiCode      int
		geminiResp      string
		downloadErr     error
		discordCode     int
		vmRecErr        error
		failTransitions []string
		wantErr         bool
		wantErrContains string
		wantStatus      string
	}{
		{
			name:          "whisper success",
			initialStatus: "awaiting_recording",
			whisperCode:   http.StatusOK,
			whisperResp:   `{"text":"Please call back tomorrow morning."}`,
			wantStatus:    "delivered",
		},
		{
			name:          "whisper fails gemini fallback succeeds",
			initialStatus: "awaiting_recording",
			whisperCode:   http.StatusInternalServerError,
			geminiCode:    http.StatusOK,
			geminiResp:    `{"candidates":[{"content":{"parts":[{"text":"Fallback transcribed text."}]}}]}`,
			wantStatus:    "delivered",
		},
		{
			name:          "recording error still transcribes and delivers",
			initialStatus: "awaiting_recording",
			whisperCode:   http.StatusOK,
			whisperResp:   `{"text":"Please call back tomorrow morning."}`,
			vmRecErr:      errors.New("recording unavailable"),
			wantStatus:    "delivered",
		},
		{
			name:            "download error with failed-transition error",
			initialStatus:   "awaiting_recording",
			downloadErr:     errors.New("download timeout"),
			failTransitions: []string{"failed"},
			wantErr:         true,
			wantErrContains: "download recording",
			wantStatus:      "processing",
		},
		{
			name:            "delivered transition error keeps processing",
			initialStatus:   "awaiting_recording",
			whisperCode:     http.StatusOK,
			whisperResp:     `{"text":"Please call back tomorrow morning."}`,
			failTransitions: []string{"delivered"},
			wantStatus:      "processing",
		},
		{
			name:            "discord failure with failed-transition error",
			initialStatus:   "awaiting_recording",
			whisperCode:     http.StatusOK,
			whisperResp:     `{"text":"Please call back tomorrow morning."}`,
			discordCode:     http.StatusInternalServerError,
			failTransitions: []string{"failed"},
			wantErr:         true,
			wantErrContains: "discord returned status",
			wantStatus:      "processing",
		},
		{
			name:          "already delivered skipped",
			initialStatus: "delivered",
			wantStatus:    "delivered",
		},
		{
			name:          "retry after failed status succeeds",
			initialStatus: "failed",
			whisperCode:   http.StatusOK,
			whisperResp:   `{"text":"Retry succeeded."}`,
			wantStatus:    "delivered",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := fakes.NewFakeStore()
			store.VMRecErr = tc.vmRecErr
			fakes.SeedFailTransitions(store, tc.failTransitions)
			call := model.VoiceCall{
				CallID:       "call-vm-1",
				FromNumber:   "+15105550199",
				ToNumber:     "+15105550100",
				ReportStatus: tc.initialStatus,
			}
			_ = store.CreateInboundCall(context.Background(), call)

			discordCode := tc.discordCode
			if discordCode == 0 {
				discordCode = http.StatusOK
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/v1/audio/transcriptions") {
					w.WriteHeader(tc.whisperCode)
					_, _ = w.Write([]byte(tc.whisperResp))
					return
				}
				if strings.Contains(r.URL.Path, "generateContent") {
					w.WriteHeader(tc.geminiCode)
					_, _ = w.Write([]byte(tc.geminiResp))
					return
				}
				w.WriteHeader(discordCode)
			}))
			defer srv.Close()

			cfg := &config.Config{
				BrainURL:              srv.URL,
				WhisperURL:            srv.URL,
				GeminiAPIKey:          "test-key",
				GeminiFallbackModel:   "gemini-2.5-flash",
				DefaultDiscordChannel: "prospera",
			}

			twilioMock := &fakes.FakeTwilio{
				AudioData:   []byte("fake-wav-audio-content"),
				ContentType: "audio/wav",
				DownloadErr: tc.downloadErr,
			}

			reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, store, twilioMock, WithReporterHTTPClient(srv.Client()), WithReporterGeminiBaseURL(srv.URL))
			err := reporter.ProcessVoicemailRecording(context.Background(), call.CallID, "https://api.twilio.com/recording.wav")

			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessVoicemailRecording() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Fatalf("ProcessVoicemailRecording() err = %v, want substring %q", err, tc.wantErrContains)
			}

			updated, _ := store.GetCall(context.Background(), call.CallID)
			if updated.ReportStatus != tc.wantStatus {
				t.Fatalf("final ReportStatus = %s, want %s", updated.ReportStatus, tc.wantStatus)
			}
		})
	}
}

func TestDeliverInboundSMS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		msg         model.VoiceMessage
		discordCode int
		markErr     error
		wantErr     bool
	}{
		{
			name: "deliver text and media sms",
			msg: model.VoiceMessage{
				MessageSID: "SM999",
				FromNumber: "+15105550199",
				ToNumber:   "+15105550100",
				Body:       "Here is the confirmation picture",
				MediaURLs:  []string{"https://media.twilio.com/pic1.jpg"},
			},
			discordCode: http.StatusOK,
			wantErr:     false,
		},
		{
			name: "discord error returns err",
			msg: model.VoiceMessage{
				MessageSID: "SM1000",
				FromNumber: "+15105550199",
				ToNumber:   "+15105550100",
				Body:       "Hello",
			},
			discordCode: http.StatusInternalServerError,
			wantErr:     true,
		},
		{
			name: "mark delivered error still succeeds",
			msg: model.VoiceMessage{
				MessageSID: "SM1001",
				FromNumber: "+15105550199",
				ToNumber:   "+15105550100",
				Body:       "Hello",
			},
			discordCode: http.StatusOK,
			markErr:     errors.New("mark failed"),
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := fakes.NewFakeStore()
			store.MarkErr = tc.markErr
			_, _ = store.SaveVoiceMessage(context.Background(), tc.msg)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(body), tc.msg.Body) {
					t.Errorf("discord payload missing sms body: %s", string(body))
				}
				w.WriteHeader(tc.discordCode)
			}))
			defer srv.Close()

			cfg := &config.Config{
				BrainURL:              srv.URL,
				DefaultDiscordChannel: "prospera",
			}

			reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, store, &fakes.FakeTwilio{}, WithReporterHTTPClient(srv.Client()))
			err := reporter.DeliverInboundSMS(context.Background(), tc.msg)

			if (err != nil) != tc.wantErr {
				t.Fatalf("DeliverInboundSMS() err = %v, wantErr = %v", err, tc.wantErr)
			}

			if !tc.wantErr && tc.markErr == nil {
				msgs, _ := store.ListVoiceMessages(context.Background(), 10)
				if len(msgs) > 0 && !msgs[0].DiscordDelivered {
					t.Fatalf("expected message to be marked delivered")
				}
			}
		})
	}
}

func TestDelivery_ErrorBranches(t *testing.T) {
	t.Parallel()

	t.Run("flush live transcript error cases", func(t *testing.T) {
		t.Parallel()
		nilStoreReporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}, nil, &fakes.FakeTwilio{})
		if err := nilStoreReporter.FlushLiveTranscript(context.Background(), "call-1"); err == nil {
			t.Fatal("expected error with nil store")
		}

		store := fakes.NewFakeStore()
		reporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}, store, &fakes.FakeTwilio{})
		if err := reporter.FlushLiveTranscript(context.Background(), "call-nonexistent"); err == nil {
			t.Fatal("expected error with nonexistent call")
		}

		storeWithOpErr := fakes.NewFakeStore()
		storeWithOpErr.OpErr = errors.New("db error")
		opErrReporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}, storeWithOpErr, &fakes.FakeTwilio{})
		if err := opErrReporter.FlushLiveTranscript(context.Background(), "call-operr"); err == nil {
			t.Fatal("expected error with store op error")
		}

		// Empty text turns advance cursor without posting to discord
		_ = store.CreateOutboundCall(context.Background(), model.VoiceCall{
			CallID: "call-empty-turn",
			Transcript: []model.TranscriptTurn{
				{Role: "user", Text: "   "},
			},
		}, 5)
		if err := reporter.FlushLiveTranscript(context.Background(), "call-empty-turn"); err != nil {
			t.Fatalf("unexpected error with empty text turns: %v", err)
		}
	})

	t.Run("process voicemail recording error cases", func(t *testing.T) {
		t.Parallel()
		nilStoreReporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}, nil, &fakes.FakeTwilio{})
		if err := nilStoreReporter.ProcessVoicemailRecording(context.Background(), "call-1", "https://rec"); err == nil {
			t.Fatal("expected error with nil store")
		}

		store := fakes.NewFakeStore()
		reporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}, store, &fakes.FakeTwilio{})
		if err := reporter.ProcessVoicemailRecording(context.Background(), "call-nonexistent", "https://rec"); err == nil {
			t.Fatal("expected error with nonexistent call")
		}

		// Download recording error
		_ = store.CreateInboundCall(context.Background(), model.VoiceCall{
			CallID:       "call-dl-err",
			ReportStatus: "awaiting_recording",
		})
		failTwilio := &fakes.FakeTwilio{DownloadErr: errors.New("download timeout")}
		failReporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}, store, failTwilio)
		if err := failReporter.ProcessVoicemailRecording(context.Background(), "call-dl-err", "https://rec"); err == nil {
			t.Fatal("expected error when download fails")
		}

		// Discord delivery error
		discordFailSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer discordFailSrv.Close()

		_ = store.CreateInboundCall(context.Background(), model.VoiceCall{
			CallID:       "call-discord-fail",
			ReportStatus: "awaiting_recording",
		})
		discordFailReporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{
			BrainURL: discordFailSrv.URL,
		}}, store, &fakes.FakeTwilio{AudioData: []byte("wav")}, WithReporterHTTPClient(discordFailSrv.Client()))
		if err := discordFailReporter.ProcessVoicemailRecording(context.Background(), "call-discord-fail", "https://rec"); err == nil {
			t.Fatal("expected error when discord delivery fails")
		}

		storeVMTransErr := fakes.NewFakeStore()
		_ = storeVMTransErr.CreateInboundCall(context.Background(), model.VoiceCall{
			CallID:       "call-vm-trans-err",
			ReportStatus: "awaiting_recording",
		})
		storeVMTransErr.TransErr = errors.New("transition failed")
		vmTransReporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}, storeVMTransErr, &fakes.FakeTwilio{})
		if err := vmTransReporter.ProcessVoicemailRecording(context.Background(), "call-vm-trans-err", "https://rec"); err == nil {
			t.Fatal("expected error on transition failure")
		}
	})

	t.Run("deliver inbound sms without brain url", func(t *testing.T) {
		t.Parallel()
		reporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: ""}}, nil, &fakes.FakeTwilio{})
		err := reporter.DeliverInboundSMS(context.Background(), model.VoiceMessage{
			MessageSID: "SM1",
			Body:       "test",
		})
		if err == nil {
			t.Fatal("expected error when brain url is empty")
		}
	})
}

type transcriptionBranchTestCase struct {
	name       string
	backend    string
	code       int
	body       string
	whisperURL string
	failClient bool
	wantErr    bool
	wantResult string
}

func TestDelivery_TranscriptionAndSummaryBranches(t *testing.T) {
	t.Parallel()
	tests := []transcriptionBranchTestCase{
		{name: "summary http 500", backend: "gemini_summary", code: http.StatusInternalServerError, body: `server error`, wantErr: true},
		{name: "summary bad json", backend: "gemini_summary", code: http.StatusOK, body: `{invalid json`, wantErr: true},
		{name: "summary empty candidates", backend: "gemini_summary", code: http.StatusOK, body: `{"candidates":[]}`, wantErr: true},
		{name: "summary success", backend: "gemini_summary", code: http.StatusOK, body: `{"candidates":[{"content":{"parts":[{"text":"summary text"}]}}]}`, wantResult: "summary text"},
		{name: "summary transport error", backend: "gemini_summary", code: http.StatusOK, failClient: true, wantErr: true},
		{name: "whisper http 500", backend: "whisper", code: http.StatusInternalServerError, body: `server error`, wantErr: true},
		{name: "whisper bad json", backend: "whisper", code: http.StatusOK, body: `{bad json`, wantErr: true},
		{name: "whisper success", backend: "whisper", code: http.StatusOK, body: `{"text":"hello from whisper"}`, wantResult: "hello from whisper"},
		{name: "whisper request creation error", backend: "whisper", code: http.StatusOK, whisperURL: ":\x7f", wantErr: true},
		{name: "flash http 500", backend: "flash", code: http.StatusInternalServerError, body: `server error`, wantErr: true},
		{name: "flash bad json", backend: "flash", code: http.StatusOK, body: `{bad json`, wantErr: true},
		{name: "flash empty candidates", backend: "flash", code: http.StatusOK, body: `{"candidates":[]}`, wantErr: true},
		{name: "flash success", backend: "flash", code: http.StatusOK, body: `{"candidates":[{"content":{"parts":[{"text":"flash text"}]}}]}`, wantResult: "flash text"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTranscriptionBranchCase(t, tc)
		})
	}
}

func runTranscriptionBranchCase(t *testing.T, tc transcriptionBranchTestCase) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(tc.code)
		_, _ = w.Write([]byte(tc.body))
	}))
	t.Cleanup(srv.Close)

	geminiBaseURL := srv.URL
	whisperURL := srv.URL
	if tc.whisperURL != "" {
		whisperURL = tc.whisperURL
	}
	httpClient := srv.Client()
	if tc.failClient {
		httpClient = &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("network drop")
			}),
		}
	}
	cfg := &config.Config{
		GeminiAPIKey:        "test-key",
		GeminiFallbackModel: "gemini-2.5-flash",
		WhisperURL:          whisperURL,
	}
	rep := NewReporter(fakes.StaticConfig{Cfg: cfg}, nil, &fakes.FakeTwilio{}, WithReporterHTTPClient(httpClient), WithReporterGeminiBaseURL(geminiBaseURL))

	var res string
	var err error
	switch tc.backend {
	case "gemini_summary":
		res, err = rep.generateGeminiSummary(context.Background(), rep.cfgSrc.Current(), "test prompt")
	case "whisper":
		res, err = rep.transcribeViaWhisper(context.Background(), rep.cfgSrc.Current(), []byte("audio"))
	case "flash":
		res, err = rep.transcribeViaGeminiFlash(context.Background(), rep.cfgSrc.Current(), []byte("audio"), "audio/wav")
	}

	if tc.wantErr && err == nil {
		t.Fatalf("%s: expected error, got nil", tc.name)
	}
	if !tc.wantErr && (err != nil || res != tc.wantResult) {
		t.Fatalf("%s: unexpected result (%q, %v)", tc.name, res, err)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestDelivery_WebhookAndStoreErrorPropagation(t *testing.T) {
	t.Parallel()

	// 1. postDiscordMessage client error
	failingClient := &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("network drop")
		}),
	}
	rBadURL := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: ":\x7f"}}, nil, nil)
	if err := rBadURL.postDiscordMessage(context.Background(), rBadURL.cfgSrc.Current(), "c1", "", "msg", nil); err == nil {
		t.Fatal("expected postDiscordMessage request creation error")
	}
	rFail := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain.local"}}, nil, nil, WithReporterHTTPClient(failingClient))
	if err := rFail.postDiscordMessage(context.Background(), rFail.cfgSrc.Current(), "c1", "", "msg", nil); err == nil {
		t.Fatal("expected postDiscordMessage network error")
	}

	// 2. FlushLiveTranscript speaker roles and store error
	storeErr := fakes.NewFakeStore()
	storeErr.OpErr = errors.New("db error")
	rStoreErr := NewReporter(fakes.StaticConfig{Cfg: &config.Config{BrainURL: "http://brain.local"}}, storeErr, nil)
	if err := rStoreErr.FlushLiveTranscript(context.Background(), "c1"); err == nil {
		t.Fatal("expected get call error")
	}
	if err := rStoreErr.ProcessVoicemailRecording(context.Background(), "c1", "http://rec"); err == nil {
		t.Fatal("expected get call error")
	}

	// 3. FlushLiveTranscript speaker roles ("system", "caller", "prospera")
	storeRoles := fakes.NewFakeStore()
	_ = storeRoles.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID: "call-roles",
		Transcript: []model.TranscriptTurn{
			{Role: "system", Text: "system turn"},
			{Role: "caller", Text: "caller turn"},
			{Role: "prospera", Text: "agent turn"},
		},
	}, 5)
	rRoles := NewReporter(fakes.StaticConfig{Cfg: &config.Config{
		BrainURL:              "http://brain.local",
		DefaultDiscordChannel: "prospera",
	}}, storeRoles, nil, WithReporterHTTPClient(&http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
			}, nil
		}),
	}))
	if err := rRoles.FlushLiveTranscript(context.Background(), "call-roles"); err != nil {
		t.Fatalf("flush roles error: %v", err)
	}

	// 4. generateGeminiSummary nil config and empty key
	rNilCfg := NewReporter(fakes.StaticConfig{Cfg: nil}, nil, nil)
	if _, err := rNilCfg.generateGeminiSummary(context.Background(), rNilCfg.cfgSrc.Current(), "p"); err == nil {
		t.Fatal("expected error on nil cfg")
	}
	rEmptyKey := NewReporter(fakes.StaticConfig{Cfg: &config.Config{GeminiAPIKey: ""}}, nil, nil)
	if _, err := rEmptyKey.generateGeminiSummary(context.Background(), rEmptyKey.cfgSrc.Current(), "p"); err == nil {
		t.Fatal("expected error on empty key")
	}
	if _, err := rEmptyKey.transcribeAudio(context.Background(), rEmptyKey.cfgSrc.Current(), []byte("a"), "audio/wav"); err == nil {
		t.Fatal("expected error when no backends configured")
	}

	// 5. DeliverInboundSMS mark delivered error
	storeSMS := fakes.NewFakeStore()
	storeSMS.OpErr = errors.New("mark err")
	rSMS := NewReporter(fakes.StaticConfig{Cfg: &config.Config{
		BrainURL: "http://brain.local",
	}}, storeSMS, nil, WithReporterHTTPClient(&http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
			}, nil
		}),
	}))
	if err := rSMS.DeliverInboundSMS(context.Background(), model.VoiceMessage{MessageSID: "SM1", Body: "hello"}); err != nil {
		t.Fatalf("deliver sms error: %v", err)
	}

	// 6. transcribeViaWhisper and transcribeViaGeminiFlash transport error
	rFailTransport := NewReporter(fakes.StaticConfig{Cfg: &config.Config{
		WhisperURL:   "http://whisper.local",
		GeminiAPIKey: "key-123",
	}}, nil, nil, WithReporterHTTPClient(failingClient))
	if _, err := rFailTransport.transcribeViaWhisper(context.Background(), rFailTransport.cfgSrc.Current(), []byte("audio")); err == nil {
		t.Fatal("expected whisper transport error")
	}
	if _, err := rFailTransport.transcribeViaGeminiFlash(context.Background(), rFailTransport.cfgSrc.Current(), []byte("audio"), "audio/wav"); err == nil {
		t.Fatal("expected flash transport error")
	}
}

func TestResolveDiscordChannel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cfg       *config.Config
		channelID string
		want      string
	}{
		{name: "configured default", cfg: &config.Config{DefaultDiscordChannel: "custom-chan"}, want: "custom-chan"},
		{name: "explicit channel wins", cfg: &config.Config{DefaultDiscordChannel: "custom-chan"}, channelID: "specific-chan", want: "specific-chan"},
		{name: "nil config falls back", cfg: nil, want: "prospera"},
		{name: "empty default falls back", cfg: &config.Config{}, want: "prospera"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ResolveDiscordChannel(tc.cfg, tc.channelID); got != tc.want {
				t.Fatalf("ResolveDiscordChannel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNewReporter_TraceHeaders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		withParent bool
	}{
		{name: "eri call continues the caller trace; third parties get no headers", withParent: true},
		{name: "eri call roots a trace without a caller; third parties get no headers"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			got := map[string]string{}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				if got[r.URL.Path] == "" { // keep any leaked header across repeat calls to one path
					got[r.URL.Path] = r.Header.Get("Traceparent")
				}
				mu.Unlock()
				_, _ = w.Write([]byte(`{"text":"ok","candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`))
			}))
			t.Cleanup(srv.Close)
			cfg := &config.Config{BrainURL: srv.URL, GeminiAPIKey: "k", WhisperURL: srv.URL, GeminiFallbackModel: "gemini-test"}
			r := NewReporter(fakes.StaticConfig{Cfg: cfg}, fakes.NewFakeStore(), nil, WithReporterGeminiBaseURL(srv.URL))
			ctx, parentTraceID := fakes.TraceCaller(tc.withParent)

			if err := r.postDiscordMessage(ctx, cfg, "chan", "", "hi", nil); err != nil {
				t.Fatalf("postDiscordMessage: %v", err)
			}
			_, _ = r.generateGeminiSummary(ctx, cfg, "summarize")
			_, _ = r.transcribeViaWhisper(ctx, cfg, []byte("wav"))
			_, _ = r.transcribeViaGeminiFlash(ctx, cfg, []byte("wav"), "audio/wav")

			mu.Lock()
			defer mu.Unlock()
			eriTrace := fakes.HeaderTraceID(http.Header{"Traceparent": {got["/discord/message"]}})
			if eriTrace == "" || (eriTrace == parentTraceID) != tc.withParent {
				t.Errorf("eri traceparent %q, caller trace %q, want continues=%v", got["/discord/message"], parentTraceID, tc.withParent)
			}
			for _, path := range []string{"/v1beta/models/gemini-test:generateContent", "/v1/audio/transcriptions"} {
				if v, ok := got[path]; !ok || v != "" {
					t.Errorf("%s reached=%v traceparent=%q, want reached with no traceparent", path, ok, v)
				}
			}
		})
	}
}
