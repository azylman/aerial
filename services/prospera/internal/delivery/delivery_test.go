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
	"github.com/azylman/aerial/services/prospera/internal/store"
)

func TestMain(m *testing.M) {
	fakes.InstallTestTracing()
	os.Exit(m.Run())
}

type flushLiveTranscriptTestCase struct {
	name        string
	call        *model.VoiceCall
	callID      string
	nilStore    bool
	storeOpErr  error
	discordResp int
	advanceErr  error
	wantErr     bool
	wantAdvance bool
}

func runFlushLiveTranscriptCase(t *testing.T, tc flushLiveTranscriptTestCase) {
	t.Helper()

	var st store.Store
	var fakeStore *fakes.FakeStore
	if !tc.nilStore {
		fakeStore = fakes.NewFakeStore()
		fakeStore.AdvanceErr = tc.advanceErr
		fakeStore.OpErr = tc.storeOpErr
		if tc.call != nil {
			if err := fakeStore.CreateOutboundCall(context.Background(), *tc.call, 5); err != nil {
				t.Fatalf("CreateOutboundCall: %v", err)
			}
		}
		st = fakeStore
	}

	srv := fakes.NewStubUpstreamServer(t, fakes.StubUpstreamConfig{
		DiscordStatusCode: tc.discordResp,
	})

	cfg := &config.Config{
		BrainURL:              srv.URL,
		DefaultDiscordChannel: "prospera",
	}

	reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, st, &fakes.FakeTwilio{}, WithReporterHTTPClient(srv.Client()))
	callID := tc.callID
	if tc.call != nil {
		callID = tc.call.CallID
	}
	err := reporter.FlushLiveTranscript(context.Background(), callID)

	if (err != nil) != tc.wantErr {
		t.Fatalf("FlushLiveTranscript() err = %v, wantErr = %v", err, tc.wantErr)
	}

	if !tc.wantErr && tc.call != nil && fakeStore != nil {
		updated, err := fakeStore.GetCall(context.Background(), tc.call.CallID)
		if err != nil {
			t.Fatalf("GetCall: %v", err)
		}
		advanced := updated.LiveTranscriptCursor > tc.call.LiveTranscriptCursor
		if advanced != tc.wantAdvance {
			t.Fatalf("cursor advanced = %v, want = %v", advanced, tc.wantAdvance)
		}
	}
}

func TestFlushLiveTranscript(t *testing.T) {
	t.Parallel()

	tests := []flushLiveTranscriptTestCase{
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
			name: "empty text turns advance without posting",
			call: &model.VoiceCall{
				CallID:               "call-empty-turn",
				LiveTranscriptCursor: 0,
				Transcript: []model.TranscriptTurn{
					{Role: "user", Text: "   "},
				},
			},
			discordResp: http.StatusOK,
			wantAdvance: true,
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
		{
			name:     "nil store returns error",
			callID:   "call-1",
			nilStore: true,
			wantErr:  true,
		},
		{
			name:    "nonexistent call returns error",
			callID:  "call-nonexistent",
			wantErr: true,
		},
		{
			name:       "store op error returns error",
			callID:     "call-operr",
			storeOpErr: errors.New("db error"),
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runFlushLiveTranscriptCase(t, tc)
		})
	}
}

type voicemailRecordingGuardCase struct {
	name          string
	initialStatus string
	nilStore      bool
	missingCall   bool
	transErr      error
	wantErr       bool
	wantStatus    string
}

func TestProcessVoicemailRecording_GuardsAndSkips(t *testing.T) {
	t.Parallel()

	tests := []voicemailRecordingGuardCase{
		{
			name:          "already delivered skipped",
			initialStatus: "delivered",
			wantStatus:    "delivered",
		},
		{
			name:     "nil store returns error",
			nilStore: true,
			wantErr:  true,
		},
		{
			name:        "nonexistent call returns error",
			missingCall: true,
			wantErr:     true,
		},
		{
			name:          "transition to processing error returns error",
			initialStatus: "awaiting_recording",
			transErr:      errors.New("transition failed"),
			wantErr:       true,
			wantStatus:    "awaiting_recording",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoicemailRecordingGuardCase(t, tc)
		})
	}
}

func seedVoicemailGuardStore(t *testing.T, tc voicemailRecordingGuardCase, callID string) *fakes.FakeStore {
	t.Helper()
	fakeStore := fakes.NewFakeStore()
	fakeStore.TransErr = tc.transErr
	if tc.missingCall {
		return fakeStore
	}
	initStatus := tc.initialStatus
	if initStatus == "" {
		initStatus = "awaiting_recording"
	}
	call := model.VoiceCall{
		CallID:       callID,
		FromNumber:   "+15105550199",
		ToNumber:     "+15105550100",
		ReportStatus: initStatus,
	}
	if err := fakeStore.CreateInboundCall(context.Background(), call); err != nil {
		t.Fatalf("CreateInboundCall: %v", err)
	}
	return fakeStore
}

func runVoicemailRecordingGuardCase(t *testing.T, tc voicemailRecordingGuardCase) {
	t.Helper()

	callID := "call-vm-guard"
	if tc.missingCall {
		callID = "call-nonexistent"
	}

	var st store.Store
	var fakeStore *fakes.FakeStore
	if !tc.nilStore {
		fakeStore = seedVoicemailGuardStore(t, tc, callID)
		st = fakeStore
	}

	cfg := &config.Config{
		BrainURL:              "http://brain.local",
		WhisperURL:            "http://voice.local",
		GeminiAPIKey:          "test-key",
		GeminiFallbackModel:   "gemini-2.5-flash",
		DefaultDiscordChannel: "prospera",
	}

	reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, st, &fakes.FakeTwilio{})
	err := reporter.ProcessVoicemailRecording(context.Background(), callID, "https://api.twilio.com/recording.wav")

	if (err != nil) != tc.wantErr {
		t.Fatalf("ProcessVoicemailRecording() err = %v, wantErr = %v", err, tc.wantErr)
	}

	if fakeStore != nil && !tc.missingCall && tc.wantStatus != "" {
		updated, err := fakeStore.GetCall(context.Background(), callID)
		if err != nil {
			t.Fatalf("GetCall: %v", err)
		}
		if updated.ReportStatus != tc.wantStatus {
			t.Fatalf("final ReportStatus = %s, want %s", updated.ReportStatus, tc.wantStatus)
		}
	}
}

type voicemailRecordingTestCase struct {
	name                  string
	initialStatus         string
	emptyAudioContentType bool
	whisperCode           int
	whisperResp           string
	geminiCode            int
	geminiResp            string
	downloadErr           error
	discordCode           int
	vmRecErr              error
	failTransitions       []string
	wantErrSub            string
	wantStatus            string
}

func TestProcessVoicemailRecording(t *testing.T) {
	t.Parallel()

	tests := []voicemailRecordingTestCase{
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
			wantErrSub:      "download recording",
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
			wantErrSub:      "discord returned status",
			wantStatus:      "processing",
		},
		{
			name:          "retry after failed status succeeds",
			initialStatus: "failed",
			whisperCode:   http.StatusOK,
			whisperResp:   `{"text":"Retry succeeded."}`,
			wantStatus:    "delivered",
		},
		{
			name:                  "whisper and gemini fail fallback to unavailable message",
			initialStatus:         "awaiting_recording",
			emptyAudioContentType: true,
			whisperCode:           http.StatusInternalServerError,
			geminiCode:            http.StatusInternalServerError,
			wantStatus:            "delivered",
		},
		{
			name:          "download error returns error",
			initialStatus: "awaiting_recording",
			downloadErr:   errors.New("download timeout"),
			wantErrSub:    "download timeout",
			wantStatus:    "failed",
		},
		{
			name:          "discord delivery error returns error",
			initialStatus: "awaiting_recording",
			whisperCode:   http.StatusOK,
			whisperResp:   `{"text":"Please call back."}`,
			discordCode:   http.StatusInternalServerError,
			wantErrSub:    "discord returned status",
			wantStatus:    "failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProcessVoicemailRecordingCase(t, tc)
		})
	}
}

func seedVoicemailStore(t *testing.T, tc voicemailRecordingTestCase, callID string) *fakes.FakeStore {
	t.Helper()
	fakeStore := fakes.NewFakeStore()
	fakeStore.VMRecErr = tc.vmRecErr
	fakes.SeedFailTransitions(fakeStore, tc.failTransitions)
	initStatus := tc.initialStatus
	if initStatus == "" {
		initStatus = "awaiting_recording"
	}
	call := model.VoiceCall{
		CallID:       callID,
		FromNumber:   "+15105550199",
		ToNumber:     "+15105550100",
		ReportStatus: initStatus,
	}
	if err := fakeStore.CreateInboundCall(context.Background(), call); err != nil {
		t.Fatalf("CreateInboundCall: %v", err)
	}
	return fakeStore
}

func runProcessVoicemailRecordingCase(t *testing.T, tc voicemailRecordingTestCase) {
	t.Helper()

	callID := "call-vm-1"
	fakeStore := seedVoicemailStore(t, tc, callID)

	discordCode := tc.discordCode
	if discordCode == 0 {
		discordCode = http.StatusOK
	}
	srv := fakes.NewStubUpstreamServer(t, fakes.StubUpstreamConfig{
		WhisperStatusCode:   tc.whisperCode,
		WhisperResponseBody: tc.whisperResp,
		GeminiStatusCode:    tc.geminiCode,
		GeminiResponseBody:  tc.geminiResp,
		DiscordStatusCode:   discordCode,
	})

	cfg := &config.Config{
		BrainURL:              srv.URL,
		WhisperURL:            srv.URL,
		GeminiAPIKey:          "test-key",
		GeminiFallbackModel:   "gemini-2.5-flash",
		DefaultDiscordChannel: "prospera",
	}

	contentType := "audio/wav"
	if tc.emptyAudioContentType {
		contentType = ""
	}
	twilioMock := &fakes.FakeTwilio{
		AudioData:   []byte("fake-wav-audio-content"),
		ContentType: contentType,
		DownloadErr: tc.downloadErr,
	}

	reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, fakeStore, twilioMock, WithReporterHTTPClient(srv.Client()), WithReporterGeminiBaseURL(srv.URL))
	err := reporter.ProcessVoicemailRecording(context.Background(), callID, "https://api.twilio.com/recording.wav")

	if tc.wantErrSub != "" {
		if err == nil {
			t.Fatalf("ProcessVoicemailRecording() err = nil, want substring %q", tc.wantErrSub)
		}
		if !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("ProcessVoicemailRecording() err = %v, want substring %q", err, tc.wantErrSub)
		}
	} else if err != nil {
		t.Fatalf("ProcessVoicemailRecording() unexpected err = %v", err)
	}

	if tc.wantStatus != "" {
		updated, err := fakeStore.GetCall(context.Background(), callID)
		if err != nil {
			t.Fatalf("GetCall: %v", err)
		}
		if updated.ReportStatus != tc.wantStatus {
			t.Fatalf("final ReportStatus = %s, want %s", updated.ReportStatus, tc.wantStatus)
		}
	}
}

type deliverInboundSMSTestCase struct {
	name          string
	msg           model.VoiceMessage
	discordCode   int
	markErr       error
	emptyBrainURL bool
	wantErr       bool
}

func assertInboundSMSDelivered(t *testing.T, tc deliverInboundSMSTestCase, fakeStore *fakes.FakeStore, recorded []fakes.RecordedRequest) {
	t.Helper()
	if len(recorded) != 1 || !strings.Contains(string(recorded[0].Body), tc.msg.Body) {
		t.Errorf("discord payload missing sms body: %+v", recorded)
	}
	if tc.markErr != nil {
		return
	}
	msgs, err := fakeStore.ListVoiceMessages(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListVoiceMessages: %v", err)
	}
	if len(msgs) > 0 && !msgs[0].DiscordDelivered {
		t.Fatalf("expected message to be marked delivered")
	}
}

func runDeliverInboundSMSCase(t *testing.T, tc deliverInboundSMSTestCase) {
	t.Helper()

	fakeStore := fakes.NewFakeStore()
	fakeStore.MarkErr = tc.markErr
	if _, err := fakeStore.SaveVoiceMessage(context.Background(), tc.msg); err != nil {
		t.Fatalf("SaveVoiceMessage: %v", err)
	}

	var recorded []fakes.RecordedRequest
	srv := fakes.NewStubUpstreamServer(t, fakes.StubUpstreamConfig{
		DiscordStatusCode: tc.discordCode,
		RecordedRequests:  &recorded,
	})

	brainURL := srv.URL
	if tc.emptyBrainURL {
		brainURL = ""
	}
	cfg := &config.Config{
		BrainURL:              brainURL,
		DefaultDiscordChannel: "prospera",
	}

	reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, fakeStore, &fakes.FakeTwilio{}, WithReporterHTTPClient(srv.Client()))
	err := reporter.DeliverInboundSMS(context.Background(), tc.msg)

	if (err != nil) != tc.wantErr {
		t.Fatalf("DeliverInboundSMS() err = %v, wantErr = %v", err, tc.wantErr)
	}

	if !tc.wantErr && !tc.emptyBrainURL {
		assertInboundSMSDelivered(t, tc, fakeStore, recorded)
	}
}

func TestDeliverInboundSMS(t *testing.T) {
	t.Parallel()

	tests := []deliverInboundSMSTestCase{
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
		{
			name: "empty brain url returns err",
			msg: model.VoiceMessage{
				MessageSID: "SM1",
				Body:       "test",
			},
			emptyBrainURL: true,
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDeliverInboundSMSCase(t, tc)
		})
	}
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

func TestTranscribeAndSummarize(t *testing.T) {
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
	if err := storeRoles.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID: "call-roles",
		Transcript: []model.TranscriptTurn{
			{Role: "system", Text: "system turn"},
			{Role: "caller", Text: "caller turn"},
			{Role: "prospera", Text: "agent turn"},
		},
	}, 5); err != nil {
		t.Fatalf("CreateOutboundCall: %v", err)
	}
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
			if _, err := r.generateGeminiSummary(ctx, cfg, "summarize"); err != nil {
				t.Fatalf("generateGeminiSummary: %v", err)
			}
			if _, err := r.transcribeViaWhisper(ctx, cfg, []byte("wav")); err != nil {
				t.Fatalf("transcribeViaWhisper: %v", err)
			}
			if _, err := r.transcribeViaGeminiFlash(ctx, cfg, []byte("wav"), "audio/wav"); err != nil {
				t.Fatalf("transcribeViaGeminiFlash: %v", err)
			}

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
