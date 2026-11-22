package delivery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/prospera/internal/audio"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
)

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return nil }

func runMockRunContext(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
	res, err := fn(mockRunContext{Context: context.Background()})
	if err != nil {
		if term, ok := errors.AsType[restate.TerminalError](err); ok {
			return term
		}
		return restate.TerminalErrorf("%v", err)
	}
	if output != nil && res != nil {
		outVal := reflect.ValueOf(output)
		resVal := reflect.ValueOf(res)
		if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
			outVal.Elem().Set(resVal)
		}
	}
	return nil
}

type deliverReportTestCase struct {
	name             string
	callID           string
	reqCallID        string
	omitKey          bool
	initialStatus    string
	direction        string
	fromNumber       string
	toNumber         string
	geminiCode       int
	geminiResp       string
	discordCode      int
	withWAV          bool
	nonexistentCall  bool
	nilStore         bool
	storeOpErr       error
	storeTransErr    error
	flushErr         error
	failTransitions  []string
	expectErrorCode  restate.Code
	expectErrSub     string
	expectDelivered  bool
	expectFailed     bool
	expectWithNumber string
	expectFilename   string
}

func deliverReportSuccessCases() []deliverReportTestCase {
	return []deliverReportTestCase{
		{
			name:             "successful delivery outbound call with gemini summary",
			callID:           "call-vo-1",
			initialStatus:    "pending",
			direction:        "outbound",
			fromNumber:       "+15105550100",
			toNumber:         "+15105550199",
			geminiCode:       http.StatusOK,
			geminiResp:       `{"candidates":[{"content":{"parts":[{"text":"Outbound call was completed successfully."}]}}]}`,
			expectDelivered:  true,
			expectWithNumber: "+15105550199",
		},
		{
			name:             "fallback to req call_id when context key is empty",
			callID:           "call-vo-req-fallback",
			reqCallID:        "call-vo-req-fallback",
			omitKey:          true,
			initialStatus:    "pending",
			direction:        "outbound",
			fromNumber:       "+15105550100",
			toNumber:         "+15105550199",
			geminiCode:       http.StatusOK,
			geminiResp:       `{"candidates":[{"content":{"parts":[{"text":"Summary generated."}]}}]}`,
			expectDelivered:  true,
			expectWithNumber: "+15105550199",
		},
		{
			name:             "successful delivery inbound call with recording attachment",
			callID:           "call-vo-2",
			initialStatus:    "pending",
			direction:        "inbound",
			fromNumber:       "+15105550222",
			toNumber:         "+15105550111",
			geminiCode:       http.StatusOK,
			geminiResp:       `{"candidates":[{"content":{"parts":[{"text":"Inbound reservation completed."}]}}]}`,
			withWAV:          true,
			expectDelivered:  true,
			expectWithNumber: "+15105550222",
			expectFilename:   "call-vo-2.wav",
		},
		{
			name:            "already delivered short-circuits without error",
			callID:          "call-vo-3",
			initialStatus:   "delivered",
			expectDelivered: true,
		},
		{
			name:            "concurrently marked delivered during step 1 reload short-circuits",
			callID:          "call-vo-concurrent-delivered",
			initialStatus:   "delivered",
			failTransitions: []string{"processing"},
			expectDelivered: true,
		},
		{
			name:             "flush live transcript error is logged without failing delivery",
			callID:           "call-vo-flush-err",
			initialStatus:    "pending",
			direction:        "outbound",
			fromNumber:       "+15105550100",
			toNumber:         "+15105550199",
			flushErr:         errors.New("advance failed"),
			geminiCode:       http.StatusOK,
			geminiResp:       `{"candidates":[{"content":{"parts":[{"text":"Summary."}]}}]}`,
			expectDelivered:  true,
			expectWithNumber: "+15105550199",
		},
		{
			name:             "crash replay re-enters from processing status",
			callID:           "call-vo-4",
			initialStatus:    "processing",
			direction:        "outbound",
			fromNumber:       "+15105550100",
			toNumber:         "+15105550199",
			geminiCode:       http.StatusOK,
			geminiResp:       `{"candidates":[{"content":{"parts":[{"text":"Replayed call report delivered."}]}}]}`,
			expectDelivered:  true,
			expectWithNumber: "+15105550199",
		},
		{
			name:             "retry after failed status succeeds",
			callID:           "call-vo-5",
			initialStatus:    "failed",
			direction:        "outbound",
			fromNumber:       "+15105550100",
			toNumber:         "+15105550199",
			geminiCode:       http.StatusOK,
			geminiResp:       `{"candidates":[{"content":{"parts":[{"text":"Retry report delivered."}]}}]}`,
			expectDelivered:  true,
			expectWithNumber: "+15105550199",
		},
		{
			name:             "gemini summary fallback on error",
			callID:           "call-vo-6",
			initialStatus:    "pending",
			direction:        "outbound",
			fromNumber:       "+15105550100",
			toNumber:         "+15105550199",
			geminiCode:       http.StatusInternalServerError,
			geminiResp:       "internal gemini error",
			expectDelivered:  true,
			expectWithNumber: "+15105550199",
		},
	}
}

func deliverReportErrorCases() []deliverReportTestCase {
	return []deliverReportTestCase{
		{
			name:            "call not found returns 404 terminal error",
			callID:          "call-vo-missing",
			nonexistentCall: true,
			expectErrorCode: http.StatusNotFound,
			expectErrSub:    "not found",
		},
		{
			name:            "call not found with store.ErrCallNotFound returns 404 terminal error",
			callID:          "call-vo-missing-err",
			storeOpErr:      store.ErrCallNotFound,
			expectErrorCode: http.StatusNotFound,
			expectErrSub:    "call not found",
		},
		{
			name:            "unmatched status awaiting_recording reload short-circuits",
			callID:          "call-vo-awaiting-rec",
			initialStatus:   "awaiting_recording",
			expectDelivered: false,
		},
		{
			name:         "store get error propagates non-terminal error",
			callID:       "call-vo-get-err",
			storeOpErr:   errors.New("db get error"),
			expectErrSub: "db get error",
		},
		{
			name:         "nil store returns terminal error",
			callID:       "call-vo-nilstore",
			nilStore:     true,
			expectErrSub: "reporter: store is nil",
		},
		{
			name:          "store transition to processing error propagates error",
			callID:        "call-vo-trans-err",
			initialStatus: "pending",
			storeTransErr: errors.New("transition failed"),
			expectErrSub:  "transition failed",
		},
		{
			name:          "discord post failure transitions to failed and returns error",
			callID:        "call-vo-7",
			initialStatus: "pending",
			direction:     "outbound",
			fromNumber:    "+15105550100",
			toNumber:      "+15105550199",
			geminiCode:    http.StatusOK,
			geminiResp:    `{"candidates":[{"content":{"parts":[{"text":"Summary generated."}]}}]}`,
			discordCode:   http.StatusInternalServerError,
			expectErrSub:  "discord returned status 500",
			expectFailed:  true,
		},
		{
			name:            "discord post failure with failed transition error still returns error",
			callID:          "call-vo-post-fail-trans-fail",
			initialStatus:   "pending",
			direction:       "outbound",
			fromNumber:      "+15105550100",
			toNumber:        "+15105550199",
			geminiCode:      http.StatusOK,
			geminiResp:      `{"candidates":[{"content":{"parts":[{"text":"Summary generated."}]}}]}`,
			discordCode:     http.StatusInternalServerError,
			failTransitions: []string{"failed"},
			expectErrSub:    "discord returned status 500",
		},
		{
			name:            "transition to delivered error propagates error",
			callID:          "call-vo-8",
			initialStatus:   "pending",
			direction:       "outbound",
			fromNumber:      "+15105550100",
			toNumber:        "+15105550199",
			geminiCode:      http.StatusOK,
			geminiResp:      `{"candidates":[{"content":{"parts":[{"text":"Summary generated."}]}}]}`,
			failTransitions: []string{"delivered"},
			expectErrSub:    "transition to delivered failed",
		},
	}
}

func deliverReportTestCases() []deliverReportTestCase {
	return append(deliverReportSuccessCases(), deliverReportErrorCases()...)
}

func TestProsperaReport_DeliverReport(t *testing.T) {
	t.Parallel()

	tests := deliverReportTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDeliverReportCase(t, tc)
		})
	}
}

func runDeliverReportCase(t *testing.T, tc deliverReportTestCase) {
	t.Helper()

	var deliveredMu sync.Mutex
	var deliveredDiscordMsg string

	geminiCode := tc.geminiCode
	if geminiCode == 0 {
		geminiCode = http.StatusOK
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "generateContent") {
			w.WriteHeader(geminiCode)
			_, _ = w.Write([]byte(tc.geminiResp))
			return
		}
		if tc.discordCode != 0 {
			w.WriteHeader(tc.discordCode)
			return
		}
		bodyBytes, _ := io.ReadAll(r.Body)
		deliveredMu.Lock()
		deliveredDiscordMsg = string(bodyBytes)
		deliveredMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var st *fakes.FakeStore
	if !tc.nilStore {
		st = fakes.NewFakeStore()
		st.OpErr = tc.storeOpErr
		st.TransErr = tc.storeTransErr
		st.AdvanceErr = tc.flushErr
		fakes.SeedFailTransitions(st, tc.failTransitions)
		if !tc.nonexistentCall {
			call := model.VoiceCall{
				CallID:       tc.callID,
				ReportStatus: tc.initialStatus,
				Direction:    tc.direction,
				FromNumber:   tc.fromNumber,
				ToNumber:     tc.toNumber,
				Transcript: []model.TranscriptTurn{
					{Role: "user", Text: "Hello"},
					{Role: "model", Text: "Hi there"},
				},
			}
			_ = st.CreateOutboundCall(context.Background(), call, 5)
		}
	}

	cfg := &config.Config{
		BrainURL:              srv.URL,
		GeminiAPIKey:          "test-key",
		GeminiFallbackModel:   "gemini-2.5-flash",
		DefaultDiscordChannel: "prospera",
	}

	opts := []ReporterOption{
		WithReporterHTTPClient(srv.Client()),
		WithReporterGeminiBaseURL(srv.URL),
	}
	if tc.withWAV {
		opts = append(opts, WithReporterRecordingProvider(func(callID string) []byte {
			return audio.EncodePCM16WAV([]int16{100, -100, 200}, 8000)
		}))
	}

	var reporterStore store.Store
	if st != nil {
		reporterStore = st
	}
	reporter := NewReporter(fakes.StaticConfig{Cfg: cfg}, reporterStore, &fakes.FakeTwilio{}, opts...)
	obj := NewProsperaReport(reporter)

	mc := mocks.NewMockContext(t)
	if tc.omitKey {
		mc.EXPECT().Key().Return("").Maybe()
	} else {
		mc.EXPECT().Key().Return(tc.callID).Maybe()
	}
	mc.EXPECT().ClearAll().Return().Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext).Maybe()

	err := obj.DeliverReport(restate.WithMockContext(mc), DeliverReportRequest{CallID: tc.reqCallID})
	if tc.expectErrSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.expectErrSub)
		if tc.expectErrorCode != 0 {
			var term restate.TerminalError
			if assert.True(t, errors.As(err, &term), "expected terminal error") {
				assert.Equal(t, tc.expectErrorCode, term.Code())
			}
		}
		if tc.expectFailed && st != nil {
			updated, _ := st.GetCall(context.Background(), tc.callID)
			assert.Equal(t, "failed", updated.ReportStatus)
		}
		return
	}

	require.NoError(t, err)

	if st != nil && !tc.nonexistentCall {
		updated, _ := st.GetCall(context.Background(), tc.callID)
		if tc.expectDelivered {
			assert.Equal(t, "delivered", updated.ReportStatus)
		}
	}

	deliveredMu.Lock()
	msg := deliveredDiscordMsg
	deliveredMu.Unlock()

	if tc.expectWithNumber != "" {
		assert.Contains(t, msg, tc.expectWithNumber)
	}
	if tc.expectFilename != "" {
		assert.Contains(t, msg, tc.expectFilename)
	}
}

func TestProsperaReport_Definition(t *testing.T) {
	t.Parallel()
	reporter := NewReporter(fakes.StaticConfig{Cfg: &config.Config{}}, nil, nil)
	obj := NewProsperaReport(reporter)
	def := obj.Definition()
	if def.Name() != "ProsperaReport" {
		t.Errorf("def.Name() = %q, want ProsperaReport", def.Name())
	}
}
