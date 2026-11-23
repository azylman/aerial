package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

// attemptAborted is raised when a Run step returns a retryable error: the real
// SDK aborts the attempt and Restate retries the invocation, replaying the
// journal of completed steps.
type attemptAborted struct{ err error }

// fakeJournal emulates Restate's journal across invocation attempts. Completed
// steps replay their recorded output by position; a terminal error fails the
// step; a retryable error aborts the attempt.
type fakeJournal struct {
	entries [][]byte
}

func (j *fakeJournal) context(t *testing.T, key string) restate.ObjectContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Key().Return(key).Maybe()
	mc.EXPECT().Log().Return(slog.Default()).Maybe()
	pos := 0
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
			idx := pos
			pos++
			if idx < len(j.entries) {
				if err := json.Unmarshal(j.entries[idx], output); err != nil {
					t.Fatalf("replay step %d: %v", idx, err)
				}
				return nil
			}
			res, err := fn(mockRunContext{Context: context.Background()})
			if err != nil {
				if term, ok := errors.AsType[restate.TerminalError](err); ok {
					return term
				}
				panic(attemptAborted{err: err})
			}
			raw, err := json.Marshal(res)
			if err != nil {
				t.Fatalf("journal step %d: %v", idx, err)
			}
			j.entries = append(j.entries, raw)
			if err := json.Unmarshal(raw, output); err != nil {
				t.Fatalf("decode step %d: %v", idx, err)
			}
			return nil
		}).Maybe()
	return restate.WithMockContext(mc)
}

// invoke runs one invocation attempt and reports a retryable abort separately
// from the handler's own (terminal or nil) result.
func (j *fakeJournal) invoke(t *testing.T, obj *ProsperaReport, key string, req DeliverReportRequest) (result, retryable error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			aborted, ok := r.(attemptAborted)
			if !ok {
				panic(r)
			}
			retryable = aborted.err
		}
	}()
	return obj.DeliverReport(j.context(t, key), req), nil
}

// discordRecorder is the fake eri /discord/message endpoint plus Gemini.
type discordRecorder struct {
	mu              sync.Mutex
	transcriptCodes []int
	summaryCodes    []int
	geminiDown      bool
	transcriptPosts int
	summaryPosts    []discordMessageRequest
}

func nextCode(codes *[]int) int {
	if len(*codes) == 0 {
		return http.StatusOK
	}
	code := (*codes)[0]
	*codes = (*codes)[1:]
	return code
}

func (d *discordRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "generateContent") {
		if d.geminiDown {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"Gemini summary."}]}}]}`))
		return
	}
	var req discordMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if strings.Contains(req.Content, "Call Summary") {
		d.summaryPosts = append(d.summaryPosts, req)
		w.WriteHeader(nextCode(&d.summaryCodes))
		return
	}
	d.transcriptPosts++
	w.WriteHeader(nextCode(&d.transcriptCodes))
}

type deliverReportTestCase struct {
	name            string
	key             string
	reqCallID       string
	noCall          bool
	nilStore        bool
	initialStatus   string
	direction       string
	cursor          int
	getErr          error
	transErr        error
	advanceErr      error
	failTransitions []string
	transcriptCodes []int
	summaryCodes    []int
	geminiDown      bool
	deleteOnRetry   bool
	retryGetErr     error
	extraTriggers   int
	wantTerminal    restate.Code
	wantRetries     int
	wantSummaries   int
	wantTranscripts int
	wantStatus      string
	wantWith        string
	wantSummaryText string
	wantRecording   bool
}

const maxTestAttempts = 3

func TestProsperaReport_DeliverReport(t *testing.T) {
	t.Parallel()

	tests := []deliverReportTestCase{
		{name: "delivers summary with audio and flushes transcript", key: "c-ok", initialStatus: "pending", direction: "outbound", wantSummaries: 1, wantTranscripts: 1, wantStatus: "delivered", wantWith: "+15105550199"},
		{name: "inbound call names the caller", key: "c-in", initialStatus: "pending", direction: "inbound", wantSummaries: 1, wantTranscripts: 1, wantStatus: "delivered", wantWith: "+15105550100"},
		{name: "falls back to request call id when key is empty", reqCallID: "c-req", initialStatus: "pending", wantSummaries: 1, wantTranscripts: 1, wantStatus: "delivered"},
		{name: "processing row from a crashed attempt is re-claimed", key: "c-proc", initialStatus: "processing", wantSummaries: 1, wantTranscripts: 1, wantStatus: "delivered"},
		{name: "failed row is re-claimed", key: "c-failed", initialStatus: "failed", wantSummaries: 1, wantTranscripts: 1, wantStatus: "delivered"},
		{name: "already flushed transcript posts only the summary", key: "c-flushed", initialStatus: "pending", cursor: 2, wantSummaries: 1, wantStatus: "delivered"},
		{name: "replay after failed post re-posts with audio", key: "c-replay", initialStatus: "pending", summaryCodes: []int{http.StatusBadGateway}, wantRetries: 1, wantSummaries: 2, wantTranscripts: 1, wantStatus: "delivered"},
		{name: "gemini outage posts the fallback summary", key: "c-gem", initialStatus: "pending", geminiDown: true, wantSummaries: 1, wantTranscripts: 1, wantStatus: "delivered", wantSummaryText: "ended with state"},
		{name: "call deleted mid-report is terminal 404", key: "c-gone", initialStatus: "pending", transcriptCodes: []int{http.StatusBadGateway}, deleteOnRetry: true, wantRetries: 1, wantTranscripts: 2, wantTerminal: http.StatusNotFound, wantRecording: true},
		{name: "schema error loading the call mid-report is terminal 400", key: "c-schema", initialStatus: "pending", transcriptCodes: []int{http.StatusBadGateway}, retryGetErr: &pgconn.PgError{Code: "42P01"}, wantRetries: 1, wantTranscripts: 2, wantTerminal: http.StatusBadRequest, wantRecording: true},
		{name: "cursor advance constraint violation is terminal 400", key: "c-adv-23", initialStatus: "pending", advanceErr: &pgconn.PgError{Code: "23514"}, wantTranscripts: 1, wantTerminal: http.StatusBadRequest, wantStatus: "processing", wantRecording: true},
		{name: "second trigger after delivery is a no-op", key: "c-twice", initialStatus: "pending", extraTriggers: 2, wantSummaries: 1, wantTranscripts: 1, wantStatus: "delivered"},
		{name: "already delivered row is skipped", key: "c-done", initialStatus: "delivered", wantStatus: "delivered", wantRecording: true},
		{name: "voicemail-owned row is skipped", key: "c-vm", initialStatus: "awaiting_recording", wantStatus: "awaiting_recording", wantRecording: true},
		{name: "transcript post failure is retried, not swallowed", key: "c-tx-fail", initialStatus: "pending", transcriptCodes: []int{http.StatusBadGateway}, wantRetries: 1, wantSummaries: 1, wantTranscripts: 2, wantStatus: "delivered"},
		{name: "cursor advance failure retries without re-posting transcript", key: "c-adv-fail", initialStatus: "pending", advanceErr: errors.New("conn reset"), wantRetries: maxTestAttempts, wantTranscripts: 1, wantStatus: "processing", wantRecording: true},
		{name: "mark delivered failure retries without re-posting summary", key: "c-mark-fail", initialStatus: "pending", failTransitions: []string{"delivered"}, wantRetries: maxTestAttempts, wantSummaries: 1, wantTranscripts: 1, wantStatus: "processing", wantRecording: true},
		{name: "claim failure is retryable", key: "c-claim-fail", initialStatus: "pending", transErr: errors.New("conn reset"), wantRetries: maxTestAttempts, wantStatus: "pending", wantRecording: true},
		{name: "missing call is terminal 404", key: "c-missing", noCall: true, wantTerminal: http.StatusNotFound, wantRecording: true},
		{name: "store not-found error is terminal 404", key: "c-nf", getErr: store.ErrCallNotFound, wantTerminal: http.StatusNotFound, wantRecording: true},
		{name: "unique violation is terminal 400", key: "c-23505", getErr: &pgconn.PgError{Code: "23505"}, wantTerminal: http.StatusBadRequest, wantRecording: true},
		{name: "invalid text representation is terminal 400", key: "c-22p02", getErr: &pgconn.PgError{Code: "22P02"}, wantTerminal: http.StatusBadRequest, wantRecording: true},
		{name: "undefined table is terminal 400", key: "c-42p01", getErr: &pgconn.PgError{Code: "42P01"}, wantTerminal: http.StatusBadRequest, wantRecording: true},
		{name: "deadlock stays retryable", key: "c-40p01", getErr: &pgconn.PgError{Code: "40P01"}, wantRetries: maxTestAttempts, wantRecording: true},
		{name: "serialization failure stays retryable", key: "c-40001", getErr: &pgconn.PgError{Code: "40001"}, wantRetries: maxTestAttempts, wantRecording: true},
		{name: "connection failure stays retryable", key: "c-08006", getErr: &pgconn.PgError{Code: "08006"}, wantRetries: maxTestAttempts, wantRecording: true},
		{name: "context timeout stays retryable", key: "c-timeout", getErr: context.DeadlineExceeded, wantRetries: maxTestAttempts, wantRecording: true},
		{name: "empty call id is terminal 400", wantTerminal: http.StatusBadRequest, wantRecording: true},
		{name: "nil store is terminal", key: "c-nil", nilStore: true, wantTerminal: http.StatusInternalServerError, wantRecording: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDeliverReportCase(t, tc)
		})
	}
}

func runDeliverReportCase(t *testing.T, tc deliverReportTestCase) {
	t.Helper()
	callID := tc.key
	if callID == "" {
		callID = tc.reqCallID
	}

	rec := &discordRecorder{transcriptCodes: tc.transcriptCodes, summaryCodes: tc.summaryCodes, geminiDown: tc.geminiDown}
	srv := httptest.NewServer(rec)
	defer srv.Close()

	st := fakes.NewFakeStore()
	if !tc.noCall && callID != "" {
		require.NoError(t, st.CreateOutboundCall(context.Background(), model.VoiceCall{
			CallID:               callID,
			ReportStatus:         tc.initialStatus,
			Direction:            tc.direction,
			FromNumber:           "+15105550100",
			ToNumber:             "+15105550199",
			LiveTranscriptCursor: tc.cursor,
			Transcript: []model.TranscriptTurn{
				{Role: "user", Text: "Hello"},
				{Role: "model", Text: "Hi there"},
			},
		}, 5))
	}
	st.OpErr = tc.getErr
	st.TransErr = tc.transErr
	st.AdvanceErr = tc.advanceErr
	fakes.SeedFailTransitions(st, tc.failTransitions)

	var recMu sync.Mutex
	recordings := map[string][]byte{callID: []byte("RIFF-wav")}
	opts := []ReporterOption{
		WithReporterHTTPClient(srv.Client()),
		WithReporterGeminiBaseURL(srv.URL),
		WithReporterRecordingProvider(func(id string) []byte {
			recMu.Lock()
			defer recMu.Unlock()
			return recordings[id]
		}),
		WithReporterRecordingDeleter(func(id string) {
			recMu.Lock()
			defer recMu.Unlock()
			delete(recordings, id)
		}),
	}
	var reporterStore store.Store = st
	if tc.nilStore {
		reporterStore = nil
	}
	cfg := &config.Config{BrainURL: srv.URL, GeminiAPIKey: "k", GeminiFallbackModel: "gemini-2.5-flash", DefaultDiscordChannel: "prospera"}
	obj := NewProsperaReport(NewReporter(fakes.StaticConfig{Cfg: cfg}, reporterStore, &fakes.FakeTwilio{}, opts...))
	req := DeliverReportRequest{CallID: tc.reqCallID}

	journal := &fakeJournal{}
	retries := 0
	var final error
	for range maxTestAttempts {
		res, retryable := journal.invoke(t, obj, tc.key, req)
		if retryable != nil {
			assert.False(t, restate.IsTerminalError(retryable), "aborted attempt must carry a retryable error")
			retries++
			if tc.deleteOnRetry {
				st.DeleteCall(callID)
			}
			if tc.retryGetErr != nil {
				st.OpErr = tc.retryGetErr
			}
			continue
		}
		final = res
		break
	}
	for range tc.extraTriggers {
		res, retryable := (&fakeJournal{}).invoke(t, obj, tc.key, req)
		require.NoError(t, retryable)
		require.NoError(t, res)
	}

	assert.Equal(t, tc.wantRetries, retries, "retryable attempts")
	if tc.wantTerminal != 0 {
		term, ok := errors.AsType[restate.TerminalError](final)
		require.True(t, ok, "want terminal error, got %v", final)
		assert.Equal(t, tc.wantTerminal, term.Code())
	} else {
		assert.NoError(t, final)
	}

	rec.mu.Lock()
	summaries, transcripts := rec.summaryPosts, rec.transcriptPosts
	rec.mu.Unlock()
	assert.Len(t, summaries, tc.wantSummaries, "summary posts")
	assert.Equal(t, tc.wantTranscripts, transcripts, "transcript posts")
	for _, s := range summaries {
		if assert.Len(t, s.Attachments, 1, "every summary post carries audio") {
			assert.Equal(t, callID+".wav", s.Attachments[0].Filename)
		}
		assert.Contains(t, s.Content, tc.wantWith)
		assert.Contains(t, s.Content, tc.wantSummaryText)
	}

	if tc.wantStatus != "" {
		st.OpErr = nil
		got, err := st.GetCall(context.Background(), callID)
		require.NoError(t, err)
		assert.Equal(t, tc.wantStatus, got.ReportStatus)
	}
	recMu.Lock()
	_, kept := recordings[callID]
	recMu.Unlock()
	assert.Equal(t, tc.wantRecording, kept, "recording kept")
}

func TestProsperaReport_Definition(t *testing.T) {
	t.Parallel()
	def := NewProsperaReport(nil).Definition()
	assert.Equal(t, "ProsperaReport", def.Name())
}

type triggerReportTestCase struct {
	name       string
	callID     string
	noIngress  bool
	ingressRC  int
	wantErr    bool
	wantPath   string
	wantKey    string
	wantCallID string
}

func TestReporter_TriggerReport(t *testing.T) {
	t.Parallel()

	tests := []triggerReportTestCase{
		{name: "sends to the call's object with the call's idempotency key", callID: " CA-1 ", ingressRC: http.StatusAccepted, wantPath: "/ProsperaReport/CA-1/DeliverReport/send", wantKey: "prospera:report:CA-1", wantCallID: "CA-1"},
		{name: "ingress failure is returned", callID: "CA-2", ingressRC: http.StatusServiceUnavailable, wantErr: true, wantPath: "/ProsperaReport/CA-2/DeliverReport/send", wantKey: "prospera:report:CA-2", wantCallID: "CA-2"},
		{name: "empty call id is rejected", callID: "  ", wantErr: true},
		{name: "missing ingress url is rejected", callID: "CA-3", noIngress: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var gotPath, gotKey string
			var gotReq DeliverReportRequest
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				gotPath, gotKey = r.URL.Path, r.Header.Get("Idempotency-Key")
				_ = json.NewDecoder(r.Body).Decode(&gotReq)
				w.WriteHeader(tc.ingressRC)
			}))
			defer srv.Close()
			cfg := &config.Config{RestateIngressURL: srv.URL + "/"}
			if tc.noIngress {
				cfg.RestateIngressURL = ""
			}
			r := NewReporter(fakes.StaticConfig{Cfg: cfg}, fakes.NewFakeStore(), &fakes.FakeTwilio{}, WithReporterHTTPClient(srv.Client()))
			err := r.TriggerReport(context.Background(), tc.callID)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tc.wantPath, gotPath)
			assert.Equal(t, tc.wantKey, gotKey)
			assert.Equal(t, tc.wantCallID, gotReq.CallID)
		})
	}
}
