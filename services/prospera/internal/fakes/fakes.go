// Package fakes holds prospera's shared test doubles: an in-memory store, a
// recording twilio client, a static config source, a Twilio webhook signer and
// tracing helpers. It is
// imported only by _test.go files.
package fakes

import (
	"context"
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // G505: Twilio signs X-Twilio-Signature with HMAC-SHA1; the protocol fixes the hash
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

// StaticConfig is a fixed config source.
type StaticConfig struct {
	Cfg *config.Config
}

// Current returns the fixed config.
func (s StaticConfig) Current() *config.Config { return s.Cfg }

// SeedFailTransitions makes TransitionReportStatus fail for each target status.
func SeedFailTransitions(store *FakeStore, toStatuses []string) {
	if len(toStatuses) == 0 {
		return
	}
	store.FailTrans = make(map[string]bool)
	for _, toStatus := range toStatuses {
		store.FailTrans[toStatus] = true
	}
}

// TwilioSignature returns the X-Twilio-Signature for a POST to fullURL with form:
// base64(HMAC-SHA1(authToken, fullURL + each key in sorted order followed by each of its values)).
func TwilioSignature(authToken, fullURL string, form url.Values) string {
	keys := make([]string, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(fullURL)
	for _, k := range keys {
		for _, v := range form[k] {
			sb.WriteString(k)
			sb.WriteString(v)
		}
	}
	mac := hmac.New(sha1.New, []byte(authToken))
	mac.Write([]byte(sb.String()))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// InstallTestTracing installs an SDK tracer provider and the W3C trace-context
// propagator as the otel globals; call it from TestMain.
func InstallTestTracing() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

// TraceCaller returns a context carrying a sampled caller span (or a bare context) and the
// caller's trace id ("" without a span).
func TraceCaller(withParent bool) (context.Context, string) {
	if !withParent {
		return context.Background(), ""
	}
	ctx, span := otel.Tracer("test").Start(context.Background(), "caller")
	span.End()
	return ctx, span.SpanContext().TraceID().String()
}

// HeaderTraceID returns the trace id of a W3C traceparent header, or "" when absent or invalid.
func HeaderTraceID(h http.Header) string {
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(h)))
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// FakeStore is an in-memory store.Store. The exported error fields inject
// failures; FailTrans fails TransitionReportStatus per target status.
type FakeStore struct {
	mu             sync.Mutex
	calls          map[string]*model.VoiceCall
	messages       map[string]*model.VoiceMessage
	advances       int
	statusTrans    []string
	vmRecordings   map[string]string
	OpErr          error
	BindErr        error
	UpdateStateErr error
	TransErr       error
	AdvanceErr     error
	VMRecErr       error
	MarkErr        error
	SaveErr        error
	FailTrans      map[string]bool
}

// NewFakeStore returns an empty FakeStore.
func NewFakeStore() *FakeStore {
	return &FakeStore{
		calls:        make(map[string]*model.VoiceCall),
		messages:     make(map[string]*model.VoiceMessage),
		vmRecordings: make(map[string]string),
	}
}

func (m *FakeStore) CreateOutboundCall(ctx context.Context, call model.VoiceCall, maxConcurrent int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpErr != nil {
		return m.OpErr
	}
	m.calls[call.CallID] = &call
	return nil
}

func (m *FakeStore) CreateInboundCall(ctx context.Context, call model.VoiceCall) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[call.CallID] = &call
	return nil
}

func (m *FakeStore) BindProviderCallSID(ctx context.Context, callID, providerSID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.BindErr != nil {
		return m.BindErr
	}
	if c, ok := m.calls[callID]; ok {
		c.ProviderCallSID = providerSID
	}
	return nil
}

func (m *FakeStore) GetCall(ctx context.Context, callID string) (*model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpErr != nil {
		return nil, m.OpErr
	}
	c, ok := m.calls[callID]
	if !ok {
		return nil, nil //nolint:nilnil // store contract: an unknown call is (nil, nil)
	}
	cpy := *c
	return &cpy, nil
}

func (m *FakeStore) GetCallByProviderSID(ctx context.Context, providerSID string) (*model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpErr != nil {
		return nil, m.OpErr
	}
	for _, c := range m.calls {
		if c.ProviderCallSID == providerSID {
			cpy := *c
			return &cpy, nil
		}
	}
	return nil, nil //nolint:nilnil // store contract: no matching call is (nil, nil)
}

func (m *FakeStore) ListCalls(ctx context.Context, limit int) ([]model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpErr != nil {
		return nil, m.OpErr
	}
	var list []model.VoiceCall
	for _, c := range m.calls {
		list = append(list, *c)
	}
	return list, nil
}

func (m *FakeStore) FindRecentOutboundCall(ctx context.Context, fromE164 string, window time.Duration) (*model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.Direction == "outbound" && c.ToNumber == fromE164 {
			cpy := *c
			return &cpy, nil
		}
	}
	return nil, nil //nolint:nilnil // store contract: no recent outbound call is (nil, nil)
}

func (m *FakeStore) UpdateCallState(ctx context.Context, callID, state, endReason, answeredBy string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.UpdateStateErr != nil {
		return m.UpdateStateErr
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

func (m *FakeStore) AppendTranscriptTurn(ctx context.Context, callID string, turn model.TranscriptTurn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.calls[callID]; ok {
		c.Transcript = append(c.Transcript, turn)
	}
	return nil
}

func (m *FakeStore) AdvanceLiveTranscriptCursor(ctx context.Context, callID string, fromCursor, toCursor int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.AdvanceErr != nil {
		return false, m.AdvanceErr
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

func (m *FakeStore) AppendOwnerInstruction(ctx context.Context, callID string, instr model.OwnerInstruction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.calls[callID]; ok {
		c.OwnerInstructions = append(c.OwnerInstructions, instr)
	}
	return nil
}

func (m *FakeStore) SetVoicemailRecording(ctx context.Context, callID, recordingURL string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.VMRecErr != nil {
		return m.VMRecErr
	}
	m.vmRecordings[callID] = recordingURL
	if c, ok := m.calls[callID]; ok {
		c.VoicemailRecordingURL = recordingURL
	}
	return nil
}

func (m *FakeStore) TransitionReportStatus(ctx context.Context, callID string, allowedFrom []string, toStatus, summary string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.TransErr != nil {
		return false, m.TransErr
	}
	if m.FailTrans[toStatus] {
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

func (*FakeStore) ReconcileOrphanedCalls(ctx context.Context, checkActive func(ctx context.Context, providerSID string) (bool, error)) (int, error) {
	return 0, nil
}

func (m *FakeStore) ListUnfinishedReportCalls(ctx context.Context, limit int) ([]model.VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpErr != nil {
		return nil, m.OpErr
	}
	var list []model.VoiceCall
	for _, c := range m.calls {
		if (c.ReportStatus == "pending" || c.ReportStatus == "processing") && c.EndedAt != nil {
			list = append(list, *c)
		}
	}
	return list, nil
}

func (m *FakeStore) SaveVoiceMessage(ctx context.Context, msg model.VoiceMessage) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SaveErr != nil {
		return false, m.SaveErr
	}
	m.messages[msg.MessageSID] = &msg
	return true, nil
}

func (m *FakeStore) MarkVoiceMessageDelivered(ctx context.Context, messageSID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.MarkErr != nil {
		return m.MarkErr
	}
	if msg, ok := m.messages[messageSID]; ok {
		msg.DiscordDelivered = true
	}
	return nil
}

func (m *FakeStore) ListVoiceMessages(ctx context.Context, limit int) ([]model.VoiceMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpErr != nil {
		return nil, m.OpErr
	}
	var list []model.VoiceMessage
	for _, msg := range m.messages {
		list = append(list, *msg)
	}
	return list, nil
}

func (*FakeStore) Close() error {
	return nil
}

// FakeTwilio is a recording twilio client. The exported error fields inject
// failures; Last* fields hold the latest request and HungUpCallSIDs,
// CreatedCalls and SMSList ("to:body") record every call.
type FakeTwilio struct {
	mu                 sync.Mutex
	AudioData          []byte
	ContentType        string
	DownloadErr        error
	CreateCallErr      error
	SendSMSErr         error
	EnsurePhoneErr     error
	HangupErr          error
	LastWebhookBaseURL string
	LastCreateCallReq  twilio.CreateCallRequest
	LastSendSMSFrom    string
	LastSendSMSTo      string
	HungUpCallSIDs     []string
	CreatedCalls       []twilio.CreateCallRequest
	SMSList            []string
}

func (m *FakeTwilio) CreateCall(ctx context.Context, req twilio.CreateCallRequest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastCreateCallReq = req
	m.CreatedCalls = append(m.CreatedCalls, req)
	if m.CreateCallErr != nil {
		return "", m.CreateCallErr
	}
	return "CA12345", nil
}

func (m *FakeTwilio) HangupCall(ctx context.Context, providerCallSID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.HungUpCallSIDs = append(m.HungUpCallSIDs, providerCallSID)
	return m.HangupErr
}

func (*FakeTwilio) IsCallActive(ctx context.Context, providerCallSID string) (bool, error) {
	return true, nil
}

func (m *FakeTwilio) SendSMS(ctx context.Context, to, from, body string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastSendSMSTo = to
	m.LastSendSMSFrom = from
	m.SMSList = append(m.SMSList, to+":"+body)
	if m.SendSMSErr != nil {
		return "", m.SendSMSErr
	}
	return "SM12345", nil
}

func (m *FakeTwilio) EnsurePhoneNumber(ctx context.Context, areaCode, webhookBaseURL string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastWebhookBaseURL = webhookBaseURL
	if m.EnsurePhoneErr != nil {
		return "", m.EnsurePhoneErr
	}
	return "+15105550100", nil
}

func (m *FakeTwilio) DownloadRecording(ctx context.Context, recordingURL string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.AudioData, m.ContentType, m.DownloadErr
}
