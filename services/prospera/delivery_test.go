package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

type mockStoreForDelivery struct {
	mu             sync.Mutex
	calls          map[string]*VoiceCall
	messages       map[string]*VoiceMessage
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
		calls:        make(map[string]*VoiceCall),
		messages:     make(map[string]*VoiceMessage),
		vmRecordings: make(map[string]string),
	}
}

func (m *mockStoreForDelivery) CreateOutboundCall(ctx context.Context, call VoiceCall, maxConcurrent int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return m.opErr
	}
	m.calls[call.CallID] = &call
	return nil
}

func (m *mockStoreForDelivery) CreateInboundCall(ctx context.Context, call VoiceCall) error {
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

func (m *mockStoreForDelivery) GetCall(ctx context.Context, callID string) (*VoiceCall, error) {
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

func (m *mockStoreForDelivery) GetCallByProviderSID(ctx context.Context, providerSID string) (*VoiceCall, error) {
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

func (m *mockStoreForDelivery) ListCalls(ctx context.Context, limit int) ([]VoiceCall, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return nil, m.opErr
	}
	var list []VoiceCall
	for _, c := range m.calls {
		list = append(list, *c)
	}
	return list, nil
}

func (m *mockStoreForDelivery) FindRecentOutboundCall(ctx context.Context, fromE164 string, window time.Duration) (*VoiceCall, error) {
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

func (m *mockStoreForDelivery) AppendTranscriptTurn(ctx context.Context, callID string, turn TranscriptTurn) error {
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

func (m *mockStoreForDelivery) AppendOwnerInstruction(ctx context.Context, callID string, instr OwnerInstruction) error {
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

func (m *mockStoreForDelivery) SaveVoiceMessage(ctx context.Context, msg VoiceMessage) (bool, error) {
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

func (m *mockStoreForDelivery) ListVoiceMessages(ctx context.Context, limit int) ([]VoiceMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opErr != nil {
		return nil, m.opErr
	}
	var list []VoiceMessage
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
	lastCreateCallReq  CreateCallRequest
	lastSendSMSFrom    string
	lastSendSMSTo      string
}

func (m *mockTwilioForDelivery) CreateCall(ctx context.Context, req CreateCallRequest) (string, error) {
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
