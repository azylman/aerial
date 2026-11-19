package mediastream

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/gemini"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

// Session is the live audio bridge managed by CallSessionManager.
// The concrete bridge implementation lives in the parent package until the
// session-bridge chunk moves it here; the manager only drives this contract.
type Session interface {
	ProviderCallSID() string
	RecordingWAV() []byte
	Steer(message, mode string) error
	HandleAMD(answeredBy string)
	InitiateHangup(reason string)
	Run(ctx context.Context)
}

// SessionConfig carries the construction parameters for a manager-owned session.
type SessionConfig struct {
	CallID           string
	Conn             *websocket.Conn
	Config           *config.Config
	Store            store.Store
	TwilioClient     twilio.TwilioClient
	ConsultClient    gemini.ContextConsultClient
	OpeningGreeting  string
	VoicemailMessage string
	SystemPrompt     string
	Language         string
	PacerInterval    time.Duration
	GeminiDialer     func(ctx context.Context, modelName string) (*websocket.Conn, error)
}

// SessionFactory constructs the live session for an accepted media stream.
type SessionFactory func(cfg SessionConfig) Session

// SessionManagerOption configures a CallSessionManager.
type SessionManagerOption func(*CallSessionManager)

// CallSessionManager coordinates active Twilio Media Stream <-> Gemini Live sessions.
type CallSessionManager struct {
	mu             sync.RWMutex
	cfgSrc         config.ConfigSource
	store          store.Store
	twilioClient   twilio.TwilioClient
	consultClient  gemini.ContextConsultClient
	tokenMgr       *twilio.StreamTokenManager
	pacerInterval  time.Duration
	geminiDialer   func(ctx context.Context, modelName string) (*websocket.Conn, error)
	newSession     SessionFactory
	sessions       map[string]Session
	sessionWaiters map[string][]chan Session
	recordings     map[string][]byte
	upgrader       websocket.Upgrader
}

// NewCallSessionManager initializes a CallSessionManager.
func NewCallSessionManager(
	cfgSrc config.ConfigSource,
	st store.Store,
	twilioClient twilio.TwilioClient,
	consultClient gemini.ContextConsultClient,
	tokenMgr *twilio.StreamTokenManager,
	opts ...SessionManagerOption,
) *CallSessionManager {
	if tokenMgr == nil {
		tokenMgr = twilio.NewStreamTokenManager(time.Now)
	}

	m := &CallSessionManager{
		cfgSrc:         cfgSrc,
		store:          st,
		twilioClient:   twilioClient,
		consultClient:  consultClient,
		tokenMgr:       tokenMgr,
		pacerInterval:  20 * time.Millisecond,
		sessions:       make(map[string]Session),
		sessionWaiters: make(map[string][]chan Session),
		recordings:     make(map[string][]byte),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}

	for _, opt := range opts {
		opt(m)
	}
	return m
}

// WithPacerInterval overrides the outbound audio pacing interval.
func WithPacerInterval(d time.Duration) SessionManagerOption {
	return func(m *CallSessionManager) {
		m.pacerInterval = d
	}
}

// WithGeminiDialer overrides the Gemini Live dialer.
func WithGeminiDialer(dialer func(ctx context.Context, modelName string) (*websocket.Conn, error)) SessionManagerOption {
	return func(m *CallSessionManager) {
		m.geminiDialer = dialer
	}
}

// WithSessionFactory sets the factory used to construct live sessions for
// accepted media streams. It must be set before serving HandleMediaWebSocket.
func WithSessionFactory(factory SessionFactory) SessionManagerOption {
	return func(m *CallSessionManager) {
		m.newSession = factory
	}
}

// GetSession retrieves an active session by callID.
func (m *CallSessionManager) GetSession(callID string) (Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, ok := m.sessions[callID]
	return sess, ok
}

// GetSessionByProviderSID searches for an active session with a matching providerCallSID.
func (m *CallSessionManager) GetSessionByProviderSID(providerCallSID string) (Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sess := range m.sessions {
		if sess.ProviderCallSID() == providerCallSID {
			return sess, true
		}
	}
	return nil, false
}

// ActiveCallIDs returns the IDs of currently active sessions.
func (m *CallSessionManager) ActiveCallIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	callIDs := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		callIDs = append(callIDs, id)
	}
	return callIDs
}

// ActiveCount returns the number of currently active sessions.
func (m *CallSessionManager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// RegisterSession tracks an active session by callID.
func (m *CallSessionManager) RegisterSession(callID string, sess Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[callID] = sess
	for _, ch := range m.sessionWaiters[callID] {
		select {
		case ch <- sess:
		default:
		}
	}
	delete(m.sessionWaiters, callID)
}

// DeregisterSession removes a session, snapshotting any recording first.
func (m *CallSessionManager) DeregisterSession(callID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok := m.sessions[callID]; ok && sess != nil {
		if wav := sess.RecordingWAV(); len(wav) > 0 {
			if len(m.recordings) >= 32 {
				for k := range m.recordings {
					delete(m.recordings, k)
					break
				}
			}
			m.recordings[callID] = wav
		}
	}
	delete(m.sessions, callID)
}

// WaitForSession blocks until the session for callID registers or ctx ends.
func (m *CallSessionManager) WaitForSession(ctx context.Context, callID string) (Session, error) {
	m.mu.Lock()
	if sess, ok := m.sessions[callID]; ok {
		m.mu.Unlock()
		return sess, nil
	}
	ch := make(chan Session, 1)
	m.sessionWaiters[callID] = append(m.sessionWaiters[callID], ch)
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		m.mu.Lock()
		waiters := m.sessionWaiters[callID]
		for i, w := range waiters {
			if w == ch {
				m.sessionWaiters[callID] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(m.sessionWaiters[callID]) == 0 {
			delete(m.sessionWaiters, callID)
		}
		m.mu.Unlock()
		return nil, ctx.Err()
	case sess := <-ch:
		return sess, nil
	}
}

// GetRecordingWAV returns and consumes the recorded WAV payload for callID, or snapshots an active session.
func (m *CallSessionManager) GetRecordingWAV(callID string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok := m.sessions[callID]; ok && sess != nil {
		if wav := sess.RecordingWAV(); len(wav) > 0 {
			return wav
		}
	}
	if wav, ok := m.recordings[callID]; ok {
		delete(m.recordings, callID)
		return wav
	}
	return nil
}

// SteerCall injects an instruction turn into an active call session.
func (m *CallSessionManager) SteerCall(ctx context.Context, callID, message, mode string) error {
	sess, ok := m.GetSession(callID)
	if !ok {
		return fmt.Errorf("session: call %q is not currently active", callID)
	}

	if m.store != nil {
		if err := m.store.AppendOwnerInstruction(ctx, callID, model.OwnerInstruction{
			Instruction: message,
			Mode:        mode,
			CreatedAt:   time.Now().UTC(),
		}); err != nil {
			slog.ErrorContext(ctx, "prospera: append owner instruction", "error", err)
		}
	}

	return sess.Steer(message, mode)
}

// HandleAMDWebhook handles an inbound AMD webhook by routing to the matching session.
func (m *CallSessionManager) HandleAMDWebhook(ctx context.Context, providerCallSID, answeredBy string) error {
	sess, ok := m.GetSessionByProviderSID(providerCallSID)
	if !ok {
		// Attempt to resolve callID from store
		if m.store != nil {
			call, err := m.store.GetCallByProviderSID(ctx, providerCallSID)
			if err == nil && call != nil {
				sess, ok = m.GetSession(call.CallID)
			}
		}
	}
	if !ok || sess == nil {
		return fmt.Errorf("session: no active session found for provider call sid %q", providerCallSID)
	}
	sess.HandleAMD(answeredBy)
	return nil
}

// EndCall terminates an active call session.
func (m *CallSessionManager) EndCall(callID string) error {
	sess, ok := m.GetSession(callID)
	if !ok {
		return fmt.Errorf("session: call %q not found", callID)
	}
	sess.InitiateHangup("operator_requested")
	return nil
}

// HandleMediaWebSocket upgrades and handles the Twilio Media Stream connection.
func (m *CallSessionManager) HandleMediaWebSocket(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	if token == "" {
		token = r.URL.Query().Get("token")
	}

	if m.newSession == nil {
		http.Error(w, "session factory not configured", http.StatusInternalServerError)
		return
	}

	callID, ok := m.tokenMgr.Consume(token)
	if !ok || callID == "" {
		http.Error(w, "unauthorized stream token", http.StatusUnauthorized)
		return
	}

	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.ErrorContext(r.Context(), "prospera: websocket upgrade failed", "error", err)
		return
	}
	defer func(ctx context.Context) {
		if closeErr := conn.Close(); closeErr != nil {
			slog.WarnContext(ctx, "prospera: connection close err", "error", closeErr)
		}
	}(r.Context())

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var call *model.VoiceCall
	if m.store != nil {
		var getErr error
		call, getErr = m.store.GetCall(ctx, callID)
		if getErr != nil {
			slog.ErrorContext(ctx, "prospera: get call failed", "call_id", callID, "error", getErr)
		}
	}

	var openingGreeting, voicemailMsg, systemPrompt, language string
	if call != nil {
		openingGreeting = call.Brief.Task
		if call.Direction == "inbound" || openingGreeting == "" {
			openingGreeting = "Answer the incoming call with a warm, brief greeting (such as 'Hi, this is Aerial, how can I help you?') and let the caller state why they are calling."
		}
		voicemailMsg = model.ResolveVoicemailMessage(call.Brief, openingGreeting)
		mode := model.ModeConversation
		if call.Brief.Mode == string(model.ModeNotify) {
			mode = model.ModeNotify
		} else if call.Direction == "inbound" || call.CallbackOfCallID != "" {
			mode = model.ModeCallback
		}
		language = call.Brief.Language
		systemPrompt = model.BuildSystemPrompt(call.Brief, mode, nil)
	} else {
		openingGreeting = "Hello, I am calling to help with an inquiry."
		voicemailMsg = openingGreeting
		systemPrompt = "You are Aerial, an assistant making or handling a phone call. Speak like a warm, natural human on a live phone call—use a relaxed conversational cadence, natural contractions, and everyday phrasing rather than stiff or scripted readback. Focus directly on the task without introducing yourself upfront. If asked who is calling, say your name is Aerial and phrase your role naturally for the situation (for example, '<Name>'s assistant' if their name is known); only disclose that you are an AI assistant if explicitly asked."
	}

	sess := m.newSession(SessionConfig{
		CallID:           callID,
		Conn:             conn,
		Config:           m.cfgSrc.Current(),
		Store:            m.store,
		TwilioClient:     m.twilioClient,
		ConsultClient:    m.consultClient,
		OpeningGreeting:  openingGreeting,
		VoicemailMessage: voicemailMsg,
		SystemPrompt:     systemPrompt,
		Language:         language,
		PacerInterval:    m.pacerInterval,
		GeminiDialer:     m.geminiDialer,
	})

	m.RegisterSession(callID, sess)
	defer m.DeregisterSession(callID)

	sess.Run(ctx)
}
