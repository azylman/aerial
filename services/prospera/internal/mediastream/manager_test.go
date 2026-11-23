package mediastream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/testutil"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

type fakeManagedSession struct {
	mu           sync.Mutex
	providerSID  string
	wav          []byte
	steerErr     error
	steeredMsg   string
	steeredMode  string
	amdAnswers   []string
	hangupReason string
	ran          chan struct{}
	runOnce      sync.Once
}

func newFakeManagedSession(providerSID string, wav []byte) *fakeManagedSession {
	return &fakeManagedSession{providerSID: providerSID, wav: wav, ran: make(chan struct{})}
}

func (s *fakeManagedSession) ProviderCallSID() string {
	return s.providerSID
}

func (s *fakeManagedSession) RecordingWAV() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wav
}

func (s *fakeManagedSession) Steer(message, mode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.steeredMsg = message
	s.steeredMode = mode
	return s.steerErr
}

func (s *fakeManagedSession) steered() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.steeredMsg, s.steeredMode
}

func (s *fakeManagedSession) HandleAMD(answeredBy string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.amdAnswers = append(s.amdAnswers, answeredBy)
}

func (s *fakeManagedSession) amdCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.amdAnswers)
}

func (s *fakeManagedSession) InitiateHangup(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hangupReason = reason
}

func (s *fakeManagedSession) hangup() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hangupReason
}

func (s *fakeManagedSession) Run(_ context.Context) {
	s.runOnce.Do(func() { close(s.ran) })
}

func newTestManager() *CallSessionManager {
	return NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, nil, nil, nil, twilio.NewStreamTokenManager(time.Now, nil))
}

func TestCallSessionManager_GetSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		register  bool
		lookup    string
		wantFound bool
	}{
		{name: "miss on empty manager", register: false, lookup: "call-1", wantFound: false},
		{name: "hit after register", register: true, lookup: "call-1", wantFound: true},
		{name: "miss for unknown id", register: true, lookup: "call-other", wantFound: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := newTestManager()
			sess := newFakeManagedSession("CA_1", nil)
			if tc.register {
				mgr.RegisterSession("call-1", sess)
			}
			found, ok := mgr.GetSession(tc.lookup)
			if ok != tc.wantFound {
				t.Fatalf("GetSession(%q) ok = %v, want %v", tc.lookup, ok, tc.wantFound)
			}
			if tc.wantFound && found != Session(sess) {
				t.Fatal("GetSession returned unexpected session")
			}
		})
	}
}

func TestCallSessionManager_GetSessionByProviderSID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lookup    string
		wantFound bool
	}{
		{name: "hit on matching provider sid", lookup: "CA_match", wantFound: true},
		{name: "miss on unknown provider sid", lookup: "CA_unknown", wantFound: false},
		{name: "miss on empty sid", lookup: "", wantFound: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := newTestManager()
			mgr.RegisterSession("call-1", newFakeManagedSession("CA_match", nil))
			found, ok := mgr.GetSessionByProviderSID(tc.lookup)
			if ok != tc.wantFound {
				t.Fatalf("GetSessionByProviderSID(%q) ok = %v, want %v", tc.lookup, ok, tc.wantFound)
			}
			if tc.wantFound && found.ProviderCallSID() != "CA_match" {
				t.Fatalf("ProviderCallSID() = %q, want CA_match", found.ProviderCallSID())
			}
		})
	}
}

func TestCallSessionManager_ActiveIDsAndCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ids       []string
		wantCount int
	}{
		{name: "empty manager", ids: nil, wantCount: 0},
		{name: "single session", ids: []string{"call-a"}, wantCount: 1},
		{name: "two sessions", ids: []string{"call-a", "call-b"}, wantCount: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := newTestManager()
			for _, id := range tc.ids {
				mgr.RegisterSession(id, newFakeManagedSession("", nil))
			}
			if got := mgr.ActiveCount(); got != tc.wantCount {
				t.Fatalf("ActiveCount() = %d, want %d", got, tc.wantCount)
			}
			gotIDs := mgr.ActiveCallIDs()
			if len(gotIDs) != len(tc.ids) {
				t.Fatalf("ActiveCallIDs() = %v, want %d ids", gotIDs, len(tc.ids))
			}
			seen := make(map[string]bool, len(gotIDs))
			for _, id := range gotIDs {
				seen[id] = true
			}
			for _, id := range tc.ids {
				if !seen[id] {
					t.Fatalf("ActiveCallIDs() = %v, missing %q", gotIDs, id)
				}
			}
		})
	}
}

func TestCallSessionManager_Deregister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		register      []string
		deregister    string
		wantRemaining []string
	}{
		{name: "removes registered session", register: []string{"call-a", "call-b"}, deregister: "call-a", wantRemaining: []string{"call-b"}},
		{name: "unknown id is a noop", register: []string{"call-a"}, deregister: "call-ghost", wantRemaining: []string{"call-a"}},
		{name: "nil session skips recording snapshot", register: nil, deregister: "call-nil", wantRemaining: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := newTestManager()
			for _, id := range tc.register {
				mgr.RegisterSession(id, newFakeManagedSession("", nil))
			}
			if tc.register == nil {
				mgr.RegisterSession(tc.deregister, nil)
			}
			mgr.DeregisterSession(tc.deregister)
			if got := mgr.ActiveCount(); got != len(tc.wantRemaining) {
				t.Fatalf("ActiveCount() = %d, want %d", got, len(tc.wantRemaining))
			}
			for _, id := range tc.wantRemaining {
				if _, ok := mgr.GetSession(id); !ok {
					t.Fatalf("GetSession(%q) missed after deregister", id)
				}
			}
			if wav := mgr.GetRecordingWAV(tc.deregister); wav != nil {
				t.Fatalf("GetRecordingWAV(%q) = %d bytes, want nil", tc.deregister, len(wav))
			}
		})
	}
}

func TestCallSessionManager_SteerCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		registered  bool
		withStore   bool
		steerErr    bool
		wantErrSub  string
		wantMessage string
		wantMode    string
	}{
		{name: "missing session returns not-active error", registered: false, withStore: false, steerErr: false, wantErrSub: `session: call "call-steer" is not currently active`},
		{name: "steers without store", registered: true, withStore: false, steerErr: false, wantMessage: "be polite", wantMode: "guidance"},
		{name: "steers with store", registered: true, withStore: true, steerErr: false, wantMessage: "say hello", wantMode: "say"},
		{name: "session steer error propagates", registered: true, withStore: false, steerErr: true, wantErrSub: "bridge offline"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := makeSteerStore(t, tc.withStore)
			mgr := NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, st, nil, nil, twilio.NewStreamTokenManager(time.Now, nil))
			sess := newFakeManagedSession("", nil)
			if tc.steerErr {
				sess.steerErr = errors.New("bridge offline")
			}
			if tc.registered {
				mgr.RegisterSession("call-steer", sess)
			}
			err := mgr.SteerCall(context.Background(), "call-steer", tc.wantMessage, tc.wantMode)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("SteerCall err = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("SteerCall failed: %v", err)
			}
			if gotMsg, gotMode := sess.steered(); gotMsg != tc.wantMessage || gotMode != tc.wantMode {
				t.Fatalf("steered = (%q, %q), want (%q, %q)", gotMsg, gotMode, tc.wantMessage, tc.wantMode)
			}
			if tc.withStore {
				assertSteeredInstruction(t, st, tc.wantMessage, tc.wantMode)
			}
		})
	}
}

func makeSteerStore(t *testing.T, withStore bool) store.Store {
	t.Helper()
	if !withStore {
		return nil
	}
	st := store.Store(testutil.SetUpTestStore(t))
	if err := st.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:     "call-steer",
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}, 5); err != nil {
		t.Fatalf("CreateOutboundCall failed: %v", err)
	}
	return st
}

func assertSteeredInstruction(t *testing.T, st store.Store, wantMessage, wantMode string) {
	t.Helper()
	call, err := st.GetCall(context.Background(), "call-steer")
	if err != nil {
		t.Fatalf("GetCall failed: %v", err)
	}
	if len(call.OwnerInstructions) != 1 {
		t.Fatalf("OwnerInstructions = %d entries, want 1", len(call.OwnerInstructions))
	}
	got := call.OwnerInstructions[0]
	if got.Instruction != wantMessage || got.Mode != wantMode {
		t.Fatalf("persisted instruction = (%q, %q), want (%q, %q)", got.Instruction, got.Mode, wantMessage, wantMode)
	}
}

func TestCallSessionManager_HandleAMDWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		directHit     bool
		storeFallback bool
		storeErr      bool
		withStore     bool
		wantErrSub    string
	}{
		{name: "missing sid without store returns not-found", directHit: false, storeFallback: false, storeErr: false, withStore: false, wantErrSub: `session: no active session found for provider call sid "CA_missing"`},
		{name: "routes to direct session", directHit: true, storeFallback: false, storeErr: false, withStore: false, wantErrSub: ""},
		{name: "resolves via store lookup", directHit: false, storeFallback: true, storeErr: false, withStore: true, wantErrSub: ""},
		{name: "store error returns not-found", directHit: false, storeFallback: false, storeErr: true, withStore: true, wantErrSub: "no active session found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st store.Store
			if tc.withStore {
				st = testutil.SetUpTestStore(t)
			}
			if tc.storeErr {
				st = errStore{Store: st}
			}
			mgr := NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, st, nil, nil, twilio.NewStreamTokenManager(time.Now, nil))
			sid := "CA_missing"
			sess := newFakeManagedSession("CA_direct", nil)
			if tc.directHit {
				sid = "CA_direct"
				mgr.RegisterSession("call-amd", sess)
			}
			if tc.storeFallback {
				sid = "CA_store_prov"
				_ = st.CreateOutboundCall(context.Background(), model.VoiceCall{
					CallID:     "call-amd-store",
					Direction:  "outbound",
					State:      "initiated",
					FromNumber: "+15105550100",
					ToNumber:   "+15105550199",
					StartedAt:  time.Now().UTC(),
					UpdatedAt:  time.Now().UTC(),
				}, 5)
				_ = st.BindProviderCallSID(context.Background(), "call-amd-store", sid)
				mgr.RegisterSession("call-amd-store", sess)
			}
			err := mgr.HandleAMDWebhook(context.Background(), sid, "human")
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("HandleAMDWebhook err = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("HandleAMDWebhook failed: %v", err)
			}
			if sess.amdCount() != 1 {
				t.Fatalf("HandleAMD calls = %d, want 1", sess.amdCount())
			}
		})
	}
}

func TestCallSessionManager_EndCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		registered bool
		wantErrSub string
	}{
		{name: "missing call returns not-found", registered: false, wantErrSub: `session: call "call-end" not found`},
		{name: "hangs up active session", registered: true, wantErrSub: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := newTestManager()
			sess := newFakeManagedSession("", nil)
			if tc.registered {
				mgr.RegisterSession("call-end", sess)
			}
			err := mgr.EndCall("call-end")
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("EndCall err = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("EndCall failed: %v", err)
			}
			if got := sess.hangup(); got != "operator_requested" {
				t.Fatalf("hangup reason = %q, want operator_requested", got)
			}
		})
	}
}

func TestCallSessionManager_GetRecordingWAV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		registered   bool
		haveWAV      bool
		deregistered bool
		deleted      bool
		wantLen      int
		wantNextLen  int
	}{
		{name: "missing call returns nil", wantLen: 0, wantNextLen: 0},
		{name: "active session snapshots wav", registered: true, haveWAV: true, wantLen: 7, wantNextLen: 7},
		{name: "active session without wav returns nil", registered: true, wantLen: 0, wantNextLen: 0},
		{name: "deregistered recording survives repeated reads", registered: true, haveWAV: true, deregistered: true, wantLen: 7, wantNextLen: 7},
		{name: "deleted recording is gone", registered: true, haveWAV: true, deregistered: true, deleted: true, wantLen: 7, wantNextLen: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := newTestManager()
			if tc.registered {
				var wav []byte
				if tc.haveWAV {
					wav = []byte("wavdata")
				}
				mgr.RegisterSession("call-rec", newFakeManagedSession("", wav))
			}
			if tc.deregistered {
				mgr.DeregisterSession("call-rec")
			}
			if got := mgr.GetRecordingWAV("call-rec"); len(got) != tc.wantLen {
				t.Fatalf("GetRecordingWAV len = %d, want %d", len(got), tc.wantLen)
			}
			if tc.deleted {
				mgr.DeleteRecording("call-rec")
			}
			if next := mgr.GetRecordingWAV("call-rec"); len(next) != tc.wantNextLen {
				t.Fatalf("second GetRecordingWAV len = %d, want %d", len(next), tc.wantNextLen)
			}
		})
	}
}

func TestCallSessionManager_RecordingCap(t *testing.T) {
	t.Parallel()

	mgr := newTestManager()
	for i := range 33 {
		id := "call-cap-" + strconv.Itoa(i)
		mgr.RegisterSession(id, newFakeManagedSession("", []byte("wavdata")))
		mgr.DeregisterSession(id)
	}
	mgr.mu.RLock()
	recorded := len(mgr.recordings)
	mgr.mu.RUnlock()
	if recorded > 32 {
		t.Fatalf("stored recordings = %d, want at most 32", recorded)
	}
	if wav := mgr.GetRecordingWAV("call-cap-32"); len(wav) == 0 {
		t.Fatal("latest recording was evicted, want it retained")
	}
}

type errStore struct {
	store.Store
}

func (errStore) GetCall(_ context.Context, _ string) (*model.VoiceCall, error) {
	return nil, errors.New("store unavailable")
}

func (errStore) GetCallByProviderSID(_ context.Context, _ string) (*model.VoiceCall, error) {
	return nil, errors.New("store unavailable")
}

type captureFactory struct {
	mu   sync.Mutex
	cfgs []SessionConfig
	sess *fakeManagedSession
}

func (f *captureFactory) factory(cfg SessionConfig) Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfgs = append(f.cfgs, cfg)
	return f.sess
}

func (f *captureFactory) last() (SessionConfig, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.cfgs) == 0 {
		return SessionConfig{}, false
	}
	return f.cfgs[len(f.cfgs)-1], true
}

func TestCallSessionManager_HandleMediaWebSocketRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{name: "invalid token is unauthorized", token: "invalid", wantStatus: http.StatusUnauthorized},
		{name: "empty token is unauthorized", token: "", wantStatus: http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr := NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, nil, nil, nil, twilio.NewStreamTokenManager(time.Now, nil),
				WithSessionFactory(func(_ SessionConfig) Session { return newFakeManagedSession("", nil) }),
			)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/twilio/media-stream?token="+tc.token, http.NoBody)
			mgr.HandleMediaWebSocket(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestCallSessionManager_HandleMediaWebSocketRequiresFactory(t *testing.T) {
	t.Parallel()

	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
	mgr := NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, nil, nil, nil, tokenMgr)
	validTok, err := tokenMgr.Issue("call-no-factory", 10*time.Second)
	if err != nil {
		t.Fatalf("Issue failed: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/twilio/media-stream?token="+validTok, http.NoBody)
	mgr.HandleMediaWebSocket(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if got := mgr.ActiveCount(); got != 0 {
		t.Fatalf("ActiveCount() = %d, want 0", got)
	}
	if _, ok := tokenMgr.Consume(validTok); !ok {
		t.Fatal("stream token was consumed by the factory-less rejection, want it preserved for retry")
	}
}

type streamServeCase struct {
	name          string
	withStore     bool
	storeErr      bool
	direction     string
	briefMode     string
	briefTask     string
	wantGreetSub  string
	wantPromptSub string
	wantLanguage  string
}

func makeServeStore(t *testing.T, tc streamServeCase, callID string) store.Store {
	t.Helper()
	if !tc.withStore {
		return nil
	}
	st := store.Store(testutil.SetUpTestStore(t))
	if tc.storeErr || tc.direction == "" {
		if tc.storeErr {
			return errStore{Store: st}
		}
		return st
	}
	call := model.VoiceCall{
		CallID:     callID,
		Direction:  tc.direction,
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		Brief: model.CallBrief{
			Mode:     tc.briefMode,
			Task:     tc.briefTask,
			Language: "en-US",
		},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	ctx := context.Background()
	if tc.direction == "inbound" {
		_ = st.CreateInboundCall(ctx, call)
	} else {
		_ = st.CreateOutboundCall(ctx, call, 5)
	}
	return st
}

func serveStreamToClose(t *testing.T, mgr *CallSessionManager, tokenMgr *twilio.StreamTokenManager, callID string) {
	t.Helper()
	token, err := tokenMgr.Issue(callID, 30*time.Second)
	if err != nil {
		t.Fatalf("Issue failed: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mgr.HandleMediaWebSocket(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + token
	twilioWS, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial twilio ws failed: %v", err)
	}
	defer twilioWS.Close()

	_ = twilioWS.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		if _, _, err := twilioWS.ReadMessage(); err != nil {
			break
		}
	}
}

func assertStreamConfig(t *testing.T, fc *captureFactory, callID string, tc streamServeCase) {
	t.Helper()
	gotCfg, ok := fc.last()
	if !ok {
		t.Fatal("factory captured no session config")
	}
	if gotCfg.CallID != callID {
		t.Fatalf("factory CallID = %q, want %q", gotCfg.CallID, callID)
	}
	if gotCfg.PacerInterval != 1*time.Millisecond {
		t.Fatalf("factory PacerInterval = %v, want 1ms", gotCfg.PacerInterval)
	}
	if !strings.Contains(gotCfg.OpeningGreeting, tc.wantGreetSub) {
		t.Fatalf("greeting %q missing %q", gotCfg.OpeningGreeting, tc.wantGreetSub)
	}
	if !strings.Contains(gotCfg.SystemPrompt, tc.wantPromptSub) {
		t.Fatalf("prompt missing %q", tc.wantPromptSub)
	}
	if gotCfg.Language != tc.wantLanguage {
		t.Fatalf("language = %q, want %q", gotCfg.Language, tc.wantLanguage)
	}
}

func TestCallSessionManager_HandleMediaWebSocketServesStream(t *testing.T) {
	t.Parallel()

	tests := []streamServeCase{
		{name: "default greeting without store", wantGreetSub: "Hello, I am calling to help with an inquiry.", wantPromptSub: "Aerial, an assistant making"},
		{name: "store error falls back to defaults", withStore: true, storeErr: true, wantGreetSub: "Hello, I am calling to help", wantPromptSub: "Aerial, an assistant making"},
		{name: "outbound notify uses task greeting", withStore: true, direction: "outbound", briefMode: string(model.ModeNotify), briefTask: "Alert: package delivered", wantGreetSub: "Alert: package delivered", wantPromptSub: "NOTIFY MODE:", wantLanguage: "en-US"},
		{name: "inbound uses receptionist prompt", withStore: true, direction: "inbound", briefTask: "Question about bill", wantGreetSub: "Answer the incoming call with a warm, brief greeting", wantPromptSub: "CALLBACK RECEPTIONIST MODE:", wantLanguage: "en-US"},
		{name: "outbound conversation uses task greeting", withStore: true, direction: "outbound", briefMode: string(model.ModeConversation), briefTask: "Confirm doctor visit", wantGreetSub: "Confirm doctor visit", wantPromptSub: "Confirm doctor visit", wantLanguage: "en-US"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			callID := "call-ws-" + strings.ReplaceAll(strings.ToLower(tc.name), " ", "-")
			st := makeServeStore(t, tc, callID)
			tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
			fc := &captureFactory{sess: newFakeManagedSession("", nil)}
			mgr := NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, st, nil, nil, tokenMgr,
				WithPacerInterval(1*time.Millisecond),
				WithSessionFactory(fc.factory),
			)
			serveStreamToClose(t, mgr, tokenMgr, callID)
			select {
			case <-fc.sess.ran:
			default:
				t.Fatal("session factory was not invoked")
			}
			if got := mgr.ActiveCount(); got != 0 {
				t.Fatalf("ActiveCount() = %d, want 0 after handler return", got)
			}
			assertStreamConfig(t, fc, callID, tc)
		})
	}
}
