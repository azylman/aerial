package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/runner"
)

type mockDiscordMessageEditor struct {
	mu          sync.Mutex
	editCalls   []struct{ channelID, messageID, content string }
	deleteCalls []struct{ channelID, messageID string }
	editErr     error
	deleteErr   error
}

func (m *mockDiscordMessageEditor) ChannelMessageEdit(channelID, messageID, content string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.editCalls = append(m.editCalls, struct{ channelID, messageID, content string }{channelID, messageID, content})
	return m.editErr
}

func (m *mockDiscordMessageEditor) ChannelMessageDelete(channelID, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls = append(m.deleteCalls, struct{ channelID, messageID string }{channelID, messageID})
	return m.deleteErr
}

type mockWebSocketWriter struct {
	mu       sync.Mutex
	messages []any
	writeErr error
	closed   bool
}

func (m *mockWebSocketWriter) WriteJSON(v any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	m.messages = append(m.messages, v)
	return nil
}

func (m *mockWebSocketWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockWebSocketWriter) getMessages() []any {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]any, len(m.messages))
	copy(copied, m.messages)
	return copied
}

func TestThrowawayTurnSink(t *testing.T) {
	t.Parallel()

	testErr := errors.New("daemon execution failure")

	tests := []struct {
		name          string
		initialSteps  bool
		sendResult    *runner.TurnResult
		sendError     error
		secondResult  *runner.TurnResult
		secondError   error
		useCtx        bool
		cancelCtxPre  bool
		cancelCtxPost bool
		wantRes       string
		wantErr       error
	}{
		{
			name:         "ResultDelivery",
			initialSteps: true,
			sendResult:   &runner.TurnResult{Response: "classification: coding"},
			wantRes:      "classification: coding",
		},
		{
			name:       "NilResult",
			sendResult: nil,
			wantRes:    "",
		},
		{
			name:      "ErrorPath",
			sendError: testErr,
			wantErr:   testErr,
		},
		{
			name:         "OnceGuarded",
			sendResult:   &runner.TurnResult{Response: "first"},
			secondResult: &runner.TurnResult{Response: "second"},
			secondError:  errors.New("should be ignored"),
			wantRes:      "first",
		},
		{
			name:         "ResultContext_Cancelled",
			useCtx:       true,
			cancelCtxPre: true,
			wantErr:      context.Canceled,
		},
		{
			name:       "ResultContext_Success",
			useCtx:     true,
			sendResult: &runner.TurnResult{Response: "context result ok"},
			wantRes:    "context result ok",
		},
		{
			name:          "ResultContext_CancelledWhileWaiting",
			useCtx:        true,
			cancelCtxPost: true,
			wantErr:       context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sink := NewThrowawayTurnSink()

			if tt.initialSteps {
				sink.OnTurnStarted()
				sink.OnThinking()
				sink.OnToolCall("bash", "echo hi")
				sink.OnTextDelta("delta")
			}

			if tt.cancelCtxPre {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				res, err := sink.ResultContext(ctx)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				if res != tt.wantRes {
					t.Fatalf("expected res %q, got %q", tt.wantRes, res)
				}
				return
			}

			if tt.cancelCtxPost {
				ctx, cancel := context.WithCancel(context.Background())
				go cancel()
				_, err := sink.ResultContext(ctx)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context.Canceled while waiting, got %v", err)
				}
				return
			}

			go func() {
				if tt.sendError != nil {
					sink.OnError(tt.sendError)
				} else {
					sink.OnResult(tt.sendResult)
				}
				if tt.secondResult != nil {
					sink.OnResult(tt.secondResult)
				}
				if tt.secondError != nil {
					sink.OnError(tt.secondError)
				}
			}()

			var res string
			var err error
			if tt.useCtx {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				res, err = sink.ResultContext(ctx)
			} else {
				res, err = sink.Result()
			}

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if res != tt.wantRes {
				t.Fatalf("expected res %q, got %q", tt.wantRes, res)
			}
		})
	}
}

func TestDiscordTurnSink_AllMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		callTool      bool
		nilEditor     bool
		emptyBadge    bool
		withEditErr   bool
		withDeleteErr bool
		triggerError  bool
		streamDeltas  bool
		wantEdits     int
		wantDeletes   int
		wantText      string
	}{
		{
			name:         "LifecycleAndDeltas",
			callTool:     true,
			streamDeltas: true,
			wantEdits:    1,
			wantDeletes:  1,
			wantText:     "Hello world!",
		},
		{
			name:         "ErrorLifecycle",
			triggerError: true,
			wantDeletes:  1,
		},
		{
			name:          "EditorErrorsLoggedWithoutPanic",
			callTool:      true,
			withEditErr:   true,
			withDeleteErr: true,
			triggerError:  true,
			wantEdits:     1,
			wantDeletes:   2,
		},
		{
			name:        "NilEditor",
			callTool:    true,
			nilEditor:   true,
			wantEdits:   0,
			wantDeletes: 0,
		},
		{
			name:        "EmptyBadge",
			callTool:    true,
			emptyBadge:  true,
			wantEdits:   0,
			wantDeletes: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var editor DiscordMessageEditor
			var mockEditor *mockDiscordMessageEditor
			if !tc.nilEditor {
				mockEditor = &mockDiscordMessageEditor{}
				if tc.withEditErr {
					mockEditor.editErr = errors.New("edit network failure")
				}
				if tc.withDeleteErr {
					mockEditor.deleteErr = errors.New("delete 404 not found")
				}
				editor = mockEditor
			}

			badge := "msg-456"
			if tc.emptyBadge {
				badge = ""
			}

			var completedRes *runner.TurnResult
			var completedErr error
			stoppedTyping := false

			sink := NewDiscordTurnSink(
				editor,
				"chan-123",
				badge,
				func(res *runner.TurnResult) { completedRes = res },
				func(err error) { completedErr = err },
			)
			sink.SetStopTyping(func() { stoppedTyping = true })

			sink.OnTurnStarted()
			sink.OnThinking()
			if tc.callTool {
				sink.OnToolCall("web_search", "search aerial")
			}

			if tc.streamDeltas {
				sink.OnTextDelta("Hello ")
				sink.OnTextDelta("world!")
				if sink.ResponseText() != tc.wantText {
					t.Fatalf("expected %q, got %q", tc.wantText, sink.ResponseText())
				}
			}

			expectedErr := errors.New("turn timeout")
			if tc.triggerError {
				sink.OnError(expectedErr)
				if !stoppedTyping {
					t.Fatal("expected stopTyping called on error")
				}
				if !errors.Is(completedErr, expectedErr) {
					t.Fatalf("expected completedErr=%v, got %v", expectedErr, completedErr)
				}
			}

			if !tc.triggerError || tc.name == "EditorErrorsLoggedWithoutPanic" || tc.nilEditor || tc.emptyBadge {
				turnRes := &runner.TurnResult{Response: "ok"}
				if tc.streamDeltas {
					turnRes.Response = "Hello world!"
				}
				sink.OnResult(turnRes)
				if !stoppedTyping {
					t.Fatal("expected stopTyping called on result")
				}
				if !tc.triggerError && completedRes != turnRes {
					t.Fatalf("expected completedRes=%+v, got %+v", turnRes, completedRes)
				}
			}

			if mockEditor != nil {
				mockEditor.mu.Lock()
				if len(mockEditor.editCalls) != tc.wantEdits || len(mockEditor.deleteCalls) != tc.wantDeletes {
					t.Fatalf("edits=%d (want %d), deletes=%d (want %d)",
						len(mockEditor.editCalls), tc.wantEdits, len(mockEditor.deleteCalls), tc.wantDeletes)
				}
				if tc.wantDeletes > 0 && badge != "" {
					if mockEditor.deleteCalls[0].channelID != "chan-123" || mockEditor.deleteCalls[0].messageID != badge {
						t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls)
					}
				}
				if tc.wantEdits > 0 {
					if mockEditor.editCalls[0].content != "⚙️ Executing web_search..." {
						t.Fatalf("unexpected edit call content: %s", mockEditor.editCalls[0].content)
					}
				}
				mockEditor.mu.Unlock()
			}
		})
	}
}

func TestDiscordTurnSink_ConcurrentDeltas(t *testing.T) {
	t.Parallel()
	sink := NewDiscordTurnSink(nil, "c", "b", nil, nil)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink.OnTextDelta("chunk ")
			_ = sink.ResponseText()
		}()
	}
	wg.Wait()

	txt := sink.ResponseText()
	if len(txt) != 20*len("chunk ") {
		t.Fatalf("expected %d bytes, got %d", 20*len("chunk "), len(txt))
	}
}

func TestVoiceTurnSink_LifecycleAndRebind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		deviceID             string
		initialConn          bool
		failWrite            bool
		zeroCapStruct        bool
		chunksToSend         []string
		wantInitialMsgs      int
		rebindAckSeq         uint64
		rebindNil            bool
		wantReplayedMsgs     int
		wantFirstSeq         uint64
		appendAfterRebind    string
		wantTotalAfterRebind int
		wantRebindErrSub     string
		terminalAction       string // "result", "error"
		checkEmptyAfter      bool
		callResultAndError   bool
	}{
		{
			name:                 "RebindAndReplay",
			deviceID:             "device-test-1",
			initialConn:          true,
			chunksToSend:         []string{"Hello ", "world", "!"},
			wantInitialMsgs:      3,
			rebindAckSeq:         1,
			wantReplayedMsgs:     2,
			appendAfterRebind:    " Welcome!",
			wantTotalAfterRebind: 3,
		},
		{
			name:             "AckAll",
			deviceID:         "device-ack",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     5,
			wantReplayedMsgs: 0,
		},
		{
			name:             "AckNone",
			deviceID:         "device-ack",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     0,
			wantReplayedMsgs: 5,
			wantFirstSeq:     1,
		},
		{
			name:             "AckPartial",
			deviceID:         "device-ack",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     3,
			wantReplayedMsgs: 2,
			wantFirstSeq:     4,
		},
		{
			name:             "AckBeyondBuffer",
			deviceID:         "device-ack",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     10,
			wantReplayedMsgs: 0,
		},
		{
			name:              "RebindNilAndResume",
			deviceID:          "device-ack",
			chunksToSend:      []string{"c1", "c2", "c3", "c4", "c5"},
			rebindNil:         true,
			appendAfterRebind: "c6",
			rebindAckSeq:      5,
			wantReplayedMsgs:  1,
		},
		{
			name:               "ErrorPropagationAndWriteErrors",
			deviceID:           "device-fail",
			initialConn:        true,
			failWrite:          true,
			chunksToSend:       []string{"chunk1"},
			rebindAckSeq:       0,
			wantRebindErrSub:   "failed to replay chunk seq=1",
			callResultAndError: true,
		},
		{
			name:            "OnResultClearsBuffer",
			deviceID:        "device-result",
			initialConn:     true,
			chunksToSend:    []string{"c1"},
			terminalAction:  "result",
			checkEmptyAfter: true,
		},
		{
			name:            "OnErrorClearsBuffer",
			deviceID:        "device-error",
			initialConn:     true,
			chunksToSend:    []string{"c1"},
			terminalAction:  "error",
			checkEmptyAfter: true,
		},
		{
			name:           "NilConnection",
			deviceID:       "device-nil",
			chunksToSend:   []string{"delta 1"},
			terminalAction: "result",
		},
		{
			name:          "ZeroCapStruct",
			deviceID:      "device-zero",
			zeroCapStruct: true,
			chunksToSend:  []string{"test"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var sink *VoiceTurnSink
			var conn1 *mockWebSocketWriter
			if tc.initialConn {
				conn1 = &mockWebSocketWriter{}
				if tc.failWrite {
					conn1.writeErr = errors.New("socket broken pipe")
				}
				sink = NewVoiceTurnSink(conn1, tc.deviceID)
			} else if tc.zeroCapStruct {
				sink = &VoiceTurnSink{deviceID: tc.deviceID}
			} else {
				sink = NewVoiceTurnSink(nil, tc.deviceID)
			}

			if tc.deviceID != "" && sink.DeviceID() != tc.deviceID {
				t.Fatalf("expected %s, got %s", tc.deviceID, sink.DeviceID())
			}

			sink.OnTurnStarted()
			sink.OnThinking()
			sink.OnToolCall("tool", "cmd")

			for _, chunk := range tc.chunksToSend {
				sink.OnTextDelta(chunk)
			}

			if conn1 != nil && tc.wantInitialMsgs > 0 {
				msgs1 := conn1.getMessages()
				if len(msgs1) != tc.wantInitialMsgs {
					t.Fatalf("expected %d initial messages, got %d", tc.wantInitialMsgs, len(msgs1))
				}
			}

			if tc.rebindNil {
				if err := sink.Rebind(nil, 0); err != nil {
					t.Fatalf("expected nil error on rebind nil, got %v", err)
				}
				if tc.appendAfterRebind != "" {
					sink.OnTextDelta(tc.appendAfterRebind)
				}
				connResume := &mockWebSocketWriter{}
				if err := sink.Rebind(connResume, tc.rebindAckSeq); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(connResume.getMessages()) != tc.wantReplayedMsgs {
					t.Fatalf("expected %d replayed message, got %d", tc.wantReplayedMsgs, len(connResume.getMessages()))
				}
				return
			}

			if tc.wantRebindErrSub != "" || tc.wantReplayedMsgs > 0 || tc.rebindAckSeq > 0 || (tc.initialConn && tc.wantInitialMsgs > 0) {
				conn2 := &mockWebSocketWriter{}
				if tc.failWrite {
					conn2.writeErr = errors.New("socket broken pipe")
				}
				err := sink.Rebind(conn2, tc.rebindAckSeq)
				if tc.wantRebindErrSub != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantRebindErrSub) {
						t.Fatalf("expected wrapped rebind error containing %q, got: %v", tc.wantRebindErrSub, err)
					}
				} else {
					if err != nil {
						t.Fatalf("unexpected rebind error: %v", err)
					}
					msgs2 := conn2.getMessages()
					if len(msgs2) != tc.wantReplayedMsgs {
						t.Fatalf("expected %d replayed messages, got %d", tc.wantReplayedMsgs, len(msgs2))
					}
					if tc.wantFirstSeq > 0 && len(msgs2) > 0 {
						chunk, ok := msgs2[0].(TokenChunk)
						if !ok || chunk.Seq != tc.wantFirstSeq {
							t.Fatalf("expected first seq %d, got %+v", tc.wantFirstSeq, msgs2[0])
						}
					}
					if tc.appendAfterRebind != "" {
						sink.OnTextDelta(tc.appendAfterRebind)
						msgs2After := conn2.getMessages()
						if len(msgs2After) != tc.wantTotalAfterRebind {
							t.Fatalf("expected %d total messages on conn2, got %d", tc.wantTotalAfterRebind, len(msgs2After))
						}
					}
				}
			}

			if tc.callResultAndError {
				sink.OnResult(&runner.TurnResult{Response: "done"})
				sink.OnError(errors.New("subsequent error"))
			}

			switch tc.terminalAction {
			case "result":
				sink.OnResult(&runner.TurnResult{Response: "test complete"})
				sink.OnResult(nil)
			case "error":
				sink.OnError(errors.New("timeout reached"))
				sink.OnError(nil)
			}

			if tc.checkEmptyAfter {
				reconn := &mockWebSocketWriter{}
				_ = sink.Rebind(reconn, 0)
				if len(reconn.getMessages()) != 0 {
					t.Fatalf("expected empty buffer after terminal action, got %d", len(reconn.getMessages()))
				}
			}

			if tc.zeroCapStruct {
				if len(sink.ringBuf) != 1 {
					t.Fatalf("expected 1 item, got %d", len(sink.ringBuf))
				}
			}
		})
	}
}

func TestVoiceTurnSink_RingBufferCapacityTrimming(t *testing.T) {
	t.Parallel()
	conn := &mockWebSocketWriter{}
	sink := NewVoiceTurnSink(conn, "device-trim")

	for i := 1; i <= 1050; i++ {
		sink.OnTextDelta(fmt.Sprintf("chunk-%d", i))
	}

	if len(conn.getMessages()) != 1050 {
		t.Fatalf("expected 1050 messages delivered, got %d", len(conn.getMessages()))
	}

	reconn := &mockWebSocketWriter{}
	if err := sink.Rebind(reconn, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	replayed := reconn.getMessages()
	if len(replayed) != 1024 {
		t.Fatalf("expected 1024 retained chunks, got %d", len(replayed))
	}
	first, _ := replayed[0].(TokenChunk)
	last, _ := replayed[1023].(TokenChunk)
	if first.Seq != 27 || last.Seq != 1050 {
		t.Fatalf("unexpected seq range: first=%d last=%d", first.Seq, last.Seq)
	}
}

func TestVoiceTurnSink_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	conn := &mockWebSocketWriter{}
	sink := NewVoiceTurnSink(conn, "device-concurrent")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				sink.OnTextDelta(fmt.Sprintf("worker-%d-chunk-%d", workerID, j))
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_ = sink.Rebind(&mockWebSocketWriter{}, 0)
		}
	}()

	wg.Wait()
}

func TestTurnSinks_NoopMethods(t *testing.T) {
	t.Parallel()

	tSink := NewThrowawayTurnSink()
	tSink.OnTurnStarted()
	tSink.OnThinking()
	tSink.OnToolCall("test-tool", "test-cmd")
	tSink.OnTextDelta("delta")

	dSink := NewDiscordTurnSink(nil, "chan-1", "msg-1", nil, nil)
	dSink.OnTurnStarted()
	dSink.OnThinking()
	dSink.OnToolCompleted("tool", "native", 10*time.Millisecond, "ok")
	dSink.OnSkillActivated("skill", "discord")

	var voiceStatus string
	vSink := newVoiceTurnSink(context.Background(), func(s string) { voiceStatus = s }, nil, nil)
	vSink.OnTurnStarted()
	vSink.OnThinking()
	vSink.OnTextDelta("voice-delta")
	vSink.OnToolCall("run_cmd", "sleep 1")
	vSink.OnToolCompleted("run_cmd", "native", 10*time.Millisecond, "ok")
	vSink.OnSkillActivated("skill", "voice")
	if voiceStatus == "" {
		t.Errorf("expected non-empty voice status")
	}
	vSink.OnToolCall("", "")
	vSinkNilStatus := newVoiceTurnSink(context.Background(), nil, nil, nil)
	vSinkNilStatus.OnToolCall("tool", "cmd")
	vSink.OnResult(&runner.TurnResult{Response: "voice-1"})
	vSink.OnError(errors.New("err-1"))

	rSink := newDiscordTurnSink(nil)
	rSink.OnTurnStarted()
	rSink.OnThinking()
	rSink.OnTextDelta("turn-delta")
	rSink.OnToolCall("tool", "cmd")
	rSink.OnToolCall("", "")

	voiceSink := NewVoiceTurnSink(&mockWebSocketWriter{}, "device-noop")
	voiceSink.OnTurnStarted()
	voiceSink.OnThinking()
	voiceSink.OnToolCall("tool", "cmd")
	voiceSink.OnToolCompleted("tool", "native", 10*time.Millisecond, "ok")
	voiceSink.OnSkillActivated("skill", "voice")
}

func TestNewVoiceTurnSink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                      string
		cancelledCtx              bool
		withDetector              bool
		deltas                    []string
		callResult                bool
		resultResponse            string
		onErrorErr                error
		wantSentencesBeforeResult []string
		wantSentences             []string
		wantWaitResponse          string
		wantWaitErrSub            string
	}{
		{
			name:                      "SentenceStreamingAndResultFallback",
			withDetector:              true,
			deltas:                    []string{"First sentence. ", "Second incomplete"},
			callResult:                true,
			resultResponse:            "First sentence. Second incomplete",
			wantSentencesBeforeResult: []string{"First sentence."},
			wantSentences:             []string{"First sentence.", "Second incomplete"},
		},
		{
			name:           "ResultFallbackWithoutDeltas",
			withDetector:   true,
			callResult:     true,
			resultResponse: "Hello there. General Kenobi.",
			wantSentences:  []string{"Hello there.", "General Kenobi."},
		},
		{
			name:          "CancellationGuards",
			cancelledCtx:  true,
			withDetector:  true,
			deltas:        []string{"Should not be emitted because context is cancelled. "},
			wantSentences: nil,
		},
		{
			name:             "AccumulatedDeltasOnEmptyResult",
			withDetector:     true,
			deltas:           []string{"Hello ", "world."},
			callResult:       true,
			resultResponse:   "",
			wantWaitResponse: "Hello world.",
		},
		{
			name:             "SubstantiveDeltasRecoveredOnError",
			withDetector:     true,
			deltas:           []string{"The quick brown fox jumps over the lazy dog."},
			onErrorErr:       errors.New("API error (attempt 2): RESOURCE_EXHAUSTED (code 429)"),
			wantWaitResponse: "The quick brown fox jumps over the lazy dog.",
		},
		{
			name:           "EmptyDeltasOnErrorForwardsErrCh",
			onErrorErr:     errors.New("hard failure without deltas"),
			wantWaitErrSub: "hard failure without deltas",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tc.cancelledCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			var sentences []string
			var detector *SentenceDetector
			var onSentence func(string)
			if tc.withDetector {
				detector = NewSentenceDetector()
				onSentence = func(s string) {
					sentences = append(sentences, s)
				}
			}

			sink := newVoiceTurnSink(ctx, nil, onSentence, detector)
			sink.OnTurnStarted()
			sink.OnThinking()

			for _, d := range tc.deltas {
				sink.OnTextDelta(d)
			}

			if len(tc.wantSentencesBeforeResult) > 0 {
				if len(sentences) != len(tc.wantSentencesBeforeResult) || sentences[0] != tc.wantSentencesBeforeResult[0] {
					t.Fatalf("expected %v before result, got %v", tc.wantSentencesBeforeResult, sentences)
				}
			}

			if tc.callResult {
				sink.OnResult(&runner.TurnResult{Response: tc.resultResponse})
			}

			if tc.onErrorErr != nil {
				sink.OnError(tc.onErrorErr)
			}

			if tc.wantSentences != nil {
				if len(sentences) != len(tc.wantSentences) {
					t.Fatalf("expected %d sentences, got %d (%v)", len(tc.wantSentences), len(sentences), sentences)
				}
				for i, want := range tc.wantSentences {
					if sentences[i] != want {
						t.Errorf("sentence[%d] = %q, want %q", i, sentences[i], want)
					}
				}
			} else if tc.cancelledCtx {
				if len(sentences) != 0 {
					t.Fatalf("expected no sentences on cancelled context, got %d", len(sentences))
				}
			}

			if tc.wantWaitResponse != "" || tc.wantWaitErrSub != "" {
				res, err := sink.Wait(context.Background())
				if tc.wantWaitErrSub != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantWaitErrSub) {
						t.Fatalf("expected wait error containing %q, got: %v", tc.wantWaitErrSub, err)
					}
				} else {
					if err != nil {
						t.Fatalf("unexpected wait error: %v", err)
					}
					if res == nil || res.Response != tc.wantWaitResponse {
						t.Fatalf("expected response %q, got %+v", tc.wantWaitResponse, res)
					}
				}
			}
		})
	}
}
