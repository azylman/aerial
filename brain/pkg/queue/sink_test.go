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

	t.Run("LifecycleAndDeltas", func(t *testing.T) {
		t.Parallel()
		mockEditor := &mockDiscordMessageEditor{}
		var completedRes *runner.TurnResult
		var completedErr error
		stoppedTyping := false

		sink := NewDiscordTurnSink(
			mockEditor,
			"chan-123",
			"msg-456",
			func(res *runner.TurnResult) { completedRes = res },
			func(err error) { completedErr = err },
		)
		sink.SetStopTyping(func() { stoppedTyping = true })

		sink.OnTurnStarted()
		sink.OnThinking()
		sink.OnToolCall("web_search", "search aerial")

		mockEditor.mu.Lock()
		if len(mockEditor.editCalls) != 1 || mockEditor.editCalls[0].content != "⚙️ Executing web_search..." {
			t.Fatalf("unexpected edit calls: %+v", mockEditor.editCalls)
		}
		mockEditor.mu.Unlock()

		sink.OnTextDelta("Hello ")
		sink.OnTextDelta("world!")
		if sink.ResponseText() != "Hello world!" {
			t.Fatalf("expected 'Hello world!', got %q", sink.ResponseText())
		}

		turnRes := &runner.TurnResult{Response: "Hello world!"}
		sink.OnResult(turnRes)

		if !stoppedTyping {
			t.Fatal("expected stopTyping to have been called")
		}

		mockEditor.mu.Lock()
		if len(mockEditor.deleteCalls) != 1 || mockEditor.deleteCalls[0].channelID != "chan-123" || mockEditor.deleteCalls[0].messageID != "msg-456" {
			t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls)
		}
		mockEditor.mu.Unlock()

		if completedRes != turnRes || completedErr != nil {
			t.Fatalf("unexpected completion: res=%+v err=%v", completedRes, completedErr)
		}
	})

	t.Run("ErrorLifecycle", func(t *testing.T) {
		t.Parallel()
		mockEditor := &mockDiscordMessageEditor{}
		var completedErr error
		stoppedTyping := false

		sink := NewDiscordTurnSink(
			mockEditor,
			"chan-err",
			"badge-err",
			func(res *runner.TurnResult) { t.Fatal("onComplete should not be called on error") },
			func(err error) { completedErr = err },
		)
		sink.SetStopTyping(func() { stoppedTyping = true })

		expectedErr := errors.New("turn timeout")
		sink.OnError(expectedErr)

		if !stoppedTyping {
			t.Fatal("expected stopTyping called on error")
		}
		if !errors.Is(completedErr, expectedErr) {
			t.Fatalf("expected completedErr=%v, got %v", expectedErr, completedErr)
		}

		mockEditor.mu.Lock()
		if len(mockEditor.deleteCalls) != 1 || mockEditor.deleteCalls[0].channelID != "chan-err" || mockEditor.deleteCalls[0].messageID != "badge-err" {
			t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls)
		}
		mockEditor.mu.Unlock()
	})

	t.Run("EditorErrorsLoggedWithoutPanic", func(t *testing.T) {
		t.Parallel()
		mockEditor := &mockDiscordMessageEditor{
			editErr:   errors.New("edit network failure"),
			deleteErr: errors.New("delete 404 not found"),
		}
		sink := NewDiscordTurnSink(mockEditor, "chan-fail", "badge-fail", nil, nil)
		sink.OnToolCall("run_command", "git status")
		sink.OnResult(&runner.TurnResult{Response: "ok"})
		sink.OnError(errors.New("subsequent error"))

		mockEditor.mu.Lock()
		if len(mockEditor.editCalls) != 1 || len(mockEditor.deleteCalls) != 2 {
			t.Fatalf("expected 1 edit and 2 deletes, got edits=%d deletes=%d", len(mockEditor.editCalls), len(mockEditor.deleteCalls))
		}
		mockEditor.mu.Unlock()
	})

	t.Run("NilEditorAndEmptyBadge", func(t *testing.T) {
		t.Parallel()
		sinkNilEditor := NewDiscordTurnSink(nil, "chan-nil", "badge-nil", nil, nil)
		sinkNilEditor.OnToolCall("tool", "cmd")
		sinkNilEditor.OnResult(&runner.TurnResult{Response: "done"})
		sinkNilEditor.OnError(errors.New("err"))

		mockEditor := &mockDiscordMessageEditor{}
		sinkEmptyBadge := NewDiscordTurnSink(mockEditor, "chan-empty", "", nil, nil)
		sinkEmptyBadge.OnToolCall("tool", "cmd")
		sinkEmptyBadge.OnResult(&runner.TurnResult{Response: "done"})
		sinkEmptyBadge.OnError(errors.New("err"))

		mockEditor.mu.Lock()
		if len(mockEditor.editCalls) != 0 || len(mockEditor.deleteCalls) != 0 {
			t.Fatalf("expected 0 calls when badge empty, got edits=%d deletes=%d", len(mockEditor.editCalls), len(mockEditor.deleteCalls))
		}
		mockEditor.mu.Unlock()
	})

	t.Run("ConcurrentDeltas", func(t *testing.T) {
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
	})
}

func TestVoiceTurnSink_LifecycleAndRebind(t *testing.T) {
	t.Parallel()

	t.Run("RebindAndReplay", func(t *testing.T) {
		t.Parallel()
		conn1 := &mockWebSocketWriter{}
		sink := NewVoiceTurnSink(conn1, "device-test-1")

		if sink.DeviceID() != "device-test-1" {
			t.Fatalf("expected device-test-1, got %s", sink.DeviceID())
		}

		sink.OnTurnStarted()
		sink.OnThinking()
		sink.OnToolCall("voice_search", "weather")
		sink.OnTextDelta("Hello ")
		sink.OnTextDelta("world")
		sink.OnTextDelta("!")

		msgs1 := conn1.getMessages()
		if len(msgs1) != 3 {
			t.Fatalf("expected 3 messages on conn1, got %d", len(msgs1))
		}

		conn2 := &mockWebSocketWriter{}
		if err := sink.Rebind(conn2, 1); err != nil {
			t.Fatalf("unexpected rebind error: %v", err)
		}

		msgs2 := conn2.getMessages()
		if len(msgs2) != 2 {
			t.Fatalf("expected 2 replayed messages on conn2, got %d", len(msgs2))
		}

		sink.OnTextDelta(" Welcome!")
		msgs2After := conn2.getMessages()
		if len(msgs2After) != 3 {
			t.Fatalf("expected 3 total messages on conn2, got %d", len(msgs2After))
		}
	})

	t.Run("RebindAckScenarios", func(t *testing.T) {
		t.Parallel()
		sink := NewVoiceTurnSink(nil, "device-ack")
		for i := 1; i <= 5; i++ {
			sink.OnTextDelta(fmt.Sprintf("c%d", i))
		}

		ackTests := []struct {
			name         string
			lastAckSeq   uint64
			wantCount    int
			wantFirstSeq uint64
		}{
			{"AllAcked", 5, 0, 0},
			{"NoneAcked", 0, 5, 1},
			{"PartialAcked", 3, 2, 4},
			{"BeyondBuffer", 10, 0, 0},
		}

		for _, at := range ackTests {
			conn := &mockWebSocketWriter{}
			if err := sink.Rebind(conn, at.lastAckSeq); err != nil {
				t.Fatalf("%s: unexpected error: %v", at.name, err)
			}
			msgs := conn.getMessages()
			if len(msgs) != at.wantCount {
				t.Fatalf("%s: expected %d replayed, got %d", at.name, at.wantCount, len(msgs))
			}
			if at.wantCount > 0 {
				chunk, ok := msgs[0].(TokenChunk)
				if !ok || chunk.Seq != at.wantFirstSeq {
					t.Fatalf("%s: expected first seq %d, got %+v", at.name, at.wantFirstSeq, msgs[0])
				}
			}
		}

		if err := sink.Rebind(nil, 0); err != nil {
			t.Fatalf("expected nil error on rebind nil, got %v", err)
		}
		sink.OnTextDelta("c6")
		connE := &mockWebSocketWriter{}
		if err := sink.Rebind(connE, 5); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(connE.getMessages()) != 1 {
			t.Fatalf("expected 1 replayed message, got %d", len(connE.getMessages()))
		}
	})

	t.Run("ErrorPropagationAndWriteErrors", func(t *testing.T) {
		t.Parallel()
		connFail := &mockWebSocketWriter{writeErr: errors.New("socket broken pipe")}
		sink := NewVoiceTurnSink(connFail, "device-fail")
		sink.OnTextDelta("chunk1")

		err := sink.Rebind(connFail, 0)
		if err == nil || !strings.Contains(err.Error(), "failed to replay chunk seq=1") {
			t.Fatalf("expected wrapped rebind error, got: %v", err)
		}

		sink.OnResult(&runner.TurnResult{Response: "done"})
		sink.OnError(errors.New("subsequent error"))
	})

	t.Run("OnResultAndOnError_MessageAndClear", func(t *testing.T) {
		t.Parallel()
		connR := &mockWebSocketWriter{}
		sinkR := NewVoiceTurnSink(connR, "device-result")
		sinkR.OnTextDelta("c1")
		sinkR.OnResult(&runner.TurnResult{Response: "test complete"})
		reconnR := &mockWebSocketWriter{}
		_ = sinkR.Rebind(reconnR, 0)
		if len(reconnR.getMessages()) != 0 {
			t.Fatalf("expected empty buffer after OnResult, got %d", len(reconnR.getMessages()))
		}
		sinkR.OnResult(nil)

		connE := &mockWebSocketWriter{}
		sinkE := NewVoiceTurnSink(connE, "device-error")
		sinkE.OnTextDelta("c1")
		sinkE.OnError(errors.New("timeout reached"))
		reconnE := &mockWebSocketWriter{}
		_ = sinkE.Rebind(reconnE, 0)
		if len(reconnE.getMessages()) != 0 {
			t.Fatalf("expected empty buffer after OnError, got %d", len(reconnE.getMessages()))
		}
		sinkE.OnError(nil)
	})

	t.Run("NilConnectionAndZeroCap", func(t *testing.T) {
		t.Parallel()
		sink := NewVoiceTurnSink(nil, "device-nil")
		sink.OnTurnStarted()
		sink.OnThinking()
		sink.OnToolCall("tool", "cmd")
		sink.OnTextDelta("delta 1")
		sink.OnResult(&runner.TurnResult{})
		sink.OnError(errors.New("error"))
		if sink.DeviceID() != "device-nil" {
			t.Fatalf("expected device-nil, got %s", sink.DeviceID())
		}

		sinkZero := &VoiceTurnSink{deviceID: "device-zero"}
		sinkZero.OnTextDelta("test")
		if len(sinkZero.ringBuf) != 1 {
			t.Fatalf("expected 1 item, got %d", len(sinkZero.ringBuf))
		}
	})
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

	t.Run("SentenceStreamingAndResultFallback", func(t *testing.T) {
		t.Parallel()
		var sentences []string
		sink := newVoiceTurnSink(context.Background(), nil, func(s string) {
			sentences = append(sentences, s)
		}, NewSentenceDetector())

		sink.OnTurnStarted()
		sink.OnThinking()
		sink.OnTextDelta("First sentence. ")
		sink.OnTextDelta("Second incomplete")

		if len(sentences) != 1 || sentences[0] != "First sentence." {
			t.Fatalf("expected 1 sentence, got: %v", sentences)
		}

		sink.OnResult(&runner.TurnResult{Response: "First sentence. Second incomplete"})
		if len(sentences) != 2 || sentences[1] != "Second incomplete" {
			t.Fatalf("expected 2 sentences after flush on result, got: %v", sentences)
		}

		var fallbackSentences []string
		fallbackSink := newVoiceTurnSink(context.Background(), nil, func(s string) {
			fallbackSentences = append(fallbackSentences, s)
		}, NewSentenceDetector())
		fallbackSink.OnResult(&runner.TurnResult{Response: "Hello there. General Kenobi."})
		if len(fallbackSentences) != 2 || fallbackSentences[0] != "Hello there." || fallbackSentences[1] != "General Kenobi." {
			t.Fatalf("unexpected fallback sentences: %v", fallbackSentences)
		}
	})

	t.Run("CancellationGuards", func(t *testing.T) {
		t.Parallel()
		var sentences []string
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cancellingSink := newVoiceTurnSink(ctx, nil, func(s string) {
			sentences = append(sentences, s)
		}, NewSentenceDetector())
		cancellingSink.OnTextDelta("Should not be emitted because context is cancelled. ")
		if len(sentences) != 0 {
			t.Fatalf("expected no sentences on cancelled context, got %d", len(sentences))
		}
	})

	t.Run("DeltaAccumulationAndErrorRecovery", func(t *testing.T) {
		t.Parallel()

		// Case 1: Streamed deltas with empty terminal response falls back to accumulated text
		sink1 := newVoiceTurnSink(context.Background(), nil, nil, NewSentenceDetector())
		sink1.OnTextDelta("Hello ")
		sink1.OnTextDelta("world.")
		sink1.OnResult(&runner.TurnResult{Response: ""})

		res1, err1 := sink1.Wait(context.Background())
		if err1 != nil || res1.Response != "Hello world." {
			t.Fatalf("expected accumulated 'Hello world.', got res=%+v err=%v", res1, err1)
		}

		// Case 2: Streamed deltas followed by OnError recovers substantive deltas on resCh
		sink2 := newVoiceTurnSink(context.Background(), nil, nil, NewSentenceDetector())
		sink2.OnTextDelta("The quick brown fox jumps over the lazy dog.")
		sink2.OnError(errors.New("API error (attempt 2): RESOURCE_EXHAUSTED (code 429)"))

		res2, err2 := sink2.Wait(context.Background())
		if err2 != nil || res2.Response != "The quick brown fox jumps over the lazy dog." {
			t.Fatalf("expected recovered substantive deltas, got res=%+v err=%v", res2, err2)
		}

		// Case 3: Empty deltas with OnError forwards to errCh
		sink3 := newVoiceTurnSink(context.Background(), nil, nil, nil)
		expectedErr := errors.New("hard failure without deltas")
		sink3.OnError(expectedErr)

		res3, err3 := sink3.Wait(context.Background())
		if res3 != nil || !errors.Is(err3, expectedErr) {
			t.Fatalf("expected error on errCh, got res=%+v err=%v", res3, err3)
		}
	})
}
