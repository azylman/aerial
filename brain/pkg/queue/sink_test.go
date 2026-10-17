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

func TestThrowawayTurnSink_ResultAndOnceDelivery(t *testing.T) {
	t.Parallel()

	testErr := errors.New("daemon execution failure")

	tests := []struct {
		name         string
		initialSteps bool
		sendResult   *runner.TurnResult
		sendError    error
		secondResult *runner.TurnResult
		secondError  error
		wantRes      string
		wantErr      error
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sink := NewThrowawayTurnSink()

			if tc.initialSteps {
				sink.OnTurnStarted()
				sink.OnThinking()
				sink.OnToolCall("bash", "echo hi")
				sink.OnTextDelta("delta")
			}

			go func() {
				if tc.sendError != nil {
					sink.OnError(tc.sendError)
				} else {
					sink.OnResult(tc.sendResult)
				}
				if tc.secondResult != nil {
					sink.OnResult(tc.secondResult)
				}
				if tc.secondError != nil {
					sink.OnError(tc.secondError)
				}
			}()

			res, err := sink.Result()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if res != tc.wantRes {
				t.Fatalf("expected res %q, got %q", tc.wantRes, res)
			}
		})
	}
}

func TestThrowawayTurnSink_ResultContext(t *testing.T) {
	t.Parallel()

	// Pre-cancelled context
	{
		sink := NewThrowawayTurnSink()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		res, err := sink.ResultContext(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected error %v, got %v", context.Canceled, err)
		}
		if res != "" {
			t.Fatalf("expected empty res, got %q", res)
		}
	}

	// Cancelled while waiting
	{
		sink := NewThrowawayTurnSink()
		ctx, cancel := context.WithCancel(context.Background())
		go cancel()
		_, err := sink.ResultContext(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled while waiting, got %v", err)
		}
	}

	// Context success delivery
	{
		sink := NewThrowawayTurnSink()
		go func() {
			sink.OnResult(&runner.TurnResult{Response: "context result ok"})
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res, err := sink.ResultContext(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != "context result ok" {
			t.Fatalf("expected res %q, got %q", "context result ok", res)
		}
	}
}

func TestDiscordTurnSink_LifecycleAndDeltas(t *testing.T) {
	t.Parallel()

	mockEditor := &mockDiscordMessageEditor{}
	badge := "msg-456"
	var completedRes *runner.TurnResult
	var completedErr error
	stoppedTyping := false

	sink := NewDiscordTurnSink(
		mockEditor,
		"chan-123",
		badge,
		func(res *runner.TurnResult) { completedRes = res },
		func(err error) { completedErr = err },
	)
	sink.SetStopTyping(func() { stoppedTyping = true })

	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("web_search", "search aerial")

	sink.OnTextDelta("Hello ")
	sink.OnTextDelta("world!")
	if sink.ResponseText() != "Hello world!" {
		t.Fatalf("expected %q, got %q", "Hello world!", sink.ResponseText())
	}

	turnRes := &runner.TurnResult{Response: "Hello world!"}
	sink.OnResult(turnRes)
	if !stoppedTyping {
		t.Fatal("expected stopTyping called on result")
	}
	if completedRes != turnRes {
		t.Fatalf("expected completedRes=%+v, got %+v", turnRes, completedRes)
	}
	if completedErr != nil {
		t.Fatalf("unexpected completedErr: %v", completedErr)
	}

	mockEditor.mu.Lock()
	defer mockEditor.mu.Unlock()
	if len(mockEditor.editCalls) != 1 || len(mockEditor.deleteCalls) != 1 {
		t.Fatalf("edits=%d (want 1), deletes=%d (want 1)",
			len(mockEditor.editCalls), len(mockEditor.deleteCalls))
	}
	if mockEditor.deleteCalls[0].channelID != "chan-123" || mockEditor.deleteCalls[0].messageID != badge {
		t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls)
	}
	if mockEditor.editCalls[0].content != "⚙️ Executing web_search..." {
		t.Fatalf("unexpected edit call content: %s", mockEditor.editCalls[0].content)
	}
}

func TestDiscordTurnSink_ErrorLifecycle(t *testing.T) {
	t.Parallel()

	mockEditor := &mockDiscordMessageEditor{}
	badge := "msg-456"
	var completedRes *runner.TurnResult
	var completedErr error
	stoppedTyping := false

	sink := NewDiscordTurnSink(
		mockEditor,
		"chan-123",
		badge,
		func(res *runner.TurnResult) { completedRes = res },
		func(err error) { completedErr = err },
	)
	sink.SetStopTyping(func() { stoppedTyping = true })

	sink.OnTurnStarted()
	sink.OnThinking()

	expectedErr := errors.New("turn timeout")
	sink.OnError(expectedErr)
	if !stoppedTyping {
		t.Fatal("expected stopTyping called on error")
	}
	if !errors.Is(completedErr, expectedErr) {
		t.Fatalf("expected completedErr=%v, got %v", expectedErr, completedErr)
	}
	if completedRes != nil {
		t.Fatalf("unexpected completedRes: %+v", completedRes)
	}

	mockEditor.mu.Lock()
	defer mockEditor.mu.Unlock()
	if len(mockEditor.editCalls) != 0 || len(mockEditor.deleteCalls) != 1 {
		t.Fatalf("edits=%d (want 0), deletes=%d (want 1)",
			len(mockEditor.editCalls), len(mockEditor.deleteCalls))
	}
	if mockEditor.deleteCalls[0].channelID != "chan-123" || mockEditor.deleteCalls[0].messageID != badge {
		t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls)
	}
}

func TestDiscordTurnSink_EditorErrorsAndGuards(t *testing.T) {
	t.Parallel()

	// Editor errors logged without panic
	{
		mockEditor := &mockDiscordMessageEditor{
			editErr:   errors.New("edit network failure"),
			deleteErr: errors.New("delete 404 not found"),
		}
		badge := "msg-456"
		var completedErr error
		stoppedTyping := false

		sink := NewDiscordTurnSink(
			mockEditor,
			"chan-123",
			badge,
			func(res *runner.TurnResult) {},
			func(err error) { completedErr = err },
		)
		sink.SetStopTyping(func() { stoppedTyping = true })

		sink.OnTurnStarted()
		sink.OnThinking()
		sink.OnToolCall("web_search", "search aerial")

		expectedErr := errors.New("turn timeout")
		sink.OnError(expectedErr)
		if !stoppedTyping {
			t.Fatal("expected stopTyping called on error")
		}
		if !errors.Is(completedErr, expectedErr) {
			t.Fatalf("expected completedErr=%v, got %v", expectedErr, completedErr)
		}

		sink.OnResult(&runner.TurnResult{Response: "ok"})

		mockEditor.mu.Lock()
		if len(mockEditor.editCalls) != 1 || len(mockEditor.deleteCalls) != 2 {
			t.Fatalf("edits=%d (want 1), deletes=%d (want 2)",
				len(mockEditor.editCalls), len(mockEditor.deleteCalls))
		}
		if mockEditor.deleteCalls[0].channelID != "chan-123" || mockEditor.deleteCalls[0].messageID != badge {
			t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls)
		}
		if mockEditor.editCalls[0].content != "⚙️ Executing web_search..." {
			t.Fatalf("unexpected edit call content: %s", mockEditor.editCalls[0].content)
		}
		mockEditor.mu.Unlock()
	}

	// Nil editor guards
	{
		badge := "msg-456"
		var completedRes *runner.TurnResult
		stoppedTyping := false

		sink := NewDiscordTurnSink(
			nil,
			"chan-123",
			badge,
			func(res *runner.TurnResult) { completedRes = res },
			func(err error) {},
		)
		sink.SetStopTyping(func() { stoppedTyping = true })

		sink.OnTurnStarted()
		sink.OnThinking()
		sink.OnToolCall("web_search", "search aerial")

		turnRes := &runner.TurnResult{Response: "ok"}
		sink.OnResult(turnRes)
		if !stoppedTyping {
			t.Fatal("expected stopTyping called on result")
		}
		if completedRes != turnRes {
			t.Fatalf("expected completedRes=%+v, got %+v", turnRes, completedRes)
		}
	}

	// Empty badge guards
	{
		mockEditor := &mockDiscordMessageEditor{}
		var completedRes *runner.TurnResult
		stoppedTyping := false

		sink := NewDiscordTurnSink(
			mockEditor,
			"chan-123",
			"",
			func(res *runner.TurnResult) { completedRes = res },
			func(err error) {},
		)
		sink.SetStopTyping(func() { stoppedTyping = true })

		sink.OnTurnStarted()
		sink.OnThinking()
		sink.OnToolCall("web_search", "search aerial")

		turnRes := &runner.TurnResult{Response: "ok"}
		sink.OnResult(turnRes)
		if !stoppedTyping {
			t.Fatal("expected stopTyping called on result")
		}
		if completedRes != turnRes {
			t.Fatalf("expected completedRes=%+v, got %+v", turnRes, completedRes)
		}

		mockEditor.mu.Lock()
		if len(mockEditor.editCalls) != 0 || len(mockEditor.deleteCalls) != 0 {
			t.Fatalf("edits=%d (want 0), deletes=%d (want 0)",
				len(mockEditor.editCalls), len(mockEditor.deleteCalls))
		}
		mockEditor.mu.Unlock()
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

func TestVoiceTurnSink_RebindReplayMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		chunksToSend         []string
		wantInitialMsgs      int
		rebindAckSeq         uint64
		wantReplayedMsgs     int
		wantFirstSeq         uint64
		appendAfterRebind    string
		wantTotalAfterRebind int
	}{
		{
			name:                 "RebindAndReplay",
			chunksToSend:         []string{"Hello ", "world", "!"},
			wantInitialMsgs:      3,
			rebindAckSeq:         1,
			wantReplayedMsgs:     2,
			appendAfterRebind:    " Welcome!",
			wantTotalAfterRebind: 3,
		},
		{
			name:             "AckAll",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     5,
			wantReplayedMsgs: 0,
		},
		{
			name:             "AckNone",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     0,
			wantReplayedMsgs: 5,
			wantFirstSeq:     1,
		},
		{
			name:             "AckPartial",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     3,
			wantReplayedMsgs: 2,
			wantFirstSeq:     4,
		},
		{
			name:             "AckBeyondBuffer",
			chunksToSend:     []string{"c1", "c2", "c3", "c4", "c5"},
			rebindAckSeq:     10,
			wantReplayedMsgs: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var conn1 WebSocketWriter
			if tc.wantInitialMsgs > 0 {
				conn1 = &mockWebSocketWriter{}
			}
			sink := NewVoiceTurnSink(conn1, "device-ack")

			sink.OnTurnStarted()
			sink.OnThinking()
			sink.OnToolCall("tool", "cmd")

			for _, chunk := range tc.chunksToSend {
				sink.OnTextDelta(chunk)
			}

			if conn1 != nil {
				m := conn1.(*mockWebSocketWriter)
				if len(m.getMessages()) != tc.wantInitialMsgs {
					t.Fatalf("expected %d initial messages, got %d", tc.wantInitialMsgs, len(m.getMessages()))
				}
			}

			conn2 := &mockWebSocketWriter{}
			if err := sink.Rebind(conn2, tc.rebindAckSeq); err != nil {
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
		})
	}
}

func TestVoiceTurnSink_RebindNilAndWriteErrors(t *testing.T) {
	t.Parallel()

	// RebindNilAndResume
	sink := NewVoiceTurnSink(nil, "device-ack")
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("tool", "cmd")
	for _, c := range []string{"c1", "c2", "c3", "c4", "c5"} {
		sink.OnTextDelta(c)
	}
	if err := sink.Rebind(nil, 0); err != nil {
		t.Fatalf("expected nil error on rebind nil, got %v", err)
	}
	sink.OnTextDelta("c6")
	connResume := &mockWebSocketWriter{}
	if err := sink.Rebind(connResume, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connResume.getMessages()) != 1 {
		t.Fatalf("expected 1 replayed message, got %d", len(connResume.getMessages()))
	}

	// ErrorPropagationAndWriteErrors
	connFail1 := &mockWebSocketWriter{writeErr: errors.New("socket broken pipe")}
	sinkFail := NewVoiceTurnSink(connFail1, "device-fail")
	sinkFail.OnTurnStarted()
	sinkFail.OnThinking()
	sinkFail.OnToolCall("tool", "cmd")
	sinkFail.OnTextDelta("chunk1")

	connFail2 := &mockWebSocketWriter{writeErr: errors.New("socket broken pipe")}
	err := sinkFail.Rebind(connFail2, 0)
	if err == nil || !strings.Contains(err.Error(), "failed to replay chunk seq=1") {
		t.Fatalf("expected wrapped rebind error containing %q, got: %v", "failed to replay chunk seq=1", err)
	}
	sinkFail.OnResult(&runner.TurnResult{Response: "done"})
	sinkFail.OnError(errors.New("subsequent error"))
}

func TestVoiceTurnSink_BufferClearAndEdgeGuards(t *testing.T) {
	t.Parallel()

	// OnResultClearsBuffer
	sinkResult := NewVoiceTurnSink(&mockWebSocketWriter{}, "device-result")
	sinkResult.OnTurnStarted()
	sinkResult.OnThinking()
	sinkResult.OnToolCall("tool", "cmd")
	sinkResult.OnTextDelta("c1")
	sinkResult.OnResult(&runner.TurnResult{Response: "test complete"})
	sinkResult.OnResult(nil)
	reconnResult := &mockWebSocketWriter{}
	_ = sinkResult.Rebind(reconnResult, 0)
	if len(reconnResult.getMessages()) != 0 {
		t.Fatalf("expected empty buffer after terminal action, got %d", len(reconnResult.getMessages()))
	}

	// OnErrorClearsBuffer
	sinkError := NewVoiceTurnSink(&mockWebSocketWriter{}, "device-error")
	sinkError.OnTurnStarted()
	sinkError.OnThinking()
	sinkError.OnToolCall("tool", "cmd")
	sinkError.OnTextDelta("c1")
	sinkError.OnError(errors.New("timeout reached"))
	sinkError.OnError(nil)
	reconnError := &mockWebSocketWriter{}
	_ = sinkError.Rebind(reconnError, 0)
	if len(reconnError.getMessages()) != 0 {
		t.Fatalf("expected empty buffer after terminal action, got %d", len(reconnError.getMessages()))
	}

	// NilConnection
	sinkNil := NewVoiceTurnSink(nil, "device-nil")
	if sinkNil.DeviceID() != "device-nil" {
		t.Fatalf("expected device-nil, got %s", sinkNil.DeviceID())
	}
	sinkNil.OnTurnStarted()
	sinkNil.OnThinking()
	sinkNil.OnToolCall("tool", "cmd")
	sinkNil.OnTextDelta("delta 1")
	sinkNil.OnResult(&runner.TurnResult{Response: "test complete"})
	sinkNil.OnResult(nil)

	// ZeroCapStruct
	sinkZero := &VoiceTurnSink{deviceID: "device-zero"}
	if sinkZero.DeviceID() != "device-zero" {
		t.Fatalf("expected device-zero, got %s", sinkZero.DeviceID())
	}
	sinkZero.OnTurnStarted()
	sinkZero.OnThinking()
	sinkZero.OnToolCall("tool", "cmd")
	sinkZero.OnTextDelta("test")
	if len(sinkZero.ringBuf) != 1 {
		t.Fatalf("expected 1 item, got %d", len(sinkZero.ringBuf))
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

func TestNewVoiceTurnSink_SentenceStreaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                      string
		cancelledCtx              bool
		deltas                    []string
		callResult                bool
		resultResponse            string
		wantSentencesBeforeResult []string
		wantSentences             []string
	}{
		{
			name:                      "SentenceStreamingAndResultFallback",
			deltas:                    []string{"First sentence. ", "Second incomplete"},
			callResult:                true,
			resultResponse:            "First sentence. Second incomplete",
			wantSentencesBeforeResult: []string{"First sentence."},
			wantSentences:             []string{"First sentence.", "Second incomplete"},
		},
		{
			name:           "ResultFallbackWithoutDeltas",
			callResult:     true,
			resultResponse: "Hello there. General Kenobi.",
			wantSentences:  []string{"Hello there.", "General Kenobi."},
		},
		{
			name:          "CancellationGuards",
			cancelledCtx:  true,
			deltas:        []string{"Should not be emitted because context is cancelled. "},
			wantSentences: nil,
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
			detector := NewSentenceDetector()
			onSentence := func(s string) {
				sentences = append(sentences, s)
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
		})
	}
}

func TestNewVoiceTurnSink_WaitAndDeltaRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		withDetector     bool
		deltas           []string
		callResult       bool
		resultResponse   string
		onErrorErr       error
		wantWaitResponse string
		wantWaitErrSub   string
	}{
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
			var detector *SentenceDetector
			var onSentence func(string)
			if tc.withDetector {
				detector = NewSentenceDetector()
				onSentence = func(s string) {}
			}

			sink := newVoiceTurnSink(ctx, nil, onSentence, detector)
			sink.OnTurnStarted()
			sink.OnThinking()

			for _, d := range tc.deltas {
				sink.OnTextDelta(d)
			}

			if tc.callResult {
				sink.OnResult(&runner.TurnResult{Response: tc.resultResponse})
			}

			if tc.onErrorErr != nil {
				sink.OnError(tc.onErrorErr)
			}

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
		})
	}
}
