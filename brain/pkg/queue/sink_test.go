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

func TestThrowawayTurnSink_ResultDelivery(t *testing.T) {
	sink := NewThrowawayTurnSink()
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("bash", "echo hi")
	sink.OnTextDelta("delta")

	go func() {
		sink.OnResult(&runner.TurnResult{
			Response: "classification: coding",
		})
	}()

	res, err := sink.Result()
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if res != "classification: coding" {
		t.Fatalf("expected 'classification: coding', got: %q", res)
	}
}

func TestThrowawayTurnSink_NilResult(t *testing.T) {
	sink := NewThrowawayTurnSink()
	sink.OnResult(nil)

	res, err := sink.Result()
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if res != "" {
		t.Fatalf("expected empty string for nil TurnResult, got: %q", res)
	}
}

func TestThrowawayTurnSink_ErrorPath(t *testing.T) {
	sink := NewThrowawayTurnSink()
	expectedErr := errors.New("daemon execution failure")

	go func() {
		sink.OnError(expectedErr)
	}()

	res, err := sink.Result()
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
	if res != "" {
		t.Fatalf("expected empty string on error, got %q", res)
	}
}

func TestThrowawayTurnSink_OnceGuarded(t *testing.T) {
	sink := NewThrowawayTurnSink()
	sink.OnResult(&runner.TurnResult{Response: "first"})
	sink.OnResult(&runner.TurnResult{Response: "second"})
	sink.OnError(errors.New("should be ignored"))

	res, err := sink.Result()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res != "first" {
		t.Fatalf("expected 'first', got %q", res)
	}
}

func TestThrowawayTurnSink_ResultContext_Cancelled(t *testing.T) {
	sink := NewThrowawayTurnSink()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	res, err := sink.ResultContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}
	if res != "" {
		t.Fatalf("expected empty string on cancellation, got: %q", res)
	}
}

func TestThrowawayTurnSink_ResultContext_Success(t *testing.T) {
	sink := NewThrowawayTurnSink()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		sink.OnResult(&runner.TurnResult{Response: "context result ok"})
	}()

	res, err := sink.ResultContext(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if res != "context result ok" {
		t.Fatalf("expected 'context result ok', got: %q", res)
	}
}

func TestDiscordTurnSink_Lifecycle(t *testing.T) {
	mockEditor := &mockDiscordMessageEditor{}
	var completedRes *runner.TurnResult
	var completedErr error
	stoppedTyping := false

	sink := NewDiscordTurnSink(
		mockEditor,
		"chan-123",
		"msg-456",
		func(res *runner.TurnResult) {
			completedRes = res
		},
		func(err error) {
			completedErr = err
		},
	)
	sink.SetStopTyping(func() {
		stoppedTyping = true
	})

	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnToolCall("web_search", "search aerial")

	mockEditor.mu.Lock()
	if len(mockEditor.editCalls) != 1 {
		t.Fatalf("expected 1 edit call, got %d", len(mockEditor.editCalls))
	}
	if mockEditor.editCalls[0].content != "⚙️ Executing web_search..." {
		t.Fatalf("expected status edit '⚙️ Executing web_search...', got %q", mockEditor.editCalls[0].content)
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
	if len(mockEditor.deleteCalls) != 1 {
		t.Fatalf("expected 1 delete call, got %d", len(mockEditor.deleteCalls))
	}
	if mockEditor.deleteCalls[0].channelID != "chan-123" || mockEditor.deleteCalls[0].messageID != "msg-456" {
		t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls[0])
	}
	mockEditor.mu.Unlock()

	if completedRes != turnRes {
		t.Fatalf("expected onComplete callback with turnRes, got %+v", completedRes)
	}
	if completedErr != nil {
		t.Fatalf("expected nil completedErr, got %v", completedErr)
	}
}

func TestDiscordTurnSink_ErrorLifecycle(t *testing.T) {
	mockEditor := &mockDiscordMessageEditor{}
	var completedErr error
	stoppedTyping := false

	sink := NewDiscordTurnSink(
		mockEditor,
		"chan-err",
		"badge-err",
		func(res *runner.TurnResult) {
			t.Fatal("onComplete should not be called on error")
		},
		func(err error) {
			completedErr = err
		},
	)
	sink.SetStopTyping(func() {
		stoppedTyping = true
	})

	expectedErr := errors.New("turn timeout")
	sink.OnError(expectedErr)

	if !stoppedTyping {
		t.Fatal("expected stopTyping to have been called on error")
	}
	if !errors.Is(completedErr, expectedErr) {
		t.Fatalf("expected completedErr=%v, got %v", expectedErr, completedErr)
	}

	mockEditor.mu.Lock()
	if len(mockEditor.deleteCalls) != 1 {
		t.Fatalf("expected 1 delete call, got %d", len(mockEditor.deleteCalls))
	}
	if mockEditor.deleteCalls[0].channelID != "chan-err" || mockEditor.deleteCalls[0].messageID != "badge-err" {
		t.Fatalf("unexpected delete target: %+v", mockEditor.deleteCalls[0])
	}
	mockEditor.mu.Unlock()
}

func TestDiscordTurnSink_EditorErrorsLogged(t *testing.T) {
	mockEditor := &mockDiscordMessageEditor{
		editErr:   errors.New("edit network failure"),
		deleteErr: errors.New("delete 404 not found"),
	}

	sink := NewDiscordTurnSink(
		mockEditor,
		"chan-fail",
		"badge-fail",
		nil,
		nil,
	)

	// OnToolCall should log error without panic
	sink.OnToolCall("run_command", "git status")

	// OnResult should log delete error without panic
	sink.OnResult(&runner.TurnResult{Response: "ok"})

	// OnError should log delete error without panic
	sink.OnError(errors.New("subsequent error"))

	mockEditor.mu.Lock()
	if len(mockEditor.editCalls) != 1 {
		t.Fatalf("expected 1 edit call, got %d", len(mockEditor.editCalls))
	}
	if len(mockEditor.deleteCalls) != 2 {
		t.Fatalf("expected 2 delete calls, got %d", len(mockEditor.deleteCalls))
	}
	mockEditor.mu.Unlock()
}

func TestDiscordTurnSink_NilEditorAndEmptyBadge(t *testing.T) {
	// Test nil editor
	sinkNilEditor := NewDiscordTurnSink(nil, "chan-nil", "badge-nil", nil, nil)
	sinkNilEditor.OnToolCall("tool", "cmd")
	sinkNilEditor.OnResult(&runner.TurnResult{Response: "done"})
	sinkNilEditor.OnError(errors.New("err"))

	// Test empty badgeMessageID
	mockEditor := &mockDiscordMessageEditor{}
	sinkEmptyBadge := NewDiscordTurnSink(mockEditor, "chan-empty", "", nil, nil)
	sinkEmptyBadge.OnToolCall("tool", "cmd")
	sinkEmptyBadge.OnResult(&runner.TurnResult{Response: "done"})
	sinkEmptyBadge.OnError(errors.New("err"))

	mockEditor.mu.Lock()
	if len(mockEditor.editCalls) != 0 || len(mockEditor.deleteCalls) != 0 {
		t.Fatalf("expected 0 calls when badgeMessageID is empty, got edits=%d deletes=%d",
			len(mockEditor.editCalls), len(mockEditor.deleteCalls))
	}
	mockEditor.mu.Unlock()
}

func TestDiscordTurnSink_ConcurrentDeltas(t *testing.T) {
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

func TestVoiceTurnSink_RebindAndReplay(t *testing.T) {
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

	chunk1, ok := msgs1[0].(TokenChunk)
	if !ok || chunk1.Seq != 1 || chunk1.Delta != "Hello " {
		t.Fatalf("unexpected chunk1 on conn1: %+v", msgs1[0])
	}
	chunk2, ok := msgs1[1].(TokenChunk)
	if !ok || chunk2.Seq != 2 || chunk2.Delta != "world" {
		t.Fatalf("unexpected chunk2 on conn1: %+v", msgs1[1])
	}
	chunk3, ok := msgs1[2].(TokenChunk)
	if !ok || chunk3.Seq != 3 || chunk3.Delta != "!" {
		t.Fatalf("unexpected chunk3 on conn1: %+v", msgs1[2])
	}

	// Rebind with conn2, acknowledging only chunk 1. Chunks 2 and 3 should replay.
	conn2 := &mockWebSocketWriter{}
	if err := sink.Rebind(conn2, 1); err != nil {
		t.Fatalf("expected nil error on Rebind, got %v", err)
	}

	msgs2 := conn2.getMessages()
	if len(msgs2) != 2 {
		t.Fatalf("expected 2 replayed messages on conn2, got %d", len(msgs2))
	}
	rChunk2, ok := msgs2[0].(TokenChunk)
	if !ok || rChunk2.Seq != 2 || rChunk2.Delta != "world" {
		t.Fatalf("unexpected replayed chunk 2: %+v", msgs2[0])
	}
	rChunk3, ok := msgs2[1].(TokenChunk)
	if !ok || rChunk3.Seq != 3 || rChunk3.Delta != "!" {
		t.Fatalf("unexpected replayed chunk 3: %+v", msgs2[1])
	}

	// Sending new delta on conn2
	sink.OnTextDelta(" Welcome!")
	msgs2After := conn2.getMessages()
	if len(msgs2After) != 3 {
		t.Fatalf("expected 3 total messages on conn2, got %d", len(msgs2After))
	}
	chunk4, ok := msgs2After[2].(TokenChunk)
	if !ok || chunk4.Seq != 4 || chunk4.Delta != " Welcome!" {
		t.Fatalf("unexpected chunk 4: %+v", msgs2After[2])
	}
}

func TestVoiceTurnSink_RingBufferCapacityTrimming(t *testing.T) {
	conn := &mockWebSocketWriter{}
	sink := NewVoiceTurnSink(conn, "device-trim")

	// Send 1050 chunks into a ring buffer of capacity 1024
	for i := 1; i <= 1050; i++ {
		sink.OnTextDelta(fmt.Sprintf("chunk-%d", i))
	}

	msgs := conn.getMessages()
	if len(msgs) != 1050 {
		t.Fatalf("expected 1050 messages delivered to conn, got %d", len(msgs))
	}

	// Rebind with new connection and lastAckSeq = 0 to inspect entire retained ring buffer
	reconn := &mockWebSocketWriter{}
	if err := sink.Rebind(reconn, 0); err != nil {
		t.Fatalf("expected nil error on Rebind, got %v", err)
	}

	replayed := reconn.getMessages()
	if len(replayed) != 1024 {
		t.Fatalf("expected exactly 1024 retained chunks in ring buffer, got %d", len(replayed))
	}

	// Oldest chunk retained must be seq 27 (1050 - 1024 + 1)
	first, ok := replayed[0].(TokenChunk)
	if !ok || first.Seq != 27 || first.Delta != "chunk-27" {
		t.Fatalf("expected first retained chunk seq=27 delta='chunk-27', got %+v", replayed[0])
	}

	// Newest chunk retained must be seq 1050
	last, ok := replayed[1023].(TokenChunk)
	if !ok || last.Seq != 1050 || last.Delta != "chunk-1050" {
		t.Fatalf("expected last retained chunk seq=1050 delta='chunk-1050', got %+v", replayed[1023])
	}
}

func TestVoiceTurnSink_CustomZeroCap(t *testing.T) {
	// Directly constructed with ringCap = 0 should default to 1024 without panicking
	sink := &VoiceTurnSink{deviceID: "device-zero"}
	sink.OnTextDelta("test")
	if len(sink.ringBuf) != 1 {
		t.Fatalf("expected 1 item, got %d", len(sink.ringBuf))
	}
}

func TestVoiceTurnSink_Rebind_AckScenarios(t *testing.T) {
	sink := NewVoiceTurnSink(nil, "device-ack")

	// Send 5 chunks (seq 1..5)
	for i := 1; i <= 5; i++ {
		sink.OnTextDelta(fmt.Sprintf("c%d", i))
	}

	// Scenario A: lastAckSeq = 5 (all acked) -> 0 replayed
	connA := &mockWebSocketWriter{}
	if err := sink.Rebind(connA, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connA.getMessages()) != 0 {
		t.Fatalf("expected 0 replayed messages for lastAckSeq=5, got %d", len(connA.getMessages()))
	}

	// Scenario B: lastAckSeq = 0 (none acked) -> all 5 replayed
	connB := &mockWebSocketWriter{}
	if err := sink.Rebind(connB, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connB.getMessages()) != 5 {
		t.Fatalf("expected 5 replayed messages for lastAckSeq=0, got %d", len(connB.getMessages()))
	}

	// Scenario C: lastAckSeq = 3 -> 2 replayed (4 and 5)
	connC := &mockWebSocketWriter{}
	if err := sink.Rebind(connC, 3); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msgsC := connC.getMessages()
	if len(msgsC) != 2 {
		t.Fatalf("expected 2 replayed messages for lastAckSeq=3, got %d", len(msgsC))
	}
	if c4, ok := msgsC[0].(TokenChunk); !ok || c4.Seq != 4 {
		t.Fatalf("expected seq 4, got %+v", msgsC[0])
	}
	if c5, ok := msgsC[1].(TokenChunk); !ok || c5.Seq != 5 {
		t.Fatalf("expected seq 5, got %+v", msgsC[1])
	}

	// Scenario D: lastAckSeq = 10 (beyond buffer) -> 0 replayed
	connD := &mockWebSocketWriter{}
	if err := sink.Rebind(connD, 10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connD.getMessages()) != 0 {
		t.Fatalf("expected 0 replayed messages for lastAckSeq=10, got %d", len(connD.getMessages()))
	}

	// Scenario E: newConn == nil -> returns nil, clears connection
	if err := sink.Rebind(nil, 0); err != nil {
		t.Fatalf("expected nil error when rebinding to nil conn, got %v", err)
	}
	sink.OnTextDelta("c6")
	// Now rebind with connE, chunk 6 (seq 6) is in the buffer
	connE := &mockWebSocketWriter{}
	if err := sink.Rebind(connE, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msgsE := connE.getMessages()
	if len(msgsE) != 1 {
		t.Fatalf("expected 1 message replayed, got %d", len(msgsE))
	}
	if c6, ok := msgsE[0].(TokenChunk); !ok || c6.Seq != 6 || c6.Delta != "c6" {
		t.Fatalf("expected seq 6 delta 'c6', got %+v", msgsE[0])
	}
}

func TestVoiceTurnSink_Rebind_ErrorPropagation(t *testing.T) {
	conn1 := &mockWebSocketWriter{}
	sink := NewVoiceTurnSink(conn1, "device-err-prop")

	sink.OnTextDelta("chunk1")
	sink.OnTextDelta("chunk2")

	connFail := &mockWebSocketWriter{
		writeErr: errors.New("socket broken pipe"),
	}

	err := sink.Rebind(connFail, 0)
	if err == nil {
		t.Fatal("expected error on Rebind with failing writer, got nil")
	}
	if !strings.Contains(err.Error(), "failed to replay chunk seq=1") {
		t.Fatalf("expected wrapped error containing 'failed to replay chunk seq=1', got: %v", err)
	}
	if !strings.Contains(err.Error(), "socket broken pipe") {
		t.Fatalf("expected wrapped error containing original cause, got: %v", err)
	}
}

func TestVoiceTurnSink_OnResult_MessageAndClear(t *testing.T) {
	conn := &mockWebSocketWriter{}
	sink := NewVoiceTurnSink(conn, "device-result")

	sink.OnTextDelta("chunk1")
	sink.OnTextDelta("chunk2")

	res := &runner.TurnResult{Response: "test complete"}
	sink.OnResult(res)

	msgs := conn.getMessages()
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	endMsg, ok := msgs[2].(map[string]string)
	if !ok || endMsg["type"] != "end_of_turn" {
		t.Fatalf("expected end_of_turn message, got %+v", msgs[2])
	}

	// Verify buffer is cleared
	reconn := &mockWebSocketWriter{}
	if err := sink.Rebind(reconn, 0); err != nil {
		t.Fatalf("unexpected rebind error: %v", err)
	}
	if len(reconn.getMessages()) != 0 {
		t.Fatalf("expected empty ring buffer after OnResult, got %d messages", len(reconn.getMessages()))
	}

	// Test OnResult with nil
	sink.OnResult(nil)
}

func TestVoiceTurnSink_OnError_MessageAndClear(t *testing.T) {
	conn := &mockWebSocketWriter{}
	sink := NewVoiceTurnSink(conn, "device-error")

	sink.OnTextDelta("chunk1")
	sink.OnError(errors.New("timeout reached"))

	msgs := conn.getMessages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	errMsg, ok := msgs[1].(map[string]string)
	if !ok || errMsg["type"] != "error" || errMsg["error"] != "timeout reached" {
		t.Fatalf("expected error message with 'timeout reached', got %+v", msgs[1])
	}

	// Verify buffer is cleared
	reconn := &mockWebSocketWriter{}
	if err := sink.Rebind(reconn, 0); err != nil {
		t.Fatalf("unexpected rebind error: %v", err)
	}
	if len(reconn.getMessages()) != 0 {
		t.Fatalf("expected empty ring buffer after OnError, got %d messages", len(reconn.getMessages()))
	}

	// Test OnError with nil err
	sink.OnError(nil)
	lastMsgs := reconn.getMessages()
	if len(lastMsgs) == 0 {
		t.Fatal("expected at least 1 message on reconn after OnError(nil)")
	}
	lastErr, ok := lastMsgs[len(lastMsgs)-1].(map[string]string)
	if !ok || lastErr["type"] != "error" || lastErr["error"] != "" {
		t.Fatalf("expected error message with empty error string for nil err, got %+v", lastErr)
	}
}

func TestVoiceTurnSink_WriteErrorsLoggedWithoutPanic(t *testing.T) {
	connFail := &mockWebSocketWriter{
		writeErr: errors.New("simulated network failure"),
	}
	sink := NewVoiceTurnSink(connFail, "device-fail")

	// None of these should panic even when WriteJSON returns an error
	sink.OnTextDelta("token")
	sink.OnResult(&runner.TurnResult{Response: "done"})
	sink.OnError(errors.New("subsequent error"))
}

func TestVoiceTurnSink_NilConnection(t *testing.T) {
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
}

func TestVoiceTurnSink_ConcurrentAccess(t *testing.T) {
	conn := &mockWebSocketWriter{}
	sink := NewVoiceTurnSink(conn, "device-concurrent")

	var wg sync.WaitGroup
	// 10 goroutines sending 50 deltas each
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				sink.OnTextDelta(fmt.Sprintf("worker-%d-chunk-%d", workerID, j))
			}
		}(i)
	}

	// Concurrent rebind
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_ = sink.Rebind(&mockWebSocketWriter{}, 0)
		}
	}()

	wg.Wait()
}

func TestTurnSinks_NoopInterfaceMethods(t *testing.T) {
	// ThrowawayTurnSink
	tSink := NewThrowawayTurnSink()
	tSink.OnTurnStarted()
	tSink.OnThinking()
	tSink.OnToolCall("test-tool", "test-cmd")
	tSink.OnTextDelta("delta")

	// DiscordTurnSink
	dSink := NewDiscordTurnSink(nil, "chan-1", "msg-1", nil, nil)
	dSink.OnTurnStarted()
	dSink.OnThinking()

	// voiceTurnSink
	var voiceStatus string
	vSink := &voiceTurnSink{
		resCh:    make(chan *runner.TurnResult, 2),
		errCh:    make(chan error, 2),
		onStatus: func(s string) { voiceStatus = s },
	}
	vSink.OnTurnStarted()
	vSink.OnThinking()
	vSink.OnTextDelta("voice-delta")
	vSink.OnToolCall("run_cmd", "sleep 1")
	if voiceStatus == "" {
		t.Errorf("expected non-empty voice status")
	}
	vSink.OnToolCall("", "")
	vSinkNilStatus := &voiceTurnSink{
		resCh: make(chan *runner.TurnResult, 2),
		errCh: make(chan error, 2),
	}
	vSinkNilStatus.OnToolCall("tool", "cmd")
	vSink.OnResult(&runner.TurnResult{Response: "voice-1"})
	vSink.OnResult(&runner.TurnResult{Response: "voice-2"})
	vSink.OnError(errors.New("err-1"))
	vSink.OnError(errors.New("err-2"))

	// turnResultSink
	rSink := &turnResultSink{
		resCh: make(chan *runner.TurnResult, 2),
		errCh: make(chan error, 2),
	}
	rSink.OnTurnStarted()
	rSink.OnThinking()
	rSink.OnTextDelta("turn-delta")
	rSink.OnToolCall("tool", "cmd")
	rSink.OnToolCall("", "")
	// VoiceTurnSink no-ops
	voiceWriter := &mockWebSocketWriter{}
	voiceSink := NewVoiceTurnSink(voiceWriter, "device-noop")
	voiceSink.OnTurnStarted()
	voiceSink.OnThinking()
	voiceSink.OnToolCall("tool", "cmd")
}

func TestThrowawayTurnSink_ResultContext(t *testing.T) {
	// 1. Pre-cancelled context
	sink1 := NewThrowawayTurnSink()
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := sink1.ResultContext(cancelledCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// 2. Result delivered successfully
	sink2 := NewThrowawayTurnSink()
	sink2.OnResult(&runner.TurnResult{Response: "hello-ctx"})
	res, err := sink2.ResultContext(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res != "hello-ctx" {
		t.Fatalf("expected 'hello-ctx', got %q", res)
	}

	// 3. Error delivered successfully
	sink3 := NewThrowawayTurnSink()
	expectedErr := errors.New("turn failure")
	sink3.OnError(expectedErr)
	_, err = sink3.ResultContext(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}

	// 4. Cancelled while waiting
	sink4 := NewThrowawayTurnSink()
	ctxWait, cancelWait := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancelWait()
	}()
	_, err = sink4.ResultContext(ctxWait)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled while waiting, got %v", err)
	}
}

func TestTurnSinks_NoopCallbacks(t *testing.T) {
	t.Parallel()
	tt := NewThrowawayTurnSink()
	tt.OnTurnStarted()
	tt.OnThinking()
	tt.OnToolCall("test_tool", "echo")
	tt.OnTextDelta("delta")

	dt := NewDiscordTurnSink(nil, "chan", "msg", nil, nil)
	dt.OnTurnStarted()
	dt.OnThinking()

	vt := NewVoiceTurnSink(nil, "kiosk")
	vt.OnTurnStarted()
	vt.OnThinking()
	vt.OnToolCall("test_tool", "echo")
}





