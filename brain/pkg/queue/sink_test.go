package queue

import (
	"errors"
	"sync"
	"testing"

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
