package session_test

import (
	"testing"

	"github.com/azylman/aerial/brain/pkg/session"
)

func TestTaskParsers(t *testing.T) {
	t.Parallel()

	startedCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"standard output", "Tool is running as a background task with task id: task-123\nTask description: test", "task-123"},
		{"quoted id", `Tool is running as a background task with task id: "task-456"`, "task-456"},
		{"single quotes", "tool is running as a background task with task id: 'prefix/task-789'", "prefix/task-789"},
		{"extra punctuation and whitespace", "Tool is running as a background task with task id:    task-abc; trailing", "task-abc"},
		{"trailing period in sentence", "Tool is running as a background task with task id: task-sentence-end.", "task-sentence-end"},
		{"punctuation only token", "Tool is running as a background task with task id: ;;;;", ""},
		{"empty string", "", ""},
		{"no task id", "Tool is running as a background task with task id:   ", ""},
		{"unrelated string", "Running command in foreground", ""},
	}
	for _, tc := range startedCases {
		t.Run("started_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := session.ParseBackgroundTaskStarted(tc.input); got != tc.expected {
				t.Errorf("ParseBackgroundTaskStarted(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}

	senderCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"standard message sender", "[Message] sender=task-sender-42 priority=HIGH content=done", "task-sender-42"},
		{"sender with quotes", `[Message] sender="task-sender-suffix" priority=NORMAL`, "task-sender-suffix"},
		{"sender with single quotes and path", "[Message] sender='prefix/task-sender-99' content=done", "prefix/task-sender-99"},
		{"uppercase SENDER", "[Message] SENDER=task-upper-10 content=ok", "task-upper-10"},
		{"empty or missing sender", "[Message] priority=HIGH content=done", ""},
		{"sender with trailing punctuation", "[Message] sender=task-comma, priority=HIGH", "task-comma"},
		{"sender with trailing period", "[Message] sender=task-period. priority=HIGH", "task-period"},
		{"sender with whitespace only", "[Message] sender=    ", ""},
		{"sender with punctuation only", "[Message] sender=;;; ", ""},
	}
	for _, tc := range senderCases {
		t.Run("sender_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := session.ParseTaskMessageSender(tc.input); got != tc.expected {
				t.Errorf("ParseTaskMessageSender(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}

	finishedCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"quoted id standard", `Task id "task-fin-99" finished with result: ok`, "task-fin-99"},
		{"path id with quotes", `Task id "prefix/task-fin-suffix" finished with result: ok`, "prefix/task-fin-suffix"},
		{"unquoted id", "Task id task-simple finished with result:\nSuccess", "task-simple"},
		{"single quotes", "Task id 'task-single' finished with result:", "task-single"},
		{"trailing period before finished marker", "Task id task-dot. finished with result:", "task-dot"},
		{"multiple task id occurrences in message preceding completion", "Tool is running as a background task with task id: task-multi-prev\nTask id \"task-multi-prev\" finished with result: ok", "task-multi-prev"},
		{"missing task id before finished marker", "Build finished with result: ok", ""},
		{"punctuation only token", "Task id ;;;; finished with result: ok", ""},
		{"reject multi-word false positive", "Task id is required for cancellation. Build finished with result: ok", ""},
		{"empty id", "Task id finished with result:", ""},
		{"unrelated content", "All tasks completed successfully", ""},
	}
	for _, tc := range finishedCases {
		t.Run("finished_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := session.ParseTaskFinishedContent(tc.input); got != tc.expected {
				t.Errorf("ParseTaskFinishedContent(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestIndexFoldAndLastIndexFold(t *testing.T) {
	t.Parallel()

	indexTests := []struct {
		name   string
		s      string
		substr string
		want   int
	}{
		{"empty substr", "hello", "", 0},
		{"string shorter", "hi", "hello", -1},
		{"uppercase match", "Hello World", "WORLD", 6},
		{"exact match", "123 ABC 456", "ABC", 4},
		{"non-letter match", "123 ABC 456", "123", 0},
		{"not found", "hello", "xyz", -1},
	}
	for _, tc := range indexTests {
		t.Run("index_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := session.IndexFoldForTest(tc.s, tc.substr); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}

	lastIndexTests := []struct {
		name   string
		s      string
		substr string
		want   int
	}{
		{"empty substr", "hello", "", 5},
		{"string shorter", "hi", "hello", -1},
		{"match", "hello WORLD hello", "WORLD", 6},
		{"non-letter match", "123 456 123", "123", 8},
		{"not found", "hello", "xyz", -1},
	}
	for _, tc := range lastIndexTests {
		t.Run("lastIndex_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := session.LastIndexFoldForTest(tc.s, tc.substr); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}
