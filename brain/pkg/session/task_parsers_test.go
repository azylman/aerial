package session_test

import (
	"testing"

	"github.com/azylman/aerial/brain/pkg/session"
)

func TestParseBackgroundTaskStarted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "standard output",
			input:    "Tool is running as a background task with task id: task-123\nTask description: test",
			expected: "task-123",
		},
		{
			name:     "quoted id",
			input:    `Tool is running as a background task with task id: "task-456"`,
			expected: "task-456",
		},
		{
			name:     "single quotes",
			input:    "tool is running as a background task with task id: 'prefix/task-789'",
			expected: "prefix/task-789",
		},
		{
			name:     "extra punctuation and whitespace",
			input:    "Tool is running as a background task with task id:    task-abc; trailing",
			expected: "task-abc",
		},
		{
			name:     "trailing period in sentence",
			input:    "Tool is running as a background task with task id: task-sentence-end.",
			expected: "task-sentence-end",
		},
		{
			name:     "punctuation only token",
			input:    "Tool is running as a background task with task id: ;;;;",
			expected: "",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "no task id",
			input:    "Tool is running as a background task with task id:   ",
			expected: "",
		},
		{
			name:     "unrelated string",
			input:    "Running command in foreground",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := session.ParseBackgroundTaskStarted(tc.input)
			if got != tc.expected {
				t.Errorf("ParseBackgroundTaskStarted(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestParseTaskMessageSender(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "standard message sender",
			input:    "[Message] sender=task-sender-42 priority=HIGH content=done",
			expected: "task-sender-42",
		},
		{
			name:     "sender with quotes",
			input:    `[Message] sender="task-sender-suffix" priority=NORMAL`,
			expected: "task-sender-suffix",
		},
		{
			name:     "sender with single quotes and path",
			input:    "[Message] sender='prefix/task-sender-99' content=done",
			expected: "prefix/task-sender-99",
		},
		{
			name:     "uppercase SENDER",
			input:    "[Message] SENDER=task-upper-10 content=ok",
			expected: "task-upper-10",
		},
		{
			name:     "empty or missing sender",
			input:    "[Message] priority=HIGH content=done",
			expected: "",
		},
		{
			name:     "sender with trailing punctuation",
			input:    "[Message] sender=task-comma, priority=HIGH",
			expected: "task-comma",
		},
		{
			name:     "sender with trailing period",
			input:    "[Message] sender=task-period. priority=HIGH",
			expected: "task-period",
		},
		{
			name:     "sender with whitespace only",
			input:    "[Message] sender=    ",
			expected: "",
		},
		{
			name:     "sender with punctuation only",
			input:    "[Message] sender=;;; ",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := session.ParseTaskMessageSender(tc.input)
			if got != tc.expected {
				t.Errorf("ParseTaskMessageSender(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestParseTaskFinishedContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "quoted id standard",
			input:    `Task id "task-fin-99" finished with result: ok`,
			expected: "task-fin-99",
		},
		{
			name:     "path id with quotes",
			input:    `Task id "prefix/task-fin-suffix" finished with result: ok`,
			expected: "prefix/task-fin-suffix",
		},
		{
			name:     "unquoted id",
			input:    "Task id task-simple finished with result:\nSuccess",
			expected: "task-simple",
		},
		{
			name:     "single quotes",
			input:    "Task id 'task-single' finished with result:",
			expected: "task-single",
		},
		{
			name:     "trailing period before finished marker",
			input:    "Task id task-dot. finished with result:",
			expected: "task-dot",
		},
		{
			name:     "multiple task id occurrences in message preceding completion",
			input:    "Tool is running as a background task with task id: task-multi-prev\nTask id \"task-multi-prev\" finished with result: ok",
			expected: "task-multi-prev",
		},
		{
			name:     "missing task id before finished marker",
			input:    "Build finished with result: ok",
			expected: "",
		},
		{
			name:     "punctuation only token",
			input:    "Task id ;;;; finished with result: ok",
			expected: "",
		},
		{
			name:     "reject multi-word false positive",
			input:    "Task id is required for cancellation. Build finished with result: ok",
			expected: "",
		},
		{
			name:     "empty id",
			input:    "Task id finished with result:",
			expected: "",
		},
		{
			name:     "unrelated content",
			input:    "All tasks completed successfully",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := session.ParseTaskFinishedContent(tc.input)
			if got != tc.expected {
				t.Errorf("ParseTaskFinishedContent(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestIndexFoldAndLastIndexFold(t *testing.T) {
	t.Parallel()

	t.Run("IndexFold branches", func(t *testing.T) {
		t.Parallel()
		if got := session.IndexFoldForTest("hello", ""); got != 0 {
			t.Errorf("expected 0 for empty substr, got %d", got)
		}
		if got := session.IndexFoldForTest("hi", "hello"); got != -1 {
			t.Errorf("expected -1 when string shorter, got %d", got)
		}
		if got := session.IndexFoldForTest("Hello World", "WORLD"); got != 6 {
			t.Errorf("expected 6 for uppercase match, got %d", got)
		}
		if got := session.IndexFoldForTest("123 ABC 456", "ABC"); got != 4 {
			t.Errorf("expected 4, got %d", got)
		}
		if got := session.IndexFoldForTest("123 ABC 456", "123"); got != 0 {
			t.Errorf("expected 0 for non-letter match, got %d", got)
		}
		if got := session.IndexFoldForTest("hello", "xyz"); got != -1 {
			t.Errorf("expected -1 for not found, got %d", got)
		}
	})

	t.Run("LastIndexFold branches", func(t *testing.T) {
		t.Parallel()
		if got := session.LastIndexFoldForTest("hello", ""); got != 5 {
			t.Errorf("expected 5 for empty substr, got %d", got)
		}
		if got := session.LastIndexFoldForTest("hi", "hello"); got != -1 {
			t.Errorf("expected -1 when string shorter, got %d", got)
		}
		if got := session.LastIndexFoldForTest("hello WORLD hello", "WORLD"); got != 6 {
			t.Errorf("expected 6 for match, got %d", got)
		}
		if got := session.LastIndexFoldForTest("123 456 123", "123"); got != 8 {
			t.Errorf("expected 8 for non-letter match, got %d", got)
		}
		if got := session.LastIndexFoldForTest("hello", "xyz"); got != -1 {
			t.Errorf("expected -1 for not found, got %d", got)
		}
	})
}
