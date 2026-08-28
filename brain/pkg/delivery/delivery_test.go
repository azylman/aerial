package delivery

import (
	"strings"
	"testing"
)

func TestSplitMessage(t *testing.T) {
	// 1. Empty string
	if res := SplitMessage("", 2000); len(res) != 0 {
		t.Errorf("Expected 0 chunks for empty string, got %d", len(res))
	}

	// 2. Short text within limit
	short := "Hello world! This is a simple test message."
	res := SplitMessage(short, 2000)
	if len(res) != 1 || res[0] != short {
		t.Errorf("Expected 1 chunk matching input, got %+v", res)
	}

	// 3. Multi-paragraph text exceeding limit
	p1 := strings.Repeat("A", 1200)
	p2 := strings.Repeat("B", 1200)
	text := p1 + "\n\n" + p2
	res = SplitMessage(text, 2000)
	if len(res) != 2 {
		t.Fatalf("Expected 2 chunks, got %d", len(res))
	}
	for i, c := range res {
		if len([]rune(c)) > 2000 {
			t.Errorf("Chunk %d exceeds 2000 runes: %d", i, len([]rune(c)))
		}
	}
	if res[0] != p1 || res[1] != p2 {
		t.Errorf("Chunks did not split cleanly on paragraph boundary")
	}

	// 4. Large single continuous string without spaces
	giant := strings.Repeat("X", 4500)
	res = SplitMessage(giant, 2000)
	if len(res) != 3 {
		t.Fatalf("Expected 3 chunks for 4500 char string, got %d", len(res))
	}
	if len([]rune(res[0])) != 2000 || len([]rune(res[1])) != 2000 || len([]rune(res[2])) != 500 {
		t.Errorf("Unexpected chunk sizes: %d, %d, %d", len([]rune(res[0])), len([]rune(res[1])), len([]rune(res[2])))
	}

	// 5. Small custom limit test (limit = 20)
	words := "The quick brown fox jumps over the lazy dog"
	res = SplitMessage(words, 20)
	for i, c := range res {
		if len([]rune(c)) > 20 {
			t.Errorf("Chunk %d exceeds limit 20: len=%d (%q)", i, len([]rune(c)), c)
		}
	}
	joined := strings.Join(res, " ")
	if strings.ReplaceAll(joined, "  ", " ") != words {
		t.Errorf("Reconstructed words mismatch: %q vs %q", joined, words)
	}
}

func TestSendMessageNilSession(t *testing.T) {
	err := SendMessage(nil, "12345", "hello")
	if err == nil {
		t.Error("Expected error when sending message with nil session")
	}

	err = SendMessage(nil, "", "hello")
	if err == nil {
		t.Error("Expected error when sending message with empty channel ID")
	}
}

func TestStartTypingNilSessionAndStop(t *testing.T) {
	stop := StartTyping(nil, "12345")
	if stop == nil {
		t.Fatal("Expected non-nil stop function")
	}
	// Calling stop should not panic
	stop()
	stop() // multiple calls should be safe

	stop = StartTyping(nil, "")
	stop()
}

func TestSplitMessageUnicode(t *testing.T) {
	// Verify unicode / emoji safety with 2000 rune limit
	sparkles := strings.Repeat("???", 1500)
	res := SplitMessage(sparkles, 2000)
	if len(res) < 2 {
		t.Fatalf("Expected at least 2 chunks, got %d", len(res))
	}
	for i, chunk := range res {
		if len([]rune(chunk)) > 2000 {
			t.Errorf("Chunk %d exceeds 2000 runes: %d", i, len([]rune(chunk)))
		}
	}
}

func TestSplitMessageMarkdownFences(t *testing.T) {
	codeSnippet := "```python\nline1 = 'hello'\nline2 = 'world'\nline3 = 'foo'\nline4 = 'bar'\n```"
	// Split with a small limit such that it breaks inside the code fence
	res := SplitMessage(codeSnippet, 35)

	if len(res) < 2 {
		t.Fatalf("Expected multiple chunks, got %d", len(res))
	}

	for i, chunk := range res {
		if len([]rune(chunk)) > 35 {
			t.Errorf("Chunk %d exceeds limit 35: len=%d (%q)", i, len([]rune(chunk)), chunk)
		}
		// Count occurrences of ``` in this chunk
		fenceCount := strings.Count(chunk, "```")
		if fenceCount%2 != 0 {
			t.Errorf("Chunk %d has unclosed/odd code fences (%d fences): %q", i, fenceCount, chunk)
		}
	}

	// First chunk should close with ```
	if !strings.HasSuffix(res[0], "```") {
		t.Errorf("Expected first chunk to close with ```, got %q", res[0])
	}

	// Subsequent chunk inside code block should reopen with ```python
	if !strings.HasPrefix(res[1], "```python") {
		t.Errorf("Expected second chunk to reopen with ```python, got %q", res[1])
	}
}
