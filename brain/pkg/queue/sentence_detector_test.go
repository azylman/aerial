package queue

import (
	"reflect"
	"strings"
	"testing"
)

func TestSentenceDetector_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		chunks   []string
		expected []string
	}{
		{
			name:     "single complete sentence",
			chunks:   []string{"Hello, world. "},
			expected: []string{"Hello, world."},
		},
		{
			name:     "token deltas streaming sentences",
			chunks:   []string{"Hello", ", ", "Alex", ". ", "How", " are", " you?"},
			expected: []string{"Hello, Alex.", "How are you?"},
		},
		{
			name:     "multiple sentences in single delta",
			chunks:   []string{"First sentence. Second sentence! Third sentence? "},
			expected: []string{"First sentence.", "Second sentence!", "Third sentence?"},
		},
		{
			name:     "decimal numbers are not split",
			chunks:   []string{"The price is $3.14 today. ", "That is cheap."},
			expected: []string{"The price is $3.14 today.", "That is cheap."},
		},
		{
			name:     "abbreviations are not split",
			chunks:   []string{"Dr. Smith visited e.g. the clinic at 5 p.m. yesterday. ", "All was well."},
			expected: []string{"Dr. Smith visited e.g. the clinic at 5 p.m. yesterday.", "All was well."},
		},
		{
			name:     "numbered list prefixes are not split",
			chunks:   []string{"1. First item on the list. ", "2. Second item on the list."},
			expected: []string{"1. First item on the list.", "2. Second item on the list."},
		},
		{
			name:     "initials and acronyms are not split",
			chunks:   []string{"Agent J. Doe arrived from the U.S. base today. ", "He is ready."},
			expected: []string{"Agent J. Doe arrived from the U.S. base today.", "He is ready."},
		},
		{
			name:     "ellipses followed by text",
			chunks:   []string{"Thinking... wait for it. ", "Done."},
			expected: []string{"Thinking... wait for it.", "Done."},
		},
		{
			name:     "closing quotes after punctuation",
			chunks:   []string{"She said, \"Turn off the lights!\" ", "And he did."},
			expected: []string{"She said, \"Turn off the lights!\"", "And he did."},
		},
		{
			name:     "double newline paragraph break",
			chunks:   []string{"Here is the first paragraph with no period\n\nAnd here is the second paragraph."},
			expected: []string{"Here is the first paragraph with no period", "And here is the second paragraph."},
		},
		{
			name:     "unpunctuated text emitted on flush",
			chunks:   []string{"Incomplete sentence with no ending punctuation"},
			expected: []string{"Incomplete sentence with no ending punctuation"},
		},
		{
			name:     "empty chunks, whitespace, and isolated punctuation",
			chunks:   []string{"", "   ", "First sentence.   ", "", "  ", ".", "  "},
			expected: []string{"First sentence."},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sd := NewSentenceDetector()
			var got []string
			emit := func(s string) {
				got = append(got, s)
			}

			for _, c := range tc.chunks {
				sd.Feed(c, emit)
			}
			sd.Flush(emit)

			if len(got) == 0 && len(tc.expected) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestSentenceDetector_SoftCeilingRunOn(t *testing.T) {
	sd := NewSentenceDetector()
	var got []string
	emit := func(s string) {
		got = append(got, s)
	}

	// Generate a 200+ char run-on clause without a period, but with a comma after char 130
	part1 := strings.Repeat("word ", 26) // ~130 chars
	part2 := "this is a very long run-on clause that just keeps going and going and going without any period"
	full := part1 + ", " + part2

	sd.Feed(full, emit)
	sd.Flush(emit)

	if len(got) < 2 {
		t.Fatalf("expected soft ceiling to split run-on clause, got %d chunks: %v", len(got), got)
	}
}

func TestSentenceDetector_IdempotentFlush(t *testing.T) {
	sd := NewSentenceDetector()
	var got []string
	emit := func(s string) {
		got = append(got, s)
	}

	sd.Feed("One sentence. Trailing thought", emit)
	sd.Flush(emit)
	sd.Flush(emit) // Second flush should be no-op

	expected := []string{"One sentence.", "Trailing thought"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("got %q, want %q", got, expected)
	}
}
