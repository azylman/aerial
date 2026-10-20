package queue

import (
	"reflect"
	"strings"
	"testing"
)

func TestSentenceDetector_TableDriven(t *testing.T) {
	tests := []struct {
		name              string
		chunks            []string
		expected          []string
		minExpectedChunks int
	}{
		{"single complete sentence", []string{"Hello, world. "}, []string{"Hello, world."}, 0},
		{"token deltas streaming sentences", []string{"Hello", ", ", "Alex", ". ", "How", " are", " you?"}, []string{"Hello, Alex.", "How are you?"}, 0},
		{"multiple sentences in single delta", []string{"First sentence. Second sentence! Third sentence? "}, []string{"First sentence.", "Second sentence!", "Third sentence?"}, 0},
		{"decimal numbers are not split", []string{"The price is $3.14 today. ", "That is cheap."}, []string{"The price is $3.14 today.", "That is cheap."}, 0},
		{"abbreviations are not split", []string{"Dr. Smith visited e.g. the clinic at 5 p.m. yesterday. ", "All was well."}, []string{"Dr. Smith visited e.g. the clinic at 5 p.m. yesterday.", "All was well."}, 0},
		{"numbered list prefixes are not split", []string{"1. First item on the list. ", "2. Second item on the list."}, []string{"1. First item on the list.", "2. Second item on the list."}, 0},
		{"initials and acronyms are not split", []string{"Agent J. Doe arrived from the U.S. base today. ", "He is ready."}, []string{"Agent J. Doe arrived from the U.S. base today.", "He is ready."}, 0},
		{"ellipses followed by text", []string{"Thinking... wait for it. ", "Done."}, []string{"Thinking... wait for it.", "Done."}, 0},
		{"closing quotes after punctuation", []string{"She said, \"Turn off the lights!\" ", "And he did."}, []string{"She said, \"Turn off the lights!\"", "And he did."}, 0},
		{"double newline paragraph break", []string{"Here is the first paragraph with no period\n\nAnd here is the second paragraph."}, []string{"Here is the first paragraph with no period", "And here is the second paragraph."}, 0},
		{"unpunctuated text emitted on flush", []string{"Incomplete sentence with no ending punctuation"}, []string{"Incomplete sentence with no ending punctuation"}, 0},
		{"empty chunks, whitespace, and isolated punctuation", []string{"", "   ", "First sentence.   ", "", "  ", ".", "  "}, []string{"First sentence."}, 0},
		{"soft ceiling run on clause split", []string{strings.Repeat("word ", 26) + ", " + "this is a very long run-on clause that just keeps going and going and going without any period"}, nil, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sd := NewSentenceDetector()
			var got []string
			emit := func(s string) { got = append(got, s) }

			for _, c := range tc.chunks {
				sd.Feed(c, emit)
			}
			sd.Flush(emit)

			if tc.minExpectedChunks > 0 {
				if len(got) < tc.minExpectedChunks {
					t.Fatalf("expected at least %d chunks, got %d: %v", tc.minExpectedChunks, len(got), got)
				}
				return
			}
			if len(got) == 0 && len(tc.expected) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestSentenceDetector_IdempotentFlush(t *testing.T) {
	sd := NewSentenceDetector()
	var got []string
	emit := func(s string) { got = append(got, s) }

	sd.Feed("One sentence. Trailing thought", emit)
	sd.Flush(emit)
	sd.Flush(emit) // Second flush should be no-op

	expected := []string{"One sentence.", "Trailing thought"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("got %q, want %q", got, expected)
	}
}
