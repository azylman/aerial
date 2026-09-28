package queue

import (
	"strings"
	"sync"
	"unicode"
)

var knownAbbreviations = map[string]bool{
	"mr":     true,
	"mrs":    true,
	"ms":     true,
	"dr":     true,
	"prof":   true,
	"sr":     true,
	"jr":     true,
	"vs":     true,
	"etc":    true,
	"e.g":    true,
	"eg":     true,
	"i.e":    true,
	"ie":     true,
	"a.m":    true,
	"am":     true,
	"p.m":    true,
	"pm":     true,
	"approx": true,
	"dept":   true,
	"est":    true,
	"fig":    true,
	"inc":    true,
	"ltd":    true,
	"gen":    true,
	"gov":    true,
	"sgt":    true,
	"cpt":    true,
	"lt":     true,
	"no":     true,
	"vol":    true,
}

// SentenceDetector buffers streaming text tokens and detects natural sentence boundaries
// to emit complete sentences for low-latency speech synthesis.
type SentenceDetector struct {
	mu     sync.Mutex
	buffer strings.Builder
}

// NewSentenceDetector creates an empty SentenceDetector.
func NewSentenceDetector() *SentenceDetector {
	return &SentenceDetector{}
}

// Feed appends a text delta and emits any completed sentences found.
func (sd *SentenceDetector) Feed(delta string, emit func(sentence string)) {
	if delta == "" {
		return
	}

	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.buffer.WriteString(delta)
	sd.drain(false, emit)
}

// Flush emits any remaining buffered text as the final sentence.
func (sd *SentenceDetector) Flush(emit func(sentence string)) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.drain(true, emit)
}

// drain scans the buffer for sentence boundaries. If isFinal is true, any trailing
// non-empty content is emitted.
func (sd *SentenceDetector) drain(isFinal bool, emit func(sentence string)) {
	txt := sd.buffer.String()
	if txt == "" {
		return
	}

	for {
		splitIdx, endIdx := findSentenceBoundary(txt)
		if splitIdx == -1 {
			break
		}

		sentence := strings.TrimSpace(txt[:endIdx])
		if isEmittable(sentence) && emit != nil {
			emit(sentence)
		}

		txt = strings.TrimLeft(txt[endIdx:], " \t\r\n")
	}

	if isFinal {
		trailing := strings.TrimSpace(txt)
		if isEmittable(trailing) && emit != nil {
			emit(trailing)
		}
		sd.buffer.Reset()
		return
	}

	sd.buffer.Reset()
	sd.buffer.WriteString(txt)
}

func isEmittable(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

func findSentenceBoundary(txt string) (int, int) {
	runes := []rune(txt)
	n := len(runes)

	for i := 0; i < n; i++ {
		r := runes[i]

		// Paragraph boundary: double newline
		if r == '\n' && i+1 < n && runes[i+1] == '\n' {
			endIdx := i + 2
			for endIdx < n && unicode.IsSpace(runes[endIdx]) {
				endIdx++
			}
			byteSplit := len(string(runes[:i]))
			byteEnd := len(string(runes[:endIdx]))
			return byteSplit, byteEnd
		}

		if r != '.' && r != '!' && r != '?' {
			continue
		}

		// Handle ellipses: if consecutive dots, skip until the last dot
		isEllipsis := false
		if r == '.' {
			if i+1 < n && runes[i+1] == '.' {
				isEllipsis = true
				for i+1 < n && runes[i+1] == '.' {
					i++
				}
			}

			// Decimal number check: digit before and digit after (e.g. 3.14)
			if i > 0 && unicode.IsDigit(runes[i-1]) && i+1 < n && unicode.IsDigit(runes[i+1]) {
				continue
			}

			// Numbered list prefix check: e.g. "1. " or "2. "
			if isNumberedListPrefix(runes, i) {
				continue
			}

			// Abbreviation / single-letter initial check
			if isAbbrev(runes, i) {
				continue
			}
		}

		// Advance past closing quotes/brackets: ", ', ), ], ”, ’
		endIdx := i + 1
		for endIdx < n && isClosingPunctuation(runes[endIdx]) {
			endIdx++
		}

		// Boundary requires whitespace (or newline) after punctuation/closing quotes
		if endIdx < n && unicode.IsSpace(runes[endIdx]) {
			if isEllipsis {
				// Peek ahead: if followed by lowercase letter, treat as continuation
				nextNonSpace := endIdx
				for nextNonSpace < n && unicode.IsSpace(runes[nextNonSpace]) {
					nextNonSpace++
				}
				if nextNonSpace < n && unicode.IsLower(runes[nextNonSpace]) {
					continue
				}
			}
			// Convert rune offsets to byte indices in txt
			byteSplit := len(string(runes[:i+1]))
			byteEnd := len(string(runes[:endIdx]))
			return byteSplit, byteEnd
		}
	}

	// Soft ceiling: if buffer exceeds 180 chars without terminal punctuation,
	// look for clause punctuation (, ; :) followed by space to avoid blocking playback
	if n > 180 {
		for i := 120; i < n; i++ {
			r := runes[i]
			if (r == ',' || r == ';' || r == ':') && i+1 < n && unicode.IsSpace(runes[i+1]) {
				byteSplit := len(string(runes[:i+1]))
				byteEnd := len(string(runes[:i+1]))
				return byteSplit, byteEnd
			}
		}
	}

	return -1, -1
}

func isClosingPunctuation(r rune) bool {
	switch r {
	case '"', '\'', ')', ']', '}', '”', '’', '»':
		return true
	default:
		return false
	}
}

func isNumberedListPrefix(runes []rune, dotIdx int) bool {
	start := dotIdx - 1
	for start >= 0 && unicode.IsDigit(runes[start]) {
		start--
	}
	// All digits between start and dotIdx. Check if preceded by line start or space.
	if start < dotIdx-1 && (start < 0 || runes[start] == '\n' || runes[start] == ' ' || runes[start] == '\t') {
		return true
	}
	return false
}

func isAbbrev(runes []rune, dotIdx int) bool {
	start := dotIdx - 1
	for start >= 0 && (unicode.IsLetter(runes[start]) || runes[start] == '.') {
		start--
	}
	start++
	if start >= dotIdx {
		return false
	}

	wordRunes := runes[start:dotIdx]
	// Single letter uppercase initial (e.g. "J. Doe")
	if len(wordRunes) == 1 && unicode.IsUpper(wordRunes[0]) {
		return true
	}

	word := strings.ToLower(string(wordRunes))
	if knownAbbreviations[word] {
		return true
	}

	// Acronyms with dots: e.g. "u.s", "ph.d", "a.m", "i.e"
	parts := strings.Split(word, ".")
	if len(parts) > 1 {
		allShort := true
		for _, p := range parts {
			if len(p) == 0 || len(p) > 2 {
				allShort = false
				break
			}
		}
		if allShort {
			return true
		}
	}

	return false
}
