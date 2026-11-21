package enginefmt

import (
	"encoding/json"
	"testing"
)

func TestExtractCodexText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		content  string
		fallback string
		want     string
		wantRaw  string
	}{
		{"fallback wins", `"x"`, "fb", "fb", "fb"},
		{"empty content", "", "", "", ""},
		{"fallback over object", `{"invalid":1}`, "fallback text", "fallback text", "fallback text"},
		{"string", `"hello"`, "", "hello", "hello"},
		{"blocks keep text kinds", `[{"type":"text","text":"a"},{"type":"input_text","text":" "},{"type":"output_text","text":"b"},{"type":"image","text":"c"}]`, "", "a\nb", "a\nb"},
		{"object no fallback", `{"invalid":1}`, "", "", `{"invalid":1}`},
		{"unknown shape", `{"k":1}`, "", "", `{"k":1}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ExtractCodexText(json.RawMessage(tc.content), tc.fallback); got != tc.want {
				t.Fatalf("ExtractCodexText = %q, want %q", got, tc.want)
			}
			if got := ExtractCodexTextOrRaw(json.RawMessage(tc.content), tc.fallback); got != tc.wantRaw {
				t.Fatalf("ExtractCodexTextOrRaw = %q, want %q", got, tc.wantRaw)
			}
		})
	}
}
