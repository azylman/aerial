package transcript

import (
	"testing"
)

type cleanPromptCase struct {
	name string
	raw  string
	want string
}

func cpc(name, raw, want string) cleanPromptCase {
	return cleanPromptCase{name: name, raw: raw, want: want}
}

func TestCleanPromptEnvelope(t *testing.T) {
	t.Parallel()

	tests := []cleanPromptCase{
		cpc("clean string untouched", "hello world", "hello world"),
		cpc("strips retrieved_memory", "<retrieved_memory>memories</retrieved_memory>hello", "hello"),
		cpc("strips unclosed tag", "<retrieved_memory>unclosed tag without end hello", "<retrieved_memory>unclosed tag without end hello"),
		cpc("strips user_request tags", "<USER_REQUEST>real user query</USER_REQUEST>", "real user query"),
		cpc("strips immediate work sentence", "real query Only formulate and output your final response once all immediate work is complete. rest", "real query"),
		cpc("fallback when empty cleaned", "   ", ""),
		cpc("strips channel instructions", "<CHANNEL_INSTRUCTIONS>guidelines</CHANNEL_INSTRUCTIONS>task", "task"),
		cpc("strips channel history", "<CHANNEL_HISTORY>history</CHANNEL_HISTORY>task", "task"),
		cpc("strips ambient context", "<ambient_context>ambient</ambient_context>task", "task"),
		cpc("strips additional metadata", "<ADDITIONAL_METADATA>meta</ADDITIONAL_METADATA>task", "task"),
		cpc("strips user settings change", "<USER_SETTINGS_CHANGE>settings</USER_SETTINGS_CHANGE>task", "task"),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CleanPromptEnvelope(tc.raw)
			if got != tc.want {
				t.Errorf("CleanPromptEnvelope(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

type cleanSummaryCase struct {
	name     string
	raw      string
	maxRunes int
	want     string
}

func csc(name, raw string, maxRunes int, want string) cleanSummaryCase {
	return cleanSummaryCase{name: name, raw: raw, maxRunes: maxRunes, want: want}
}

func TestCleanSessionSummary(t *testing.T) {
	t.Parallel()

	tests := []cleanSummaryCase{
		csc("short summary below limit", "short summary", 50, "short summary"),
		csc("summary exceeding limit", "long summary exceeding limit here", 10, "long summa... [truncated]"),
		csc("zero maxRunes uses default", "short default summary", 0, "short default summary"),
		csc("negative maxRunes uses default", "short default summary", -5, "short default summary"),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CleanSessionSummary(tc.raw, tc.maxRunes)
			if got != tc.want {
				t.Errorf("CleanSessionSummary(%q, %d) = %q, want %q", tc.raw, tc.maxRunes, got, tc.want)
			}
		})
	}
}

type clipContentCase struct {
	name      string
	content   string
	headLimit int
	tailLimit int
	want      string
}

func ccc(name, content string, headLimit, tailLimit int, want string) clipContentCase {
	return clipContentCase{name: name, content: content, headLimit: headLimit, tailLimit: tailLimit, want: want}
}

func TestClipContent(t *testing.T) {
	t.Parallel()

	tests := []clipContentCase{
		ccc("content within limit", "hello world", 10, 10, "hello world"),
		ccc("content exceeding limit clipped", "1234567890abcdefghij", 5, 5, "12345"+TruncationMarker+"fghij"),
		ccc("default limits used when both zero", "hello default", 0, 0, "hello default"),
		ccc("negative limits clamped to zero", "1234567890", -1, 5, TruncationMarker+"67890"),
		ccc("negative tail clamped to zero", "1234567890", 5, -1, "12345"+TruncationMarker),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ClipContent(tc.content, tc.headLimit, tc.tailLimit)
			if got != tc.want {
				t.Errorf("ClipContent(%q, %d, %d) = %q, want %q", tc.content, tc.headLimit, tc.tailLimit, got, tc.want)
			}
		})
	}
}

type substantiveStepCase struct {
	name     string
	stepType string
	toolName string
	content  string
	want     bool
}

func ssc(name, stepType, toolName, content string, want bool) substantiveStepCase {
	return substantiveStepCase{name: name, stepType: stepType, toolName: toolName, content: content, want: want}
}

func TestIsSubstantiveStep(t *testing.T) {
	t.Parallel()

	tests := []substantiveStepCase{
		ssc("empty content is non-substantive", "output", "tool", "   ", false),
		ssc("heartbeat type is non-substantive", "heartbeat", "check", "alive", false),
		ssc("heartbeat tool is non-substantive", "tool", "heartbeat", "alive", false),
		ssc("short content with heartbeat is non-substantive", "tool", "poll", "system heartbeat ok", false),
		ssc("manage_task status tool is non-substantive", "tool", "manage_task status", "output", false),
		ssc("manage_task action status json is non-substantive", "tool", "manage_task", `{"action":"status"}`, false),
		ssc("manage_task action space status json is non-substantive", "tool", "manage_task", `{"action": "status"}`, false),
		ssc("manage_task with status in short content is non-substantive", "tool", "manage_task", `Checking "status" of task`, false),
		ssc("manage_task in content with status is non-substantive", "output", "other", `task manage_task status check`, false),
		ssc("substantive tool execution", "tool", "run_command", "git status --short", true),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsSubstantiveStep(tc.stepType, tc.toolName, tc.content)
			if got != tc.want {
				t.Errorf("IsSubstantiveStep(%q, %q, %q) = %v, want %v", tc.stepType, tc.toolName, tc.content, got, tc.want)
			}
		})
	}
}
