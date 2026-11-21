package router

import (
	"testing"
)

func TestParseAerialPRMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body, wantTarget, wantBotID string
		wantOK                            bool
	}{
		{name: "no trailer", body: "Standard PR body\n<!-- regular comment -->"},
		{name: "unclosed trailer", body: "Body\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"}"},
		{name: "invalid json", body: "Body\n<!-- aerial-pr: {bad-json} -->"},
		{name: "invalid target and empty", body: "Body\n<!-- aerial-pr: {\"target_id\":\"bad\"} -->"},
		{name: "valid target", body: "<!-- x -->\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "discord prefixed target and pool prefix", body: "<!-- aerial-pr: {\"target_id\":\"stateful_medium:discord-100000000000000002\"} -->", wantTarget: "100000000000000002", wantOK: true},
		{name: "valid bot id only", body: "<!-- aerial-pr: {\"bot_id\":\"eri-sub\"} -->", wantBotID: "eri-sub", wantOK: true},
		{name: "valid target and bot id", body: "<!-- aerial-pr: {\"target_id\":\"100000000000000002\",\"bot_id\":\"eri-sub\"} -->", wantTarget: "100000000000000002", wantBotID: "eri-sub", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseAerialPRMetadata(tc.body)
			if got.OK() != tc.wantOK || got.TargetID != tc.wantTarget || got.BotID != tc.wantBotID {
				t.Fatalf("ParseAerialPRMetadata() = %+v (ok=%v), want target=%q bot=%q ok=%v", got, got.OK(), tc.wantTarget, tc.wantBotID, tc.wantOK)
			}
		})
	}
}

func TestClassifyCheckState_Unit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       string
	}{
		{name: "completed success", status: "completed", conclusion: "success", want: "passed"},
		{name: "completed neutral", status: "completed", conclusion: "neutral", want: "passed"},
		{name: "completed skipped", status: "completed", conclusion: "skipped", want: "passed"},
		{name: "completed failure", status: "completed", conclusion: "failure", want: "failed"},
		{name: "completed timed_out", status: "completed", conclusion: "timed_out", want: "failed"},
		{name: "completed cancelled", status: "completed", conclusion: "cancelled", want: "failed"},
		{name: "completed action_required", status: "completed", conclusion: "action_required", want: "failed"},
		{name: "completed unknown conclusion", status: "completed", conclusion: "stale", want: ""},
		{name: "queued", status: "queued", want: "pending"},
		{name: "in_progress", status: "in_progress", want: "pending"},
		{name: "waiting", status: "waiting", want: "pending"},
		{name: "requested", status: "requested", want: "pending"},
		{name: "pending", status: "pending", want: "pending"},
		{name: "other non-completed", status: "custom_status", want: "pending"},
		{name: "empty status", status: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyCheckState(tc.status, tc.conclusion)
			if got != tc.want {
				t.Fatalf("classifyCheckState(%q, %q) = %q, want %q", tc.status, tc.conclusion, got, tc.want)
			}
		})
	}
}
