package guard

import (
	"encoding/json"
	"testing"
)

func TestNewTraceAnnouncement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		args   string
		want   TraceAnnouncement
		wantOK bool
	}{
		{name: "agy names, arguments dropped", args: `{"ServerName":"docket","ToolName":"create","Arguments":{"title":"secret"}}`, want: TraceAnnouncement{DaemonID: "d1", ConversationID: "conv-1", Server: "docket", Tool: "create"}, wantOK: true},
		{name: "snake case names", args: `{"server_name":"kernel","tool_name":"browse","arguments":{"url":"x"}}`, want: TraceAnnouncement{DaemonID: "d1", ConversationID: "conv-1", Server: "kernel", Tool: "browse"}, wantOK: true},
		{name: "args encoded as a JSON string", args: `"{\"ServerName\":\"docket\",\"ToolName\":\"list\"}"`, want: TraceAnnouncement{DaemonID: "d1", ConversationID: "conv-1", Server: "docket", Tool: "list"}, wantOK: true},
		{name: "missing tool", args: `{"ServerName":"docket"}`},
		{name: "no args", args: ``},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload := Payload{ConversationID: "conv-1", ToolCall: ToolCall{Name: "call_mcp_tool", Args: json.RawMessage(tc.args)}}
			got, ok := NewTraceAnnouncement(payload, "d1")
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("NewTraceAnnouncement = %+v, %v; want %+v, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
