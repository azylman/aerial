package runner

import "testing"

func TestStreamingDaemon_Claude_StreamUsageFallback(t *testing.T) {
	t.Parallel()
	const (
		msg1Block1 = `{"type":"assistant","message":{"id":"msg_1","content":[{"type":"thinking"}],"usage":{"input_tokens":2,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000,"output_tokens":50}}}`
		msg1Block2 = `{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":2,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000,"output_tokens":50}}}`
		msg2       = `{"type":"assistant","parent_tool_use_id":"toolu_agent","message":{"id":"msg_2","content":[{"type":"text","text":"sub"}],"usage":{"input_tokens":3,"cache_creation_input_tokens":10,"cache_read_input_tokens":1100,"output_tokens":20}}}`
		zeroUsage  = `{"type":"assistant","message":{"id":"synthetic","content":[{"type":"text","text":"No response requested."}],"usage":{"input_tokens":0,"output_tokens":0}}}`
	)
	const (
		mainStart  = `{"type":"stream_event","event":{"type":"message_start","message":{"id":"msg_m","usage":{"input_tokens":2,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000,"output_tokens":1}}}}`
		mainBlock  = `{"type":"assistant","message":{"id":"msg_m","content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":2,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000,"output_tokens":1}}}`
		mainDelta  = `{"type":"stream_event","event":{"type":"message_delta","usage":{"output_tokens":400}}}`
		subStart   = `{"type":"stream_event","parent_tool_use_id":"toolu_agent","event":{"type":"message_start","message":{"id":"msg_s","usage":{"input_tokens":3,"cache_read_input_tokens":500,"output_tokens":1}}}}`
		subDelta   = `{"type":"stream_event","parent_tool_use_id":"toolu_agent","event":{"type":"message_delta","usage":{"output_tokens":50}}}`
		killResult = `{"type":"result","is_error":true,"result":"overloaded"}`
	)
	streamed := AgyUsage{InputTokens: 115, OutputTokens: 70, CacheReadTokens: 2100, CacheCreationTokens: 110, TotalTokens: 185}
	tests := []struct {
		name      string
		lines     []string
		wantErr   bool
		wantUsage AgyUsage
	}{
		{"result usage wins over streamed usage", []string{msg1Block1, msg2, `{"type":"result","result":"done","usage":{"input_tokens":7,"output_tokens":8}}`}, false, AgyUsage{InputTokens: 7, OutputTokens: 8, TotalTokens: 15}},
		{"result without usage falls back to streamed usage", []string{msg1Block1, msg1Block2, msg2, zeroUsage, `{"type":"result","result":"done"}`}, false, streamed},
		{"error result carries streamed usage", []string{msg1Block1, msg1Block2, msg2, `{"type":"result","is_error":true,"result":"overloaded"}`}, true, streamed},
		{"message_delta carries the final output count", []string{mainStart, mainBlock, mainDelta, mainBlock, killResult}, true, AgyUsage{InputTokens: 102, OutputTokens: 400, CacheReadTokens: 1000, CacheCreationTokens: 100, TotalTokens: 502}},
		{"interleaved subagent deltas land on their own message", []string{mainStart, subStart, subDelta, mainDelta, killResult}, true, AgyUsage{InputTokens: 105, OutputTokens: 450, CacheReadTokens: 1500, CacheCreationTokens: 100, TotalTokens: 555}},
		{"delta without a started message is ignored", []string{mainDelta, killResult}, true, AgyUsage{}},
		{"error result without streamed usage", []string{zeroUsage, `{"type":"result","is_error":true,"result":"overloaded"}`}, true, AgyUsage{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			daemon := &StreamingDaemon{cfg: DaemonConfig{Engine: "claude"}, claudeToolIDs: make(map[string]int), inFlightTools: make(map[int]inFlightToolCall)}
			sink := newMockTurnSink()
			enqueueInflightTurn(daemon, "turn", sink)
			for _, line := range tc.lines {
				daemon.dispatchNDJSONLine(line)
			}
			<-sink.done
			sink.mu.Lock()
			defer sink.mu.Unlock()
			if (sink.err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %t", sink.err, tc.wantErr)
			}
			got := AgyUsage{}
			if sink.result != nil {
				got = sink.result.Usage
			}
			if sink.err != nil {
				got, _ = UsageFromError(sink.err)
			}
			if got != tc.wantUsage {
				t.Errorf("usage = %+v, want %+v", got, tc.wantUsage)
			}
		})
	}
}
