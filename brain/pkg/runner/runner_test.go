package runner

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseAgyOutput(t *testing.T) {
	tests := []struct {
		name       string
		stdout     string
		wantErr    bool
		wantConvID string
		wantStatus string
		wantResp   string
		wantTokens int
	}{
		{name: "Valid Success Response", stdout: `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"Hello world!","duration_seconds":1.25,"num_turns":1,"usage":{"total_tokens":42}}`, wantConvID: "11111111-2222-3333-4444-555555555555", wantStatus: "SUCCESS", wantResp: "Hello world!", wantTokens: 42}, {name: "Valid Error Response", stdout: `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"ERROR","error":"context window exceeded","duration_seconds":0.5}`, wantConvID: "11111111-2222-3333-4444-555555555555", wantStatus: "ERROR"}, {name: "Empty Stdout", stdout: "", wantErr: true}, {name: "Invalid JSON", stdout: "plain text without json formatting", wantErr: true}, {name: "Valid stream-json Success Stream", stdout: "{\"event\":\"init\",\"conversation_id\":\"22222222-3333-4444-5555-666666666666\"}\n{\"event\":\"step_update\",\"type\":\"tool_call\",\"name\":\"view_file\"}\n{\"event\":\"result\",\"result\":{\"conversation_id\":\"22222222-3333-4444-5555-666666666666\",\"status\":\"SUCCESS\",\"response\":\"Stream finished!\",\"duration_seconds\":3.14,\"num_turns\":2,\"usage\":{\"total_tokens\":88}}}", wantConvID: "22222222-3333-4444-5555-666666666666", wantStatus: "SUCCESS", wantResp: "Stream finished!", wantTokens: 88},
		{name: "Stream-json with Init Propagation to Result", stdout: "{\"event\":\"init\",\"conversation_id\":\"33333333-4444-5555-6666-777777777777\"}\n{\"event\":\"step_update\",\"type\":\"thinking\"}\n{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"Propagated UUID!\",\"duration_seconds\":1.0,\"num_turns\":1}}", wantConvID: "33333333-4444-5555-6666-777777777777", wantStatus: "SUCCESS", wantResp: "Propagated UUID!"}, {name: "Stream-json Error Stream", stdout: "{\"event\":\"init\",\"conversation_id\":\"44444444-5555-6666-7777-888888888888\"}\n{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"model timeout\"}}", wantConvID: "44444444-5555-6666-7777-888888888888", wantStatus: "ERROR"}, {name: "Truncated Stream-json Missing Result Event", stdout: "{\"event\":\"init\",\"conversation_id\":\"55555555-6666-7777-8888-999999999999\"}\n{\"event\":\"step_update\",\"type\":\"run_command\"}", wantErr: true}, {name: "Large Line (>100KB) Stream-json Without Scanner Panic", stdout: "{\"event\":\"init\",\"conversation_id\":\"66666666-7777-8888-9999-000000000000\"}\n{\"event\":\"step_update\",\"huge_payload\":\"" + strings.Repeat("a", 120*1024) + "\"}\n{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"Handled large line\",\"duration_seconds\":2.0,\"num_turns\":1}}", wantConvID: "66666666-7777-8888-9999-000000000000", wantStatus: "SUCCESS", wantResp: "Handled large line"},
		{name: "Stream-json Result Flat", stdout: "{\"event\":\"init\",\"conversation_id\":\"77777777-7777-7777-7777-777777777777\"}\n{\"event\":\"result\",\"status\":\"SUCCESS\",\"response\":\"flat response\"}", wantConvID: "77777777-7777-7777-7777-777777777777", wantStatus: "SUCCESS", wantResp: "flat response"}, {name: "Multiline Legacy JSON fallback", stdout: "{\n\"conversation_id\":\"88888888-8888-8888-8888-888888888888\",\n\"status\":\"SUCCESS\",\n\"response\":\"multiline legacy\"\n}", wantConvID: "88888888-8888-8888-8888-888888888888", wantStatus: "SUCCESS", wantResp: "multiline legacy"}, {name: "Root Result Flat Format", stdout: `{"event":"result","status":"SUCCESS","response":"all good"}`, wantStatus: "SUCCESS", wantResp: "all good"}, {name: "Fallback Unmarshal Non-Result JSON", stdout: `{"custom":"field"}`},
		{name: "Stream-json Init With session_id", stdout: "{\"event\":\"init\",\"session_id\":\"87654321-4321-4321-4321-210987654321\"}\n{\"event\":\"result\",\"status\":\"SUCCESS\",\"response\":\"done\"}\n", wantConvID: "87654321-4321-4321-4321-210987654321", wantStatus: "SUCCESS", wantResp: "done"}, {name: "Stream-json Init With conversationId CamelCase", stdout: "{\"conversationId\":\"87654321-4321-4321-4321-210987654321\",\"event\":\"init\"}\n{\"event\":\"result\",\"status\":\"SUCCESS\",\"response\":\"done\"}\n", wantConvID: "87654321-4321-4321-4321-210987654321", wantStatus: "SUCCESS", wantResp: "done"}, {name: "Empty Lines With Probe Session", stdout: "\n\n{\"event\":\"result\",\"session_id\":\"22222222-3333-4444-5555-666666666666\",\"content\":\"ok\"}\n\n", wantConvID: "22222222-3333-4444-5555-666666666666"}, {name: "Init Event ConvID Fallback When Result Omits ID", stdout: "{\"event\":\"init\",\"session_id\":\"11111111-2222-3333-4444-555555555555\"}\n{\"event\":\"result\",\"response\":\"finished\"}\n", wantConvID: "11111111-2222-3333-4444-555555555555", wantResp: "finished"},
		{name: "Empty Lines Interspersed", stdout: "{\"event\":\"init\",\"conversation_id\":\"test-conv-id\"}\n\n   \n\n{\"event\":\"result\",\"response\":\"success\"}", wantResp: "success"},
	}

	for _, tc := range tests {
		resp, err := ParseAgyOutput(tc.stdout)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: ParseAgyOutput() error = %v, wantErr %v", tc.name, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && resp != nil {
			for _, check := range []struct{ got, want, field string }{
				{resp.ConversationID, tc.wantConvID, "ConvID"}, {resp.Status, tc.wantStatus, "Status"}, {resp.Response, tc.wantResp, "Resp"},
			} {
				if check.want != "" && check.got != check.want {
					t.Errorf("%s: %s = %q, want %q", tc.name, check.field, check.got, check.want)
				}
			}
			if tc.wantTokens > 0 && resp.Usage.TotalTokens != tc.wantTokens {
				t.Errorf("%s: TotalTokens = %d, want %d", tc.name, resp.Usage.TotalTokens, tc.wantTokens)
			}
		}
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name                 string
		exitCode             int
		stdout, stderr       string
		wantFailure          bool
		wantTransient        bool
		wantCorrupt          bool
		errDetailMustContain string
	}{
		{name: "Clean Success With JSON Envelope", exitCode: 0, stdout: `{"conversation_id":"abc","status":"SUCCESS","response":"Hello there!"}`}, {name: "Empty Response With Status SUCCESS On Exit 0", exitCode: 0, stdout: `{"conversation_id":"abc","status":"SUCCESS","response":""}`, wantFailure: true, errDetailMustContain: "empty response"}, {name: "Clean Success Discussing Context Limit", exitCode: 0, stdout: `{"conversation_id":"abc","status":"SUCCESS","response":"The model's maximum context length is 1M tokens. When context window exceeded occurs, handle error 503."}`}, {name: "Clean Success With Benign Debug Stderr", exitCode: 0, stdout: `{"conversation_id":"abc","status":"SUCCESS","response":"Processed your request cleanly."}`, stderr: "[DEBUG] rate limit check passed, timeout set to 30s"},
		{name: "Clean Success Discussing Inactivity Timeout", exitCode: 0, stdout: `{"conversation_id":"abc","status":"SUCCESS","response":"Here is how [watchdog] inactivity timeout exceeded works when monitoring stderr."}`}, {name: "Clean Success Discussing Watchdog Max Duration", exitCode: 0, stdout: `{"conversation_id":"abc","status":"SUCCESS","response":"The [watchdog] max duration exceeded cap is set to 60m."}`}, {name: "Transient Quota Exceeded In Stdout On Exit 1", exitCode: 1, stdout: `{"status":"ERROR","error":"RESOURCE_EXHAUSTED: You have exceeded your current quota"}`, wantFailure: true, wantTransient: true, errDetailMustContain: "quota"}, {name: "Self-Healing Exit 0 Conversation Not Found Warning", exitCode: 0, stdout: `{"conversation_id":"new-uuid","status":"SUCCESS","response":"Hello fresh session!"}`, stderr: `warning: conversation "stale-uuid" not found`},
		{name: "Exit Code 0 With Empty Stdout", exitCode: 0, stdout: "", wantFailure: true, wantTransient: true, errDetailMustContain: "process produced empty stdout"}, {name: "Exit Code 0 With Non-JSON Stdout", exitCode: 0, stdout: "not a valid json output", wantFailure: true, wantTransient: true, errDetailMustContain: "invalid json response"}, {name: "Exit Code 0 Context Window Exceeded In JSON", exitCode: 0, stdout: `{"conversation_id":"abc","status":"ERROR","error":"context length exceeded: max context length is 1000000"}`, wantFailure: true, wantCorrupt: true, errDetailMustContain: "context length exceeded"}, {name: "Transient 503 Unavailable", exitCode: 1, stderr: "Error: status: UNAVAILABLE: Model is overloaded (Error 503)", wantFailure: true, wantTransient: true, errDetailMustContain: "503"},
		{name: "Transient 429 Rate Limit", exitCode: 1, stderr: "RESOURCE_EXHAUSTED: Rate limit exceeded (429)", wantFailure: true, wantTransient: true, errDetailMustContain: "Rate limit"}, {name: "Transient Context Deadline Exceeded", exitCode: -1, stderr: "context deadline exceeded", wantFailure: true, wantTransient: true, errDetailMustContain: "context deadline exceeded"}, {name: "Session Corruption Corrupted Session", exitCode: 1, stderr: "Error: failed to load conversation: session corrupted", wantFailure: true, wantCorrupt: true, errDetailMustContain: "failed to load"}, {name: "Database Locked Not Session Corruption", exitCode: 1, stderr: "sqlite error: database is locked", wantFailure: true, wantTransient: true, errDetailMustContain: "database is locked"},
		{name: "General Process Failure", exitCode: 127, stderr: "command not found: agy", wantFailure: true, errDetailMustContain: "command not found"}, {name: "Exit Code 0 Truncated Stream 503 In Stderr", exitCode: 0, stdout: `{"conversation_id":"abc","status":"ERROR","error":"503 service unavailable"}`, stderr: "Agent execution terminated due to error: Error 503, Status: UNAVAILABLE", wantFailure: true, wantTransient: true, errDetailMustContain: "503"}, {name: "Exit Code 0 Truncated Stream Generic Stderr", exitCode: 0, stdout: `{"conversation_id":"abc","status":"SUCCESS","response":"partial"}`, stderr: "Agent execution terminated due to error: panic: runtime error", wantFailure: true, errDetailMustContain: "terminated"}, {name: "Empty Stdout Panic In Stderr Exit 0", exitCode: 0, stderr: "panic: internal null pointer", wantFailure: true, errDetailMustContain: "panic:"},
		{name: "MCP SSE Handshake Drop Exit 0", exitCode: 0, stdout: `{"conversation_id":"abc","status":"ERROR","error":"server name github failed to load: calling \"initialize\": session not found"}`, wantFailure: true, wantTransient: true, errDetailMustContain: "session not found"}, {name: "MCP SSE Handshake Drop Exit 1", exitCode: 1, stderr: "server name docker failed to load: calling \"initialize\": session not found", wantFailure: true, wantTransient: true, errDetailMustContain: "session not found"}, {name: "Transient ModelProvider Missing API Key Mismatch", exitCode: 1, stderr: `modelProvider is set to "gemini" in settings.json, but the GEMINI_API_KEY environment variable is not set.`, wantFailure: true, wantTransient: true, errDetailMustContain: "modelProvider is set to \"gemini\""}, {name: "Stream Interrupted In Result JSON Error Exit 0", exitCode: 0, stdout: `{"event":"result","status":"error","error":"The stream was interrupted. Please continue the task you were working on."}`, wantFailure: true, wantCorrupt: true, errDetailMustContain: "stream was interrupted"},
		{name: "Stream Interrupted In Stderr Non-Zero Exit", exitCode: 1, stderr: "Error: The stream was interrupted. Please continue the task you were working on.", wantFailure: true, wantCorrupt: true, errDetailMustContain: "stream was interrupted"}, {name: "Unknown Exit 1 Defaults To Transient", exitCode: 1, stderr: "unexpected internal socket glitch occurred in daemon", wantFailure: true, wantTransient: true, errDetailMustContain: "socket glitch"}, {name: "Known Non-Transient Invalid API Key Exit 1", exitCode: 1, stderr: "Error: invalid api key provided", wantFailure: true, errDetailMustContain: "invalid api key"}, {name: "Known Non-Transient Unknown Flag Exit 0", exitCode: 0, stdout: `{"event":"result","status":"error","error":"unknown flag: --bogus"}`, wantFailure: true, errDetailMustContain: "unknown flag"},
		{name: "Known Non-Transient Executable Not Found Exit 1", exitCode: 1, stderr: `exec: "agy": executable file not found in $PATH`, wantFailure: true, errDetailMustContain: "executable file not found"}, {name: "Clean Success With Permission Denied In Response Body", exitCode: 0, stdout: `{"status":"SUCCESS","response":"The script failed earlier with permission denied, but I fixed chmod."}`}, {name: "Watchdog Inactivity Timeout Not Transient", exitCode: -1, stderr: "Starting conversation update stream for uuid-123\n[watchdog] inactivity timeout exceeded (5m without output or transcript update)", wantFailure: true, errDetailMustContain: "inactivity timeout exceeded"}, {name: "Watchdog Max Duration Not Transient", exitCode: -1, stderr: "Starting conversation update stream for uuid-123\n[watchdog] max duration exceeded (60m total duration cap)", wantFailure: true, errDetailMustContain: "max duration exceeded"},
		{name: "Exit 0 Response Discussing Context Limit Clean", exitCode: 0, stdout: `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"The maximum context length in tokens is 1 million. When context window exceeded occurs, the system rotates sessions."}`}, {name: "Exit 0 Empty Stdout Fatal Stderr", exitCode: 0, stderr: "fatal error: runtime panic", wantFailure: true, errDetailMustContain: "fatal"}, {name: "Exit 0 Unparseable Stdout Context Window", exitCode: 0, stdout: "not json maximum context length exceeded", wantFailure: true, wantCorrupt: true, errDetailMustContain: "context window exceeded"}, {name: "Exit 0 Unparseable Stdout Corrupt Session", exitCode: 0, stdout: "not json corrupted session state", stderr: "failed to load conversation", wantFailure: true, wantCorrupt: true},
		{name: "Exit 0 Unparseable Stdout 503 Transient", exitCode: 0, stdout: "not json 503 service unavailable", stderr: "high demand", wantFailure: true, wantTransient: true}, {name: "Exit 0 Unparseable Stdout Fatal Stderr", exitCode: 0, stdout: "not json output", stderr: "panic: nil pointer dereference", wantFailure: true, errDetailMustContain: "panic"}, {name: "Exit 0 Unparseable Stdout Generic Fallback", exitCode: 0, stdout: "not json output", stderr: "regular stderr", wantFailure: true, wantTransient: true, errDetailMustContain: "invalid json response"}, {name: "Exit 0 Status FAILED Without Error", exitCode: 0, stdout: `{"status":"FAILED"}`, wantFailure: true, wantTransient: true, errDetailMustContain: "runner status: FAILED"},
		{name: "Exit 0 Status SUCCESS With Error Quota Exceeded", exitCode: 0, stdout: `{"status":"SUCCESS","error":"quota exceeded"}`, wantFailure: true, wantTransient: true, errDetailMustContain: "quota exceeded"}, {name: "Exit 0 Status SUCCESS With Fatal Stderr", exitCode: 0, stdout: `{"status":"SUCCESS","response":"all good"}`, stderr: "fatal: unrecoverable crash", wantFailure: true, errDetailMustContain: "fatal"}, {name: "Exit 1 Conversation Not Found Corruption", exitCode: 1, stderr: "conversation not found in registry", wantFailure: true, wantCorrupt: true, errDetailMustContain: "conversation not found"}, {name: "Exit 2 Short Stdout Error Detail", exitCode: 2, stdout: "short failure message", wantFailure: true, wantTransient: true, errDetailMustContain: "short failure message"},
		{name: "Exit 2 JSON Error In Stdout", exitCode: 2, stdout: `{"error":"inner json error"}`, wantFailure: true, wantTransient: true, errDetailMustContain: "inner json error"}, {name: "Exit 2 Long Stdout Fallback Exit Code", exitCode: 2, stdout: strings.Repeat("a", 400), wantFailure: true, wantTransient: true, errDetailMustContain: "exit code 2"}, {name: "Exit 0 Stderr Context Limit Keyword", exitCode: 0, stdout: "not json", stderr: "error: maximum context length exceeded", wantFailure: true, wantCorrupt: true, errDetailMustContain: "context window exceeded"}, {name: "Exit 0 Stderr Corrupted Transcript", exitCode: 0, stdout: "not json", stderr: "corrupted transcript detected", wantFailure: true, wantCorrupt: true, errDetailMustContain: "corrupt"},
		{name: "Exit 0 Stderr Fatal Crash", exitCode: 0, stdout: "not json", stderr: "fatal: unrecoverable crash", wantFailure: true, errDetailMustContain: "fatal"}, {name: "Exit 0 Result Event Status ERROR Corrupt", exitCode: 0, stdout: `{"event":"result","status":"ERROR","error":"session corrupt error"}`, wantFailure: true, wantCorrupt: true, errDetailMustContain: "session corrupt error"}, {name: "Exit 0 Result Event Status FAILED", exitCode: 0, stdout: `{"event":"result","status":"FAILED"}`, wantFailure: true, wantTransient: true, errDetailMustContain: "runner status: FAILED"}, {name: "Resp Error Session Corrupt", exitCode: 0, stdout: `{"error":"session corrupt: malformed database"}`, wantFailure: true, wantCorrupt: true, errDetailMustContain: "session corrupt"},
		{name: "Resp Error Invalid API Key", exitCode: 0, stdout: `{"error":"invalid api key provided"}`, wantFailure: true, errDetailMustContain: "invalid api key"}, {name: "Exit 42 Long Stdout Fallback", exitCode: 42, stdout: strings.Repeat("unparseable text ", 30), wantFailure: true, wantTransient: true, errDetailMustContain: "exit code 42"}, {name: "Exit 1 Short Stdout Fallback", exitCode: 1, stdout: "custom python failure on line 42", wantFailure: true, wantTransient: true, errDetailMustContain: "custom python failure on line 42"},
	}

	for _, tc := range tests {
		isFailure, isTransient, isCorrupt, errDetail := ClassifyError(tc.exitCode, tc.stdout, tc.stderr)
		if isFailure != tc.wantFailure || isTransient != tc.wantTransient || isCorrupt != tc.wantCorrupt {
			t.Errorf("%s: flags = (%v, %v, %v), want (%v, %v, %v)", tc.name, isFailure, isTransient, isCorrupt, tc.wantFailure, tc.wantTransient, tc.wantCorrupt)
		}
		if tc.errDetailMustContain != "" && !strings.Contains(errDetail, tc.errDetailMustContain) {
			t.Errorf("%s: errDetail %q does not contain %q", tc.name, errDetail, tc.errDetailMustContain)
		}
	}
}

func TestExtractSessionID(t *testing.T) {
	for _, tc := range []struct{ name, input, wantUUID string }{
		{name: "NDJSON Init Event", input: "{\"event\":\"init\",\"conversation_id\":\"88888888-9999-aaaa-bbbb-cccccccccccc\"}\n{\"event\":\"step_update\"}\n", wantUUID: "88888888-9999-aaaa-bbbb-cccccccccccc"}, {name: "Conversation Update Stream In Stderr", input: "2026-08-28T10:00:00Z Starting conversation update stream for 12345678-abcd-1234-abcd-1234567890ab\nConnecting...", wantUUID: "12345678-abcd-1234-abcd-1234567890ab"}, {name: "Initialized session_id Format", input: "Initialized session_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee successfully", wantUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}, {name: "Ignore Session Started Common Words", input: "Session started\nStarting session now"},
		{name: "Ignore Conversation Initialization Failed", input: "Conversation initialization-failed: could not load"}, {name: "RFC4122 In Update Stream Line", input: "Starting conversation update stream for 12345678-abcd-ef01-2345-6789abcdef01", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "RFC4122 In conversation_id Line", input: "conversation_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", wantUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}, {name: "Invalid Short Hex Length", input: "conversation: 1234-5678"},
		{name: "Invalid Update Stream UUID Format", input: "Starting conversation update stream for invalid-uuid-format\n"}, {name: "Invalid Candidate In General Session", input: "conversation_id: not-a-valid-uuid\n"}, {name: "Raw UUID In Unstructured Trace", input: "Some unstructured error log mentioning 12345678-abcd-ef01-2345-6789abcdef01 in the trace", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "JSON Inverted Key Order", input: "{\"conversation_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"event\":\"init\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON session_id Key", input: "{\"event\":\"init\",\"session_id\":\"12345678-abcd-ef01-2345-6789abcdef01\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "JSON camelCase conversationId", input: "{\"event\":\"init\",\"conversationId\":\"12345678-abcd-ef01-2345-6789abcdef01\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "JSON camelCase sessionId", input: "{\"event\":\"init\",\"sessionId\":\"12345678-abcd-ef01-2345-6789abcdef01\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "JSON step_update session_id", input: "{\"event\":\"step_update\",\"session_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"type\":\"tool_call\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON result session_id", input: "{\"event\":\"result\",\"session_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"status\":\"SUCCESS\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "JSON Nested Data conversation_id", input: "{\"event\":\"init\",\"data\":{\"conversation_id\":\"12345678-abcd-ef01-2345-6789abcdef01\"}}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "JSON Nested Result conversation_id", input: "{\"event\":\"result\",\"result\":{\"conversation_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"status\":\"SUCCESS\"}}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"}, {name: "JSON Init session_id Single Line", input: `{"event":"init","session_id":"123e4567-e89b-12d3-a456-426614174000"}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "JSON Init conversation_id Single Line", input: `{"event":"init","conversation_id":"123e4567-e89b-12d3-a456-426614174000"}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"}, {name: "JSON Nested step_update session_id Single Line", input: `{"event":"step_update","step_update":{"session_id":"123e4567-e89b-12d3-a456-426614174000"}}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"}, {name: "NDJSON Init session Field Fallback", input: `{"event":"init","session":"123e4567-e89b-12d3-a456-426614174000"}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"}, {name: "Text session_id Prefix Fallback", input: "session_id: 123e4567-e89b-12d3-a456-426614174000", wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "Raw Text UUID Prefix/Suffix", input: "some raw prefix 123e4567-e89b-12d3-a456-426614174000 suffix", wantUUID: "123e4567-e89b-12d3-a456-426614174000"}, {name: "Empty Output Returns Empty String", input: ""}, {name: "Embedded JSON In Raw Log Line", input: `raw stdout log: {"event": "init", "conversation_id": "11111111-2222-3333-4444-555555555555"}`, wantUUID: "11111111-2222-3333-4444-555555555555"}, {name: "Logger Prefix With Braces Before JSON", input: `[2026-09-19 {thread-1}] {"event": "init", "conversation_id": "11111111-2222-3333-4444-555555555555"}`, wantUUID: "11111111-2222-3333-4444-555555555555"},
		{name: "Embedded JSON Invalid UUID Followed By Text UUID", input: "raw stdout log: {\"event\": \"init\", \"conversation_id\": \"not-a-uuid\"}\nsubsequent text 11111111-2222-3333-4444-555555555555", wantUUID: "11111111-2222-3333-4444-555555555555"},
	} {
		if got := ExtractSessionID(tc.input, time.Now()); got != tc.wantUUID {
			t.Errorf("%s: ExtractSessionID(%q) = %q, want %q", tc.name, tc.input, got, tc.wantUUID)
		}
	}
}

func TestIsInactivityTimeout(t *testing.T) {
	for _, tc := range []struct {
		name, errDetail, stderr string
		want                    bool
	}{
		{name: "Inactivity in errDetail", errDetail: "[watchdog] inactivity timeout exceeded (5m without output)", want: true}, {name: "Inactivity in stderr", errDetail: "some other detail", stderr: "logs...\n[watchdog] inactivity timeout exceeded (5m0s without output or transcript update)", want: true}, {name: "Case insensitivity", errDetail: "[WATCHDOG] Inactivity Timeout Exceeded", want: true}, {name: "Max duration is not inactivity timeout", errDetail: "[watchdog] max duration exceeded (60m0s total duration cap)"},
		{name: "Transient context deadline exceeded is not watchdog inactivity", errDetail: "context deadline exceeded"}, {name: "Generic timeout is not watchdog inactivity", stderr: "request timed out"}, {name: "Empty strings"}, {name: "Watchdog in errDetail and inactivity in stderr", errDetail: "[watchdog]", stderr: "inactivity detected", want: true},
	} {
		if got := IsInactivityTimeout(tc.errDetail, tc.stderr); got != tc.want {
			t.Errorf("%s: IsInactivityTimeout(%q, %q) = %v, want %v", tc.name, tc.errDetail, tc.stderr, got, tc.want)
		}
	}

	if !isWatchdogInactivity("[WATCHDOG] inactivity happened") || isWatchdogInactivity("all good") {
		t.Errorf("unexpected isWatchdogInactivity result")
	}
	if !isWatchdogMaxDuration("[WATCHDOG] max duration reached") || isWatchdogMaxDuration("all good") {
		t.Errorf("unexpected isWatchdogMaxDuration result")
	}
}

func TestIsQuotaPause(t *testing.T) {
	for _, tc := range []struct {
		name, errDetail, stderr string
		want                    bool
	}{
		{name: "Google subscription quota reached", errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", want: true}, {name: "Subscription quota in stderr", errDetail: "execution failed with exit code 1", stderr: "[ERROR] Individual quota reached. Resets in 5m.", want: true}, {name: "Resource has been exhausted with reset timer", errDetail: "Resource has been exhausted (e.g. check quota). Resets in 45s.", want: true}, {name: "Resource exhausted with quota in stderr", errDetail: "429 Too Many Requests", stderr: "RESOURCE_EXHAUSTED: check quota limits. Resets in 30s.", want: true},
		{name: "Generic 503 unavailable", errDetail: "503 Service Unavailable", stderr: "The service is experiencing high demand"}, {name: "Process syntax error", errDetail: "syntax error near token"}, {name: "Google capacity exhausted reset after 0s", errDetail: "API error (attempt 2): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", want: true}, {name: "Google capacity exhausted reset after 1s", errDetail: "API error (attempt 1): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 1s.", want: true},
	} {
		if got := IsQuotaPause(tc.errDetail, tc.stderr); got != tc.want {
			t.Errorf("%s: IsQuotaPause(%q, %q) = %v, want %v", tc.name, tc.errDetail, tc.stderr, got, tc.want)
		}
	}
}

func TestExtractQuotaResetDuration(t *testing.T) {
	for _, tc := range []struct {
		name, errDetail, stderr string
		wantDur                 time.Duration
		wantExact               bool
	}{
		{name: "Standard Google minutes and seconds", errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", wantDur: 16*time.Minute + 58*time.Second, wantExact: true}, {name: "Minutes and seconds with space", errDetail: "Resets in 16m 58s.", wantDur: 16*time.Minute + 58*time.Second, wantExact: true}, {name: "Spelled out unit minutes", errDetail: "Resets in 15 minutes.", wantDur: 15 * time.Minute, wantExact: true}, {name: "Short seconds", errDetail: "Resource has been exhausted. Resets in 45s.", wantDur: 45 * time.Second, wantExact: true},
		{name: "Hours and minutes", errDetail: "Resets in 1h 15m.", wantDur: 1*time.Hour + 15*time.Minute, wantExact: true}, {name: "Reset after 0s preserved exact", errDetail: "You have exhausted your capacity on this model. Your quota will reset after 0s.", wantExact: true}, {name: "Reset after 1s preserved exact", errDetail: "You have exhausted your capacity on this model. Your quota will reset after 1s.", wantDur: time.Second, wantExact: true}, {name: "Reset after compound duration", errDetail: "Your quota will reset after 2m 15s.", wantDur: 2*time.Minute + 15*time.Second, wantExact: true},
		{name: "No reset time specified fallback 20m", errDetail: "Individual quota reached. Please upgrade your subscription.", wantDur: 20 * time.Minute}, {name: "Model capacity without reset time fallback 70s", errDetail: "API error: RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model.", wantDur: 70 * time.Second}, {name: "Rate limit reached without reset time fallback 70s", stderr: "Rate limit reached on model gemini-2.5-pro", wantDur: 70 * time.Second}, {name: "Generic error without capacity words fallback 20m", errDetail: "RESOURCE_EXHAUSTED: server error", wantDur: 20 * time.Minute},
		{name: "Short duration <= 5s preserved exact", errDetail: "Resets in 5s.", wantDur: 5 * time.Second, wantExact: true}, {name: "Negative duration clamped to 0s", errDetail: "Resets in -5s.", wantExact: true}, {name: "Clamped > 24h to 24h", errDetail: "Resets in 48h.", wantDur: 24 * time.Hour, wantExact: true}, {name: "Unparseable duration fallback 20m", errDetail: "Resets in abcdefg.", wantDur: 20 * time.Minute},
	} {
		gotDur, gotExact := ExtractQuotaResetDuration(tc.errDetail, tc.stderr)
		if gotDur != tc.wantDur || gotExact != tc.wantExact {
			t.Errorf("%s: ExtractQuotaResetDuration() = (%v, %v), want (%v, %v)", tc.name, gotDur, gotExact, tc.wantDur, tc.wantExact)
		}
	}
}

func TestIsCapacityBlip(t *testing.T) {
	for _, tc := range []struct {
		name, errDetail, stderr string
		want                    bool
	}{
		{name: "Google capacity exhaustion with 0s countdown", errDetail: "RESOURCE_EXHAUSTED: You have exhausted your capacity on this model. Your quota will reset after 0s.", want: true}, {name: "Google capacity exhaustion with 1s countdown in stderr", stderr: "You have exhausted your capacity on this model. Your quota will reset after 1s.", want: true}, {name: "Model capacity marker without reset time", errDetail: "exhausted your capacity on this model", want: true}, {name: "Rate limit reached phrase", errDetail: "Rate limit reached on model gemini-2.5-pro", want: true},
		{name: "Short reset duration <= 5s without subscription phrases", errDetail: "RESOURCE_EXHAUSTED (code 429). Resets in 3s.", want: true}, {name: "Subscription quota exhaustion with short countdown", errDetail: "Individual quota reached. Please upgrade your subscription. Resets in 2s."}, {name: "Multi-hour individual quota pause", errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 4h 54m."}, {name: "Subscription phrase upgrade prompt", errDetail: "Please upgrade your subscription to increase your limits."},
		{name: "Exceeded current quota phrase", errDetail: "You exceeded your current quota, please check your plan and billing details."}, {name: "Resource exhausted with long reset countdown", errDetail: "Resource has been exhausted. Resets in 45s."}, {name: "Unrelated generic error", errDetail: "execution failed with exit code 1"}, {name: "Empty inputs"},
	} {
		if got := IsCapacityBlip(tc.errDetail, tc.stderr); got != tc.want {
			t.Errorf("%s: IsCapacityBlip(%q, %q) = %v, want %v", tc.name, tc.errDetail, tc.stderr, got, tc.want)
		}
	}
}

func TestIsYieldTrap(t *testing.T) {
	for _, tc := range []struct {
		name           string
		exitCode       int
		stdout, stderr string
		wantYield      bool
		wantCount      int
	}{
		{name: "Clean exit code 0 without background tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"All tasks complete."}`}, {name: "Non-zero exit code with terminating background tasks ignored", exitCode: 1, stderr: "terminating 1 background task(s) on exit"}, {name: "Exit 0 with 1 terminating background task on exit in stderr", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Waiting on background task."}`, stderr: "terminating 1 background task(s) on exit\n", wantYield: true, wantCount: 1}, {name: "Exit 0 with multiple terminating background tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Waiting on tasks."}`, stderr: "terminating 3 background task(s) on exit\n", wantYield: true, wantCount: 3},
		{name: "Exit 0 with terminating background tasks and leaving daemon tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Waiting on tasks."}`, stderr: "terminating 2 background task(s), leaving 1 daemon task(s) running on exit\n", wantYield: true, wantCount: 2}, {name: "Exit 0 with only leaving daemon tasks (no terminating tasks)", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Done."}`, stderr: "leaving 1 daemon task(s) running on exit\n"}, {name: "Exit 0 with terminating 0 background tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Done."}`, stderr: "terminating 0 background task(s) on exit\n"}, {name: "Exit 0 with background task notice in stdout fallback", exitCode: 0, stdout: "terminating 1 background task(s) on exit\n", wantYield: true, wantCount: 1},
	} {
		gotYield, gotCount := IsYieldTrap(tc.exitCode, tc.stdout, tc.stderr)
		if gotYield != tc.wantYield || gotCount != tc.wantCount {
			t.Errorf("%s: IsYieldTrap(%d) = (%v, %d); want (%v, %d)", tc.name, tc.exitCode, gotYield, gotCount, tc.wantYield, tc.wantCount)
		}
	}
}

func TestExtractCommandName(t *testing.T) {
	for _, tc := range []struct{ input, expected string }{
		{"git status", "git"}, {"go test -v ./...", "go"}, {"docker run -d alpine", "docker"}, {"npm install", "npm"}, {"python3 main.py", "python3"}, {"/usr/local/bin/git push", "git"}, {"./scripts/verify.sh --staged", "verify.sh"}, {`C:\Users\Alex\tools\kubectl.exe get pods`, "kubectl.exe"}, {`C:\\tools\\app.exe`, "app.exe"}, {"FOO=bar git diff", "git"},
		{`TOKEN="secret with spaces" ./deploy.sh`, "deploy.sh"}, {"KEY1=val1 KEY2=val2 cargo build", "cargo"}, {"_PRIVATE_KEY=xyz pytest", "pytest"}, {"sudo systemctl restart foo", "systemctl"}, {"time go test ./...", "go"}, {"nohup ./service.sh &", "service.sh"}, {"env -i CI=true cargo build", "cargo"}, {"env --ignore-environment python3 app.py", "python3"},
		{"env -u FOO python script.py", "env"}, {"", ""}, {"   ", ""}, {"FOO=bar", ""}, {"&& rm -rf /", ""}, {"$(which git)", ""}, {"`which git`", ""}, {"< input.txt", ""}, {"(cd foo && make)", ""}, {"ghp_123456789012345678901234567890123456", ""}, {`curl -H "Auth...`, ""}, {"cat << 'EOF' > test.txt", "cat"},
	} {
		if got := ExtractCommandName(tc.input); got != tc.expected {
			t.Errorf("ExtractCommandName(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestStepUpdateEvent_Resolvers(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ev          *StepUpdateEvent
		wantType    string
		wantTool    string
		wantDelta   string
		wantCmdName string
	}{
		{name: "Nil Event"}, {name: "StepType and ToolName", ev: &StepUpdateEvent{StepType: "tool", ToolName: "view_file", State: "ACTIVE"}, wantType: "tool", wantTool: "view_file"}, {name: "Type and Name", ev: &StepUpdateEvent{Type: "tool_call", Name: "run_command", State: "ACTIVE"}, wantType: "tool_call", wantTool: "run_command"}, {name: "Priority: StepType and ToolName override Type and Name", ev: &StepUpdateEvent{StepType: "tool", Type: "other", ToolName: "grep_search", Name: "fallback"}, wantType: "tool", wantTool: "grep_search"},
		{name: "TextDelta Field", ev: &StepUpdateEvent{TextDelta: "chunk 1"}, wantDelta: "chunk 1"}, {name: "Delta Field", ev: &StepUpdateEvent{Delta: "chunk 2"}, wantDelta: "chunk 2"}, {name: "Agent Response Content", ev: &StepUpdateEvent{StepType: "agent_response", Content: "chunk 3"}, wantType: "agent_response", wantDelta: "chunk 3"}, {name: "Tool Step Content Does Not Populate Delta", ev: &StepUpdateEvent{StepType: "tool", Content: "tool output content"}, wantType: "tool"},
		{name: "Tool Without ToolInfo", ev: &StepUpdateEvent{ToolName: "run_command"}, wantTool: "run_command"}, {name: "CommandLine Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{CommandLine: "git commit -m 'test'"}}}, wantTool: "run_command", wantCmdName: "git"}, {name: "CommandLineSnake Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{CommandLineSnake: "docker ps -a"}}}, wantTool: "run_command", wantCmdName: "docker"}, {name: "Command Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{Command: "npm test"}}}, wantTool: "run_command", wantCmdName: "npm"},
		{name: "Cmd Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{Cmd: "go test ./..."}}}, wantTool: "run_command", wantCmdName: "go"}, {name: "Empty Tool Parameters", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{}}}, wantTool: "run_command"}, {name: "Unparseable Command Line", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{CommandLine: "echo \"unclosed quote"}}}, wantTool: "run_command"}, {name: "Non-Zero Exit Tool Step", ev: &StepUpdateEvent{StepType: "tool", Status: "FAILED", ToolName: "run_command"}, wantType: "tool", wantTool: "run_command"},
		{name: "Unknown Event Type Defaults Gracefully", ev: &StepUpdateEvent{Type: "CUSTOM_UNKNOWN"}, wantType: "custom_unknown"},
	} {
		if tc.ev == nil {
			var nilEv *StepUpdateEvent
			if nilEv.ResolvedType() != "" || nilEv.ResolvedToolName() != "" || nilEv.ResolvedTextDelta() != "" || nilEv.ResolvedCommandName() != "" || nilEv.ResolvedCommandLine() != "" {
				t.Errorf("%s: nil event returned non-empty string", tc.name)
			}
			continue
		}
		for _, check := range []struct{ got, want, field string }{
			{tc.ev.ResolvedType(), tc.wantType, "Type"}, {tc.ev.ResolvedToolName(), tc.wantTool, "ToolName"},
			{tc.ev.ResolvedTextDelta(), tc.wantDelta, "TextDelta"}, {tc.ev.ResolvedCommandName(), tc.wantCmdName, "CommandName"},
		} {
			if check.got != check.want {
				t.Errorf("%s: %s = %q, want %q", tc.name, check.field, check.got, check.want)
			}
		}
	}
}

func TestActivityWriter_SessionDiscoveryAndConcurrency(t *testing.T) {
	w := NewActivityWriter("")
	if w.SessionID() != "" {
		t.Errorf("expected empty initial session ID, got %q", w.SessionID())
	}
	startNano := w.LastActivity().UnixNano()

	chunk := []byte("INFO: Starting conversation update stream for 12345678-abcd-ef01-2345-6789abcdef01\n")
	n, err := w.Write(chunk)
	if err != nil || n != len(chunk) || w.SessionID() != "12345678-abcd-ef01-2345-6789abcdef01" || w.LastActivity().UnixNano() < startNano || w.String() != string(chunk) {
		t.Fatalf("Write checks failed: n=%d, err=%v", n, err)
	}

	if nZero, errZero := w.Write([]byte{}); nZero != 0 || errZero != nil {
		t.Errorf("expected 0, nil from empty write")
	}

	w.ResetBuffer()
	if w.String() != "" || w.SessionID() != "12345678-abcd-ef01-2345-6789abcdef01" {
		t.Errorf("expected empty string and preserved session ID after ResetBuffer")
	}
	var nilW *ActivityWriter
	nilW.ResetBuffer()

	wPre := NewActivityWriter("11111111-2222-3333-4444-555555555555")
	_, _ = wPre.Write([]byte("INFO: Starting conversation update stream for 99999999-8888-7777-6666-555555555555\n"))
	if wPre.SessionID() != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("expected pre-existing session ID preserved")
	}

	wGen := NewActivityWriter("")
	_, _ = wGen.Write([]byte("Initialized conversation_id: 99999999-8888-7777-6666-555555555555\n"))
	if wGen.SessionID() != "99999999-8888-7777-6666-555555555555" {
		t.Errorf("expected conversation_id extracted")
	}

	wChunk := NewActivityWriter("")
	_, _ = wChunk.Write([]byte("Some logs... Starting conversation update stream for 12345678-abcd-"))
	if wChunk.SessionID() != "" {
		t.Errorf("expected empty session after chunk1")
	}
	_, _ = wChunk.Write([]byte("ef01-2345-6789abcdef01 and more logs..."))
	if wChunk.SessionID() != "12345678-abcd-ef01-2345-6789abcdef01" {
		t.Errorf("expected extracted session after chunk2")
	}

	wRaw := NewActivityWriter("")
	_, _ = wRaw.Write([]byte("some random text with 12345678-abcd-ef01-2345-6789abcdef0z invalid uuid"))
	if wRaw.SessionID() != "" {
		t.Errorf("expected empty session for invalid uuid")
	}
	_, _ = wRaw.Write([]byte("some random text with 11111111-2222-3333-4444-555555555555 embedded inside"))
	if wRaw.SessionID() != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("expected extracted raw UUID")
	}

	wSet := NewActivityWriter("")
	wSet.SetSessionID("not-a-uuid")
	if wSet.SessionID() != "" {
		t.Errorf("expected empty session after invalid UUID")
	}
	wSet.SetSessionID("11111111-2222-3333-4444-555555555555")
	wSet.SetSessionID("99999999-8888-7777-6666-555555555555")
	if wSet.SessionID() != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("expected original session preserved")
	}

	wConc := NewActivityWriter("")
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 50; j++ {
				_, _ = wConc.Write([]byte(fmt.Sprintf("log line from worker %d turn %d\n", id, j)))
				_ = wConc.LastActivity()
				_ = wConc.SessionID()
				_ = wConc.String()
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	if wConc.LastActivity().IsZero() || len(wConc.String()) == 0 {
		t.Errorf("expected non-zero activity and non-empty buffer")
	}
}

func TestErrorAndWatchdogDetailHelpers(t *testing.T) {
	longLine := "[watchdog] " + strings.Repeat("x", 250)
	if detail := extractWatchdogDetail(longLine, "fallback"); len(detail) != 200 || !strings.HasSuffix(detail, "...") {
		t.Errorf("expected 200-char truncated detail with ..., got len %d: %q", len(detail), detail)
	}
	if fallback := extractWatchdogDetail("regular log line 1\nregular log line 2", "my-fallback-keyword"); fallback != "my-fallback-keyword" {
		t.Errorf("expected fallback keyword, got %q", fallback)
	}

	stderrWithNoise := "\nStarting conversation update stream for session-123\nDEBUG: initializing subsystem\nINFO: connecting to gateway\n\nActual error occurred here\n"
	if detailNoise := extractErrorDetail(stderrWithNoise, 1); detailNoise != "Actual error occurred here" {
		t.Errorf("expected 'Actual error occurred here', got %q", detailNoise)
	}

	longError := strings.Repeat("e", 250)
	if detailLong := extractErrorDetail(longError, 1); len(detailLong) != 200 || !strings.HasSuffix(detailLong, "...") {
		t.Errorf("expected 200-char truncated detail, got len %d: %q", len(detailLong), detailLong)
	}

	onlyNoise := "DEBUG: step 1\nINFO: step 2\nStarting conversation update stream for test\n"
	if detailOnlyNoise := extractErrorDetail(onlyNoise, 42); detailOnlyNoise != "execution failed with exit code 42" {
		t.Errorf("expected fallback for only-noise stderr, got %q", detailOnlyNoise)
	}

	if !isNonTransientError("fork/exec /bin/sh: permission denied") {
		t.Errorf("expected true for fork/exec permission denied")
	}
	if isNonTransientError("unknown model key MODEL_PLACEHOLDER_M417: model not found") {
		t.Errorf("expected false for MODEL_PLACEHOLDER_ model error")
	}
}

func TestTokenizeCommandLine_EdgeCases(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  []string
		ok    bool
	}{
		{"foo\\", nil, false}, {"\"a\\$b\\`c\\\"d\\\\e\\nf\\xg\"", []string{"a$b`c\"d\\e\\nf\\xg"}, true}, {"a\\ b\\tc\\\"d\\'e\\\\f\\xg", []string{"a b\\tc\"d'e\\f\\xg"}, true}, {"'unclosed", nil, false}, {`"unclosed`, nil, false}, {"", nil, true}, {"   \t\r\n  ", nil, true},
	} {
		got, ok := tokenizeCommandLine(tc.input)
		if ok != tc.ok || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("tokenizeCommandLine(%q) = (%v, %v), want (%v, %v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

func TestIsPOSIXIdentifier(t *testing.T) {
	for _, s := range []string{"foo", "Foo", "_bar", "_", "FOO_BAR_123", "a1"} {
		if !isPOSIXIdentifier(s) {
			t.Errorf("isPOSIXIdentifier(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "1foo", "foo-bar", "foo bar", "foo@bar", "foo.bar", "foo/bar"} {
		if isPOSIXIdentifier(s) {
			t.Errorf("isPOSIXIdentifier(%q) = true, want false", s)
		}
	}
}
