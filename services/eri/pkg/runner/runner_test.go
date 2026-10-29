package runner

import (
	"reflect"
	"testing"
	"time"
)

func TestExtractSessionID(t *testing.T) {
	for _, tc := range []struct{ name, input, wantUUID string }{
		{name: "NDJSON Init Event", input: "{\"event\":\"init\",\"conversation_id\":\"88888888-9999-aaaa-bbbb-cccccccccccc\"}\n{\"event\":\"step_update\"}\n", wantUUID: "88888888-9999-aaaa-bbbb-cccccccccccc"},
		{name: "Conversation Update Stream In Stderr", input: "2026-08-28T10:00:00Z Starting conversation update stream for 12345678-abcd-1234-abcd-1234567890ab\nConnecting...", wantUUID: "12345678-abcd-1234-abcd-1234567890ab"},
		{name: "Initialized session_id Format", input: "Initialized session_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee successfully", wantUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
		{name: "Ignore Session Started Common Words", input: "Session started\nStarting session now"},
		{name: "Ignore Conversation Initialization Failed", input: "Conversation initialization-failed: could not load"},
		{name: "RFC4122 In Update Stream Line", input: "Starting conversation update stream for 12345678-abcd-ef01-2345-6789abcdef01", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "RFC4122 In conversation_id Line", input: "conversation_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", wantUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
		{name: "Invalid Short Hex Length", input: "conversation: 1234-5678"},
		{name: "Invalid Update Stream UUID Format", input: "Starting conversation update stream for invalid-uuid-format\n"},
		{name: "Invalid Candidate In General Session", input: "conversation_id: not-a-valid-uuid\n"},
		{name: "Raw UUID In Unstructured Trace", input: "Some unstructured error log mentioning 12345678-abcd-ef01-2345-6789abcdef01 in the trace", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON Inverted Key Order", input: "{\"conversation_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"event\":\"init\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON session_id Key", input: "{\"event\":\"init\",\"session_id\":\"12345678-abcd-ef01-2345-6789abcdef01\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON camelCase conversationId", input: "{\"event\":\"init\",\"conversationId\":\"12345678-abcd-ef01-2345-6789abcdef01\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON camelCase sessionId", input: "{\"event\":\"init\",\"sessionId\":\"12345678-abcd-ef01-2345-6789abcdef01\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON step_update session_id", input: "{\"event\":\"step_update\",\"session_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"type\":\"tool_call\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON result session_id", input: "{\"event\":\"result\",\"session_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"status\":\"SUCCESS\"}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON Nested Data conversation_id", input: "{\"event\":\"init\",\"data\":{\"conversation_id\":\"12345678-abcd-ef01-2345-6789abcdef01\"}}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON Nested Result conversation_id", input: "{\"event\":\"result\",\"result\":{\"conversation_id\":\"12345678-abcd-ef01-2345-6789abcdef01\",\"status\":\"SUCCESS\"}}\n", wantUUID: "12345678-abcd-ef01-2345-6789abcdef01"},
		{name: "JSON Init session_id Single Line", input: `{"event":"init","session_id":"123e4567-e89b-12d3-a456-426614174000"}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "JSON Init conversation_id Single Line", input: `{"event":"init","conversation_id":"123e4567-e89b-12d3-a456-426614174000"}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "JSON Nested step_update session_id Single Line", input: `{"event":"step_update","step_update":{"session_id":"123e4567-e89b-12d3-a456-426614174000"}}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "NDJSON Init session Field Fallback", input: `{"event":"init","session":"123e4567-e89b-12d3-a456-426614174000"}`, wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "Text session_id Prefix Fallback", input: "session_id: 123e4567-e89b-12d3-a456-426614174000", wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "Raw Text UUID Prefix/Suffix", input: "some raw prefix 123e4567-e89b-12d3-a456-426614174000 suffix", wantUUID: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "Empty Output Returns Empty String", input: ""},
		{name: "Embedded JSON In Raw Log Line", input: `raw stdout log: {"event": "init", "conversation_id": "11111111-2222-3333-4444-555555555555"}`, wantUUID: "11111111-2222-3333-4444-555555555555"},
		{name: "Logger Prefix With Braces Before JSON", input: `[2026-09-19 {thread-1}] {"event": "init", "conversation_id": "11111111-2222-3333-4444-555555555555"}`, wantUUID: "11111111-2222-3333-4444-555555555555"},
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
		{name: "Inactivity in errDetail", errDetail: "[watchdog] inactivity timeout exceeded (5m without output)", want: true},
		{name: "Inactivity in stderr", errDetail: "some other detail", stderr: "logs...\n[watchdog] inactivity timeout exceeded (5m0s without output or transcript update)", want: true},
		{name: "Case insensitivity", errDetail: "[WATCHDOG] Inactivity Timeout Exceeded", want: true},
		{name: "Max duration is not inactivity timeout", errDetail: "[watchdog] max duration exceeded (60m0s total duration cap)"},
		{name: "Transient context deadline exceeded is not watchdog inactivity", errDetail: "context deadline exceeded"},
		{name: "Generic timeout is not watchdog inactivity", stderr: "request timed out"},
		{name: "Empty strings"},
		{name: "Watchdog in errDetail and inactivity in stderr", errDetail: "[watchdog]", stderr: "inactivity detected", want: true},
	} {
		if got := IsInactivityTimeout(tc.errDetail, tc.stderr); got != tc.want {
			t.Errorf("%s: IsInactivityTimeout(%q, %q) = %v, want %v", tc.name, tc.errDetail, tc.stderr, got, tc.want)
		}
	}
}

func TestIsQuotaPause(t *testing.T) {
	for _, tc := range []struct {
		name, errDetail, stderr string
		want                    bool
	}{
		{name: "Google subscription quota reached", errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", want: true},
		{name: "Subscription quota in stderr", errDetail: "execution failed with exit code 1", stderr: "[ERROR] Individual quota reached. Resets in 5m.", want: true},
		{name: "Resource has been exhausted with reset timer", errDetail: "Resource has been exhausted (e.g. check quota). Resets in 45s.", want: true},
		{name: "Resource exhausted with quota in stderr", errDetail: "429 Too Many Requests", stderr: "RESOURCE_EXHAUSTED: check quota limits. Resets in 30s.", want: true},
		{name: "Generic 503 unavailable", errDetail: "503 Service Unavailable", stderr: "The service is experiencing high demand"},
		{name: "Process syntax error", errDetail: "syntax error near token"},
		{name: "Google capacity exhausted reset after 0s", errDetail: "API error (attempt 2): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", want: true},
		{name: "Google capacity exhausted reset after 1s", errDetail: "API error (attempt 1): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 1s.", want: true},
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
		{name: "Standard Google minutes and seconds", errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.", wantDur: 16*time.Minute + 58*time.Second, wantExact: true},
		{name: "Minutes and seconds with space", errDetail: "Resets in 16m 58s.", wantDur: 16*time.Minute + 58*time.Second, wantExact: true},
		{name: "Spelled out unit minutes", errDetail: "Resets in 15 minutes.", wantDur: 15 * time.Minute, wantExact: true},
		{name: "Short seconds", errDetail: "Resource has been exhausted. Resets in 45s.", wantDur: 45 * time.Second, wantExact: true},
		{name: "Hours and minutes", errDetail: "Resets in 1h 15m.", wantDur: 1*time.Hour + 15*time.Minute, wantExact: true},
		{name: "Reset after 0s preserved exact", errDetail: "You have exhausted your capacity on this model. Your quota will reset after 0s.", wantExact: true},
		{name: "Reset after 1s preserved exact", errDetail: "You have exhausted your capacity on this model. Your quota will reset after 1s.", wantDur: time.Second, wantExact: true},
		{name: "Reset after compound duration", errDetail: "Your quota will reset after 2m 15s.", wantDur: 2*time.Minute + 15*time.Second, wantExact: true},
		{name: "No reset time specified fallback 20m", errDetail: "Individual quota reached. Please upgrade your subscription.", wantDur: 20 * time.Minute},
		{name: "Model capacity without reset time fallback 70s", errDetail: "API error: RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model.", wantDur: 70 * time.Second},
		{name: "Rate limit reached without reset time fallback 70s", stderr: "Rate limit reached on model gemini-2.5-pro", wantDur: 70 * time.Second},
		{name: "Generic error without capacity words fallback 20m", errDetail: "RESOURCE_EXHAUSTED: server error", wantDur: 20 * time.Minute},
		{name: "Short duration <= 5s preserved exact", errDetail: "Resets in 5s.", wantDur: 5 * time.Second, wantExact: true},
		{name: "Negative duration clamped to 0s", errDetail: "Resets in -5s.", wantExact: true},
		{name: "Clamped > 24h to 24h", errDetail: "Resets in 48h.", wantDur: 24 * time.Hour, wantExact: true},
		{name: "Unparseable duration fallback 20m", errDetail: "Resets in abcdefg.", wantDur: 20 * time.Minute},
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
		{name: "Google capacity exhaustion with 0s countdown", errDetail: "RESOURCE_EXHAUSTED: You have exhausted your capacity on this model. Your quota will reset after 0s.", want: true},
		{name: "Google capacity exhaustion with 1s countdown in stderr", stderr: "You have exhausted your capacity on this model. Your quota will reset after 1s.", want: true},
		{name: "Model capacity marker without reset time", errDetail: "exhausted your capacity on this model", want: true},
		{name: "Rate limit reached phrase", errDetail: "Rate limit reached on model gemini-2.5-pro", want: true},
		{name: "Short reset duration <= 5s without subscription phrases", errDetail: "RESOURCE_EXHAUSTED (code 429). Resets in 3s.", want: true},
		{name: "Subscription quota exhaustion with short countdown", errDetail: "Individual quota reached. Please upgrade your subscription. Resets in 2s."},
		{name: "Multi-hour individual quota pause", errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 4h 54m."},
		{name: "Subscription phrase upgrade prompt", errDetail: "Please upgrade your subscription to increase your limits."},
		{name: "Exceeded current quota phrase", errDetail: "You exceeded your current quota, please check your plan and billing details."},
		{name: "Resource exhausted with long reset countdown", errDetail: "Resource has been exhausted. Resets in 45s."},
		{name: "Unrelated generic error", errDetail: "execution failed with exit code 1"},
		{name: "Empty inputs"},
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
		{name: "Clean exit code 0 without background tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"All tasks complete."}`},
		{name: "Non-zero exit code with terminating background tasks ignored", exitCode: 1, stderr: "terminating 1 background task(s) on exit"},
		{name: "Exit 0 with 1 terminating background task on exit in stderr", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Waiting on background task."}`, stderr: "terminating 1 background task(s) on exit\n", wantYield: true, wantCount: 1},
		{name: "Exit 0 with multiple terminating background tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Waiting on tasks."}`, stderr: "terminating 3 background task(s) on exit\n", wantYield: true, wantCount: 3},
		{name: "Exit 0 with terminating background tasks and leaving daemon tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Waiting on tasks."}`, stderr: "terminating 2 background task(s), leaving 1 daemon task(s) running on exit\n", wantYield: true, wantCount: 2},
		{name: "Exit 0 with only leaving daemon tasks (no terminating tasks)", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Done."}`, stderr: "leaving 1 daemon task(s) running on exit\n"},
		{name: "Exit 0 with terminating 0 background tasks", exitCode: 0, stdout: `{"status":"SUCCESS","response":"Done."}`, stderr: "terminating 0 background task(s) on exit\n"},
		{name: "Exit 0 with background task notice in stdout fallback", exitCode: 0, stdout: "terminating 1 background task(s) on exit\n", wantYield: true, wantCount: 1},
	} {
		gotYield, gotCount := IsYieldTrap(tc.exitCode, tc.stdout, tc.stderr)
		if gotYield != tc.wantYield || gotCount != tc.wantCount {
			t.Errorf("%s: IsYieldTrap(%d) = (%v, %d); want (%v, %d)", tc.name, tc.exitCode, gotYield, gotCount, tc.wantYield, tc.wantCount)
		}
	}
}

func TestExtractCommandName(t *testing.T) {
	for _, tc := range []struct{ input, expected string }{
		{"git status", "git"},
		{"go test -v ./...", "go"},
		{"docker run -d alpine", "docker"},
		{"npm install", "npm"},
		{"python3 main.py", "python3"},
		{"/usr/local/bin/git push", "git"},
		{"./scripts/verify.sh --staged", "verify.sh"},
		{`C:\Users\Alex\tools\kubectl.exe get pods`, "kubectl.exe"},
		{`C:\\tools\\app.exe`, "app.exe"},
		{"FOO=bar git diff", "git"},
		{`TOKEN="secret with spaces" ./deploy.sh`, "deploy.sh"},
		{"KEY1=val1 KEY2=val2 cargo build", "cargo"},
		{"_PRIVATE_KEY=xyz pytest", "pytest"},
		{"sudo systemctl restart foo", "systemctl"},
		{"time go test ./...", "go"},
		{"nohup ./service.sh &", "service.sh"},
		{"env -i CI=true cargo build", "cargo"},
		{"env --ignore-environment python3 app.py", "python3"},
		{"env -u FOO python script.py", "env"},
		{"", ""},
		{"   ", ""},
		{"FOO=bar", ""},
		{"&& rm -rf /", ""},
		{"$(which git)", ""},
		{"`which git`", ""},
		{"< input.txt", ""},
		{"(cd foo && make)", ""},
		{"ghp_123456789012345678901234567890123456", ""},
		{`curl -H "Auth...`, ""},
		{"cat << 'EOF' > test.txt", "cat"},
	} {
		if got := ExtractCommandName(tc.input); got != tc.expected {
			t.Errorf("ExtractCommandName(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestStepUpdateEvent_Resolvers(t *testing.T) {
	for _, tc := range []struct {
		name           string
		ev             *StepUpdateEvent
		wantType       string
		wantTool       string
		wantCmdName    string
		wantToolDetail string
	}{
		{name: "Nil Event"},
		{name: "StepType and ToolName", ev: &StepUpdateEvent{StepType: "tool", ToolName: "view_file", State: "ACTIVE"}, wantType: "tool", wantTool: "view_file"},
		{name: "Type and Name", ev: &StepUpdateEvent{Type: "tool_call", Name: "run_command", State: "ACTIVE"}, wantType: "tool_call", wantTool: "run_command"},
		{name: "Priority: StepType and ToolName override Type and Name", ev: &StepUpdateEvent{StepType: "tool", Type: "other", ToolName: "grep_search", Name: "fallback"}, wantType: "tool", wantTool: "grep_search"},
		{name: "Tool Without ToolInfo", ev: &StepUpdateEvent{ToolName: "run_command"}, wantTool: "run_command"},
		{name: "CommandLine Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{CommandLine: "git commit -m 'test'"}}}, wantTool: "run_command", wantCmdName: "git", wantToolDetail: "git"},
		{name: "CommandLineSnake Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{CommandLineSnake: "docker ps -a"}}}, wantTool: "run_command", wantCmdName: "docker", wantToolDetail: "docker"},
		{name: "Command Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{Command: "npm test"}}}, wantTool: "run_command", wantCmdName: "npm", wantToolDetail: "npm"},
		{name: "Cmd Parameter", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{Cmd: "go test ./..."}}}, wantTool: "run_command", wantCmdName: "go", wantToolDetail: "go"},
		{name: "Empty Tool Parameters", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{}}}, wantTool: "run_command"},
		{name: "Unparseable Command Line", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Name: "run_command", Parameters: StepToolParameters{CommandLine: "echo \"unclosed quote"}}}, wantTool: "run_command"},
		{name: "Non-Zero Exit Tool Step", ev: &StepUpdateEvent{StepType: "tool", Status: "FAILED", ToolName: "run_command"}, wantType: "tool", wantTool: "run_command"},
		{name: "Unknown Event Type Defaults Gracefully", ev: &StepUpdateEvent{Type: "CUSTOM_UNKNOWN"}, wantType: "custom_unknown"},
		{name: "Explicit ToolDetail preserves multi-word and punctuation", ev: &StepUpdateEvent{ToolName: "invoke_subagent", ToolDetail: "Codebase Researcher"}, wantTool: "invoke_subagent", wantToolDetail: "Codebase Researcher"},
		{name: "StepToolInfo Detail takes precedence over CommandLine", ev: &StepUpdateEvent{ToolName: "run_command", ToolInfo: &StepToolInfo{Detail: "Build Go Binary", Parameters: StepToolParameters{CommandLine: "go build ./..."}}}, wantTool: "run_command", wantCmdName: "go", wantToolDetail: "Build Go Binary"},
	} {
		if tc.ev == nil {
			var nilEv *StepUpdateEvent
			if nilEv.ResolvedType() != "" || nilEv.ResolvedToolName() != "" || nilEv.ResolvedCommandName() != "" || nilEv.ResolvedCommandLine() != "" || nilEv.ResolvedToolDetail() != "" {
				t.Errorf("%s: nil event returned non-empty string", tc.name)
			}
			continue
		}
		for _, check := range []struct{ got, want, field string }{
			{tc.ev.ResolvedType(), tc.wantType, "Type"},
			{tc.ev.ResolvedToolName(), tc.wantTool, "ToolName"},
			{tc.ev.ResolvedCommandName(), tc.wantCmdName, "CommandName"},
			{tc.ev.ResolvedToolDetail(), tc.wantToolDetail, "ToolDetail"},
		} {
			if check.got != check.want {
				t.Errorf("%s: %s = %q, want %q", tc.name, check.field, check.got, check.want)
			}
		}
	}
}

func TestErrorAndWatchdogDetailHelpers(t *testing.T) {
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
