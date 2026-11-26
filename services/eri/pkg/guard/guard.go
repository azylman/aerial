package guard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"strings"
	"time"
)

// Tool names the guards inspect.
const (
	toolBash               = "bash"
	toolRunCommand         = "run_command"
	toolCallMCPTool        = "call_mcp_tool"
	toolRead               = "read"
	toolEdit               = "edit"
	toolReadFile           = "read_file"
	toolViewFile           = "view_file"
	toolEditFile           = "edit_file"
	toolWriteFile          = "write_file"
	toolReplaceFileContent = "replace_file_content"
	toolWriteToFile        = "write_to_file"
	toolMultiEdit          = "multiedit"
	toolWrite              = "write"
)

// argCommandLine is the tool argument key carrying a shell command.
const argCommandLine = "CommandLine"

// Shell binaries, subcommands and flags the command parsers recognize.
const (
	binBash             = "bash"
	binGit              = "git"
	binGrep             = "grep"
	binStrings          = "strings"
	binInstall          = "install"
	gitSubcommandCommit = "commit"
	flagHelp            = "--help"
	flagQuiet           = "--quiet"
	flagVersion         = "--version"
	flagMaxCount        = "--max-count"
)

// Filesystem roots and directory names the guards treat specially.
const (
	sharedEngineRoot = "/share/aerial"
	procRoot         = "/proc"
	dirPkg           = "pkg"
)

// Mode represents the guard operating mode.
type Mode string

const (
	ModeAll       Mode = "all"
	ModeShare     Mode = "share"
	ModeLiveState Mode = "live-state"
	ModeSchedule  Mode = "schedule"
	ModeBatch     Mode = "batch"
	ModePost      Mode = "post-batch"
	ModeInv       Mode = "pre-invocation"
	ModeSearch    Mode = "search"
	ModeCommit    Mode = "commit"
)

// IsInvocationMode reports whether mode uses the InvocationResponse ({}) stdout contract.
func IsInvocationMode(mode string) bool {
	return mode == string(ModeInv) || mode == "invocation" || mode == string(ModePost) || mode == "post-tool"
}

// FormatClaudeResponse formats a Decision into Claude Code's hook output schema.
//
// Guards emit overwrites as a sparse map of canonical Antigravity keys, which agy
// merges field-by-field into the tool call. Claude Code instead replaces
// tool_input wholesale with updatedInput, so this adapter is the single place
// that merges the translated overwrite into a copy of the original toolInput.
func FormatClaudeResponse(decision *Decision, toolInput json.RawMessage) ClaudeHookResponse {
	resp := ClaudeHookResponse{
		HookSpecificOutput: ClaudeHookSpecificOutput{
			HookEventName:      "PreToolUse",
			PermissionDecision: "allow",
		},
	}
	if decision == nil {
		return resp
	}
	if decision.Decision == DecisionDeny {
		resp.HookSpecificOutput.PermissionDecision = "deny"
		resp.HookSpecificOutput.PermissionDecisionReason = decision.Reason
		return resp
	}
	if len(decision.Overwrite) > 0 {
		resp.HookSpecificOutput.UpdatedInput = mergeClaudeOverwrite(toolInput, decision.Overwrite)
	}
	return resp
}

// mergeClaudeOverwrite applies canonical agy overwrite keys onto a copy of the
// original Claude/Codex tool_input, translating key names to the target schema.
func mergeClaudeOverwrite(toolInput json.RawMessage, overwrite map[string]any) map[string]any {
	merged := map[string]any{}
	var hasCmdKey, hasCommandKey bool
	if len(toolInput) > 0 {
		var original map[string]any
		if err := json.Unmarshal(toolInput, &original); err == nil && original != nil {
			merged = original
			_, hasCmdKey = original["cmd"]
			_, hasCommandKey = original["command"]
		}
	}
	for k, v := range overwrite {
		switch k {
		case argCommandLine:
			if hasCmdKey && !hasCommandKey {
				merged["cmd"] = v
			} else {
				merged["command"] = v
			}
		case "StartLine":
			merged["offset"] = v
		case "EndLine":
			if start, ok := overwrite["StartLine"].(int); ok {
				if end, okEnd := v.(int); okEnd && end >= start {
					merged["limit"] = end - start + 1
				}
			}
		case "Arguments":
			if args, ok := v.(map[string]any); ok {
				maps.Copy(merged, args)
			}
		case "WaitMsBeforeAsync":
			// agy-only async threshold; Claude's Bash schema has no equivalent.
		default:
			merged[k] = v
		}
	}
	return merged
}

// Options carries every environment-dependent input of the guards so library code never reads
// the process environment: BatchDir holds batch-guard state, Getenv resolves the remaining
// settings (SCRATCH_DIR, AERIAL_COMMIT_GUARD_PATTERNS, HOME, ...) and SQL describes psql
// queries (nil skips plan validation). EgressScanner scans MCP call arguments bound for
// blocklisted destinations; nil counts as unavailable, so MCP calls are denied. Steerer drains
// pending mid-turn user steering prompts on active session turns.
type Options struct {
	BatchDir      string
	Getenv        func(string) string
	SQL           SQLDescriber
	EgressScanner EgressScanner
	Steerer       TurnSteerer
}

// OptionsFromEnv builds the production Options from getenv (os.Getenv in hook-guard's main): the batch directory from
// AERIAL_BATCH_GUARD_DIR (default DefaultBatchDir), a describer from DATABASE_URL and the HTTP
// egress scanner targeting eri-restate's loopback egress scan endpoint.
func OptionsFromEnv(getenv func(string) string) Options {
	if getenv == nil {
		getenv = noEnv
	}
	opts := Options{
		BatchDir:      GetBatchDir(getenv),
		Getenv:        getenv,
		SQL:           DefaultSQLDescriber(getenv),
		EgressScanner: NewHTTPEgressScanner(),
	}
	if daemonID := strings.TrimSpace(getenv("ERI_DAEMON_ID")); daemonID != "" {
		opts.Steerer = &HTTPTurnSteerer{
			Endpoint: SteerDrainURL,
			DaemonID: daemonID,
		}
	}
	return opts
}

// ProcessWithOptions reads an Antigravity PreToolUse hook payload from r, evaluates guards per
// mode with opts, and writes the resulting Decision JSON to w.
// Guaranteed fail-open: on unexpected error or panic, it outputs {"decision": "allow"}.
func ProcessWithOptions(r io.Reader, w io.Writer, mode string, opts Options) (err error) {
	if opts.Getenv == nil {
		opts.Getenv = noEnv
	}
	var isClaude bool
	writeAllow := func() error {
		if isClaude {
			return json.NewEncoder(w).Encode(FormatClaudeResponse(&Decision{Decision: DecisionAllow}, nil))
		}
		if IsInvocationMode(mode) {
			return json.NewEncoder(w).Encode(InvocationResponse{})
		}
		return json.NewEncoder(w).Encode(Decision{Decision: DecisionAllow})
	}

	defer func() {
		if rec := recover(); rec != nil {
			if encErr := writeAllow(); encErr != nil {
				err = encErr
			} else {
				err = nil
			}
		}
	}()

	data, readErr := io.ReadAll(r)
	if readErr != nil || len(bytes.TrimSpace(data)) == 0 {
		return writeAllow()
	}

	var payload Payload
	if parseErr := json.Unmarshal(data, &payload); parseErr != nil {
		return writeAllow()
	}

	isClaude = payload.IsClaude()
	payload.Normalize()

	args := ParseArgs(payload.ToolCall.Args)
	args.AdoptClaudeMCPCall(payload.ToolCall.Name, payload.ToolCall.Args)
	// Claude Code carries the shell cwd at the payload top level, not in tool_input.
	if args.Cwd == "" {
		args.Cwd = payload.Cwd
	}
	args.Namespace = payload.Namespace

	switch mode {
	case string(ModeInv), "invocation":
		return handleInvocationPreMode(w, payload, opts)
	case string(ModePost), "post-tool":
		return json.NewEncoder(w).Encode(RecordPostBatchWithDir(payload, args, opts.BatchDir, time.Now(), opts.Getenv))
	default:
		decision := evaluateGuardMode(payload, args, mode, opts)
		if isClaude {
			return json.NewEncoder(w).Encode(formatClaudeDecision(decision, payload, opts))
		}
		return json.NewEncoder(w).Encode(decision)
	}
}

func handleInvocationPreMode(w io.Writer, payload Payload, opts Options) error {
	resp := RecordPreInvocationWithDir(payload, opts.BatchDir, time.Now())
	if payload.InvocationNum > 1 && opts.Steerer != nil {
		prompts := opts.Steerer.DrainPending(context.Background(), payload)
		for _, p := range prompts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				resp.InjectSteps = append(resp.InjectSteps, InvocationStep{UserMessage: trimmed})
			}
		}
	}
	return json.NewEncoder(w).Encode(resp)
}

func formatClaudeDecision(decision *Decision, payload Payload, opts Options) ClaudeHookResponse {
	claudeResp := FormatClaudeResponse(decision, payload.ToolInput)
	if decision == nil || decision.Decision != DecisionAllow || opts.Steerer == nil || strings.TrimSpace(payload.AgentID) != "" {
		return claudeResp
	}
	prompts := opts.Steerer.DrainPending(context.Background(), payload)
	var clean []string
	for _, p := range prompts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			clean = append(clean, trimmed)
		}
	}
	if len(clean) > 0 {
		claudeResp.HookSpecificOutput.AdditionalContext = strings.Join(clean, "\n\n")
	}
	return claudeResp
}

// noEnv resolves every variable to empty; it stands in for a missing Options.Getenv.
func noEnv(string) string { return "" }

func evaluateGuardMode(payload Payload, args ToolArgs, mode string, opts Options) *Decision {
	switch mode {
	case "share":
		return CheckShare(payload.ToolCall.Name, args)
	case string(ModeLiveState):
		return CheckLiveState(payload.ToolCall.Name, args)
	case "schedule":
		return CheckSchedule(payload, args)
	case "batch":
		return CheckBatchWithDir(payload, args, opts.BatchDir, time.Now(), opts.Getenv)
	case "search":
		return CheckSearchWithSQL(payload.ToolCall.Name, args, opts.SQL)
	case string(ModeCommit):
		return CheckOutboundSecrets(payload.ToolCall.Name, payload.ToolCall.Args, args, opts.Getenv, opts.EgressScanner)
	default:
		return evaluateAllGuards(payload, args, opts)
	}
}

func evaluateAllGuards(payload Payload, args ToolArgs, opts Options) *Decision {
	checks := []func() *Decision{
		func() *Decision { return CheckShare(payload.ToolCall.Name, args) },
		func() *Decision { return CheckLiveState(payload.ToolCall.Name, args) },
		func() *Decision { return CheckSchedule(payload, args) },
		func() *Decision { return CheckBatchWithDir(payload, args, opts.BatchDir, time.Now(), opts.Getenv) },
		func() *Decision { return CheckSearchWithSQL(payload.ToolCall.Name, args, opts.SQL) },
		func() *Decision {
			return CheckOutboundSecrets(payload.ToolCall.Name, payload.ToolCall.Args, args, opts.Getenv, opts.EgressScanner)
		},
	}
	var lastDecision *Decision
	var mergedOverwrite map[string]any
	for _, check := range checks {
		lastDecision = check()
		if lastDecision.Decision == DecisionDeny {
			return lastDecision
		}
		for k, v := range lastDecision.Overwrite {
			if mergedOverwrite == nil {
				mergedOverwrite = make(map[string]any)
			}
			mergedOverwrite[k] = v
			if k == argCommandLine {
				if s, ok := v.(string); ok {
					args.CommandLine = s
				}
			}
		}
	}
	if lastDecision == nil {
		return &Decision{Decision: DecisionAllow}
	}
	lastDecision.Overwrite = mergedOverwrite
	return lastDecision
}
