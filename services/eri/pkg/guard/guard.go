package guard

import (
	"bytes"
	"encoding/json"
	"io"
)

// Mode represents the guard operating mode.
type Mode string

const (
	ModeAll      Mode = "all"
	ModeShare    Mode = "share"
	ModeSchedule Mode = "schedule"
	ModeBatch    Mode = "batch"
	ModePost     Mode = "post-batch"
	ModeInv      Mode = "pre-invocation"
	ModeSearch   Mode = "search"
	ModeCommit   Mode = "commit"
)

// IsInvocationMode reports whether mode uses the InvocationResponse ({}) stdout contract.
func IsInvocationMode(mode string) bool {
	return mode == string(ModeInv) || mode == "invocation" || mode == string(ModePost) || mode == "post-tool"
}

// FormatClaudeResponse formats a Decision into Claude Code's hook output schema.
func FormatClaudeResponse(decision *Decision) ClaudeHookResponse {
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
		resp.HookSpecificOutput.UpdatedInput = make(map[string]any, len(decision.Overwrite))
		for k, v := range decision.Overwrite {
			switch k {
			case "CommandLine":
				resp.HookSpecificOutput.UpdatedInput["command"] = v
			case "StartLine":
				resp.HookSpecificOutput.UpdatedInput["offset"] = v
			case "EndLine":
				if start, ok := decision.Overwrite["StartLine"].(int); ok {
					if end, okEnd := v.(int); okEnd && end >= start {
						resp.HookSpecificOutput.UpdatedInput["limit"] = end - start + 1
					}
				}
			default:
				resp.HookSpecificOutput.UpdatedInput[k] = v
			}
		}
	}
	return resp
}

// Process reads an Antigravity PreToolUse hook payload from r, evaluates guards per mode,
// and writes the resulting Decision JSON to w.
// Guaranteed fail-open: on unexpected error or panic, it outputs {"decision": "allow"}.
func Process(r io.Reader, w io.Writer, mode string) (err error) {
	var isClaude bool
	writeAllow := func() error {
		if isClaude {
			return json.NewEncoder(w).Encode(FormatClaudeResponse(&Decision{Decision: DecisionAllow}))
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

	switch mode {
	case string(ModeInv), "invocation":
		return json.NewEncoder(w).Encode(RecordPreInvocation(payload))
	case string(ModePost), "post-tool":
		return json.NewEncoder(w).Encode(RecordPostBatch(payload, args))
	default:
		decision := evaluateGuardMode(payload, args, mode)
		if isClaude {
			return json.NewEncoder(w).Encode(FormatClaudeResponse(decision))
		}
		return json.NewEncoder(w).Encode(decision)
	}
}

func evaluateGuardMode(payload Payload, args ToolArgs, mode string) *Decision {
	switch mode {
	case "share":
		return CheckShare(payload.ToolCall.Name, args)
	case "schedule":
		return CheckSchedule(payload, args)
	case "batch":
		return CheckBatch(payload, args)
	case "search":
		return CheckSearch(payload.ToolCall.Name, args)
	case "commit":
		return CheckCommit(payload.ToolCall.Name, args)
	default:
		return evaluateAllGuards(payload, args)
	}
}

func evaluateAllGuards(payload Payload, args ToolArgs) *Decision {
	checks := []func() *Decision{
		func() *Decision { return CheckShare(payload.ToolCall.Name, args) },
		func() *Decision { return CheckSchedule(payload, args) },
		func() *Decision { return CheckBatch(payload, args) },
		func() *Decision { return CheckSearch(payload.ToolCall.Name, args) },
		func() *Decision { return CheckCommit(payload.ToolCall.Name, args) },
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
			if k == "CommandLine" {
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
