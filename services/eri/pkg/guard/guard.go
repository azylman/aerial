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

// Process reads an Antigravity PreToolUse hook payload from r, evaluates guards per mode,
// and writes the resulting Decision JSON to w.
// Guaranteed fail-open: on unexpected error or panic, it outputs {"decision": "allow"}.
func Process(r io.Reader, w io.Writer, mode string) (err error) {
	writeAllow := func() error {
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

	args := ParseArgs(payload.ToolCall.Args)

	switch mode {
	case string(ModeInv), "invocation":
		return json.NewEncoder(w).Encode(RecordPreInvocation(payload))
	case string(ModePost), "post-tool":
		return json.NewEncoder(w).Encode(RecordPostBatch(payload, args))
	default:
		decision := evaluateGuardMode(payload, args, mode)
		return json.NewEncoder(w).Encode(decision)
	}
}

func evaluateGuardMode(payload Payload, args ToolArgs, mode string) *Decision {
	switch mode {
	case "share":
		return CheckShare(payload.ToolCall.Name, args)
	case "schedule":
		return CheckSchedule(payload.ToolCall.Name, args)
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
		func() *Decision { return CheckSchedule(payload.ToolCall.Name, args) },
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
