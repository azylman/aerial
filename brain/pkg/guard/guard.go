package guard

import (
	"bytes"
	"encoding/json"
	"io"
)

// Process reads an Antigravity PreToolUse hook payload from r, evaluates guards per mode,
// and writes the resulting Decision JSON to w.
// Guaranteed fail-open: on unexpected error or panic, it outputs {"decision": "allow"}.
func Process(r io.Reader, w io.Writer, mode string) (err error) {
	writeAllow := func() error {
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

	var decision *Decision
	switch mode {
	case "share":
		decision = CheckShare(payload.ToolCall.Name, args)
	case "schedule":
		decision = CheckSchedule(payload.ToolCall.Name, args)
	case "batch":
		decision = CheckBatch(payload, args)
	default:
		// "all" mode: evaluate share first, then schedule, then batch
		dShare := CheckShare(payload.ToolCall.Name, args)
		if dShare.Decision == DecisionDeny {
			decision = dShare
		} else {
			dSched := CheckSchedule(payload.ToolCall.Name, args)
			if dSched.Decision == DecisionDeny {
				decision = dSched
			} else {
				decision = CheckBatch(payload, args)
			}
		}
	}

	return json.NewEncoder(w).Encode(decision)
}
