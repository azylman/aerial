package guard

import (
	"encoding/json"
	"strings"
)

// DecisionType represents the hook authorization verdict.
type DecisionType string

const (
	DecisionAllow DecisionType = "allow"
	DecisionDeny  DecisionType = "deny"
)

// Decision is the JSON payload returned to Antigravity.
type Decision struct {
	Decision DecisionType `json:"decision"`
	Reason   string       `json:"reason,omitempty"`
}

// Payload is the root JSON structure received from Antigravity via stdin.
type Payload struct {
	ToolCall ToolCall `json:"toolCall"`
}

// ToolCall represents the intercepted tool invocation.
type ToolCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

// ToolArgs represents commonly inspected tool parameters.
type ToolArgs struct {
	TargetFile  string `json:"TargetFile"`
	CommandLine string `json:"CommandLine"`
	Cwd         string `json:"Cwd"`
}

// ParseArgs extracts ToolArgs from raw JSON message (handling both object and JSON-string representations).
func ParseArgs(raw json.RawMessage) (ToolArgs, error) {
	var args ToolArgs
	if len(raw) == 0 {
		return args, nil
	}

	// 1. Try direct object unmarshal
	if err := json.Unmarshal(raw, &args); err == nil && (args.TargetFile != "" || args.CommandLine != "" || args.Cwd != "") {
		return args, nil
	}

	// 2. Try string containing JSON
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		if err := json.Unmarshal([]byte(s), &args); err == nil {
			return args, nil
		}
	}

	// 3. Fallback: unmarshal into map[string]any for case-insensitive lookup
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err == nil {
		for k, v := range m {
			vs, ok := v.(string)
			if !ok {
				continue
			}
			switch strings.ToLower(k) {
			case "targetfile":
				args.TargetFile = vs
			case "commandline":
				args.CommandLine = vs
			case "cwd":
				args.Cwd = vs
			}
		}
	}

	return args, nil
}
