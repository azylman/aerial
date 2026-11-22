package guard

import (
	"bytes"
	"cmp"
	"encoding/json"
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/sanitizer"
)

// maxEgressJSONDepth bounds recursion into string leaves that themselves hold JSON.
const maxEgressJSONDepth = 8

// EgressScanRequest asks the eri-restate egress scan endpoint whether the
// arguments of an MCP call to Server/Tool carry a secret.
type EgressScanRequest struct {
	Server    string          `json:"server"`
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

// EgressViolation is one detected secret. Sample is always masked.
type EgressViolation struct {
	Category string `json:"category"`
	Sample   string `json:"sample"`
}

// EgressScanResult reports the scan outcome. Blocked is true only when the
// destination is blocklisted and at least one violation was found.
type EgressScanResult struct {
	Blocked    bool              `json:"blocked"`
	Violations []EgressViolation `json:"violations"`
}

// EvaluateEgress scans req.Arguments when blocked (the destination is on the
// egress blocklist) is true; non-blocklisted destinations are never scanned.
// Rules are the registered secret values plus patterns, minus the
// RFC1918_192_168 and InternalClusterDomain rules. Samples are masked.
func EvaluateEgress(req EgressScanRequest, blocked bool, patterns []PatternRule) EgressScanResult {
	result := EgressScanResult{Violations: []EgressViolation{}}
	if !blocked {
		return result
	}
	texts := egressScanTexts(req.Arguments)
	seen := make(map[EgressViolation]bool)
	add := func(v EgressViolation) {
		if !seen[v] {
			seen[v] = true
			result.Violations = append(result.Violations, v)
		}
	}
	for _, text := range texts {
		for _, sample := range sanitizer.MatchSensitiveTokens(text) {
			add(EgressViolation{Category: categoryRegisteredSecret, Sample: sample})
		}
		for _, p := range patterns {
			if p.Regex == nil || p.Category == categoryRFC1918 || p.Category == categoryInternalClusterDomain {
				continue
			}
			for _, hit := range p.Regex.FindAllString(text, -1) {
				add(EgressViolation{Category: p.Category, Sample: sanitizer.MaskTokenSample(hit)})
			}
		}
	}
	slices.SortFunc(result.Violations, func(a, b EgressViolation) int {
		return cmp.Or(cmp.Compare(a.Category, b.Category), cmp.Compare(a.Sample, b.Sample))
	})
	result.Blocked = len(result.Violations) > 0
	return result
}

// egressScanTexts returns the raw argument bytes followed by every decoded
// JSON string leaf and object key, recursing into string leaves that parse as JSON.
func egressScanTexts(raw json.RawMessage) []string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	texts := []string{string(trimmed)}
	var decoded any
	if err := json.Unmarshal(trimmed, &decoded); err != nil {
		return texts
	}
	return collectJSONStrings(decoded, texts, 0)
}

func collectJSONStrings(v any, out []string, depth int) []string {
	switch val := v.(type) {
	case string:
		out = append(out, val)
		if depth >= maxEgressJSONDepth {
			return out
		}
		inner := strings.TrimSpace(val)
		if inner == "" || (inner[0] != '{' && inner[0] != '[' && inner[0] != '"') {
			return out
		}
		var nested any
		if json.Unmarshal([]byte(inner), &nested) == nil {
			out = collectJSONStrings(nested, out, depth+1)
		}
	case map[string]any:
		for k, child := range val {
			out = append(out, k)
			out = collectJSONStrings(child, out, depth)
		}
	case []any:
		for _, child := range val {
			out = collectJSONStrings(child, out, depth)
		}
	}
	return out
}
