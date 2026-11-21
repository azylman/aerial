package main

import (
	"encoding/json"
	"net/http"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/guard"
)

// GuardScanRequest represents the payload for POST /internal/guard/scan.
type GuardScanRequest struct {
	Diff                 string `json:"diff"`
	Text                 string `json:"text"`
	SourceLabel          string `json:"source_label"`
	CheckTestFiles       bool   `json:"check_test_files"`
	IgnoreInternalDomain bool   `json:"ignore_internal_domain"`
}

// GuardScanResponse represents the outcome of a guard scan request.
type GuardScanResponse struct {
	Decision   string            `json:"decision"`
	Reason     string            `json:"reason,omitempty"`
	Violations []guard.Violation `json:"violations,omitempty"`
}

func handleInternalGuardScan() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req GuardScanRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{jsonKeyError: "Invalid request payload"})
			return
		}

		var violations []guard.Violation
		patterns := guard.GetActivePatterns()

		if req.Diff != "" {
			diffPatterns := patterns
			if req.IgnoreInternalDomain {
				filtered := make([]guard.PatternRule, 0, len(patterns))
				for _, p := range patterns {
					if p.Category != "InternalClusterDomain" {
						filtered = append(filtered, p)
					}
				}
				diffPatterns = filtered
			}
			violations = append(violations, guard.ScanDiffForSecrets(req.Diff, req.CheckTestFiles, diffPatterns)...)
		}

		if req.Text != "" {
			violations = append(violations, guard.ScanTextForSecrets(req.Text, req.SourceLabel, patterns)...)
		}

		resp := GuardScanResponse{
			Decision: "allow",
		}
		if len(violations) > 0 {
			resp.Decision = "deny"
			resp.Reason = guard.FormatCommitDenyReason(violations)
			resp.Violations = violations
		}

		httpkit.WriteJSON(w, http.StatusOK, resp)
	}
}
