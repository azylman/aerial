package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestMetadataTransportTotals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		checks        map[string]string
		total, failed int
	}{
		{"neutral transport", map[string]string{"build-metadata": "passed", "build-metadata-result": "failed", "Unit Tests": "passed"}, 1, 0},
		{"ordinary failure", map[string]string{"build-metadata-like": "failed"}, 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, total, _, failed := analyzeBuildChecks(tc.checks)
			if total != tc.total || failed != tc.failed {
				t.Fatalf("total=%d failed=%d", total, failed)
			}
		})
	}
}

func TestParseBuildMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, raw string
		valid     bool
	}{
		{"trailing", `{} {}`, false},
		{"malformed", `{"version":`, false},
		{"scalar", `1`, false},
		{"incomplete final selection", `{"version":1,"kind":"final","repository":"owner/repo","workflow_path":".github/workflows/ci.yml","run_id":1,"run_number":1,"run_attempt":1,"event":"push","source_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","execution_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","pr_number":null,"selected":[{"service":"engine","image_name":"engine"}]}`, false},
		{"empty selection", `{"version":1,"kind":"manifest","repository":"owner/repo","workflow_path":".github/workflows/ci.yml","run_id":1,"run_number":1,"run_attempt":1,"event":"push","source_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","execution_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","pr_number":null,"selected":[]}`, true},
		{"missing selection", `{"version":1,"kind":"manifest"}`, false},
		{"oversize", strings.Repeat("x", 65537), false},
		{"empty", "", false},
		{"fenced", "```json\n{}\n```", false},
		{"unknown version", `{"version":2}`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseBuildMetadata([]byte(tc.raw))
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestLegacyWorkflowValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, path, sha string
		valid                 bool
	}{
		{"valid", "owner/repo", ".github/workflows/ci.yml", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
		{"bad repo", "repo", ".github/workflows/ci.yml", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"traversal", "owner/repo", ".github/workflows/../ci.yml", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"bad hash", "owner/repo", ".github/workflows/ci.yml", "main", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := normalizeLegacyBuildWorkflows([]LegacyBuildWorkflow{{Repository: tc.repo, WorkflowPath: tc.path, BlobSHA: tc.sha}})
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestBuildMetadataValidationCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, replace, with string
		valid               bool
	}{
		{"duplicate key", `"version":1`, `"version":1,"version":1`, false},
		{"null selection", `"selected":[]`, `"selected":null`, false},
		{"missing selection", `,"selected":[]`, ``, false},
		{"wrong type", `"version":1`, `"version":"one"`, false},
		{"unknown field", `"version":1`, `"unknown":1,"version":1`, false},
		{"selection", `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}]`, true},
		{"duplicate service", `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"},{"service":"engine","image_name":"other"}]`, false},
		{"invalid service", `"selected":[]`, `"selected":[{"service":"!","image_name":"engine"}]`, false},
		{"incomplete final", `"kind":"manifest"`, `"kind":"final","results":[{"service":"engine","image_name":"engine","state":"success","source_attempt":1}]`, false},
		{"unexpected result", `"selected":[]`, `"selected":[],"results":[{"service":"engine","image_name":"engine","state":"failure","source_attempt":1}]`, false},
		{"success result", `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"success","source_attempt":1,"pushed":true}]`, true},
		{"running result", `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"running","source_attempt":1}]`, true},
		{"cancelled result", `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"cancelled","source_attempt":1}]`, true},
		{"invalid state", `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"unknown","source_attempt":1}]`, false},
		{"invalid final count", `"kind":"manifest"`, `"kind":"final","selected":[{"service":"engine","image_name":"engine"}]`, false},
		{"empty final", `"kind":"manifest"`, `"kind":"final"`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile("testdata/build-metadata-v1.json")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(tc.with, `"results"`) && tc.valid {
				raw = []byte(strings.Replace(string(raw), `"kind":"manifest"`, `"kind":"final"`, 1))
			}
			_, err = ParseBuildMetadata([]byte(strings.Replace(string(raw), tc.replace, tc.with, 1)))
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestMetadataCheckRouting(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, conclusion string }{{"build-metadata", "failure"}, {"build-metadata-result", "cancelled"}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &RouterServer{}
			body := []byte(`{"action":"completed","repository":{"full_name":"owner/repo"},"check_run":{"name":"` + tc.name + `","status":"completed","conclusion":"` + tc.conclusion + `","head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pull_requests":[{"number":1}]}}`)
			res, err := s.ProcessGitHubEvent(context.Background(), "check_run", "delivery", body)
			if err != nil {
				t.Fatal(err)
			}
			if res.PRNumber != 0 {
				t.Fatal("transport check entered PR lifecycle routing")
			}
		})
	}
}
