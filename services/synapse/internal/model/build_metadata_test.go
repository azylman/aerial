package model

import (
	"os"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

func TestParseBuildMetadata(t *testing.T) {
	t.Parallel()
	fixture, err := os.ReadFile("../../testdata/build-metadata-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, raw string
		valid     bool
	}{
		{"trailing", string(fixture) + ` {}`, false},
		{"malformed", `{"version":`, false},
		{"scalar", `1`, false},
		{"incomplete final selection", `{"version":1,"kind":"final","repository":"owner/repo","workflow_path":".github/workflows/ci.yml","run_id":1,"run_number":1,"run_attempt":1,"event":"push","source_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","execution_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","pr_number":null,"selected":[{"service":"engine","image_name":"engine"}]}`, false},
		{"empty selection", `{"version":1,"kind":"manifest","repository":"owner/repo","workflow_path":".github/workflows/ci.yml","run_id":1,"run_number":1,"run_attempt":1,"event":"push","source_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","execution_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","pr_number":null,"selected":[]}`, true},
		{"oversize", string(fixture) + strings.Repeat(" ", 65537), false},
		{"empty", "", false},
		{"fenced", "```json\n{}\n```", false},
		{"unknown version", strings.Replace(string(fixture), `"version":1`, `"version":2`, 1), false},
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

func TestBuildMetadataValidationCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, replace, with string
		valid               bool
	}{
		{"version alias", `"version":1`, `"version":2,"Version":1`, false},
		{"identity alias", `"repository":"owner/repo"`, `"repository":"other/repo","Repository":"owner/repo"`, false},
		{"nested alias", `"selected":[]`, `"selected":[{"service":"engine","Service":"other","image_name":"engine"}]`, false},
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
		{"empty final", `"kind":"manifest"`, `"kind":"final"`, true},
		{"manual dispatch event", `"event":"push"`, `"event":"workflow_dispatch"`, true},
		{"unsupported event", `"event":"push"`, `"event":"schedule"`, false},
		{"progress wire kind", `"kind":"manifest"`, `"kind":"progress"`, false},
		{"manifest with results", `"kind":"manifest"`, `"kind":"manifest","results":[{"service":"engine","image_name":"engine","state":"success","source_attempt":1}]`, false},
		{"result with invalid transport", `"kind":"manifest"`, `"kind":"result","service":"!","image_name":"engine","state":"success","source_attempt":1}`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile("../../testdata/build-metadata-v1.json")
			if err != nil {
				t.Fatal(err)
			}
			if tc.replace == `"selected":[]` && strings.Contains(tc.with, `"results"`) {
				raw = []byte(strings.Replace(string(raw), `"kind":"manifest"`, `"kind":"final"`, 1))
			}
			_, err = ParseBuildMetadata([]byte(strings.Replace(string(raw), tc.replace, tc.with, 1)))
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestParseStoredBuildMetadata(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("../../testdata/build-metadata-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	progressRaw := strings.Replace(string(raw), `"kind":"manifest"`, `"kind":"progress"`, 1)

	// Direct parse rejects progress wire kind
	if _, parseErr := ParseBuildMetadata([]byte(progressRaw)); parseErr == nil {
		t.Fatal("expected ParseBuildMetadata to reject progress wire kind")
	}

	// ParseStoredBuildMetadata accepts progress snapshot
	m, err := ParseStoredBuildMetadata([]byte(progressRaw))
	if err != nil {
		t.Fatalf("unexpected error parsing stored progress metadata: %v", err)
	}
	if m.Kind != prwire.BuildMetadataProgress {
		t.Fatalf("got kind %q, want %q", m.Kind, prwire.BuildMetadataProgress)
	}
}

func TestValidWorkflowPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path string
		want bool
	}{
		{"valid yaml", ".github/workflows/ci.yml", true},
		{"valid deploy yaml", ".github/workflows/deploy.yaml", true},
		{"bad prefix", "workflows/ci.yml", false},
		{"subfolder rejection", ".github/workflows/sub/ci.yml", false},
		{"traversal rejection", ".github/workflows/../ci.yml", false},
		{"non yaml", ".github/workflows/ci.txt", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ValidWorkflowPath(tc.path); got != tc.want {
				t.Fatalf("ValidWorkflowPath(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

func TestValidBuildMetadataTransport(t *testing.T) {
	t.Parallel()
	mManifest := prwire.BuildMetadata{Kind: prwire.BuildMetadataManifest}
	if !validBuildMetadataTransport(mManifest) {
		t.Fatal("manifest should pass transport check")
	}

	mResultValid := prwire.BuildMetadata{
		Kind:          prwire.BuildMetadataResult,
		Service:       "engine",
		ImageName:     "engine",
		State:         ConclusionSuccess,
		SourceAttempt: 1,
		RunAttempt:    1,
		Pushed:        true,
	}
	if !validBuildMetadataTransport(mResultValid) {
		t.Fatal("valid result should pass transport check")
	}

	mResultBadState := mResultValid
	mResultBadState.State = "invalid"
	if validBuildMetadataTransport(mResultBadState) {
		t.Fatal("result with invalid state should fail transport check")
	}

	mResultPushedNotSuccess := mResultValid
	mResultPushedNotSuccess.State = BuildResultRunning
	mResultPushedNotSuccess.Pushed = true
	if validBuildMetadataTransport(mResultPushedNotSuccess) {
		t.Fatal("result pushed but not success should fail transport check")
	}

	mResultBadAttempt := mResultValid
	mResultBadAttempt.SourceAttempt = 2
	mResultBadAttempt.RunAttempt = 1
	if validBuildMetadataTransport(mResultBadAttempt) {
		t.Fatal("sourceAttempt > runAttempt should fail transport check")
	}

	mResultBadService := mResultValid
	mResultBadService.Service = "!"
	if validBuildMetadataTransport(mResultBadService) {
		t.Fatal("result with invalid service should fail transport check")
	}

	mResultBadImage := mResultValid
	mResultBadImage.ImageName = "!"
	if validBuildMetadataTransport(mResultBadImage) {
		t.Fatal("result with invalid image name should fail transport check")
	}

	mResultZeroAttempt := mResultValid
	mResultZeroAttempt.SourceAttempt = 0
	if validBuildMetadataTransport(mResultZeroAttempt) {
		t.Fatal("sourceAttempt 0 should fail transport check")
	}
}
