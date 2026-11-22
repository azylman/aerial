package model

import (
	"os"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

func TestParseBuildMetadata(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("../../testdata/build-metadata-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture := string(raw)
	// edit applies old/new replacement pairs to the valid manifest fixture.
	edit := func(pairs ...string) string {
		return strings.NewReplacer(pairs...).Replace(fixture)
	}
	const final = `"kind":"final"`
	tests := []struct {
		name  string
		raw   string
		valid bool
	}{
		{"fixture manifest", fixture, true},
		{"trailing", fixture + ` {}`, false},
		{"malformed", `{"version":`, false},
		{"scalar", `1`, false},
		{"oversize", fixture + strings.Repeat(" ", 65537), false},
		{"empty", "", false},
		{"fenced", "```json\n{}\n```", false},
		{"unknown version", edit(`"version":1`, `"version":2`), false},
		{"version alias", edit(`"version":1`, `"version":2,"Version":1`), false},
		{"identity alias", edit(`"repository":"owner/repo"`, `"repository":"other/repo","Repository":"owner/repo"`), false},
		{"nested alias", edit(`"selected":[]`, `"selected":[{"service":"engine","Service":"other","image_name":"engine"}]`), false},
		{"duplicate key", edit(`"version":1`, `"version":1,"version":1`), false},
		{"null selection", edit(`"selected":[]`, `"selected":null`), false},
		{"missing selection", edit(`,"selected":[]`, ``), false},
		{"wrong type", edit(`"version":1`, `"version":"one"`), false},
		{"unknown field", edit(`"version":1`, `"unknown":1,"version":1`), false},
		{"selection", edit(`"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}]`), true},
		{"duplicate service", edit(`"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"},{"service":"engine","image_name":"other"}]`), false},
		{"invalid service", edit(`"selected":[]`, `"selected":[{"service":"!","image_name":"engine"}]`), false},
		{"incomplete final selection", edit(`"kind":"manifest"`, final, `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}]`), false},
		{"incomplete final", edit(`"kind":"manifest"`, `"kind":"final","results":[{"service":"engine","image_name":"engine","state":"success","source_attempt":1}]`), false},
		{"unexpected result", edit(`"kind":"manifest"`, final, `"selected":[]`, `"selected":[],"results":[{"service":"engine","image_name":"engine","state":"failure","source_attempt":1}]`), false},
		{"success result", edit(`"kind":"manifest"`, final, `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"success","source_attempt":1,"pushed":true}]`), true},
		{"running result", edit(`"kind":"manifest"`, final, `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"running","source_attempt":1}]`), true},
		{"cancelled result", edit(`"kind":"manifest"`, final, `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"cancelled","source_attempt":1}]`), true},
		{"invalid state", edit(`"kind":"manifest"`, final, `"selected":[]`, `"selected":[{"service":"engine","image_name":"engine"}],"results":[{"service":"engine","image_name":"engine","state":"unknown","source_attempt":1}]`), false},
		{"empty final", edit(`"kind":"manifest"`, final), true},
		{"manual dispatch event", edit(`"event":"push"`, `"event":"workflow_dispatch"`), true},
		{"unsupported event", edit(`"event":"push"`, `"event":"schedule"`), false},
		{"progress wire kind", edit(`"kind":"manifest"`, `"kind":"progress"`), false},
		{"manifest with results", edit(`"kind":"manifest"`, `"kind":"manifest","results":[{"service":"engine","image_name":"engine","state":"success","source_attempt":1}]`), false},
		{"result with invalid transport", edit(`"kind":"manifest"`, `"kind":"result","service":"!","image_name":"engine","state":"success","source_attempt":1}`), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseBuildMetadata([]byte(tc.raw)); (err == nil) != tc.valid {
				t.Fatalf("ParseBuildMetadata() err = %v, want valid %v", err, tc.valid)
			}
		})
	}
}

func TestIsBuildMetadataCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		check string
		want  bool
	}{
		{"manifest check", "build-metadata", true},
		{"result check", "build-metadata-result", true},
		{"other check", "roborev", false},
		{"case differs", "Build-Metadata", false},
		{"empty", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsBuildMetadataCheck(tc.check); got != tc.want {
				t.Fatalf("IsBuildMetadataCheck(%q) = %v, want %v", tc.check, got, tc.want)
			}
		})
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
