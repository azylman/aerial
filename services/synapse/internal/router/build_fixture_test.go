package router

import (
	"archive/zip"
	"bytes"
	"encoding/json"

	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
)

const (
	tRepo   = "example/aerial"
	tPath   = ".github/workflows/docker-publish.yml"
	tHead   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tBase   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	tMerge  = "cccccccccccccccccccccccccccccccccccccccc"
	tTree   = "dddddddddddddddddddddddddddddddddddddddd"
	tOther  = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	tDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	tRunID  = 77
)

// tRun is the authoritative main-branch producer run at an attempt.
func tRun(attempt int, status, conclusion string) metadataWorkflowRun {
	r := metadataWorkflowRun{ID: tRunID, RunNumber: 8, RunAttempt: attempt, Path: tPath, Event: buildEventPush, HeadSHA: tHead, HeadBranch: "main", Status: status, Conclusion: conclusion}
	r.Repository.FullName = tRepo
	return r
}

// tPRRun is the authoritative PR producer run for tHead.
func tPRRun(attempt int, status, conclusion string) metadataWorkflowRun {
	r := tRun(attempt, status, conclusion)
	r.Event, r.HeadBranch = ghEventPullRequest, "feature"
	return r
}

func tSelected(services ...string) []BuildSelection {
	out := make([]BuildSelection, 0, len(services))
	for _, svc := range services {
		out = append(out, BuildSelection{Service: svc, ImageName: "aerial-" + svc})
	}
	return out
}

// tMeta is a manifest identity for the main-branch run selecting services.
func tMeta(attempt int, services ...string) BuildMetadata {
	return BuildMetadata{Version: 1, Kind: buildMetadataManifest, Repository: tRepo, WorkflowPath: tPath, RunID: tRunID, RunNumber: 8, RunAttempt: attempt, Event: buildEventPush, SourceSHA: tHead, ExecutionSHA: tHead, Selected: tSelected(services...)}
}

func tPRMeta(attempt int, services ...string) BuildMetadata {
	m := tMeta(attempt, services...)
	pr := 42
	m.Event, m.ExecutionSHA, m.PRNumber = ghEventPullRequest, tMerge, &pr
	return m
}

func tImageResult(svc, state string, pushed bool, sourceAttempt int) BuildResult {
	r := BuildResult{Service: svc, ImageName: "aerial-" + svc, State: state, Pushed: pushed, SourceAttempt: sourceAttempt}
	if pushed {
		r.ImageRefs, r.Digest = []string{"ghcr.io/example/aerial-" + svc + ":tree-" + tTree}, tDigest
	}
	return r
}

// tResultOf turns an identity into a per-service result transport record.
func tResultOf(m BuildMetadata, svc, state string, pushed bool) BuildMetadata {
	r := tImageResult(svc, state, pushed, m.RunAttempt)
	m.Kind, m.Service, m.ImageName, m.State, m.Pushed, m.SourceAttempt, m.ImageRefs, m.Digest = buildMetadataResult, r.Service, r.ImageName, r.State, r.Pushed, r.SourceAttempt, r.ImageRefs, r.Digest
	return m
}

func tFinalOf(m BuildMetadata, results ...BuildResult) BuildMetadata {
	m.Kind, m.Results = buildMetadataFinal, append([]BuildResult{}, results...)
	return m
}

// tCheck publishes a record exactly as the producer does.
func tCheck(m BuildMetadata) GitHubMetadataCheck {
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	c := GitHubMetadataCheck{ID: 100 + int64(m.RunAttempt), Name: "build-metadata", HeadSHA: m.SourceSHA, ExternalID: buildmeta.MetadataExternalID(m), Status: ghStatusCompleted, Conclusion: "neutral"}
	if m.Kind == buildMetadataResult {
		c.Name = "build-metadata-result"
	}
	c.App.ID = buildmeta.BuildMetadataActionsAppID
	c.CheckSuite.ID = 999
	c.Output.Text = string(raw)
	return c
}

func checks(cs ...GitHubMetadataCheck) []GitHubMetadataCheck { return cs }

// tLegacyLogs is an archived attempt log whose checkout resolved revision.
func tLegacyLogs(revision string) []byte {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("build/1_checkout.txt")
	if err == nil {
		_, err = f.Write([]byte("t [command]/usr/bin/git log -1 --format=%H\nt " + revision + "\n"))
	}
	if err != nil || w.Close() != nil {
		panic("zip fixture")
	}
	return buf.Bytes()
}
