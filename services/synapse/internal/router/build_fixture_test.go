package router

import (
	"archive/zip"
	"bytes"
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
