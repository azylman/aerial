package router

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
