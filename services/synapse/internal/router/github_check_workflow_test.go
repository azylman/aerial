package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var checkRunTestCases = []ghEventCase{
	{
		name: "check_run with pull_requests array", event: "check_run", delivery: "del-cr-1",
		payload: `{"action":"completed","check_run":{"id":111,"name":"Unit Tests","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		wantPR:  515, wantBranch: "feat/awesome", wantSource: "check_run_payload", wantConclusion: "success",
	},
	{
		name: "check_run fallback via commit SHA lookup", event: "check_run", delivery: "del-cr-2",
		payload: `{"action":"completed","check_run":{"id":222,"name":"Integration Tests","head_sha":"commit-fallback","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
		wantPR:  777, wantBranch: "feat/fallback", wantSource: "commit_sha_lookup", wantConclusion: "failure",
	},
	{
		name: "check_run malformed", event: "check_run", delivery: "del-cr-3",
		payload: `{invalid`, wantErr: true,
	},
	{
		name: "check_run empty head sha and no prs", event: "check_run", delivery: "del-cr-empty-sha",
		payload: `{"action":"completed","check_run":{"id":991,"name":"Lint","head_sha":"","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
	},
	{
		name: "check_run with no prs and registry resolve error", event: "check_run", delivery: "del-cr-reg-err",
		payload: `{"action":"completed","check_run":{"id":992,"name":"Build","head_sha":"unknown-sha","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{resolveErr: errors.New("reg err")},
	},
	{
		name: "check_run ci failure transition error", event: "check_run", delivery: "del-cr-fail-err",
		payload: `{"action":"completed","check_run":{"id":993,"name":"Test","head_sha":"headsha111","status":"completed","conclusion":"failure","pull_requests":[{"number":515,"head":{"ref":"feat/fail","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{ciFailedErr: errors.New("ci err")},
		wantErr: true,
	},
	{
		name: "check_run ci failure by sha transition error", event: "check_run", delivery: "del-cr-fail-sha-err",
		payload: `{"action":"completed","check_run":{"id":994,"name":"Test","head_sha":"headsha994","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{ciFailedBySHAErr: errors.New("sha err")},
		wantErr: true,
	},
	{
		name: "check_run completed success records passed check status and syncs widget", event: "check_run", delivery: "del-cr-ci-passed",
		payload: `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{recordCheckUpdated: true, resolveTargetID: "100000000000000001", resolveBranch: "feat/awesome"},
		disp:    &mockOutboundDispatcher{},
		wantPR:  515, wantRecordCheckCalls: 1, wantRecordCheckState: "passed",
	},
	{
		name: "check_run in_progress records pending check status", event: "check_run", delivery: "del-cr-ci-pending",
		payload: `{"action":"in_progress","check_run":{"name":"Unit Tests","head_sha":"headsha111","status":"in_progress","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{recordCheckUpdated: false},
		wantPR:  515, wantRecordCheckCalls: 1, wantRecordCheckState: "pending",
	},
	{
		name: "check_run failure does not record check status", event: "check_run", delivery: "del-cr-ci-failed-norecord",
		payload: `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"headsha111","status":"completed","conclusion":"failure","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedUpdated: false},
		wantPR:  515, wantCIFailedCalls: 1, wantRecordCheckCalls: 0,
	},
	{
		name: "check_run Container Builds Gate aggregator does not record check status", event: "check_run", delivery: "del-cr-gate-norecord",
		payload: `{"action":"completed","check_run":{"name":"Container Builds Gate","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{},
		wantPR:  515, wantRecordCheckCalls: 0,
	},
	{
		name: "check_run record check status error logs warning cleanly", event: "check_run", delivery: "del-cr-record-err",
		payload: `{"action":"completed","check_run":{"name":"Lint","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{recordCheckErr: errors.New("db record failure")},
		wantPR:  515, wantRecordCheckCalls: 1, wantRecordCheckState: "passed",
	},
	{
		name: "check_run failure with PR in payload transitions registry", event: "check_run", delivery: "del-cr-fail",
		payload: `{"action":"completed","check_run":{"name":"Tests","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
		wantPR:  518, wantCIFailedCalls: 1,
	},
	{
		name: "check_run failure for CI Gate is ignored", event: "check_run", delivery: "del-cr-ci-gate-ignore",
		payload: `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
		wantPR:  518, wantCIFailedCalls: 0,
	},
	{
		name: "check_run failure for CI Gate after cancelled upstream check transitions registry", event: "check_run", delivery: "del-cr-ci-gate-after-cancel",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-upstream-cancelled",
			payload:  `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"cancelhead777","status":"completed","conclusion":"cancelled","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"cancelhead777"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
		},
		payload:           `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"cancelhead777","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/mirrormere/actions/runs/777","output":{"title":"Gate Failed","summary":"Upstream job cancelled"},"pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"cancelhead777"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
		reg:               &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedUpdated: true},
		disp:              &mockOutboundDispatcher{},
		wantPR:            518,
		wantHeadSHA:       "cancelhead777",
		wantCIFailedCalls: 1,
	},
	{
		name: "check_run success for CI Gate recovers after prior check failure on same SHA", event: "check_run", delivery: "del-cr-ci-gate-recover-fail",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-unit-fail-before-rerun",
			payload:  `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"rerunhead888","status":"completed","conclusion":"failure","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunhead888"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		},
		payload:           `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"rerunhead888","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunhead888"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:               &mockPRRegistry{syncTargetID: "100000000000000001", ciFailedTargetID: "100000000000000001", ciFailedUpdated: true},
		wantPR:            530,
		wantHeadSHA:       "rerunhead888",
		wantTargetID:      "100000000000000001",
		wantSyncCalls:     1,
		wantConclusion:    "success",
		wantCIFailedCalls: 1,
		wantSyncEvents:    1,
	},
	{
		name: "check_run success for CI Gate recovers after prior cancelled check on same SHA", event: "check_run", delivery: "del-cr-ci-gate-recover-cancel",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-cancelled-before-rerun",
			payload:  `{"action":"completed","check_run":{"name":"roborev","head_sha":"cancelhead999","status":"completed","conclusion":"cancelled","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"cancelhead999"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		},
		payload:        `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"cancelhead999","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"cancelhead999"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:            &mockPRRegistry{syncTargetID: "100000000000000001"},
		wantPR:         530,
		wantHeadSHA:    "cancelhead999",
		wantTargetID:   "100000000000000001",
		wantSyncCalls:  1,
		wantConclusion: "success",
		wantSyncEvents: 1,
	},
	{
		name: "check_run success for CI Gate without prior failure ignores UpdatePRSync", event: "check_run", delivery: "del-cr-ci-gate-clean-success",
		payload:        `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"cleanhead111","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"cleanhead111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:            &mockPRRegistry{syncTargetID: "100000000000000001"},
		wantPR:         530,
		wantHeadSHA:    "cleanhead111",
		wantSyncCalls:  0,
		wantConclusion: "success",
	},
	{
		name: "check_run success on rerun for non-gate check recovers after prior failure on same SHA", event: "check_run", delivery: "del-cr-nongate-recover-fail",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-perf-fail-before-rerun",
			payload:  `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"rerunperf888","status":"completed","conclusion":"failure","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunperf888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
		},
		payload:              `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"rerunperf888","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunperf888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001", ciFailedTargetID: "100000000000000001", ciFailedUpdated: true},
		wantPR:               530,
		wantHeadSHA:          "rerunperf888",
		wantTargetID:         "100000000000000001",
		wantSyncCalls:        1,
		wantConclusion:       "success",
		wantCIFailedCalls:    1,
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
		wantSyncEvents:       1,
	},
	{
		name: "check_run success for unrelated non-gate check after another check failed on same SHA does not recover", event: "check_run", delivery: "del-cr-unrelated-nongate-no-recover",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-perf-fail-parallel",
			payload:  `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"parallelhead888","status":"completed","conclusion":"failure","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"parallelhead888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
		},
		payload:              `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"parallelhead888","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"parallelhead888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001", ciFailedTargetID: "100000000000000001", ciFailedUpdated: true},
		wantPR:               530,
		wantHeadSHA:          "parallelhead888",
		wantSyncCalls:        0,
		wantConclusion:       "success",
		wantCIFailedCalls:    1,
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
	},
	{
		name: "check_run failure for Container Builds Gate is ignored", event: "check_run", delivery: "del-cr-gate-ignore",
		payload: `{"action":"completed","check_run":{"name":"Container Builds Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
		wantPR:  518, wantCIFailedCalls: 0,
	},
	{
		name: "check_run failure for Sidecar Builds Gate is ignored", event: "check_run", delivery: "del-cr-sidecar-gate-ignore",
		payload: `{"action":"completed","check_run":{"name":"Sidecar Builds Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial-sidecars"}}`,
		reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
		wantPR:  518, wantCIFailedCalls: 0,
	},
	{
		name: "check_run failure for Configuration Gate is ignored", event: "check_run", delivery: "del-cr-config-gate-ignore",
		payload: `{"action":"completed","check_run":{"name":"Configuration Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial-config"}}`,
		reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
		wantPR:  518, wantCIFailedCalls: 0,
	},
	{
		name: "check_run failure for superseded commit SHA is ignored", event: "check_run", delivery: "del-cr-stale-sha",
		payload: `{"action":"completed","check_run":{"name":"Run Service Unit Tests","head_sha":"stalehead111","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"stalehead111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{resolveHeadSHA: "newerhead999", ciFailedTargetID: "target-111", ciFailedUpdated: true},
		wantPR:  518, wantCIFailedCalls: 0,
	},
	{
		name: "check_run success for roborev calls UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-success",
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Succeeded","summary":"All clean"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001"},
		wantPR:               525,
		wantHeadSHA:          "robo525abcdef",
		wantTargetID:         "100000000000000001",
		wantSyncCalls:        1,
		wantConclusion:       "success",
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
	},
	{
		name: "check_run success for roborev recovers after workflow_run failure", event: "check_run", delivery: "del-cr-roborev-recover",
		priorEvent: &ghPriorEvent{
			event:    "workflow_run",
			delivery: "del-wf-cd-fail",
			payload:  `{"action":"completed","workflow_run":{"id":525,"name":"Continuous Delivery","head_sha":"robo525abcdef","head_branch":"feat/test","status":"completed","conclusion":"failure","pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		},
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Succeeded","summary":"All clean"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001", ciFailedTargetID: "100000000000000001", ciFailedUpdated: false},
		wantPR:               525,
		wantHeadSHA:          "robo525abcdef",
		wantTargetID:         "100000000000000001",
		wantSyncCalls:        1,
		wantConclusion:       "success",
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
		wantCIFailedCalls:    1,
	},
	{
		name: "check_run success for Check RoboRev Bootstrap State ignores UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-bootstrap-success",
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Check RoboRev Bootstrap State","head_sha":"robo525abcdef","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Bootstrap OK","summary":"All clean"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001"},
		wantPR:               525,
		wantHeadSHA:          "robo525abcdef",
		wantSyncCalls:        0,
		wantConclusion:       "success",
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
	},
	{
		name: "check_run success for roborev without prior roborev failure ignores UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-firstpass-success",
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"headsha789","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/530","output":{"title":"Review Succeeded","summary":"All clean"},"pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"headsha789"}}]}}`,
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001"},
		wantPR:               530,
		wantHeadSHA:          "headsha789",
		wantSyncCalls:        0,
		wantConclusion:       "success",
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
	},
	{
		name: "check_run success for roborev blocked by non-roborev failure ignores UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-blocked",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-unit-fail",
			payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Unit Tests","head_sha":"robo525abcdef","status":"completed","conclusion":"failure","pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
		},
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Succeeded","summary":"All clean"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001", ciFailedTargetID: "100000000000000001"},
		wantPR:               525,
		wantHeadSHA:          "robo525abcdef",
		wantSyncCalls:        0,
		wantConclusion:       "success",
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
		wantCIFailedCalls:    1,
	},
	{
		name: "check_run timed_out with PR in payload transitions registry", event: "check_run", delivery: "del-cr-timeout",
		payload: `{"action":"completed","check_run":{"name":"Tests","head_sha":"checkhead555","status":"completed","conclusion":"timed_out","pull_requests":[{"number":519,"head":{"ref":"feat/test","sha":"checkhead555"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
		wantPR:  519, wantCIFailedCalls: 1,
	},
	{
		name: "check_run failure without PR falls back to headSHA transition", event: "check_run", delivery: "del-cr-fallback",
		payload:     `{"action":"completed","check_run":{"name":"Lint","head_sha":"orphansha666","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:         &mockPRRegistry{ciFailedBySHAPRNum: 520, ciFailedBySHATarget: "target-222", ciFailedBySHAUpdated: true},
		wantHeadSHA: "orphansha666", wantCIFailedSHAs: 1,
	},
	{
		name: "check_run local PR resolution priority avoids GitHub API", event: "check_run", delivery: "del-cr-local",
		payload:              `{"action":"completed","check_run":{"name":"Integration Tests","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
		reg:                  &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
		spyNoGitHubHit:       true,
		wantPR:               530,
		wantTargetID:         "thread-local-530",
		wantSource:           "local_pr_registry",
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
	},
	{
		name: "check_run failure duplicate does not trigger prompt", event: "check_run", delivery: "del-cr-dup-prompt",
		payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Build Failed","summary":"Compilation error in brain/main.go:42"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
		reg:               &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedUpdated: false},
		disp:              &mockOutboundDispatcher{},
		wantCIFailedCalls: 1, wantPrompts: 0,
	},
	{
		name: "check_run failure on merge_sha is ignored", event: "check_run", delivery: "del-cr-main-merge-sha-fail",
		payload: `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Integration Tests","head_sha":"merge224","status":"completed","conclusion":"failure","pull_requests":[]}}`,
		reg:     &mockPRRegistry{resolvePRNum: 224, resolveBranch: "feat/restate", resolveHeadSHA: "premerge224", resolveMergeSHA: "merge224", resolveTargetID: "100000000000000002"},
		disp:    &mockOutboundDispatcher{}, wantPR: 224, wantHeadSHA: "merge224",
	},
	{
		name: "roborev failure followed by lint failure on same head_sha emits both events", event: "check_run", delivery: "del-cr-lint-fail-after-roborev",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-roborev-fail-multi",
			payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"multihead1","status":"completed","conclusion":"failure","pull_requests":[{"number":601,"head":{"ref":"feat/m1","sha":"multihead1"}}]}}`,
		},
		payload:            `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead1","status":"completed","conclusion":"failure","pull_requests":[{"number":601,"head":{"ref":"feat/m1","sha":"multihead1"}}]}}`,
		reg:                &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedResults: []bool{true, false}},
		wantPR:             601,
		wantHeadSHA:        "multihead1",
		wantCIFailedCalls:  2,
		wantCIFailedChecks: []string{"roborev", "Lint"},
	},
	{
		name: "duplicate failure delivery for same check emits 0 additional events", event: "check_run", delivery: "del-cr-lint-dup-fail",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-lint-first-fail",
			payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead2","status":"completed","conclusion":"failure","pull_requests":[{"number":602,"head":{"ref":"feat/m2","sha":"multihead2"}}]}}`,
		},
		payload:            `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead2","status":"completed","conclusion":"failure","pull_requests":[{"number":602,"head":{"ref":"feat/m2","sha":"multihead2"}}]}}`,
		reg:                &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedResults: []bool{true, false}},
		wantPR:             602,
		wantHeadSHA:        "multihead2",
		wantCIFailedCalls:  2,
		wantCIFailedChecks: []string{"Lint"},
	},
	{
		name: "same-commit rerun failure after in_progress emits ci_failed again", event: "check_run", delivery: "del-cr-lint-rerun-fail",
		priorEvents: []ghPriorEvent{
			{
				event:    "check_run",
				delivery: "del-cr-lint-run1-fail",
				payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead3","status":"completed","conclusion":"failure","pull_requests":[{"number":603,"head":{"ref":"feat/m3","sha":"multihead3"}}]}}`,
			},
			{
				event:    "check_run",
				delivery: "del-cr-lint-run2-progress",
				payload:  `{"action":"in_progress","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead3","status":"in_progress","pull_requests":[{"number":603,"head":{"ref":"feat/m3","sha":"multihead3"}}]}}`,
			},
		},
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead3","status":"completed","conclusion":"failure","pull_requests":[{"number":603,"head":{"ref":"feat/m3","sha":"multihead3"}}]}}`,
		reg:                  &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedResults: []bool{true, false}},
		wantPR:               603,
		wantHeadSHA:          "multihead3",
		wantCIFailedCalls:    2,
		wantCIFailedChecks:   []string{"Lint", "Lint"},
		wantRecordCheckCalls: 1,
	},
	{
		name: "multi-check recovery roborev succeeds first then lint succeeds recovers and calls UpdatePRSync", event: "check_run", delivery: "del-cr-lint-succ-after-roborev-succ",
		priorEvents: []ghPriorEvent{
			{
				event:    "check_run",
				delivery: "del-cr-roborev-fail-4a",
				payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"multihead4a","status":"completed","conclusion":"failure","pull_requests":[{"number":604,"head":{"ref":"feat/m4a","sha":"multihead4a"}}]}}`,
			},
			{
				event:    "check_run",
				delivery: "del-cr-lint-fail-4a",
				payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead4a","status":"completed","conclusion":"failure","pull_requests":[{"number":604,"head":{"ref":"feat/m4a","sha":"multihead4a"}}]}}`,
			},
			{
				event:    "check_run",
				delivery: "del-cr-roborev-succ-4a",
				payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"multihead4a","status":"completed","conclusion":"success","pull_requests":[{"number":604,"head":{"ref":"feat/m4a","sha":"multihead4a"}}]}}`,
			},
		},
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead4a","status":"completed","conclusion":"success","pull_requests":[{"number":604,"head":{"ref":"feat/m4a","sha":"multihead4a"}}]}}`,
		reg:                  &mockPRRegistry{ciFailedTargetID: "100000000000000001", syncTargetID: "100000000000000001", ciFailedResults: []bool{true, false}},
		wantPR:               604,
		wantHeadSHA:          "multihead4a",
		wantTargetID:         "100000000000000001",
		wantCIFailedCalls:    2,
		wantSyncCalls:        1,
		wantRecordCheckCalls: 2,
	},
	{
		name: "multi-check recovery lint succeeds first then roborev succeeds recovers and calls UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-succ-after-lint-succ",
		priorEvents: []ghPriorEvent{
			{
				event:    "check_run",
				delivery: "del-cr-roborev-fail-4b",
				payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"multihead4b","status":"completed","conclusion":"failure","pull_requests":[{"number":605,"head":{"ref":"feat/m4b","sha":"multihead4b"}}]}}`,
			},
			{
				event:    "check_run",
				delivery: "del-cr-lint-fail-4b",
				payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead4b","status":"completed","conclusion":"failure","pull_requests":[{"number":605,"head":{"ref":"feat/m4b","sha":"multihead4b"}}]}}`,
			},
			{
				event:    "check_run",
				delivery: "del-cr-lint-succ-4b",
				payload:  `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"multihead4b","status":"completed","conclusion":"success","pull_requests":[{"number":605,"head":{"ref":"feat/m4b","sha":"multihead4b"}}]}}`,
			},
		},
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"multihead4b","status":"completed","conclusion":"success","pull_requests":[{"number":605,"head":{"ref":"feat/m4b","sha":"multihead4b"}}]}}`,
		reg:                  &mockPRRegistry{ciFailedTargetID: "100000000000000001", syncTargetID: "100000000000000001", ciFailedResults: []bool{true, false}},
		wantPR:               605,
		wantHeadSHA:          "multihead4b",
		wantTargetID:         "100000000000000001",
		wantCIFailedCalls:    2,
		wantSyncCalls:        1,
		wantRecordCheckCalls: 2,
	},
	{
		name: "check_run failure on dependabot branch on non-default owner skips self-heal registration", event: "check_run", delivery: "del-cr-dependabot-fail-non-default-owner",
		payload:         `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"depfailsha2","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/1000","output":{"title":"Tests Failed","summary":"Error in qux.go"},"pull_requests":[{"number":610,"head":{"ref":"dependabot/pip/qux-1.0","sha":"depfailsha2"}}]}}`,
		systemChannelID: "100000000000000004", nonDefaultOwner: true, reg: &mockPRRegistry{ciFailedTargetID: "100000000000000004", ciFailedUpdated: true}, disp: &mockOutboundDispatcher{},
		wantPR: 610, wantBranch: "dependabot/pip/qux-1.0", wantHeadSHA: "depfailsha2", wantEnsureCalls: 0, wantCIFailedCalls: 1, wantTargetID: "100000000000000004",
	},
	{
		name: "check_run failure on dependabot branch self-heals registration and transitions CI failed", event: "check_run", delivery: "del-cr-dependabot-fail",
		payload:         `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"depfailsha","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/999","output":{"title":"Tests Failed","summary":"Error in bar.go"},"pull_requests":[{"number":607,"head":{"ref":"dependabot/pip/foo-1.0","sha":"depfailsha"}}]}}`,
		systemChannelID: "100000000000000004", reg: &mockPRRegistry{ciFailedTargetID: "100000000000000004", ciFailedUpdated: true}, disp: &mockOutboundDispatcher{},
		wantPR: 607, wantBranch: "dependabot/pip/foo-1.0", wantHeadSHA: "depfailsha", wantEnsureCalls: 1, wantCIFailedCalls: 1, wantTargetID: "100000000000000004",
	},
	{
		name: "check_run success on dependabot branch skips recording check status and widget sync", event: "check_run", delivery: "del-cr-dependabot-pass",
		payload: `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"deppasssha","status":"completed","conclusion":"success","pull_requests":[{"number":608,"head":{"ref":"dependabot/pip/bar-1.0","sha":"deppasssha"}}]}}`,
		reg:     &mockPRRegistry{recordCheckUpdated: true}, disp: &mockOutboundDispatcher{},
		wantPR: 608, wantBranch: "dependabot/pip/bar-1.0", wantHeadSHA: "deppasssha", wantRecordCheckCalls: 0,
	},
	{
		name: "check_run failure cascades open pr conflicts and updates next clean pr", event: "check_run", delivery: "del-cr-cascade-update",
		payload:           `{"action":"completed","repository":{"full_name":"azylman/mirrormere"},"check_run":{"name":"Service Unit Tests","head_sha":"failhead891","status":"completed","conclusion":"failure","pull_requests":[{"number":891,"head":{"ref":"feat/fail-pr","sha":"failhead891"}}]}}`,
		autoUpdateAuthors: []string{"trusted-author"}, reg: &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedUpdated: true}, disp: &mockOutboundDispatcher{},
		wantPR: 891, wantHeadSHA: "failhead891", wantCIFailedCalls: 1, wantUpdatedBranchHit: "/repos/azylman/mirrormere/pulls/894/update-branch",
	},
	{
		name: "check_run gate aggregator success triggers open pr conflicts check and branch update", event: "check_run", delivery: "del-cr-gate-success-update",
		payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"CI Gate","head_sha":"head543","status":"completed","conclusion":"success","pull_requests":[{"number":543,"head":{"ref":"feat/conflict-check","sha":"head543"}}]}}`,
		autoUpdateAuthors: []string{"trusted-author"}, wantPR: 543, wantHeadSHA: "head543", wantUpdatedBranchHit: "/repos/azylman/aerial/pulls/543/update-branch",
	},
	{
		name: "check_run gate aggregator success handles conflict check error cleanly", event: "check_run", delivery: "del-cr-gate-success-err",
		payload:           `{"action":"completed","repository":{"full_name":"azylman/unknown-repo"},"check_run":{"name":"CI Gate","head_sha":"head999","status":"completed","conclusion":"success","pull_requests":[{"number":999,"head":{"ref":"feat/unknown","sha":"head999"}}]}}`,
		autoUpdateAuthors: []string{"trusted-author"}, wantPR: 999, wantHeadSHA: "head999", wantErr: false,
	},
	{
		name: "check_run roborev success triggers open pr conflicts check and branch update", event: "check_run", delivery: "del-cr-roborev-success-update",
		payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"head543","status":"completed","conclusion":"success","pull_requests":[{"number":543,"head":{"ref":"feat/conflict-check","sha":"head543"}}]}}`,
		autoUpdateAuthors: []string{"trusted-author"}, wantPR: 543, wantHeadSHA: "head543", wantUpdatedBranchHit: "/repos/azylman/aerial/pulls/543/update-branch",
	},
	{
		name: "check_run recovered check success triggers open pr conflicts check and branch update", event: "check_run", delivery: "del-cr-recovered-success-update",
		priorEvent: &ghPriorEvent{
			event:    "check_run",
			delivery: "del-cr-unit-fail-before-update",
			payload:  `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"head543","status":"completed","conclusion":"failure","pull_requests":[{"number":543,"head":{"ref":"feat/conflict-check","sha":"head543"}}]},"repository":{"full_name":"azylman/aerial"}}`,
		},
		payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Unit Tests","head_sha":"head543","status":"completed","conclusion":"success","pull_requests":[{"number":543,"head":{"ref":"feat/conflict-check","sha":"head543"}}]}}`,
		autoUpdateAuthors:    []string{"trusted-author"},
		reg:                  &mockPRRegistry{syncTargetID: "100000000000000001", ciFailedTargetID: "100000000000000001", ciFailedUpdated: true},
		wantPR:               543,
		wantHeadSHA:          "head543",
		wantSyncCalls:        1,
		wantSyncEvents:       1,
		wantCIFailedCalls:    1,
		wantConflictCalls:    1,
		wantRecordCheckCalls: 1,
		wantRecordCheckState: "passed",
		wantUpdatedBranchHit: "/repos/azylman/aerial/pulls/543/update-branch",
	},
	{
		name:     "check_run on fork PR resolves PR and branch from cached pull_request synchronize event",
		event:    "check_run",
		delivery: "del-cr-fork-pr-cache",
		priorEvent: &ghPriorEvent{
			event:    "pull_request",
			delivery: "del-pr-fork-sync",
			payload:  `{"action":"synchronize","number":711,"pull_request":{"number":711,"head":{"ref":"fork/patch-1","sha":"forksha711"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"contributor"}}`,
		},
		payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"forksha711","status":"completed","conclusion":"failure","pull_requests":[]}}`,
		reg:               &mockPRRegistry{ciFailedTargetID: "100000000000000001", ciFailedUpdated: true},
		disp:              &mockOutboundDispatcher{},
		wantPR:            711,
		wantBranch:        "fork/patch-1",
		wantHeadSHA:       "forksha711",
		wantSyncCalls:     1,
		wantCIFailedCalls: 1,
	},
}

func TestProcessGitHubEvent_CheckRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tc := range checkRunTestCases {
		t.Run(tc.name, func(t *testing.T) {
			runProcessGitHubEventCase(ctx, t, tc)
		})
	}
}

func TestProcessGitHubEvent_WorkflowRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []ghEventCase{
		{
			name: "workflow_run with PR array", event: "workflow_run", delivery: "del-wf-1",
			payload: `{"action":"completed","workflow_run":{"id":333,"name":"Continuous Delivery","head_sha":"headsha333","head_branch":"feat/cd","status":"completed","conclusion":"success","pull_requests":[{"number":516,"head":{"ref":"feat/cd","sha":"headsha333"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  516, wantBranch: "feat/cd", wantSource: "workflow_run_payload",
		},
		{
			name: "workflow_run fallback to commit lookup", event: "workflow_run", delivery: "del-wf-2",
			payload: `{"action":"completed","workflow_run":{"id":444,"name":"Continuous Delivery","head_sha":"wf-fallback","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  888, wantBranch: "feat/wf-branch", wantSource: "commit_sha_lookup",
		},
		{
			name: "workflow_run malformed", event: "workflow_run", delivery: "del-wf-3",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "workflow_run failure with PR transitions registry", event: "workflow_run", delivery: "del-wf-fail",
			payload: `{"action":"completed","workflow_run":{"name":"CI","head_sha":"wfhead777","status":"completed","conclusion":"failure","pull_requests":[{"number":521,"head":{"ref":"feat/wf","sha":"wfhead777"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  521, wantCIFailedCalls: 1,
		},
		{
			name: "workflow_run timed_out without PR falls back to headSHA transition", event: "workflow_run", delivery: "del-wf-timeout",
			payload:     `{"action":"completed","workflow_run":{"name":"Build Docker","head_sha":"wfhead888","status":"completed","conclusion":"timed_out","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:         &mockPRRegistry{ciFailedBySHAPRNum: 520, ciFailedBySHATarget: "target-222", ciFailedBySHAUpdated: true},
			wantHeadSHA: "wfhead888", wantCIFailedSHAs: 1,
		},
		{
			name: "workflow_run local PR resolution priority avoids GitHub API", event: "workflow_run", delivery: "del-wf-local",
			payload:        `{"action":"completed","workflow_run":{"name":"Build Pipeline","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:            &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantSource: "local_pr_registry",
		},
		{
			name: "workflow_run on main with expected deploy jobs defers prompt until deploy_success", event: "workflow_run", delivery: "del-wf-mirrormere-expected",
			payload: `{"action":"completed","workflow_run":{"id":800,"run_attempt":1,"name":"Continuous Delivery","head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 222, resolveBranch: "feat/ui-deploy", resolveTargetID: "100000000000000002", hasExpectedRes: true},
			disp:    &mockOutboundDispatcher{},
			wantPR:  222, wantPrompts: 0,
		},
		{
			name: "workflow_run on main kustomization discovery error propagates", event: "workflow_run", delivery: "del-wf-aerial-config-discover-err",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge223","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial-config"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 223, resolveBranch: "feat/config-only", resolveTargetID: "100000000000000002", hasExpectedRes: false},
			disp:    &mockOutboundDispatcher{repoDiscoverErr: errors.New("kube api down")},
			wantErr: true,
		},
		{
			name: "workflow_run on main completeNoDeployWorkflowRun HasExpectedDeployJobs error propagates", event: "workflow_run", delivery: "del-wf-mirrormere-has-expected-err",
			payload: `{"action":"completed","workflow_run":{"id":800,"run_attempt":1,"name":"Continuous Delivery","head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 221, resolveBranch: "feat/ui-no-deploy", resolveTargetID: "100000000000000002", hasExpectedErr: errors.New("simulated has expected deploy jobs error")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
		{
			name: "workflow_run failure on main run_attempt 1 reruns failed jobs and suppresses deploy_failed", event: "workflow_run", delivery: "del-wf-main-merge-sha-rerun",
			payload: `{"action":"completed","workflow_run":{"id":801,"run_attempt":1,"name":"Continuous Delivery","head_sha":"merge224","head_branch":"main","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 224, resolveBranch: "feat/restate", resolveHeadSHA: "premerge224", resolveMergeSHA: "merge224", resolveTargetID: "100000000000000002"},
			disp:    &mockOutboundDispatcher{}, bus: &mockEventBus{}, wantPR: 224, wantHeadSHA: "merge224", wantTargetID: "100000000000000002",
			wantRerunHit: "/repos/azylman/aerial/actions/runs/801/rerun-failed-jobs",
		},
		{
			name: "workflow_run failure on main matching merge_sha transitions merged PR to deploy_failed", event: "workflow_run", delivery: "del-wf-main-merge-sha-fail",
			payload: `{"action":"completed","workflow_run":{"id":801,"run_attempt":2,"name":"Continuous Delivery","head_sha":"merge224","head_branch":"main","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 224, resolveBranch: "feat/restate", resolveHeadSHA: "premerge224", resolveMergeSHA: "merge224", resolveTargetID: "100000000000000002", deployFailedTargetID: "100000000000000002", deployFailedUpdated: true},
			disp:    &mockOutboundDispatcher{}, bus: &mockEventBus{}, wantPR: 224, wantHeadSHA: "merge224", wantTargetID: "100000000000000002", wantDeployFailedCalls: 1, wantDeployFailedEvents: 1, wantDeployEventMergeSHA: "merge224",
		},
		{
			name: "workflow_run failure on main matching merge_sha deploy_failed transition error propagates", event: "workflow_run", delivery: "del-wf-main-merge-sha-deploy-err",
			payload: `{"action":"completed","workflow_run":{"id":801,"run_attempt":2,"name":"Continuous Delivery","head_sha":"merge224","head_branch":"main","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 224, resolveBranch: "feat/restate", resolveHeadSHA: "premerge224", resolveMergeSHA: "merge224", resolveTargetID: "100000000000000002", deployFailedErr: errors.New("deploy failed db error")},
			disp:    &mockOutboundDispatcher{}, wantErr: true,
		},
		{
			name: "workflow_run failure on main run_attempt 1 rerun error falls back to deploy_failed", event: "workflow_run", delivery: "del-wf-main-merge-sha-rerun-err",
			payload: `{"action":"completed","workflow_run":{"id":802,"run_attempt":1,"name":"Continuous Delivery","head_sha":"merge224","head_branch":"main","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 224, resolveBranch: "feat/restate", resolveHeadSHA: "premerge224", resolveMergeSHA: "merge224", resolveTargetID: "100000000000000002", deployFailedTargetID: "100000000000000002", deployFailedUpdated: true},
			disp:    &mockOutboundDispatcher{}, bus: &mockEventBus{}, wantPR: 224, wantHeadSHA: "merge224", wantTargetID: "100000000000000002", wantDeployFailedCalls: 1, wantDeployFailedEvents: 1, wantDeployEventMergeSHA: "merge224",
			wantRerunHit: "/repos/azylman/aerial/actions/runs/802/rerun-failed-jobs",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runProcessGitHubEventCase(ctx, t, tc)
		})
	}
}

type dispatchWorkflowRunImagesCase struct {
	name                   string
	isFinalAttempt         bool
	targetID               string
	resolveErr             error
	deployFailedTID        string
	deployFailedErr        error
	deployFailedNotUpdated bool
	promoteErr             error
	recordErr              error
	reconcileErr           error
	hasKustReqs            bool
	nilReg                 bool
	prNum                  int
	wantErr                bool
	wantDeployFailed       int
	wantDeployEvents       int
	wantTargetID           string
}

func runDispatchWorkflowRunImagesCase(ctx context.Context, t *testing.T, tc dispatchWorkflowRunImagesCase) {
	t.Helper()
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/actions/runs/") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"total_count": 1,
				"jobs": [
					{
						"id": 1,
						"name": "Build & Push Container Images (eri)",
						"status": "completed",
						"conclusion": "success"
					}
				]
			}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockServer.Close()

	bus := &mockEventBus{}
	var reg PRRegistryUpdater
	var mockReg *mockPRRegistry
	if !tc.nilReg {
		mockReg = &mockPRRegistry{
			resolveTargetID:         tc.targetID,
			resolveBranch:           "feat/test",
			resolveHeadSHA:          "sha-reg-123",
			resolveErr:              tc.resolveErr,
			deployFailedTargetID:    tc.deployFailedTID,
			deployFailedUpdated:     !tc.deployFailedNotUpdated && tc.deployFailedErr == nil,
			deployFailedErr:         tc.deployFailedErr,
			kustomizationRequestErr: tc.recordErr,
			hasKustomizationReqs:    tc.hasKustReqs,
		}
		reg = mockReg
	}
	reconcileErrs := make(map[string]error)
	if tc.reconcileErr != nil {
		reconcileErrs["aerial-core"] = tc.reconcileErr
	}
	disp := &mockOutboundDispatcher{
		kustomizationsByImage: map[string][]fluxKustomizationRef{
			"aerial-eri": {{Namespace: "flux-system", Name: "aerial-core"}},
		},
		promoteErr:    tc.promoteErr,
		reconcileErrs: reconcileErrs,
	}
	cfg := Config{
		GitHubAPIURL:    mockServer.URL,
		DefaultOwner:    true,
		SystemChannelID: "100000000000000001",
	}
	srv := newTestRouter(legacyFixtureConfig(cfg), legacyFixtureClient(mockServer.Client()), reg, disp)
	srv.bus = bus

	testCtx := ctx
	if tc.isFinalAttempt {
		testCtx = withFinalAttempt(ctx, true)
	}
	err := srv.dispatchWorkflowRunImages(testCtx, "azylman/aerial", workflowRunEvent{ID: 1, Attempt: 1, HeadSHA: legacyFixtureHead}, "main", "success", tc.targetID, tc.prNum)
	if (err != nil) != tc.wantErr {
		t.Fatalf("dispatchWorkflowRunImages() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if mockReg != nil && len(mockReg.deployFailedCalls) != tc.wantDeployFailed {
		t.Errorf("len(deployFailedCalls) = %d, want %d", len(mockReg.deployFailedCalls), tc.wantDeployFailed)
	}
	deployEventsCount := 0
	for _, ev := range bus.publishedPREvents {
		if ev.Event == "deploy_failed" {
			deployEventsCount++
			if ev.Repo != "azylman/aerial" || ev.PRNumber != 42 || ev.Status != "deploy_failed" {
				t.Errorf("unexpected deploy_failed event: %+v", ev)
			}
			if tc.wantTargetID != "" && ev.TargetID != tc.wantTargetID {
				t.Errorf("event.TargetID = %q, want %q", ev.TargetID, tc.wantTargetID)
			}
		}
	}
	if deployEventsCount != tc.wantDeployEvents {
		t.Errorf("deploy_failed events count = %d, want %d", deployEventsCount, tc.wantDeployEvents)
	}
}

func TestDispatchWorkflowRunImages_ImagePromotionDeployFailed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []dispatchWorkflowRunImagesCase{
		{
			name:             "transient promote error leaves PR deploying",
			isFinalAttempt:   false,
			targetID:         "100000000000000001",
			promoteErr:       errors.New("transient error"),
			prNum:            42,
			wantErr:          true,
			wantDeployFailed: 0,
			wantDeployEvents: 0,
		},
		{
			name:           "terminal promote error transitions to deploy_failed",
			isFinalAttempt: true, targetID: "100000000000000001", promoteErr: errors.New("terminal error"), prNum: 42,
			wantErr: true, wantDeployFailed: 1, wantDeployEvents: 1, wantTargetID: "100000000000000001",
		},
		{
			name:           "terminal promote error with empty targetID adopts transition targetID",
			isFinalAttempt: true, deployFailedTID: "100000000000000002", promoteErr: errors.New("terminal error"), prNum: 42,
			wantErr: true, wantDeployFailed: 1, wantDeployEvents: 1, wantTargetID: "100000000000000002",
		},
		{
			name:           "terminal promote error with resolve error falls back to system channel",
			isFinalAttempt: true, resolveErr: errors.New("db resolve error"), promoteErr: errors.New("terminal error"), prNum: 42,
			wantErr: true, wantDeployFailed: 1, wantDeployEvents: 1, wantTargetID: "100000000000000001",
		},
		{
			name:           "terminal promote error with atomic transition error logs warning and skips publish",
			isFinalAttempt: true, targetID: "100000000000000001", deployFailedErr: errors.New("db error"), promoteErr: errors.New("terminal error"), prNum: 42,
			wantErr: true,
		},
		{
			name:           "terminal promote error with atomic transition not updated skips publish",
			isFinalAttempt: true, targetID: "100000000000000001", deployFailedNotUpdated: true, promoteErr: errors.New("terminal error"), prNum: 42,
			wantErr: true, wantDeployFailed: 1,
		},
		{
			name:           "terminal record kustomizations error transitions to deploy_failed",
			isFinalAttempt: true, targetID: "100000000000000001", recordErr: errors.New("db record error"), prNum: 42,
			wantErr: true, wantDeployFailed: 1, wantDeployEvents: 1, wantTargetID: "100000000000000001",
		},
		{
			name:           "terminal reconcile error with requests recorded relies on watcher repair without deploy_failed",
			isFinalAttempt: true, targetID: "100000000000000001", reconcileErr: errors.New("flux api error"), hasKustReqs: true, prNum: 42,
			wantErr: true,
		},
		{
			name:           "terminal promote error with nil registry skips deploy_failed",
			isFinalAttempt: true, targetID: "100000000000000001", nilReg: true, promoteErr: errors.New("terminal error"), prNum: 42,
			wantErr: true,
		},
		{
			name:           "success completes without deploy_failed",
			isFinalAttempt: true, targetID: "100000000000000001", hasKustReqs: true, prNum: 42,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchWorkflowRunImagesCase(ctx, t, tc)
		})
	}
}
