package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// SetReconnectDelay overrides reconnection backoff parameters for fast hermetic unit tests.
func (sub *NomadStreamSubscriber) SetReconnectDelay(initial, maxDelay time.Duration) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	sub.reconnectDelay = initial
	sub.maxBackoff = maxDelay
}

func TestExtractJobsFromMetadata_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]any
		expected []string
	}{
		{name: "nil metadata", metadata: nil, expected: nil},
		{name: "jobs array string", metadata: map[string]any{"jobs": []any{"brain", "scheduler-mcp"}}, expected: []string{"brain", "scheduler-mcp"}},
		{name: "deduplicates clean job names", metadata: map[string]any{"jobs": []any{"brain", "brain", "hangar"}}, expected: []string{"brain", "hangar"}},
		{name: "empty jobs array", metadata: map[string]any{"jobs": []any{}}, expected: nil},
		{name: "non-slice jobs", metadata: map[string]any{"jobs": 123}, expected: nil},
		{name: "typed string slice", metadata: map[string]any{"jobs": []string{"job-a", "job-b"}}, expected: []string{"job-a", "job-b"}},
		{name: "whitespace and empty jobs filtered", metadata: map[string]any{"jobs": []any{"", "  ", "brain"}}, expected: []string{"brain"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJobsFromMetadata(tt.metadata)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("extractJobsFromMetadata() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExtractStartedJobsFromMetadata_TableDriven(t *testing.T) {
	startedTests := []struct {
		name     string
		metadata map[string]any
		expected []string
	}{
		{name: "nil metadata", metadata: nil, expected: nil},
		{name: "legacy metadata without started_jobs falls back to jobs", metadata: map[string]any{"jobs": []any{"coven", "db-migrate"}}, expected: []string{"coven", "db-migrate"}},
		{name: "started_jobs filters out unstarted sibling job", metadata: map[string]any{"jobs": []any{"coven", "db-migrate"}, "started_jobs": []any{"db-migrate"}}, expected: []string{"db-migrate"}},
		{name: "empty started_jobs returns nil", metadata: map[string]any{"jobs": []any{"coven"}, "started_jobs": []any{}}, expected: nil},
	}
	for _, tt := range startedTests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractStartedJobsFromMetadata(tt.metadata)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("extractStartedJobsFromMetadata() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHasNonTransientPlacementFailure_TableDriven(t *testing.T) {
	placementTests := []struct {
		name     string
		failedTG map[string]any
		queued   map[string]int
		desc     string
		want     bool
		wantDesc string
	}{
		{name: "empty failedTG", failedTG: nil, desc: "only desc", want: false, wantDesc: "only desc"},
		{name: "queued task group skipped", failedTG: map[string]any{"tg": map[string]any{"NodesExhausted": 1}}, queued: map[string]int{"tg": 1}, want: false},
		{name: "non-map rawMetrics returns true", failedTG: map[string]any{"tg": 1}, desc: "", want: true, wantDesc: "failed task group allocations:"},
		{name: "NodesAvailable zero returns true", failedTG: map[string]any{"tg": map[string]any{"NodesAvailable": map[string]any{"dc1": float64(0)}}}, desc: "no nodes", want: true, wantDesc: "no nodes (failed task group allocations:"},
		{name: "NodesAvailable positive and transient port collision with NodesExhausted returns false", failedTG: map[string]any{"tg": map[string]any{"NodesAvailable": map[string]any{"dc1": float64(1)}, "NodesExhausted": float64(1), "ClassExhausted": map[string]any{"quiet-zero": float64(1)}, "DimensionExhausted": map[string]any{"network: port collision": float64(1)}}}, want: false},
		{name: "NodesExhausted positive without DimensionExhausted returns true", failedTG: map[string]any{"tg": map[string]any{"NodesExhausted": float64(1)}}, want: true},
		{name: "ConstraintFiltered map positive returns true", failedTG: map[string]any{"tg": map[string]any{"ConstraintFiltered": map[string]any{"missing drivers": float64(1)}}}, want: true},
		{name: "ConstraintFiltered int positive returns true", failedTG: map[string]any{"tg": map[string]any{"ConstraintFiltered": 1}}, want: true},
		{name: "QuotaExhausted slice non-empty returns true", failedTG: map[string]any{"tg": map[string]any{"QuotaExhausted": []any{"default"}}}, want: true},
		{name: "DimensionExhausted memory returns true", failedTG: map[string]any{"tg": map[string]any{"DimensionExhausted": map[string]any{"memory": float64(1)}}}, want: true},
		{name: "AllocationResourceExhausted without DimensionExhausted returns true", failedTG: map[string]any{"tg": map[string]any{"AllocationResourceExhausted": float64(1)}}, want: true},
	}
	for _, tt := range placementTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasNonTransientPlacementFailure(tt.failedTG, tt.queued); got != tt.want {
				t.Errorf("hasNonTransientPlacementFailure() = %v, want %v", got, tt.want)
			}
			if tt.wantDesc != "" {
				if gotDesc := formatFailedTGAllocs(tt.desc, tt.failedTG); !strings.Contains(gotDesc, tt.wantDesc) {
					t.Errorf("formatFailedTGAllocs() = %q, want substring %q", gotDesc, tt.wantDesc)
				}
			}
		})
	}
}

func TestNomadStreamSubscriber_Events(t *testing.T) {
	defaultHealthyDeploys := `[{"ID":"dep-1","Status":"successful"}]`
	defaultHealthyAllocs := `[{"ID":"alloc-999","DesiredStatus":"run","ClientStatus":"running"}]`
	validThread := "1555405874565091380"
	validSHA := "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092"
	validRepo := "azylman/aerial"

	tests := []struct {
		name             string
		streamLines      []string
		deploymentsJSON  string
		allocationsJSON  string
		jobSpecJSON      string
		evalSpecJSON     string
		reg              *mockPRRegistry
		systemChannelID  string
		wantDeployedJobs []string
		wantFailedJobs   []string
		wantPrompts      int
		wantDispatches   int
		wantPromptSubstr []string
		wantDMs          int
		wantDMSubstr     string
	}{
		{
			name:             "heartbeat and deployment success",
			streamLines:      []string{"{}", "", `{"Index":1001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-123","JobID":"brain","Status":"successful","StatusDescription":"Deployment completed successfully"}}}]}`, "{}"},
			deploymentsJSON:  defaultHealthyDeploys,
			allocationsJSON:  defaultHealthyAllocs,
			reg:              mkDeployedReg(validThread, 552, validSHA, validRepo, true),
			wantDeployedJobs: []string{"brain"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"Continuous Delivery deployment completed for job brain", "Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely."},
		},
		{
			name:             "allocation running",
			streamLines:      []string{`{"Index":1002,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-999","JobID":"synapse","DesiredStatus":"run","ClientStatus":"running"}}}]}`},
			deploymentsJSON:  defaultHealthyDeploys,
			allocationsJSON:  defaultHealthyAllocs,
			reg:              mkDeployedReg(validThread, 552, validSHA, validRepo, true),
			wantDeployedJobs: []string{"synapse"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"Continuous Delivery deployment completed for job synapse", "Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely."},
		},
		{
			name:             "deployment failure dispatches DM and remediation prompt",
			streamLines:      []string{`{"Index":1003,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-fail-1","JobID":"brain","Status":"failed","StatusDescription":"Allocation failed health check: connection refused on port 8080"}}}]}`},
			reg:              mkDeployFailedReg(validThread, 553, "sha123456", validRepo, true),
			wantFailedJobs:   []string{"brain"},
			wantDispatches:   1,
			wantPromptSubstr: []string{"Nomad deployment for job brain failed", "Allocation failed health check: connection refused on port 8080", "Please investigate and fix the deployment failure."},
		},
		{
			name:             "config reload allocation running dispatches prompt",
			streamLines:      []string{`{"Index":1003,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-cfg-1","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"running"}}}]}`},
			deploymentsJSON:  defaultHealthyDeploys,
			allocationsJSON:  `[{"ID":"alloc-cfg-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg:              mkDeployedReg(validThread, 245, "sha_config_reload_abc", "azylman/aerial-config", true),
			wantDeployedJobs: []string{"scheduler-mcp"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"Continuous Delivery deployment completed for job scheduler-mcp on azylman/aerial-config"},
		},
		{
			name:             "evaluation in-place success",
			streamLines:      []string{`{"Index":1004,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-inplace-1","JobID":"brain","Status":"complete","DeploymentID":""}}}]}`},
			deploymentsJSON:  `[{"ID":"dep-old-1","Status":"successful"}]`,
			allocationsJSON:  `[{"ID":"alloc-brain-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg:              mkDeployedReg(validThread, 585, "sha_eval_success", validRepo, true),
			wantDeployedJobs: []string{"brain"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"job brain on azylman/aerial"},
		},
		{
			name:        "evaluation deployment-managed skipped when job unhealthy",
			streamLines: []string{`{"Index":1005,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-managed-1","JobID":"brain","Status":"complete","DeploymentID":"dep-rolling-99"}}}]}`},
			reg:         &mockPRRegistry{},
		},
		{
			name:             "evaluation placement failed task group allocs treated as failure",
			streamLines:      []string{`{"Index":1006,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-fail-allocs","JobID":"brain","Status":"complete","DeploymentID":"","FailedTGAllocs":{"brain":{"AllocationResourceExhausted":1}}}}}]}`},
			reg:              mkDeployFailedReg(validThread, 586, "sha_eval_fail", validRepo, true),
			wantFailedJobs:   []string{"brain"},
			wantDispatches:   1,
			wantPromptSubstr: []string{`failed with status "complete"`},
		},
		{
			name:             "evaluation status failed treated as failure",
			streamLines:      []string{`{"Index":1007,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-status-failed","JobID":"brain","Status":"failed","StatusDescription":"scheduling timeout","DeploymentID":""}}}]}`},
			reg:              mkDeployFailedReg(validThread, 587, "sha_eval_fail_status", validRepo, true),
			wantFailedJobs:   []string{"brain"},
			wantDispatches:   1,
			wantPromptSubstr: []string{"scheduling timeout"},
		},
		{
			name:            "allocation restarting guarded",
			streamLines:     []string{`{"Index":1004,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-restart-1","JobID":"brain","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"brain":{"State":"restarting"}}}}}]}`},
			deploymentsJSON: defaultHealthyDeploys,
			allocationsJSON: `[{"ID":"alloc-restart-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg:             &mockPRRegistry{},
		},
		{
			name:             "evaluation restart with deployment ID succeeds when healthy",
			streamLines:      []string{`{"Index":1008,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-restart-1","JobID":"kiosk-client","Status":"complete","DeploymentID":"dep-old-stale-1"}}}]}`},
			deploymentsJSON:  `[{"ID":"dep-old-stale-1","Status":"successful"}]`,
			allocationsJSON:  `[{"ID":"alloc-kiosk-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg:              mkDeployedReg(validThread, 599, "sha_eval_restart", "azylman/mirrormere", true),
			wantDeployedJobs: []string{"kiosk-client"},
			wantPrompts:      1,
		},
		{
			name:             "allocation failure driver error",
			streamLines:      []string{`{"Index":1009,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-fail-1","JobID":"brain","DesiredStatus":"stop","ClientStatus":"failed","ClientDescription":"Failed tasks","TaskStates":{"brain":{"State":"dead","Failed":true,"Events":[{"Type":"Driver Failure","DriverError":"invalid mount config: bind source path does not exist: /mnt/data/supervisor/share/coverage/brain"}]}}}}}]}`},
			reg:              mkDeployFailedReg(validThread, 613, "7cbc1f9b1234567890", validRepo, true),
			wantFailedJobs:   []string{"brain"},
			wantDispatches:   1,
			wantPromptSubstr: []string{"Nomad deployment for job brain failed", "bind source path does not exist: /mnt/data/supervisor/share/coverage/brain"},
		},
		{
			name:        "evaluation queued allocations ignored",
			streamLines: []string{`{"Index":2020,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-queued-1","JobID":"scheduler-mcp","Status":"complete","DeploymentID":"","FailedTGAllocs":{"scheduler-mcp":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"scheduler-mcp":1}}}}]}`},
			reg:         &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:             "evaluation multi-task-group unqueued failure treated as failure",
			streamLines:      []string{`{"Index":2021,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-multi-fail","JobID":"brain","Status":"complete","DeploymentID":"","FailedTGAllocs":{"taskGroupA":{"ResourceExhausted":1}},"QueuedAllocations":{"taskGroupB":1}}}}]}`},
			reg:              mkDeployFailedReg(validThread, 630, "sha_multi_tg_fail", validRepo, true),
			wantFailedJobs:   []string{"brain"},
			wantDispatches:   1,
			wantPromptSubstr: []string{"failed task group allocations"},
		},
		{
			name: "plan result events ignored across all topics",
			streamLines: []string{
				`{"Index":2030,"Events":[{"Topic":"Allocation","Type":"PlanResult","Payload":{"Allocation":{"ID":"alloc-plan-1","JobID":"github-mcp","DesiredStatus":"stop","ClientStatus":"failed"}}}]}`,
				`{"Index":2031,"Events":[{"Topic":"Deployment","Type":"PlanResult","Payload":{"Deployment":{"ID":"dep-plan-1","JobID":"github-mcp","Status":"failed"}}}]}`,
				`{"Index":2032,"Events":[{"Topic":"Evaluation","Type":"PlanResult","Payload":{"Evaluation":{"ID":"eval-plan-1","JobID":"github-mcp","Status":"failed"}}}]}`,
			},
			reg: &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:            "stale allocation failure ignored when job already healthy",
			streamLines:     []string{`{"Index":2040,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"old-dead-alloc","JobID":"github-mcp","DesiredStatus":"stop","ClientStatus":"failed","ClientDescription":"Failed tasks"}}}]}`},
			deploymentsJSON: `[{"ID":"dep-succ-1","Status":"successful","StatusDescription":"Deployment completed successfully","JobVersion":10,"CreateIndex":22255}]`,
			reg:             &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:            "evaluation failure ignored when job already healthy",
			streamLines:     []string{`{"Index":2045,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"old-eval-fail","JobID":"github-mcp","Status":"failed","StatusDescription":"placement failed"}}}]}`},
			deploymentsJSON: `[{"ID":"dep-succ-1","Status":"successful","StatusDescription":"Deployment completed successfully","JobVersion":10,"CreateIndex":22255}]`,
			reg:             &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:        "evaluation blocked status ignored",
			streamLines: []string{`{"Index":2050,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"995a3489-6ff6-c3aa-6541-0175856c6e84","JobID":"hangar","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"3b3eb97e-6077-6951-6489-5dbe1dfdbb6f"}}}]}`},
			reg:         &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:        "evaluation complete with BlockedEval ignored",
			streamLines: []string{`{"Index":2051,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"033659ef-f967-628b-9bd2-f1b76f646261","JobID":"hangar","Status":"complete","BlockedEval":"995a3489-6ff6-c3aa-6541-0175856c6e84","DeploymentID":"3b3eb97e-6077-6951-6489-5dbe1dfdbb6f","FailedTGAllocs":{"hangar":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"hangar":1}}}}]}`},
			reg:         &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:           "evaluation mixed state blocked with unqueued failure treated as failure",
			streamLines:    []string{`{"Index":2052,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-mixed-fail","JobID":"brain","Status":"complete","BlockedEval":"eval-blocked-b","FailedTGAllocs":{"groupA":{"DimensionExhausted":{"network: port collision":1}},"groupB":{"ConstraintFiltered":1}},"QueuedAllocations":{"groupA":1,"groupB":0}}}}]}`},
			reg:            mkDeployFailedReg(validThread, 637, "sha_mixed_fail", validRepo, true),
			wantFailedJobs: []string{"brain"},
			wantDispatches: 1,
		},
		{
			name:        "evaluation blocked with FailedTGAllocs ignored",
			streamLines: []string{`{"Index":2053,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"bda9b66d-3cef-17a0-fce1-4faec0f3a52d","JobID":"synapse","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"5622959a-4da3-fb28-09c7-5f6909503687","FailedTGAllocs":{"synapse":{"NodesExhausted":1,"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"synapse":0}}}}]}`},
			reg:         &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:         "evaluation blocked with deployment ID and non-transient exhaustion ignored",
			streamLines:  []string{`{"Index":2054,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"47bc9ad3-f6f2-927b-098d-236d45a10f04","JobID":"vanadis","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"618eee0a-977d-b048-96e0-bc600031808d","FailedTGAllocs":{"vanadis":{"NodesExhausted":1,"DimensionExhausted":{"cpu":1}}},"QueuedAllocations":{"vanadis":0}}}}]}`},
			evalSpecJSON: `{"ID":"47bc9ad3-f6f2-927b-098d-236d45a10f04","Status":"blocked"}`,
			reg:          &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:             "allocation restart exhaustion triggers tracked PR failure",
			streamLines:      []string{`{"Index":2050,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-exhausted-1","JobID":"infisical","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"server":{"State":"pending","Events":[{"Type":"Terminated","ExitCode":1,"Message":"Docker container exited with non-zero exit code: 1"},{"Type":"Restarting","RestartReason":"Exceeded allowed attempts, applying a delay"}]}}}}}]}`},
			deploymentsJSON:  `[{"ID":"dep-old-1","Status":"successful","StatusDescription":"Deployment completed successfully","JobVersion":5,"CreateIndex":100}]`,
			reg:              mkDeployFailedReg(validThread, 667, "sha-infisical-test", validRepo, true),
			wantFailedJobs:   []string{"infisical"},
			wantDispatches:   1,
			wantPromptSubstr: []string{"infisical", `task "server"`, "exited with code 1"},
		},
		{
			name:             "allocation restart exhaustion triggers unmanaged crash alert",
			streamLines:      []string{`{"Index":2051,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-exhausted-unmanaged","JobID":"infisical","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"server":{"State":"pending","Events":[{"Type":"Terminated","ExitCode":1,"Message":"Docker container exited with non-zero exit code: 1"},{"Type":"Restarting","RestartReason":"Exceeded allowed attempts, applying a delay"}]}}}}}]}`},
			reg:              &mockPRRegistry{deployFailedJobUpdated: false},
			systemChannelID:  validThread,
			wantFailedJobs:   []string{"infisical"},
			wantPrompts:      1,
			wantPromptSubstr: []string{`task "server"`, "exited with code 1"},
			wantDMs:          1,
			wantDMSubstr:     "💥 Nomad job `infisical` crashed or failed. Following up...",
		},
		{
			name:             "job registered periodic batch job dispatches success",
			streamLines:      []string{`{"Index":2060,"Events":[{"Topic":"Job","Type":"JobRegistered","Payload":{"Job":{"ID":"coverage-ingest","ModifyIndex":851}}}]}`},
			jobSpecJSON:      `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`,
			reg:              mkDeployedReg(validThread, 851, validSHA, validRepo, true),
			wantDeployedJobs: []string{"coverage-ingest"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"Continuous Delivery deployment completed for job coverage-ingest", "ref: job-registered-851"},
		},
		{
			name:             "one-shot batch allocation complete dispatches success",
			streamLines:      []string{`{"Index":2061,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-migrate-1","JobID":"db-migrate","DesiredStatus":"run","ClientStatus":"complete","TaskStates":{"migrate":{"State":"dead","Failed":false}}}}}]}`},
			allocationsJSON:  `[{"ID":"alloc-migrate-1","JobID":"db-migrate","DesiredStatus":"run","ClientStatus":"complete","JobVersion":2}]`,
			jobSpecJSON:      `{"ID":"db-migrate","Type":"batch","Status":"dead","Version":2,"Stop":false}`,
			reg:              mkDeployedReg(validThread, 971, validSHA, validRepo, true),
			wantDeployedJobs: []string{"db-migrate"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"Continuous Delivery deployment completed for job db-migrate", "ref: alloc-migrate-1"},
		},
		{
			name:             "evaluation blocked with non-transient memory exhaustion dispatches failure immediately",
			streamLines:      []string{`{"Index":2062,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-blocked-mem","JobID":"coven","Status":"blocked","StatusDescription":"created to place remaining allocations","FailedTGAllocs":{"coven":{"NodesExhausted":1,"DimensionExhausted":{"memory":1}}},"QueuedAllocations":{"coven":0}}}}]}`},
			evalSpecJSON:     `{"ID":"eval-blocked-mem","Status":"blocked"}`,
			reg:              mkDeployFailedReg(validThread, 971, validSHA, validRepo, true),
			wantFailedJobs:   []string{"coven"},
			wantDispatches:   1,
			wantPromptSubstr: []string{`failed with status "blocked"`, "memory"},
		},
		{
			name:         "evaluation blocked ignored when live evaluation already resolved",
			streamLines:  []string{`{"Index":2063,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-resolved-cpu","JobID":"coven","Status":"blocked","StatusDescription":"created to place remaining allocations","FailedTGAllocs":{"coven":{"NodesExhausted":1,"DimensionExhausted":{"cpu":1}}},"QueuedAllocations":{"coven":0}}}}]}`},
			evalSpecJSON: `{"ID":"eval-resolved-cpu","Status":"complete"}`,
			reg:          &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name:             "running allocation with pending poststop hook verified via job API dispatches success",
			streamLines:      []string{`{"Index":2064,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-kb-1","JobID":"kernel-browser","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"cleanup-locks":{"State":"dead","Failed":false,"Events":[{"Type":"Started"},{"Type":"Terminated","ExitCode":0}]},"kernel-browser":{"State":"running","Failed":false,"Events":[{"Type":"Started"}]},"persist-profile":{"State":"pending","Failed":false,"Restarts":0,"Events":[{"Type":"Received"}]}}}}}]}`},
			allocationsJSON:  `[{"ID":"alloc-kb-1","JobID":"kernel-browser","DesiredStatus":"run","ClientStatus":"running","JobVersion":5}]`,
			jobSpecJSON:      `{"ID":"kernel-browser","Type":"service","Status":"running","Version":5,"Stop":false,"TaskGroups":[{"Name":"kernel-browser","Tasks":[{"Name":"cleanup-locks","Lifecycle":{"Hook":"prestart","Sidecar":false}},{"Name":"kernel-browser"},{"Name":"persist-profile","Lifecycle":{"Hook":"poststop","Sidecar":false}}]}]}`,
			reg:              mkDeployedReg(validThread, 461, validSHA, validRepo, true),
			wantDeployedJobs: []string{"kernel-browser"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"Continuous Delivery deployment completed for job kernel-browser", "ref: alloc-kb-1"},
		},
		{
			name:            "running allocation with pending non-poststop sibling task verified via job API ignored",
			streamLines:     []string{`{"Index":2065,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-multi-1","JobID":"github-runner","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"runner-1":{"State":"running","Failed":false,"Events":[{"Type":"Started"}]},"runner-2":{"State":"pending","Failed":false,"Restarts":0,"Events":[{"Type":"Received"}]}}}}}]}`},
			allocationsJSON: `[{"ID":"alloc-multi-1","JobID":"github-runner","DesiredStatus":"run","ClientStatus":"running","JobVersion":1}]`,
			jobSpecJSON:     `{"ID":"github-runner","Type":"service","Status":"running","Version":1,"Stop":false,"TaskGroups":[{"Name":"runners","Tasks":[{"Name":"runner-1"},{"Name":"runner-2"}]}]}`,
			reg:             mkDeployedReg(validThread, 462, validSHA, validRepo, true),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			streamData := strings.Join(tt.streamLines, "\n") + "\n"
			nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/deployments") {
					if tt.deploymentsJSON != "" {
						_, _ = w.Write([]byte(tt.deploymentsJSON))
						return
					}
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if strings.Contains(r.URL.Path, "/allocations") {
					if tt.allocationsJSON != "" {
						_, _ = w.Write([]byte(tt.allocationsJSON))
						return
					}
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if strings.HasPrefix(r.URL.Path, "/v1/evaluation/") {
					if tt.evalSpecJSON != "" {
						_, _ = w.Write([]byte(tt.evalSpecJSON))
						return
					}
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if strings.HasPrefix(r.URL.Path, "/v1/job/") {
					if tt.jobSpecJSON != "" {
						_, _ = w.Write([]byte(tt.jobSpecJSON))
						return
					}
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = w.Write([]byte(streamData))
			}))
			defer nomadServer.Close()

			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{NomadAddr: nomadServer.URL, SystemChannelID: tt.systemChannelID}, nil)
			srv.SetRegistry(tt.reg)
			srv.SetDispatcher(mockDisp)

			sub := NewNomadStreamSubscriber(srv)
			sub.SetReconnectDelay(10*time.Millisecond, 50*time.Millisecond)

			if err := sub.consumeStream(ctx); err != nil && !strings.Contains(err.Error(), "EOF") {
				t.Fatalf("unexpected stream error: %v", err)
			}

			if !reflect.DeepEqual(tt.reg.deployedJobCalls, tt.wantDeployedJobs) {
				t.Fatalf("deployedJobCalls = %v, want %v", tt.reg.deployedJobCalls, tt.wantDeployedJobs)
			}
			if !reflect.DeepEqual(tt.reg.deployFailedJobCalls, tt.wantFailedJobs) {
				t.Fatalf("deployFailedJobCalls = %v, want %v", tt.reg.deployFailedJobCalls, tt.wantFailedJobs)
			}

			pCalls := mockDisp.PromptCalls()
			if len(pCalls) != tt.wantPrompts {
				t.Fatalf("prompt calls = %d, want %d", len(pCalls), tt.wantPrompts)
			}
			dCalls := mockDisp.DispatchCalls()
			if len(dCalls) != tt.wantDispatches {
				t.Fatalf("dispatch calls = %d, want %d", len(dCalls), tt.wantDispatches)
			}
			for _, subStr := range tt.wantPromptSubstr {
				found := false
				for _, p := range pCalls {
					if strings.Contains(p.Prompt, subStr) {
						found = true
						break
					}
				}
				for _, d := range dCalls {
					if strings.Contains(d.Prompt, subStr) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("prompt/dispatch missing %q", subStr)
				}
			}

			dmCalls := mockDisp.DirectMessageCalls()
			if len(dmCalls) != tt.wantDMs {
				t.Fatalf("direct message calls = %d, want %d", len(dmCalls), tt.wantDMs)
			}
			if tt.wantDMSubstr != "" && (len(dmCalls) == 0 || !strings.Contains(dmCalls[0].Content, tt.wantDMSubstr)) {
				t.Errorf("DM missing %q in %q", tt.wantDMSubstr, dmCalls[0].Content)
			}
		})
	}
}

func TestProcessNomadEvent_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		topic       string
		eventType   string
		payload     string
		reg         *mockPRRegistry
		wantErr     bool
		wantWidgets int
	}{
		{name: "PlanResult dropped", topic: "Allocation", eventType: "PlanResult", payload: `{"Allocation":{"ID":"alloc-1","JobID":"github-mcp","ClientStatus":"failed"}}`},
		{name: "blocked evaluation returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-blocked-direct","JobID":"hangar","Status":"blocked","StatusDescription":"created to place remaining allocations"}}`},
		{name: "blocked evaluation with deployment ID returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-blocked-dep","JobID":"vanadis","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"dep-123","FailedTGAllocs":{"vanadis":{"DimensionExhausted":{"cpu":1}}},"QueuedAllocations":{"vanadis":0}}}`},
		{name: "blocked evaluation with FailedTGAllocs returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-blocked-direct","JobID":"synapse","Status":"blocked","StatusDescription":"created to place remaining allocations","FailedTGAllocs":{"synapse":{"NodesExhausted":1,"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"synapse":0}}}`},
		{name: "blocked evaluation with non-transient placement failure dispatches failure", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-blocked-nontransient","JobID":"coven","Status":"blocked","StatusDescription":"created to place remaining allocations","FailedTGAllocs":{"coven":{"ConstraintFiltered":{"missing drivers":1}}},"QueuedAllocations":{"coven":0}}}`, reg: mkDeployFailedReg("target-1", 100, "sha1", "repo", true)},
		{name: "allocation complete with failed task state ignored", topic: "Allocation", eventType: "AllocationUpdated", payload: `{"Allocation":{"ID":"alloc-comp-failed","JobID":"db-migrate","DesiredStatus":"run","ClientStatus":"complete","TaskStates":{"migrate":{"State":"dead","Failed":true}}}}`},
		{name: "blocked evaluation with queued failed allocs returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"BlockedEval":"b-1","JobID":"synapse","FailedTGAllocs":{"tg":1},"QueuedAllocations":{"tg":1}}}`},
		{name: "evaluation with all failed groups queued returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"JobID":"synapse","FailedTGAllocs":{"tg":1},"QueuedAllocations":{"tg":1}}}`},
		{name: "evaluation with unqueued failed group and empty desc dispatches failure", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-fail-unqueued","JobID":"brain","StatusDescription":"","FailedTGAllocs":{"tg":1},"QueuedAllocations":{"tg":0}}}`, reg: mkDeployFailedReg("target-1", 100, "sha1", "repo", true)},
		{name: "evaluation with empty job ID returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"JobID":"","Status":"complete"}}`},
		{name: "evaluation with unknown status returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"JobID":"job1","Status":"unknown"}}`},
		{name: "malformed deployment payload returns error", topic: "Deployment", eventType: "DeploymentStatusUpdate", payload: `{invalid`, wantErr: true},
		{name: "malformed allocation payload returns error", topic: "Allocation", eventType: "AllocationUpdated", payload: `{invalid`, wantErr: true},
		{name: "malformed evaluation payload returns error", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{invalid`, wantErr: true},
		{name: "unknown topic returns nil", topic: "UnknownTopic", eventType: "SomethingUpdated", payload: `{}`},
		{name: "deployment success dispatches success", topic: "Deployment", eventType: "DeploymentStatusUpdate", payload: `{"Deployment":{"ID":"dep-succ-direct","JobID":"brain","Status":"successful"}}`, reg: mkDeployedReg("target-1", 100, "sha1", "repo", true)},
		{name: "deployment failed dispatches failure", topic: "Deployment", eventType: "DeploymentStatusUpdate", payload: `{"Deployment":{"ID":"dep-fail-direct","JobID":"brain","Status":"failed","StatusDescription":"oom"}}`, reg: mkDeployFailedReg("target-1", 100, "sha1", "repo", true)},
		{name: "allocation running dispatches success", topic: "Allocation", eventType: "AllocationUpdated", payload: `{"Allocation":{"ID":"alloc-run-direct","JobID":"brain","DesiredStatus":"run","ClientStatus":"running"}}`, reg: mkDeployedReg("target-1", 100, "sha1", "repo", true)},
		{
			name:      "allocation running updates job progress and syncs widget",
			topic:     "Allocation",
			eventType: "AllocationUpdated",
			payload:   `{"Allocation":{"ID":"alloc-run-prog","JobID":"brain","DesiredStatus":"run","ClientStatus":"running"}}`,
			reg: &mockPRRegistry{
				recordJobStateUpdated: true,
				recordJobStatePRNum:   42,
				recordJobStateRepo:    "azylman/aerial",
				resolveTargetID:       "1555405874565091380",
			},
			wantWidgets: 1,
		},
		{name: "allocation failed dispatches failure", topic: "Allocation", eventType: "AllocationUpdated", payload: `{"Allocation":{"ID":"alloc-fail-direct","JobID":"brain","DesiredStatus":"stop","ClientStatus":"failed"}}`, reg: mkDeployFailedReg("target-1", 100, "sha1", "repo", true)},
		{name: "evaluation complete dispatches success", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-comp-direct","JobID":"brain","Status":"complete","DeploymentID":""}}`, reg: mkDeployedReg("target-1", 100, "sha1", "repo", true)},
		{name: "evaluation failed dispatches failure", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-fail-direct","JobID":"brain","Status":"failed","StatusDescription":"timed out","DeploymentID":""}}`, reg: mkDeployFailedReg("target-1", 100, "sha1", "repo", true)},
		{name: "evaluation canceled after more recent eval was processed returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-cancel-superseded","JobID":"synapse","Status":"canceled","StatusDescription":"canceled after more recent eval was processed","DeploymentID":"dep-123"}}`},
		{name: "evaluation canceled with deployment ID returns nil", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-cancel-dep","JobID":"synapse","Status":"canceled","StatusDescription":"canceled","DeploymentID":"dep-123"}}`},
		{name: "evaluation canceled without deployment ID and not superseded dispatches failure", topic: "Evaluation", eventType: "EvaluationUpdated", payload: `{"Evaluation":{"ID":"eval-cancel-direct","JobID":"brain","Status":"canceled","StatusDescription":"manually canceled","DeploymentID":""}}`, reg: mkDeployFailedReg("target-1", 100, "sha1", "repo", true)},
		{name: "job deregistered returns nil", topic: "Job", eventType: "JobDeregistered", payload: `{"Job":{"ID":"coverage-ingest","ModifyIndex":10}}`},
		{name: "malformed job payload returns error", topic: "Job", eventType: "JobRegistered", payload: `{invalid`, wantErr: true},
		{name: "job registered with empty ID returns nil", topic: "Job", eventType: "JobRegistered", payload: `{"Job":{"ID":"","ModifyIndex":10}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReg := tt.reg
			if mockReg == nil {
				mockReg = &mockPRRegistry{deployFailedJobUpdated: true}
			}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			err := srv.ProcessNomadEvent(context.Background(), tt.topic, tt.eventType, json.RawMessage(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Fatalf("ProcessNomadEvent() err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantWidgets > 0 && len(mockDisp.WidgetCalls()) != tt.wantWidgets {
				t.Errorf("got %d widget calls, want %d", len(mockDisp.WidgetCalls()), tt.wantWidgets)
			}
		})
	}

	if allFailedTaskGroupsQueued(map[string]any{"g": 1}, map[string]int{"g": 0}) {
		t.Errorf("expected false for unqueued task group")
	}
}

func TestRouterServer_ReconcileActiveDeployments_StartupSweep(t *testing.T) {
	tests := []struct {
		name             string
		reg              PRRegistryUpdater
		allocsJSON       string
		deploysJSON      string
		wantErr          bool
		wantDeployedJobs []string
	}{
		{
			name: "startup sweep success",
			reg: func() PRRegistryUpdater {
				r := mkDeployedReg("1555405874565091380", 552, "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092", "azylman/aerial", true)
				r.listDeployingPRs = []DeployingPR{
					{ID: 101, Repo: "azylman/aerial", PRNumber: 552, MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092", TargetID: "1555405874565091380", Metadata: map[string]any{"jobs": []any{"synapse"}}},
				}
				return r
			}(),
			allocsJSON:       `[{"ID":"alloc-old","JobID":"synapse","DesiredStatus":"stop","ClientStatus":"complete"},{"ID":"alloc-new","JobID":"synapse","DesiredStatus":"run","ClientStatus":"running"}]`,
			wantDeployedJobs: []string{"synapse"},
		},
		{
			name: "startup sweep with started_jobs skips unstarted sibling job",
			reg: func() PRRegistryUpdater {
				r := mkDeployedReg("1555405874565091380", 971, "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092", "azylman/aerial", true)
				r.listDeployingPRs = []DeployingPR{
					{
						ID: 102, Repo: "azylman/aerial", PRNumber: 971, MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092", TargetID: "1555405874565091380",
						Metadata: map[string]any{"jobs": []any{"coven", "synapse"}, "started_jobs": []any{"synapse"}},
					},
				}
				return r
			}(),
			allocsJSON:       `[{"ID":"alloc-new","JobID":"synapse","DesiredStatus":"run","ClientStatus":"running"}]`,
			wantDeployedJobs: []string{"synapse"},
		},
		{name: "nil registry returns nil", reg: nil},
		{name: "registry list error returns error", reg: &mockPRRegistry{err: errors.New("db error")}, wantErr: true},
		{name: "empty pending deployments returns nil", reg: &mockPRRegistry{listDeployingPRs: nil}},
		{
			name: "transition error logs warning and continues",
			reg: &mockPRRegistry{
				listDeployingPRs: []DeployingPR{{PRNumber: 1, Repo: "azylman/aerial", Metadata: map[string]any{"job": "brain"}}},
				deployedErr:      errors.New("transition failed"),
			},
			deploysJSON: `[{"ID":"dep-1","Status":"successful"}]`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/deployments") && tc.deploysJSON != "" {
					_, _ = w.Write([]byte(tc.deploysJSON))
					return
				}
				if strings.Contains(r.URL.Path, "/allocations") && tc.allocsJSON != "" {
					_, _ = w.Write([]byte(tc.allocsJSON))
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer ts.Close()

			srv := NewRouterServer(Config{NomadAddr: ts.URL}, nil)
			srv.SetRegistry(tc.reg)
			srv.SetDispatcher(&mockOutboundDispatcher{})

			err := srv.ReconcileActiveDeployments(ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReconcileActiveDeployments() err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(tc.wantDeployedJobs) > 0 {
				mockReg, ok := tc.reg.(*mockPRRegistry)
				if !ok || !reflect.DeepEqual(mockReg.deployedJobCalls, tc.wantDeployedJobs) {
					t.Fatalf("deployedJobCalls = %v, want %v", mockReg.deployedJobCalls, tc.wantDeployedJobs)
				}
			}
		})
	}
}

func TestRouterServer_IsNomadJobHealthy_TableDriven(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var (
		deploymentsJSON string
		deployments404  bool
		allocsJSON      string
		jobSpecJSON     string
		jobSpecStatus   int
	)

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/deployments") {
			if deployments404 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(deploymentsJSON))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(allocsJSON))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/job/") {
			if jobSpecStatus != 0 && jobSpecStatus != http.StatusOK {
				w.WriteHeader(jobSpecStatus)
				return
			}
			if jobSpecJSON != "" {
				_, _ = w.Write([]byte(jobSpecJSON))
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer nomadServer.Close()

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL, NomadToken: "test-token"}, nil)

	tests := []struct {
		name        string
		job         string
		nomadAddr   string
		deploysJSON string
		deploys404  bool
		allocs      string
		jobSpec     string
		jobStatus   int
		wantHealthy bool
	}{
		{name: "deployment running is unhealthy", job: "brain", deploysJSON: `[{"ID":"dep-1","JobID":"brain","Status":"running"}]`, wantHealthy: false},
		{name: "deployment successful is healthy", job: "brain", deploysJSON: `[{"ID":"dep-1","JobID":"brain","Status":"successful"}]`, wantHealthy: true},
		{name: "no deployments and allocation pending is unhealthy", job: "brain", deploys404: true, allocs: `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"pending"}]`, wantHealthy: false},
		{name: "no deployments and allocation running is healthy", job: "brain", deploys404: true, allocs: `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"running"}]`, wantHealthy: true},
		{name: "deployments out of order newest successful is healthy", job: "brain", deploysJSON: `[{"ID":"dep-v14-failed","JobID":"brain","Status":"failed","JobVersion":14,"CreateIndex":19265},{"ID":"dep-v16-success","JobID":"brain","Status":"successful","JobVersion":16,"CreateIndex":19397},{"ID":"dep-v15-success","JobID":"brain","Status":"successful","JobVersion":15,"CreateIndex":19349}]`, wantHealthy: true},
		{name: "auto-revert higher JobVersion failed is unhealthy", job: "brain", deploysJSON: `[{"ID":"dep-revert-v16","JobID":"brain","Status":"successful","JobVersion":16,"CreateIndex":20500},{"ID":"dep-v17-failed","JobID":"brain","Status":"failed","JobVersion":17,"CreateIndex":20400}]`, wantHealthy: false},
		{name: "auto-revert indicated in StatusDescription is unhealthy", job: "brain", deploysJSON: `[{"ID":"dep-v17-roll","JobID":"brain","Status":"successful","JobVersion":17,"CreateIndex":20600,"StatusDescription":"Rolling back to version 16"}]`, wantHealthy: false},
		{name: "newer deployment failed highest CreateIndex is unhealthy", job: "brain", deploysJSON: `[{"ID":"dep-v15-success","JobID":"brain","Status":"successful","JobVersion":15,"CreateIndex":19349},{"ID":"dep-v16-failed","JobID":"brain","Status":"failed","JobVersion":16,"CreateIndex":19397}]`, wantHealthy: false},
		{name: "periodic batch job with 0 allocations running and enabled is healthy", job: "coverage-ingest", deploysJSON: `[]`, allocs: `[]`, jobSpec: `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`, jobStatus: http.StatusOK, wantHealthy: true},
		{name: "periodic batch job with completed past allocations is healthy", job: "coverage-ingest", deploysJSON: `[]`, allocs: `[{"ID":"a1","DesiredStatus":"stop","ClientStatus":"complete"}]`, jobSpec: `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`, jobStatus: http.StatusOK, wantHealthy: true},
		{name: "periodic batch job with actively failing allocation is unhealthy", job: "coverage-ingest", deploysJSON: `[]`, allocs: `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"failed"}]`, jobSpec: `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`, jobStatus: http.StatusOK, wantHealthy: false},
		{name: "parameterized batch job with 0 allocations running is healthy", job: "batch-worker", deploysJSON: `[]`, allocs: `[]`, jobSpec: `{"ID":"batch-worker","Type":"batch","Status":"running","Stop":false,"Periodic":null,"ParameterizedJob":{"Payload":"optional"}}`, jobStatus: http.StatusOK, wantHealthy: true},
		{name: "non-periodic batch job with 0 allocations is unhealthy", job: "migrate", deploysJSON: `[]`, allocs: `[]`, jobSpec: `{"ID":"migrate","Type":"batch","Status":"running","Stop":false,"Periodic":null,"ParameterizedJob":null}`, jobStatus: http.StatusOK, wantHealthy: false},
		{name: "one-shot batch job with running allocation is unhealthy", job: "db-migrate", deploysJSON: `[]`, allocs: `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"running","JobVersion":2}]`, jobSpec: `{"ID":"db-migrate","Type":"batch","Status":"running","Version":2,"Stop":false}`, jobStatus: http.StatusOK, wantHealthy: false},
		{name: "one-shot batch job with complete allocation at current version and older failed version is healthy", job: "db-migrate", deploysJSON: `[]`, allocs: `[{"ID":"a0","DesiredStatus":"run","ClientStatus":"failed","JobVersion":1},{"ID":"a1","DesiredStatus":"run","ClientStatus":"complete","JobVersion":2}]`, jobSpec: `{"ID":"db-migrate","Type":"batch","Status":"dead","Version":2,"Stop":false}`, jobStatus: http.StatusOK, wantHealthy: true},
		{name: "one-shot batch job with pending job status is unhealthy", job: "db-migrate", deploysJSON: `[]`, allocs: `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"complete","JobVersion":1}]`, jobSpec: `{"ID":"db-migrate","Type":"batch","Status":"pending","Version":2,"Stop":false}`, jobStatus: http.StatusOK, wantHealthy: false},
		{name: "one-shot batch job with Stop=true is unhealthy", job: "db-migrate", deploysJSON: `[]`, allocs: `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"complete","JobVersion":2}]`, jobSpec: `{"ID":"db-migrate","Type":"batch","Status":"dead","Version":2,"Stop":true}`, jobStatus: http.StatusOK, wantHealthy: false},
		{name: "stopped periodic batch job is unhealthy", job: "coverage-ingest", deploysJSON: `[]`, allocs: `[]`, jobSpec: `{"ID":"coverage-ingest","Type":"batch","Status":"dead","Stop":true,"Periodic":{"Enabled":true}}`, jobStatus: http.StatusOK, wantHealthy: false},
		{name: "disabled periodic batch job is unhealthy", job: "coverage-ingest", deploysJSON: `[]`, allocs: `[]`, jobSpec: `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":false}}`, jobStatus: http.StatusOK, wantHealthy: false},
		{name: "batch job spec API 500 error is unhealthy", job: "coverage-ingest", deploysJSON: `[]`, allocs: `[]`, jobStatus: http.StatusInternalServerError, wantHealthy: false},
		{name: "deployments and allocations 404 is unhealthy", job: "brain", deploys404: true, allocs: "", wantHealthy: false},
		{name: "deployments invalid json is unhealthy", job: "brain", deploysJSON: "not valid json", wantHealthy: false},
		{name: "unreachable nomad address is unhealthy", job: "brain", nomadAddr: "http://127.0.0.1:0", wantHealthy: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deploymentsJSON = tt.deploysJSON
			deployments404 = tt.deploys404
			allocsJSON = tt.allocs
			jobSpecJSON = tt.jobSpec
			jobSpecStatus = tt.jobStatus

			testSrv := srv
			if tt.nomadAddr != "" {
				testSrv = NewRouterServer(Config{NomadAddr: tt.nomadAddr, NomadToken: "test-token"}, &http.Client{Timeout: 5 * time.Millisecond})
			}

			if got := testSrv.isNomadJobHealthy(ctx, tt.job); got != tt.wantHealthy {
				t.Errorf("isNomadJobHealthy(%q) = %v, want %v", tt.job, got, tt.wantHealthy)
			}
		})
	}
}

func TestNomadStreamSubscriber_RiverQueue(t *testing.T) {
	tests := []struct {
		name             string
		streamLines      []string
		skipDuplicate    bool
		riverErr         error
		noRiver          bool
		wantJobs         int
		wantTopics       []string
		wantSubstrs      []string
		wantMetricSubstr string
	}{
		{
			name: "filters heartbeats and intermediate states before inserting terminal events",
			streamLines: []string{
				"{}",
				`{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-run","JobID":"brain","Status":"running"}}}]}`,
				`{"Index":2002,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-pend","JobID":"brain","DesiredStatus":"run","ClientStatus":"pending"}}}]}`,
				`{"Index":2003,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-succ","JobID":"brain","Status":"successful"}}}]}`,
				`{"Index":2004,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-run","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"running"}}}]}`,
				`{"Index":2005,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-fail","JobID":"discord-mcp","Status":"failed","StatusDescription":"oom"}}}]}`,
				`{"Index":2010,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-dead-1","JobID":"brain","DesiredStatus":"stop","ClientStatus":"failed"}}}]}`,
				`{"Index":2011,"Events":[{"Topic":"Job","Type":"JobRegistered","Payload":{"Job":{"ID":"coverage-ingest","ModifyIndex":42}}}]}`,
				`{"Index":2012,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-run","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"complete"}}}]}`,
				`{"Index":2013,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-blk-1","JobID":"coven","Status":"blocked","FailedTGAllocs":{"coven":{"DimensionExhausted":{"memory":1}}}}}}]}`,
				`{"Index":2014,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-haro","JobID":"haro","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"haro":{"State":"running","Restarts":0,"Events":[{"Type":"Started"},{"Type":"Restart Signaled"}]}}}}}]}`,
				`{"Index":2015,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-haro","JobID":"haro","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"haro":{"State":"running","Restarts":0,"Events":[{"Type":"Started"},{"Type":"Restart Signaled"},{"Type":"Terminated","ExitCode":137}]}}}}}]}`,
				`{"Index":2016,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-haro","JobID":"haro","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"haro":{"State":"pending","Restarts":1,"Events":[{"Type":"Started"},{"Type":"Restart Signaled"},{"Type":"Terminated","ExitCode":137},{"Type":"Restarting"}]}}}}}]}`,
				`{"Index":2017,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-haro","JobID":"haro","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"haro":{"State":"running","Restarts":1,"Events":[{"Type":"Started"},{"Type":"Restart Signaled"},{"Type":"Terminated","ExitCode":137},{"Type":"Restarting"},{"Type":"Started"}]}}}}}]}`,
				`{"Index":2018,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-canceled-superseded","JobID":"synapse","Status":"canceled","StatusDescription":"canceled after more recent eval was processed","DeploymentID":"dep-123"}}}]}`,
				`{"Index":2019,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-canceled-dep","JobID":"synapse","Status":"canceled","DeploymentID":"dep-123"}}}]}`,
				"{}",
			},
			wantJobs:    8,
			wantTopics:  []string{"Deployment", "Allocation", "Deployment", "Allocation", "Job", "Allocation", "Evaluation", "Allocation"},
			wantSubstrs: []string{"dep-succ", "nomad:allocation:scheduler-mcp:alloc-run:running", "dep-fail", ":failed", "nomad:job:coverage-ingest:42:registered", "nomad:allocation:scheduler-mcp:alloc-run:complete", "nomad:evaluation:coven:eval-blk-1:blocked", "nomad:allocation:haro:alloc-haro:running:1"},
		},
		{
			name: "duplicate nomad event increments skipped metric",
			streamLines: []string{
				`{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-dup-1","JobID":"brain","Status":"successful"}}}]}`,
			},
			skipDuplicate:    true,
			wantMetricSubstr: `synapse_unique_skipped_total{kind="nomad_event"}`,
		},
		{
			name: "river insert error logs and continues",
			streamLines: []string{
				`{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-err-1","JobID":"brain","Status":"successful"}}}]}`,
			},
			riverErr: errors.New("db insert failure"),
		},
		{
			name: "unhandled topic and malformed payload dropped before river",
			streamLines: []string{
				`{"Index":2001,"Events":[{"Topic":"Node","Type":"NodeRegistration","Payload":{}}]}`,
				`{"Index":2002,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":"malformed"}]}`,
				`{"Index":2003,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":"malformed"}]}`,
				`{"Index":2004,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":"malformed"}]}`,
				`{"Index":2005,"Events":[{"Topic":"Job","Type":"JobDeregistered","Payload":{"Job":{"ID":"coverage-ingest"}}}]}`,
				`{"Index":2006,"Events":[{"Topic":"Job","Type":"JobRegistered","Payload":"malformed"}]}`,
				`{"Index":2007,"Events":[{"Topic":"Job","Type":"JobRegistered","Payload":{"Job":{"ID":"   "}}}]}`,
			},
			wantJobs: 0,
		},
		{
			name: "handleNomadEvent direct processing error without river",
			streamLines: []string{
				`{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":"invalid-json"}]}`,
			},
			noRiver: true,
		},
		{
			name: "handleNomadEvent direct processing success without river",
			streamLines: []string{
				`{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-succ","JobID":"brain","Status":"successful"}}}]}`,
			},
			noRiver: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			streamData := strings.Join(tt.streamLines, "\n") + "\n"
			nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(streamData))
			}))
			defer nomadServer.Close()

			var riverArgs []RiverInserter
			var mockRiver *mockRiverInserter
			if !tt.noRiver {
				mockRiver = &mockRiverInserter{skipDuplicate: tt.skipDuplicate, err: tt.riverErr}
				riverArgs = append(riverArgs, mockRiver)
			}
			srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil, riverArgs...)
			sub := NewNomadStreamSubscriber(srv)
			sub.SetReconnectDelay(10*time.Millisecond, 50*time.Millisecond)

			if err := sub.consumeStream(ctx); err != nil && !strings.Contains(err.Error(), "EOF") {
				t.Fatalf("unexpected stream error: %v", err)
			}

			if mockRiver != nil && (tt.wantJobs > 0 || len(tt.wantTopics) > 0) {
				if len(mockRiver.insertedNomadJobs) != tt.wantJobs {
					t.Fatalf("expected %d nomad jobs inserted into river, got %d: %+v", tt.wantJobs, len(mockRiver.insertedNomadJobs), mockRiver.insertedNomadJobs)
				}
				for i, wantTopic := range tt.wantTopics {
					if mockRiver.insertedNomadJobs[i].Topic != wantTopic {
						t.Errorf("job %d topic = %q, want %q", i, mockRiver.insertedNomadJobs[i].Topic, wantTopic)
					}
				}
				for i, wantSubstr := range tt.wantSubstrs {
					jobStr := string(mockRiver.insertedNomadJobs[i].Payload) + mockRiver.insertedNomadJobs[i].IdempotencyKey
					if !strings.Contains(jobStr, wantSubstr) {
						t.Errorf("job %d missing %q in %+v", i, wantSubstr, mockRiver.insertedNomadJobs[i])
					}
				}
				for i, opts := range mockRiver.insertedOpts {
					if opts == nil || opts.MaxAttempts != 5 || !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
						t.Errorf("job %d unexpected river insert opts: %+v", i, opts)
					}
				}
			}

			if tt.wantMetricSubstr != "" {
				reqM := httptest.NewRequest(http.MethodGet, "/metrics", nil)
				recM := httptest.NewRecorder()
				srv.Routes().ServeHTTP(recM, reqM)
				if !strings.Contains(recM.Body.String(), tt.wantMetricSubstr) {
					t.Errorf("expected /metrics to include %q, got:\n%s", tt.wantMetricSubstr, recM.Body.String())
				}
			}
		})
	}
}

func TestIsAllocationRestarting_TableDriven(t *testing.T) {
	tests := []struct {
		name          string
		taskStates    map[string]NomadTaskState
		poststopTasks map[string]bool
		want          bool
	}{
		{name: "nil or empty map", taskStates: nil, want: false},
		{name: "running task without events", taskStates: map[string]NomadTaskState{"brain": {State: "running", Failed: false}}, want: false},
		{name: "pending task state", taskStates: map[string]NomadTaskState{"brain": {State: "pending"}}, want: true},
		{name: "restarting task state", taskStates: map[string]NomadTaskState{"brain": {State: "restarting"}}, want: true},
		{
			name: "last event Restart Signaled",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "running", Events: []NomadTaskEvent{{Type: "Started"}, {Type: "Restart Signaled", DisplayMessage: "Template with change_mode restart re-rendered"}}},
			},
			want: true,
		},
		{
			name: "last event Killing",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "running", Events: []NomadTaskEvent{{Type: "Started"}, {Type: "Restart Signaled"}, {Type: "Killing"}}},
			},
			want: true,
		},
		{
			name: "last event Terminated on running task during restart",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "running", Events: []NomadTaskEvent{{Type: "Started"}, {Type: "Restart Signaled"}, {Type: "Terminated", ExitCode: 137}}},
			},
			want: true,
		},
		{
			name: "last event Terminated on dead completed batch task",
			taskStates: map[string]NomadTaskState{
				"migrate": {State: "dead", Events: []NomadTaskEvent{{Type: "Started"}, {Type: "Terminated", ExitCode: 0}}},
			},
			want: false,
		},
		{
			name: "last event Restarting",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "running", Events: []NomadTaskEvent{{Type: "Restart Signaled"}, {Type: "Restarting"}}},
			},
			want: true,
		},
		{
			name: "last event Started after Restarting",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "running", Events: []NomadTaskEvent{{Type: "Restart Signaled"}, {Type: "Restarting"}, {Type: "Started"}}},
			},
			want: false,
		},
		{
			name: "exhausted task with fail mode returns false",
			taskStates: map[string]NomadTaskState{
				"task1": {State: "dead", Events: []NomadTaskEvent{{Type: "Not Restarting", RestartReason: `Exceeded allowed attempts 5 in interval 10m0s and mode is "fail"`}}},
			},
			want: false,
		},
		{
			name: "dormant poststop hook verified in poststopTasks map returns false",
			taskStates: map[string]NomadTaskState{
				"cleanup-locks":   {State: "dead", Events: []NomadTaskEvent{{Type: "Started"}, {Type: "Terminated", ExitCode: 0}}},
				"kernel-browser":  {State: "running", Events: []NomadTaskEvent{{Type: "Task Setup"}, {Type: "Started"}}},
				"persist-profile": {State: "pending", Restarts: 0, Events: []NomadTaskEvent{{Type: "Received"}}},
			},
			poststopTasks: map[string]bool{"persist-profile": true},
			want:          false,
		},
		{
			name: "unverified pending task with only Received event alongside running sibling returns true",
			taskStates: map[string]NomadTaskState{
				"main":    {State: "running", Events: []NomadTaskEvent{{Type: "Started"}}},
				"sidecar": {State: "pending", Restarts: 0, Events: []NomadTaskEvent{{Type: "Received"}}},
			},
			poststopTasks: nil,
			want:          true,
		},
		{
			name: "poststop task with positive restarts returns true",
			taskStates: map[string]NomadTaskState{
				"main":            {State: "running", Events: []NomadTaskEvent{{Type: "Started"}}},
				"persist-profile": {State: "pending", Restarts: 1, Events: []NomadTaskEvent{{Type: "Received"}, {Type: "Terminated", ExitCode: 1}}},
			},
			poststopTasks: map[string]bool{"persist-profile": true},
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAllocationRestarting(tt.taskStates, tt.poststopTasks); got != tt.want {
				t.Errorf("isAllocationRestarting() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTaskRestartExhausted_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		ts   NomadTaskState
		want bool
	}{
		{
			name: "running task state is never exhausted",
			ts:   NomadTaskState{State: "running", Events: []NomadTaskEvent{{Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"}}},
			want: false,
		},
		{
			name: "nil or empty events",
			ts:   NomadTaskState{State: "pending"},
			want: false,
		},
		{
			name: "transient restart within policy",
			ts:   NomadTaskState{State: "pending", Events: []NomadTaskEvent{{Type: "Terminated", ExitCode: 1}, {Type: "Restarting", RestartReason: "Restart within policy"}}},
			want: false,
		},
		{
			name: "exhausted attempts mode delay",
			ts:   NomadTaskState{State: "pending", Events: []NomadTaskEvent{{Type: "Terminated", ExitCode: 1}, {Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"}}},
			want: true,
		},
		{
			name: "exhausted attempts mode fail",
			ts:   NomadTaskState{State: "dead", Events: []NomadTaskEvent{{Type: "Terminated", ExitCode: 1}, {Type: "Not Restarting", RestartReason: `Exceeded allowed attempts 5 in interval 10m0s and mode is "fail"`}}},
			want: true,
		},
		{
			name: "subsequent recovery with Started event resets exhaustion",
			ts:   NomadTaskState{State: "pending", Events: []NomadTaskEvent{{Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"}, {Type: "Started"}}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTaskRestartExhausted(tt.ts); got != tt.want {
				t.Errorf("isTaskRestartExhausted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsAllocationRestartExhausted_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		taskStates map[string]NomadTaskState
		want       bool
	}{
		{name: "nil task states", taskStates: nil, want: false},
		{name: "healthy multi-task allocation", taskStates: map[string]NomadTaskState{"redis": {State: "running"}, "server": {State: "running"}}, want: false},
		{
			name: "multi-task allocation with one exhausted task",
			taskStates: map[string]NomadTaskState{
				"redis":  {State: "running"},
				"server": {State: "pending", Events: []NomadTaskEvent{{Type: "Terminated", ExitCode: 1}, {Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"}}},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAllocationRestartExhausted(tt.taskStates); got != tt.want {
				t.Errorf("isAllocationRestartExhausted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractAllocationErrorDetails_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		clientDesc string
		taskStates map[string]NomadTaskState
		wantSubstr string
	}{
		{name: "nil states and empty desc", clientDesc: "", taskStates: nil, wantSubstr: "allocation failed"},
		{name: "fallback to client description", clientDesc: "Failed tasks", taskStates: nil, wantSubstr: "Failed tasks"},
		{
			name:       "driver error in event",
			clientDesc: "Failed tasks",
			taskStates: map[string]NomadTaskState{
				"brain": {Failed: true, State: "dead", Events: []NomadTaskEvent{{Type: "Received", DisplayMessage: "Task received by client"}, {Type: "Driver Failure", DriverError: "bind source path does not exist: /mnt/data/supervisor/share/coverage/brain"}}},
			},
			wantSubstr: `task "brain": bind source path does not exist: /mnt/data/supervisor/share/coverage/brain`,
		},
		{
			name:       "driver error in event details map",
			clientDesc: "Failed tasks",
			taskStates: map[string]NomadTaskState{
				"brain": {Failed: true, State: "dead", Events: []NomadTaskEvent{{Type: "Driver Failure", Details: map[string]string{"driver_error": "failed to create container: out of memory"}}}},
			},
			wantSubstr: `task "brain": failed to create container: out of memory`,
		},
		{
			name: "setup error in event",
			taskStates: map[string]NomadTaskState{
				"sidecar": {Failed: true, Events: []NomadTaskEvent{{Type: "Task Setup", SetupError: "prestart hook failed: permission denied"}}},
			},
			wantSubstr: `task "sidecar": prestart hook failed: permission denied`,
		},
		{
			name: "validation error in event",
			taskStates: map[string]NomadTaskState{
				"api": {Failed: true, Events: []NomadTaskEvent{{Type: "Failed Validation", ValidationError: "invalid environment variable"}}},
			},
			wantSubstr: `task "api": invalid environment variable`,
		},
		{
			name: "exit code non-zero",
			taskStates: map[string]NomadTaskState{
				"worker": {Failed: true, Events: []NomadTaskEvent{{Type: "Terminated", ExitCode: 1, DisplayMessage: "Terminated unexpectedly"}}},
			},
			wantSubstr: `task "worker": Terminated unexpectedly (exited with code 1)`,
		},
		{
			name: "fallback to last event display message on dead task",
			taskStates: map[string]NomadTaskState{
				"proxy": {Failed: true, State: "dead", Events: []NomadTaskEvent{{Type: "Alloc Unhealthy", DisplayMessage: "Unhealthy because of failed task"}}},
			},
			wantSubstr: `task "proxy": Unhealthy because of failed task`,
		},
		{
			name:       "multiple tasks sorted deterministically",
			clientDesc: "Failed tasks",
			taskStates: map[string]NomadTaskState{
				"worker": {Failed: true, Events: []NomadTaskEvent{{Type: "Driver Failure", DriverError: "worker failed"}}},
				"api":    {Failed: true, Events: []NomadTaskEvent{{Type: "Driver Failure", DriverError: "api failed"}}},
			},
			wantSubstr: "task \"api\": api failed\ntask \"worker\": worker failed",
		},
		{
			name:       "restart-exhausted task with Failed=false extracts termination reason",
			clientDesc: "Tasks are running",
			taskStates: map[string]NomadTaskState{
				"server": {
					State: "pending",
					Events: []NomadTaskEvent{
						{Type: "Terminated", ExitCode: 1, DisplayMessage: "Docker container exited with non-zero exit code: 1"},
						{Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"},
					},
				},
			},
			wantSubstr: `task "server": Docker container exited with non-zero exit code: 1 (exited with code 1)`,
		},
		{
			name:       "download error in event",
			taskStates: map[string]NomadTaskState{"task": {Failed: true, Events: []NomadTaskEvent{{DownloadError: "download failed"}}}},
			wantSubstr: `task "task": download failed`,
		},
		{
			name:       "vault error in event",
			taskStates: map[string]NomadTaskState{"task": {Failed: true, Events: []NomadTaskEvent{{VaultError: "vault failed"}}}},
			wantSubstr: `task "task": vault failed`,
		},
		{
			name:       "exit code 137 without message",
			taskStates: map[string]NomadTaskState{"task": {Failed: true, Events: []NomadTaskEvent{{ExitCode: 137}}}},
			wantSubstr: `task "task": exited with code 137`,
		},
		{
			name:       "display message on failed task",
			taskStates: map[string]NomadTaskState{"task": {Failed: true, Events: []NomadTaskEvent{{DisplayMessage: "signal: killed"}}}},
			wantSubstr: `task "task": signal: killed`,
		},
		{
			name:       "dead task state with driver error",
			taskStates: map[string]NomadTaskState{"task": {State: "dead", Events: []NomadTaskEvent{{DriverError: "dead task driver error"}}}},
			wantSubstr: `task "task": dead task driver error`,
		},
		{
			name:       "running healthy task skipped",
			taskStates: map[string]NomadTaskState{"healthy": {State: "running", Failed: false}},
			wantSubstr: "allocation failed",
		},
		{
			name:       "driver failure event with display message only",
			taskStates: map[string]NomadTaskState{"task": {Failed: true, Events: []NomadTaskEvent{{Type: "Driver Failure", DisplayMessage: "driver msg"}}}},
			wantSubstr: `task "task": driver msg`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractAllocationErrorDetails(tt.clientDesc, tt.taskStates); !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("extractAllocationErrorDetails() = %q, want substring %q", got, tt.wantSubstr)
			}
		})
	}
}

func TestNomadUnmanagedJobCrashAndDelegation(t *testing.T) {
	tests := []struct {
		name             string
		useFailure       bool
		reg              PRRegistryUpdater
		skipDuplicate    bool
		riverErr         error
		noRiver          bool
		jobID            string
		allocID          string
		status           string
		desc             string
		dmErr            error
		promptErr        error
		dispatchErr      error
		widgetErr        error
		wantCrashJobs    int
		wantDMs          int
		wantPrompts      int
		wantDispatches   int
		wantWidgets      int
		wantMetricSubstr string
		wantErr          bool
	}{
		{name: "HandleUnmanagedJobCrash enqueues River job", jobID: "openobserve", allocID: "alloc-1", status: "failed", desc: "OOMKilled", wantCrashJobs: 1},
		{name: "HandleUnmanagedJobCrash duplicate metric", skipDuplicate: true, jobID: "openobserve", allocID: "alloc-2", status: "failed", desc: "OOMKilled", wantCrashJobs: 1, wantMetricSubstr: `synapse_unique_skipped_total{kind="nomad_crash_alert"}`},
		{name: "HandleUnmanagedJobCrash river error", riverErr: errors.New("db error"), jobID: "openobserve", allocID: "alloc-err", status: "failed", desc: "crash", wantErr: true},
		{name: "HandleUnmanagedJobCrash direct execution without River", noRiver: true, jobID: "openobserve", allocID: "alloc-3", status: "failed", desc: "signal: killed", wantDMs: 1, wantPrompts: 1},
		{name: "HandleUnmanagedJobCrash direct execution dm error logs and continues", noRiver: true, jobID: "openobserve", allocID: "alloc-4", status: "failed", desc: "crash", dmErr: errors.New("dm fail"), wantDMs: 1, wantPrompts: 1},
		{name: "HandleUnmanagedJobCrash direct execution prompt error returns error", noRiver: true, jobID: "openobserve", allocID: "alloc-5", status: "failed", desc: "crash", promptErr: errors.New("prompt fail"), wantDMs: 1, wantPrompts: 1, wantErr: true},
		{name: "HandleUnmanagedJobCrash empty job ID no-op", noRiver: true, jobID: ""},
		{name: "HandleNomadJobFailure untracked in registry", useFailure: true, noRiver: true, reg: &mockPRRegistry{deployFailedJobUpdated: false}, jobID: "openobserve", allocID: "alloc-99", status: "failed", desc: "Task OOM killed", wantDMs: 1, wantPrompts: 1},
		{name: "HandleNomadJobFailure already deploy_failed PR skips unmanaged crash alert", useFailure: true, noRiver: true, reg: &mockPRRegistry{deployFailedJobPRNum: 613, deployFailedJobUpdated: false}, jobID: "eri", allocID: "alloc-613", status: "failed", desc: "dotenv error", wantDMs: 0, wantPrompts: 0},
		{name: "HandleNomadJobFailure tracked syncs widget", useFailure: true, noRiver: true, reg: &mockPRRegistry{deployFailedJobTargetID: "1555405874565091380", deployFailedJobPRNum: 613, deployFailedJobMergeSHA: "sha613", deployFailedJobRepo: "azylman/aerial", deployFailedJobUpdated: true, resolveTargetID: "1555405874565091380", resolveBranch: "feat/x"}, jobID: "brain", allocID: "alloc-613", status: "failed", desc: "Task OOM killed", widgetErr: errors.New("widget err"), wantDMs: 0, wantDispatches: 1, wantWidgets: 1},
		{name: "HandleNomadJobFailure prompt error returns error for retry", useFailure: true, noRiver: true, reg: &mockPRRegistry{deployFailedJobTargetID: "", deployFailedJobPRNum: 614, deployFailedJobMergeSHA: "sha614", deployFailedJobRepo: "azylman/aerial", deployFailedJobUpdated: true}, jobID: "eri", allocID: "alloc-614", status: "failed", desc: "dotenv error", dispatchErr: errors.New("502 bad gateway"), wantDMs: 0, wantDispatches: 1, wantErr: true},
		{name: "HandleNomadJobFailure nil registry", useFailure: true, noRiver: true, reg: nil, jobID: "cadvisor", allocID: "alloc-99", status: "failed", desc: "Docker daemon died", wantDMs: 1, wantPrompts: 1},
		{name: "HandleNomadJobFailure empty job ID no-op", useFailure: true, noRiver: true, jobID: "", allocID: "alloc-1", status: "failed", desc: "oom"},
		{name: "HandleNomadJobFailure registry error", useFailure: true, noRiver: true, reg: &mockPRRegistry{err: errors.New("registry failure")}, jobID: "vector", allocID: "alloc-99", status: "failed", desc: "Config parse failed", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var mockRiver *mockRiverInserter
			var riverArgs []RiverInserter
			if !tc.noRiver {
				mockRiver = &mockRiverInserter{skipDuplicate: tc.skipDuplicate, err: tc.riverErr}
				riverArgs = append(riverArgs, mockRiver)
			}
			mockDisp := &mockOutboundDispatcher{directMessageErr: tc.dmErr, promptErr: tc.promptErr, dispatchErr: tc.dispatchErr, widgetErr: tc.widgetErr}
			srv := NewRouterServer(Config{SystemChannelID: "1555405874565091380"}, nil, riverArgs...)
			srv.SetRegistry(tc.reg)
			srv.SetDispatcher(mockDisp)

			var err error
			if tc.useFailure {
				err = srv.HandleNomadJobFailure(ctx, tc.jobID, tc.allocID, tc.status, tc.desc)
			} else {
				err = srv.HandleUnmanagedJobCrash(ctx, tc.jobID, tc.allocID, tc.status, tc.desc)
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
				t.Errorf("DM calls = %d, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
			}
			if len(mockDisp.PromptCalls()) != tc.wantPrompts {
				t.Errorf("Prompt calls = %d, want %d", len(mockDisp.PromptCalls()), tc.wantPrompts)
			}
			if len(mockDisp.DispatchCalls()) != tc.wantDispatches {
				t.Errorf("Dispatch calls = %d, want %d", len(mockDisp.DispatchCalls()), tc.wantDispatches)
			}
			if len(mockDisp.WidgetCalls()) != tc.wantWidgets {
				t.Errorf("Widget calls = %d, want %d", len(mockDisp.WidgetCalls()), tc.wantWidgets)
			}
			if mockRiver != nil && len(mockRiver.insertedCrashAlertJobs) != tc.wantCrashJobs {
				t.Errorf("insertedCrashAlertJobs = %d, want %d", len(mockRiver.insertedCrashAlertJobs), tc.wantCrashJobs)
			}
			if tc.wantMetricSubstr != "" {
				reqM := httptest.NewRequest(http.MethodGet, "/metrics", nil)
				recM := httptest.NewRecorder()
				srv.Routes().ServeHTTP(recM, reqM)
				if !strings.Contains(recM.Body.String(), tc.wantMetricSubstr) {
					t.Errorf("expected /metrics to include %q, got:\n%s", tc.wantMetricSubstr, recM.Body.String())
				}
			}
		})
	}
}

func TestNomadStreamSubscriber_StartAndReconnect(t *testing.T) {
	ctx := t.Context()

	var attempts atomic.Int32
	reconnectedChan := make(chan struct{})

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if att == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"temporary nomad error"}`))
			return
		}
		if att == 2 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}\n"))
			return
		}
		select {
		case <-reconnectedChan:
		default:
			close(reconnectedChan)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}))
	defer nomadServer.Close()

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)
	sub := NewNomadStreamSubscriber(srv)
	sub.SetReconnectDelay(1*time.Millisecond, 5*time.Millisecond)

	sub.Start(ctx)

	select {
	case <-reconnectedChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reconnect")
	}
}

func TestRouterServer_HandleNomadJobSuccess_TableDriven(t *testing.T) {
	tests := []struct {
		name              string
		reg               PRRegistryUpdater
		disp              OutboundDispatcher
		job               string
		ref               string
		wantErr           bool
		wantDispatches    int
		wantWidgets       int
		wantAppendCalls   int
		wantStatusReverts int
	}{
		{name: "empty job is no-op", reg: &mockPRRegistry{}, job: ""},
		{name: "nil registry is no-op", reg: nil, job: "brain"},
		{name: "registry error returned", reg: &mockPRRegistry{err: errors.New("reg fail")}, job: "brain", wantErr: true},
		{name: "untracked job updated false is no-op", reg: &mockPRRegistry{deployedUpdated: false}, job: "brain"},
		{name: "intermediate job success updated false syncs widget", reg: mkDeployedReg("target-1", 10, "sha", "repo", false), disp: &mockOutboundDispatcher{}, job: "brain", ref: "dep-1", wantWidgets: 1},
		{name: "successful transition dispatches prompt", reg: mkDeployedReg("target-1", 10, "sha", "repo", true), disp: &mockOutboundDispatcher{}, job: "brain", ref: "dep-1", wantDispatches: 1},
		{
			name:              "dispatchDeploymentSuccessPrompt error reverts status to deploying and restores job in metadata",
			reg:               mkDeployedReg("1555405874565091380", 10, "sha", "azylman/aerial", true),
			disp:              &mockOutboundDispatcher{promptErr: errors.New("dispatch prompt failed")},
			job:               "brain",
			ref:               "dep-1",
			wantErr:           true,
			wantAppendCalls:   1,
			wantStatusReverts: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(tc.reg)
			srv.SetDispatcher(tc.disp)
			err := srv.HandleNomadJobSuccess(context.Background(), tc.job, tc.ref)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleNomadJobSuccess() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.disp != nil && tc.wantWidgets > 0 {
				mockDisp := tc.disp.(*mockOutboundDispatcher)
				if len(mockDisp.WidgetCalls()) != tc.wantWidgets {
					t.Errorf("got %d widget calls, want %d", len(mockDisp.WidgetCalls()), tc.wantWidgets)
				}
			}
			if tc.reg != nil {
				mockReg := tc.reg.(*mockPRRegistry)
				if tc.wantAppendCalls > 0 && len(mockReg.appendPRJobsCalls) != tc.wantAppendCalls {
					t.Errorf("got %d appendPRJobs calls, want %d", len(mockReg.appendPRJobsCalls), tc.wantAppendCalls)
				}
				if tc.wantStatusReverts > 0 && len(mockReg.updatePRStatusCalls) != tc.wantStatusReverts {
					t.Errorf("got %d updatePRStatus calls, want %d", len(mockReg.updatePRStatusCalls), tc.wantStatusReverts)
				}
			}
		})
	}
}

func TestNomadStreamSubscriber_ConsumeStream_Errors(t *testing.T) {
	tests := []struct {
		name       string
		nomadAddr  string
		statusCode int
		respBody   string
		token      string
		lastIndex  uint64
		wantErr    bool
		wantSubstr string
		wantIndex  uint64
	}{
		{name: "server returns 500 error", statusCode: http.StatusInternalServerError, respBody: "internal error", wantErr: true},
		{name: "server returns 403 error", statusCode: http.StatusForbidden, wantErr: true, wantSubstr: "unexpected status 403"},
		{name: "out of bounds error resets lastIndex to 0", statusCode: http.StatusBadRequest, respBody: "index 999999 is out of bounds", lastIndex: 999999, wantErr: true, wantIndex: 0, wantSubstr: "status 400"},
		{name: "payload index advances lastIndex", statusCode: http.StatusOK, respBody: "{\"Index\":1234}\n", wantIndex: 1234},
		{name: "token header passed in request", statusCode: http.StatusOK, respBody: "{}\n", token: "test-secret-token"},
		{name: "heartbeats and blank lines before EOF", statusCode: http.StatusOK, respBody: "{}\n\n   \n{}\n"},
		{name: "malformed json stream line skipped", statusCode: http.StatusOK, respBody: "invalid json\n{}\n"},
		{name: "stream evaluation empty job, unknown status, and unknown topic skipped", statusCode: http.StatusOK, respBody: "{\"Index\":3001,\"Events\":[{\"Topic\":\"Evaluation\",\"Type\":\"EvaluationUpdated\",\"Payload\":{\"Evaluation\":{\"JobID\":\"\",\"Status\":\"complete\"}}}]}\n{\"Index\":3002,\"Events\":[{\"Topic\":\"Evaluation\",\"Type\":\"EvaluationUpdated\",\"Payload\":{\"Evaluation\":{\"JobID\":\"job1\",\"Status\":\"unknown\"}}}]}\n{\"Index\":3003,\"Events\":[{\"Topic\":\"UnknownTopic\",\"Type\":\"CustomEvent\",\"Payload\":{}}]}\n"},
		{name: "unreachable stream connection failed", nomadAddr: "http://127.0.0.1:0", wantErr: true, wantSubstr: "stream connection failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			var capturedToken string
			nomadAddr := tt.nomadAddr
			if nomadAddr == "" {
				srvMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					capturedToken = r.Header.Get("X-Nomad-Token")
					status := tt.statusCode
					if status == 0 {
						status = http.StatusOK
					}
					w.WriteHeader(status)
					_, _ = w.Write([]byte(tt.respBody))
				}))
				defer srvMock.Close()
				nomadAddr = srvMock.URL
			}

			client := &http.Client{Timeout: 50 * time.Millisecond}
			srv := NewRouterServer(Config{NomadAddr: nomadAddr, NomadToken: tt.token}, client)
			sub := NewNomadStreamSubscriber(srv)
			sub.lastIndex = tt.lastIndex

			err := sub.consumeStream(ctx)
			if (err != nil) != tt.wantErr && !errors.Is(err, io.EOF) {
				t.Fatalf("consumeStream() err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantSubstr)) {
				t.Fatalf("consumeStream() err = %v, want substring %q", err, tt.wantSubstr)
			}
			if tt.token != "" && capturedToken != tt.token {
				t.Errorf("capturedToken = %q, want %q", capturedToken, tt.token)
			}
			if tt.wantIndex != 0 || tt.lastIndex != 0 {
				sub.mu.Lock()
				idx := sub.lastIndex
				sub.mu.Unlock()
				if idx != tt.wantIndex {
					t.Errorf("lastIndex = %d, want %d", idx, tt.wantIndex)
				}
			}
		})
	}
}
