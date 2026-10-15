package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExtractJobsFromMetadata_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]interface{}
		expected []string
	}{
		{name: "nil metadata", metadata: nil, expected: nil},
		{name: "jobs array string", metadata: map[string]interface{}{"jobs": []interface{}{"brain", "scheduler-mcp"}}, expected: []string{"brain", "scheduler-mcp"}},
		{name: "single job string", metadata: map[string]interface{}{"job": "hangar"}, expected: []string{"hangar"}},
		{name: "both jobs and job", metadata: map[string]interface{}{"jobs": []interface{}{"brain"}, "job": "hangar"}, expected: []string{"brain", "hangar"}},
		{name: "deduplicates clean job names", metadata: map[string]interface{}{"jobs": []interface{}{"brain", "brain", "hangar"}}, expected: []string{"brain", "hangar"}},
		{name: "deduplicates duplicate job names in jobs and job", metadata: map[string]interface{}{"jobs": []interface{}{"brain", "hangar"}, "job": "brain"}, expected: []string{"brain", "hangar"}},
		{name: "empty jobs array", metadata: map[string]interface{}{"jobs": []interface{}{}}, expected: nil},
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

func TestNomadStreamSubscriber_Events(t *testing.T) {
	defaultHealthyDeploys := `[{"ID":"dep-1","Status":"successful"}]`
	defaultHealthyAllocs := `[{"ID":"alloc-999","DesiredStatus":"run","ClientStatus":"running"}]`

	tests := []struct {
		name             string
		streamLines      []string
		deploymentsJSON  string
		allocationsJSON  string
		reg              *mockPRRegistry
		systemChannelID  string
		wantDeployedJobs []string
		wantFailedJobs   []string
		wantPrompts      int
		wantPromptSubstr []string
		wantDMs          int
		wantDMSubstr     string
	}{
		{
			name: "heartbeat and deployment success",
			streamLines: []string{
				"{}",
				"",
				`{"Index":1001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-123","JobID":"brain","Status":"successful","StatusDescription":"Deployment completed successfully"}}}]}`,
				"{}",
			},
			deploymentsJSON: defaultHealthyDeploys,
			allocationsJSON: defaultHealthyAllocs,
			reg: &mockPRRegistry{
				deployedTargetID: "1555405874565091380",
				deployedPRNum:    552,
				deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				deployedRepo:     "azylman/aerial",
				deployedUpdated:  true,
			},
			wantDeployedJobs: []string{"brain"},
			wantPrompts:      1,
			wantPromptSubstr: []string{
				"Continuous Delivery deployment completed for job brain",
				"Directive: Based on the recent conversation context above",
			},
			wantDMs: 0,
		},
		{
			name: "allocation running with channel context",
			streamLines: []string{
				`{"Index":1002,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-999","JobID":"webhooks-router","DesiredStatus":"run","ClientStatus":"running"}}}]}`,
			},
			deploymentsJSON: defaultHealthyDeploys,
			allocationsJSON: defaultHealthyAllocs,
			reg: &mockPRRegistry{
				deployedTargetID: "1555405874565091380",
				deployedPRNum:    552,
				deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				deployedRepo:     "azylman/aerial",
				deployedUpdated:  true,
				channelContextMap: map[string][]ChannelMessageContext{
					"1555405874565091380": {{AuthorName: "arcane103", Content: "Ship it", ResponseText: "Shipping!"}},
				},
			},
			wantDeployedJobs: []string{"webhooks-router"},
			wantPrompts:      1,
			wantPromptSubstr: []string{
				"Continuous Delivery deployment completed for job webhooks-router",
				"[arcane103]: Ship it",
				"[Aerial]: Shipping!",
				"Directive: Based on the recent conversation context above",
			},
		},
		{
			name: "deployment failure dispatches DM and remediation prompt",
			streamLines: []string{
				`{"Index":1003,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-fail-1","JobID":"brain","Status":"failed","StatusDescription":"Allocation failed health check: connection refused on port 8080"}}}]}`,
			},
			reg: &mockPRRegistry{
				deployFailedJobTargetID: "1555405874565091380",
				deployFailedJobPRNum:    553,
				deployFailedJobMergeSHA: "sha123456",
				deployFailedJobRepo:     "azylman/aerial",
				deployFailedJobUpdated:  true,
			},
			wantFailedJobs: []string{"brain"},
			wantPrompts:    1,
			wantPromptSubstr: []string{
				"Nomad deployment for job brain failed",
				"Allocation failed health check: connection refused on port 8080",
				"Please investigate and fix the deployment failure.",
			},
			wantDMs:      1,
			wantDMSubstr: "💥 **(PR: #553, repo: aerial)** Nomad deployment failed for brain. Following up...",
		},
		{
			name: "config reload allocation running dispatches prompt",
			streamLines: []string{
				`{"Index":1003,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-cfg-1","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"running"}}}]}`,
			},
			deploymentsJSON: defaultHealthyDeploys,
			allocationsJSON: `[{"ID":"alloc-cfg-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg: &mockPRRegistry{
				deployedTargetID: "1555405874565091380",
				deployedPRNum:    245,
				deployedMergeSHA: "sha_config_reload_abc",
				deployedRepo:     "azylman/aerial-config",
				deployedUpdated:  true,
			},
			wantDeployedJobs: []string{"scheduler-mcp"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"Continuous Delivery deployment completed for job scheduler-mcp on azylman/aerial-config"},
		},
		{
			name: "evaluation in-place success",
			streamLines: []string{
				`{"Index":1004,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-inplace-1","JobID":"brain","Status":"complete","DeploymentID":""}}}]}`,
			},
			deploymentsJSON: `[{"ID":"dep-old-1","Status":"successful"}]`,
			allocationsJSON: `[{"ID":"alloc-brain-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg: &mockPRRegistry{
				deployedTargetID: "1555405874565091380",
				deployedPRNum:    585,
				deployedMergeSHA: "sha_eval_success",
				deployedRepo:     "azylman/aerial",
				deployedUpdated:  true,
			},
			wantDeployedJobs: []string{"brain"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"job brain on azylman/aerial"},
		},
		{
			name: "evaluation deployment-managed skipped when job unhealthy",
			streamLines: []string{
				`{"Index":1005,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-managed-1","JobID":"brain","Status":"complete","DeploymentID":"dep-rolling-99"}}}]}`,
			},
			reg: &mockPRRegistry{},
		},
		{
			name: "evaluation placement failed task group allocs treated as failure",
			streamLines: []string{
				`{"Index":1006,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-fail-allocs","JobID":"brain","Status":"complete","DeploymentID":"","FailedTGAllocs":{"brain":{"AllocationResourceExhausted":1}}}}}]}`,
			},
			reg: &mockPRRegistry{
				deployFailedJobTargetID: "1555405874565091380",
				deployFailedJobPRNum:    586,
				deployFailedJobMergeSHA: "sha_eval_fail",
				deployFailedJobRepo:     "azylman/aerial",
				deployFailedJobUpdated:  true,
			},
			wantFailedJobs:   []string{"brain"},
			wantPrompts:      1,
			wantPromptSubstr: []string{`failed with status "complete"`},
			wantDMs:          1,
		},
		{
			name: "evaluation status failed treated as failure",
			streamLines: []string{
				`{"Index":1007,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-status-failed","JobID":"brain","Status":"failed","StatusDescription":"scheduling timeout","DeploymentID":""}}}]}`,
			},
			reg: &mockPRRegistry{
				deployFailedJobTargetID: "1555405874565091380",
				deployFailedJobPRNum:    587,
				deployFailedJobMergeSHA: "sha_eval_fail_status",
				deployFailedJobRepo:     "azylman/aerial",
				deployFailedJobUpdated:  true,
			},
			wantFailedJobs:   []string{"brain"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"scheduling timeout"},
			wantDMs:          1,
		},
		{
			name: "allocation restarting guarded",
			streamLines: []string{
				`{"Index":1004,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-restart-1","JobID":"brain","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"brain":{"State":"restarting"}}}}}]}`,
			},
			deploymentsJSON: defaultHealthyDeploys,
			allocationsJSON: `[{"ID":"alloc-restart-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg:             &mockPRRegistry{},
		},
		{
			name: "evaluation restart with deployment ID succeeds when healthy",
			streamLines: []string{
				`{"Index":1008,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-restart-1","JobID":"kiosk-client","Status":"complete","DeploymentID":"dep-old-stale-1"}}}]}`,
			},
			deploymentsJSON: `[{"ID":"dep-old-stale-1","Status":"successful"}]`,
			allocationsJSON: `[{"ID":"alloc-kiosk-1","DesiredStatus":"run","ClientStatus":"running"}]`,
			reg: &mockPRRegistry{
				deployedTargetID: "1555405874565091380",
				deployedPRNum:    599,
				deployedMergeSHA: "sha_eval_restart",
				deployedRepo:     "azylman/mirrormere",
				deployedUpdated:  true,
			},
			wantDeployedJobs: []string{"kiosk-client"},
			wantPrompts:      1,
		},
		{
			name: "allocation failure driver error",
			streamLines: []string{
				`{"Index":1009,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-fail-1","JobID":"brain","DesiredStatus":"stop","ClientStatus":"failed","ClientDescription":"Failed tasks","TaskStates":{"brain":{"State":"dead","Failed":true,"Events":[{"Type":"Driver Failure","DriverError":"invalid mount config: bind source path does not exist: /mnt/data/supervisor/share/coverage/brain"}]}}}}}]}`,
			},
			reg: &mockPRRegistry{
				deployFailedJobTargetID: "1555405874565091380",
				deployFailedJobPRNum:    613,
				deployFailedJobMergeSHA: "7cbc1f9b1234567890",
				deployFailedJobRepo:     "azylman/aerial",
				deployFailedJobUpdated:  true,
			},
			wantFailedJobs: []string{"brain"},
			wantPrompts:    1,
			wantPromptSubstr: []string{
				"Nomad deployment for job brain failed",
				"bind source path does not exist: /mnt/data/supervisor/share/coverage/brain",
			},
			wantDMs:      1,
			wantDMSubstr: "💥 **(PR: #613, repo: aerial)** Nomad deployment failed for brain. Following up...",
		},
		{
			name: "evaluation queued allocations ignored",
			streamLines: []string{
				`{"Index":2020,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-queued-1","JobID":"scheduler-mcp","Status":"complete","DeploymentID":"","FailedTGAllocs":{"scheduler-mcp":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"scheduler-mcp":1}}}}]}`,
			},
			reg: &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name: "evaluation multi-task-group unqueued failure treated as failure",
			streamLines: []string{
				`{"Index":2021,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-multi-fail","JobID":"brain","Status":"complete","DeploymentID":"","FailedTGAllocs":{"taskGroupA":{"ResourceExhausted":1}},"QueuedAllocations":{"taskGroupB":1}}}}]}`,
			},
			reg: &mockPRRegistry{
				deployFailedJobTargetID: "1555405874565091380",
				deployFailedJobPRNum:    630,
				deployFailedJobMergeSHA: "sha_multi_tg_fail",
				deployFailedJobRepo:     "azylman/aerial",
				deployFailedJobUpdated:  true,
			},
			wantFailedJobs:   []string{"brain"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"failed task group allocations"},
			wantDMs:          1,
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
			name: "stale allocation failure ignored when job already healthy",
			streamLines: []string{
				`{"Index":2040,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"old-dead-alloc","JobID":"github-mcp","DesiredStatus":"stop","ClientStatus":"failed","ClientDescription":"Failed tasks"}}}]}`,
			},
			deploymentsJSON: `[{"ID":"dep-succ-1","Status":"successful","StatusDescription":"Deployment completed successfully","JobVersion":10,"CreateIndex":22255}]`,
			reg:             &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name: "evaluation blocked status ignored",
			streamLines: []string{
				`{"Index":2050,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"995a3489-6ff6-c3aa-6541-0175856c6e84","JobID":"hangar","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"3b3eb97e-6077-6951-6489-5dbe1dfdbb6f"}}}]}`,
			},
			reg: &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name: "evaluation complete with BlockedEval ignored",
			streamLines: []string{
				`{"Index":2051,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"033659ef-f967-628b-9bd2-f1b76f646261","JobID":"hangar","Status":"complete","BlockedEval":"995a3489-6ff6-c3aa-6541-0175856c6e84","DeploymentID":"3b3eb97e-6077-6951-6489-5dbe1dfdbb6f","FailedTGAllocs":{"hangar":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"hangar":1}}}}]}`,
			},
			reg: &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name: "evaluation mixed state blocked with unqueued failure treated as failure",
			streamLines: []string{
				`{"Index":2052,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-mixed-fail","JobID":"brain","Status":"complete","BlockedEval":"eval-blocked-b","FailedTGAllocs":{"groupA":{"DimensionExhausted":{"network: port collision":1}},"groupB":{"ConstraintFiltered":1}},"QueuedAllocations":{"groupA":1,"groupB":0}}}}]}`,
			},
			reg: &mockPRRegistry{
				deployFailedJobTargetID: "1555405874565091380",
				deployFailedJobPRNum:    637,
				deployFailedJobMergeSHA: "sha_mixed_fail",
				deployFailedJobRepo:     "azylman/aerial",
				deployFailedJobUpdated:  true,
			},
			wantFailedJobs: []string{"brain"},
			wantPrompts:    1,
			wantDMs:        1,
		},
		{
			name: "evaluation blocked with FailedTGAllocs ignored",
			streamLines: []string{
				`{"Index":2053,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"bda9b66d-3cef-17a0-fce1-4faec0f3a52d","JobID":"webhooks-router","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"5622959a-4da3-fb28-09c7-5f6909503687","FailedTGAllocs":{"webhooks-router":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"webhooks-router":0}}}}]}`,
			},
			reg: &mockPRRegistry{deployFailedJobUpdated: true},
		},
		{
			name: "allocation restart exhaustion triggers tracked PR failure",
			streamLines: []string{
				`{"Index":2050,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-exhausted-1","JobID":"infisical","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"server":{"State":"pending","Events":[{"Type":"Terminated","ExitCode":1,"Message":"Docker container exited with non-zero exit code: 1"},{"Type":"Restarting","RestartReason":"Exceeded allowed attempts, applying a delay"}]}}}}}]}`,
			},
			deploymentsJSON: `[{"ID":"dep-old-1","Status":"successful","StatusDescription":"Deployment completed successfully","JobVersion":5,"CreateIndex":100}]`,
			reg: &mockPRRegistry{
				deployFailedJobTargetID: "1555405874565091380",
				deployFailedJobPRNum:    667,
				deployFailedJobMergeSHA: "sha-infisical-test",
				deployFailedJobRepo:     "azylman/aerial",
				deployFailedJobUpdated:  true,
			},
			wantFailedJobs:   []string{"infisical"},
			wantPrompts:      1,
			wantPromptSubstr: []string{"infisical", `task "server"`, "exited with code 1"},
			wantDMs:          1,
		},
		{
			name: "allocation restart exhaustion triggers unmanaged crash alert",
			streamLines: []string{
				`{"Index":2051,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-exhausted-unmanaged","JobID":"infisical","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"server":{"State":"pending","Events":[{"Type":"Terminated","ExitCode":1,"Message":"Docker container exited with non-zero exit code: 1"},{"Type":"Restarting","RestartReason":"Exceeded allowed attempts, applying a delay"}]}}}}}]}`,
			},
			reg:              &mockPRRegistry{deployFailedJobUpdated: false},
			systemChannelID:  "1555405874565091380",
			wantFailedJobs:   []string{"infisical"},
			wantPrompts:      1,
			wantPromptSubstr: []string{`task "server"`, "exited with code 1"},
			wantDMs:          1,
			wantDMSubstr:     "💥 Nomad job `infisical` crashed or failed. Following up...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

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
			for _, subStr := range tt.wantPromptSubstr {
				if len(pCalls) == 0 || !strings.Contains(pCalls[0].Prompt, subStr) {
					t.Errorf("prompt missing %q in %q", subStr, pCalls[0].Prompt)
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
		name      string
		topic     string
		eventType string
		payload   string
	}{
		{
			name:      "PlanResult dropped",
			topic:     "Allocation",
			eventType: "PlanResult",
			payload:   `{"Allocation":{"ID":"alloc-1","JobID":"github-mcp","ClientStatus":"failed"}}`,
		},
		{
			name:      "blocked evaluation returns nil",
			topic:     "Evaluation",
			eventType: "EvaluationUpdated",
			payload:   `{"Evaluation":{"ID":"eval-blocked-direct","JobID":"hangar","Status":"blocked","StatusDescription":"created to place remaining allocations"}}`,
		},
		{
			name:      "blocked evaluation with FailedTGAllocs returns nil",
			topic:     "Evaluation",
			eventType: "EvaluationUpdated",
			payload:   `{"Evaluation":{"ID":"eval-blocked-direct","JobID":"webhooks-router","Status":"blocked","StatusDescription":"created to place remaining allocations","FailedTGAllocs":{"webhooks-router":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"webhooks-router":0}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReg := &mockPRRegistry{deployFailedJobUpdated: true}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{}, nil)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			if err := srv.ProcessNomadEvent(context.Background(), tt.topic, tt.eventType, json.RawMessage(tt.payload)); err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
			if len(mockReg.deployFailedJobCalls) != 0 {
				t.Fatalf("expected 0 deployFailedJobCalls, got %v", mockReg.deployFailedJobCalls)
			}
		})
	}
}

func TestNomadStreamSubscriber_IndexResetOnOutOfBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attemptCount int
	var mu sync.Mutex

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attemptCount++
		attempt := attemptCount
		mu.Unlock()

		if attempt == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"index 999999 is out of bounds"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}))
	defer nomadServer.Close()

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)
	sub := NewNomadStreamSubscriber(srv)
	sub.lastIndex = 999999

	if err := sub.consumeStream(ctx); err == nil {
		t.Fatalf("expected error on attempt 1, got nil")
	}

	sub.mu.Lock()
	resetIndex := sub.lastIndex
	sub.mu.Unlock()
	if resetIndex != 0 {
		t.Errorf("expected lastIndex to be reset to 0, got %d", resetIndex)
	}

	if err := sub.consumeStream(ctx); err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("unexpected error on attempt 2: %v", err)
	}
}

func TestRouterServer_ReconcileActiveDeployments_StartupSweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/v1/job/webhooks-router/allocations") {
			_, _ = w.Write([]byte(`[
				{"ID":"alloc-old","JobID":"webhooks-router","DesiredStatus":"stop","ClientStatus":"complete"},
				{"ID":"alloc-new","JobID":"webhooks-router","DesiredStatus":"run","ClientStatus":"running"}
			]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		listDeployingPRs: []DeployingPR{
			{
				ID:       101,
				Repo:     "azylman/aerial",
				PRNumber: 552,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				TargetID: "1555405874565091380",
				Metadata: map[string]interface{}{"jobs": []interface{}{"webhooks-router"}},
			},
		},
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    552,
		deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
		deployedRepo:     "azylman/aerial",
		deployedUpdated:  true,
	}
	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(&mockOutboundDispatcher{})

	if err := srv.ReconcileActiveDeployments(ctx); err != nil {
		t.Fatalf("ReconcileActiveDeployments failed: %v", err)
	}
	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "webhooks-router" {
		t.Fatalf("expected deployedJobCalls [webhooks-router], got %v", mockReg.deployedJobCalls)
	}
}

func TestRouterServer_IsNomadJobHealthy_TableDriven(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var (
		deploymentsJSON  string
		deployments404   bool
		allocsJSON       string
		jobSpecJSON      string
		jobSpecStatus    int
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

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)

	tests := []struct {
		name           string
		job            string
		deploysJSON    string
		deploys404     bool
		allocs         string
		jobSpec        string
		jobStatus      int
		wantHealthy    bool
	}{
		{
			name:        "deployment running is unhealthy",
			job:         "brain",
			deploysJSON: `[{"ID":"dep-1","JobID":"brain","Status":"running"}]`,
			wantHealthy: false,
		},
		{
			name:        "deployment successful is healthy",
			job:         "brain",
			deploysJSON: `[{"ID":"dep-1","JobID":"brain","Status":"successful"}]`,
			wantHealthy: true,
		},
		{
			name:        "no deployments and allocation pending is unhealthy",
			job:         "brain",
			deploys404:  true,
			allocs:      `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"pending"}]`,
			wantHealthy: false,
		},
		{
			name:        "no deployments and allocation running is healthy",
			job:         "brain",
			deploys404:  true,
			allocs:      `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"running"}]`,
			wantHealthy: true,
		},
		{
			name:        "deployments out of order newest successful is healthy",
			job:         "brain",
			deploysJSON: `[{"ID":"dep-v14-failed","JobID":"brain","Status":"failed","JobVersion":14,"CreateIndex":19265},{"ID":"dep-v16-success","JobID":"brain","Status":"successful","JobVersion":16,"CreateIndex":19397},{"ID":"dep-v15-success","JobID":"brain","Status":"successful","JobVersion":15,"CreateIndex":19349}]`,
			wantHealthy: true,
		},
		{
			name:        "auto-revert higher JobVersion failed is unhealthy",
			job:         "brain",
			deploysJSON: `[{"ID":"dep-revert-v16","JobID":"brain","Status":"successful","JobVersion":16,"CreateIndex":20500},{"ID":"dep-v17-failed","JobID":"brain","Status":"failed","JobVersion":17,"CreateIndex":20400}]`,
			wantHealthy: false,
		},
		{
			name:        "auto-revert indicated in StatusDescription is unhealthy",
			job:         "brain",
			deploysJSON: `[{"ID":"dep-v17-roll","JobID":"brain","Status":"successful","JobVersion":17,"CreateIndex":20600,"StatusDescription":"Rolling back to version 16"}]`,
			wantHealthy: false,
		},
		{
			name:        "newer deployment failed highest CreateIndex is unhealthy",
			job:         "brain",
			deploysJSON: `[{"ID":"dep-v15-success","JobID":"brain","Status":"successful","JobVersion":15,"CreateIndex":19349},{"ID":"dep-v16-failed","JobID":"brain","Status":"failed","JobVersion":16,"CreateIndex":19397}]`,
			wantHealthy: false,
		},
		{
			name:        "periodic batch job with 0 allocations running and enabled is healthy",
			job:         "coverage-ingest",
			deploysJSON: `[]`,
			allocs:      `[]`,
			jobSpec:     `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`,
			jobStatus:   http.StatusOK,
			wantHealthy: true,
		},
		{
			name:        "periodic batch job with completed past allocations is healthy",
			job:         "coverage-ingest",
			deploysJSON: `[]`,
			allocs:      `[{"ID":"a1","DesiredStatus":"stop","ClientStatus":"complete"}]`,
			jobSpec:     `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`,
			jobStatus:   http.StatusOK,
			wantHealthy: true,
		},
		{
			name:        "periodic batch job with actively failing allocation is unhealthy",
			job:         "coverage-ingest",
			deploysJSON: `[]`,
			allocs:      `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"failed"}]`,
			jobSpec:     `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`,
			jobStatus:   http.StatusOK,
			wantHealthy: false,
		},
		{
			name:        "parameterized batch job with 0 allocations running is healthy",
			job:         "batch-worker",
			deploysJSON: `[]`,
			allocs:      `[]`,
			jobSpec:     `{"ID":"batch-worker","Type":"batch","Status":"running","Stop":false,"Periodic":null,"ParameterizedJob":{"Payload":"optional"}}`,
			jobStatus:   http.StatusOK,
			wantHealthy: true,
		},
		{
			name:        "non-periodic batch job with 0 allocations is unhealthy",
			job:         "migrate",
			deploysJSON: `[]`,
			allocs:      `[]`,
			jobSpec:     `{"ID":"migrate","Type":"batch","Status":"running","Stop":false,"Periodic":null,"ParameterizedJob":null}`,
			jobStatus:   http.StatusOK,
			wantHealthy: false,
		},
		{
			name:        "stopped periodic batch job is unhealthy",
			job:         "coverage-ingest",
			deploysJSON: `[]`,
			allocs:      `[]`,
			jobSpec:     `{"ID":"coverage-ingest","Type":"batch","Status":"dead","Stop":true,"Periodic":{"Enabled":true}}`,
			jobStatus:   http.StatusOK,
			wantHealthy: false,
		},
		{
			name:        "disabled periodic batch job is unhealthy",
			job:         "coverage-ingest",
			deploysJSON: `[]`,
			allocs:      `[]`,
			jobSpec:     `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":false}}`,
			jobStatus:   http.StatusOK,
			wantHealthy: false,
		},
		{
			name:        "batch job spec API 500 error is unhealthy",
			job:         "coverage-ingest",
			deploysJSON: `[]`,
			allocs:      `[]`,
			jobStatus:   http.StatusInternalServerError,
			wantHealthy: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deploymentsJSON = tt.deploysJSON
			deployments404 = tt.deploys404
			allocsJSON = tt.allocs
			jobSpecJSON = tt.jobSpec
			jobSpecStatus = tt.jobStatus

			if got := srv.isNomadJobHealthy(ctx, tt.job); got != tt.wantHealthy {
				t.Errorf("isNomadJobHealthy(%q) = %v, want %v", tt.job, got, tt.wantHealthy)
			}
		})
	}
}

func TestNomadStreamSubscriber_RiverQueue(t *testing.T) {
	t.Run("filters heartbeats and intermediate states before inserting terminal events", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		streamData := strings.Join([]string{
			"{}",
			`{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-run","JobID":"brain","Status":"running"}}}]}`,
			`{"Index":2002,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-pend","JobID":"brain","DesiredStatus":"run","ClientStatus":"pending"}}}]}`,
			`{"Index":2003,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-succ","JobID":"brain","Status":"successful"}}}]}`,
			`{"Index":2004,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-run","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"running"}}}]}`,
			`{"Index":2005,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-fail","JobID":"discord-mcp","Status":"failed","StatusDescription":"oom"}}}]}`,
			`{"Index":2010,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-dead-1","JobID":"brain","DesiredStatus":"stop","ClientStatus":"failed"}}}]}`,
			"{}",
		}, "\n") + "\n"

		nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(streamData))
		}))
		defer nomadServer.Close()

		mockRiver := &mockRiverInserter{}
		srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil, mockRiver)
		sub := NewNomadStreamSubscriber(srv)
		sub.SetReconnectDelay(10*time.Millisecond, 50*time.Millisecond)

		if err := sub.consumeStream(ctx); err != nil && !strings.Contains(err.Error(), "EOF") {
			t.Fatalf("unexpected stream error: %v", err)
		}

		if len(mockRiver.insertedNomadJobs) != 4 {
			t.Fatalf("expected 4 nomad jobs inserted into river, got %d: %+v", len(mockRiver.insertedNomadJobs), mockRiver.insertedNomadJobs)
		}
		if mockRiver.insertedNomadJobs[0].Topic != "Deployment" || !strings.Contains(string(mockRiver.insertedNomadJobs[0].Payload), "dep-succ") {
			t.Errorf("unexpected first inserted job: %+v", mockRiver.insertedNomadJobs[0])
		}
		if mockRiver.insertedNomadJobs[1].Topic != "Allocation" || !strings.Contains(string(mockRiver.insertedNomadJobs[1].Payload), "alloc-run") {
			t.Errorf("unexpected second inserted job: %+v", mockRiver.insertedNomadJobs[1])
		}
		if mockRiver.insertedNomadJobs[2].Topic != "Deployment" || !strings.Contains(string(mockRiver.insertedNomadJobs[2].Payload), "dep-fail") {
			t.Errorf("unexpected third inserted job: %+v", mockRiver.insertedNomadJobs[2])
		}
		if mockRiver.insertedNomadJobs[3].Topic != "Allocation" || !strings.Contains(mockRiver.insertedNomadJobs[3].IdempotencyKey, ":failed") {
			t.Errorf("unexpected fourth inserted job: %+v", mockRiver.insertedNomadJobs[3])
		}

		for i, opts := range mockRiver.insertedOpts {
			if opts == nil || opts.MaxAttempts != 5 || !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
				t.Errorf("job %d unexpected river insert opts: %+v", i, opts)
			}
		}
	})

	t.Run("duplicate nomad event increments skipped metric", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		streamData := `{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-dup-1","JobID":"brain","Status":"successful"}}}]}` + "\n"
		nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(streamData))
		}))
		defer nomadServer.Close()

		mockRiver := &mockRiverInserter{skipDuplicate: true}
		srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil, mockRiver)
		sub := NewNomadStreamSubscriber(srv)

		if err := sub.consumeStream(ctx); err != nil && !strings.Contains(err.Error(), "EOF") {
			t.Fatalf("unexpected stream error: %v", err)
		}

		reqM := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		recM := httptest.NewRecorder()
		srv.Routes().ServeHTTP(recM, reqM)
		if !strings.Contains(recM.Body.String(), `webhooks_router_unique_skipped_total{kind="nomad_event"}`) {
			t.Errorf("expected /metrics to include nomad_event duplicate count, got:\n%s", recM.Body.String())
		}
	})
}

func TestIsAllocationRestarting_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		taskStates map[string]NomadTaskState
		want       bool
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAllocationRestarting(tt.taskStates); got != tt.want {
				t.Errorf("isAllocationRestarting() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTaskAndAllocationRestartExhausted_TableDriven(t *testing.T) {
	taskTests := []struct {
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

	for _, tt := range taskTests {
		t.Run("task/"+tt.name, func(t *testing.T) {
			if got := isTaskRestartExhausted(tt.ts); got != tt.want {
				t.Errorf("isTaskRestartExhausted() = %v, want %v", got, tt.want)
			}
		})
	}

	allocTests := []struct {
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

	for _, tt := range allocTests {
		t.Run("alloc/"+tt.name, func(t *testing.T) {
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
	ctx := context.Background()

	// 1. HandleUnmanagedJobCrash with riverClient != nil -> enqueues NomadCrashAlertArgs
	mockRiver := &mockRiverInserter{}
	srvRiver := NewRouterServer(Config{SystemChannelID: "1555405874565091380"}, nil, mockRiver)
	if err := srvRiver.HandleUnmanagedJobCrash(ctx, "openobserve", "alloc-1", "failed", "OOMKilled"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(mockRiver.insertedCrashAlertJobs) != 1 || mockRiver.insertedCrashAlertJobs[0].JobID != "openobserve" || mockRiver.insertedCrashAlertJobs[0].AllocID != "alloc-1" {
		t.Fatalf("unexpected crash alert jobs: %+v", mockRiver.insertedCrashAlertJobs)
	}

	// 2. HandleUnmanagedJobCrash with riverClient != nil and skipDuplicate == true -> increments metric
	mockRiverDup := &mockRiverInserter{skipDuplicate: true}
	srvRiverDup := NewRouterServer(Config{SystemChannelID: "1555405874565091380"}, nil, mockRiverDup)
	if err := srvRiverDup.HandleUnmanagedJobCrash(ctx, "openobserve", "alloc-2", "failed", "OOMKilled"); err != nil {
		t.Fatalf("expected nil error on duplicate crash, got %v", err)
	}
	reqM := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recM := httptest.NewRecorder()
	srvRiverDup.Routes().ServeHTTP(recM, reqM)
	if !strings.Contains(recM.Body.String(), `webhooks_router_unique_skipped_total{kind="nomad_crash_alert"}`) {
		t.Errorf("expected /metrics to include nomad_crash_alert duplicate count, got:\n%s", recM.Body.String())
	}

	// 3. HandleUnmanagedJobCrash direct execution and empty job ID no-op
	mockDisp := &mockOutboundDispatcher{}
	srvDirect := NewRouterServer(Config{SystemChannelID: "1555405874565091380"}, nil)
	srvDirect.SetDispatcher(mockDisp)
	if err := srvDirect.HandleUnmanagedJobCrash(ctx, "openobserve", "alloc-3", "failed", "signal: killed"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(mockDisp.DirectMessageCalls()) != 1 || len(mockDisp.PromptCalls()) != 1 {
		t.Errorf("expected 1 DM and 1 prompt call on direct crash alert")
	}
	if err := srvDirect.HandleUnmanagedJobCrash(ctx, "", "", "", ""); err != nil {
		t.Errorf("expected nil error on empty job ID, got %v", err)
	}

	// 4. HandleNomadJobFailure delegates to HandleUnmanagedJobCrash when registry returns updated=false or registry is nil
	for _, tc := range []struct {
		name string
		reg  PRRegistryUpdater
		job  string
		desc string
	}{
		{name: "untracked in registry", reg: &mockPRRegistry{deployFailedJobUpdated: false}, job: "openobserve", desc: "Task OOM killed"},
		{name: "nil registry", reg: nil, job: "cadvisor", desc: "Docker daemon died"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disp := &mockOutboundDispatcher{}
			srv := NewRouterServer(Config{SystemChannelID: "1555405874565091380"}, nil)
			srv.SetRegistry(tc.reg)
			srv.SetDispatcher(disp)

			if err := srv.HandleNomadJobFailure(ctx, tc.job, "alloc-99", "failed", tc.desc); err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if len(disp.DirectMessageCalls()) != 1 || !strings.Contains(disp.DirectMessageCalls()[0].Content, tc.job) {
				t.Errorf("unexpected DM calls: %+v", disp.DirectMessageCalls())
			}
			if len(disp.PromptCalls()) != 1 || !strings.Contains(disp.PromptCalls()[0].Prompt, tc.desc) {
				t.Errorf("unexpected prompt calls: %+v", disp.PromptCalls())
			}
		})
	}
}
