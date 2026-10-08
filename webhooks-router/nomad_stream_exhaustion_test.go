package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIsTaskRestartExhausted_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		ts   NomadTaskState
		want bool
	}{
		{
			name: "running task state is never exhausted",
			ts: NomadTaskState{
				State: "running",
				Events: []NomadTaskEvent{
					{Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"},
				},
			},
			want: false,
		},
		{
			name: "nil or empty events",
			ts:   NomadTaskState{State: "pending"},
			want: false,
		},
		{
			name: "transient restart within policy",
			ts: NomadTaskState{
				State: "pending",
				Events: []NomadTaskEvent{
					{Type: "Terminated", ExitCode: 1},
					{Type: "Restarting", RestartReason: "Restart within policy"},
				},
			},
			want: false,
		},
		{
			name: "exhausted attempts mode delay",
			ts: NomadTaskState{
				State: "pending",
				Events: []NomadTaskEvent{
					{Type: "Terminated", ExitCode: 1},
					{Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"},
				},
			},
			want: true,
		},
		{
			name: "exhausted attempts mode fail",
			ts: NomadTaskState{
				State: "dead",
				Events: []NomadTaskEvent{
					{Type: "Terminated", ExitCode: 1},
					{Type: "Not Restarting", RestartReason: `Exceeded allowed attempts 5 in interval 10m0s and mode is "fail"`},
				},
			},
			want: true,
		},
		{
			name: "subsequent recovery with Started event resets exhaustion",
			ts: NomadTaskState{
				State: "pending",
				Events: []NomadTaskEvent{
					{Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"},
					{Type: "Started"},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTaskRestartExhausted(tt.ts)
			if got != tt.want {
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
		{
			name:       "nil task states",
			taskStates: nil,
			want:       false,
		},
		{
			name: "healthy multi-task allocation",
			taskStates: map[string]NomadTaskState{
				"redis":  {State: "running"},
				"server": {State: "running"},
			},
			want: false,
		},
		{
			name: "multi-task allocation with one exhausted task",
			taskStates: map[string]NomadTaskState{
				"redis": {State: "running"},
				"server": {
					State: "pending",
					Events: []NomadTaskEvent{
						{Type: "Terminated", ExitCode: 1},
						{Type: "Restarting", RestartReason: "Exceeded allowed attempts, applying a delay"},
					},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAllocationRestartExhausted(tt.taskStates)
			if got != tt.want {
				t.Errorf("isAllocationRestartExhausted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNomadStreamSubscriber_AllocationRestartExhaustion_TriggersFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := `{"Index":2050,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-exhausted-1","JobID":"infisical","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"server":{"State":"pending","Events":[{"Type":"Terminated","ExitCode":1,"Message":"Docker container exited with non-zero exit code: 1"},{"Type":"Restarting","RestartReason":"Exceeded allowed attempts, applying a delay"}]}}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-old-1","Status":"successful","StatusDescription":"Deployment completed successfully","JobVersion":5,"CreateIndex":100}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    667,
		deployFailedJobMergeSHA: "sha-infisical-test",
		deployFailedJobRepo:     "azylman/aerial",
		deployFailedJobUpdated:  true,
	}
	mockDisp := &mockOutboundDispatcher{}

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	sub := NewNomadStreamSubscriber(srv)
	sub.SetReconnectDelay(10*time.Millisecond, 50*time.Millisecond)

	err := sub.consumeStream(ctx)
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("unexpected stream error: %v", err)
	}

	if len(mockReg.deployFailedJobCalls) != 1 {
		t.Fatalf("expected 1 deployFailedJobCalls on restart exhaustion, got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockDisp.DirectMessageCalls()) != 1 {
		t.Fatalf("expected 1 DM call on restart exhaustion, got %d", len(mockDisp.DirectMessageCalls()))
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call on restart exhaustion, got %d", len(mockDisp.PromptCalls()))
	}
	prompt := mockDisp.PromptCalls()[0].Prompt
	if !strings.Contains(prompt, "infisical") || !strings.Contains(prompt, "task \"server\"") || !strings.Contains(prompt, "exited with code 1") {
		t.Errorf("prompt missing task failure diagnostics: %s", prompt)
	}
}

func TestNomadStreamSubscriber_AllocationRestartExhaustion_UnmanagedCrash(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := `{"Index":2051,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-exhausted-unmanaged","JobID":"infisical","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"server":{"State":"pending","Events":[{"Type":"Terminated","ExitCode":1,"Message":"Docker container exited with non-zero exit code: 1"},{"Type":"Restarting","RestartReason":"Exceeded allowed attempts, applying a delay"}]}}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobUpdated: false, // not tracked in pr_registry (unmanaged)
	}
	mockDisp := &mockOutboundDispatcher{}

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL, SystemChannelID: "1555405874565091380"}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	sub := NewNomadStreamSubscriber(srv)
	sub.SetReconnectDelay(10*time.Millisecond, 50*time.Millisecond)

	err := sub.consumeStream(ctx)
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("unexpected stream error: %v", err)
	}

	if len(mockDisp.DirectMessageCalls()) != 1 {
		t.Fatalf("expected 1 DM call for unmanaged crash, got %d", len(mockDisp.DirectMessageCalls()))
	}
	if !strings.Contains(mockDisp.DirectMessageCalls()[0].Content, "💥 Nomad job `infisical` crashed or failed. Following up...") {
		t.Errorf("unexpected DM content: %s", mockDisp.DirectMessageCalls()[0].Content)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call for unmanaged crash, got %d", len(mockDisp.PromptCalls()))
	}
	prompt := mockDisp.PromptCalls()[0].Prompt
	if !strings.Contains(prompt, "task \"server\"") || !strings.Contains(prompt, "exited with code 1") {
		t.Errorf("expected prompt error diagnostics: %s", prompt)
	}
}

func TestExtractAllocationErrorDetails_RestartExhaustedTask(t *testing.T) {
	taskStates := map[string]NomadTaskState{
		"server": {
			State:  "pending",
			Failed: false,
			Events: []NomadTaskEvent{
				{
					Type:           "Terminated",
					ExitCode:       1,
					DisplayMessage: "Docker container exited with non-zero exit code: 1",
				},
				{
					Type:          "Restarting",
					RestartReason: "Exceeded allowed attempts, applying a delay",
				},
			},
		},
	}

	desc := extractAllocationErrorDetails("Tasks are running", taskStates)
	if !strings.Contains(desc, "task \"server\"") || !strings.Contains(desc, "exited with code 1") {
		t.Errorf("expected error details to extract failure from restart-exhausted task, got: %s", desc)
	}
}
