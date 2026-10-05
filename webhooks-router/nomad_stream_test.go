package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		{
			name:     "nil metadata",
			metadata: nil,
			expected: nil,
		},
		{
			name: "jobs array string",
			metadata: map[string]interface{}{
				"jobs": []interface{}{"brain", "scheduler-mcp"},
			},
			expected: []string{"brain", "scheduler-mcp"},
		},
		{
			name: "single job string",
			metadata: map[string]interface{}{
				"job": "hangar",
			},
			expected: []string{"hangar"},
		},
		{
			name: "both jobs and job",
			metadata: map[string]interface{}{
				"jobs": []interface{}{"brain"},
				"job":  "hangar",
			},
			expected: []string{"brain", "hangar"},
		},
		{
			name: "deduplicates clean job names",
			metadata: map[string]interface{}{
				"jobs": []interface{}{"brain", "brain", "hangar"},
			},
			expected: []string{"brain", "hangar"},
		},
		{
			name: "deduplicates duplicate job names in jobs and job",
			metadata: map[string]interface{}{
				"jobs": []interface{}{"brain", "hangar"},
				"job":  "brain",
			},
			expected: []string{"brain", "hangar"},
		},
		{
			name: "empty jobs array",
			metadata: map[string]interface{}{
				"jobs": []interface{}{},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJobsFromMetadata(tt.metadata)
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %d jobs, got %d: %v", len(tt.expected), len(got), got)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("job[%d] = %q, expected %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestNomadStreamSubscriber_HeartbeatAndDeployment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := strings.Join([]string{
		"{}",
		"",
		`{"Index":1001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-123","JobID":"brain","Status":"successful","StatusDescription":"Deployment completed successfully"}}}]}`,
		"{}",
	}, "\n") + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-1","Status":"successful"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(`[{"ID":"alloc-999","DesiredStatus":"run","ClientStatus":"running"}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    552,
		deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
		deployedRepo:     "azylman/aerial",
		deployedUpdated:  true,
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

	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "brain" {
		t.Fatalf("expected deployedJobCalls [brain], got %v", mockReg.deployedJobCalls)
	}

	// Verify prompt was dispatched (NOT direct message)
	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(pCalls))
	}
	if pCalls[0].ChannelID != "1555405874565091380" {
		t.Errorf("expected target ID 1555405874565091380, got %s", pCalls[0].ChannelID)
	}
	if !strings.Contains(pCalls[0].Prompt, "Continuous Delivery deployment completed for job brain") {
		t.Errorf("unexpected prompt content: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "Directive: Based on the recent conversation context above") {
		t.Errorf("expected directive in prompt: %s", pCalls[0].Prompt)
	}
	if len(mockDisp.DirectMessageCalls()) != 0 {
		t.Fatalf("expected 0 direct message calls on success prompt, got %d", len(mockDisp.DirectMessageCalls()))
	}
}

func TestNomadStreamSubscriber_AllocationRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := `{"Index":1002,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-999","JobID":"webhooks-router","DesiredStatus":"run","ClientStatus":"running"}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-1","Status":"successful"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(`[{"ID":"alloc-999","DesiredStatus":"run","ClientStatus":"running"}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    552,
		deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
		deployedRepo:     "azylman/aerial",
		deployedUpdated:  true,
		channelContextMap: map[string][]ChannelMessageContext{
			"1555405874565091380": {
				{AuthorName: "arcane103", Content: "Ship it", ResponseText: "Shipping!"},
			},
		},
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

	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "webhooks-router" {
		t.Fatalf("expected deployedJobCalls [webhooks-router], got %v", mockReg.deployedJobCalls)
	}

	// Verify prompt with context was dispatched
	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(pCalls))
	}
	if pCalls[0].ChannelID != "1555405874565091380" {
		t.Errorf("expected target ID 1555405874565091380, got %s", pCalls[0].ChannelID)
	}
	if !strings.Contains(pCalls[0].Prompt, "Continuous Delivery deployment completed for job webhooks-router") {
		t.Errorf("unexpected prompt content: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "[arcane103]: Ship it") || !strings.Contains(pCalls[0].Prompt, "[Aerial]: Shipping!") {
		t.Errorf("expected channel context in prompt: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "Directive: Based on the recent conversation context above") {
		t.Errorf("expected directive in prompt: %s", pCalls[0].Prompt)
	}
}

func TestNomadStreamSubscriber_DeploymentFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := strings.Join([]string{
		`{"Index":1003,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-fail-1","JobID":"brain","Status":"failed","StatusDescription":"Allocation failed health check: connection refused on port 8080"}}}]}`,
	}, "\n") + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    553,
		deployFailedJobMergeSHA: "sha123456",
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

	if len(mockReg.deployFailedJobCalls) != 1 || mockReg.deployFailedJobCalls[0] != "brain" {
		t.Fatalf("expected deployFailedJobCalls [brain], got %v", mockReg.deployFailedJobCalls)
	}

	// Verify prompt was dispatched (NOT direct message)
	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(pCalls))
	}
	if pCalls[0].ChannelID != "1555405874565091380" {
		t.Errorf("expected target ID 1555405874565091380, got %s", pCalls[0].ChannelID)
	}
	if !strings.Contains(pCalls[0].Prompt, "Nomad deployment for job brain failed") {
		t.Errorf("unexpected prompt content: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "Allocation failed health check: connection refused on port 8080") {
		t.Errorf("expected error details in prompt: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "Please investigate and fix the deployment failure.") {
		t.Errorf("expected directive in prompt: %s", pCalls[0].Prompt)
	}
	dmCalls := mockDisp.DirectMessageCalls()
	if len(dmCalls) != 1 {
		t.Fatalf("expected 1 direct message call on failure, got %d", len(dmCalls))
	}
	if dmCalls[0].ChannelID != "1555405874565091380" || !strings.Contains(dmCalls[0].Content, "💥 **(PR: #553, repo: aerial)** Nomad deployment failed for brain. Following up...") {
		t.Errorf("unexpected direct message content: %+v", dmCalls[0])
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
			// First call: index is out of bounds
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"index 999999 is out of bounds"}`))
			return
		}

		// Second call: index was reset, return valid stream
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}))
	defer nomadServer.Close()

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)
	sub := NewNomadStreamSubscriber(srv)
	sub.lastIndex = 999999

	err1 := sub.consumeStream(ctx)
	if err1 == nil {
		t.Fatalf("expected error on attempt 1, got nil")
	}

	sub.mu.Lock()
	resetIndex := sub.lastIndex
	sub.mu.Unlock()

	if resetIndex != 0 {
		t.Errorf("expected lastIndex to be reset to 0, got %d", resetIndex)
	}

	// Attempt 2 should succeed connecting without error
	err2 := sub.consumeStream(ctx)
	if err2 != nil && !strings.Contains(err2.Error(), "EOF") {
		t.Fatalf("unexpected error on attempt 2: %v", err2)
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
				Metadata: map[string]interface{}{
					"jobs": []interface{}{"webhooks-router"},
				},
			},
		},
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    552,
		deployedMergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
		deployedRepo:     "azylman/aerial",
		deployedUpdated:  true,
	}
	mockDisp := &mockOutboundDispatcher{}

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	if err := srv.ReconcileActiveDeployments(ctx); err != nil {
		t.Fatalf("ReconcileActiveDeployments failed: %v", err)
	}

	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "webhooks-router" {
		t.Fatalf("expected deployedJobCalls [webhooks-router], got %v", mockReg.deployedJobCalls)
	}
}

func TestRouterServer_IsNomadJobHealthy_DeploymentsAndAllocations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var deploymentStatus string
	var returnDeployments404 bool
	var allocsJSON string

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			if returnDeployments404 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`[{"ID":"dep-1","JobID":"brain","Status":"` + deploymentStatus + `"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(allocsJSON))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer nomadServer.Close()

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)

	// Case 1: Deployments endpoint has status "running" -> incomplete, return false
	deploymentStatus = "running"
	returnDeployments404 = false
	if srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be false when deployment is running")
	}

	// Case 2: Deployments endpoint has status "successful" -> complete, return true
	deploymentStatus = "successful"
	if !srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be true when deployment is successful")
	}

	// Case 3: No deployments endpoint, allocation is pending -> incomplete, return false
	returnDeployments404 = true
	allocsJSON = `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"pending"}]`
	if srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be false when allocation is pending")
	}

	// Case 4: No deployments endpoint, allocation is running -> complete, return true
	allocsJSON = `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"running"}]`
	if !srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be true when allocation is running")
	}
}

func TestRouterServer_IsNomadJobHealthy_BatchTemplates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var jobSpecJSON string
	var jobSpecStatus int
	var allocsJSON string

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[]`))
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

	// Case 1: Periodic batch job with 0 allocations, running & enabled -> healthy
	allocsJSON = `[]`
	jobSpecJSON = `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":true}}`
	jobSpecStatus = http.StatusOK
	if !srv.isNomadJobHealthy(ctx, "coverage-ingest") {
		t.Errorf("expected periodic batch job with 0 allocations to be healthy")
	}

	// Case 2: Periodic batch job with completed past allocations -> healthy
	allocsJSON = `[{"ID":"a1","DesiredStatus":"stop","ClientStatus":"complete"}]`
	if !srv.isNomadJobHealthy(ctx, "coverage-ingest") {
		t.Errorf("expected periodic batch job with completed past allocations to be healthy")
	}

	// Case 3: Periodic batch job with actively failing allocation -> unhealthy
	allocsJSON = `[{"ID":"a1","DesiredStatus":"run","ClientStatus":"failed"}]`
	if srv.isNomadJobHealthy(ctx, "coverage-ingest") {
		t.Errorf("expected periodic batch job with actively failing allocation to be unhealthy")
	}

	// Case 4: Parameterized batch job with 0 allocations, running -> healthy
	allocsJSON = `[]`
	jobSpecJSON = `{"ID":"batch-worker","Type":"batch","Status":"running","Stop":false,"Periodic":null,"ParameterizedJob":{"Payload":"optional"}}`
	if !srv.isNomadJobHealthy(ctx, "batch-worker") {
		t.Errorf("expected parameterized batch job with 0 allocations to be healthy")
	}

	// Case 5: Non-periodic batch job (Periodic == null, ParameterizedJob == null) with 0 allocations -> unhealthy
	allocsJSON = `[]`
	jobSpecJSON = `{"ID":"migrate","Type":"batch","Status":"running","Stop":false,"Periodic":null,"ParameterizedJob":null}`
	if srv.isNomadJobHealthy(ctx, "migrate") {
		t.Errorf("expected non-periodic batch job with 0 allocations to be unhealthy")
	}

	// Case 6: Stopped periodic batch job (Stop == true / Status == "dead") -> unhealthy
	allocsJSON = `[]`
	jobSpecJSON = `{"ID":"coverage-ingest","Type":"batch","Status":"dead","Stop":true,"Periodic":{"Enabled":true}}`
	if srv.isNomadJobHealthy(ctx, "coverage-ingest") {
		t.Errorf("expected stopped periodic batch job to be unhealthy")
	}

	// Case 7: Periodic batch job with Periodic.Enabled == false -> unhealthy
	allocsJSON = `[]`
	jobSpecJSON = `{"ID":"coverage-ingest","Type":"batch","Status":"running","Stop":false,"Periodic":{"Enabled":false}}`
	if srv.isNomadJobHealthy(ctx, "coverage-ingest") {
		t.Errorf("expected disabled periodic batch job to be unhealthy")
	}

	// Case 8: Nomad job spec API 404/500 -> unhealthy
	allocsJSON = `[]`
	jobSpecStatus = http.StatusInternalServerError
	if srv.isNomadJobHealthy(ctx, "coverage-ingest") {
		t.Errorf("expected API error to be unhealthy")
	}
}

func TestNomadStreamSubscriber_WithRiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := strings.Join([]string{
		"{}", // heartbeat: noise, must be ignored
		`{"Index":2001,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-run","JobID":"brain","Status":"running"}}}]}`, // intermediate deployment status: noise, must be ignored
		`{"Index":2002,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-pend","JobID":"brain","DesiredStatus":"run","ClientStatus":"pending"}}}]}`, // non-running allocation: noise, must be ignored
		`{"Index":2003,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-succ","JobID":"brain","Status":"successful"}}}]}`, // terminal deployment success: must be enqueued
		`{"Index":2004,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-run","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"running"}}}]}`, // running allocation: must be enqueued
		`{"Index":2005,"Events":[{"Topic":"Deployment","Type":"DeploymentStatusUpdate","Payload":{"Deployment":{"ID":"dep-fail","JobID":"discord-mcp","Status":"failed","StatusDescription":"oom"}}}]}`, // terminal deployment failure: must be enqueued
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

	err := sub.consumeStream(ctx)
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("unexpected stream error: %v", err)
	}

	// Only 3 terminal/running events should be inserted into River: dep-succ, alloc-run, dep-fail.
	// Heartbeats and intermediate running/pending statuses are filtered out in memory before River.
	if len(mockRiver.insertedNomadJobs) != 3 {
		t.Fatalf("expected 3 nomad jobs inserted into river, got %d: %+v", len(mockRiver.insertedNomadJobs), mockRiver.insertedNomadJobs)
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

	// Verify all River inserts used MaxAttempts: 5 and ByArgs: true
	for i, opts := range mockRiver.insertedOpts {
		if opts == nil || opts.MaxAttempts != 5 {
			t.Errorf("job %d missing MaxAttempts: 5: %+v", i, opts)
		}
		if opts == nil || !opts.UniqueOpts.ByArgs {
			t.Errorf("job %d missing UniqueOpts.ByArgs: true: %+v", i, opts)
		}
		if opts == nil || opts.UniqueOpts.ByPeriod != 15*time.Minute {
			t.Errorf("job %d unexpected ByPeriod: %+v", i, opts)
		}
	}
}

func TestIsAllocationRestarting_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		taskStates map[string]NomadTaskState
		want       bool
	}{
		{
			name:       "nil or empty map",
			taskStates: nil,
			want:       false,
		},
		{
			name: "running task without events",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "running", Failed: false},
			},
			want: false,
		},
		{
			name: "pending task state",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "pending"},
			},
			want: true,
		},
		{
			name: "restarting task state",
			taskStates: map[string]NomadTaskState{
				"brain": {State: "restarting"},
			},
			want: true,
		},
		{
			name: "last event Restart Signaled",
			taskStates: map[string]NomadTaskState{
				"brain": {
					State: "running",
					Events: []NomadTaskEvent{
						{Type: "Started"},
						{Type: "Restart Signaled", DisplayMessage: "Template with change_mode restart re-rendered"},
					},
				},
			},
			want: true,
		},
		{
			name: "last event Restarting",
			taskStates: map[string]NomadTaskState{
				"brain": {
					State: "running",
					Events: []NomadTaskEvent{
						{Type: "Restart Signaled"},
						{Type: "Restarting"},
					},
				},
			},
			want: true,
		},
		{
			name: "last event Started after Restarting",
			taskStates: map[string]NomadTaskState{
				"brain": {
					State: "running",
					Events: []NomadTaskEvent{
						{Type: "Restart Signaled"},
						{Type: "Restarting"},
						{Type: "Started"},
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAllocationRestarting(tt.taskStates)
			if got != tt.want {
				t.Errorf("isAllocationRestarting() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNomadStreamSubscriber_ConfigReload_PromptDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := `{"Index":1003,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-cfg-1","JobID":"scheduler-mcp","DesiredStatus":"run","ClientStatus":"running"}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-1","Status":"successful"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(`[{"ID":"alloc-cfg-1","DesiredStatus":"run","ClientStatus":"running"}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    245,
		deployedMergeSHA: "sha_config_reload_abc",
		deployedRepo:     "azylman/aerial-config",
		deployedUpdated:  true,
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

	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "scheduler-mcp" {
		t.Fatalf("expected deployedJobCalls [scheduler-mcp], got %v", mockReg.deployedJobCalls)
	}

	// Verify prompt was dispatched uniformly across repos (NO direct messages!)
	if len(mockDisp.DirectMessageCalls()) != 0 {
		t.Errorf("expected 0 direct message calls, got %d", len(mockDisp.DirectMessageCalls()))
	}
	promptCalls := mockDisp.PromptCalls()
	if len(promptCalls) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(promptCalls))
	}
	if promptCalls[0].ChannelID != "1555405874565091380" {
		t.Errorf("expected target ID 1555405874565091380, got %s", promptCalls[0].ChannelID)
	}
	if !strings.Contains(promptCalls[0].Prompt, "Continuous Delivery deployment completed for job scheduler-mcp on azylman/aerial-config") {
		t.Errorf("unexpected prompt content: %s", promptCalls[0].Prompt)
	}
}

func TestNomadStreamSubscriber_Evaluation_InPlaceSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := `{"Index":1004,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-inplace-1","JobID":"brain","Status":"complete","DeploymentID":""}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-old-1","Status":"successful"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(`[{"ID":"alloc-brain-1","DesiredStatus":"run","ClientStatus":"running"}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    585,
		deployedMergeSHA: "sha_eval_success",
		deployedRepo:     "azylman/aerial",
		deployedUpdated:  true,
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

	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "brain" {
		t.Fatalf("expected deployedJobCalls [brain], got %v", mockReg.deployedJobCalls)
	}
	promptCalls := mockDisp.PromptCalls()
	if len(promptCalls) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(promptCalls))
	}
	if !strings.Contains(promptCalls[0].Prompt, "job brain on azylman/aerial") {
		t.Errorf("unexpected prompt content: %s", promptCalls[0].Prompt)
	}
}

func TestNomadStreamSubscriber_Evaluation_DeploymentManaged_Skipped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// An evaluation that spawned a deployment (DeploymentID != "") must be skipped by evaluation handler
	streamData := `{"Index":1005,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-managed-1","JobID":"brain","Status":"complete","DeploymentID":"dep-rolling-99"}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{}
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

	if len(mockReg.deployedJobCalls) != 0 {
		t.Fatalf("expected 0 deployedJobCalls, got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestNomadStreamSubscriber_Evaluation_PlacementFailedTGAllocs_TreatedAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// When scheduler cannot place allocations, eval status is "complete" but FailedTGAllocs is populated
	streamData := `{"Index":1006,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-fail-allocs","JobID":"brain","Status":"complete","DeploymentID":"","FailedTGAllocs":{"brain":{"AllocationResourceExhausted":1}}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    586,
		deployFailedJobMergeSHA: "sha_eval_fail",
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

	if len(mockReg.deployFailedJobCalls) != 1 || mockReg.deployFailedJobCalls[0] != "brain" {
		t.Fatalf("expected failedJobCalls [brain], got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 failure prompt call, got %d", len(mockDisp.PromptCalls()))
	}
	if !strings.Contains(mockDisp.PromptCalls()[0].Prompt, "failed with status \"complete\"") {
		t.Errorf("unexpected failure prompt content: %s", mockDisp.PromptCalls()[0].Prompt)
	}
	if len(mockDisp.DirectMessageCalls()) != 1 {
		t.Fatalf("expected 1 failure direct message call, got %d", len(mockDisp.DirectMessageCalls()))
	}
}

func TestNomadStreamSubscriber_Evaluation_StatusFailed_TreatedAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := `{"Index":1007,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-status-failed","JobID":"brain","Status":"failed","StatusDescription":"scheduling timeout","DeploymentID":""}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    587,
		deployFailedJobMergeSHA: "sha_eval_fail_status",
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

	if len(mockReg.deployFailedJobCalls) != 1 || mockReg.deployFailedJobCalls[0] != "brain" {
		t.Fatalf("expected failedJobCalls [brain], got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 failure prompt call, got %d", len(mockDisp.PromptCalls()))
	}
	if !strings.Contains(mockDisp.PromptCalls()[0].Prompt, "scheduling timeout") {
		t.Errorf("unexpected failure prompt content: %s", mockDisp.PromptCalls()[0].Prompt)
	}
}

func TestNomadStreamSubscriber_AllocationRestarting_Guarded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := `{"Index":1004,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-restart-1","JobID":"brain","DesiredStatus":"run","ClientStatus":"running","TaskStates":{"brain":{"State":"restarting"}}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-1","Status":"successful"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(`[{"ID":"alloc-restart-1","DesiredStatus":"run","ClientStatus":"running"}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{}
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

	// Should NOT call deployedJobCalls or dispatch anything while restarting
	if len(mockReg.deployedJobCalls) != 0 {
		t.Errorf("expected 0 deployedJobCalls while restarting, got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Errorf("expected 0 prompt calls, got %d", len(mockDisp.PromptCalls()))
	}
	if len(mockDisp.DirectMessageCalls()) != 0 {
		t.Errorf("expected 0 direct message calls, got %d", len(mockDisp.DirectMessageCalls()))
	}
}

func TestNomadStreamSubscriber_Evaluation_RestartWithDeploymentID_Succeeds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Evaluation with a non-empty DeploymentID (e.g. from an allocation restart / older spec)
	// should succeed when the job is verified healthy in Nomad.
	streamData := `{"Index":1008,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-restart-1","JobID":"kiosk-client","Status":"complete","DeploymentID":"dep-old-stale-1"}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-old-stale-1","Status":"successful"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/allocations") {
			_, _ = w.Write([]byte(`[{"ID":"alloc-kiosk-1","DesiredStatus":"run","ClientStatus":"running"}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployedTargetID: "1555405874565091380",
		deployedPRNum:    599,
		deployedMergeSHA: "sha_eval_restart",
		deployedRepo:     "azylman/mirrormere",
		deployedUpdated:  true,
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

	if len(mockReg.deployedJobCalls) != 1 || mockReg.deployedJobCalls[0] != "kiosk-client" {
		t.Fatalf("expected deployedJobCalls [kiosk-client], got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(mockDisp.PromptCalls()))
	}
}




func TestExtractAllocationErrorDetails_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		clientDesc string
		taskStates map[string]NomadTaskState
		wantSubstr string
	}{
		{
			name:       "nil states and empty desc",
			clientDesc: "",
			taskStates: nil,
			wantSubstr: "allocation failed",
		},
		{
			name:       "fallback to client description",
			clientDesc: "Failed tasks",
			taskStates: nil,
			wantSubstr: "Failed tasks",
		},
		{
			name:       "driver error in event",
			clientDesc: "Failed tasks",
			taskStates: map[string]NomadTaskState{
				"brain": {
					Failed: true,
					State:  "dead",
					Events: []NomadTaskEvent{
						{Type: "Received", DisplayMessage: "Task received by client"},
						{Type: "Driver Failure", DriverError: "bind source path does not exist: /mnt/data/supervisor/share/coverage/brain"},
					},
				},
			},
			wantSubstr: `task "brain": bind source path does not exist: /mnt/data/supervisor/share/coverage/brain`,
		},
		{
			name:       "driver error in event details map",
			clientDesc: "Failed tasks",
			taskStates: map[string]NomadTaskState{
				"brain": {
					Failed: true,
					State:  "dead",
					Events: []NomadTaskEvent{
						{
							Type:    "Driver Failure",
							Details: map[string]string{"driver_error": "failed to create container: out of memory"},
						},
					},
				},
			},
			wantSubstr: `task "brain": failed to create container: out of memory`,
		},
		{
			name:       "setup error in event",
			clientDesc: "",
			taskStates: map[string]NomadTaskState{
				"sidecar": {
					Failed: true,
					Events: []NomadTaskEvent{
						{Type: "Task Setup", SetupError: "prestart hook failed: permission denied"},
					},
				},
			},
			wantSubstr: `task "sidecar": prestart hook failed: permission denied`,
		},
		{
			name:       "validation error in event",
			clientDesc: "",
			taskStates: map[string]NomadTaskState{
				"api": {
					Failed: true,
					Events: []NomadTaskEvent{
						{Type: "Failed Validation", ValidationError: "invalid environment variable"},
					},
				},
			},
			wantSubstr: `task "api": invalid environment variable`,
		},
		{
			name:       "exit code non-zero",
			clientDesc: "",
			taskStates: map[string]NomadTaskState{
				"worker": {
					Failed: true,
					Events: []NomadTaskEvent{
						{Type: "Terminated", ExitCode: 1, DisplayMessage: "Terminated unexpectedly"},
					},
				},
			},
			wantSubstr: `task "worker": Terminated unexpectedly (exited with code 1)`,
		},
		{
			name:       "fallback to last event display message on dead task",
			clientDesc: "",
			taskStates: map[string]NomadTaskState{
				"proxy": {
					Failed: true,
					State:  "dead",
					Events: []NomadTaskEvent{
						{Type: "Alloc Unhealthy", DisplayMessage: "Unhealthy because of failed task"},
					},
				},
			},
			wantSubstr: `task "proxy": Unhealthy because of failed task`,
		},
		{
			name:       "multiple tasks sorted deterministically",
			clientDesc: "Failed tasks",
			taskStates: map[string]NomadTaskState{
				"worker": {
					Failed: true,
					Events: []NomadTaskEvent{
						{Type: "Driver Failure", DriverError: "worker failed"},
					},
				},
				"api": {
					Failed: true,
					Events: []NomadTaskEvent{
						{Type: "Driver Failure", DriverError: "api failed"},
					},
				},
			},
			wantSubstr: "task \"api\": api failed\ntask \"worker\": worker failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractAllocationErrorDetails(tt.clientDesc, tt.taskStates)
			if !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("extractAllocationErrorDetails() = %q, want substring %q", got, tt.wantSubstr)
			}
		})
	}
}

func TestRouterServer_IsNomadJobHealthy_DeploymentsOutOfOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var depsJSON string
	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(depsJSON))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer nomadServer.Close()

	srv := NewRouterServer(Config{NomadAddr: nomadServer.URL}, nil)

	// Case 1: Deployments out of order in API response: older failed deployment at index 0, newer successful at index 1
	// Must sort by CreateIndex descending and evaluate the newest deployment as successful!
	depsJSON = `[
		{"ID":"dep-v14-failed","JobID":"brain","Status":"failed","JobVersion":14,"CreateIndex":19265},
		{"ID":"dep-v16-success","JobID":"brain","Status":"successful","JobVersion":16,"CreateIndex":19397},
		{"ID":"dep-v15-success","JobID":"brain","Status":"successful","JobVersion":15,"CreateIndex":19349}
	]`
	if !srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be true when newest deployment (highest CreateIndex) is successful")
	}

	// Case 2: Auto-revert case: higher JobVersion failed, but auto-revert deployment rolled back to older JobVersion
	// Must detect that higher JobVersion failed/was reverted and return false!
	depsJSON = `[
		{"ID":"dep-revert-v16","JobID":"brain","Status":"successful","JobVersion":16,"CreateIndex":20500},
		{"ID":"dep-v17-failed","JobID":"brain","Status":"failed","JobVersion":17,"CreateIndex":20400}
	]`
	if srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be false when deployment was rolled back from failed higher version")
	}

	// Case 2b: Auto-revert indicated in StatusDescription
	depsJSON = `[
		{"ID":"dep-v17-roll","JobID":"brain","Status":"successful","JobVersion":17,"CreateIndex":20600,"StatusDescription":"Rolling back to version 16"}
	]`
	if srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be false when StatusDescription indicates rolling back")
	}

	// Case 3: Newer deployment failed (highest CreateIndex is failed)
	depsJSON = `[
		{"ID":"dep-v15-success","JobID":"brain","Status":"successful","JobVersion":15,"CreateIndex":19349},
		{"ID":"dep-v16-failed","JobID":"brain","Status":"failed","JobVersion":16,"CreateIndex":19397}
	]`
	if srv.isNomadJobHealthy(ctx, "brain") {
		t.Errorf("expected isNomadJobHealthy to be false when newest deployment (highest CreateIndex) is failed")
	}
}

func TestNomadStreamSubscriber_AllocationFailure_DriverError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := strings.Join([]string{
		`{"Index":1009,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-fail-1","JobID":"brain","DesiredStatus":"stop","ClientStatus":"failed","ClientDescription":"Failed tasks","TaskStates":{"brain":{"State":"dead","Failed":true,"Events":[{"Type":"Driver Failure","DriverError":"invalid mount config: bind source path does not exist: /mnt/data/supervisor/share/coverage/brain"}]}}}}}]}`,
	}, "\n") + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    613,
		deployFailedJobMergeSHA: "7cbc1f9b1234567890",
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

	if len(mockReg.deployFailedJobCalls) != 1 || mockReg.deployFailedJobCalls[0] != "brain" {
		t.Fatalf("expected deployFailedJobCalls [brain], got %v", mockReg.deployFailedJobCalls)
	}

	dmCalls := mockDisp.DirectMessageCalls()
	if len(dmCalls) != 1 {
		t.Fatalf("expected 1 direct message call on failure, got %d", len(dmCalls))
	}
	if dmCalls[0].ChannelID != "1555405874565091380" || !strings.Contains(dmCalls[0].Content, "💥 **(PR: #613, repo: aerial)** Nomad deployment failed for brain. Following up...") {
		t.Errorf("unexpected direct message content: %+v", dmCalls[0])
	}

	pCalls := mockDisp.PromptCalls()
	if len(pCalls) != 1 {
		t.Fatalf("expected 1 prompt call, got %d", len(pCalls))
	}
	if pCalls[0].ChannelID != "1555405874565091380" {
		t.Errorf("expected target ID 1555405874565091380, got %s", pCalls[0].ChannelID)
	}
	if !strings.Contains(pCalls[0].Prompt, "Nomad deployment for job brain failed") {
		t.Errorf("unexpected prompt content: %s", pCalls[0].Prompt)
	}
	if !strings.Contains(pCalls[0].Prompt, "bind source path does not exist: /mnt/data/supervisor/share/coverage/brain") {
		t.Errorf("expected driver error in prompt: %s", pCalls[0].Prompt)
	}
}

func TestNomadStreamSubscriber_WithRiver_AllocationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamData := strings.Join([]string{
		`{"Index":2010,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"alloc-dead-1","JobID":"brain","DesiredStatus":"stop","ClientStatus":"failed"}}}]}`,
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

	err := sub.consumeStream(ctx)
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("unexpected stream error: %v", err)
	}

	if len(mockRiver.insertedNomadJobs) != 1 {
		t.Fatalf("expected 1 nomad job inserted into river, got %d: %+v", len(mockRiver.insertedNomadJobs), mockRiver.insertedNomadJobs)
	}
	if mockRiver.insertedNomadJobs[0].Topic != "Allocation" {
		t.Errorf("expected Topic Allocation, got %s", mockRiver.insertedNomadJobs[0].Topic)
	}
	if !strings.Contains(mockRiver.insertedNomadJobs[0].IdempotencyKey, ":failed") {
		t.Errorf("expected idempotency key to contain ':failed', got %s", mockRiver.insertedNomadJobs[0].IdempotencyKey)
	}
}

func TestNomadStreamSubscriber_Evaluation_QueuedAllocations_Ignored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Evaluation with port collision but queued allocations represents transient rolling update queueing;
	// it should neither trigger deployment failure nor premature success.
	streamData := `{"Index":2020,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-queued-1","JobID":"scheduler-mcp","Status":"complete","DeploymentID":"","FailedTGAllocs":{"scheduler-mcp":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"scheduler-mcp":1}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    629,
		deployFailedJobMergeSHA: "d9d4129bbfae5726d7d5953d8d7b0d87426afe53",
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

	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls, got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockReg.deployedJobCalls) != 0 {
		t.Fatalf("expected 0 deployedJobCalls, got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls, got %d", len(mockDisp.PromptCalls()))
	}
	if len(mockDisp.DirectMessageCalls()) != 0 {
		t.Fatalf("expected 0 direct message calls, got %d", len(mockDisp.DirectMessageCalls()))
	}
}

func TestNomadStreamSubscriber_Evaluation_MultiTaskGroup_UnqueuedFailure_TreatedAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// In a multi-task-group job, if taskGroupA fatally fails with 0 queued allocations
	// while taskGroupB has 1 queued allocation, the evaluation MUST still be treated as a failure.
	streamData := `{"Index":2021,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-multi-fail","JobID":"brain","Status":"complete","DeploymentID":"","FailedTGAllocs":{"taskGroupA":{"ResourceExhausted":1}},"QueuedAllocations":{"taskGroupB":1}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    630,
		deployFailedJobMergeSHA: "sha_multi_tg_fail",
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

	if len(mockReg.deployFailedJobCalls) != 1 || mockReg.deployFailedJobCalls[0] != "brain" {
		t.Fatalf("expected deployFailedJobCalls [brain], got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call for unqueued task group failure, got %d", len(mockDisp.PromptCalls()))
	}
	if !strings.Contains(mockDisp.PromptCalls()[0].Prompt, "failed task group allocations") {
		t.Errorf("expected failure prompt to mention failed task group allocations: %s", mockDisp.PromptCalls()[0].Prompt)
	}
}

func TestNomadStreamSubscriber_PlanResult_Ignored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Internal scheduler planning calculation events (PlanResult) must be completely ignored
	// and never trigger deployment failure or success.
	streamData := strings.Join([]string{
		`{"Index":2030,"Events":[{"Topic":"Allocation","Type":"PlanResult","Payload":{"Allocation":{"ID":"alloc-plan-1","JobID":"github-mcp","DesiredStatus":"stop","ClientStatus":"failed"}}}]}`,
		`{"Index":2031,"Events":[{"Topic":"Deployment","Type":"PlanResult","Payload":{"Deployment":{"ID":"dep-plan-1","JobID":"github-mcp","Status":"failed"}}}]}`,
		`{"Index":2032,"Events":[{"Topic":"Evaluation","Type":"PlanResult","Payload":{"Evaluation":{"ID":"eval-plan-1","JobID":"github-mcp","Status":"failed"}}}]}`,
	}, "\n") + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    631,
		deployFailedJobMergeSHA: "63abb6557e1aca574d792aa1f7c9ca07dbf86cf3",
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

	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls for PlanResult events, got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockReg.deployedJobCalls) != 0 {
		t.Fatalf("expected 0 deployedJobCalls for PlanResult events, got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls for PlanResult events, got %d", len(mockDisp.PromptCalls()))
	}
	if len(mockDisp.DirectMessageCalls()) != 0 {
		t.Fatalf("expected 0 direct message calls for PlanResult events, got %d", len(mockDisp.DirectMessageCalls()))
	}
}

func TestNomadStreamSubscriber_AllocationFailure_StaleAllocation_IgnoredWhenJobHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If an old or superseded allocation emits a failure event, but the job's latest deployment is already successful,
	// the failure event must be safely ignored without failing the active PR.
	streamData := `{"Index":2040,"Events":[{"Topic":"Allocation","Type":"AllocationUpdated","Payload":{"Allocation":{"ID":"old-dead-alloc","JobID":"github-mcp","DesiredStatus":"stop","ClientStatus":"failed","ClientDescription":"Failed tasks"}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments") {
			_, _ = w.Write([]byte(`[{"ID":"dep-succ-1","Status":"successful","StatusDescription":"Deployment completed successfully","JobVersion":10,"CreateIndex":22255}]`))
			return
		}
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    631,
		deployFailedJobMergeSHA: "63abb6557e1aca574d792aa1f7c9ca07dbf86cf3",
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

	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls when job is already healthy, got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls when job is already healthy, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestProcessNomadEvent_PlanResult_Dropped(t *testing.T) {
	ctx := context.Background()
	mockReg := &mockPRRegistry{}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	payload := json.RawMessage(`{"Allocation":{"ID":"alloc-1","JobID":"github-mcp","ClientStatus":"failed"}}`)
	err := srv.ProcessNomadEvent(ctx, "Allocation", "PlanResult", payload)
	if err != nil {
		t.Fatalf("expected nil error on PlanResult, got: %v", err)
	}
	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls, got %v", mockReg.deployFailedJobCalls)
	}
}

func TestNomadStreamSubscriber_Evaluation_BlockedStatus_Ignored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A blocked evaluation represents Nomad waiting for cluster state changes to place remaining allocations;
	// it must be safely ignored without failing the active deployment.
	streamData := `{"Index":2050,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"995a3489-6ff6-c3aa-6541-0175856c6e84","JobID":"hangar","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"3b3eb97e-6077-6951-6489-5dbe1dfdbb6f"}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    636,
		deployFailedJobMergeSHA: "b1507e486bd8758a392e6903ae5829bcf7a632a3",
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

	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls for blocked evaluation, got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockReg.deployedJobCalls) != 0 {
		t.Fatalf("expected 0 deployedJobCalls for blocked evaluation, got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls for blocked evaluation, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestNomadStreamSubscriber_Evaluation_Complete_WithBlockedEval_Ignored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// An initial evaluation that placed what it could and created a blocked evaluation for the remainder
	// (BlockedEval != "") must be safely ignored while queued allocations await placement.
	streamData := `{"Index":2051,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"033659ef-f967-628b-9bd2-f1b76f646261","JobID":"hangar","Status":"complete","BlockedEval":"995a3489-6ff6-c3aa-6541-0175856c6e84","DeploymentID":"3b3eb97e-6077-6951-6489-5dbe1dfdbb6f","FailedTGAllocs":{"hangar":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"hangar":1}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    636,
		deployFailedJobMergeSHA: "b1507e486bd8758a392e6903ae5829bcf7a632a3",
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

	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls for complete eval with BlockedEval, got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls for complete eval with BlockedEval, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestNomadStreamSubscriber_Evaluation_MixedState_BlockedWithUnqueuedFailure_TreatedAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// In a multi-task-group job, if an evaluation has a BlockedEval/queued allocation for group A,
	// but a terminal unqueued failure for group B, it MUST still be treated as a deployment failure.
	streamData := `{"Index":2052,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"eval-mixed-fail","JobID":"brain","Status":"complete","BlockedEval":"eval-blocked-b","FailedTGAllocs":{"groupA":{"DimensionExhausted":{"network: port collision":1}},"groupB":{"ConstraintFiltered":1}},"QueuedAllocations":{"groupA":1,"groupB":0}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    637,
		deployFailedJobMergeSHA: "sha_mixed_fail",
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

	if len(mockReg.deployFailedJobCalls) != 1 || mockReg.deployFailedJobCalls[0] != "brain" {
		t.Fatalf("expected deployFailedJobCalls [brain], got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 1 {
		t.Fatalf("expected 1 prompt call for unqueued failure in mixed evaluation, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestProcessNomadEvent_Evaluation_Blocked_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	mockReg := &mockPRRegistry{}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	payload := json.RawMessage(`{"Evaluation":{"ID":"eval-blocked-direct","JobID":"hangar","Status":"blocked","StatusDescription":"created to place remaining allocations"}}`)
	err := srv.ProcessNomadEvent(ctx, "Evaluation", "EvaluationUpdated", payload)
	if err != nil {
		t.Fatalf("expected nil error on blocked evaluation, got: %v", err)
	}
	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls for blocked evaluation, got %v", mockReg.deployFailedJobCalls)
	}
}

func TestNomadStreamSubscriber_Evaluation_BlockedWithFailedTGAllocs_Ignored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A blocked evaluation created by Nomad during a rolling update (e.g. port collision before old alloc stops)
	// includes FailedTGAllocs and QueuedAllocations: 0, with Status: "blocked" and StatusDescription: "created to place remaining allocations".
	// It MUST be ignored without failing the active deployment.
	streamData := `{"Index":2053,"Events":[{"Topic":"Evaluation","Type":"EvaluationUpdated","Payload":{"Evaluation":{"ID":"bda9b66d-3cef-17a0-fce1-4faec0f3a52d","JobID":"webhooks-router","Status":"blocked","StatusDescription":"created to place remaining allocations","DeploymentID":"5622959a-4da3-fb28-09c7-5f6909503687","FailedTGAllocs":{"webhooks-router":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"webhooks-router":0}}}}]}` + "\n"

	nomadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(streamData))
	}))
	defer nomadServer.Close()

	mockReg := &mockPRRegistry{
		deployFailedJobTargetID: "1555405874565091380",
		deployFailedJobPRNum:    642,
		deployFailedJobMergeSHA: "e1d523155b4dfb34689d1a186090507960d9425d",
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

	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls for blocked evaluation with FailedTGAllocs, got %v", mockReg.deployFailedJobCalls)
	}
	if len(mockReg.deployedJobCalls) != 0 {
		t.Fatalf("expected 0 deployedJobCalls for blocked evaluation with FailedTGAllocs, got %v", mockReg.deployedJobCalls)
	}
	if len(mockDisp.PromptCalls()) != 0 {
		t.Fatalf("expected 0 prompt calls for blocked evaluation with FailedTGAllocs, got %d", len(mockDisp.PromptCalls()))
	}
}

func TestProcessNomadEvent_Evaluation_BlockedWithFailedTGAllocs_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	mockReg := &mockPRRegistry{
		deployFailedJobUpdated: true,
	}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(Config{}, nil)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	payload := json.RawMessage(`{"Evaluation":{"ID":"eval-blocked-direct","JobID":"webhooks-router","Status":"blocked","StatusDescription":"created to place remaining allocations","FailedTGAllocs":{"webhooks-router":{"DimensionExhausted":{"network: port collision":1}}},"QueuedAllocations":{"webhooks-router":0}}}`)
	err := srv.ProcessNomadEvent(ctx, "Evaluation", "EvaluationUpdated", payload)
	if err != nil {
		t.Fatalf("expected nil error on blocked evaluation with FailedTGAllocs, got: %v", err)
	}
	if len(mockReg.deployFailedJobCalls) != 0 {
		t.Fatalf("expected 0 deployFailedJobCalls for blocked evaluation with FailedTGAllocs, got %v", mockReg.deployFailedJobCalls)
	}
}


