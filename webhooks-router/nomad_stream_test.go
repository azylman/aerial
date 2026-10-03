package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJobNameFromImage_TableDriven(t *testing.T) {
	tests := []struct {
		image    string
		expected string
	}{
		{"ghcr.io/azylman/aerial-brain:latest", "brain"},
		{"ghcr.io/azylman/aerial-scheduler-mcp:latest", "scheduler-mcp"},
		{"ghcr.io/azylman/aerial-discord-mcp:latest", "discord-mcp"},
		{"ghcr.io/azylman/aerial-hangar:latest", "hangar"},
		{"ghcr.io/azylman/aerial-webhooks-router:latest", "webhooks-router"},
		{"ghcr.io/azylman/aerial-dashboard:latest", "dashboard"},
		{"ghcr.io/azylman/mirrormere:latest", "mirrormere"},
		{"ghcr.io/azylman/mirrormere-voice-fingerprinter:latest", "mirrormere-voice-fingerprinter"},
		{"ghcr.io/azylman/custom-service:v1.0.0", "custom-service"},
		{"", ""},
	}

	for _, tt := range tests {
		got := jobNameFromImage(tt.image)
		if got != tt.expected {
			t.Errorf("jobNameFromImage(%q) = %q, expected %q", tt.image, got, tt.expected)
		}
	}
}

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

	// Verify all River inserts used MaxAttempts: 5
	for i, opts := range mockRiver.insertedOpts {
		if opts == nil || opts.MaxAttempts != 5 {
			t.Errorf("job %d missing MaxAttempts: 5: %+v", i, opts)
		}
	}
}


