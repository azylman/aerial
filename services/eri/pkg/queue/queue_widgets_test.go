package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
)

type deliverStatusWidgetCase struct {
	name, widgetMsgID, expectErrContains, expectMsgID string
	nilPool, nilSession                               bool
	req                                               discord.StatusWidgetRequest
	noBuildUpdates                                    bool
	parentNoBuildUpdates                              bool
	systemChannel                                     string
	cachedChannel                                     *discordgo.Channel
	expectDeliveredChannelID                          string
	useMockSession                                    bool
	priorRequests                                     []discord.StatusWidgetRequest
	priorSteps                                        map[string]string
	expectDeliveredChunks                             []discord.StatusWidgetChunk
	expectDeliveredMessageID                          string
	fifoFillCount                                     int
	expectEvictedBuildID                              string
	deliveryFuncErr                                   error
	useAppConfigPolicy                                bool
	expectDisownedChunkIDs                            []string
	expectRetainedWidgetSteps                         map[string]string
}

func makeStatusStepCase(name, id, chunkID, priorStatus, reqStatus, activeStep string, retain bool) deliverStatusWidgetCase {
	targetChan := "target-chan-1"
	bID := "b-" + id
	msgID := "msg-" + id
	deliveredStep := ""
	retainedStep := ""
	if retain {
		deliveredStep = activeStep
		retainedStep = activeStep
	}
	return deliverStatusWidgetCase{
		name:        name,
		widgetMsgID: msgID,
		priorRequests: []discord.StatusWidgetRequest{
			{ID: bID, MessageID: msgID, ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: chunkID, Status: priorStatus, ActiveStep: activeStep}}},
		},
		req:                       discord.StatusWidgetRequest{ID: bID, MessageID: msgID, ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: chunkID, Status: reqStatus, ActiveStep: ""}}},
		expectMsgID:               msgID,
		expectDeliveredChunks:     []discord.StatusWidgetChunk{{ID: chunkID, Status: reqStatus, ActiveStep: deliveredStep}},
		expectRetainedWidgetSteps: map[string]string{chunkID: retainedStep},
	}
}

func setupStatusWidgetPool(tc deliverStatusWidgetCase, mockSess *discordgo.Session, deliveredChanID *string, deliveredReq *discord.StatusWidgetRequest) *WorkerPool {
	const (
		targetChan = "target-chan-1"
		parentChan = "parent-chan-1"
	)
	p := &WorkerPool{}
	if !tc.useAppConfigPolicy {
		p.cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
			if tc.noBuildUpdates && (channelID == targetChan || channelName == targetChan) {
				return config.ChannelPolicy{NoBuildUpdates: true}
			}
			if tc.parentNoBuildUpdates && (channelID == parentChan || channelName == parentChan) {
				return config.ChannelPolicy{NoBuildUpdates: true}
			}
			return config.ChannelPolicy{}
		}
	}
	if tc.useMockSession {
		p.SetDiscordSession(mockSess)
	}
	if tc.useAppConfigPolicy {
		p.appCfg = config.NewFromData(&config.ConfigData{
			SystemChannel: tc.systemChannel,
			Channels: map[string]config.ChannelPolicy{
				targetChan: {NoBuildUpdates: tc.noBuildUpdates},
			},
		})
	} else if tc.systemChannel != "" {
		p.appCfg = config.NewFromData(&config.ConfigData{SystemChannel: tc.systemChannel})
	}
	p.SetWidgetDeliveryFunc(func(req discord.StatusWidgetRequest) (string, error) {
		if tc.deliveryFuncErr != nil {
			return "", tc.deliveryFuncErr
		}
		*deliveredChanID = req.ChannelID
		*deliveredReq = req
		if tc.widgetMsgID != "" {
			return tc.widgetMsgID, nil
		}
		if req.MessageID != "" {
			return req.MessageID, nil
		}
		return "default-delivered-msg", nil
	})
	return p
}

func assertRetainedWidgetSteps(t *testing.T, p *WorkerPool, tc deliverStatusWidgetCase) {
	t.Helper()
	if len(tc.expectRetainedWidgetSteps) == 0 {
		return
	}
	p.widgetMu.Lock()
	cachedWidget, hasWidget := p.widgetsByID[tc.req.ID]
	p.widgetMu.Unlock()
	if !hasWidget {
		t.Fatalf("expected widget %q in cache", tc.req.ID)
	}
	for cID, wantStep := range tc.expectRetainedWidgetSteps {
		found := false
		for _, c := range cachedWidget.Chunks {
			if c.ID == cID {
				found = true
				if c.ActiveStep != wantStep {
					t.Fatalf("cached widget chunk %q ActiveStep = %q, want %q", cID, c.ActiveStep, wantStep)
				}
				break
			}
		}
		if !found {
			t.Fatalf("cached widget missing chunk %q", cID)
		}
	}
}

func assertStatusWidgetDelivery(t *testing.T, tc deliverStatusWidgetCase, p *WorkerPool, deliveredChanID string, deliveredReq discord.StatusWidgetRequest, msgID string, err error) {
	t.Helper()
	if tc.expectErrContains != "" {
		if err == nil || !strings.Contains(err.Error(), tc.expectErrContains) {
			t.Fatalf("expected error containing %q, got %v", tc.expectErrContains, err)
		}
		return
	}
	if err != nil || msgID != tc.expectMsgID {
		t.Fatalf("unexpected delivery: err=%v msgID=%q", err, msgID)
	}
	if tc.expectDeliveredChannelID != "" && deliveredChanID != tc.expectDeliveredChannelID {
		t.Fatalf("delivered ChannelID = %q, want %q", deliveredChanID, tc.expectDeliveredChannelID)
	}
	if tc.expectDeliveredMessageID != "" && deliveredReq.MessageID != tc.expectDeliveredMessageID {
		t.Fatalf("delivered MessageID = %q, want %q", deliveredReq.MessageID, tc.expectDeliveredMessageID)
	}
	if len(tc.expectDeliveredChunks) > 0 {
		if len(deliveredReq.Chunks) != len(tc.expectDeliveredChunks) {
			t.Fatalf("delivered chunks length mismatch: got %d, want %d", len(deliveredReq.Chunks), len(tc.expectDeliveredChunks))
		}
		for i, exp := range tc.expectDeliveredChunks {
			got := deliveredReq.Chunks[i]
			if got.ID != exp.ID || got.Status != exp.Status || got.ActiveStep != exp.ActiveStep {
				t.Fatalf("chunk[%d] mismatch: got (ID=%q, Status=%q, ActiveStep=%q), want (ID=%q, Status=%q, ActiveStep=%q)",
					i, got.ID, got.Status, got.ActiveStep, exp.ID, exp.Status, exp.ActiveStep)
			}
		}
	}
	if tc.expectEvictedBuildID != "" {
		p.widgetMu.Lock()
		_, exists := p.widgetsByID[tc.expectEvictedBuildID]
		p.widgetMu.Unlock()
		if exists {
			t.Fatalf("expected build %q to be evicted from widgetsByID", tc.expectEvictedBuildID)
		}
	}
	for _, cID := range tc.expectDisownedChunkIDs {
		p.widgetMu.Lock()
		turn, okTurn := p.activeChunkTurns[cID]
		step, okStep := p.activeChunkSteps[cID]
		p.widgetMu.Unlock()
		if okTurn {
			t.Fatalf("expected chunk %q to be disowned from activeChunkTurns, found turn %q", cID, turn)
		}
		if okStep {
			t.Fatalf("expected chunk %q to have step deleted from activeChunkSteps, found step %q", cID, step)
		}
	}
	assertRetainedWidgetSteps(t, p, tc)
}

func runDeliverStatusWidgetCase(t *testing.T, tc deliverStatusWidgetCase, mockSess *discordgo.Session) {
	t.Helper()
	if tc.cachedChannel != nil {
		CacheDiscordChannel(tc.cachedChannel)
	}
	if tc.nilPool || tc.nilSession {
		var p *WorkerPool
		if !tc.nilPool {
			p = &WorkerPool{}
		}
		if _, err := p.DeliverStatusWidget(tc.req); err == nil || err.Error() != tc.expectErrContains {
			t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
		}
		return
	}
	var deliveredChanID string
	var deliveredReq discord.StatusWidgetRequest
	p := setupStatusWidgetPool(tc, mockSess, &deliveredChanID, &deliveredReq)

	for _, pr := range tc.priorRequests {
		if _, err := p.DeliverStatusWidget(pr); err != nil {
			t.Fatalf("setup prior request failed: %v", err)
		}
	}
	for i := range tc.fifoFillCount {
		fillReq := discord.StatusWidgetRequest{
			ID:        fmt.Sprintf("fifo-build-%d", i),
			ChannelID: "target-chan-1",
			MessageID: fmt.Sprintf("fifo-msg-%d", i),
			Chunks:    []discord.StatusWidgetChunk{{ID: fmt.Sprintf("fifo-chunk-%d", i), Status: "running"}},
		}
		if _, err := p.DeliverStatusWidget(fillReq); err != nil {
			t.Fatalf("fifo fill failed: %v", err)
		}
	}
	for cID, step := range tc.priorSteps {
		p.StartChunkStepTurn(context.Background(), cID, tc.req.ID, -1, "test-turn")
		p.widgetMu.Lock()
		p.activeChunkSteps[cID] = step
		p.widgetMu.Unlock()
	}
	msgID, err := p.DeliverStatusWidget(tc.req)
	assertStatusWidgetDelivery(t, tc, p, deliveredChanID, deliveredReq, msgID, err)
}

func deliverStatusWidgetTestCases() []deliverStatusWidgetCase {
	const (
		sysChanSnowflake = "1564150557705248879"
		targetChan       = "target-chan-1"
		threadSnowflake  = "1564150557705248880"
		parentChan       = "parent-chan-1"
	)
	return []deliverStatusWidgetCase{
		{name: "nil pool returns error", nilPool: true, expectErrContains: "worker pool is nil"},
		{name: "nil session returns error", nilSession: true, req: discord.StatusWidgetRequest{ChannelID: "chan-1"}, expectErrContains: "discord session is not connected"},
		{
			name:              "delivery func returns error",
			req:               discord.StatusWidgetRequest{ID: "b-err", ChannelID: targetChan},
			deliveryFuncErr:   errors.New("simulated delivery error"),
			expectErrContains: "simulated delivery error",
		},
		{
			name:                     "uses appConfig for channel policy when ResolveChannelPolicy is nil",
			widgetMsgID:              "msg-appcfg",
			req:                      discord.StatusWidgetRequest{ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: "c-deploy", Status: "running", DeployingJobs: []string{"eri"}}}},
			useAppConfigPolicy:       true,
			noBuildUpdates:           true,
			systemChannel:            sysChanSnowflake,
			expectMsgID:              "msg-appcfg",
			expectDeliveredChannelID: sysChanSnowflake,
		},
		{name: "injected WidgetDeliveryFunc is invoked without redirection", widgetMsgID: "widget-msg-123", req: discord.StatusWidgetRequest{ChannelID: targetChan}, expectMsgID: "widget-msg-123", expectDeliveredChannelID: targetChan},
		{name: "no_build_updates redirects target channel to system channel snowflake", widgetMsgID: "widget-msg-456", req: discord.StatusWidgetRequest{ChannelID: targetChan}, noBuildUpdates: true, systemChannel: sysChanSnowflake, expectMsgID: "widget-msg-456", expectDeliveredChannelID: sysChanSnowflake},
		{name: "no_build_updates redirects thread to system channel via parent policy", widgetMsgID: "widget-msg-789", req: discord.StatusWidgetRequest{ChannelID: threadSnowflake}, parentNoBuildUpdates: true, systemChannel: sysChanSnowflake, cachedChannel: &discordgo.Channel{ID: threadSnowflake, Type: discordgo.ChannelTypeGuildPublicThread, ParentID: parentChan, Name: "build-thread"}, expectMsgID: "widget-msg-789", expectDeliveredChannelID: sysChanSnowflake},
		{name: "no_build_updates resolves channel name system_channel via discord session", widgetMsgID: "widget-msg-321", req: discord.StatusWidgetRequest{ChannelID: targetChan}, noBuildUpdates: true, systemChannel: "aerial-dev", useMockSession: true, expectMsgID: "widget-msg-321", expectDeliveredChannelID: sysChanSnowflake},
		{name: "no_build_updates unresolvable system channel falls back to target channel", widgetMsgID: "widget-msg-101", req: discord.StatusWidgetRequest{ChannelID: targetChan}, noBuildUpdates: true, systemChannel: "nonexistent-channel", expectMsgID: "widget-msg-101", expectDeliveredChannelID: targetChan},
		{
			name:        "inherits existing message ID when req message ID is empty",
			widgetMsgID: "msg-inherit-999",
			priorRequests: []discord.StatusWidgetRequest{
				{ID: "b-inherit", MessageID: "msg-inherit-999", ChannelID: targetChan},
			},
			req:         discord.StatusWidgetRequest{ID: "b-inherit", ChannelID: targetChan},
			expectMsgID: "msg-inherit-999",
		},
		{
			name: "cached newer snowflake overrides stale caller message ID",
			priorRequests: []discord.StatusWidgetRequest{
				{ID: "b-newer-cached", MessageID: "1568701370469584946", ChannelID: targetChan},
			},
			req:                      discord.StatusWidgetRequest{ID: "b-newer-cached", MessageID: "1568690000000000000", ChannelID: targetChan},
			expectMsgID:              "1568701370469584946",
			expectDeliveredMessageID: "1568701370469584946",
		},
		{
			name: "caller with newer snowflake overrides cached older message ID",
			priorRequests: []discord.StatusWidgetRequest{
				{ID: "b-newer-caller", MessageID: "1568690000000000000", ChannelID: targetChan},
			},
			req:                      discord.StatusWidgetRequest{ID: "b-newer-caller", MessageID: "1568701370469584946", ChannelID: targetChan},
			expectMsgID:              "1568701370469584946",
			expectDeliveredMessageID: "1568701370469584946",
		},
		{
			name:        "preserves existing chunk IDs when req chunk ID is empty",
			widgetMsgID: "msg-pres",
			priorRequests: []discord.StatusWidgetRequest{
				{ID: "b-preserve", MessageID: "msg-pres", ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: "chunk-preserved", Status: "running"}}},
			},
			req:                   discord.StatusWidgetRequest{ID: "b-preserve", ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: "", Status: "running"}}},
			expectMsgID:           "msg-pres",
			expectDeliveredChunks: []discord.StatusWidgetChunk{{ID: "chunk-preserved", Status: "running"}},
		},
		{
			name:                  "overlays active chunk step onto running chunk",
			widgetMsgID:           "msg-overlay",
			priorSteps:            map[string]string{"chunk-overlay": "⚡ Running `view_file`... (1.2s)"},
			req:                   discord.StatusWidgetRequest{ID: "b-overlay", MessageID: "msg-overlay", ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: "chunk-overlay", Status: "running"}}},
			expectMsgID:           "msg-overlay",
			expectDeliveredChunks: []discord.StatusWidgetChunk{{ID: "chunk-overlay", Status: "running", ActiveStep: "⚡ Running `view_file`... (1.2s)"}},
		},
		{
			name:                  "defaults standalone pr widget chunk ID to widget ID and overlays active step on failed status",
			widgetMsgID:           "msg-pr-rem",
			priorSteps:            map[string]string{"pr-azylman/aerial-1250": "⚡ Executing `verify.sh`... (2.1s)"},
			req:                   discord.StatusWidgetRequest{ID: "pr-azylman/aerial-1250", MessageID: "msg-pr-rem", ChannelID: targetChan, Status: "failed", Chunks: []discord.StatusWidgetChunk{{Status: "failed", PRNumber: 1250}}},
			expectMsgID:           "msg-pr-rem",
			expectDeliveredChunks: []discord.StatusWidgetChunk{{ID: "pr-azylman/aerial-1250", Status: "failed", PRNumber: 1250, ActiveStep: "⚡ Executing `verify.sh`... (2.1s)"}},
		},
		{
			name:                  "clears active step on non-in-progress chunk",
			widgetMsgID:           "msg-done",
			priorSteps:            map[string]string{"chunk-done": "⚡ Running `view_file`... (1.2s)"},
			req:                   discord.StatusWidgetRequest{ID: "b-done", MessageID: "msg-done", ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: "chunk-done", Status: "pr_opened"}}},
			expectMsgID:           "msg-done",
			expectDeliveredChunks: []discord.StatusWidgetChunk{{ID: "chunk-done", Status: "pr_opened", ActiveStep: ""}},
		},
		{
			name:                  "clears active steps on completed build status",
			widgetMsgID:           "msg-term",
			priorSteps:            map[string]string{"chunk-term": "⚡ Running `view_file`... (1.2s)"},
			req:                   discord.StatusWidgetRequest{ID: "b-term", MessageID: "msg-term", ChannelID: targetChan, Status: "completed", Chunks: []discord.StatusWidgetChunk{{ID: "chunk-term", Status: "running"}}},
			expectMsgID:           "msg-term",
			expectDeliveredChunks: []discord.StatusWidgetChunk{{ID: "chunk-term", Status: "running", ActiveStep: ""}},
		},
		{
			name:                      "preserves active step on open chunk and disowns prior coding turn steps",
			widgetMsgID:               "msg-open-ci",
			priorSteps:                map[string]string{"chunk-open-ci": "⚡ Executing `git commit`..."},
			req:                       discord.StatusWidgetRequest{ID: "b-open-ci", MessageID: "msg-open-ci", ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: "chunk-open-ci", Status: "open", ActiveStep: "Waiting on CI (1/3 passed)"}}},
			expectMsgID:               "msg-open-ci",
			expectDeliveredChunks:     []discord.StatusWidgetChunk{{ID: "chunk-open-ci", Status: "open", ActiveStep: "Waiting on CI (1/3 passed)"}},
			expectDisownedChunkIDs:    []string{"chunk-open-ci"},
			expectRetainedWidgetSteps: map[string]string{"chunk-open-ci": "Waiting on CI (1/3 passed)"},
		},
		makeStatusStepCase("preserves existing active step on pr_opened chunk when subsequent sync omits active step", "ci-persist", "chunk-ci-1", "pr_opened", "pr_opened", "Waiting on CI (2/4 passed)", true),
		{
			name:        "clears active step on open chunk when build is terminal",
			widgetMsgID: "msg-ci-term",
			priorRequests: []discord.StatusWidgetRequest{
				{ID: "b-ci-term", MessageID: "msg-ci-term", ChannelID: targetChan, Chunks: []discord.StatusWidgetChunk{{ID: "chunk-ci-term", Status: "open", ActiveStep: "Waiting on CI (3/3 passed)"}}},
			},
			req:                   discord.StatusWidgetRequest{ID: "b-ci-term", MessageID: "msg-ci-term", ChannelID: targetChan, Status: "completed", Chunks: []discord.StatusWidgetChunk{{ID: "chunk-ci-term", Status: "open", ActiveStep: "Waiting on CI (3/3 passed)"}}},
			expectMsgID:           "msg-ci-term",
			expectDeliveredChunks: []discord.StatusWidgetChunk{{ID: "chunk-ci-term", Status: "open", ActiveStep: ""}},
		},
		makeStatusStepCase("preserves existing active step on merged chunk when subsequent sync omits active step", "merged-persist", "chunk-merged-1", "merged", "merged", "Building container images (1/2 completed)", true),
		makeStatusStepCase("preserves existing active step on synced chunk when subsequent sync omits active step", "synced-persist", "chunk-synced-1", "synced", "synced", "Building container images (2/2 completed)", true),
		makeStatusStepCase("preserves existing active step on deploying chunk when subsequent sync omits active step", "deploying-persist", "chunk-deploying-1", "deploying", "deploying", "`eri`: 🔄 starting", true),
		makeStatusStepCase("clears stale active step when chunk transitions from open to merged", "trans-open-merged", "chunk-trans-1", "open", "merged", "Waiting on CI (3/3 passed)", false),
		makeStatusStepCase("clears stale active step when chunk transitions from merged to deploying", "trans-merged-deploying", "chunk-trans-2", "merged", "deploying", "Building container images (2/2 completed)", false),
		makeStatusStepCase("clears stale active step when chunk transitions from deploying to deployed", "trans-deploying-deployed", "chunk-trans-3", "deploying", "deployed", "`eri`: 🔄 starting", false),
		{
			name:                 "evicts oldest build from widgetsByID when exceeding 64 entries",
			widgetMsgID:          "msg-final",
			fifoFillCount:        65,
			req:                  discord.StatusWidgetRequest{ID: "b-final", MessageID: "msg-final", ChannelID: targetChan},
			expectMsgID:          "msg-final",
			expectEvictedBuildID: "fifo-build-0",
		},
	}
}

func TestWorkerPool_DeliverStatusWidget(t *testing.T) {
	t.Parallel()
	const (
		sysChanSnowflake = "1564150557705248879"
		targetChan       = "target-chan-1"
		threadSnowflake  = "1564150557705248880"
		parentChan       = "parent-chan-1"
	)
	mockSess, _ := discordgo.New("Bot fake-token")
	mockSess.State = discordgo.NewState()
	_ = mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: sysChanSnowflake, Name: "aerial-dev"},
		},
	})

	tests := deliverStatusWidgetTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDeliverStatusWidgetCase(t, tc, mockSess)
		})
	}
	t.Run("DeliverStatusWidgetWithContext canceled context returns context.Canceled without delivery or cache mutation", func(t *testing.T) {
		t.Parallel()
		deliveryCalled := false
		p := &WorkerPool{}
		p.SetWidgetDeliveryFunc(func(req discord.StatusWidgetRequest) (string, error) {
			deliveryCalled = true
			return "delivered-msg", nil
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		req := discord.StatusWidgetRequest{
			ID:        "b-canceled",
			ChannelID: targetChan,
			MessageID: "msg-init",
			Chunks:    []discord.StatusWidgetChunk{{ID: "c-1", Status: "running"}},
		}

		_, err := p.DeliverStatusWidgetWithContext(ctx, req)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if deliveryCalled {
			t.Fatal("expected delivery func not to be called")
		}
		p.widgetMu.Lock()
		_, exists := p.widgetsByID["b-canceled"]
		p.widgetMu.Unlock()
		if exists {
			t.Fatal("expected widgetsByID not to be mutated on canceled context")
		}
	})
}

func TestIsBuildCompletionPrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		prompt string
		want   bool
	}{
		{name: "continuous delivery exact", prompt: "Continuous Delivery deployment completed for job git-merge on azylman/mirrormere", want: true},
		{name: "continuous delivery lower", prompt: "continuous delivery deployment completed for job eri", want: true},
		{name: "vanadis build completed exact", prompt: "Vanadis build build-123 completed (Tier 2, 2 chunks merged).", want: true},
		{name: "vanadis build completed lower", prompt: "vanadis build build-abc completed", want: true},
		{name: "vanadis chunk merged", prompt: "Vanadis chunk build-1-chunk-1 merged for aerial#10", want: false},
		{name: "regular user request", prompt: "Please summarize recent commits on main", want: false},
		{name: "empty prompt", prompt: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsBuildCompletionPrompt(tc.prompt); got != tc.want {
				t.Errorf("IsBuildCompletionPrompt(%q) = %v, want %v", tc.prompt, got, tc.want)
			}
		})
	}
}

type redirectBuildPromptCase struct {
	name                 string
	nilPool              bool
	channelID            string
	prompt               string
	noBuildUpdates       bool
	parentNoBuildUpdates bool
	systemChannel        string
	cachedChannel        *discordgo.Channel
	useMockSession       bool
	want                 string
}

func runRedirectBuildPromptCase(t *testing.T, tc redirectBuildPromptCase, mockSess *discordgo.Session) {
	t.Helper()
	const (
		targetChan = "target-chan-1"
		parentChan = "parent-chan-1"
	)
	if tc.cachedChannel != nil {
		CacheDiscordChannel(tc.cachedChannel)
	}
	if tc.nilPool {
		var p *WorkerPool
		if got := p.RedirectBuildPrompt(tc.channelID, tc.prompt); got != tc.want {
			t.Fatalf("RedirectBuildPrompt() = %q, want %q", got, tc.want)
		}
		return
	}
	p := &WorkerPool{}
	p.cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
		if tc.noBuildUpdates && (channelID == targetChan || channelName == targetChan) {
			return config.ChannelPolicy{NoBuildUpdates: true}
		}
		if tc.parentNoBuildUpdates && (channelID == parentChan || channelName == parentChan) {
			return config.ChannelPolicy{NoBuildUpdates: true}
		}
		return config.ChannelPolicy{}
	}
	if tc.useMockSession {
		p.SetDiscordSession(mockSess)
	}
	if tc.systemChannel != "" {
		p.appCfg = config.NewFromData(&config.ConfigData{SystemChannel: tc.systemChannel})
	}
	if got := p.RedirectBuildPrompt(tc.channelID, tc.prompt); got != tc.want {
		t.Fatalf("RedirectBuildPrompt() = %q, want %q", got, tc.want)
	}
}

func TestWorkerPool_RedirectBuildPrompt(t *testing.T) {
	t.Parallel()
	const (
		sysChanSnowflake = "1564150557705248879"
		targetChan       = "target-chan-1"
		threadSnowflake  = "1564150557705248880"
		parentChan       = "parent-chan-1"
	)
	mockSess, _ := discordgo.New("Bot fake-token")
	mockSess.State = discordgo.NewState()
	_ = mockSess.State.GuildAdd(&discordgo.Guild{
		ID: "guild-1",
		Channels: []*discordgo.Channel{
			{ID: sysChanSnowflake, Name: "aerial-dev"},
		},
	})

	tests := []redirectBuildPromptCase{
		{name: "nil pool returns channelID", nilPool: true, channelID: "chan-1", prompt: "Continuous Delivery deployment completed for job eri", want: "chan-1"},
		{name: "empty channelID returns empty", channelID: "", prompt: "Continuous Delivery deployment completed for job eri", want: ""},
		{name: "empty prompt returns channelID", channelID: "chan-1", prompt: "", want: "chan-1"},
		{name: "non-build prompt not redirected", channelID: targetChan, prompt: "Check system health", noBuildUpdates: true, systemChannel: sysChanSnowflake, want: targetChan},
		{name: "continuous delivery redirects target channel", channelID: targetChan, prompt: "Continuous Delivery deployment completed for job eri", noBuildUpdates: true, systemChannel: sysChanSnowflake, want: sysChanSnowflake},
		{name: "vanadis build completed redirects target channel", channelID: targetChan, prompt: "Vanadis build build-99 completed", noBuildUpdates: true, systemChannel: sysChanSnowflake, want: sysChanSnowflake},
		{name: "thread with suppression on parent redirects to system channel", channelID: threadSnowflake, prompt: "Vanadis build build-99 completed", parentNoBuildUpdates: true, cachedChannel: &discordgo.Channel{ID: threadSnowflake, ParentID: parentChan, Type: discordgo.ChannelTypeGuildPublicThread, Name: "build-thread"}, systemChannel: sysChanSnowflake, want: sysChanSnowflake},
		{name: "thread without suppression on parent returns thread channelID", channelID: threadSnowflake, prompt: "Continuous Delivery deployment completed for job eri", cachedChannel: &discordgo.Channel{ID: threadSnowflake, ParentID: parentChan, Type: discordgo.ChannelTypeGuildPublicThread}, systemChannel: sysChanSnowflake, want: threadSnowflake},
		{name: "build prompt without no_build_updates does not redirect", channelID: targetChan, prompt: "Continuous Delivery deployment completed for job eri", noBuildUpdates: false, systemChannel: sysChanSnowflake, want: targetChan},
		{name: "unresolvable system channel falls back to target channel", channelID: targetChan, prompt: "Continuous Delivery deployment completed for job eri", noBuildUpdates: true, systemChannel: "nonexistent", want: targetChan},
		{name: "resolves channel name system_channel via discord session", channelID: targetChan, prompt: "Continuous Delivery deployment completed for job eri", noBuildUpdates: true, systemChannel: "aerial-dev", useMockSession: true, want: sysChanSnowflake},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRedirectBuildPromptCase(t, tc, mockSess)
		})
	}
}

func TestWidgetChunkPhaseHelpers(t *testing.T) {
	t.Parallel()
	phaseTests := []struct {
		name string
		a, b string
		want bool
	}{
		{name: "both open", a: "open", b: "open", want: true},
		{name: "pr_opened and open", a: "pr_opened", b: "open", want: true},
		{name: "open and pr_opened", a: "open", b: "pr_opened", want: true},
		{name: "pr_opened and pr_opened", a: "pr_opened", b: "pr_opened", want: true},
		{name: "both merged", a: "merged", b: "merged", want: true},
		{name: "both synced", a: "synced", b: "synced", want: true},
		{name: "both deploying", a: "deploying", b: "deploying", want: true},
		{name: "open and merged", a: "open", b: "merged", want: false},
		{name: "merged and synced", a: "merged", b: "synced", want: false},
		{name: "synced and deploying", a: "synced", b: "deploying", want: false},
		{name: "deploying and deployed", a: "deploying", b: "deployed", want: false},
		{name: "both empty", a: "", b: "", want: false},
		{name: "whitespace and case insensitivity", a: "  PR_OPENED  ", b: "  open  ", want: true},
	}
	for _, tc := range phaseTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sameWidgetChunkPhase(tc.a, tc.b); got != tc.want {
				t.Errorf("sameWidgetChunkPhase(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}

	ciTests := []struct {
		status string
		want   bool
	}{
		{"open", true},
		{"pr_opened", true},
		{"merged", true},
		{"synced", true},
		{"deploying", true},
		{"deployed", false},
		{"running", false},
		{"coding", false},
		{"failed", false},
		{"", false},
	}
	for _, tc := range ciTests {
		t.Run("ci_"+tc.status, func(t *testing.T) {
			t.Parallel()
			if got := isWidgetChunkWaitingOnCI(tc.status); got != tc.want {
				t.Errorf("isWidgetChunkWaitingOnCI(%q) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

type chunkStepStreamingCase struct {
	name                  string
	nilPool               bool
	noSession             bool
	chunkID, turnID       string
	stepText              string
	seedWidget            bool
	chunkStatus           string
	mismatchedTurn        bool
	expectFlushErr        bool
	expectFlushDelivered  bool
	expectFinishDelivered bool
	deliveredMsgID        string
	wantCachedMsgID       string
	wantFinishReqMsgID    string
}

func runChunkStepStreamingCase(t *testing.T, tc chunkStepStreamingCase) {
	t.Helper()
	if tc.nilPool {
		var p *WorkerPool
		if err := p.FlushChunkActiveStep(context.Background(), tc.chunkID, tc.turnID, tc.stepText); err == nil {
			t.Fatalf("expected error from nil pool FlushChunkActiveStep")
		}
		p.StartChunkStepTurn(context.Background(), tc.chunkID, "b1", 0, tc.turnID)
		p.FinishChunkStepTurn(context.Background(), tc.chunkID, tc.turnID)
		return
	}
	var deliveredMu sync.Mutex
	var deliveredReqs []discord.StatusWidgetRequest
	p := &WorkerPool{}
	if !tc.noSession {
		p.SetWidgetDeliveryFunc(func(req discord.StatusWidgetRequest) (string, error) {
			deliveredMu.Lock()
			deliveredReqs = append(deliveredReqs, req)
			deliveredMu.Unlock()
			if tc.deliveredMsgID != "" {
				return tc.deliveredMsgID, nil
			}
			if req.MessageID != "" {
				return req.MessageID, nil
			}
			return "msg-delivered", nil
		})
	}
	if tc.seedWidget {
		p.widgetMu.Lock()
		p.ensureWidgetMapsLocked()
		p.widgetsByID["build-"+tc.chunkID] = discord.StatusWidgetRequest{
			ID:        "build-" + tc.chunkID,
			ChannelID: "chan-1",
			MessageID: "msg-" + tc.chunkID,
			Status:    "running",
			Chunks: []discord.StatusWidgetChunk{
				{ID: "", Status: tc.chunkStatus},
				{ID: "c-sibling", Status: "running"},
			},
		}
		p.activeChunkSteps["c-sibling"] = "sibling step"
		p.widgetMu.Unlock()
	}
	assignedTurn := tc.turnID
	if tc.mismatchedTurn {
		assignedTurn = "t-actual"
	}
	p.StartChunkStepTurn(context.Background(), tc.chunkID, "build-"+tc.chunkID, 0, assignedTurn)
	p.ensureChunkWidgetHydrated(context.Background(), "Prompt for "+tc.chunkID, tc.chunkID, "build-"+tc.chunkID)
	err := p.FlushChunkActiveStep(context.Background(), tc.chunkID, tc.turnID, tc.stepText)
	if (err != nil) != tc.expectFlushErr {
		t.Fatalf("FlushChunkActiveStep err = %v, expectFlushErr = %v", err, tc.expectFlushErr)
	}
	p.FinishChunkStepTurn(context.Background(), tc.chunkID, tc.turnID)

	deliveredMu.Lock()
	reqs := append([]discord.StatusWidgetRequest(nil), deliveredReqs...)
	deliveredMu.Unlock()
	count := len(reqs)

	if tc.expectFlushDelivered && count == 0 {
		t.Fatalf("expected at least 1 delivered request")
	}
	if tc.expectFinishDelivered && count < 2 {
		t.Fatalf("expected at least 2 delivered requests (flush + finish), got %d", count)
	}
	for i, req := range reqs {
		if req.SkipSink {
			t.Errorf("delivered step request %d has SkipSink=true, want false so buried cards can sink", i)
		}
	}
	if tc.wantFinishReqMsgID != "" {
		if got := reqs[count-1].MessageID; got != tc.wantFinishReqMsgID {
			t.Errorf("finish delivery MessageID = %q, want %q", got, tc.wantFinishReqMsgID)
		}
	}
	if tc.wantCachedMsgID != "" {
		p.widgetMu.Lock()
		got := p.widgetsByID["build-"+tc.chunkID].MessageID
		p.widgetMu.Unlock()
		if got != tc.wantCachedMsgID {
			t.Errorf("cached widget MessageID = %q, want %q", got, tc.wantCachedMsgID)
		}
	}
}

func TestWorkerPool_ChunkStepStreaming(t *testing.T) {
	t.Parallel()
	tests := []chunkStepStreamingCase{
		{
			name:           "nil pool returns error on flush",
			nilPool:        true,
			chunkID:        "c1",
			turnID:         "t1",
			stepText:       "step 1",
			expectFlushErr: true,
		},
		{
			name:            "nil session returns error on flush",
			noSession:       true,
			chunkID:         "c-nosess",
			turnID:          "t-nosess",
			stepText:        "step 1",
			seedWidget:      true,
			chunkStatus:     "running",
			expectFlushErr:  true,
			wantCachedMsgID: "msg-c-nosess",
		},
		{
			name:     "empty chunkID or turnID is no-op",
			chunkID:  "",
			turnID:   "",
			stepText: "step 1",
		},
		{
			name:           "mismatched turn ID is rejected",
			chunkID:        "c-mismatch",
			turnID:         "t-wrong",
			stepText:       "step 1",
			seedWidget:     true,
			chunkStatus:    "running",
			mismatchedTurn: true,
		},
		{
			name:        "chunk not in progress skips flush",
			chunkID:     "c-pr-open",
			turnID:      "t-open",
			stepText:    "step 1",
			seedWidget:  true,
			chunkStatus: "pr_opened",
		},
		{
			name:                  "running chunk flushes and finishes cleanly",
			chunkID:               "c-run",
			turnID:                "t-run",
			stepText:              "⚡ Executing `git status`...",
			seedWidget:            true,
			chunkStatus:           "running",
			expectFlushDelivered:  true,
			expectFinishDelivered: true,
			wantCachedMsgID:       "msg-c-run",
			wantFinishReqMsgID:    "msg-c-run",
		},
		{
			name:                  "sunk step edit records new message ID for later edits",
			chunkID:               "c-sunk",
			turnID:                "t-sunk",
			stepText:              "⚡ Executing `go build ./...`...",
			seedWidget:            true,
			chunkStatus:           "running",
			expectFlushDelivered:  true,
			expectFinishDelivered: true,
			deliveredMsgID:        "msg-sunk-new",
			wantCachedMsgID:       "msg-sunk-new",
			wantFinishReqMsgID:    "msg-sunk-new",
		},
		{
			name:                  "failed chunk during CI remediation flushes and finishes cleanly",
			chunkID:               "c-failed",
			turnID:                "t-failed",
			stepText:              "⚡ Executing `go test ./...`...",
			seedWidget:            true,
			chunkStatus:           "failed",
			expectFlushDelivered:  true,
			expectFinishDelivered: true,
		},
		{
			name:                  "conflict chunk during rebase remediation flushes and finishes cleanly",
			chunkID:               "c-conflict",
			turnID:                "t-conflict",
			stepText:              "⚡ Executing `git rebase origin/main`...",
			seedWidget:            true,
			chunkStatus:           "conflict",
			expectFlushDelivered:  true,
			expectFinishDelivered: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runChunkStepStreamingCase(t, tc)
		})
	}
}

func TestExtractVanadisDispatchInfo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		dispatch    db.Dispatch
		wantChunkID string
		wantBuildID string
		wantIdx     int
		wantPayload string
		wantSyncOK  bool
	}{
		{
			name:        "vanadis session ID with build and active chunk header",
			dispatch:    db.Dispatch{SessionID: "vanadis-chunk-42", Prompt: "Build ID: build-99\n=== Active Chunk (Chunk 2 of 3) ===\nChunk ID: chunk-42"},
			wantChunkID: "chunk-42",
			wantBuildID: "build-99",
			wantIdx:     1,
			wantPayload: `{"build_id":"build-99"}`,
			wantSyncOK:  true,
		},
		{
			name:        "vanadis chunk ID from prompt when session ID is thread",
			dispatch:    db.Dispatch{SessionID: "thread-1", Prompt: "CI failure\nBuild ID: build-77\nChunk ID: chunk-7"},
			wantChunkID: "chunk-7",
			wantBuildID: "build-77",
			wantIdx:     -1,
			wantPayload: `{"build_id":"build-77"}`,
			wantSyncOK:  true,
		},
		{
			name:        "vanadis chunk PR CI failure infers build ID and extracts PR sync payload",
			dispatch:    db.Dispatch{SessionID: "vanadis-build-123-chunk-3", Prompt: "❌ **GitHub Check Failed (`build`)** on azylman/mirrormere (commit: abc1234, PR #733, branch: `feat/test`).\n\nChunk ID: build-123-chunk-3"},
			wantChunkID: "build-123-chunk-3",
			wantBuildID: "build-123",
			wantIdx:     -1,
			wantPayload: `{"repo":"azylman/mirrormere","pr_number":733}`,
			wantSyncOK:  true,
		},
		{
			name:        "standalone PR CI failure remediation prompt",
			dispatch:    db.Dispatch{SessionID: "thread-1", Prompt: "❌ **GitHub Check Failed (`lint`)** on azylman/aerial (commit: abc1234, PR #1250, branch: `feat/test`)."},
			wantChunkID: "pr-azylman/aerial-1250",
			wantBuildID: "pr-azylman/aerial-1250",
			wantIdx:     0,
			wantPayload: `{"repo":"azylman/aerial","pr_number":1250}`,
			wantSyncOK:  true,
		},
		{
			name:        "standalone PR merge conflict remediation prompt",
			dispatch:    db.Dispatch{SessionID: "thread-1", Prompt: "⚠️ **Merge Conflict Detected** on PR #1250 on azylman/aerial (branch: `feat/test`)."},
			wantChunkID: "pr-azylman/aerial-1250",
			wantBuildID: "pr-azylman/aerial-1250",
			wantIdx:     0,
			wantPayload: `{"repo":"azylman/aerial","pr_number":1250}`,
			wantSyncOK:  true,
		},
		{
			name:        "standalone PR Nomad deployment failure remediation prompt",
			dispatch:    db.Dispatch{SessionID: "thread-1", Prompt: "🚨 **Nomad Deployment Failure (`eri`)** on azylman/aerial (PR #1250, branch: `main`)."},
			wantChunkID: "pr-azylman/aerial-1250",
			wantBuildID: "pr-azylman/aerial-1250",
			wantIdx:     0,
			wantPayload: `{"repo":"azylman/aerial","pr_number":1250}`,
			wantSyncOK:  true,
		},
		{
			name:        "standalone PR Hangar sync failure remediation prompt",
			dispatch:    db.Dispatch{SessionID: "thread-1", Prompt: "🚨 **Hangar GitOps Sync Failure** for repo azylman/aerial (PR #1250, branch: `main`)."},
			wantChunkID: "pr-azylman/aerial-1250",
			wantBuildID: "pr-azylman/aerial-1250",
			wantIdx:     0,
			wantPayload: `{"repo":"azylman/aerial","pr_number":1250}`,
			wantSyncOK:  true,
		},
		{
			name:        "structured vanadis_chunk_id populated directly",
			dispatch:    db.Dispatch{SessionID: "vanadis-other-chunk", VanadisChunkID: "build-200-chunk-1"},
			wantChunkID: "build-200-chunk-1",
			wantBuildID: "build-200",
			wantIdx:     -1,
			wantPayload: `{"build_id":"build-200"}`,
			wantSyncOK:  true,
		},
		{
			name:        "structured target_id with build prefix fallback",
			dispatch:    db.Dispatch{SessionID: "dispatch-123", TargetID: "build-300-chunk-2"},
			wantChunkID: "build-300-chunk-2",
			wantBuildID: "build-300",
			wantIdx:     -1,
			wantPayload: `{"build_id":"build-300"}`,
			wantSyncOK:  true,
		},
		{
			name:        "unrelated dispatch returns empty",
			dispatch:    db.Dispatch{SessionID: "thread-1", Prompt: "Run daily summary"},
			wantChunkID: "",
			wantBuildID: "",
			wantIdx:     -1,
			wantPayload: "",
			wantSyncOK:  false,
		},
	}

	if c, b, idx := extractVanadisDispatchInfo(nil); c != "" || b != "" || idx != -1 {
		t.Fatalf("extractVanadisDispatchInfo(nil) = (%q, %q, %d), want (\"\", \"\", -1)", c, b, idx)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunkID, buildID, idx := extractVanadisDispatchInfo(&tc.dispatch)
			if chunkID != tc.wantChunkID || buildID != tc.wantBuildID || idx != tc.wantIdx {
				t.Fatalf("extractVanadisDispatchInfo() = (%q, %q, %d), want (%q, %q, %d)",
					chunkID, buildID, idx, tc.wantChunkID, tc.wantBuildID, tc.wantIdx)
			}
			payload, ok := extractDispatchWidgetSyncPayload(tc.dispatch.Prompt, buildID)
			if ok != tc.wantSyncOK || payload != tc.wantPayload {
				t.Fatalf("extractDispatchWidgetSyncPayload() = (%q, %v), want (%q, %v)",
					payload, ok, tc.wantPayload, tc.wantSyncOK)
			}
		})
	}
}

func TestBuildLeaseTurnContext(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		te                 *turnExecution
		wantTargetID       string
		wantVanadisChunkID string
	}{
		{
			name: "discord turn populates target ID from threadID",
			te: &turnExecution{
				threadID: "discord-11223344",
				lease:    &testSessionLease{sessionID: "sess-1"},
			},
			wantTargetID:       "11223344",
			wantVanadisChunkID: "",
		},
		{
			name: "dispatch turn populates target ID and chunk ID",
			te: &turnExecution{
				threadID: "dispatch-1",
				dispatch: &db.Dispatch{
					ID:             "d-1",
					TargetID:       "discord-55667788",
					VanadisChunkID: "build-10-chunk-1",
				},
				lease: &testSessionLease{sessionID: "sess-2"},
			},
			wantTargetID:       "55667788",
			wantVanadisChunkID: "build-10-chunk-1",
		},
		{
			name: "dispatch turn with empty target ID falls back to thread ID",
			te: &turnExecution{
				threadID: "discord-998877",
				dispatch: &db.Dispatch{
					ID:             "d-2",
					VanadisChunkID: "build-20-chunk-2",
				},
				lease: &testSessionLease{sessionID: "sess-3"},
			},
			wantTargetID:       "998877",
			wantVanadisChunkID: "build-20-chunk-2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := tc.te.buildLeaseTurnContext(context.Background(), "model-a", "prompt-a", nil)
			if ctx.TargetID != tc.wantTargetID {
				t.Errorf("TargetID = %q, want %q", ctx.TargetID, tc.wantTargetID)
			}
			if ctx.VanadisChunkID != tc.wantVanadisChunkID {
				t.Errorf("VanadisChunkID = %q, want %q", ctx.VanadisChunkID, tc.wantVanadisChunkID)
			}
		})
	}
}

type processPoolWiringCase struct {
	name                                                                                                                     string
	nilPool, defaultConfig, withProcessPool, withVoicePool, checkAutoLLM, interchangeable, cleanAllThree, reconcilePrewarmed bool
	prewarmedTargets, expectedVoiceTargets                                                                                   []string
}

func testProcessPoolInterchangeable(t *testing.T) {
	t.Helper()
	spawner := newMockDaemonSpawner()
	u1 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, spawner)
	defer func() { _ = u1.Close() }()
	i1 := runner.NewInterchangeablePool(u1, runner.InterchangeablePoolConfig{WorkerCount: 2})
	defer func() { _ = i1.Close() }()
	u2 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-medium"}, spawner)
	defer func() { _ = u2.Close() }()
	i2 := runner.NewInterchangeablePool(u2, runner.InterchangeablePoolConfig{WorkerCount: 2})
	defer func() { _ = i2.Close() }()
	p := New(nil, WorkerPoolConfig{LowEffortProcessPool: i1, MediumEffortProcessPool: i2})
	defer p.Stop()
	if p.LowEffortProcessPool() != u1 || p.MediumEffortProcessPool() != u2 {
		t.Fatalf("expected underlying pools returned")
	}
}

func testProcessPoolCleanAllThree(t *testing.T) {
	t.Helper()
	pPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, newMockSpawner())
	defer func() { _ = pPool.Close() }()
	lSpawner := newMockSpawner()
	lPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lSpawner)
	defer func() { _ = lPool.Close() }()
	vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-2.5-flash"}, newMockSpawner())
	defer func() { _ = vPool.Close() }()
	pool := NewWorkerPool(WorkerPoolConfig{ProcessPool: pPool, LowEffortProcessPool: lPool, VoiceProcessPool: vPool})
	if len(pool.StatefulProcessPools()) != 1 {
		t.Fatalf("expected 1 stateful pool")
	}
	_, _ = pool.cfg.LLMFunc(context.Background(), "", "summarize")
	if got := lSpawner.LastSpawnCfg().Model; got != "gemini-3.8-flash-low" {
		t.Fatalf("expected LowEffortProcessPool model, got %q", got)
	}
	pool.MarkDirty()
	pool.Stop()
}

func testProcessPoolReconcilePrewarmed(t *testing.T, tc processPoolWiringCase) {
	t.Helper()
	dSpawner, mSpawner, lSpawner, vSpawner := newMockSpawner(), newMockSpawner(), newMockSpawner(), newMockSpawner()
	dPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-4-argon-high"}, dSpawner)
	defer func() { _ = dPool.Close() }()
	mPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-medium"}, mSpawner)
	defer func() { _ = mPool.Close() }()
	lPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lSpawner)
	defer func() { _ = lPool.Close() }()
	vPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-2.5-flash", PrewarmedTargets: tc.prewarmedTargets}, vSpawner)
	defer func() { _ = vPool.Close() }()
	appCfg := newTestConfig(func(d *config.ConfigData) {
		d.Model = "claude-sonnet-5-5-low"
		d.MediumEffortModel = "gpt-oss-120b-medium"
		d.LowEffortModel = "gpt-oss-120b-low"
		d.Voice.PrewarmedTargets = tc.expectedVoiceTargets
	})
	pool := New(appCfg, WorkerPoolConfig{ProcessPool: dPool, MediumEffortProcessPool: mPool, LowEffortProcessPool: lPool, VoiceProcessPool: vPool})
	defer pool.Stop()
	pool.MarkDirty()
	_, _ = vPool.GetOrCreate(context.Background(), tc.expectedVoiceTargets[0], "")
	targets := vPool.PrewarmedTargets()
	_, hasDaemon := vPool.Get(tc.expectedVoiceTargets[0])
	if len(targets) != len(tc.expectedVoiceTargets) || targets[0] != tc.expectedVoiceTargets[0] || !hasDaemon {
		t.Fatalf("prewarmed targets mismatch: %v", targets)
	}
	if len(dPool.PrewarmedTargets()) != 0 {
		t.Fatalf("expected discord pool prewarmed targets to remain empty, got %v", dPool.PrewarmedTargets())
	}
	_, _ = dPool.GetOrCreate(context.Background(), "t-high", "")
	_, _ = mPool.GetOrCreate(context.Background(), "t-med", "")
	_, _ = lPool.GetOrCreate(context.Background(), "t-low", "")
	if dSpawner.LastSpawnCfg().Model != "claude-sonnet-5-5-low" || mSpawner.LastSpawnCfg().Model != "gpt-oss-120b-medium" || lSpawner.LastSpawnCfg().Model != "gpt-oss-120b-low" || vSpawner.LastSpawnCfg().Model != "gpt-oss-120b-low" {
		t.Fatalf("unexpected pool models after MarkDirty: d=%q m=%q l=%q v=%q", dSpawner.LastSpawnCfg().Model, mSpawner.LastSpawnCfg().Model, lSpawner.LastSpawnCfg().Model, vSpawner.LastSpawnCfg().Model)
	}
}

func runProcessPoolWiringCase(t *testing.T, tc processPoolWiringCase) {
	t.Helper()
	if tc.nilPool {
		var p *WorkerPool
		if p.ProcessPool() != nil || p.LowEffortProcessPool() != nil || p.MediumEffortProcessPool() != nil || p.StatefulProcessPools() != nil {
			t.Fatalf("expected nil pools from nil WorkerPool")
		}
		p.MarkDirty()
		return
	}
	if tc.defaultConfig {
		p := New(nil, WorkerPoolConfig{})
		if p.ProcessPool() != nil || p.LowEffortProcessPool() != nil || p.MediumEffortProcessPool() != nil {
			t.Fatalf("expected nil pools by default")
		}
		return
	}
	if tc.interchangeable {
		testProcessPoolInterchangeable(t)
		return
	}
	if tc.cleanAllThree {
		testProcessPoolCleanAllThree(t)
		return
	}
	if tc.reconcilePrewarmed {
		testProcessPoolReconcilePrewarmed(t, tc)
		return
	}

	mockSpawner := newMockDaemonSpawner()
	var unifiedPool, voicePool runner.AgentPool
	if tc.withProcessPool {
		up := runner.NewUnifiedProcessPool(runner.PoolConfig{PrewarmedTargets: tc.prewarmedTargets, DefaultModel: "gemini-2.5-flash"}, mockSpawner)
		defer func() { _ = up.Close() }()
		unifiedPool = up
	}
	if tc.withVoicePool {
		vp := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
		defer func() { _ = vp.Close() }()
		voicePool = vp
	}
	p := New(nil, WorkerPoolConfig{ProcessPool: unifiedPool, VoiceProcessPool: voicePool})
	defer p.Stop()

	if tc.withProcessPool && p.ProcessPool() != unifiedPool {
		t.Fatalf("expected ProcessPool %p, got %p", unifiedPool, p.ProcessPool())
	}
	if tc.checkAutoLLM && p.cfg.LLMFunc == nil {
		t.Fatalf("expected cfg.LLMFunc to be auto-populated")
	}
	if tc.withVoicePool {
		p.MarkDirty()
	}
}

func TestWorkerPool_ProcessPoolWiringAndTurnRouting(t *testing.T) {
	t.Parallel()
	tests := []processPoolWiringCase{
		{name: "nil pool returns nil pools", nilPool: true},
		{name: "default config has nil pools", defaultConfig: true},
		{name: "configured process pool populates and accesses", withProcessPool: true, prewarmedTargets: []string{"kiosk", "ephemeral:classifier", "ephemeral:summarizer"}},
		{name: "auto populates LLMFunc from process pool", withProcessPool: true, checkAutoLLM: true},
		{name: "voice process pool wiring and accessors", withProcessPool: true, withVoicePool: true},
		{name: "interchangeable pool returns underlying pool", interchangeable: true},
		{name: "mark dirty and stop cleans all three pools", cleanAllThree: true},
		{name: "mark dirty reconciles voice prewarmed targets", reconcilePrewarmed: true, prewarmedTargets: []string{"kiosk"}, expectedVoiceTargets: []string{"touch-kiosk-kitchen"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProcessPoolWiringCase(t, tc)
		})
	}
}

type spyPoolSessionDeps struct {
	runner.AgentPool

	callbacksSet bool
	managerSet   bool
}

func (s *spyPoolSessionDeps) SetSessionCallbacks(getRec func(context.Context, string) (runner.SessionRecord, error), onRot func(context.Context, string, string, string) error) {
	s.callbacksSet = true
}

func (s *spyPoolSessionDeps) SetSessionManager(sm *session.Manager) {
	s.managerSet = true
}

func (s *spyPoolSessionDeps) SetTranscriptRescuer(r func(string, time.Time) string) {}

func TestWorkerPool_WirePoolSessionDependencies_TableDriven(t *testing.T) {
	t.Parallel()
	sessMgr := &session.Manager{}
	fakeStore := dbtest.NewFakeStore()

	tests := []struct {
		name                   string
		hasStore               bool
		wantStatefulCallbacks  bool
		wantEphemeralCallbacks bool
		wantMgrWired           bool
	}{
		{
			name:                   "stateful pools receive callbacks while ephemeral pools do not when store provided",
			hasStore:               true,
			wantStatefulCallbacks:  true,
			wantEphemeralCallbacks: false,
			wantMgrWired:           true,
		},
		{
			name:                   "nil store wires manager to all pools but not callbacks",
			hasStore:               false,
			wantStatefulCallbacks:  false,
			wantEphemeralCallbacks: false,
			wantMgrWired:           true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			primary := &spyPoolSessionDeps{}
			statefulLow := &spyPoolSessionDeps{}
			ephemLow := &spyPoolSessionDeps{}

			var store db.Store
			if tc.hasStore {
				store = fakeStore
			}

			pools := map[PoolKey]runner.AgentPool{
				{Ephemeral: false, Effort: "high"}: primary,
				{Ephemeral: false, Effort: "low"}:  statefulLow,
				{Ephemeral: true, Effort: "low"}:   ephemLow,
			}
			wirePoolSessionDependencies(pools, sessMgr, store)

			if primary.callbacksSet != tc.wantStatefulCallbacks || statefulLow.callbacksSet != tc.wantStatefulCallbacks {
				t.Errorf("stateful callbacksSet = (primary:%v, statefulLow:%v), want %v", primary.callbacksSet, statefulLow.callbacksSet, tc.wantStatefulCallbacks)
			}
			if ephemLow.callbacksSet != tc.wantEphemeralCallbacks {
				t.Errorf("ephemLow.callbacksSet = %v, want %v", ephemLow.callbacksSet, tc.wantEphemeralCallbacks)
			}
			if primary.managerSet != tc.wantMgrWired || statefulLow.managerSet != tc.wantMgrWired || ephemLow.managerSet != tc.wantMgrWired {
				t.Errorf("managerSet = (primary:%v, statefulLow:%v, ephemLow:%v), want %v", primary.managerSet, statefulLow.managerSet, ephemLow.managerSet, tc.wantMgrWired)
			}
		})
	}
}
