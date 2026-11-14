package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/bwmarrin/discordgo"
	"github.com/nats-io/nats.go"
)

type synapseMemoryMsg struct {
	data         []byte
	subject      string
	numDelivered uint64
	termed       bool
}

func (m *synapseMemoryMsg) Data() []byte        { return m.data }
func (m *synapseMemoryMsg) Subject() string     { return m.subject }
func (m *synapseMemoryMsg) Header() nats.Header { return nil }
func (m *synapseMemoryMsg) NumDelivered() uint64 {
	if m.numDelivered > 0 {
		return m.numDelivered
	}
	return 1
}
func (m *synapseMemoryMsg) Ack() error                         { return nil }
func (m *synapseMemoryMsg) NakWithDelay(_ time.Duration) error { return nil }
func (m *synapseMemoryMsg) Term() error                        { m.termed = true; return nil }
func (m *synapseMemoryMsg) Snooze(_ time.Duration) error       { return nil }
func (m *synapseMemoryMsg) InProgress() error                  { return nil }

func TestResolveJetStreamJobCoords(t *testing.T) {
	t.Parallel()
	nowSec := time.Now().Unix() / 900

	tests := []struct {
		name        string
		args        riverJobArgsWithOpts
		wantSubject string
		wantPrefix  string
	}{
		{
			name:        "GitHubWebhookArgs",
			args:        GitHubWebhookArgs{Delivery: "del-123"},
			wantSubject: "aerial.jobs.synapse.github",
			wantPrefix:  "github:del-123",
		},
		{
			name:        "*GitHubWebhookArgs",
			args:        &GitHubWebhookArgs{Delivery: "del-456"},
			wantSubject: "aerial.jobs.synapse.github",
			wantPrefix:  "github:del-456",
		},
		{
			name:        "DeployWebhookArgs",
			args:        DeployWebhookArgs{IdempotencyKey: "dep-key-1"},
			wantSubject: "aerial.jobs.synapse.deploy",
			wantPrefix:  "deploy:dep-key-1",
		},
		{
			name:        "*DeployWebhookArgs",
			args:        &DeployWebhookArgs{IdempotencyKey: "dep-key-2"},
			wantSubject: "aerial.jobs.synapse.deploy",
			wantPrefix:  "deploy:dep-key-2",
		},
		{
			name:        "MergeConflictArgs",
			args:        MergeConflictArgs{Repo: "azylman/aerial", PRNumber: 42},
			wantSubject: "aerial.jobs.synapse.merge_conflict",
			wantPrefix:  fmt.Sprintf("conflict:azylman/aerial:42:%d", nowSec),
		},
		{
			name:        "*MergeConflictArgs",
			args:        &MergeConflictArgs{Repo: "azylman/aerial", PRNumber: 43},
			wantSubject: "aerial.jobs.synapse.merge_conflict",
			wantPrefix:  fmt.Sprintf("conflict:azylman/aerial:43:%d", nowSec),
		},
		{
			name:        "DiscordIngressArgs",
			args:        DiscordIngressArgs{MessageID: "msg-999"},
			wantSubject: "aerial.jobs.synapse.discord_ingress",
			wantPrefix:  "discord:msg:msg-999",
		},
		{
			name:        "*DiscordIngressArgs",
			args:        &DiscordIngressArgs{MessageID: "msg-888"},
			wantSubject: "aerial.jobs.synapse.discord_ingress",
			wantPrefix:  "discord:msg:msg-888",
		},
		{
			name:        "DiscordInteractionArgs",
			args:        DiscordInteractionArgs{InteractionID: "ix-111"},
			wantSubject: "aerial.jobs.synapse.discord_interaction",
			wantPrefix:  "discord:ix:ix-111",
		},
		{
			name:        "*DiscordInteractionArgs",
			args:        &DiscordInteractionArgs{InteractionID: "ix-222"},
			wantSubject: "aerial.jobs.synapse.discord_interaction",
			wantPrefix:  "discord:ix:ix-222",
		},
		{
			name:        "WidgetSyncArgs build",
			args:        WidgetSyncArgs{BuildID: "b-123"},
			wantSubject: "aerial.coalesced.widget_sync.build.b-123",
			wantPrefix:  "widget:build:b-123",
		},
		{
			name:        "*WidgetSyncArgs build",
			args:        &WidgetSyncArgs{BuildID: "b-456"},
			wantSubject: "aerial.coalesced.widget_sync.build.b-456",
			wantPrefix:  "widget:build:b-456",
		},
		{
			name:        "WidgetSyncArgs pr",
			args:        WidgetSyncArgs{Repo: "azylman/aerial", PRNumber: 77},
			wantSubject: "aerial.coalesced.widget_sync.pr.azylman.aerial.77",
			wantPrefix:  "widget:pr:azylman/aerial:77",
		},
		{
			name:        "*WidgetSyncArgs pr",
			args:        &WidgetSyncArgs{Repo: "azylman/aerial", PRNumber: 78},
			wantSubject: "aerial.coalesced.widget_sync.pr.azylman.aerial.78",
			wantPrefix:  "widget:pr:azylman/aerial:78",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subj, msgID := resolveJetStreamJobCoords(tc.args)
			if subj != tc.wantSubject {
				t.Errorf("subject = %q, want %q", subj, tc.wantSubject)
			}
			if !strings.HasPrefix(msgID, tc.wantPrefix) {
				t.Errorf("msgID = %q, want prefix %q", msgID, tc.wantPrefix)
			}
		})
	}
}

func initInsertRiverJobBus(ctx context.Context, busErr error, busReceived chan struct{}) pubsub.Bus {
	memBus := pubsub.NewMemoryBus()
	if busErr != nil {
		_ = memBus.Close()
		return memBus
	}
	_, _ = memBus.ConsumeQueue(ctx, "aerial.jobs.synapse.github", "test-watcher", func(_ context.Context, _ pubsub.Message) error {
		select {
		case busReceived <- struct{}{}:
		default:
		}
		return nil
	})
	return memBus
}

func TestInsertRiverJobWithJetStream(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		hasBus        bool
		busErr        error
		hasRiver      bool
		riverErr      error
		duplicateSkip bool
		args          riverJobArgsWithOpts
		wantErr       bool
		wantRiverCall bool
	}{
		{
			name:     "publishes to jetstream when bus available",
			hasBus:   true,
			hasRiver: true,
			args:     GitHubWebhookArgs{Delivery: "d1"},
			wantErr:  false,
		},
		{
			name:          "falls back to river when bus publish fails",
			hasBus:        true,
			busErr:        errors.New("nats down"),
			hasRiver:      true,
			args:          GitHubWebhookArgs{Delivery: "d2"},
			wantErr:       false,
			wantRiverCall: true,
		},
		{
			name:          "falls back to river when bus nil",
			hasBus:        false,
			hasRiver:      true,
			args:          GitHubWebhookArgs{Delivery: "d3"},
			wantErr:       false,
			wantRiverCall: true,
		},
		{
			name:          "duplicate skipped on river fallback",
			hasBus:        false,
			hasRiver:      true,
			duplicateSkip: true,
			args:          GitHubWebhookArgs{Delivery: "d4"},
			wantErr:       false,
			wantRiverCall: true,
		},
		{
			name:          "returns error when river fails",
			hasBus:        false,
			hasRiver:      true,
			riverErr:      errors.New("db error"),
			args:          GitHubWebhookArgs{Delivery: "d5"},
			wantErr:       true,
			wantRiverCall: true,
		},
		{
			name:     "returns error when both bus and river nil",
			hasBus:   false,
			hasRiver: false,
			args:     GitHubWebhookArgs{Delivery: "d6"},
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := &RouterServer{}
			var mockRiver *mockRiverInserter
			if tc.hasRiver {
				mockRiver = &mockRiverInserter{err: tc.riverErr, skipDuplicate: tc.duplicateSkip}
				srv.SetRiverClient(mockRiver)
			}
			busReceived := make(chan struct{}, 1)
			if tc.hasBus {
				memBus := initInsertRiverJobBus(ctx, tc.busErr, busReceived)
				t.Cleanup(func() { _ = memBus.Close() })
				srv.SetEventBus(memBus)
			}

			err := srv.insertRiverJob(ctx, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("insertRiverJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantRiverCall && tc.riverErr == nil && (mockRiver == nil || len(mockRiver.insertedJobs) == 0) {
				t.Errorf("expected river insert call, got none")
			}
			if tc.hasBus && tc.busErr == nil && !tc.wantErr {
				select {
				case <-busReceived:
				case <-time.After(2 * time.Second):
					t.Errorf("expected bus to receive published job")
				}
				if mockRiver != nil && len(mockRiver.insertedJobs) > 0 {
					t.Errorf("expected zero river calls when bus succeeded, got %d", len(mockRiver.insertedJobs))
				}
			}
		})
	}
}

type jetstreamHandlerCase struct {
	name         string
	handlerType  string
	payload      string
	numDelivered uint64
	dispErrType  string
	withMockReg  bool
	wantErr      bool
	wantTerm     bool
}

func jetstreamWebhookAndDeployCases() []jetstreamHandlerCase {
	return []jetstreamHandlerCase{
		{
			name:        "HandleGitHubJob malformed json",
			handlerType: "github",
			payload:     `{bad json`,
			wantTerm:    true,
		},
		{
			name:        "HandleGitHubJob valid ping payload",
			handlerType: "github",
			payload:     `{"event":"ping","delivery":"d-1","body":"e30="}`,
			wantErr:     false,
		},
		{
			name:         "HandleGitHubJob error processing payload below limit",
			handlerType:  "github",
			payload:      `{"event":"push","delivery":"d-2","body":"e2JhZA=="}`,
			numDelivered: 1,
			wantErr:      true,
			wantTerm:     false,
		},
		{
			name:         "HandleGitHubJob error processing payload at limit terms",
			handlerType:  "github",
			payload:      `{"event":"push","delivery":"d-2","body":"e2JhZA=="}`,
			numDelivered: 5,
			wantErr:      false,
			wantTerm:     true,
		},
		{
			name:        "HandleDeployJob malformed json",
			handlerType: "deploy",
			payload:     `{bad json`,
			wantTerm:    true,
		},
		{
			name:        "HandleDeployJob valid payload",
			handlerType: "deploy",
			payload:     `{"idempotency_key":"k-1","event":{"event":"unknown_deploy_event"}}`,
			wantErr:     false,
		},
		{
			name:         "HandleDeployJob error processing payload below limit",
			handlerType:  "deploy",
			payload:      `{"idempotency_key":"k-2","event":{"event":"deploy_started","repo":"azylman/aerial","pr_number":10,"target_id":"100000000000000001"}}`,
			dispErrType:  "widget_err",
			withMockReg:  true,
			numDelivered: 1,
			wantErr:      true,
			wantTerm:     false,
		},
		{
			name:         "HandleDeployJob error processing payload at limit terms",
			handlerType:  "deploy",
			payload:      `{"idempotency_key":"k-2","event":{"event":"deploy_started","repo":"azylman/aerial","pr_number":10,"target_id":"100000000000000001"}}`,
			dispErrType:  "widget_err",
			withMockReg:  true,
			numDelivered: 5,
			wantErr:      false,
			wantTerm:     true,
		},
		{
			name:        "HandleMergeConflictJob malformed json",
			handlerType: "conflict",
			payload:     `{bad json`,
			wantTerm:    true,
		},
		{
			name:        "HandleMergeConflictJob valid payload",
			handlerType: "conflict",
			payload:     `{"repo":"azylman/aerial","pr_number":42}`,
			wantErr:     false,
		},
		{
			name:         "HandleMergeConflictJob error dispatching prompt below limit",
			handlerType:  "conflict",
			payload:      `{"repo":"azylman/aerial","pr_number":42,"target_id":"100000000000000001"}`,
			dispErrType:  "prompt_err",
			numDelivered: 1,
			wantErr:      true,
			wantTerm:     false,
		},
		{
			name:         "HandleMergeConflictJob error dispatching prompt at limit terms",
			handlerType:  "conflict",
			payload:      `{"repo":"azylman/aerial","pr_number":42,"target_id":"100000000000000001"}`,
			dispErrType:  "prompt_err",
			numDelivered: 5,
			wantErr:      false,
			wantTerm:     true,
		},
	}
}

func jetstreamDiscordAndWidgetCases() []jetstreamHandlerCase {
	return []jetstreamHandlerCase{
		{
			name:        "HandleDiscordIngressJob malformed json",
			handlerType: "ingress",
			payload:     `{bad json`,
			wantTerm:    true,
		},
		{
			name:        "HandleDiscordIngressJob stale message terms",
			handlerType: "ingress",
			payload:     fmt.Sprintf(`{"message_id":"123","message":{"id":"123","timestamp":%q}}`, time.Now().Add(-35*time.Minute).Format(time.RFC3339)),
			wantTerm:    true,
		},
		{
			name:        "HandleDiscordIngressJob valid 200 payload",
			handlerType: "ingress",
			payload:     `{"message_id":"123","message":{"id":"123"}}`,
			wantErr:     false,
		},
		{
			name:        "HandleDiscordIngressJob client 4xx terms",
			handlerType: "ingress",
			payload:     `{"message_id":"124","message":{"id":"124"}}`,
			dispErrType: "msg_4xx",
			wantTerm:    true,
			wantErr:     false,
		},
		{
			name:         "HandleDiscordIngressJob 5xx retries below limit",
			handlerType:  "ingress",
			payload:      `{"message_id":"125","message":{"id":"125"}}`,
			dispErrType:  "msg_500",
			numDelivered: 1,
			wantErr:      true,
			wantTerm:     false,
		},
		{
			name:         "HandleDiscordIngressJob 5xx at limit terms",
			handlerType:  "ingress",
			payload:      `{"message_id":"125","message":{"id":"125"}}`,
			dispErrType:  "msg_500",
			numDelivered: 30,
			wantErr:      false,
			wantTerm:     true,
		},
		{
			name:        "HandleDiscordInteractionJob malformed json",
			handlerType: "interaction",
			payload:     `{bad json`,
			wantTerm:    true,
		},
		{
			name:        "HandleDiscordInteractionJob valid payload",
			handlerType: "interaction",
			payload:     `{"interaction_id":"ix-1"}`,
			wantErr:     false,
		},
		{
			name:         "HandleDiscordInteractionJob dispatch error at limit terms",
			handlerType:  "interaction",
			payload:      `{"interaction_id":"ix-2","interaction":{}}`,
			dispErrType:  "interaction_err",
			numDelivered: 1,
			wantErr:      false,
			wantTerm:     true,
		},
		{
			name:        "HandleWidgetSyncJob malformed json",
			handlerType: "widget",
			payload:     `{bad json`,
			wantTerm:    true,
		},
		{
			name:        "HandleWidgetSyncJob valid payload with mock registry",
			handlerType: "widget",
			payload:     `{"repo":"azylman/aerial","pr_number":99}`,
			withMockReg: true,
			wantErr:     false,
		},
		{
			name:         "HandleWidgetSyncJob dispatch error below limit",
			handlerType:  "widget",
			payload:      `{"repo":"azylman/aerial","pr_number":99}`,
			dispErrType:  "widget_err",
			withMockReg:  true,
			numDelivered: 1,
			wantErr:      true,
			wantTerm:     false,
		},
		{
			name:         "HandleWidgetSyncJob dispatch error at limit terms",
			handlerType:  "widget",
			payload:      `{"repo":"azylman/aerial","pr_number":99}`,
			dispErrType:  "widget_err",
			withMockReg:  true,
			numDelivered: 5,
			wantErr:      false,
			wantTerm:     true,
		},
	}
}

func jetstreamJobHandlerCases() []jetstreamHandlerCase {
	return append(jetstreamWebhookAndDeployCases(), jetstreamDiscordAndWidgetCases()...)
}

func TestJetStreamJobHandlers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range jetstreamJobHandlerCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := &RouterServer{
				cfgSrc: fixedConfig(Config{SystemChannelID: "100000000000000001"}),
			}
			disp := &mockOutboundDispatcher{}
			switch tc.dispErrType {
			case "msg_4xx":
				disp.discordMessageErr = errors.New("request returned status 400 Bad Request")
			case "msg_500":
				disp.discordMessageErr = errors.New("request returned status 500 Internal Server Error")
			case "interaction_err":
				disp.discordInteractionErr = errors.New("interaction dispatch failed")
			case "widget_err":
				disp.widgetErr = errors.New("widget dispatch failed")
			case "prompt_err":
				disp.promptErr = errors.New("prompt dispatch failed")
				disp.dispatchErr = errors.New("dispatch background failed")
			}
			srv.SetDispatcher(disp)
			if tc.withMockReg {
				srv.registry = &mockPRRegistry{
					resolveTargetID: "100000000000000001",
					resolveBranch:   "main",
				}
			}

			memMsg := &synapseMemoryMsg{
				data:         []byte(tc.payload),
				numDelivered: tc.numDelivered,
			}

			var err error
			switch tc.handlerType {
			case "github":
				err = srv.HandleGitHubJob(ctx, memMsg)
			case "deploy":
				err = srv.HandleDeployJob(ctx, memMsg)
			case "conflict":
				err = srv.HandleMergeConflictJob(ctx, memMsg)
			case "ingress":
				err = srv.HandleDiscordIngressJob(ctx, memMsg)
			case "interaction":
				err = srv.HandleDiscordInteractionJob(ctx, memMsg)
			case "widget":
				err = srv.HandleWidgetSyncJob(ctx, memMsg)
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("handler err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantTerm && !memMsg.termed {
				t.Errorf("expected msg.Term() called")
			}
		})
	}
}

type wiringDispatcher struct {
	mockOutboundDispatcher

	onPrompt      func()
	onBackground  func()
	onMessage     func()
	onInteraction func()
	onWidget      func()
}

func (w *wiringDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	err := w.mockOutboundDispatcher.DispatchPrompt(ctx, req)
	if w.onPrompt != nil {
		w.onPrompt()
	}
	return err
}

func (w *wiringDispatcher) DispatchBackground(ctx context.Context, req DispatchRequest) error {
	err := w.mockOutboundDispatcher.DispatchBackground(ctx, req)
	if w.onBackground != nil {
		w.onBackground()
	}
	return err
}

func (w *wiringDispatcher) DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error {
	err := w.mockOutboundDispatcher.DispatchDiscordMessage(ctx, req)
	if w.onMessage != nil {
		w.onMessage()
	}
	return err
}

func (w *wiringDispatcher) DispatchDiscordInteraction(ctx context.Context, ix *discordgo.Interaction) error {
	err := w.mockOutboundDispatcher.DispatchDiscordInteraction(ctx, ix)
	if w.onInteraction != nil {
		w.onInteraction()
	}
	return err
}

func (w *wiringDispatcher) DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error) {
	id, err := w.mockOutboundDispatcher.DispatchWidget(ctx, payload)
	if w.onWidget != nil {
		w.onWidget()
	}
	return id, err
}

func TestStartJobConsumersAndPublishWiring(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	srv := &RouterServer{
		cfgSrc: fixedConfig(Config{SystemChannelID: "100000000000000001"}),
	}
	disp := &wiringDispatcher{}
	srv.SetDispatcher(disp)
	srv.registry = &mockPRRegistry{
		resolveTargetID: "100000000000000001",
		resolveBranch:   "main",
	}
	srv.SetEventBus(memBus)

	if err := srv.StartJobConsumers(ctx); err != nil {
		t.Fatalf("StartJobConsumers() error: %v", err)
	}

	ghCh := make(chan struct{}, 1)
	srv.onProcessed = func(_, delivery string) {
		if delivery == "wire-gh" {
			select {
			case ghCh <- struct{}{}:
			default:
			}
		}
	}

	deployCh := make(chan struct{}, 1)
	conflictCh := make(chan struct{}, 1)
	ingressCh := make(chan struct{}, 1)
	interactionCh := make(chan struct{}, 1)
	widgetCh := make(chan struct{}, 1)

	disp.onWidget = func() {
		select {
		case deployCh <- struct{}{}:
		default:
		}
		select {
		case widgetCh <- struct{}{}:
		default:
		}
	}
	disp.onBackground = func() {
		select {
		case conflictCh <- struct{}{}:
		default:
		}
	}
	disp.onMessage = func() {
		select {
		case ingressCh <- struct{}{}:
		default:
		}
	}
	disp.onInteraction = func() {
		select {
		case interactionCh <- struct{}{}:
		default:
		}
	}

	cases := []struct {
		name   string
		args   riverJobArgsWithOpts
		signal <-chan struct{}
	}{
		{
			name:   "github",
			args:   GitHubWebhookArgs{Event: "ping", Delivery: "wire-gh", Body: []byte("{}")},
			signal: ghCh,
		},
		{
			name: "deploy",
			args: DeployWebhookArgs{
				IdempotencyKey: "wire-dep",
				Event: DeployEvent{
					Event:    "deploy_started",
					Repo:     "azylman/aerial",
					PRNumber: 10,
					TargetID: "100000000000000001",
				},
			},
			signal: deployCh,
		},
		{
			name:   "merge_conflict",
			args:   MergeConflictArgs{Repo: "azylman/aerial", PRNumber: 10, TargetID: "100000000000000001"},
			signal: conflictCh,
		},
		{
			name:   "discord_ingress",
			args:   DiscordIngressArgs{MessageID: "wire-ing", Message: &discordgo.Message{ID: "wire-ing"}},
			signal: ingressCh,
		},
		{
			name:   "discord_interaction",
			args:   DiscordInteractionArgs{InteractionID: "wire-ix"},
			signal: interactionCh,
		},
		{
			name:   "widget_sync",
			args:   WidgetSyncArgs{Repo: "azylman/aerial", PRNumber: 10},
			signal: widgetCh,
		},
	}

	for _, tc := range cases {
		if err := srv.insertRiverJob(ctx, tc.args); err != nil {
			t.Fatalf("insertRiverJob(%s) error: %v", tc.name, err)
		}
		select {
		case <-tc.signal:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s job consumer", tc.name)
		}
	}
}
