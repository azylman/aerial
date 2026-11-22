package router

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

type synapseMemoryMsg struct {
	data         []byte
	subject      string
	numDelivered uint64
	termed       bool
}

func (m *synapseMemoryMsg) Data() []byte      { return m.data }
func (m *synapseMemoryMsg) Subject() string   { return m.subject }
func (*synapseMemoryMsg) Header() nats.Header { return nil }
func (m *synapseMemoryMsg) NumDelivered() uint64 {
	if m.numDelivered > 0 {
		return m.numDelivered
	}
	return 1
}
func (*synapseMemoryMsg) Ack() error                         { return nil }
func (*synapseMemoryMsg) NakWithDelay(_ time.Duration) error { return nil }
func (m *synapseMemoryMsg) Term() error                      { m.termed = true; return nil }
func (*synapseMemoryMsg) Snooze(_ time.Duration) error       { return nil }
func (*synapseMemoryMsg) InProgress() error                  { return nil }

func TestDiscordRoutingForJetStreamJobCoords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		args        any
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
			name:        "DiscordIngressArgs",
			args:        DiscordIngressArgs{MessageID: "msg-999"},
			wantSubject: "aerial.jobs.synapse.discord_ingress.beta",
			wantPrefix:  "discord:msg:msg-999",
		},
		{
			name:        "*DiscordIngressArgs",
			args:        &DiscordIngressArgs{MessageID: "msg-888"},
			wantSubject: "aerial.jobs.synapse.discord_ingress.beta",
			wantPrefix:  "discord:msg:msg-888",
		},
		{
			name:        "DiscordIngressArgs non-default agent",
			args:        DiscordIngressArgs{MessageID: "msg-777", Agent: "alpha"},
			wantSubject: "aerial.jobs.synapse.discord_ingress.alpha",
			wantPrefix:  "discord:msg:alpha:msg-777",
		},
		{
			name:        "DiscordInteractionArgs",
			args:        DiscordInteractionArgs{InteractionID: "ix-111"},
			wantSubject: "aerial.jobs.synapse.discord_interaction.beta",
			wantPrefix:  "discord:ix:ix-111",
		},
		{
			name:        "*DiscordInteractionArgs",
			args:        &DiscordInteractionArgs{InteractionID: "ix-222"},
			wantSubject: "aerial.jobs.synapse.discord_interaction.beta",
			wantPrefix:  "discord:ix:ix-222",
		},
		{
			name:        "DiscordInteractionArgs non-default agent",
			args:        DiscordInteractionArgs{InteractionID: "ix-333", Agent: "Alpha"},
			wantSubject: "aerial.jobs.synapse.discord_interaction.alpha",
			wantPrefix:  "discord:ix:Alpha:ix-333",
		},
	}
	routing := discordRoutingFor(&Config{Bots: map[string]BotConfig{"alpha": {}, "beta": {DefaultOwner: true}}})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subj, msgID := metrics.ResolveJetStreamJobCoords(tc.args, routing)
			if subj != tc.wantSubject {
				t.Errorf("subject = %q, want %q", subj, tc.wantSubject)
			}
			if !strings.HasPrefix(msgID, tc.wantPrefix) {
				t.Errorf("msgID = %q, want prefix %q", msgID, tc.wantPrefix)
			}
		})
	}
}

func initTestJobBus(ctx context.Context, busErr error, busReceived chan struct{}) pubsub.Bus {
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

func TestPublishJetStreamJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name    string
		hasBus  bool
		busErr  error
		args    any
		wantErr bool
	}{
		{
			name:    "publishes to jetstream when bus available",
			hasBus:  true,
			args:    GitHubWebhookArgs{Delivery: "d1"},
			wantErr: false,
		},
		{
			name:    "returns error when bus publish fails",
			hasBus:  true,
			busErr:  errors.New("nats down"),
			args:    GitHubWebhookArgs{Delivery: "d2"},
			wantErr: true,
		},
		{
			name:    "returns error when bus nil",
			hasBus:  false,
			args:    GitHubWebhookArgs{Delivery: "d3"},
			wantErr: true,
		},
		{
			name:    "returns error for unsupported job args",
			hasBus:  true,
			args:    "invalid args string",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := &RouterServer{}
			busReceived := make(chan struct{}, 1)
			if tc.hasBus {
				memBus := initTestJobBus(ctx, tc.busErr, busReceived)
				t.Cleanup(func() { _ = memBus.Close() })
				srv.SetEventBus(memBus)
			}

			err := srv.publishJetStreamJob(ctx, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("publishJetStreamJob() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.hasBus && tc.busErr == nil && !tc.wantErr {
				select {
				case <-busReceived:
				case <-time.After(2 * time.Second):
					t.Errorf("expected bus to receive published job")
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
			name:         "HandleDeployJob error processing payload below limit",
			handlerType:  "deploy",
			payload:      `{"idempotency_key":"k-2","event":{"event":"deploy_failed","repo":"azylman/aerial","pr_number":0,"target_id":"100000000000000001"}}`,
			dispErrType:  "prompt_err",
			numDelivered: 1,
			wantErr:      true,
			wantTerm:     false,
		},
		{
			name:         "HandleDeployJob error processing payload at limit terms",
			handlerType:  "deploy",
			payload:      `{"idempotency_key":"k-2","event":{"event":"deploy_failed","repo":"azylman/aerial","pr_number":0,"target_id":"100000000000000001"}}`,
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
			name:         "HandleDiscordIngressJob recovers after temporary error",
			handlerType:  "ingress",
			payload:      `{"message_id":"126","message":{"id":"126"}}`,
			dispErrType:  "msg_recover",
			numDelivered: 1,
			wantErr:      false,
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
			name:         "HandleDiscordInteractionJob dispatch error at limit terms",
			handlerType:  "interaction",
			payload:      `{"interaction_id":"ix-2","interaction":{}}`,
			dispErrType:  "interaction_err",
			numDelivered: 1,
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
				cfgSrc:                   fixedConfig(Config{SystemChannelID: "100000000000000001"}),
				discordIngressRetryDelay: 1 * time.Millisecond,
			}
			disp := &mockOutboundDispatcher{}
			switch tc.dispErrType {
			case "msg_4xx":
				disp.discordMessageErr = errors.New("request returned status 400 Bad Request")
			case "msg_recover":
				disp.discordMessageErr = errors.New("request returned status 500 Internal Server Error")
				disp.discordMessageFailCount = 1
			case "msg_500":
				disp.discordMessageErr = errors.New("request returned status 500 Internal Server Error")
			case "interaction_err":
				disp.discordInteractionErr = errors.New("interaction dispatch failed")
			case "prompt_err":
				disp.promptErr = errors.New("prompt dispatch failed")
				disp.dispatchErr = errors.New("dispatch background failed")
			}
			srv.SetDispatcher(disp)

			memMsg := &synapseMemoryMsg{
				data:         []byte(tc.payload),
				numDelivered: tc.numDelivered,
			}

			testCtx := ctx
			if tc.dispErrType == "msg_500" && tc.wantErr {
				var cancel context.CancelFunc
				testCtx, cancel = context.WithTimeout(ctx, 15*time.Millisecond)
				defer cancel()
			}

			var err error
			switch tc.handlerType {
			case "github":
				err = srv.HandleGitHubJob(testCtx, memMsg)
			case "deploy":
				err = srv.HandleDeployJob(testCtx, memMsg)
			case "ingress":
				err = srv.HandleDiscordIngressJob(testCtx, memMsg)
			case "interaction":
				err = srv.HandleDiscordInteractionJob(testCtx, memMsg)
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

	onDirect      func()
	onBackground  func()
	onMessage     func()
	onInteraction func()
}

func (w *wiringDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	err := w.mockOutboundDispatcher.DispatchDirectMessage(ctx, req)
	if w.onDirect != nil {
		w.onDirect()
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

func (w *wiringDispatcher) DispatchDiscordInteractionForAgent(ctx context.Context, agent string, ix *discordgo.Interaction) error {
	err := w.mockOutboundDispatcher.DispatchDiscordInteractionForAgent(ctx, agent, ix)
	if w.onInteraction != nil {
		w.onInteraction()
	}
	return err
}

func TestStartJobConsumersAndPublishWiring(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	srv := &RouterServer{
		cfgSrc: fixedConfig(Config{
			SystemChannelID: "100000000000000001",
			Bots:            map[string]BotConfig{"alpha": {DefaultOwner: true}, "beta": {}},
		}),
	}
	disp := &wiringDispatcher{}
	srv.SetDispatcher(disp)
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
	ingressCh := make(chan struct{}, 1)
	interactionCh := make(chan struct{}, 1)

	disp.onDirect = func() {
		select {
		case deployCh <- struct{}{}:
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
		args   any
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
					Event:    "deploy_failed",
					Repo:     "azylman/aerial",
					TargetID: "100000000000000001",
				},
			},
			signal: deployCh,
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
			name:   "discord_ingress non-default agent",
			args:   DiscordIngressArgs{MessageID: "wire-ing-beta", Agent: "beta", Message: &discordgo.Message{ID: "wire-ing-beta"}},
			signal: ingressCh,
		},
		{
			name:   "discord_interaction non-default agent",
			args:   DiscordInteractionArgs{InteractionID: "wire-ix-beta", Agent: "beta"},
			signal: interactionCh,
		},
	}

	for _, tc := range cases {
		if err := srv.publishJetStreamJob(ctx, tc.args); err != nil {
			t.Fatalf("publish(%s) error: %v", tc.name, err)
		}
		select {
		case <-tc.signal:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s job consumer", tc.name)
		}
	}
}

func TestJobArgsKindsAndResolveJobQueueAndKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		args      any
		wantKind  string
		wantQueue string
	}{
		{
			name:      "GitHubWebhookArgs value",
			args:      GitHubWebhookArgs{},
			wantKind:  "github_webhook",
			wantQueue: QueueDefault,
		},
		{
			name:      "GitHubWebhookArgs pointer",
			args:      &GitHubWebhookArgs{},
			wantKind:  "github_webhook",
			wantQueue: QueueDefault,
		},
		{
			name:      "DeployWebhookArgs value",
			args:      DeployWebhookArgs{},
			wantKind:  "deploy_webhook",
			wantQueue: QueueDefault,
		},
		{
			name:      "DiscordIngressArgs value",
			args:      DiscordIngressArgs{},
			wantKind:  "discord_ingress",
			wantQueue: QueueDiscordIngress,
		},
		{
			name:      "DiscordIngressArgs pointer",
			args:      &DiscordIngressArgs{},
			wantKind:  "discord_ingress",
			wantQueue: QueueDiscordIngress,
		},
		{
			name:      "DiscordInteractionArgs value",
			args:      DiscordInteractionArgs{},
			wantKind:  "discord_interaction",
			wantQueue: QueueDiscordInteraction,
		},
		{
			name:      "DiscordInteractionArgs pointer",
			args:      &DiscordInteractionArgs{},
			wantKind:  "discord_interaction",
			wantQueue: QueueDiscordInteraction,
		},
		{
			name:      "unknown args fallback",
			args:      "unexpected payload",
			wantKind:  "unknown",
			wantQueue: QueueDefault,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotQueue, gotKind := resolveJobQueueAndKind(tc.args)
			if gotQueue != tc.wantQueue || gotKind != tc.wantKind {
				t.Errorf("resolveJobQueueAndKind(%T) = (%q, %q), want (%q, %q)", tc.args, gotQueue, gotKind, tc.wantQueue, tc.wantKind)
			}
		})
	}
}

func TestJobConsumerSpecs(t *testing.T) {
	t.Parallel()

	static := []string{
		"aerial.jobs.synapse.github synapse-github",
		"aerial.jobs.synapse.deploy synapse-deploy",
	}
	tests := []struct {
		name      string
		cfg       *Config
		wantAgent []string
	}{
		{
			name: "multiple bots subscribe one durable pair each in sorted order",
			cfg:  &Config{Bots: map[string]BotConfig{"gamma": {}, "alpha": {DefaultOwner: true}, "beta": {}}},
			wantAgent: []string{
				"aerial.jobs.synapse.discord_ingress.alpha synapse-discord-ingress-alpha",
				"aerial.jobs.synapse.discord_interaction.alpha synapse-discord-interaction-alpha",
				"aerial.jobs.synapse.discord_ingress.beta synapse-discord-ingress-beta",
				"aerial.jobs.synapse.discord_interaction.beta synapse-discord-interaction-beta",
				"aerial.jobs.synapse.discord_ingress.gamma synapse-discord-ingress-gamma",
				"aerial.jobs.synapse.discord_interaction.gamma synapse-discord-interaction-gamma",
			},
		},
		{
			name: "single bot without explicit default owner",
			cfg:  &Config{Bots: map[string]BotConfig{"delta": {}}},
			wantAgent: []string{
				"aerial.jobs.synapse.discord_ingress.delta synapse-discord-ingress-delta",
				"aerial.jobs.synapse.discord_interaction.delta synapse-discord-interaction-delta",
			},
		},
		{
			name: "agent needing sanitization",
			cfg:  &Config{Bots: map[string]BotConfig{"ops.bot": {DefaultOwner: true}}},
			wantAgent: []string{
				"aerial.jobs.synapse.discord_ingress.ops_bot synapse-discord-ingress-ops_bot",
				"aerial.jobs.synapse.discord_interaction.ops_bot synapse-discord-interaction-ops_bot",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := &RouterServer{}
			var got []string
			for _, spec := range srv.jobConsumerSpecs(discordRoutingFor(tc.cfg)) {
				if spec.handler == nil {
					t.Errorf("spec %s has nil handler", spec.queue)
				}
				got = append(got, spec.subject+" "+spec.queue)
			}
			want := append(slices.Clone(static), tc.wantAgent...)
			if !slices.Equal(got, want) {
				t.Errorf("jobConsumerSpecs() =\n%q\nwant\n%q", got, want)
			}
		})
	}
}
