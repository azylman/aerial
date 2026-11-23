package router

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
)

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

// jetstreamJobRouter is a router whose dispatcher is disp, retrying Discord
// ingress after retryDelay (1ms when zero).
func jetstreamJobRouter(disp *mockOutboundDispatcher, retryDelay time.Duration) *RouterServer {
	if retryDelay == 0 {
		retryDelay = time.Millisecond
	}
	srv := &RouterServer{cfgSrc: fixedConfig(Config{SystemChannelID: "100000000000000001"}), discordIngressRetryDelay: retryDelay}
	srv.SetDispatcher(disp)
	return srv
}

// jetstreamJobMsg is a fake JetStream message with the given delivery count (at least 1).
func jetstreamJobMsg(payload string, delivered uint64) *pubsubtest.Message {
	return &pubsubtest.Message{Payload: []byte(payload), Delivered: max(delivered, 1)}
}

func assertJetstreamJobOutcome(t *testing.T, err error, msg *pubsubtest.Message, wantErr, wantTerm bool) {
	t.Helper()
	if (err != nil) != wantErr {
		t.Fatalf("handler err = %v, wantErr = %v", err, wantErr)
	}
	if msg.Termed() != wantTerm {
		t.Errorf("msg.Term() called = %v, want %v", msg.Termed(), wantTerm)
	}
}

const (
	jetstreamBadGitHubJob = `{"event":"push","delivery":"d-2","body":"e2JhZA=="}`
	jetstreamBadDeployJob = `{"idempotency_key":"k-2","event":{"event":"deploy_failed","repo":"acme/aerial","pr_number":0,"target_id":"100000000000000001"}}`
)

func TestHandleGitHubJob(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		payload   string
		delivered uint64
		wantErr   bool
		wantTerm  bool
	}{
		{name: "malformed json terms", payload: `{bad json`, wantTerm: true},
		{name: "processing error below the delivery limit naks", payload: jetstreamBadGitHubJob, delivered: 1, wantErr: true},
		{name: "processing error at the delivery limit terms", payload: jetstreamBadGitHubJob, delivered: 5, wantTerm: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := jetstreamJobMsg(tc.payload, tc.delivered)
			err := jetstreamJobRouter(&mockOutboundDispatcher{}, 0).HandleGitHubJob(context.Background(), msg)
			assertJetstreamJobOutcome(t, err, msg, tc.wantErr, tc.wantTerm)
		})
	}
}

func TestHandleDeployJob(t *testing.T) {
	t.Parallel()
	errPrompt, errDispatch := errors.New("prompt dispatch failed"), errors.New("dispatch background failed")
	tests := []struct {
		name      string
		payload   string
		delivered uint64
		disp      *mockOutboundDispatcher
		wantErr   bool
		wantTerm  bool
	}{
		{name: "malformed json terms", payload: `{bad json`, disp: &mockOutboundDispatcher{}, wantTerm: true},
		{
			name: "dispatch error below the delivery limit naks", payload: jetstreamBadDeployJob, delivered: 1,
			disp: &mockOutboundDispatcher{promptErr: errPrompt, dispatchErr: errDispatch}, wantErr: true,
		},
		{
			name: "dispatch error at the delivery limit terms", payload: jetstreamBadDeployJob, delivered: 5,
			disp: &mockOutboundDispatcher{promptErr: errPrompt, dispatchErr: errDispatch}, wantTerm: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := jetstreamJobMsg(tc.payload, tc.delivered)
			err := jetstreamJobRouter(tc.disp, 0).HandleDeployJob(context.Background(), msg)
			assertJetstreamJobOutcome(t, err, msg, tc.wantErr, tc.wantTerm)
		})
	}
}

func TestHandleDiscordIngressJob(t *testing.T) {
	t.Parallel()
	err4xx := errors.New("request returned status 400 Bad Request")
	err5xx := errors.New("request returned status 500 Internal Server Error")
	stale := fmt.Sprintf(`{"message_id":"123","message":{"id":"123","timestamp":%q}}`, time.Now().Add(-35*time.Minute).Format(time.RFC3339))
	tests := []struct {
		name             string
		payload          string
		delivered        uint64
		disp             *mockOutboundDispatcher
		retryDelay       time.Duration
		cancelled        bool // ctx is cancelled before the handler runs
		cancelOnDispatch bool // the failing dispatch cancels ctx, so the backoff wait exits
		wantErr          bool
		wantTerm         bool
	}{
		{name: "malformed json terms", payload: `{bad json`, disp: &mockOutboundDispatcher{}, wantTerm: true},
		{name: "stale message terms", payload: stale, disp: &mockOutboundDispatcher{}, wantTerm: true},
		{name: "client 4xx terms", payload: `{"message_id":"124","message":{"id":"124"}}`, disp: &mockOutboundDispatcher{discordMessageErr: err4xx}, wantTerm: true},
		{
			name: "cancelled context returns before dispatch", payload: `{"message_id":"125","message":{"id":"125"}}`, delivered: 1,
			disp: &mockOutboundDispatcher{discordMessageErr: err5xx}, cancelled: true, wantErr: true,
		},
		{
			name: "5xx cancelled during backoff", payload: `{"message_id":"127","message":{"id":"127"}}`, delivered: 1,
			disp: &mockOutboundDispatcher{discordMessageErr: err5xx}, retryDelay: 5 * time.Second, cancelOnDispatch: true, wantErr: true,
		},
		{
			name: "recovers after a temporary error", payload: `{"message_id":"126","message":{"id":"126"}}`, delivered: 1,
			disp: &mockOutboundDispatcher{discordMessageErr: err5xx, discordMessageFailCount: 1},
		},
		{
			name: "5xx at the delivery limit terms", payload: `{"message_id":"125","message":{"id":"125"}}`, delivered: 30,
			disp: &mockOutboundDispatcher{discordMessageErr: err5xx}, wantTerm: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			if tc.cancelOnDispatch {
				tc.disp.discordMessageCancel = cancel
			}
			msg := jetstreamJobMsg(tc.payload, tc.delivered)
			err := jetstreamJobRouter(tc.disp, tc.retryDelay).HandleDiscordIngressJob(ctx, msg)
			assertJetstreamJobOutcome(t, err, msg, tc.wantErr, tc.wantTerm)
		})
	}
}

func TestHandleDiscordInteractionJob(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		payload  string
		disp     *mockOutboundDispatcher
		wantErr  bool
		wantTerm bool
	}{
		{name: "malformed json terms", payload: `{bad json`, disp: &mockOutboundDispatcher{}, wantTerm: true},
		{
			name: "dispatch error at the single-delivery limit terms", payload: `{"interaction_id":"ix-2","interaction":{}}`,
			disp: &mockOutboundDispatcher{discordInteractionErr: errors.New("interaction dispatch failed")}, wantTerm: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := jetstreamJobMsg(tc.payload, 1)
			err := jetstreamJobRouter(tc.disp, 0).HandleDiscordInteractionJob(context.Background(), msg)
			assertJetstreamJobOutcome(t, err, msg, tc.wantErr, tc.wantTerm)
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
					Repo:     "acme/aerial",
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

func TestInitSynapseJetStream(t *testing.T) {
	origNewBus := newBusFn
	defer func() { newBusFn = origNewBus }()

	const natsURL = "nats://127.0.0.1:4222"
	errStream, errSub, errClose := errors.New("stream err"), errors.New("sub err"), errors.New("close err")
	tests := []struct {
		name          string
		natsURL       string
		busErr        error
		ensureErr     error
		consumeErr    error
		closeErr      error
		wantErr       bool
		wantBusCalled bool
	}{
		{name: "empty nats url returns noop without connecting", natsURL: ""},
		{name: "connect error returns error", natsURL: natsURL, busErr: errors.New("connection refused"), wantBusCalled: true, wantErr: true},
		{name: "ensure core streams error returns error", natsURL: natsURL, ensureErr: errStream, wantBusCalled: true, wantErr: true},
		{name: "start job consumers error returns error", natsURL: natsURL, consumeErr: errSub, wantBusCalled: true, wantErr: true},
		{name: "ensure core streams error with close error", natsURL: natsURL, ensureErr: errStream, closeErr: errClose, wantBusCalled: true, wantErr: true},
		{name: "start job consumers error with close error", natsURL: natsURL, consumeErr: errSub, closeErr: errClose, wantBusCalled: true, wantErr: true},
		{name: "successful connection with close error on teardown", natsURL: natsURL, closeErr: errClose, wantBusCalled: true},
		{name: "successful connection configures bus and starts consumers", natsURL: natsURL, wantBusCalled: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			newBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
				called = true
				if tc.busErr != nil {
					return nil, tc.busErr
				}
				return &pubsubtest.FaultBus{Bus: pubsub.NewMemoryBus(), EnsureErr: tc.ensureErr, ConsumeErr: tc.consumeErr, CloseErr: tc.closeErr}, nil
			}
			srv := &RouterServer{}
			stop, err := initSynapseJetStream(context.Background(), srv, tc.natsURL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("initSynapseJetStream() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if called != tc.wantBusCalled {
				t.Errorf("newBusFn called = %v, want %v", called, tc.wantBusCalled)
			}
			if tc.wantErr {
				if stop != nil {
					t.Error("expected nil stop func on error")
				}
				return
			}
			if stop == nil {
				t.Fatal("expected non-nil stop func")
			}
			stop()
			if tc.wantBusCalled && srv.EventBus() == nil {
				t.Errorf("expected non-nil event bus on server")
			}
		})
	}
}

func TestHandleJetStreamJobRestatePolicy(t *testing.T) {
	t.Parallel()
	retryable := &restateSendError{handler: "HandleMerged", err: errors.New("status 503")}
	terminal := &restateSendError{handler: "HandleMerged", terminal: true, err: errors.New("status 404")}
	tests := []struct {
		name         string
		numDelivered uint64
		processErr   error
		wantErr      bool
		wantTerm     bool
		wantNak      bool
		wantNakDelay time.Duration
	}{
		{name: "success acks", numDelivered: 1, processErr: nil},
		{name: "non-restate error naks on short backoff", numDelivered: 2, processErr: errors.New("db down"), wantErr: true, wantNak: true, wantNakDelay: 2 * time.Second},
		{name: "non-restate error terms at short budget", numDelivered: 5, processErr: errors.New("db down"), wantTerm: true},
		{name: "restate retryable outlives short budget", numDelivered: 5, processErr: retryable, wantErr: true, wantNak: true, wantNakDelay: 16 * time.Second},
		{name: "restate retryable caps backoff", numDelivered: 20, processErr: retryable, wantErr: true, wantNak: true, wantNakDelay: time.Minute},
		{name: "restate retryable terms at restate budget", numDelivered: 45, processErr: retryable, wantTerm: true},
		{name: "wrapped restate terminal drops job", numDelivered: 1, processErr: errors.Join(errors.New("other"), terminal), wantTerm: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := &pubsubtest.Message{Payload: []byte(`{}`), Subj: "aerial.jobs.synapse.github", Delivered: tc.numDelivered}
			err := handleJetStreamJob(context.Background(), msg, maxDeliverGitHubWebhook, "github", func(struct{}) error { return tc.processErr })
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleJetStreamJob() err = %v, wantErr %v", err, tc.wantErr)
			}
			if msg.Termed() != tc.wantTerm {
				t.Errorf("termed = %v, want %v", msg.Termed(), tc.wantTerm)
			}
			if nakDelay, naked := msg.NakDelay(); naked != tc.wantNak || nakDelay != tc.wantNakDelay {
				t.Errorf("nak = %v/%v, want %v/%v", naked, nakDelay, tc.wantNak, tc.wantNakDelay)
			}
		})
	}
}

func TestEventIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		source string
		id     string
	}{
		{name: "empty id has no identity", source: "github", id: ""},
		{name: "blank whitespace id has no identity", source: "github", id: "   "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := eventIdentity(tc.source, tc.id); got != "" {
				t.Errorf("eventIdentity(%q, %q) = %q, want empty", tc.source, tc.id, got)
			}
		})
	}
}
