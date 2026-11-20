package metrics

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

type mockConsumerStatsQuerier struct {
	stats map[string][2]int64
	err   error
}

func (m *mockConsumerStatsQuerier) ConsumerStats(ctx context.Context, stream, consumer string) (int64, int64, error) {
	if m.err != nil {
		return 0, 0, m.err
	}
	if v, ok := m.stats[stream+"/"+consumer]; ok {
		return v[0], v[1], nil
	}
	return 0, 0, nil
}

func TestCollectJetStreamQueueMetrics(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		nilQuerier     bool
		querierErr     error
		bots           []string
		defaultAgent   string
		stats          map[string][2]int64
		wantErr        bool
		wantSubstr     string
		wantDepth      map[[2]string]float64
		wantAgentDepth map[[3]string]float64
	}{
		{
			name:         "nil querier zeroes known queues without error",
			nilQuerier:   true,
			defaultAgent: "alpha",
			wantDepth: map[[2]string]float64{
				{"discord_ingress", "pending"}:       0,
				{"discord_interaction", "in_flight"}: 0,
			},
		},
		{
			name:         "querier error returns wrapped error",
			querierErr:   errors.New("nats down"),
			defaultAgent: "alpha",
			wantErr:      true,
			wantSubstr:   "consumer stats",
		},
		{
			name:         "aggregates pending and in_flight across static consumers",
			defaultAgent: "alpha",
			stats: map[string][2]int64{
				"AERIAL_JOBS/synapse-github":              {3, 1},
				"AERIAL_JOBS/synapse-deploy":              {4, 2},
				"AERIAL_JOBS/synapse-discord-ingress":     {5, 3},
				"AERIAL_JOBS/synapse-discord-interaction": {2, 1},
			},
			wantDepth: map[[2]string]float64{
				{"default", "pending"}:               7,
				{"default", "in_flight"}:             3,
				{"discord_ingress", "pending"}:       5,
				{"discord_interaction", "in_flight"}: 1,
			},
		},
		{
			name:         "sums per-agent and legacy drain durables into existing discord series",
			bots:         []string{"beta", "alpha"},
			defaultAgent: "alpha",
			stats: map[string][2]int64{
				"AERIAL_JOBS/synapse-discord-ingress":           {1, 0},
				"AERIAL_JOBS/synapse-discord-interaction":       {1, 1},
				"AERIAL_JOBS/synapse-discord-ingress-alpha":     {2, 1},
				"AERIAL_JOBS/synapse-discord-ingress-beta":      {7, 1},
				"AERIAL_JOBS/synapse-discord-interaction-alpha": {0, 0},
				"AERIAL_JOBS/synapse-discord-interaction-beta":  {3, 2},
			},
			wantDepth: map[[2]string]float64{
				{"discord_ingress", "pending"}:       10,
				{"discord_ingress", "in_flight"}:     2,
				{"discord_interaction", "pending"}:   4,
				{"discord_interaction", "in_flight"}: 3,
			},
			wantAgentDepth: map[[3]string]float64{
				{"discord_ingress", "alpha", "pending"}:       2,
				{"discord_ingress", "beta", "pending"}:        7,
				{"discord_ingress", "beta", "in_flight"}:      1,
				{"discord_interaction", "beta", "pending"}:    3,
				{"discord_interaction", "alpha", "in_flight"}: 0,
			},
		},
		{
			name:         "default owner outside bots and sanitized agent get their own durables",
			bots:         []string{" Ops.Bot "},
			defaultAgent: "gamma",
			stats: map[string][2]int64{
				"AERIAL_JOBS/synapse-discord-ingress-ops_bot": {4, 0},
				"AERIAL_JOBS/synapse-discord-ingress-gamma":   {6, 2},
			},
			wantDepth: map[[2]string]float64{
				{"discord_ingress", "pending"}:   10,
				{"discord_ingress", "in_flight"}: 2,
			},
			wantAgentDepth: map[[3]string]float64{
				{"discord_ingress", "ops_bot", "pending"}: 4,
				{"discord_ingress", "gamma", "in_flight"}: 2,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var q ConsumerStatsQuerier
			if !tc.nilQuerier {
				q = &mockConsumerStatsQuerier{stats: tc.stats, err: tc.querierErr}
			}
			err := CollectJetStreamQueueMetrics(ctx, q, NewDiscordRouting(tc.bots, tc.defaultAgent))
			if (err != nil) != tc.wantErr {
				t.Fatalf("CollectJetStreamQueueMetrics() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			for labels, want := range tc.wantDepth {
				if got := testutil.ToFloat64(QueueDepth.WithLabelValues(labels[0], labels[1])); got != want {
					t.Errorf("QueueDepth%v = %v, want %v", labels, got, want)
				}
			}
			for labels, want := range tc.wantAgentDepth {
				if got := testutil.ToFloat64(DiscordAgentQueueDepth.WithLabelValues(labels[0], labels[1], labels[2])); got != want {
					t.Errorf("DiscordAgentQueueDepth%v = %v, want %v", labels, got, want)
				}
			}
		})
	}
}

func TestRecordJobHelpers(t *testing.T) {
	RecordJobEnqueued("", "", "")
	RecordJobEnqueued("default", "test_kind", "success")
	if v := testutil.ToFloat64(JobsEnqueuedTotal.WithLabelValues("default", "unknown", "unknown")); v < 1 {
		t.Errorf("JobsEnqueuedTotal default/unknown/unknown = %v, want >= 1", v)
	}

	RecordJobRetry("", "", "")
	RecordJobRetry("default", "test_kind", "error")
	if v := testutil.ToFloat64(JobRetriesTotal.WithLabelValues("default", "unknown", "unknown")); v < 1 {
		t.Errorf("JobRetriesTotal default/unknown/unknown = %v, want >= 1", v)
	}

	RecordJobDropped("", "", "")
	RecordJobDropped("default", "test_kind", "cancelled")
	if v := testutil.ToFloat64(JobsDroppedTotal.WithLabelValues("default", "unknown", "unknown")); v < 1 {
		t.Errorf("JobsDroppedTotal default/unknown/unknown = %v, want >= 1", v)
	}

	SetDiscordGatewayLatency(0.42)
	if v := testutil.ToFloat64(DiscordGatewayLatencySeconds); v != 0.42 {
		t.Errorf("DiscordGatewayLatencySeconds = %v, want 0.42", v)
	}
}

func TestNormalizeJobArgs(t *testing.T) {
	t.Parallel()

	gh := model.GitHubWebhookArgs{Event: "push"}
	deploy := model.DeployWebhookArgs{IdempotencyKey: "k1"}
	ingress := model.DiscordIngressArgs{MessageID: "m1"}
	interaction := model.DiscordInteractionArgs{InteractionID: "i1"}

	tests := []struct {
		name string
		in   any
		want any
	}{
		{name: "nil github ptr", in: (*model.GitHubWebhookArgs)(nil), want: (*model.GitHubWebhookArgs)(nil)},
		{name: "github ptr", in: &gh, want: gh},
		{name: "nil deploy ptr", in: (*model.DeployWebhookArgs)(nil), want: (*model.DeployWebhookArgs)(nil)},
		{name: "deploy ptr", in: &deploy, want: deploy},
		{name: "nil ingress ptr", in: (*model.DiscordIngressArgs)(nil), want: (*model.DiscordIngressArgs)(nil)},
		{name: "ingress ptr", in: &ingress, want: ingress},
		{name: "nil interaction ptr", in: (*model.DiscordInteractionArgs)(nil), want: (*model.DiscordInteractionArgs)(nil)},
		{name: "interaction ptr", in: &interaction, want: interaction},
		{name: "plain string", in: "unsupported", want: "unsupported"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeJobArgs(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("NormalizeJobArgs(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveJobQueueAndKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        any
		wantQueue string
		wantKind  string
	}{
		{name: "github args", in: model.GitHubWebhookArgs{}, wantQueue: QueueDefault, wantKind: "github_webhook"},
		{name: "deploy args", in: model.DeployWebhookArgs{}, wantQueue: QueueDefault, wantKind: "deploy_webhook"},
		{name: "ingress args", in: model.DiscordIngressArgs{}, wantQueue: QueueDiscordIngress, wantKind: "discord_ingress"},
		{name: "interaction args", in: model.DiscordInteractionArgs{}, wantQueue: QueueDiscordInteraction, wantKind: "discord_interaction"},
		{name: "unknown", in: "string", wantQueue: QueueDefault, wantKind: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q, k := ResolveJobQueueAndKind(tc.in)
			if q != tc.wantQueue || k != tc.wantKind {
				t.Errorf("ResolveJobQueueAndKind() = (%q, %q), want (%q, %q)", q, k, tc.wantQueue, tc.wantKind)
			}
		})
	}
}

func TestResolveJetStreamJobCoords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		in           any
		bots         []string
		defaultAgent string
		wantSubject  string
		wantMsgID    string
	}{
		{
			name:         "github args",
			in:           model.GitHubWebhookArgs{Delivery: "d123"},
			defaultAgent: "alpha",
			wantSubject:  "aerial.jobs.synapse.github",
			wantMsgID:    "github:d123",
		},
		{
			name:         "deploy args",
			in:           model.DeployWebhookArgs{IdempotencyKey: "dep123"},
			defaultAgent: "alpha",
			wantSubject:  "aerial.jobs.synapse.deploy",
			wantMsgID:    "deploy:dep123",
		},
		{
			name:         "ingress args without agent route to default owner",
			in:           model.DiscordIngressArgs{MessageID: "msg123"},
			bots:         []string{"alpha", "beta"},
			defaultAgent: "beta",
			wantSubject:  "aerial.jobs.synapse.discord_ingress.beta",
			wantMsgID:    "discord:msg:msg123",
		},
		{
			name:         "ingress args with agent",
			in:           model.DiscordIngressArgs{MessageID: "msg123", Agent: "alpha"},
			bots:         []string{"alpha", "beta"},
			defaultAgent: "beta",
			wantSubject:  "aerial.jobs.synapse.discord_ingress.alpha",
			wantMsgID:    "discord:msg:alpha:msg123",
		},
		{
			name:         "ingress args with padded mixed-case agent keep raw dedup id",
			in:           model.DiscordIngressArgs{MessageID: "msg123", Agent: " Alpha "},
			bots:         []string{"alpha"},
			defaultAgent: "alpha",
			wantSubject:  "aerial.jobs.synapse.discord_ingress.alpha",
			wantMsgID:    "discord:msg:Alpha:msg123",
		},
		{
			name:         "ingress args with unconfigured agent route to default owner",
			in:           model.DiscordIngressArgs{MessageID: "msg123", Agent: "delta"},
			bots:         []string{"alpha", "beta"},
			defaultAgent: "alpha",
			wantSubject:  "aerial.jobs.synapse.discord_ingress.alpha",
			wantMsgID:    "discord:msg:delta:msg123",
		},
		{
			name:         "ingress args with agent needing sanitization",
			in:           model.DiscordIngressArgs{MessageID: "msg123", Agent: "ops.bot>*"},
			bots:         []string{"ops.bot>*"},
			defaultAgent: "alpha",
			wantSubject:  "aerial.jobs.synapse.discord_ingress.ops_bot__",
			wantMsgID:    "discord:msg:ops.bot>*:msg123",
		},
		{
			name:         "interaction args without agent route to default owner",
			in:           model.DiscordInteractionArgs{InteractionID: "int123"},
			bots:         []string{"alpha", "beta"},
			defaultAgent: "alpha",
			wantSubject:  "aerial.jobs.synapse.discord_interaction.alpha",
			wantMsgID:    "discord:ix:int123",
		},
		{
			name:         "interaction args with agent",
			in:           model.DiscordInteractionArgs{InteractionID: "int123", Agent: "beta"},
			bots:         []string{"alpha", "beta"},
			defaultAgent: "alpha",
			wantSubject:  "aerial.jobs.synapse.discord_interaction.beta",
			wantMsgID:    "discord:ix:beta:int123",
		},
		{
			name:         "interaction pointer args with no bots use default owner",
			in:           &model.DiscordInteractionArgs{InteractionID: "int456", Agent: "beta"},
			defaultAgent: "beta",
			wantSubject:  "aerial.jobs.synapse.discord_interaction.beta",
			wantMsgID:    "discord:ix:beta:int456",
		},
		{
			name:        "blank default owner falls back to unknown token",
			in:          model.DiscordIngressArgs{MessageID: "msg789"},
			wantSubject: "aerial.jobs.synapse.discord_ingress.unknown",
			wantMsgID:   "discord:msg:msg789",
		},
		{
			name:         "unknown",
			in:           12345,
			defaultAgent: "alpha",
			wantSubject:  "",
			wantMsgID:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subj, msgID := ResolveJetStreamJobCoords(tc.in, NewDiscordRouting(tc.bots, tc.defaultAgent))
			if subj != tc.wantSubject || msgID != tc.wantMsgID {
				t.Errorf("ResolveJetStreamJobCoords() = (%q, %q), want (%q, %q)", subj, msgID, tc.wantSubject, tc.wantMsgID)
			}
		})
	}
}

func TestDiscordAgentNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		agent              string
		wantSanitized      string
		wantIngressSubject string
		wantIxSubject      string
		wantIngressDurable string
		wantIxDurable      string
	}{
		{
			name:               "plain lowercase agent",
			agent:              "alpha",
			wantSanitized:      "alpha",
			wantIngressSubject: "aerial.jobs.synapse.discord_ingress.alpha",
			wantIxSubject:      "aerial.jobs.synapse.discord_interaction.alpha",
			wantIngressDurable: "synapse-discord-ingress-alpha",
			wantIxDurable:      "synapse-discord-interaction-alpha",
		},
		{
			name:               "trimmed and lowercased",
			agent:              "  Beta-2_x ",
			wantSanitized:      "beta-2_x",
			wantIngressSubject: "aerial.jobs.synapse.discord_ingress.beta-2_x",
			wantIxSubject:      "aerial.jobs.synapse.discord_interaction.beta-2_x",
			wantIngressDurable: "synapse-discord-ingress-beta-2_x",
			wantIxDurable:      "synapse-discord-interaction-beta-2_x",
		},
		{
			name:               "subject metacharacters and whitespace replaced",
			agent:              "a.b*c>d e/f",
			wantSanitized:      "a_b_c_d_e_f",
			wantIngressSubject: "aerial.jobs.synapse.discord_ingress.a_b_c_d_e_f",
			wantIxSubject:      "aerial.jobs.synapse.discord_interaction.a_b_c_d_e_f",
			wantIngressDurable: "synapse-discord-ingress-a_b_c_d_e_f",
			wantIxDurable:      "synapse-discord-interaction-a_b_c_d_e_f",
		},
		{
			name:               "blank agent uses unknown token",
			agent:              "   ",
			wantSanitized:      "",
			wantIngressSubject: "aerial.jobs.synapse.discord_ingress.unknown",
			wantIxSubject:      "aerial.jobs.synapse.discord_interaction.unknown",
			wantIngressDurable: "synapse-discord-ingress-unknown",
			wantIxDurable:      "synapse-discord-interaction-unknown",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := []string{
				SanitizeDiscordAgent(tc.agent),
				DiscordIngressSubject(tc.agent),
				DiscordInteractionSubject(tc.agent),
				DiscordIngressDurable(tc.agent),
				DiscordInteractionDurable(tc.agent),
			}
			want := []string{tc.wantSanitized, tc.wantIngressSubject, tc.wantIxSubject, tc.wantIngressDurable, tc.wantIxDurable}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("names(%q) = %q, want %q", tc.agent, got, want)
			}
		})
	}
}

func TestNewDiscordRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bots         []string
		defaultAgent string
		resolve      string
		wantAgents   []string
		wantResolved string
	}{
		{
			name:         "sorted dedup with default owner included",
			bots:         []string{"gamma", "Alpha", "alpha", "  "},
			defaultAgent: "beta",
			resolve:      "GAMMA",
			wantAgents:   []string{"alpha", "beta", "gamma"},
			wantResolved: "gamma",
		},
		{
			name:         "no bots routes to default owner",
			defaultAgent: "alpha",
			resolve:      "",
			wantAgents:   []string{"alpha"},
			wantResolved: "alpha",
		},
		{
			name:         "unconfigured agent resolves to default owner",
			bots:         []string{"alpha", "beta"},
			defaultAgent: "beta",
			resolve:      "delta",
			wantAgents:   []string{"alpha", "beta"},
			wantResolved: "beta",
		},
		{
			name:         "sanitized agent resolves to its own token",
			bots:         []string{"ops.bot"},
			defaultAgent: "alpha",
			resolve:      "Ops.Bot",
			wantAgents:   []string{"alpha", "ops_bot"},
			wantResolved: "ops_bot",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewDiscordRouting(tc.bots, tc.defaultAgent)
			if got := r.Agents(); !reflect.DeepEqual(got, tc.wantAgents) {
				t.Errorf("Agents() = %q, want %q", got, tc.wantAgents)
			}
			if got := r.Resolve(tc.resolve); got != tc.wantResolved {
				t.Errorf("Resolve(%q) = %q, want %q", tc.resolve, got, tc.wantResolved)
			}
		})
	}
}
