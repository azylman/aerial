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
		stats          map[string][2]int64
		wantErr        bool
		wantSubstr     string
		wantDepthQueue string
		wantDepthState string
		wantDepthVal   float64
	}{
		{
			name:       "nil querier returns nil without error",
			nilQuerier: true,
			wantErr:    false,
		},
		{
			name:       "querier error returns wrapped error",
			querierErr: errors.New("nats down"),
			wantErr:    true,
			wantSubstr: "consumer stats",
		},
		{
			name: "aggregates pending and in_flight across consumers",
			stats: map[string][2]int64{
				"AERIAL_JOBS/synapse-github":              {3, 1},
				"AERIAL_JOBS/synapse-deploy":              {4, 2},
				"AERIAL_JOBS/synapse-discord-ingress":     {5, 3},
				"AERIAL_JOBS/synapse-discord-interaction": {2, 1},
			},
			wantDepthQueue: QueueDefault,
			wantDepthState: "pending",
			wantDepthVal:   7,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var q ConsumerStatsQuerier
			if !tc.nilQuerier {
				q = &mockConsumerStatsQuerier{stats: tc.stats, err: tc.querierErr}
			}
			err := CollectJetStreamQueueMetrics(ctx, q)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CollectJetStreamQueueMetrics() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantSubstr)) {
				t.Errorf("err = %v, want substr %q", err, tc.wantSubstr)
			}
			if tc.wantDepthQueue != "" {
				gotDepth := testutil.ToFloat64(QueueDepth.WithLabelValues(tc.wantDepthQueue, tc.wantDepthState))
				if gotDepth != tc.wantDepthVal {
					t.Errorf("QueueDepth[%s, %s] = %v, want %v", tc.wantDepthQueue, tc.wantDepthState, gotDepth, tc.wantDepthVal)
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
		name        string
		in          any
		wantSubject string
		wantMsgID   string
	}{
		{
			name:        "github args",
			in:          model.GitHubWebhookArgs{Delivery: "d123"},
			wantSubject: "aerial.jobs.synapse.github",
			wantMsgID:   "github:d123",
		},
		{
			name:        "deploy args",
			in:          model.DeployWebhookArgs{IdempotencyKey: "dep123"},
			wantSubject: "aerial.jobs.synapse.deploy",
			wantMsgID:   "deploy:dep123",
		},
		{
			name:        "ingress args",
			in:          model.DiscordIngressArgs{MessageID: "msg123"},
			wantSubject: "aerial.jobs.synapse.discord_ingress",
			wantMsgID:   "discord:msg:msg123",
		},
		{
			name:        "interaction args",
			in:          model.DiscordInteractionArgs{InteractionID: "int123"},
			wantSubject: "aerial.jobs.synapse.discord_interaction",
			wantMsgID:   "discord:ix:int123",
		},
		{
			name:        "unknown",
			in:          12345,
			wantSubject: "",
			wantMsgID:   "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subj, msgID := ResolveJetStreamJobCoords(tc.in)
			if subj != tc.wantSubject || msgID != tc.wantMsgID {
				t.Errorf("ResolveJetStreamJobCoords() = (%q, %q), want (%q, %q)", subj, msgID, tc.wantSubject, tc.wantMsgID)
			}
		})
	}
}
