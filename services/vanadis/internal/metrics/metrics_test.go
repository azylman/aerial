package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis/internal/model"
)

func entry(id, queueState string, statuses ...string) model.VanadisBuildEntry {
	chunks := make([]model.VanadisChunk, len(statuses))
	for i, s := range statuses {
		chunks[i] = model.VanadisChunk{ID: id + "-c", Status: s}
	}
	//nolint:modernize // explicit embedded type keeps the fixture shape obvious
	return model.VanadisBuildEntry{VanadisBuild: model.VanadisBuild{ID: id}, QueueState: queueState, Chunks: chunks}
}

func scrapeMetrics(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler status = %d, want %d", rec.Code, http.StatusOK)
	}
	return rec.Body.String()
}

type buildMetricsCase struct {
	name    string
	entries []model.VanadisBuildEntry
	want    []string
}

func TestCollectBuildMetrics(t *testing.T) {
	tests := []buildMetricsCase{
		{
			name:    "empty entries zero all gauges",
			entries: nil,
			want: []string{
				`vanadis_builds_active{queue_state="running"} 0`,
				`vanadis_builds_active{queue_state="queued"} 0`,
				`vanadis_chunks_active{status="pending"} 0`,
				`vanadis_chunks_active{status="running"} 0`,
				`vanadis_chunks_active{status="pr_opened"} 0`,
			},
		},
		{
			name: "running build counts running pending and coding chunks",
			entries: []model.VanadisBuildEntry{
				entry("b-run", "running", "running", "pending", "coding"),
				entry("b-queue", "queued", "pending", "pending"),
			},
			want: []string{
				`vanadis_builds_active{queue_state="running"} 1`,
				`vanadis_builds_active{queue_state="queued"} 1`,
				`vanadis_chunks_active{status="pending"} 3`,
				`vanadis_chunks_active{status="running"} 2`,
				`vanadis_chunks_active{status="pr_opened"} 0`,
			},
		},
		{
			name: "completed builds skip chunk counts",
			entries: []model.VanadisBuildEntry{
				entry("b-comp", "completed", "merged", "pending"),
				entry("b-run", "running", "pr_opened"),
			},
			want: []string{
				`vanadis_builds_active{queue_state="running"} 1`,
				`vanadis_builds_active{queue_state="queued"} 0`,
				`vanadis_chunks_active{status="pending"} 0`,
				`vanadis_chunks_active{status="pr_opened"} 1`,
			},
		},
		{
			name: "unknown statuses ignored",
			entries: []model.VanadisBuildEntry{
				entry("b-weird", "stalled", "migrated"),
			},
			want: []string{
				`vanadis_builds_active{queue_state="running"} 0`,
				`vanadis_chunks_active{status="pending"} 0`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			CollectBuildMetrics(tc.entries)
			body := scrapeMetrics(t)
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Errorf("metrics exposition missing %q\nbody:\n%s", want, body)
				}
			}
		})
	}
}

var errQueueStatsDown = errors.New("stats down")

// recordingQuerier records each "stream/consumer" pair CollectQueueMetrics asks
// for, so rows assert the stream wiring, then delegates to the stats fake.
type recordingQuerier struct {
	stats pubsubtest.ConsumerStats
	calls []string
}

func (r *recordingQuerier) ConsumerStats(ctx context.Context, stream, consumer string) (int64, int64, error) {
	r.calls = append(r.calls, stream+"/"+consumer)
	return r.stats.ConsumerStats(ctx, stream, consumer)
}

type queueMetricsCase struct {
	name      string
	nilQuery  bool
	stats     pubsubtest.ConsumerStats
	want      []string
	wantCalls []string
}

func TestCollectQueueMetrics(t *testing.T) {
	pending := map[string]int64{"vanadis-dispatch": 2, "vanadis-pr-events": 3}
	inflight := map[string]int64{"vanadis-dispatch": 1, "vanadis-pr-events": 0}
	allCalls := []string{"AERIAL_EVENTS/vanadis-dispatch", "AERIAL_EVENTS/vanadis-pr-events"}
	tests := []queueMetricsCase{
		{
			name:     "nil querier zeroes depth",
			nilQuery: true,
			want:     []string{`vanadis_queue_depth{state="pending"} 0`, `vanadis_queue_depth{state="in_flight"} 0`},
		},
		{
			name:      "sums backlog of the two vanadis-nats consumers",
			stats:     pubsubtest.ConsumerStats{Pending: pending, Inflight: inflight},
			want:      []string{`vanadis_queue_depth{state="pending"} 5`, `vanadis_queue_depth{state="in_flight"} 1`},
			wantCalls: allCalls,
		},
		{
			name:      "failed consumer is skipped and the rest still count",
			stats:     pubsubtest.ConsumerStats{Pending: pending, Inflight: inflight, Errs: map[string]error{"vanadis-pr-events": errQueueStatsDown}},
			want:      []string{`vanadis_queue_depth{state="pending"} 2`, `vanadis_queue_depth{state="in_flight"} 1`},
			wantCalls: allCalls,
		},
		{
			name:      "every query failing leaves zeroes",
			stats:     pubsubtest.ConsumerStats{Pending: pending, Inflight: inflight, Err: errQueueStatsDown},
			want:      []string{`vanadis_queue_depth{state="pending"} 0`, `vanadis_queue_depth{state="in_flight"} 0`},
			wantCalls: allCalls,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingQuerier{stats: tc.stats}
			var q ConsumerStatsQuerier
			if !tc.nilQuery {
				q = rec
			}
			CollectQueueMetrics(context.Background(), q)
			if !slices.Equal(rec.calls, tc.wantCalls) {
				t.Errorf("ConsumerStats calls = %v, want %v", rec.calls, tc.wantCalls)
			}
			body := scrapeMetrics(t)
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Errorf("metrics exposition missing %q\nbody:\n%s", want, body)
				}
			}
		})
	}
}

type totalsCase struct {
	name   string
	status string
}

func buildTotalValue(t *testing.T, status string) float64 {
	t.Helper()
	key := fmt.Sprintf("vanadis_builds_total{status=%q}", status)
	for line := range strings.Lines(scrapeMetrics(t)) {
		if !strings.HasPrefix(line, key+" ") {
			continue
		}
		var got float64
		if _, err := fmt.Sscanf(strings.TrimPrefix(line, key+" "), "%f", &got); err != nil {
			t.Fatalf("unparsable exposition line %q: %v", line, err)
		}
		return got
	}
	return 0
}

func TestRecordBuildTotal(t *testing.T) {
	tests := []totalsCase{
		{name: "created increments by one", status: "created"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := buildTotalValue(t, tc.status)
			RecordBuildTotal(tc.status)
			if got := buildTotalValue(t, tc.status); got-before != 1 {
				t.Errorf("RecordBuildTotal(%q) delta = %v, want 1", tc.status, got-before)
			}
		})
	}
}
