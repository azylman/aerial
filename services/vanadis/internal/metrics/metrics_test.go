package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"

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

// scriptQuerier answers ConsumerStats from a "stream/consumer" keyed table;
// keys listed in errs fail and unknown consumers fail too.
type scriptQuerier struct {
	stats map[string][2]int64
	errs  map[string]bool
}

func (q scriptQuerier) ConsumerStats(_ context.Context, stream, consumer string) (int64, int64, error) {
	key := stream + "/" + consumer
	if q.errs[key] {
		return 0, 0, errors.New("stats down")
	}
	v, ok := q.stats[key]
	if !ok {
		return 0, 0, fmt.Errorf("consumer %s not found", key)
	}
	return v[0], v[1], nil
}

type queueMetricsCase struct {
	name     string
	nilQuery bool
	stats    map[string][2]int64
	errs     map[string]bool
	want     []string
}

func TestCollectQueueMetrics(t *testing.T) {
	allThree := map[string][2]int64{
		"AERIAL_EVENTS/vanadis-dispatch":  {2, 1},
		"AERIAL_EVENTS/vanadis-pr-events": {3, 0},
		"AERIAL_COALESCED/vanadis-widget": {1, 4},
	}
	tests := []queueMetricsCase{
		{
			name:     "nil querier zeroes depth",
			nilQuery: true,
			want: []string{
				`vanadis_queue_depth{state="pending"} 0`,
				`vanadis_queue_depth{state="in_flight"} 0`,
			},
		},
		{
			name:  "sums backlog of the three vanadis-nats consumers",
			stats: allThree,
			want: []string{
				`vanadis_queue_depth{state="pending"} 6`,
				`vanadis_queue_depth{state="in_flight"} 5`,
			},
		},
		{
			name:  "failed consumer is skipped and the rest still count",
			stats: allThree,
			errs:  map[string]bool{"AERIAL_EVENTS/vanadis-pr-events": true},
			want: []string{
				`vanadis_queue_depth{state="pending"} 3`,
				`vanadis_queue_depth{state="in_flight"} 5`,
			},
		},
		{
			name: "every query failing leaves zeroes",
			errs: map[string]bool{"AERIAL_EVENTS/vanadis-dispatch": true, "AERIAL_EVENTS/vanadis-pr-events": true, "AERIAL_COALESCED/vanadis-widget": true},
			want: []string{
				`vanadis_queue_depth{state="pending"} 0`,
				`vanadis_queue_depth{state="in_flight"} 0`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var q ConsumerStatsQuerier
			if !tc.nilQuery {
				q = scriptQuerier{stats: tc.stats, errs: tc.errs}
			}
			CollectQueueMetrics(context.Background(), q)
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
