package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/azylman/aerial/services/haro/internal/model"
)

func TestRegisterHaroCollector(t *testing.T) {
	tests := []struct {
		name       string
		collector  prometheus.Collector
		wantPanics bool
	}{
		{"duplicate registration tolerated", SchedulesActive, false},
		{"conflicting descriptor panics", prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "haro_schedules_active", Help: "Conflicting help string."},
			[]string{"other_label"},
		), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			didPanic := func() (panicked bool) {
				defer func() { panicked = recover() != nil }()
				RegisterHaroCollector(tc.collector)
				return false
			}()
			if didPanic != tc.wantPanics {
				t.Errorf("RegisterHaroCollector() panicked = %v, want %v", didPanic, tc.wantPanics)
			}
		})
	}
}

func TestSeededSeries(t *testing.T) {
	tests := []struct {
		name       string
		collector  prometheus.Collector
		familyName string
		want       string
	}{
		{"schedules active gauges", SchedulesActive, "haro_schedules_active", `# HELP haro_schedules_active Number of active persistent schedules by type.
# TYPE haro_schedules_active gauge
haro_schedules_active{schedule_type="cron"} 0
haro_schedules_active{schedule_type="event_watch"} 0
haro_schedules_active{schedule_type="heartbeat"} 0
haro_schedules_active{schedule_type="one_shot"} 0
`},
		{"schedule runs counters", ScheduleRunsTotal, "haro_schedule_runs_total", `# HELP haro_schedule_runs_total Total number of schedule runs executed by type and status.
# TYPE haro_schedule_runs_total counter
haro_schedule_runs_total{schedule_type="cron",status="completed"} 0
haro_schedule_runs_total{schedule_type="cron",status="failed"} 0
haro_schedule_runs_total{schedule_type="cron",status="skipped_active"} 0
haro_schedule_runs_total{schedule_type="cron",status="stale"} 0
haro_schedule_runs_total{schedule_type="event_watch",status="completed"} 0
haro_schedule_runs_total{schedule_type="event_watch",status="failed"} 0
haro_schedule_runs_total{schedule_type="event_watch",status="skipped_active"} 0
haro_schedule_runs_total{schedule_type="event_watch",status="stale"} 0
haro_schedule_runs_total{schedule_type="heartbeat",status="completed"} 0
haro_schedule_runs_total{schedule_type="heartbeat",status="failed"} 0
haro_schedule_runs_total{schedule_type="heartbeat",status="skipped_active"} 0
haro_schedule_runs_total{schedule_type="heartbeat",status="stale"} 0
haro_schedule_runs_total{schedule_type="one_shot",status="completed"} 0
haro_schedule_runs_total{schedule_type="one_shot",status="failed"} 0
haro_schedule_runs_total{schedule_type="one_shot",status="skipped_active"} 0
haro_schedule_runs_total{schedule_type="one_shot",status="stale"} 0
`},
		{"dispatch duration histograms", ScheduleDispatchDurationSeconds, "haro_schedule_dispatch_duration_seconds", `# HELP haro_schedule_dispatch_duration_seconds Dispatch duration of schedule runs in seconds.
# TYPE haro_schedule_dispatch_duration_seconds histogram
haro_schedule_dispatch_duration_seconds_bucket{le="0.005",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.01",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.025",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.05",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.1",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.25",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.5",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="1",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="2.5",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="5",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="10",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="+Inf",schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_sum{schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_count{schedule_type="cron"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.005",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.01",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.025",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.05",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.1",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.25",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.5",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="1",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="2.5",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="5",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="10",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="+Inf",schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_sum{schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_count{schedule_type="event_watch"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.005",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.01",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.025",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.05",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.1",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.25",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.5",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="1",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="2.5",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="5",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="10",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="+Inf",schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_sum{schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_count{schedule_type="heartbeat"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.005",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.01",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.025",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.05",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.1",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.25",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="0.5",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="1",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="2.5",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="5",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="10",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_bucket{le="+Inf",schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_sum{schedule_type="one_shot"} 0
haro_schedule_dispatch_duration_seconds_count{schedule_type="one_shot"} 0
`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := testutil.CollectAndCompare(tc.collector, strings.NewReader(tc.want), tc.familyName); err != nil {
				t.Errorf("CollectAndCompare(%s) mismatch: %v", tc.familyName, err)
			}
		})
	}
}

func TestRecordScheduleRun(t *testing.T) {
	tests := []struct {
		name         string
		scheduleType string
		status       string
	}{
		{"cron completed", model.ScheduleTypeCron, model.StatusCompleted},
		{"cron failed", model.ScheduleTypeCron, model.StatusFailed},
		{"one shot stale", model.ScheduleTypeOneShot, model.StatusStale},
		{"heartbeat skipped active", model.ScheduleTypeHeartbeat, model.StatusSkippedActive},
		{"event watch completed", model.ScheduleTypeEventWatch, model.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := testutil.ToFloat64(ScheduleRunsTotal.WithLabelValues(tc.scheduleType, tc.status))
			RecordScheduleRun(tc.scheduleType, tc.status)
			if got := testutil.ToFloat64(ScheduleRunsTotal.WithLabelValues(tc.scheduleType, tc.status)); got != before+1 {
				t.Errorf("ScheduleRunsTotal(%q, %q) = %v, want %v", tc.scheduleType, tc.status, got, before+1)
			}
		})
	}
}

func TestRecordScheduleDispatchDuration(t *testing.T) {
	tests := []struct {
		name         string
		scheduleType string
		observation  float64
	}{
		{"dispatch observation recorded", model.ScheduleTypeCron, 0.42},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seriesBefore := testutil.CollectAndCount(ScheduleDispatchDurationSeconds, "haro_schedule_dispatch_duration_seconds")
			hist, ok := ScheduleDispatchDurationSeconds.WithLabelValues(tc.scheduleType).(prometheus.Histogram)
			if !ok {
				t.Fatalf("ScheduleDispatchDurationSeconds(%q) is not a prometheus.Histogram", tc.scheduleType)
			}
			var before dto.Metric
			if err := hist.Write(&before); err != nil {
				t.Fatalf("hist.Write() err = %v", err)
			}
			RecordScheduleDispatchDuration(tc.scheduleType, tc.observation)
			var after dto.Metric
			if err := hist.Write(&after); err != nil {
				t.Fatalf("hist.Write() err = %v", err)
			}
			if got, want := after.GetHistogram().GetSampleCount(), before.GetHistogram().GetSampleCount()+1; got != want {
				t.Errorf("sample count = %v, want %v (one observation must be recorded)", got, want)
			}
			if got, want := after.GetHistogram().GetSampleSum(), before.GetHistogram().GetSampleSum()+tc.observation; got < want-1e-9 || got > want+1e-9 {
				t.Errorf("sample sum = %v, want %v (observation value must be recorded)", got, want)
			}
			if got := testutil.CollectAndCount(ScheduleDispatchDurationSeconds, "haro_schedule_dispatch_duration_seconds"); got != seriesBefore {
				t.Errorf("CollectAndCount() = %v, want %v (series count must not change)", got, seriesBefore)
			}
		})
	}
}
