package metrics

import (
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
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() err = %v", err)
	}
	families := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		families[mf.GetName()] = mf
	}
	tests := []struct {
		name       string
		familyName string
		labels     map[string]string
	}{
		{"gauge cron", "haro_schedules_active", map[string]string{"schedule_type": model.ScheduleTypeCron}},
		{"gauge event watch", "haro_schedules_active", map[string]string{"schedule_type": model.ScheduleTypeEventWatch}},
		{"gauge heartbeat", "haro_schedules_active", map[string]string{"schedule_type": model.ScheduleTypeHeartbeat}},
		{"gauge one shot", "haro_schedules_active", map[string]string{"schedule_type": model.ScheduleTypeOneShot}},
		{"counter cron completed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeCron, "status": model.StatusCompleted}},
		{"counter cron failed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeCron, "status": model.StatusFailed}},
		{"counter cron skipped active", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeCron, "status": model.StatusSkippedActive}},
		{"counter cron stale", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeCron, "status": model.StatusStale}},
		{"counter event watch completed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeEventWatch, "status": model.StatusCompleted}},
		{"counter event watch failed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeEventWatch, "status": model.StatusFailed}},
		{"counter event watch skipped active", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeEventWatch, "status": model.StatusSkippedActive}},
		{"counter event watch stale", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeEventWatch, "status": model.StatusStale}},
		{"counter heartbeat completed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeHeartbeat, "status": model.StatusCompleted}},
		{"counter heartbeat failed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeHeartbeat, "status": model.StatusFailed}},
		{"counter heartbeat skipped active", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeHeartbeat, "status": model.StatusSkippedActive}},
		{"counter heartbeat stale", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeHeartbeat, "status": model.StatusStale}},
		{"counter one shot completed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeOneShot, "status": model.StatusCompleted}},
		{"counter one shot failed", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeOneShot, "status": model.StatusFailed}},
		{"counter one shot skipped active", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeOneShot, "status": model.StatusSkippedActive}},
		{"counter one shot stale", "haro_schedule_runs_total", map[string]string{"schedule_type": model.ScheduleTypeOneShot, "status": model.StatusStale}},
		{"histogram cron", "haro_schedule_dispatch_duration_seconds", map[string]string{"schedule_type": model.ScheduleTypeCron}},
		{"histogram event watch", "haro_schedule_dispatch_duration_seconds", map[string]string{"schedule_type": model.ScheduleTypeEventWatch}},
		{"histogram heartbeat", "haro_schedule_dispatch_duration_seconds", map[string]string{"schedule_type": model.ScheduleTypeHeartbeat}},
		{"histogram one shot", "haro_schedule_dispatch_duration_seconds", map[string]string{"schedule_type": model.ScheduleTypeOneShot}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mf, ok := families[tc.familyName]
			if !ok {
				t.Fatalf("family %s not gathered (init seeding missing?)", tc.familyName)
			}
			found := false
			for _, m := range mf.GetMetric() {
				if len(m.GetLabel()) != len(tc.labels) {
					continue
				}
				match := true
				for _, lp := range m.GetLabel() {
					if tc.labels[lp.GetName()] != lp.GetValue() {
						match = false
						break
					}
				}
				if match {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s%v not seeded", tc.familyName, tc.labels)
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
		})
	}
}
