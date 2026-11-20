package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordRestateShadowSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler string
		status  string
	}{
		{name: "blank labels fall back to unknown", handler: "", status: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler, status := tc.handler, tc.status
			if handler == "" {
				handler = unknownLabel
			}
			if status == "" {
				status = unknownLabel
			}
			counter := RestateShadowSendsTotal.WithLabelValues(handler, status)
			before := testutil.ToFloat64(counter)
			RecordRestateShadowSend(tc.handler, tc.status)
			if got, want := testutil.ToFloat64(counter), before+1; got != want {
				t.Errorf("shadow sends = %v, want %v", got, want)
			}
		})
	}
}

func TestRecordRestateShadowDivergence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reason string
	}{
		{name: "blank reason falls back to unknown", reason: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reason := tc.reason
			if reason == "" {
				reason = unknownLabel
			}
			counter := RestateShadowDivergenceTotal.WithLabelValues(reason)
			before := testutil.ToFloat64(counter)
			RecordRestateShadowDivergence(tc.reason)
			if got, want := testutil.ToFloat64(counter), before+1; got != want {
				t.Errorf("shadow divergences = %v, want %v", got, want)
			}
		})
	}
}
