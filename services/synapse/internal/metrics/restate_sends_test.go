package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordRestateSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		handler     string
		status      string
		wantHandler string
		wantStatus  string
	}{
		{name: "labels are recorded", handler: "HandleOpened", status: "sent", wantHandler: "HandleOpened", wantStatus: "sent"},
		{name: "terminal outcome is recorded", handler: "Register", status: "terminal", wantHandler: "Register", wantStatus: "terminal"},
		{name: "blank labels fall back to unknown", handler: "", status: "", wantHandler: "unknown", wantStatus: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			counter := RestateSendsTotal.WithLabelValues(tc.wantHandler, tc.wantStatus)
			before := testutil.ToFloat64(counter)
			RecordRestateSend(tc.handler, tc.status)
			if got, want := testutil.ToFloat64(counter), before+1; got != want {
				t.Errorf("restate sends = %v, want %v", got, want)
			}
		})
	}
}
