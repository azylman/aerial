package restatetest

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestRunMockRunContext(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	tests := []struct {
		name    string
		res     any
		err     error
		wantErr bool
		want    string
	}{
		{"success copies result", "payload", nil, false, "payload"},
		{"error becomes terminal", nil, boom, true, ""},
		{"nil result leaves output", nil, nil, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out string
			terr := RunMockRunContext(func(rc restate.RunContext) (any, error) {
				assert.NotNil(t, rc.Log())
				assert.Nil(t, rc.Request())
				return tc.res, tc.err
			}, &out)
			assert.Equal(t, tc.wantErr, terr != nil)
			assert.Equal(t, tc.want, out)
		})
	}
}

func TestObserveJournalCallback(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	terminal := restate.ToTerminalError(errors.New("gone"), restate.WithErrorCode(404))
	tests := []struct {
		name string
		err  error
		code restate.Code
	}{
		{"success", nil, 0},
		{"transient", boom, 0},
		{"terminal", terminal, 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			ObserveJournalCallback(t, mc, tc.err, tc.code)
			_, err := restate.Run(restate.WithMockContext(mc), func(restate.RunContext) (string, error) {
				return "x", tc.err
			})
			assert.Equal(t, tc.err != nil, err != nil)
		})
	}
}

func TestMetadataMutationStore(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	tests := []struct {
		name string
		err  error
	}{
		{"ok", nil},
		{"failing", boom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &MetadataMutationStore{Err: tc.err}
			b, err := s.UpdateBuild(context.Background(), "b1", store.UpdateBuildRequest{})
			stRes, stErr := s.SteerChunk(context.Background(), "b1", "c1", store.SteerChunkRequest{Prompt: "p"})
			_, err2 := s.LookupChunkBuildID(context.Background(), "c1")
			_, _, err3 := s.LookupPRChunk(context.Background(), "r", 1)
			_, err4 := s.LookupPRBuildRoutes(context.Background(), "r", 1)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.err, stErr)
			assert.Equal(t, tc.err, err2)
			assert.Equal(t, tc.err, err3)
			assert.Equal(t, tc.err, err4)
			gb, gc, gerr := s.GetBuild(context.Background(), "b2")
			require.NoError(t, gerr)
			assert.Equal(t, "b2", gb.ID)
			assert.Empty(t, gc)
			if tc.err == nil {
				require.NotNil(t, b)
				assert.Equal(t, "b1", b.ID)
				require.NotNil(t, stRes)
				assert.Equal(t, "b1", stRes.BuildID)
			}
		})
	}
}

func TestMetricValue(t *testing.T) {
	// Not parallel-safe on shared registry names: use a unique metric.
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "restatetest_probe_total", Help: "probe"}, []string{"handler"})
	prometheus.MustRegister(cv)
	t.Cleanup(func() { prometheus.Unregister(cv) })
	cv.WithLabelValues("h").Add(3)
	tests := []struct {
		name, metric, handler string
		want                  float64
	}{
		{"present", "restatetest_probe_total", "h", 3},
		{"other handler", "restatetest_probe_total", "x", 0},
		{"absent metric", "restatetest_missing_total", "h", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.InDelta(t, tc.want, MetricValue(t, tc.metric, tc.handler), 0)
		})
	}
}
