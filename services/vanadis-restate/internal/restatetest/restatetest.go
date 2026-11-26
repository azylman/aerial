// Package restatetest holds Restate SDK test plumbing shared by the build and
// router test suites. Only tests import it.
package restatetest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// MockRunContext is a minimal restate.RunContext.
type MockRunContext struct {
	context.Context //nolint:containedctx // restate.RunContext embeds context.Context.
}

func (MockRunContext) Log() *slog.Logger         { return slog.Default() }
func (MockRunContext) Request() *restate.Request { return nil }

// RunMockRunContext executes a Run callback inline and adapts its result.
func RunMockRunContext(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
	res, err := fn(MockRunContext{Context: context.Background()})
	if err != nil {
		if term, ok := errors.AsType[restate.TerminalError](err); ok {
			return term
		}
		return restate.TerminalErrorf("%v", err)
	}
	if output != nil && res != nil {
		outVal := reflect.ValueOf(output)
		resVal := reflect.ValueOf(res)
		if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
			outVal.Elem().Set(resVal)
		}
	}
	return nil
}

// ObserveJournalCallback inspects the callback error before the SDK mock adapter
// terminates execution. It does not simulate Restate scheduling or retries.
func ObserveJournalCallback(t *testing.T, mc *mocks.MockContext, want error, code restate.Code) {
	t.Helper()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(MockRunContext{Context: context.Background()})
		if code != 0 {
			var terminal restate.TerminalError
			require.ErrorAs(t, err, &terminal)
			assert.Equal(t, code, terminal.Code())
		} else {
			assert.False(t, restate.IsTerminalError(err))
		}
		switch {
		case want != nil && code != 0:
			assert.EqualError(t, err, want.Error())
		case want != nil:
			assert.ErrorIs(t, err, want)
		default:
			assert.NoError(t, err)
		}
		// Only adapt after observing the raw callback error above.
		return RunMockRunContext(func(restate.RunContext) (any, error) { return res, err }, output, opts...)
	})
}

// MetadataMutationStore fails (or, with a nil Err, trivially succeeds) the
// build mutation and routing lookups; every other method panics via the nil
// embedded store.
type MetadataMutationStore struct {
	store.VanadisStore

	Err error
}

func (s *MetadataMutationStore) UpdateBuild(_ context.Context, id string, req store.UpdateBuildRequest) (*model.VanadisBuild, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return &model.VanadisBuild{ID: id, Title: req.Title}, nil
}

func (s *MetadataMutationStore) LookupChunkBuildID(context.Context, string) (store.BuildRoute, error) {
	return store.BuildRoute{}, s.Err
}

func (s *MetadataMutationStore) LookupPRChunk(context.Context, string, int) (store.BuildRoute, string, error) {
	return store.BuildRoute{}, "", s.Err
}

func (s *MetadataMutationStore) LookupPRBuildRoutes(context.Context, string, int) ([]store.BuildRoute, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return nil, nil
}

func (*MetadataMutationStore) GetBuild(_ context.Context, id string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: id}, nil, nil
}

// MetricsMux serves the default Prometheus registry at /metrics.
func MetricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// MetricValue returns the value of metric{handler="<handler>"}, or 0 if absent.
func MetricValue(t *testing.T, metric, handler string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	MetricsMux().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", http.NoBody))
	prefix := fmt.Sprintf(`%s{handler=%q} `, metric, handler)
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if valStr, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			val, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
			require.NoError(t, err)
			return val
		}
	}
	return 0
}
