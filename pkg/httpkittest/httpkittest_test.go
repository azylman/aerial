package httpkittest_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/httpkittest"
)

func TestMain(m *testing.M) {
	hermetictest.Main(m)
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestStubCoverageWriters is not parallel: it swaps httpkit package variables.
func TestStubCoverageWriters(t *testing.T) {
	tests := []struct {
		name         string
		metaErr      error
		countersErr  error
		wantMeta     string
		wantCounters string
	}{
		{name: "no errors"},
		{name: "meta error", metaErr: errors.New("meta failed"), wantMeta: "meta failed"},
		{name: "counters error", countersErr: errors.New("counters failed"), wantCounters: "counters failed"},
	}
	for _, tc := range tests {
		origMeta, origCounters := reflect.ValueOf(httpkit.WriteMetaDirFn).Pointer(), reflect.ValueOf(httpkit.WriteCountersDirFn).Pointer()
		t.Run(tc.name, func(t *testing.T) {
			httpkittest.StubCoverageWriters(t, tc.metaErr, tc.countersErr)
			if got := errText(httpkit.WriteMetaDirFn("/nonexistent")); got != tc.wantMeta {
				t.Fatalf("WriteMetaDirFn err = %q, want %q", got, tc.wantMeta)
			}
			if got := errText(httpkit.WriteCountersDirFn("/nonexistent")); got != tc.wantCounters {
				t.Fatalf("WriteCountersDirFn err = %q, want %q", got, tc.wantCounters)
			}
		})
		if reflect.ValueOf(httpkit.WriteMetaDirFn).Pointer() != origMeta || reflect.ValueOf(httpkit.WriteCountersDirFn).Pointer() != origCounters {
			t.Fatal("cleanup did not restore the original coverage writers")
		}
	}
}
