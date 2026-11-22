// Package httpkittest holds test helpers for httpkit callers. It lives apart
// from hermetictest because it imports httpkit (and hermetictest imports no
// aerial package).
package httpkittest

import (
	"testing"

	"github.com/azylman/aerial/pkg/httpkit"
)

// StubCoverageWriters swaps httpkit.WriteMetaDirFn and httpkit.WriteCountersDirFn
// for no-ops returning metaErr and countersErr, and restores the originals via
// t.Cleanup. It mutates package state, so callers must not use t.Parallel.
func StubCoverageWriters(t testing.TB, metaErr, countersErr error) {
	t.Helper()
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() { httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters })
	httpkit.WriteMetaDirFn = func(string) error { return metaErr }
	httpkit.WriteCountersDirFn = func(string) error { return countersErr }
}
