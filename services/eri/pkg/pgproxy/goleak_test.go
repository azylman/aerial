package pgproxy

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/pgtest"
)

type pgtestRunner struct{ m *testing.M }

func (r pgtestRunner) Run() int {
	return pgtest.Main(r.m)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(pgtestRunner{m: m})
}
