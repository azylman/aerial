package pgutil

import (
	"testing"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/pgtest"
)

type pgtestRunner struct{ m *testing.M }

func (r pgtestRunner) Run() int {
	return pgtest.Main(r.m)
}

func TestMain(m *testing.M) {
	hermetictest.Main(pgtestRunner{m: m})
}
