package core_test

import (
	"testing"

	"example.com/fx/app/internal/core"
	"example.com/fx/lib/onlytest"
)

func TestName(t *testing.T) { _ = core.Name + onlytest.Name }
