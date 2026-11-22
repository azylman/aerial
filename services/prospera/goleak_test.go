package main

import (
	"testing"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
)

func TestMain(m *testing.M) {
	fakes.InstallTestTracing()
	hermetictest.Main(m)
}
