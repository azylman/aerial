package main

import (
	"testing"

	"github.com/azylman/aerial/pkg/hermetictest"
)

func TestMain(m *testing.M) {
	installTestTracing()
	hermetictest.Main(m)
}
