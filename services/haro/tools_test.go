package main

import (
	"context"
	"testing"
)

func TestNewBusFn_FastFailure(t *testing.T) {
	if _, err := newBusFn(context.Background(), "://bad-url"); err == nil {
		t.Error("expected error connecting bus to malformed url")
	}
}
