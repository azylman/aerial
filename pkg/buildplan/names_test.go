package buildplan

import "testing"

// TestServiceNamesCallThrough exercises exported helpers consumed by sibling service modules
// to satisfy deadcode analysis until later chunks inline the canonical constants.
func TestServiceNamesCallThrough(t *testing.T) {
	t.Parallel()
	if got := BuildServiceName(""); got == "" {
		t.Error("expected non-empty Build service name")
	}
	if got := BuildRouterServiceName(""); got == "" {
		t.Error("expected non-empty BuildRouter service name")
	}
}
