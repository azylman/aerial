package httpkit

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterCollector(t *testing.T) {
	name := fmt.Sprintf("test_httpkit_reg_%d", time.Now().UnixNano())
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: "Test counter for RegisterCollector",
	})

	// First registration should succeed.
	RegisterCollector(c)

	// Second registration of identical collector returns AlreadyRegisteredError and does not panic.
	RegisterCollector(c)

	// Conflicting registration with different Help descriptor must panic.
	conflicting := prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: "Conflicting help text provoking error",
	})
	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		RegisterCollector(conflicting)
	}()
	if !didPanic {
		t.Fatal("expected panic on conflicting collector registration, got none")
	}
}
