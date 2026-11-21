package prstate

import (
	"errors"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	restate "github.com/restatedev/sdk-go"
)

// errLegacyEffectsOwner is the terminal cause for a legacy-stamped event.
var errLegacyEffectsOwner = errors.New("prstate: effects_owner=legacy is no longer accepted; restate owns every PR effect and pr_registry write")

// legacyEventsRejectedTotal counts legacy-stamped events PRObject and
// RepoObject rejected. Any increase means a synapse still runs the legacy
// lifecycle path and must alert: its effects and registry writes happened
// outside Restate.
var legacyEventsRejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "prstate_legacy_events_rejected_total",
	Help: "Events stamped effects_owner=legacy that prstate rejected with a terminal error, by handler.",
}, []string{"handler"})

func init() {
	registerCollector(legacyEventsRejectedTotal)
}

// rejectLegacyOwner returns a terminal 409 for an event stamped legacy and
// nil otherwise. It runs before any state read or journaled step, so a
// rejected event changes nothing and is never retried.
func rejectLegacyOwner(ctx restate.ObjectSharedContext, handler string, owner EffectsOwner) error {
	if ParseEffectsOwner(string(owner)) != EffectsOwnerLegacy {
		return nil
	}
	legacyEventsRejectedTotal.WithLabelValues(handler).Inc()
	slog.ErrorContext(ctx, "[prstate] legacy-owned event rejected", "handler", handler, "key", restate.Key(ctx))
	return restate.ToTerminalError(errLegacyEffectsOwner, restate.WithErrorCode(409))
}
