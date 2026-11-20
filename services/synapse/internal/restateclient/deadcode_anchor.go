package restateclient

// Deadcode anchors for restateclient helper functions.
//
// WithIdempotencyKey is part of the restateclient contract for callers that
// need explicit Restate request deduplication keys. To prevent deadcode analysis
// against synapse's main package from reporting it as unreachable before caller
// cutovers, it is anchored here.
func keepLive(_ ...any) {}

func init() {
	keepLive(
		WithIdempotencyKey,
	)
}
