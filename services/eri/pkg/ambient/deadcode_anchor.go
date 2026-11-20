package ambient

// Deadcode anchors for ambient resolver helpers.
//
// NewResolver and Resolver.Resolve are part of the shared contract used by
// services/eri-restate. To prevent deadcode analysis against eri's main package
// from reporting them as unreachable in this module prior to downstream cutover,
// they are anchored here.
func keepLive(_ ...any) {}

func init() {
	keepLive(
		NewResolver,
		(*Resolver).Resolve,
	)
}
