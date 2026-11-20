package ericlient

// Deadcode anchors for ericlient helper functions.
//
// VoiceSessionServiceName and ReconcileRestateServiceTimeouts are part of the
// shared ericlient contract. ReconcileRestateServiceTimeouts is called during
// Eri startup/reconciliation and VoiceSessionServiceName is used for voice session
// routing. To prevent deadcode analysis against vanadis-restate's main package
// from reporting them as unreachable in this service, they are anchored here.
func keepLive(_ ...any) {}

func init() {
	keepLive(
		VoiceSessionServiceName,
		ReconcileRestateServiceTimeouts,
	)
}
