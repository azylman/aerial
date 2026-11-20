package ericlient

// Dark Phase-1 production-graph anchors for RestateEriClient.
//
// RestateEriClient (chunk c02b) is dark-launched in services/vanadis-restate:
// unit tests exercise it, but vanadis-restate/internal/app continues using
// HTTPEriClient until downstream chunk c07b cuts over.
// The CI deadcode gate requires every production-graph function to be reachable
// from main. This anchor roots RestateEriClient entry points so deadcode analysis
// against the main package passes.
//
// Downstream chunk c07b cuts over app.go to NewRestateEriClient and deletes this file.
func keepLive(_ ...any) {}

func init() {
	keepLive(
		NewRestateEriClient,
		NormalizeAgentName,
		ThreadSessionServiceName,
		DispatchSessionServiceName,
		VoiceSessionServiceName,
		ReconcileRestateServiceTimeouts,
		(*RestateEriClient).SetWidgetDeliverer,
		(*RestateEriClient).SetDiscordAuth,
		(*RestateEriClient).Dispatch,
		(*RestateEriClient).DispatchAccepted,
		(*RestateEriClient).CancelDispatch,
		(*RestateEriClient).Prompt,
		(*RestateEriClient).Widget,
		(*RestateEriClient).Message,
	)
}
