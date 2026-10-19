package session

// Export internal functions for whitebox unit testing in session_test.
var (
	IndexFoldForTest           = indexFold
	LastIndexFoldForTest       = lastIndexFold
	ExportGetLastStepIndex     = getLastStepIndex
	ExportFormatStepError      = formatStepError
	ExportIsStepBefore         = isStepBefore
	ExportExtractCommandOrArgs = extractCommandOrArgs
	ExportAppendTranscriptStep = appendTranscriptStep
)

// ExportGetTargetDirs exposes getTargetDirs for testing.
func (m *Manager) ExportGetTargetDirs(convID string) []string {
	return m.getTargetDirs(convID)
}
