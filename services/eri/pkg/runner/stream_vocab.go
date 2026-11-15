package runner

// Stream-JSON event vocabulary shared by the daemon parsers.
const (
	formatStreamJSON   = "stream-json"
	eventInit          = "init"
	eventResult        = "result"
	eventUser          = "user"
	fieldType          = "type"
	fieldContent       = "content"
	fieldResult        = "result"
	fieldError         = "error"
	fieldText          = "text"
	blockTypeText      = "text"
	subtypeErrorPrefix = "error"
	statusError        = "error"
	toolCallMCPTool    = "call_mcp_tool"
	defaultModelLabel  = "default"
)
