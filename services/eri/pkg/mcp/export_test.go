package mcp

// DefaultServiceAccountTokenPath exports defaultServiceAccountTokenPath for package mcp_test.
const DefaultServiceAccountTokenPath = defaultServiceAccountTokenPath

// TokenPathForTest exports the configured tokenPath for package mcp_test.
func (p *TraceProxy) TokenPathForTest() string {
	return p.tokenPath
}
