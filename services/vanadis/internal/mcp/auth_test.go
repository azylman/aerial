package mcp

import (
	"testing"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/k8sauthtest"
)

// authFake authenticates every token as the eri service account and answers
// SubjectAccessReview with sarOK.
func authFake(sarOK bool) *k8sauthtest.RoundTripper {
	return &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-alpha"), SARBody: k8sauthtest.SAR(sarOK, "")}
}

// TestMCP_AuthRBAC calls every tool through the JSON-RPC path with auth
// enabled; the caller identity rides on the request context the way the
// server's auth middleware sets it.
func TestMCP_AuthRBAC(t *testing.T) {
	t.Parallel()
	caller := k8sauth.CallerIdentity{Username: "system:serviceaccount:default:eri-alpha", CallerAgent: "alpha", IsService: true}
	const (
		buildArgs    = `{"build_id":"build-mcp-1","title":"new"}`
		chunkArgs    = `{"build_id":"build-mcp-1","chunk_ref":"1","title":"new"}`
		appendArgs   = `{"build_id":"build-mcp-1","chunks":[{"title":"c3","instructions":"i3","depends_on":[]}]}`
		removeArgs   = `{"build_id":"build-mcp-1","chunk_ref":"1"}`
		failArgs     = `{"chunk_id":"chunk-4-0","reason":"failure"}`
		completeArgs = `{"chunk_id":"chunk-1","dispatch_id":"vanadis-chunk-1-turn-0","summary":"ok"}`
		retryArgs    = `{"build_id":"build-mcp-1","chunk_id":"chunk-1","expected_dispatch_id":"d1","reason":"retry"}`
		createArgs   = `{"agent":"aerial","thread_id":"1234567890","objective":"test obj","chunks":[{"repo":"aerial","title":"c1","instructions":"i1","depends_on":[]}]}`
		cancelArgs   = `{"build_id":"build-mcp-1"}`
	)
	tests := []struct {
		name, tool, args  string
		withCaller, sarOK bool
		wantToolErr       bool
		wantSubstr        string
	}{
		{"get build missing caller", "get_build", `{"build_id":"build-mcp-1"}`, false, true, true, "unauthorized"},
		{"get build forbidden", "get_build", `{"build_id":"build-mcp-1"}`, true, false, true, "access denied"},
		{"get build allowed", "get_build", `{"build_id":"build-mcp-1"}`, true, true, false, `"id":"build-mcp-1"`},
		{"get chunk missing caller", "get_chunk", `{"chunk_id":"chunk-1"}`, false, true, true, "unauthorized"},
		{"get chunk forbidden", "get_chunk", `{"chunk_id":"chunk-1"}`, true, false, true, "access denied"},
		{"get chunk allowed", "get_chunk", `{"chunk_id":"chunk-1"}`, true, true, false, `"id":"chunk-1"`},
		{"update build missing caller", "update_build", buildArgs, false, true, true, "unauthorized"},
		{"update build not found", "update_build", `{"build_id":"b-missing","title":"new"}`, true, true, true, "not found"},
		{"update build forbidden", "update_build", buildArgs, true, false, true, "access denied"},
		{"update build allowed", "update_build", buildArgs, true, true, false, `"status":"updated"`},
		{"update chunk missing caller", "update_chunk", chunkArgs, false, true, true, "unauthorized"},
		{"update chunk not found", "update_chunk", `{"build_id":"b-missing","chunk_ref":"1","title":"new"}`, true, true, true, "not found"},
		{"update chunk forbidden", "update_chunk", chunkArgs, true, false, true, "access denied"},
		{"update chunk allowed", "update_chunk", chunkArgs, true, true, false, `"status":"updated"`},
		{"append chunk missing caller", "append_chunk", appendArgs, false, true, true, "unauthorized"},
		{"append chunk not found", "append_chunk", `{"build_id":"b-missing","chunks":[{"title":"c3","instructions":"i3","depends_on":[]}]}`, true, true, true, "not found"},
		{"append chunk forbidden", "append_chunk", appendArgs, true, false, true, "access denied"},
		{"append chunk allowed", "append_chunk", appendArgs, true, true, false, `"status":"appended"`},
		{"remove chunk missing caller", "remove_chunk", removeArgs, false, true, true, "unauthorized"},
		{"remove chunk not found", "remove_chunk", `{"build_id":"b-missing","chunk_ref":"1"}`, true, true, true, "not found"},
		{"remove chunk forbidden", "remove_chunk", removeArgs, true, false, true, "access denied"},
		{"remove chunk allowed", "remove_chunk", removeArgs, true, true, false, `"status":"removed"`},
		{"fail chunk missing caller", "fail_chunk", failArgs, false, true, true, "unauthorized"},
		{"fail chunk not found", "fail_chunk", `{"chunk_id":"c-missing","reason":"failure"}`, true, true, true, "not found"},
		{"fail chunk forbidden", "fail_chunk", failArgs, true, false, true, "access denied"},
		{"fail chunk allowed", "fail_chunk", failArgs, true, true, false, `"status":"failed"`},
		{"complete chunk missing caller", "complete_chunk", completeArgs, false, true, true, "unauthorized"},
		{"complete chunk forbidden", "complete_chunk", completeArgs, true, false, true, "access denied"},
		{"complete chunk allowed", "complete_chunk", completeArgs, true, true, false, `"completed":true`},
		{"retry action missing caller", "retry_action_chunk", retryArgs, false, true, true, "unauthorized"},
		{"retry action build not found", "retry_action_chunk", `{"build_id":"b-missing","chunk_id":"chunk-1","expected_dispatch_id":"d1","reason":"retry"}`, true, true, true, "not found"},
		{"retry action forbidden", "retry_action_chunk", retryArgs, true, false, true, "access denied"},
		{"create build missing caller", "create_build", createArgs, false, true, true, "unauthorized"},
		{"create build forbidden", "create_build", createArgs, true, false, true, "access denied"},
		{"create build allowed", "create_build", createArgs, true, true, false, `"status":"accepted"`},
		{"cancel build missing caller", "cancel_build", cancelArgs, false, true, true, "unauthorized"},
		{"cancel build not found", "cancel_build", `{"build_id":"b-missing"}`, true, true, true, "not found"},
		{"cancel build forbidden", "cancel_build", cancelArgs, true, false, true, "access denied"},
		{"cancel build allowed", "cancel_build", cancelArgs, true, true, false, `"status":"cancelled"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := k8sauthtest.Authenticator(t, authFake(tc.sarOK))
			var who *k8sauth.CallerIdentity
			if tc.withCaller {
				who = &caller
			}
			got := callMCPTool(t, mcpToolCase{tool: tc.tool, args: tc.args}, auth, who)
			assertMCPToolResult(t, got, tc.wantToolErr, tc.wantSubstr)
		})
	}
}
