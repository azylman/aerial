// Package mcpkit holds MCP server scaffolding shared by the Go MCP servers.
// It is separate from httpkit so httpkit importers do not link the MCP SDK.
package mcpkit

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
)

// HandleHealth handles GET and HEAD readiness requests.
func HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// AddTextTool registers a tool whose logic returns its result text and whether it is an error.
func AddTextTool[In any](srv *mcp.Server, tool *mcp.Tool, run func(context.Context, In) (string, bool)) {
	mcp.AddTool(srv, tool, func(ctx context.Context, _ *mcp.CallToolRequest, args In) (*mcp.CallToolResult, any, error) {
		text, isErr := run(ctx, args)
		return &mcp.CallToolResult{IsError: isErr, Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil, nil
	})
}

// InputSchema infers T's input schema and collapses the ["null", X] unions inferred
// for pointer and slice fields to plain X, matching the types clients have always seen.
func InputSchema[T any]() *jsonschema.Schema {
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		panic(fmt.Sprintf("mcpkit: infer input schema: %v", err))
	}
	for _, prop := range schema.Properties {
		if len(prop.Types) == 2 && prop.Types[0] == "null" {
			prop.Type, prop.Types = prop.Types[1], nil
		}
	}
	return schema
}
