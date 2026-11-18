package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/itchyny/gojq"

	"github.com/azylman/aerial/services/eri/pkg/env"
)

const (
	mcpMethodToolsList  = "tools/list"
	mcpMethodInitialize = "initialize"
)

// PolicyResolver resolves the ServerPolicy for a target's MCP server.
type PolicyResolver interface {
	Policy(target, server string) (env.ServerPolicy, bool)
}

func hasActivePolicy(policy env.ServerPolicy) bool {
	return len(policy.EnabledTools) > 0 || len(policy.DisabledTools) > 0 || policy.Instructions != "" || len(policy.ResponseFilters) > 0
}

func needsResponseShaping(httpMethod, rpcMethod, tool string, policy env.ServerPolicy) bool {
	if httpMethod != "POST" {
		return false
	}
	switch rpcMethod {
	case mcpMethodToolsList:
		return len(policy.EnabledTools) > 0 || len(policy.DisabledTools) > 0
	case mcpMethodInitialize:
		return policy.Instructions != ""
	case mcpMethodToolsCall:
		return policy.ResponseFilters[strings.ToLower(strings.TrimSpace(tool))] != nil
	default:
		return false
	}
}

// ToolCallParams represents params of a tools/call request.
type ToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func isBlockedCall(method string, paramsRaw json.RawMessage, reqID any, policy env.ServerPolicy) (bool, any, string) {
	if strings.TrimSpace(method) != mcpMethodToolsCall {
		return false, nil, ""
	}
	if len(bytes.TrimSpace(paramsRaw)) == 0 {
		return false, nil, ""
	}
	var params ToolCallParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return false, nil, ""
	}
	clean := strings.ToLower(strings.TrimSpace(params.Name))
	if clean == "" {
		return false, nil, ""
	}
	if policy.DisabledTools[clean] || (len(policy.EnabledTools) > 0 && !policy.EnabledTools[clean]) {
		return true, reqID, params.Name
	}
	return false, nil, ""
}

// IsBlockedToolCall returns whether a JSON-RPC request tries to call a blocked tool.
// It returns true ONLY when the request is positively identified as tools/call to a disallowed tool.
// Non-tools/call methods, notifications, and unparseable bodies return false so upstream handles them.
func IsBlockedToolCall(rawReq []byte, policy env.ServerPolicy) (blocked bool, reqID any, toolName string) {
	if len(policy.DisabledTools) == 0 && len(policy.EnabledTools) == 0 {
		return false, nil, ""
	}
	trimmed := bytes.TrimSpace(rawReq)
	if len(trimmed) == 0 {
		return false, nil, ""
	}
	if trimmed[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(trimmed, &batch); err != nil {
			return false, nil, ""
		}
		for _, rawItem := range batch {
			var req struct {
				ID     any             `json:"id"`
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			}
			if err := json.Unmarshal(rawItem, &req); err != nil {
				continue
			}
			if b, id, name := isBlockedCall(req.Method, req.Params, req.ID, policy); b {
				return true, id, name
			}
		}
		return false, nil, ""
	}
	if trimmed[0] == '{' {
		var req struct {
			ID     any             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(trimmed, &req); err != nil {
			return false, nil, ""
		}
		return isBlockedCall(req.Method, req.Params, req.ID, policy)
	}
	return false, nil, ""
}

// BuildJSONRPCErrorResponse constructs a JSON-RPC 2.0 error response payload.
func BuildJSONRPCErrorResponse(reqID any, code int, message string) []byte {
	resp := map[string]any{
		"jsonrpc": "2.0",
		"id":      reqID,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":null,"error":{"code":%d,"message":%q}}`, code, message))
	}
	return b
}

func modifyJSONRPCResult(rawResp []byte, mutate func(result map[string]any)) ([]byte, error) {
	trimmed := bytes.TrimSpace(rawResp)
	if len(trimmed) == 0 {
		return rawResp, nil
	}
	if trimmed[0] == '[' {
		var batch []map[string]any
		if err := json.Unmarshal(trimmed, &batch); err != nil {
			return rawResp, err
		}
		for _, item := range batch {
			if res, ok := item["result"].(map[string]any); ok {
				mutate(res)
			}
		}
		return json.Marshal(batch)
	}
	var root map[string]any
	if err := json.Unmarshal(trimmed, &root); err != nil {
		return rawResp, err
	}
	if res, ok := root["result"].(map[string]any); ok {
		mutate(res)
	}
	return json.Marshal(root)
}

// FilterToolsResponse filters tools from a tools/list JSON-RPC response based on policy.
func FilterToolsResponse(rawResp []byte, policy env.ServerPolicy) ([]byte, error) {
	if len(policy.EnabledTools) == 0 && len(policy.DisabledTools) == 0 {
		return rawResp, nil
	}
	return modifyJSONRPCResult(rawResp, func(result map[string]any) {
		toolsList, ok := result["tools"].([]any)
		if !ok {
			return
		}
		filtered := make([]any, 0, len(toolsList))
		for _, t := range toolsList {
			toolMap, ok := t.(map[string]any)
			if !ok {
				continue
			}
			name, ok := toolMap["name"].(string)
			if !ok {
				continue
			}
			clean := strings.ToLower(strings.TrimSpace(name))
			if policy.DisabledTools[clean] {
				continue
			}
			if len(policy.EnabledTools) > 0 && !policy.EnabledTools[clean] {
				continue
			}
			filtered = append(filtered, toolMap)
		}
		result["tools"] = filtered
	})
}

// InjectInstructions injects instructions into an initialize JSON-RPC response.
func InjectInstructions(rawResp []byte, instructions string) ([]byte, error) {
	trimmedInst := strings.TrimSpace(instructions)
	if trimmedInst == "" {
		return rawResp, nil
	}
	return modifyJSONRPCResult(rawResp, func(result map[string]any) {
		existing, ok := result["instructions"].(string)
		if ok && strings.TrimSpace(existing) != "" {
			result["instructions"] = existing + "\n\n" + trimmedInst
		} else {
			result["instructions"] = trimmedInst
		}
	})
}

// ApplyResponseFilter runs a compiled gojq filter against each text content in a tools/call response.
func ApplyResponseFilter(ctx context.Context, rawResp, rawReq []byte, code *gojq.Code) ([]byte, error) {
	if code == nil {
		return rawResp, nil
	}
	trimmed := bytes.TrimSpace(rawResp)
	if len(trimmed) == 0 {
		return rawResp, nil
	}
	if trimmed[0] == '[' {
		var batch []map[string]any
		if err := json.Unmarshal(trimmed, &batch); err != nil {
			return rawResp, err
		}
		for _, item := range batch {
			if err := applyFilterToResultMap(ctx, item, rawReq, code); err != nil {
				return rawResp, err
			}
		}
		out, err := json.Marshal(batch)
		if err != nil {
			return rawResp, err
		}
		return out, nil
	}
	var respRoot map[string]any
	if err := json.Unmarshal(trimmed, &respRoot); err != nil {
		return rawResp, err
	}
	if err := applyFilterToResultMap(ctx, respRoot, rawReq, code); err != nil {
		return rawResp, err
	}
	out, err := json.Marshal(respRoot)
	if err != nil {
		return rawResp, err
	}
	return out, nil
}

func applyFilterToResultMap(ctx context.Context, respRoot map[string]any, rawReq []byte, code *gojq.Code) error {
	result, ok := respRoot["result"].(map[string]any)
	if !ok {
		return nil
	}
	if isErr, hasErr := result["isError"].(bool); hasErr && isErr {
		return nil
	}
	argsMap := extractReqArguments(rawReq)
	contentList, ok := result["content"].([]any)
	if !ok {
		return nil
	}
	for i, item := range contentList {
		itemMap, isMap := item.(map[string]any)
		if !isMap {
			continue
		}
		if t, typeOK := itemMap["type"].(string); !typeOK || t != "text" {
			continue
		}
		textVal, textOK := itemMap["text"].(string)
		if !textOK {
			continue
		}
		var parsedText any
		if err := json.Unmarshal([]byte(textVal), &parsedText); err != nil {
			continue
		}
		runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		iter := code.RunWithContext(runCtx, parsedText, argsMap)
		v, ok := iter.Next()
		cancel()
		if !ok {
			return fmt.Errorf("filter returned no output")
		}
		if err, isErr := v.(error); isErr {
			return err
		}
		compact, err := json.Marshal(v)
		if err != nil {
			return err
		}
		itemMap["text"] = string(compact)
		contentList[i] = itemMap
	}
	result["content"] = contentList
	return nil
}

func extractReqArguments(rawReq []byte) map[string]any {
	argsMap := make(map[string]any)
	trimmed := bytes.TrimSpace(rawReq)
	if len(trimmed) == 0 {
		return argsMap
	}
	var reqRoot map[string]any
	if trimmed[0] == '[' {
		var batch []map[string]any
		if err := json.Unmarshal(trimmed, &batch); err == nil && len(batch) > 0 {
			reqRoot = batch[0]
		}
	} else {
		if err := json.Unmarshal(trimmed, &reqRoot); err != nil {
			return argsMap
		}
	}
	if reqRoot == nil {
		return argsMap
	}
	params, ok := reqRoot["params"].(map[string]any)
	if !ok {
		return argsMap
	}
	rawArgs, exists := params["arguments"]
	if !exists || rawArgs == nil {
		return argsMap
	}
	switch a := rawArgs.(type) {
	case map[string]any:
		return a
	case string:
		var parsed map[string]any
		if err := json.Unmarshal([]byte(a), &parsed); err == nil && parsed != nil {
			return parsed
		}
	}
	return argsMap
}

// ShapeResponseBody transforms response bytes according to policy and RPC method.
func ShapeResponseBody(ctx context.Context, respBytes []byte, contentType, rpcMethod, tool string, reqBody []byte, policy env.ServerPolicy) ([]byte, error) {
	if strings.HasPrefix(contentType, "text/event-stream") {
		return shapeSSEBody(ctx, respBytes, rpcMethod, tool, reqBody, policy)
	}
	return shapeSingleJSONRPC(ctx, respBytes, rpcMethod, tool, reqBody, policy)
}

func shapeSingleJSONRPC(ctx context.Context, respBytes []byte, rpcMethod, tool string, reqBody []byte, policy env.ServerPolicy) ([]byte, error) {
	switch rpcMethod {
	case mcpMethodToolsList:
		return FilterToolsResponse(respBytes, policy)
	case mcpMethodInitialize:
		return InjectInstructions(respBytes, policy.Instructions)
	case mcpMethodToolsCall:
		cleanTool := strings.ToLower(strings.TrimSpace(tool))
		if code, ok := policy.ResponseFilters[cleanTool]; ok && code != nil {
			return ApplyResponseFilter(ctx, respBytes, reqBody, code)
		}
		return respBytes, nil
	default:
		return respBytes, nil
	}
}

func shapeSSEBody(ctx context.Context, respBytes []byte, rpcMethod, tool string, reqBody []byte, policy env.ServerPolicy) ([]byte, error) {
	var buf bytes.Buffer
	if err := ShapeSSEStream(ctx, bytes.NewReader(respBytes), &buf, rpcMethod, tool, reqBody, policy); err != nil {
		return respBytes, err
	}
	return buf.Bytes(), nil
}

// ShapeSSEStream streams and transforms SSE data: events line-by-line from r to w.
func ShapeSSEStream(ctx context.Context, r io.Reader, w io.Writer, rpcMethod, tool string, reqBody []byte, policy env.ServerPolicy) error {
	scanner := bufio.NewScanner(r)
	const maxScanTokenSize = 16 * 1024 * 1024
	bufScan := make([]byte, 64*1024)
	scanner.Buffer(bufScan, maxScanTokenSize)

	for scanner.Scan() {
		outLine := transformSSELine(ctx, scanner.Bytes(), rpcMethod, tool, reqBody, policy)
		if _, writeErr := w.Write(outLine); writeErr != nil {
			return writeErr
		}
	}
	return scanner.Err()
}

func transformSSELine(ctx context.Context, line []byte, rpcMethod, tool string, reqBody []byte, policy env.ServerPolicy) []byte {
	if !bytes.HasPrefix(line, []byte("data:")) {
		var out bytes.Buffer
		out.Write(line)
		out.WriteByte('\n')
		return out.Bytes()
	}
	prefixLen := len("data:")
	spaceLen := 0
	if len(line) > prefixLen && line[prefixLen] == ' ' {
		spaceLen = 1
	}
	payload := line[prefixLen+spaceLen:]
	transformed, err := shapeSingleJSONRPC(ctx, payload, rpcMethod, tool, reqBody, policy)
	if err != nil || len(transformed) == 0 {
		var out bytes.Buffer
		out.Write(line)
		out.WriteByte('\n')
		return out.Bytes()
	}
	var out bytes.Buffer
	out.WriteString("data:")
	if spaceLen > 0 {
		out.WriteByte(' ')
	}
	out.Write(transformed)
	out.WriteByte('\n')
	return out.Bytes()
}
