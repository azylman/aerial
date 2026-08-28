package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
}

type RPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type ToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type Server struct {
	handler *ToolHandler
}

func NewServer(handler *ToolHandler) *Server {
	return &Server{handler: handler}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/mcp", s.handleMCP)
	mux.HandleFunc("/sse", s.handleMCP)
	mux.HandleFunc("/", s.handleMCP)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	// If GET /health hits root handler
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
	}

	// Handle SSE / Streamable HTTP GET probe
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("MCP Scheduler Service Ready\n"))
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		http.Error(w, `{"error":"Empty request body"}`, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Support JSON-RPC batch arrays if sent
	if strings.HasPrefix(trimmed, "[") {
		var requests []JSONRPCRequest
		if err := json.Unmarshal(body, &requests); err != nil {
			resp := JSONRPCResponse{
				JSONRPC: "2.0",
				Error:   &RPCError{Code: -32700, Message: "Parse error"},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		var responses []JSONRPCResponse
		for _, req := range requests {
			resp := s.processRequest(req)
			if resp != nil {
				responses = append(responses, *resp)
			}
		}
		_ = json.NewEncoder(w).Encode(responses)
		return
	}

	// Single JSON-RPC request
	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		resp := JSONRPCResponse{
			JSONRPC: "2.0",
			Error:   &RPCError{Code: -32700, Message: "Parse error"},
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	resp := s.processRequest(req)
	if resp != nil {
		_ = json.NewEncoder(w).Encode(resp)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) processRequest(req JSONRPCRequest) *JSONRPCResponse {
	// Notifications (no ID)
	if req.ID == nil && strings.HasPrefix(req.Method, "notifications/") {
		return nil
	}

	switch req.Method {
	case "initialize":
		return &JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]interface{}{
					"tools": map[string]interface{}{},
				},
				"serverInfo": map[string]interface{}{
					"name":    "scheduler-mcp",
					"version": "1.0.0",
				},
			},
		}

	case "ping":
		return &JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]interface{}{},
		}

	case "tools/list":
		return &JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"tools": s.getToolsList(),
			},
		}

	case "tools/call":
		var params ToolCallParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return &JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   &RPCError{Code: -32602, Message: "Invalid params: " + err.Error()},
			}
		}

		toolName := strings.TrimPrefix(params.Name, "scheduler_")

		var result interface{}
		var callErr error

		switch toolName {
		case "schedule_recurring":
			result, callErr = s.handler.HandleScheduleRecurring(params.Arguments)
		case "schedule_once":
			result, callErr = s.handler.HandleScheduleOnce(params.Arguments)
		case "list_schedules":
			result, callErr = s.handler.HandleListSchedules(params.Arguments)
		case "cancel_schedule":
			result, callErr = s.handler.HandleCancelSchedule(params.Arguments)
		default:
			return &JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   &RPCError{Code: -32601, Message: fmt.Sprintf("Unknown tool: %s", params.Name)},
			}
		}

		if callErr != nil {
			return &JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: map[string]interface{}{
					"isError": true,
					"content": []map[string]interface{}{
						{
							"type": "text",
							"text": fmt.Sprintf("Tool error: %v", callErr),
						},
					},
				},
			}
		}

		resultBytes, _ := json.MarshalIndent(result, "", "  ")
		return &JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"content": []map[string]interface{}{
					{
						"type": "text",
						"text": string(resultBytes),
					},
				},
			},
		}

	default:
		if req.ID == nil {
			return nil
		}
		return &JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &RPCError{Code: -32601, Message: fmt.Sprintf("Method not found: %s", req.Method)},
		}
	}
}

func (s *Server) getToolsList() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name":        "schedule_recurring",
			"description": "Register a persistent recurring schedule. Fresh Discord threads will be created in the target channel on each run.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"channel_id": map[string]interface{}{
						"type":        "string",
						"description": "Discord Channel ID where fresh threads will be spawned.",
					},
					"cron_expression": map[string]interface{}{
						"type":        "string",
						"description": "Standard 5-field cron expression (e.g. '0 20 * * 5') or macro (@daily, @weekly, @monthly).",
					},
					"prompt": map[string]interface{}{
						"type":        "string",
						"description": "Instructions to execute on every occurrence.",
					},
					"title_prefix": map[string]interface{}{
						"type":        "string",
						"description": "Title prefix for spawned threads (e.g. 'Weekly Meal Plan').",
					},
					"timezone": map[string]interface{}{
						"type":        "string",
						"description": "Timezone for evaluation (e.g. 'America/New_York' or 'UTC'). Defaults to 'America/New_York'.",
					},
				},
				"required": []string{"channel_id", "cron_expression", "prompt"},
			},
		},
		{
			"name":        "schedule_once",
			"description": "Register a persistent one-shot reminder that triggers once at a designated time or relative duration.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"target_id": map[string]interface{}{
						"type":        "string",
						"description": "Target Discord thread ID or channel ID where reminder will be delivered.",
					},
					"run_at": map[string]interface{}{
						"type":        "string",
						"description": "ISO 8601 timestamp (e.g. '2026-08-28T21:00:00Z') or relative duration (e.g. '30m', '2h', '1d').",
					},
					"prompt": map[string]interface{}{
						"type":        "string",
						"description": "Content/instructions of the reminder.",
					},
					"timezone": map[string]interface{}{
						"type":        "string",
						"description": "Timezone for absolute timestamp evaluation (e.g. 'America/New_York' or 'UTC'). Defaults to 'America/New_York'.",
					},
				},
				"required": []string{"target_id", "run_at", "prompt"},
			},
		},
		{
			"name":        "list_schedules",
			"description": "List all active recurring schedules and pending reminders.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"target_id": map[string]interface{}{
						"type":        "string",
						"description": "Optional Discord Channel or Thread ID to filter schedules.",
					},
				},
			},
		},
		{
			"name":        "cancel_schedule",
			"description": "Cancel and delete an existing schedule by schedule ID.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"schedule_id": map[string]interface{}{
						"type":        "string",
						"description": "The ID of the schedule to cancel.",
					},
				},
				"required": []string{"schedule_id"},
			},
		},
	}
}
