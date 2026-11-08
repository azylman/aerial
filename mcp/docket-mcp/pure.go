package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/ollama"
	"gopkg.in/yaml.v3"
)

// Config holds runtime configuration for docket-mcp.
type Config struct {
	Port             string        `yaml:"port"`
	VikunjaURL       string        `yaml:"vikunja_url"`
	VikunjaUsername  string        `yaml:"vikunja_username"`
	VikunjaPassword  string        `yaml:"vikunja_password"`
	DefaultProjectID int64         `yaml:"default_project_id"`
	DatabaseURL      string        `yaml:"database_url"`
	Ollama           ollama.Config `yaml:"ollama"`
}

// LoadConfig loads configuration from path or default locations with environment overrides.
func LoadConfig(path string) (*Config, error) {
	resolved := path
	if strings.TrimSpace(resolved) == "" {
		resolved = httpkit.ResolveConfigPathWithLocal(
			"CONFIG_PATH",
			"docket-mcp.yaml",
			"/etc/docket-mcp/config.yaml",
			"config.yaml",
		)
	}

	cfg := &Config{
		Port:            "8080",
		VikunjaURL:      "http://vikunja:3456",
		VikunjaUsername: "aerial",
		VikunjaPassword: "aerial-password",
		DatabaseURL:     "postgres://postgres:postgres@localhost:5432/docket?sslmode=disable",
		Ollama: ollama.Config{
			BaseURL: ollama.DefaultBaseURL,
			Model:   ollama.DefaultModel,
			NumCtx:  ollama.DefaultNumCtx,
		},
	}

	if err := loadConfigFile(resolved, cfg); err != nil {
		return nil, err
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

func loadConfigFile(resolved string, cfg *Config) error {
	fi, err := os.Stat(resolved)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if fi.IsDir() {
		return fmt.Errorf("config path is a directory: %s", resolved)
	}
	data, err := os.ReadFile(resolved)
	if err != nil || len(data) == 0 {
		return err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("failed to parse config %s: %w", resolved, err)
	}
	return nil
}

func applyEnvOverrides(cfg *Config) {
	if p := strings.TrimSpace(os.Getenv("PORT")); p != "" {
		cfg.Port = p
	}
	if vu := strings.TrimSpace(os.Getenv("VIKUNJA_URL")); vu != "" {
		cfg.VikunjaURL = vu
	}
	if vu := strings.TrimSpace(os.Getenv("VIKUNJA_USERNAME")); vu != "" {
		cfg.VikunjaUsername = vu
	}
	if vp := strings.TrimSpace(os.Getenv("VIKUNJA_PASSWORD")); vp != "" {
		cfg.VikunjaPassword = vp
	}
	if pidStr := strings.TrimSpace(os.Getenv("DEFAULT_PROJECT_ID")); pidStr != "" {
		if pid, err := strconv.ParseInt(pidStr, 10, 64); err == nil && pid > 0 {
			cfg.DefaultProjectID = pid
		}
	}
	if dsn := httpkit.BuildPostgresDSN(os.Getenv, "docket"); dsn != "" {
		cfg.DatabaseURL = dsn
	}
	if ob := strings.TrimSpace(os.Getenv("OLLAMA_BASE_URL")); ob != "" {
		cfg.Ollama.BaseURL = ob
	}
	if om := strings.TrimSpace(os.Getenv("OLLAMA_MODEL")); om != "" {
		cfg.Ollama.Model = om
	}
}

// TaskNode represents a task graph node with relations.
type TaskNode struct {
	ID          int64   `json:"id"`
	Title       string  `json:"title"`
	Description string  `json:"description,omitempty"`
	Done        bool    `json:"done"`
	Resolution  string  `json:"resolution,omitempty"`
	Status      string  `json:"status"`
	ParentID    *int64  `json:"parent_id,omitempty"`
	SubtaskIDs  []int64 `json:"subtask_ids,omitempty"`
	BlockedBy   []int64 `json:"blocked_by,omitempty"`
	Blocks      []int64 `json:"blocks,omitempty"`
	Related     []int64 `json:"related,omitempty"`
}

// TaskSummary is a compact task representation.
type TaskSummary struct {
	ID        int64   `json:"id"`
	Title     string  `json:"title"`
	Status    string  `json:"status"`
	ParentID  *int64  `json:"parent_id,omitempty"`
	BlockedBy []int64 `json:"blocked_by,omitempty"`
	Blocks    []int64 `json:"blocks,omitempty"`
}

// ReadyTask represents an unblocked open task with nested ready subtasks.
type ReadyTask struct {
	ID            int64       `json:"id"`
	Title         string      `json:"title"`
	Status        string      `json:"status"`
	ParentID      *int64      `json:"parent_id,omitempty"`
	ReadySubtasks []ReadyTask `json:"ready_subtasks,omitempty"`
}

// ScoredTaskSummary couples a task with its search relevance score.
type ScoredTaskSummary struct {
	ID       int64   `json:"id"`
	Title    string  `json:"title"`
	Status   string  `json:"status"`
	ParentID *int64  `json:"parent_id,omitempty"`
	Score    float64 `json:"score"`
}

// TaskComment represents a comment on a task.
type TaskComment struct {
	ID        int64     `json:"id"`
	Author    string    `json:"author,omitempty"`
	Comment   string    `json:"comment"`
	CreatedAt time.Time `json:"created_at"`
}

// HydratedTask contains full task details, relations, and comments.
type HydratedTask struct {
	ID          int64         `json:"id"`
	Title       string        `json:"title"`
	Status      string        `json:"status"`
	Description string        `json:"description,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
	ClosedAt    *time.Time    `json:"closed_at,omitempty"`
	Parent      *TaskSummary  `json:"parent,omitempty"`
	Subtasks    []TaskSummary `json:"subtasks,omitempty"`
	Blocks      []TaskSummary `json:"blocks,omitempty"`
	BlockedBy   []TaskSummary `json:"blocked_by,omitempty"`
	Related     []TaskSummary `json:"related,omitempty"`
	Comments    []TaskComment `json:"comments,omitempty"`
}

// ClosedTaskResult encapsulates the result of closing a task.
type ClosedTaskResult struct {
	ID           int64   `json:"id"`
	Title        string  `json:"title"`
	Status       string  `json:"status"`
	UnblockedIDs []int64 `json:"unblocked_ids,omitempty"`
}

// ResolveTaskStatus converts done and resolution flags to a 3-state status string.
func ResolveTaskStatus(done bool, resolution string) string {
	if !done {
		return "open"
	}
	if strings.TrimSpace(resolution) == "wont_do" {
		return "wont_do"
	}
	return "completed"
}

// DetectGraphCycle checks if adding a directed edge would introduce a cycle.
func DetectGraphCycle(nodes map[int64]TaskNode, sourceID, targetID int64, edgeType string) error {
	if sourceID == targetID {
		return fmt.Errorf("self-link cycle: task %d cannot link to itself", sourceID)
	}
	switch edgeType {
	case "parent":
		if isSubtreeDescendant(nodes, sourceID, targetID) {
			return fmt.Errorf("cycle detected: task %d is already an ancestor of task %d", sourceID, targetID)
		}
	case "blocks":
		if isReachableAlongBlocks(nodes, targetID, sourceID) {
			return fmt.Errorf("cycle detected: task %d already transitively blocks task %d", targetID, sourceID)
		}
	case "related":
		return nil
	default:
		return fmt.Errorf("unknown edge type: %q", edgeType)
	}
	return nil
}

func isSubtreeDescendant(nodes map[int64]TaskNode, ancestorID, candidateID int64) bool {
	visited := make(map[int64]bool)
	queue := []int64{ancestorID}
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		if curr == candidateID && curr != ancestorID {
			return true
		}
		if visited[curr] {
			continue
		}
		visited[curr] = true
		if node, ok := nodes[curr]; ok {
			queue = append(queue, node.SubtaskIDs...)
		}
	}
	return false
}

func isReachableAlongBlocks(nodes map[int64]TaskNode, startID, targetID int64) bool {
	visited := make(map[int64]bool)
	queue := []int64{startID}
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		if curr == targetID {
			return true
		}
		if visited[curr] {
			continue
		}
		visited[curr] = true
		if node, ok := nodes[curr]; ok {
			queue = append(queue, node.Blocks...)
		}
	}
	return false
}

func isTaskReady(node TaskNode, nodes map[int64]TaskNode) bool {
	if node.Status != "open" {
		return false
	}
	for _, bid := range node.BlockedBy {
		if bNode, exists := nodes[bid]; exists && bNode.Status != "completed" {
			return false
		}
	}
	return true
}

func isTopNode(node TaskNode, nodes map[int64]TaskNode) bool {
	if node.ParentID == nil || *node.ParentID <= 0 {
		return true
	}
	_, parentExists := nodes[*node.ParentID]
	return !parentExists
}

func collectTopReadyNodes(nodes map[int64]TaskNode, scopedParentID *int64) []TaskNode {
	var topNodes []TaskNode
	if scopedParentID == nil {
		for _, node := range nodes {
			if isTopNode(node, nodes) && isTaskReady(node, nodes) {
				topNodes = append(topNodes, node)
			}
		}
	} else {
		for _, node := range nodes {
			if node.ParentID != nil && *node.ParentID == *scopedParentID && isTaskReady(node, nodes) {
				topNodes = append(topNodes, node)
			}
		}
	}
	slices.SortFunc(topNodes, func(a, b TaskNode) int { return cmp.Compare(a.ID, b.ID) })
	return topNodes
}

// BuildReadyTree computes unblocked ready tasks nested top-down.
func BuildReadyTree(nodes map[int64]TaskNode, scopedParentID *int64) []ReadyTask {
	topNodes := collectTopReadyNodes(nodes, scopedParentID)
	res := make([]ReadyTask, 0, len(topNodes))
	for _, n := range topNodes {
		res = append(res, buildReadyNode(n, nodes))
	}
	return res
}

func buildReadyNode(node TaskNode, nodes map[int64]TaskNode) ReadyTask {
	rt := ReadyTask{
		ID:       node.ID,
		Title:    node.Title,
		Status:   node.Status,
		ParentID: node.ParentID,
	}
	var readySubs []ReadyTask
	for _, sid := range node.SubtaskIDs {
		if sub, ok := nodes[sid]; ok && isTaskReady(sub, nodes) {
			readySubs = append(readySubs, buildReadyNode(sub, nodes))
		}
	}
	slices.SortFunc(readySubs, func(a, b ReadyTask) int { return cmp.Compare(a.ID, b.ID) })
	if len(readySubs) > 0 {
		rt.ReadySubtasks = readySubs
	}
	return rt
}

// ValidateCloseGuard validates completion prerequisites for closing a task.
func ValidateCloseGuard(nodes map[int64]TaskNode, id int64, resolution string) error {
	switch resolution {
	case "wont_do":
		if _, exists := nodes[id]; !exists {
			return fmt.Errorf("task %d not found in graph", id)
		}
		return validateWontDoClose(nodes, id)
	case "completed":
		node, exists := nodes[id]
		if !exists {
			return fmt.Errorf("task %d not found in graph", id)
		}
		return validateCompletedClose(nodes, node, id)
	default:
		return fmt.Errorf("invalid resolution: %q (must be 'completed' or 'wont_do')", resolution)
	}
}

func validateWontDoClose(nodes map[int64]TaskNode, id int64) error {
	closingSet := make(map[int64]bool)
	closingSet[id] = true
	for _, sid := range CollectOpenSubtreeIDs(nodes, id) {
		closingSet[sid] = true
	}
	openBlockedSet := make(map[int64]bool)
	for cid := range closingSet {
		for _, bid := range nodes[cid].Blocks {
			if !closingSet[bid] {
				if bNode, ok := nodes[bid]; ok && bNode.Status == "open" {
					openBlockedSet[bid] = true
				}
			}
		}
	}
	if len(openBlockedSet) > 0 {
		openBlocked := make([]int64, 0, len(openBlockedSet))
		for bid := range openBlockedSet {
			openBlocked = append(openBlocked, bid)
		}
		slices.Sort(openBlocked)
		return fmt.Errorf("cannot close task %d as wont_do: blocks open tasks: %v", id, openBlocked)
	}
	return nil
}

func validateCompletedClose(nodes map[int64]TaskNode, node TaskNode, id int64) error {
	var openBlockers []int64
	for _, bid := range node.BlockedBy {
		if b, ok := nodes[bid]; ok && b.Status == "open" {
			openBlockers = append(openBlockers, bid)
		}
	}
	var openSubtasks []int64
	for _, sid := range node.SubtaskIDs {
		if s, ok := nodes[sid]; ok && s.Status == "open" {
			openSubtasks = append(openSubtasks, sid)
		}
	}
	if len(openBlockers) > 0 || len(openSubtasks) > 0 {
		return fmt.Errorf("cannot complete task %d: open blockers: %v, open subtasks: %v", id, openBlockers, openSubtasks)
	}
	return nil
}

// CollectOpenSubtreeIDs collects IDs of all open tasks transitively beneath rootID.
func CollectOpenSubtreeIDs(nodes map[int64]TaskNode, rootID int64) []int64 {
	var openIDs []int64
	visited := make(map[int64]bool)

	var walk func(currID int64)
	walk = func(currID int64) {
		node, exists := nodes[currID]
		if !exists {
			return
		}
		for _, sid := range node.SubtaskIDs {
			if visited[sid] {
				continue
			}
			visited[sid] = true
			if sub, ok := nodes[sid]; ok {
				if sub.Status == "open" {
					openIDs = append(openIDs, sid)
				}
				walk(sid)
			}
		}
	}
	walk(rootID)
	slices.Sort(openIDs)
	return openIDs
}

// ComputeUnblockedIDs identifies tasks that transitioned from blocked to ready.
func ComputeUnblockedIDs(before, after map[int64]TaskNode, closedIDs []int64) []int64 {
	closedSet := make(map[int64]bool, len(closedIDs))
	for _, id := range closedIDs {
		closedSet[id] = true
	}
	var unblocked []int64
	for id, afterNode := range after {
		if closedSet[id] || afterNode.Status != "open" {
			continue
		}
		beforeNode, exists := before[id]
		if !exists || beforeNode.Status != "open" {
			continue
		}
		if !isTaskReady(beforeNode, before) && isTaskReady(afterNode, after) {
			unblocked = append(unblocked, id)
		}
	}
	slices.Sort(unblocked)
	return unblocked
}

// Tool represents an MCP Tool definition.
type Tool struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	InputSchema InputSchema `json:"inputSchema"`
}

// InputSchema defines the JSON schema for tool inputs.
type InputSchema struct {
	Type       string                 `json:"type"`
	Properties map[string]PropertyDef `json:"properties,omitempty"`
	Required   []string               `json:"required,omitempty"`
}

// PropertyDef describes a parameter field in the schema.
type PropertyDef struct {
	Type        string         `json:"type"`
	Description string         `json:"description,omitempty"`
	Enum        []string       `json:"enum,omitempty"`
	Items       map[string]any `json:"items,omitempty"`
	Default     any            `json:"default,omitempty"`
}

// JSONRPCRequest represents an incoming JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// JSONRPCResponse represents an outgoing JSON-RPC 2.0 response.
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC 2.0 error object.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ToolCallParams defines params for "tools/call".
type ToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// ContentItem represents a content block in an MCP tool call result.
type ContentItem struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	MIMEType string `json:"mimeType,omitempty"`
	Data     string `json:"data,omitempty"`
}

// ToolCallResult defines the MCP result payload.
type ToolCallResult struct {
	Content []ContentItem `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// Tools contains all 9 registered MCP tools for docket-mcp.
var Tools = []Tool{
	{
		Name:        "ready",
		Description: "Returns unblocked open tasks ready for execution, top-down with subtasks nested in ready_subtasks. If parent_id is omitted, returns top-level tasks. If parent_id is provided, returns ready subtasks under that parent.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"parent_id": {Type: "integer", Description: "Optional parent task ID to scope ready subtasks under."},
			},
		},
	},
	{
		Name:        "list",
		Description: "List tasks filtered by status ('open', 'completed', 'wont_do', 'all', default 'open') and optional parent_id.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"status":    {Type: "string", Description: "Status filter: 'open', 'completed', 'wont_do', 'all'", Enum: []string{"open", "completed", "wont_do", "all"}, Default: "open"},
				"parent_id": {Type: "integer", Description: "Optional parent task ID to filter by."},
			},
		},
	},
	{
		Name:        "search",
		Description: "Search tasks using hybrid vector and full-text search (Weighted RRF) with status pre-filtering.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"query":  {Type: "string", Description: "Search query text."},
				"status": {Type: "string", Description: "Status filter: 'open', 'completed', 'wont_do', 'all'", Enum: []string{"open", "completed", "wont_do", "all"}, Default: "open"},
				"limit":  {Type: "integer", Description: "Maximum number of results to return (default 10).", Default: 10},
			},
			Required: []string{"query"},
		},
	},
	{
		Name:        "show",
		Description: "Show full details of a task including parent, subtasks, blockers, blocked tasks, related tasks, and comments.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"id": {Type: "integer", Description: "Task ID to show."},
			},
			Required: []string{"id"},
		},
	},
	{
		Name:        "create",
		Description: "Create a new task with optional description, parent, blocker relations, and related links.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"title":       {Type: "string", Description: "Task title (required)."},
				"description": {Type: "string", Description: "Task description in markdown."},
				"parent_id":   {Type: "integer", Description: "Optional parent task ID."},
				"blocks":      {Type: "array", Description: "Optional task IDs that this new task blocks.", Items: map[string]any{"type": "integer"}},
				"blocked_by":  {Type: "array", Description: "Optional task IDs that block this new task.", Items: map[string]any{"type": "integer"}},
				"related":     {Type: "array", Description: "Optional related task IDs.", Items: map[string]any{"type": "integer"}},
			},
			Required: []string{"title"},
		},
	},
	{
		Name:        "update",
		Description: "Update task title and/or description.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"id":          {Type: "integer", Description: "Task ID to update."},
				"title":       {Type: "string", Description: "New task title."},
				"description": {Type: "string", Description: "New task description."},
			},
			Required: []string{"id"},
		},
	},
	{
		Name:        "comment",
		Description: "Add a comment to a task.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"id":   {Type: "integer", Description: "Task ID to comment on."},
				"text": {Type: "string", Description: "Comment text."},
			},
			Required: []string{"id", "text"},
		},
	},
	{
		Name:        "link",
		Description: "Create or remove a relation between tasks ('parent', 'blocks', 'related') with cycle detection.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"id":        {Type: "integer", Description: "Source task ID."},
				"target_id": {Type: "integer", Description: "Target task ID."},
				"type":      {Type: "string", Description: "Relation type: 'parent', 'blocks', or 'related'.", Enum: []string{"parent", "blocks", "related"}},
				"remove":    {Type: "boolean", Description: "Whether to remove the relation (default false).", Default: false},
			},
			Required: []string{"id", "target_id", "type"},
		},
	},
	{
		Name:        "close",
		Description: "Close a task with resolution 'completed' or 'wont_do' and a required reason. Validates no open blockers/subtasks for completed, cascades for wont_do.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"id":         {Type: "integer", Description: "Task ID to close."},
				"resolution": {Type: "string", Description: "Resolution: 'completed' or 'wont_do'.", Enum: []string{"completed", "wont_do"}},
				"reason":     {Type: "string", Description: "Required explanation for closing the task."},
			},
			Required: []string{"id", "resolution", "reason"},
		},
	},
}
