package main

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/ollama"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/google/jsonschema-go/jsonschema"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server encapsulates HTTP routes, Vikunja client, and SQL task store for docket-mcp.
type Server struct {
	cfg           *Config
	vikunja       *VikunjaClient
	store         TaskStore
	ollamaClient  *ollama.Client
	streamHandler http.Handler
}

// serverInstructions is sent on initialize so clients that defer tool schemas
// still learn how the task tools fit together.
const serverInstructions = `Task IDs are integers returned by create, ready, list and search.
- Next work: ready lists open tasks whose blockers are all completed (a wont_do blocker still blocks). Ready subtasks nest only under a ready parent; pass parent_id to see them under a blocked one. list browses by status or parent; show returns one task with relations and comments.
- Run search before create to avoid duplicates.
- create sets parent_id, blocks, blocked_by and related at once; link adds or (remove=true) drops edges later. update edits only title and description; status changes go through close.
- close completed is refused while open blockers or open direct subtasks remain. close wont_do also closes the open subtree and is refused if any of it blocks an open task outside it. close returns unblocked_ids.`

// NewServer initializes a Server instance and its MCP tool handler.
func NewServer(cfg *Config, vikunja *VikunjaClient, store TaskStore, ollamaClient *ollama.Client) *Server {
	s := &Server{
		cfg:          cfg,
		vikunja:      vikunja,
		store:        store,
		ollamaClient: ollamaClient,
	}
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "docket-mcp", Version: "1.0.0"}, &mcp.ServerOptions{Instructions: serverInstructions})
	s.registerTools(mcpServer)
	s.streamHandler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return s
}

// HandleHealth handles GET and HEAD readiness requests.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// HandleMCPStreamable serves MCP over streamable HTTP, accepting legacy docket_-prefixed tool names.
func (s *Server) HandleMCPStreamable(w http.ResponseWriter, r *http.Request) {
	httpkit.ServeMCPStream(w, r, s.streamHandler, "docket-mcp ready\n", "docket_", "")
}

var taskStatusFilters = []any{"open", "completed", "wont_do", "all"}

func (s *Server) registerTools(srv *mcp.Server) {
	listSchema := inputSchema[ListArgs]()
	setEnum(listSchema, "status", `"open"`, taskStatusFilters...)
	searchSchema := inputSchema[SearchArgs]()
	setEnum(searchSchema, "status", `"open"`, taskStatusFilters...)
	searchSchema.Properties["limit"].Default = json.RawMessage(`10`)
	linkSchema := inputSchema[LinkArgs]()
	setEnum(linkSchema, "type", "", "parent", "blocks", "related")
	linkSchema.Properties["remove"].Default = json.RawMessage(`false`)
	closeSchema := inputSchema[CloseArgs]()
	setEnum(closeSchema, "resolution", "", "completed", "wont_do")

	addTextTool(srv, &mcp.Tool{
		Name:        "ready",
		Description: "Returns unblocked open tasks ready for execution, top-down with subtasks nested in ready_subtasks. If parent_id is omitted, returns top-level tasks. If parent_id is provided, returns ready subtasks under that parent.",
		InputSchema: inputSchema[ReadyArgs](),
	}, s.toolReady)
	addTextTool(srv, &mcp.Tool{
		Name:        "list",
		Description: "List tasks filtered by status ('open', 'completed', 'wont_do', 'all', default 'open') and optional parent_id.",
		InputSchema: listSchema,
	}, s.toolList)
	addTextTool(srv, &mcp.Tool{
		Name:        "search",
		Description: "Search tasks using hybrid semantic and keyword search (Weighted RRF) with status pre-filtering.",
		InputSchema: searchSchema,
	}, s.toolSearch)
	addTextTool(srv, &mcp.Tool{
		Name:        "show",
		Description: "Show full details of a task including parent, subtasks, blockers, blocked tasks, related tasks, and comments.",
		InputSchema: inputSchema[ShowArgs](),
	}, s.toolShow)
	addTextTool(srv, &mcp.Tool{
		Name:        "create",
		Description: "Create a new task with optional description, parent, blocker relations, and related links; any unknown referenced task ID rejects the create.",
		InputSchema: inputSchema[CreateArgs](),
	}, s.toolCreate)
	addTextTool(srv, &mcp.Tool{
		Name:        "update",
		Description: "Update task title and/or description.",
		InputSchema: inputSchema[UpdateArgs](),
	}, s.toolUpdate)
	addTextTool(srv, &mcp.Tool{
		Name:        "comment",
		Description: "Add a comment to a task.",
		InputSchema: inputSchema[CommentArgs](),
	}, s.toolComment)
	addTextTool(srv, &mcp.Tool{
		Name:        "link",
		Description: "Create or remove a relation between tasks ('parent', 'blocks', 'related'); new parent and blocks edges that would form a cycle are rejected.",
		InputSchema: linkSchema,
	}, s.toolLink)
	addTextTool(srv, &mcp.Tool{
		Name:        "close",
		Description: "Close a task with resolution 'completed' or 'wont_do' and a required reason. Validates no open blockers/subtasks for completed, cascades for wont_do.",
		InputSchema: closeSchema,
	}, s.toolClose)
}

// addTextTool registers a tool whose logic returns its result text and whether it is an error.
func addTextTool[In any](srv *mcp.Server, tool *mcp.Tool, run func(context.Context, In) (string, bool)) {
	mcp.AddTool(srv, tool, func(ctx context.Context, _ *mcp.CallToolRequest, args In) (*mcp.CallToolResult, any, error) {
		text, isErr := run(ctx, args)
		return &mcp.CallToolResult{IsError: isErr, Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil, nil
	})
}

// inputSchema infers T's input schema and collapses the ["null", X] unions inferred
// for pointer and slice fields to plain X, matching the types clients have always seen.
func inputSchema[T any]() *jsonschema.Schema {
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		panic(fmt.Sprintf("docket-mcp: infer input schema: %v", err))
	}
	for _, prop := range schema.Properties {
		if len(prop.Types) == 2 && prop.Types[0] == "null" {
			prop.Type, prop.Types = prop.Types[1], nil
		}
	}
	return schema
}

// setEnum restricts a string property to values, with an optional JSON-encoded default.
func setEnum(schema *jsonschema.Schema, prop, defaultJSON string, values ...any) {
	schema.Properties[prop].Enum = values
	if defaultJSON != "" {
		schema.Properties[prop].Default = json.RawMessage(defaultJSON)
	}
}

func (s *Server) loadGraph(ctx context.Context) (map[int64]TaskNode, error) {
	projectID, err := s.vikunja.GetDefaultProject(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get default project: %w", err)
	}

	tasks, err := s.vikunja.ListProjectTasks(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to list project tasks: %w", err)
	}

	taskIDs := make([]int64, len(tasks))
	for i, t := range tasks {
		taskIDs[i] = t.ID
	}

	resMap, err := s.store.FetchTaskResolutions(ctx, taskIDs)
	if err != nil {
		resMap = make(map[int64]string)
	}

	nodes := make(map[int64]TaskNode, len(tasks))
	for _, t := range tasks {
		nodes[t.ID] = taskToNode(t, resMap[t.ID])
	}
	return nodes, nil
}

// vikunjaNodeSource resolves graph nodes one Vikunja task fetch at a time,
// memoizing within a single request.
type vikunjaNodeSource struct {
	vikunja   *VikunjaClient
	store     TaskStore
	projectID int64
	cache     map[int64]TaskNode
}

func (s *Server) newNodeSource(ctx context.Context) (*vikunjaNodeSource, error) {
	projectID, err := s.vikunja.GetDefaultProject(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get default project: %w", err)
	}
	return &vikunjaNodeSource{vikunja: s.vikunja, store: s.store, projectID: projectID, cache: make(map[int64]TaskNode)}, nil
}

// Node implements NodeSource; tasks missing from Vikunja or outside the project report ok=false.
func (src *vikunjaNodeSource) Node(ctx context.Context, id int64) (TaskNode, bool, error) {
	if node, ok := src.cache[id]; ok {
		return node, true, nil
	}
	task, err := src.vikunja.GetTask(ctx, id)
	if err != nil {
		if isVikunjaNotFound(err) {
			return TaskNode{}, false, nil
		}
		return TaskNode{}, false, fmt.Errorf("failed to fetch task %d: %w", id, err)
	}
	if task.ProjectID != src.projectID {
		return TaskNode{}, false, nil
	}
	var resolution string
	if resMap, err := src.store.FetchTaskResolutions(ctx, []int64{id}); err == nil {
		resolution = resMap[id]
	}
	node := taskToNode(*task, resolution)
	src.cache[id] = node
	return node, true, nil
}

func isVikunjaNotFound(err error) bool {
	var statusErr *HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound
}

// ReadyArgs are the ready tool arguments.
type ReadyArgs struct {
	ParentID *int64 `json:"parent_id,omitempty" jsonschema:"Optional parent task ID to scope ready subtasks under."`
}

func (s *Server) toolReady(ctx context.Context, args ReadyArgs) (string, bool) {
	nodes, err := s.loadGraph(ctx)
	if err != nil {
		return err.Error(), true
	}

	tree := BuildReadyTree(nodes, args.ParentID)
	data, err := json.Marshal(tree)
	if err != nil {
		return fmt.Sprintf("failed to marshal ready tasks: %v", err), true
	}
	return string(data), false
}

// ErrTaskNotFound reports that a task id is absent from the project graph.
var ErrTaskNotFound = errors.New("not found in project graph")

// ListArgs are the list tool arguments; the schema defaults status to "open".
type ListArgs struct {
	Status   string `json:"status,omitempty" jsonschema:"Status filter: 'open', 'completed', 'wont_do', 'all'"`
	ParentID *int64 `json:"parent_id,omitempty" jsonschema:"Optional parent task ID to filter by."`
}

func (s *Server) toolList(ctx context.Context, args ListArgs) (string, bool) {
	summaries, err := s.listTasks(ctx, args.Status, args.ParentID)
	if err != nil {
		return err.Error(), true
	}

	return marshalToolResult(summaries, "task list")
}

// marshalToolResult encodes a tool payload as JSON text, naming what failed on error.
func marshalToolResult(v any, what string) (string, bool) {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("failed to marshal %s: %v", what, err), true
	}
	return string(data), false
}

// listTasks returns graph tasks matching status (or "all") and, when set, parentID, sorted by id.
func (s *Server) listTasks(ctx context.Context, status string, parentID *int64) ([]TaskSummary, error) {
	nodes, err := s.loadGraph(ctx)
	if err != nil {
		return nil, err
	}

	var summaries []TaskSummary
	for _, node := range nodes {
		if status != "all" && node.Status != status {
			continue
		}
		if parentID != nil {
			if node.ParentID == nil || *node.ParentID != *parentID {
				continue
			}
		}
		summaries = append(summaries, summarizeNode(node))
	}
	slices.SortFunc(summaries, func(a, b TaskSummary) int { return cmp.Compare(a.ID, b.ID) })
	return summaries, nil
}

// SearchArgs are the search tool arguments; the schema defaults status to "open" and limit to 10.
type SearchArgs struct {
	Query  string `json:"query" jsonschema:"Search query text."`
	Status string `json:"status,omitempty" jsonschema:"Status filter: 'open', 'completed', 'wont_do', 'all' (default 'open')"`
	Limit  int    `json:"limit,omitempty" jsonschema:"Maximum number of results to return (default 10)."`
}

func (s *Server) toolSearch(ctx context.Context, args SearchArgs) (string, bool) {
	if strings.TrimSpace(args.Query) == "" {
		return "parameter 'query' is required", true
	}

	results, err := s.searchTasks(ctx, args.Query, args.Status, args.Limit)
	if err != nil {
		return err.Error(), true
	}

	return marshalToolResult(results, "search results")
}

// searchTasks runs hybrid RRF search and enriches hits with graph parent ids when the graph loads.
func (s *Server) searchTasks(ctx context.Context, query, status string, limit int) ([]ScoredTaskSummary, error) {
	var emb []float32
	if s.ollamaClient != nil {
		if genEmb, err := s.ollamaClient.GenerateEmbedding(ctx, query, true, 3); err == nil {
			emb = genEmb
		}
	}

	results, err := s.store.SearchTasksRRF(ctx, emb, query, status, limit)
	if err != nil {
		return nil, err
	}

	nodes, err := s.loadGraph(ctx)
	if err == nil {
		for i := range results {
			if node, ok := nodes[results[i].ID]; ok {
				results[i].ParentID = node.ParentID
			}
		}
	}
	return results, nil
}

// ShowArgs are the show tool arguments.
type ShowArgs struct {
	ID int64 `json:"id" jsonschema:"Task ID to show."`
}

func (s *Server) toolShow(ctx context.Context, args ShowArgs) (string, bool) {
	if args.ID <= 0 {
		return "parameter 'id' must be a positive integer", true
	}

	hydrated, err := s.showTask(ctx, args.ID)
	if err != nil {
		return err.Error(), true
	}

	return marshalToolResult(hydrated, "hydrated task")
}

// showTask hydrates a task with its description, relations, and comments.
// It wraps ErrTaskNotFound when id belongs to another project.
func (s *Server) showTask(ctx context.Context, id int64) (HydratedTask, error) {
	task, err := s.vikunja.GetTask(ctx, id)
	if err != nil {
		return HydratedTask{}, fmt.Errorf("failed to fetch task %d: %w", id, err)
	}
	src, err := s.newNodeSource(ctx)
	if err != nil {
		return HydratedTask{}, err
	}
	if task.ProjectID != src.projectID {
		return HydratedTask{}, fmt.Errorf("task %d %w", id, ErrTaskNotFound)
	}

	comments, err := s.vikunja.ListComments(ctx, id)
	if err != nil {
		comments = nil
	}

	neighbors := make(map[int64]TaskNode)
	for _, related := range task.RelatedTasks {
		for _, t := range related {
			if _, seen := neighbors[t.ID]; seen {
				continue
			}
			node, ok, err := src.Node(ctx, t.ID)
			if err != nil {
				return HydratedTask{}, err
			}
			if ok {
				neighbors[t.ID] = node
			}
		}
	}

	var resolution string
	if resolutions, err := s.store.FetchTaskResolutions(ctx, []int64{task.ID}); err == nil {
		resolution = resolutions[task.ID]
	}

	return HydrateTask(*task, ResolveTaskStatus(task.Done, resolution), comments, neighbors), nil
}

// validateCreateRelations checks that every referenced task exists and that the
// new task's blocking edges would not close a cycle.
func validateCreateRelations(ctx context.Context, src NodeSource, parentID *int64, blocks, blockedBy, related []int64) error {
	type ref struct {
		role string
		ids  []int64
	}
	refs := []ref{{"blocked", blocks}, {"blocker", blockedBy}, {"related", related}}
	if parentID != nil {
		refs = append([]ref{{"parent", []int64{*parentID}}}, refs...)
	}
	for _, r := range refs {
		for _, id := range r.ids {
			_, ok, err := src.Node(ctx, id)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%s task %d not found", r.role, id)
			}
		}
	}
	return DetectCreateCycle(ctx, src, blocks, blockedBy)
}

func (s *Server) wireCreateRelations(ctx context.Context, taskID int64, parentID *int64, blocks, blockedBy, related []int64) {
	if parentID != nil {
		if err := s.vikunja.CreateRelation(ctx, taskID, *parentID, "parenttask"); err != nil {
			log.Printf("[docket-mcp] create relation parenttask error: %v", err)
		}
	}
	for _, b := range blocks {
		if err := s.vikunja.CreateRelation(ctx, taskID, b, "blocking"); err != nil {
			log.Printf("[docket-mcp] create relation blocking error: %v", err)
		}
	}
	for _, bb := range blockedBy {
		if err := s.vikunja.CreateRelation(ctx, bb, taskID, "blocking"); err != nil {
			log.Printf("[docket-mcp] create relation blocked_by error: %v", err)
		}
	}
	for _, r := range related {
		if err := s.vikunja.CreateRelation(ctx, taskID, r, "related"); err != nil {
			log.Printf("[docket-mcp] create relation related error: %v", err)
		}
	}
}

// CreateArgs are the create tool arguments.
type CreateArgs struct {
	Title       string  `json:"title" jsonschema:"Task title (required)."`
	Description string  `json:"description,omitempty" jsonschema:"Task description in markdown."`
	ParentID    *int64  `json:"parent_id,omitempty" jsonschema:"Optional parent task ID."`
	Blocks      []int64 `json:"blocks,omitempty" jsonschema:"Optional task IDs that this new task blocks."`
	BlockedBy   []int64 `json:"blocked_by,omitempty" jsonschema:"Optional task IDs that block this new task."`
	Related     []int64 `json:"related,omitempty" jsonschema:"Optional related task IDs."`
}

func (s *Server) toolCreate(ctx context.Context, args CreateArgs) (string, bool) {
	if strings.TrimSpace(args.Title) == "" {
		return "parameter 'title' cannot be empty", true
	}

	src, err := s.newNodeSource(ctx)
	if err != nil {
		return err.Error(), true
	}

	if err := validateCreateRelations(ctx, src, args.ParentID, args.Blocks, args.BlockedBy, args.Related); err != nil {
		return err.Error(), true
	}

	task, err := s.vikunja.CreateTask(ctx, src.projectID, args.Title, args.Description)
	if err != nil {
		return fmt.Sprintf("failed to create task: %v", err), true
	}

	s.wireCreateRelations(ctx, task.ID, args.ParentID, args.Blocks, args.BlockedBy, args.Related)

	var emb []float32
	if s.ollamaClient != nil {
		text := args.Title + "\n" + args.Description
		if genEmb, err := s.ollamaClient.GenerateEmbedding(ctx, text, false, 3); err == nil {
			emb = genEmb
		}
	}
	if err := s.store.UpdateTaskEmbeddingAndResolution(ctx, task.ID, emb, ""); err != nil {
		log.Printf("[docket-mcp] update embedding error: %v", err)
	}

	summary := TaskSummary{
		ID:        task.ID,
		Title:     task.Title,
		Status:    "open",
		ParentID:  args.ParentID,
		BlockedBy: args.BlockedBy,
		Blocks:    args.Blocks,
	}
	data, err := json.Marshal(summary)
	if err != nil {
		return fmt.Sprintf("failed to marshal summary: %v", err), true
	}
	return string(data), false
}

// UpdateArgs are the update tool arguments; nil fields are left unchanged.
type UpdateArgs struct {
	ID          int64   `json:"id" jsonschema:"Task ID to update."`
	Title       *string `json:"title,omitempty" jsonschema:"New task title."`
	Description *string `json:"description,omitempty" jsonschema:"New task description."`
}

func (s *Server) toolUpdate(ctx context.Context, args UpdateArgs) (string, bool) {
	if args.ID <= 0 {
		return "parameter 'id' must be a positive integer", true
	}

	task, err := s.vikunja.GetTask(ctx, args.ID)
	if err != nil {
		return fmt.Sprintf("failed to fetch task %d: %v", args.ID, err), true
	}

	if args.Title != nil && strings.TrimSpace(*args.Title) != "" {
		task.Title = strings.TrimSpace(*args.Title)
	}
	if args.Description != nil {
		task.Description = *args.Description
	}

	updated, err := s.vikunja.UpdateTask(ctx, task)
	if err != nil {
		return fmt.Sprintf("failed to update task: %v", err), true
	}

	var emb []float32
	if s.ollamaClient != nil {
		text := updated.Title + "\n" + updated.Description
		if genEmb, err := s.ollamaClient.GenerateEmbedding(ctx, text, false, 3); err == nil {
			emb = genEmb
		}
	}

	if len(emb) > 0 {
		if err := s.store.UpdateTaskEmbedding(ctx, args.ID, emb); err != nil {
			log.Printf("[docket-mcp] update embedding error: %v", err)
		}
	}

	var res string
	resMap, err := s.store.FetchTaskResolutions(ctx, []int64{args.ID})
	if err != nil {
		log.Printf("[docket-mcp] fetch resolution for update error: %v", err)
	} else {
		res = resMap[args.ID]
	}

	summary := TaskSummary{
		ID:     updated.ID,
		Title:  updated.Title,
		Status: ResolveTaskStatus(updated.Done, res),
	}
	data, err := json.Marshal(summary)
	if err != nil {
		return fmt.Sprintf("failed to marshal summary: %v", err), true
	}
	return string(data), false
}

// CommentArgs are the comment tool arguments.
type CommentArgs struct {
	ID   int64  `json:"id" jsonschema:"Task ID to comment on."`
	Text string `json:"text" jsonschema:"Comment text."`
}

func (s *Server) toolComment(ctx context.Context, args CommentArgs) (string, bool) {
	if args.ID <= 0 || strings.TrimSpace(args.Text) == "" {
		return "parameters 'id' and non-empty 'text' are required", true
	}

	if err := s.vikunja.CreateComment(ctx, args.ID, args.Text); err != nil {
		return fmt.Sprintf("failed to add comment to task %d: %v", args.ID, err), true
	}
	return fmt.Sprintf("comment added to task %d", args.ID), false
}

// LinkArgs are the link tool arguments.
type LinkArgs struct {
	ID       int64  `json:"id" jsonschema:"Source task ID."`
	TargetID int64  `json:"target_id" jsonschema:"Target task ID."`
	Type     string `json:"type" jsonschema:"Relation type: 'parent', 'blocks', or 'related'."`
	Remove   bool   `json:"remove,omitempty" jsonschema:"Whether to remove the relation (default false)."`
}

func (s *Server) toolLink(ctx context.Context, args LinkArgs) (string, bool) {
	if args.ID <= 0 || args.TargetID <= 0 {
		return "parameters 'id' and 'target_id' must be positive integers", true
	}
	if args.ID == args.TargetID {
		return fmt.Sprintf("self-link cycle: task %d cannot link to itself", args.ID), true
	}

	var kind string
	switch args.Type {
	case "parent":
		kind = "parenttask"
	case "blocks":
		kind = "blocking"
	case "related":
		kind = "related"
	default:
		return fmt.Sprintf("invalid relation type: %q (must be parent, blocks, or related)", args.Type), true
	}

	if args.Remove {
		if err := s.vikunja.DeleteRelation(ctx, args.ID, args.TargetID, kind); err != nil {
			return fmt.Sprintf("failed to remove relation: %v", err), true
		}
		return fmt.Sprintf("relation %s between task %d and %d removed", args.Type, args.ID, args.TargetID), false
	}

	if args.Type != "related" {
		src, err := s.newNodeSource(ctx)
		if err != nil {
			return err.Error(), true
		}
		if err := DetectGraphCycle(ctx, src, args.ID, args.TargetID, args.Type); err != nil {
			return err.Error(), true
		}
	}

	if err := s.vikunja.CreateRelation(ctx, args.ID, args.TargetID, kind); err != nil {
		return fmt.Sprintf("failed to create relation: %v", err), true
	}
	return fmt.Sprintf("relation %s between task %d and %d created", args.Type, args.ID, args.TargetID), false
}

func (s *Server) closeCompleted(ctx context.Context, id int64, reason string, now time.Time) error {
	task, err := s.vikunja.GetTask(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	task.Done = true
	task.DoneAt = &now
	if _, err := s.vikunja.UpdateTask(ctx, task); err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}
	if err := s.vikunja.CreateComment(ctx, id, "Closed as completed: "+reason); err != nil {
		log.Printf("[docket-mcp] comment error: %v", err)
	}
	if err := s.store.UpdateTaskResolutionBatch(ctx, []int64{id}, "completed"); err != nil {
		log.Printf("[docket-mcp] update resolution error: %v", err)
	}
	return nil
}

func (s *Server) closeWontDo(ctx context.Context, id int64, reason string, openSubIDs []int64, now time.Time) error {
	task, err := s.vikunja.GetTask(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	task.Done = true
	task.DoneAt = &now
	if _, err := s.vikunja.UpdateTask(ctx, task); err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}
	if err := s.vikunja.CreateComment(ctx, id, "Closed as wont_do: "+reason); err != nil {
		log.Printf("[docket-mcp] comment error: %v", err)
	}

	for _, sid := range openSubIDs {
		subTask, err := s.vikunja.GetTask(ctx, sid)
		if err == nil {
			subTask.Done = true
			subTask.DoneAt = &now
			if _, err := s.vikunja.UpdateTask(ctx, subTask); err != nil {
				log.Printf("[docket-mcp] update subtask error: %v", err)
			}
			if err := s.vikunja.CreateComment(ctx, sid, fmt.Sprintf("Cascaded close (wont_do) from parent task %d: %s", id, reason)); err != nil {
				log.Printf("[docket-mcp] comment subtask error: %v", err)
			}
		}
	}
	closedIDs := append([]int64{id}, openSubIDs...)
	if err := s.store.UpdateTaskResolutionBatch(ctx, closedIDs, "wont_do"); err != nil {
		log.Printf("[docket-mcp] update resolution batch error: %v", err)
	}
	return nil
}

// CloseArgs are the close tool arguments.
type CloseArgs struct {
	ID         int64  `json:"id" jsonschema:"Task ID to close."`
	Resolution string `json:"resolution" jsonschema:"Resolution: 'completed' or 'wont_do'."`
	Reason     string `json:"reason" jsonschema:"Required explanation for closing the task."`
}

func (s *Server) toolClose(ctx context.Context, args CloseArgs) (string, bool) {
	if args.ID <= 0 {
		return "parameter 'id' must be a positive integer", true
	}
	if strings.TrimSpace(args.Reason) == "" {
		return "parameter 'reason' cannot be empty", true
	}
	if args.Resolution != "completed" && args.Resolution != "wont_do" {
		return fmt.Sprintf("invalid resolution: %q (must be 'completed' or 'wont_do')", args.Resolution), true
	}

	src, err := s.newNodeSource(ctx)
	if err != nil {
		return err.Error(), true
	}
	nodesBefore, err := loadCloseNeighborhood(ctx, src, args.ID)
	if err != nil {
		return err.Error(), true
	}
	if err := ValidateCloseGuard(nodesBefore, args.ID, args.Resolution); err != nil {
		return err.Error(), true
	}

	now := time.Now().UTC()
	var closedIDs []int64
	if args.Resolution == "completed" {
		closedIDs = []int64{args.ID}
		if err := s.closeCompleted(ctx, args.ID, args.Reason, now); err != nil {
			return err.Error(), true
		}
	} else {
		openSubIDs := CollectOpenSubtreeIDs(nodesBefore, args.ID)
		closedIDs = append([]int64{args.ID}, openSubIDs...)
		if err := s.closeWontDo(ctx, args.ID, args.Reason, openSubIDs, now); err != nil {
			return err.Error(), true
		}
	}

	nodesAfter := maps.Clone(nodesBefore)
	for _, cid := range closedIDs {
		if n, ok := nodesAfter[cid]; ok {
			n.Status = args.Resolution
			n.Done = true
			nodesAfter[cid] = n
		}
	}

	unblocked := ComputeUnblockedIDs(nodesBefore, nodesAfter, closedIDs)
	title := nodesBefore[args.ID].Title

	res := ClosedTaskResult{
		ID:           args.ID,
		Title:        title,
		Status:       args.Resolution,
		UnblockedIDs: unblocked,
	}
	data, err := json.Marshal(res)
	if err != nil {
		return fmt.Sprintf("failed to marshal close result: %v", err), true
	}
	return string(data), false
}

// HandleTasks serves GET /api/tasks?status=... as a JSON task list.
func (s *Server) HandleTasks(w http.ResponseWriter, r *http.Request) {
	if !requireGET(w, r) {
		return
	}
	q, err := ParseTaskListQuery(r.URL.Query())
	if err != nil {
		writeRESTError(w, http.StatusBadRequest, err)
		return
	}
	tasks, err := s.listTasks(r.Context(), q.Status, nil)
	if err != nil {
		writeRESTError(w, restErrorStatus(err), err)
		return
	}
	if tasks == nil {
		tasks = []TaskSummary{}
	}
	writeRESTJSON(w, tasks)
}

// HandleTaskDetail serves GET /api/tasks/{id} as a hydrated task.
func (s *Server) HandleTaskDetail(w http.ResponseWriter, r *http.Request) {
	if !requireGET(w, r) {
		return
	}
	id, err := ParseTaskID(strings.TrimPrefix(r.URL.Path, "/api/tasks/"))
	if err != nil {
		writeRESTError(w, http.StatusBadRequest, err)
		return
	}
	task, err := s.showTask(r.Context(), id)
	if err != nil {
		writeRESTError(w, restErrorStatus(err), err)
		return
	}
	writeRESTJSON(w, task)
}

// HandleSearch serves GET /api/search?q=...&status=...&limit=... as scored hits.
func (s *Server) HandleSearch(w http.ResponseWriter, r *http.Request) {
	if !requireGET(w, r) {
		return
	}
	q, err := ParseTaskSearchQuery(r.URL.Query())
	if err != nil {
		writeRESTError(w, http.StatusBadRequest, err)
		return
	}
	results, err := s.searchTasks(r.Context(), q.Query, q.Status, q.Limit)
	if err != nil {
		writeRESTError(w, restErrorStatus(err), err)
		return
	}
	if results == nil {
		results = []ScoredTaskSummary{}
	}
	writeRESTJSON(w, results)
}

func requireGET(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	w.Header().Set("Allow", http.MethodGet)
	writeRESTError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	return false
}

// restErrorStatus maps missing tasks to 404 and every other upstream failure to 502.
func restErrorStatus(err error) int {
	if errors.Is(err, ErrTaskNotFound) || isVikunjaNotFound(err) {
		return http.StatusNotFound
	}
	return http.StatusBadGateway
}

func writeRESTJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Cache-Control", "no-cache")
	httpkit.WriteJSON(w, http.StatusOK, data)
}

func writeRESTError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Cache-Control", "no-cache")
	httpkit.WriteJSON(w, status, map[string]string{"error": err.Error()})
}

// NewHTTPMux registers HTTP endpoints for docket-mcp.
func NewHTTPMux(srv *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.HandleHealth)
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("docket-mcp"))
	mux.Handle("/mcp", tracing.Middleware("docket-mcp", http.HandlerFunc(srv.HandleMCPStreamable)))
	mux.HandleFunc("/api/tasks", srv.HandleTasks)
	mux.HandleFunc("/api/tasks/", srv.HandleTaskDetail)
	mux.HandleFunc("/api/search", srv.HandleSearch)
	mux.Handle("/", tracing.Middleware("docket-mcp", http.HandlerFunc(srv.HandleMCPStreamable)))
	return mux
}

func runServer(ctx context.Context, cfg *Config) error {
	db, err := sqlOpenFn("pgx", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer httpkit.CloseWarn(db, "database")

	store := NewSQLTaskStore(db)
	vikunja := NewVikunjaClient(cfg, nil)
	ollamaClient := ollama.NewDynamicClient(func() ollama.Config { return cfg.Ollama })

	srv := NewServer(cfg, vikunja, store, ollamaClient)
	mux := NewHTTPMux(srv)

	addr := net.JoinHostPort("0.0.0.0", cfg.Port)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[docket-mcp] Listening on %s (Vikunja: %s)", addr, cfg.VikunjaURL)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

var (
	runServerFn = runServer
	exitFn      = log.Fatalf
	sqlOpenFn   = sql.Open
)

func main() {
	cfg, err := LoadConfig("")
	if err != nil {
		exitFn("[docket-mcp] Failed to load config: %v", err)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "docket-mcp", tracing.DefaultConfigPath)

	err = runServerFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[docket-mcp] Server exited with error: %v", err)
	}
}
