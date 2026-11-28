package server

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	_ "github.com/jackc/pgx/v5/stdlib" // register pgx database/sql driver for RunServer
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/mcp/docket-mcp/internal/config"
	"github.com/azylman/aerial/mcp/docket-mcp/internal/store"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/mcpkit"
	"github.com/azylman/aerial/pkg/ollama"
	"github.com/azylman/aerial/pkg/tracing"
)

const (
	errMsgInvalidID   = "parameter 'id' must be a positive integer"
	relKindParenttask = "parenttask"
	relKindBlocking   = "blocking"

	HeaderAerialAgent  = "X-Aerial-Agent"
	HeaderAerialTurnID = "X-Aerial-Turn-Id"
)

type contextKey string

const (
	agentContextKey  contextKey = "aerial_agent"
	turnIDContextKey contextKey = "aerial_turn_id"
)

// WithAgentAndTurnID attaches agent and turn_id identifiers to ctx.
func WithAgentAndTurnID(ctx context.Context, agent, turnID string) context.Context {
	if agent != "" {
		ctx = context.WithValue(ctx, agentContextKey, agent)
	}
	if turnID != "" {
		ctx = context.WithValue(ctx, turnIDContextKey, turnID)
	}
	return ctx
}

// AgentFromContext extracts the agent identifier from ctx.
func AgentFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(agentContextKey).(string); ok {
		return v
	}
	return ""
}

// TurnIDFromContext extracts the turn ID from ctx.
func TurnIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(turnIDContextKey).(string); ok {
		return v
	}
	return ""
}

// Server encapsulates HTTP routes, Vikunja client, and SQL task store for docket-mcp.
type Server struct {
	cfg           *config.Config
	vikunja       *store.VikunjaClient
	store         store.TaskStore
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
func NewServer(cfg *config.Config, vikunja *store.VikunjaClient, taskStore store.TaskStore, ollamaClient *ollama.Client) *Server {
	s := &Server{
		cfg:          cfg,
		vikunja:      vikunja,
		store:        taskStore,
		ollamaClient: ollamaClient,
	}
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "docket-mcp", Version: "1.0.0"}, &mcp.ServerOptions{Instructions: serverInstructions})
	s.registerTools(mcpServer)
	s.streamHandler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return s
}

// HandleMCPStreamable serves MCP over streamable HTTP, accepting legacy docket_-prefixed tool names.
func (s *Server) HandleMCPStreamable(w http.ResponseWriter, r *http.Request) {
	agent := r.Header.Get(HeaderAerialAgent)
	turnID := r.Header.Get(HeaderAerialTurnID)
	ctx := WithAgentAndTurnID(r.Context(), agent, turnID)
	r = r.WithContext(ctx)
	httpkit.ServeMCPStream(w, r, s.streamHandler, "docket-mcp ready\n", "docket_", "")
}

var taskStatusFilters = []any{config.TaskStatusOpen, config.TaskStatusCompleted, config.TaskStatusWontDo, config.TaskStatusAll}

func (s *Server) registerTools(srv *mcp.Server) {
	listSchema := mcpkit.InputSchema[ListArgs]()
	setEnum(listSchema, "status", `"open"`, taskStatusFilters...)
	searchSchema := mcpkit.InputSchema[SearchArgs]()
	setEnum(searchSchema, "status", `"open"`, taskStatusFilters...)
	searchSchema.Properties["limit"].Default = json.RawMessage(`10`)
	linkSchema := mcpkit.InputSchema[LinkArgs]()
	setEnum(linkSchema, "type", "", "parent", "blocks", "related")
	linkSchema.Properties["remove"].Default = json.RawMessage(`false`)
	closeSchema := mcpkit.InputSchema[CloseArgs]()
	setEnum(closeSchema, "resolution", "", config.TaskStatusCompleted, config.TaskStatusWontDo)

	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "ready",
		Description: "Returns unblocked open tasks ready for execution, top-down with subtasks nested in ready_subtasks. If parent_id is omitted, returns top-level tasks. If parent_id is provided, returns ready subtasks under that parent.",
		InputSchema: mcpkit.InputSchema[ReadyArgs](),
	}, s.toolReady)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "list",
		Description: "List tasks filtered by status ('open', 'completed', 'wont_do', 'all', default 'open') and optional parent_id.",
		InputSchema: listSchema,
	}, s.toolList)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "search",
		Description: "Search tasks using hybrid semantic and keyword search (Weighted RRF) with status pre-filtering.",
		InputSchema: searchSchema,
	}, s.toolSearch)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "show",
		Description: "Show full details of a task including parent, subtasks, blockers, blocked tasks, related tasks, and comments.",
		InputSchema: mcpkit.InputSchema[ShowArgs](),
	}, s.toolShow)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "create",
		Description: "Create a new task with optional description, parent, blocker relations, and related links; any unknown referenced task ID rejects the create.",
		InputSchema: mcpkit.InputSchema[CreateArgs](),
	}, s.toolCreate)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "update",
		Description: "Update task title and/or description.",
		InputSchema: mcpkit.InputSchema[UpdateArgs](),
	}, s.toolUpdate)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "comment",
		Description: "Add a comment to a task.",
		InputSchema: mcpkit.InputSchema[CommentArgs](),
	}, s.toolComment)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "link",
		Description: "Create or remove a relation between tasks ('parent', 'blocks', 'related'); new parent and blocks edges that would form a cycle are rejected.",
		InputSchema: linkSchema,
	}, s.toolLink)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "close",
		Description: "Close a task with resolution 'completed' or 'wont_do' and a required reason. Validates no open blockers/subtasks for completed, cascades for wont_do.",
		InputSchema: closeSchema,
	}, s.toolClose)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "claim",
		Description: "Claim an open top-level task atomically for an agent and turn ID. Rejects if subtask, already claimed, or closed.",
		InputSchema: mcpkit.InputSchema[ClaimArgs](),
	}, s.toolClaim)
	mcpkit.AddTextTool(srv, &mcp.Tool{
		Name:        "unclaim",
		Description: "Release a claimed task atomically, making it available to other agents.",
		InputSchema: mcpkit.InputSchema[UnclaimArgs](),
	}, s.toolUnclaim)
}

func setEnum(schema *jsonschema.Schema, prop, defaultJSON string, values ...any) {
	schema.Properties[prop].Enum = values
	if defaultJSON != "" {
		schema.Properties[prop].Default = json.RawMessage(defaultJSON)
	}
}

func (s *Server) loadGraph(ctx context.Context) (map[int64]config.TaskNode, error) {
	nodes, _, err := s.loadGraphMaps(ctx)
	return nodes, err
}

func (s *Server) loadGraphMaps(ctx context.Context) (map[int64]config.TaskNode, map[int64]config.VikunjaTask, error) {
	src, err := s.newNodeSource(ctx)
	if err != nil {
		return nil, nil, err
	}
	return s.loadGraphMapsFromSource(ctx, src)
}

func (s *Server) loadGraphMapsFromSource(ctx context.Context, src *vikunjaNodeSource) (map[int64]config.TaskNode, map[int64]config.VikunjaTask, error) {
	taskIDs := make([]int64, 0, len(src.tasksByDisplay))
	for _, t := range src.tasksByDisplay {
		taskIDs = append(taskIDs, t.ID)
	}

	metaMap, err := s.store.FetchTaskMeta(ctx, taskIDs)
	if err != nil {
		metaMap = make(map[int64]config.TaskMeta)
	}

	nodes := make(map[int64]config.TaskNode, len(src.tasksByDisplay))
	byGlobal := make(map[int64]config.VikunjaTask, len(src.tasksByDisplay))
	for dID, t := range src.tasksByDisplay {
		meta := metaMap[t.ID]
		node := config.TaskToNode(t, meta.Resolution, meta)
		nodes[dID] = node
		src.cache[dID] = node
		byGlobal[t.ID] = t
	}
	return nodes, byGlobal, nil
}

type vikunjaNodeSource struct {
	vikunja         *store.VikunjaClient
	store           store.TaskStore
	projectID       int64
	cache           map[int64]config.TaskNode
	displayToGlobal map[int64]int64
	globalToDisplay map[int64]int64
	tasksByDisplay  map[int64]config.VikunjaTask
}

func (s *Server) newNodeSource(ctx context.Context) (*vikunjaNodeSource, error) {
	projectID, err := s.vikunja.GetDefaultProject(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get default project: %w", err)
	}
	tasks, err := s.vikunja.ListProjectTasks(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to list project tasks: %w", err)
	}
	displayToGlobal := make(map[int64]int64, len(tasks))
	globalToDisplay := make(map[int64]int64, len(tasks))
	tasksByDisplay := make(map[int64]config.VikunjaTask, len(tasks))
	for _, t := range tasks {
		dID := t.DisplayID()
		displayToGlobal[dID] = t.ID
		globalToDisplay[t.ID] = dID
		tasksByDisplay[dID] = t
	}
	return &vikunjaNodeSource{
		vikunja:         s.vikunja,
		store:           s.store,
		projectID:       projectID,
		cache:           make(map[int64]config.TaskNode),
		displayToGlobal: displayToGlobal,
		globalToDisplay: globalToDisplay,
		tasksByDisplay:  tasksByDisplay,
	}, nil
}

func (src *vikunjaNodeSource) ResolveGlobalID(ctx context.Context, displayID int64) (int64, error) {
	if global, ok := src.displayToGlobal[displayID]; ok {
		return global, nil
	}
	task, err := src.vikunja.GetTask(ctx, displayID)
	if err != nil {
		return 0, err
	}
	if task.ProjectID != src.projectID {
		return 0, fmt.Errorf("task %d %w", displayID, ErrTaskNotFound)
	}
	src.displayToGlobal[displayID] = task.ID
	if src.globalToDisplay != nil {
		src.globalToDisplay[task.ID] = task.DisplayID()
	}
	return task.ID, nil
}

func (src *vikunjaNodeSource) ResolveGlobalIDs(ctx context.Context, displayIDs []int64) ([]int64, error) {
	out := make([]int64, 0, len(displayIDs))
	for _, id := range displayIDs {
		global, err := src.ResolveGlobalID(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, global)
	}
	return out, nil
}

func (src *vikunjaNodeSource) GetTask(ctx context.Context, displayID int64) (*config.VikunjaTask, error) {
	if t, ok := src.tasksByDisplay[displayID]; ok {
		cpy := t
		return &cpy, nil
	}
	if dID, ok := src.globalToDisplay[displayID]; ok {
		if t, ok := src.tasksByDisplay[dID]; ok {
			cpy := t
			return &cpy, nil
		}
	}
	return src.vikunja.GetTask(ctx, displayID)
}

func (src *vikunjaNodeSource) Node(ctx context.Context, id int64) (config.TaskNode, bool, error) {
	if node, ok := src.cache[id]; ok {
		return node, true, nil
	}
	task, err := src.GetTask(ctx, id)
	if err != nil {
		if isVikunjaNotFound(err) {
			return config.TaskNode{}, false, nil
		}
		return config.TaskNode{}, false, fmt.Errorf("failed to fetch task %d: %w", id, err)
	}
	if task.ProjectID != src.projectID {
		return config.TaskNode{}, false, nil
	}
	var meta config.TaskMeta
	if metaMap, err := src.store.FetchTaskMeta(ctx, []int64{task.ID}); err == nil {
		meta = metaMap[task.ID]
	}
	node := config.TaskToNode(*task, meta.Resolution, meta)
	src.cache[id] = node
	return node, true, nil
}

// resolveGlobalID maps a user-facing display id to Vikunja's global id.
func (s *Server) resolveGlobalID(ctx context.Context, displayID int64) (int64, error) {
	src, err := s.newNodeSource(ctx)
	if err != nil {
		return 0, err
	}
	return src.ResolveGlobalID(ctx, displayID)
}

func (s *Server) getTaskByDisplayID(ctx context.Context, displayID int64) (*config.VikunjaTask, *vikunjaNodeSource, error) {
	src, err := s.newNodeSource(ctx)
	if err != nil {
		return nil, nil, err
	}
	task, err := src.GetTask(ctx, displayID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch task %d: %w", displayID, err)
	}
	if task.ProjectID != src.projectID {
		return nil, nil, fmt.Errorf("task %d %w", displayID, ErrTaskNotFound)
	}
	return task, src, nil
}

func isVikunjaNotFound(err error) bool {
	var statusErr *store.HTTPStatusError
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

	tree := config.BuildReadyTree(nodes, args.ParentID)
	return marshalToolResult(tree, "ready tasks")
}

// ErrTaskNotFound reports that a task id is absent from the project graph.
var ErrTaskNotFound = errors.New("not found in project graph")

// ListArgs are the list tool arguments; the schema defaults status to "open".
type ListArgs struct {
	Status   string `json:"status,omitempty"    jsonschema:"Status filter: 'open', 'completed', 'wont_do', 'all'"`
	ParentID *int64 `json:"parent_id,omitempty" jsonschema:"Optional parent task ID to filter by."`
	Claimed  *bool  `json:"claimed,omitempty"   jsonschema:"Optional filter for task claimed status (true for claimed, false for unclaimed)."`
}

func (s *Server) toolList(ctx context.Context, args ListArgs) (string, bool) {
	summaries, err := s.listTasks(ctx, args.Status, args.ParentID, args.Claimed)
	if err != nil {
		return err.Error(), true
	}

	return marshalToolResult(summaries, "task list")
}

func marshalToolResult(v any, what string) (string, bool) {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("failed to marshal %s: %v", what, err), true
	}
	return string(data), false
}

func (s *Server) listTasks(ctx context.Context, status string, parentID *int64, claimed *bool) ([]config.TaskSummary, error) {
	nodes, err := s.loadGraph(ctx)
	if err != nil {
		return nil, err
	}

	summaries := []config.TaskSummary{}
	for _, node := range nodes {
		if status != config.TaskStatusAll && node.Status != status {
			continue
		}
		if parentID != nil {
			if node.ParentID == nil || *node.ParentID != *parentID {
				continue
			}
		}
		if claimed != nil {
			isClaimed := node.ClaimedByAgent != "" || node.ClaimedByTurnID != ""
			if isClaimed != *claimed {
				continue
			}
		}
		summaries = append(summaries, config.SummarizeNode(node))
	}
	slices.SortFunc(summaries, func(a, b config.TaskSummary) int { return cmp.Compare(a.ID, b.ID) })
	return summaries, nil
}

// SearchArgs are the search tool arguments; the schema defaults status to "open" and limit to 10.
type SearchArgs struct {
	Query  string `json:"query"            jsonschema:"Search query text."`
	Status string `json:"status,omitempty" jsonschema:"Status filter: 'open', 'completed', 'wont_do', 'all' (default 'open')"`
	Limit  int    `json:"limit,omitempty"  jsonschema:"Maximum number of results to return (default 10)."`
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

func (s *Server) searchTasks(ctx context.Context, query, status string, limit int) ([]config.ScoredTaskSummary, error) {
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

	// Search hits are keyed by global id in the store; rewrite to display ids.
	nodes, byGlobal, graphErr := s.loadGraphMaps(ctx)
	if graphErr == nil {
		for i := range results {
			if t, ok := byGlobal[results[i].ID]; ok {
				results[i].ID = t.DisplayID()
			}
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
		return errMsgInvalidID, true
	}

	hydrated, err := s.showTask(ctx, args.ID)
	if err != nil {
		return err.Error(), true
	}

	return marshalToolResult(hydrated, "hydrated task")
}

func (s *Server) showTask(ctx context.Context, id int64) (config.HydratedTask, error) {
	task, src, err := s.getTaskByDisplayID(ctx, id)
	if err != nil {
		return config.HydratedTask{}, err
	}

	comments, err := s.vikunja.ListComments(ctx, task.ID)
	if err != nil {
		comments = nil
	}

	neighbors := make(map[int64]config.TaskNode)
	for _, related := range task.RelatedTasks {
		for _, t := range related {
			if t.ProjectID != 0 && t.ProjectID != task.ProjectID {
				continue
			}
			if _, seen := neighbors[t.ID]; seen {
				continue
			}
			dID := t.DisplayID()
			if mapped, ok := src.globalToDisplay[t.ID]; ok {
				dID = mapped
			}
			node, ok, err := src.Node(ctx, dID)
			if err != nil {
				return config.HydratedTask{}, err
			}
			if ok {
				neighbors[t.ID] = node
			}
		}
	}

	var meta config.TaskMeta
	if metaMap, err := s.store.FetchTaskMeta(ctx, []int64{task.ID}); err == nil {
		meta = metaMap[task.ID]
	}

	return config.HydrateTask(*task, config.ResolveTaskStatus(task.Done, meta.Resolution), comments, neighbors, meta), nil
}

func validateCreateRelations(ctx context.Context, src config.NodeSource, parentID *int64, blocks, blockedBy, related []int64) error {
	type ref struct {
		role string
		ids  []int64
	}
	refs := []ref{{"blocked", blocks}, {"blocker", blockedBy}, {config.RelationRelated, related}}
	if parentID != nil {
		refs = append([]ref{{config.RelationParent, []int64{*parentID}}}, refs...)
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
	return config.DetectCreateCycle(ctx, src, blocks, blockedBy)
}

func (s *Server) wireCreateRelations(ctx context.Context, src *vikunjaNodeSource, taskGlobalID int64, parentID *int64, blocks, blockedBy, related []int64) {
	type relEdge struct {
		from, to int64
		kind     string
		logKind  string
	}
	var edges []relEdge
	if parentID != nil {
		to, err := src.ResolveGlobalID(ctx, *parentID)
		if err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] resolve relation error", "kind", relKindParenttask, "parent_id", *parentID, "error", err)
		} else {
			edges = append(edges, relEdge{taskGlobalID, to, relKindParenttask, relKindParenttask})
		}
	}
	for _, b := range blocks {
		to, err := src.ResolveGlobalID(ctx, b)
		if err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] resolve relation error", "kind", relKindBlocking, "blocked_id", b, "error", err)
		} else {
			edges = append(edges, relEdge{taskGlobalID, to, relKindBlocking, relKindBlocking})
		}
	}
	for _, bb := range blockedBy {
		from, err := src.ResolveGlobalID(ctx, bb)
		if err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] resolve relation error", "kind", "blocked_by", "blocker_id", bb, "error", err)
		} else {
			edges = append(edges, relEdge{from, taskGlobalID, relKindBlocking, "blocked_by"})
		}
	}
	for _, r := range related {
		to, err := src.ResolveGlobalID(ctx, r)
		if err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] resolve relation error", "kind", config.RelationRelated, "related_id", r, "error", err)
		} else {
			edges = append(edges, relEdge{taskGlobalID, to, config.RelationRelated, config.RelationRelated})
		}
	}
	for _, e := range edges {
		if err := s.vikunja.CreateRelation(ctx, e.from, e.to, e.kind); err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] create relation error", "kind", e.logKind, "task_id", e.from, "other_id", e.to, "error", err)
		}
	}
}

// CreateArgs are the create tool arguments.
type CreateArgs struct {
	Title       string  `json:"title"                 jsonschema:"Task title (required)."`
	Description string  `json:"description,omitempty" jsonschema:"Task description in markdown."`
	ParentID    *int64  `json:"parent_id,omitempty"   jsonschema:"Optional parent task ID."`
	Blocks      []int64 `json:"blocks,omitempty"      jsonschema:"Optional task IDs that this new task blocks."`
	BlockedBy   []int64 `json:"blocked_by,omitempty"  jsonschema:"Optional task IDs that block this new task."`
	Related     []int64 `json:"related,omitempty"     jsonschema:"Optional related task IDs."`
}

func (s *Server) toolCreate(ctx context.Context, args CreateArgs) (string, bool) {
	if strings.TrimSpace(args.Title) == "" {
		return "parameter 'title' cannot be empty", true
	}

	src, err := s.newNodeSource(ctx)
	if err != nil {
		return err.Error(), true
	}

	if err = validateCreateRelations(ctx, src, args.ParentID, args.Blocks, args.BlockedBy, args.Related); err != nil {
		return err.Error(), true
	}

	task, err := s.vikunja.CreateTask(ctx, src.projectID, args.Title, args.Description)
	if err != nil {
		return fmt.Sprintf("failed to create task: %v", err), true
	}

	s.wireCreateRelations(ctx, src, task.ID, args.ParentID, args.Blocks, args.BlockedBy, args.Related)

	var emb []float32
	if s.ollamaClient != nil {
		text := args.Title + "\n" + args.Description
		if genEmb, generateEmbeddingErr := s.ollamaClient.GenerateEmbedding(ctx, text, false, 3); generateEmbeddingErr == nil {
			emb = genEmb
		}
	}
	if err = s.store.UpdateTaskEmbeddingAndResolution(ctx, task.ID, emb, ""); err != nil {
		slog.ErrorContext(ctx, "[docket-mcp] update embedding error", "task_id", task.ID, "error", err)
	}

	summary := config.TaskSummary{
		ID:        task.DisplayID(),
		Title:     task.Title,
		Status:    config.TaskStatusOpen,
		ParentID:  args.ParentID,
		BlockedBy: args.BlockedBy,
		Blocks:    args.Blocks,
	}
	return marshalToolResult(summary, "summary")
}

// UpdateArgs are the update tool arguments; nil fields are left unchanged.
type UpdateArgs struct {
	ID          int64   `json:"id"                    jsonschema:"Task ID to update."`
	Title       *string `json:"title,omitempty"       jsonschema:"New task title."`
	Description *string `json:"description,omitempty" jsonschema:"New task description."`
}

func (s *Server) toolUpdate(ctx context.Context, args UpdateArgs) (string, bool) {
	if args.ID <= 0 {
		return errMsgInvalidID, true
	}

	task, _, err := s.getTaskByDisplayID(ctx, args.ID)
	if err != nil {
		return err.Error(), true
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
		if genEmb, generateEmbeddingErr := s.ollamaClient.GenerateEmbedding(ctx, text, false, 3); generateEmbeddingErr == nil {
			emb = genEmb
		}
	}

	if len(emb) > 0 {
		if err = s.store.UpdateTaskEmbedding(ctx, task.ID, emb); err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] update embedding error", "task_id", task.ID, "error", err)
		}
	}

	var meta config.TaskMeta
	metaMap, err := s.store.FetchTaskMeta(ctx, []int64{task.ID})
	if err != nil {
		slog.ErrorContext(ctx, "[docket-mcp] fetch meta for update error", "task_id", task.ID, "error", err)
	} else {
		meta = metaMap[task.ID]
	}

	summary := config.TaskSummary{
		ID:              updated.DisplayID(),
		Title:           updated.Title,
		Status:          config.ResolveTaskStatus(updated.Done, meta.Resolution),
		ClaimedByAgent:  meta.ClaimedByAgent,
		ClaimedByTurnID: meta.ClaimedByTurnID,
		ClaimedAt:       meta.ClaimedAt,
	}
	return marshalToolResult(summary, "summary")
}

// CommentArgs are the comment tool arguments.
type CommentArgs struct {
	ID   int64  `json:"id"   jsonschema:"Task ID to comment on."`
	Text string `json:"text" jsonschema:"Comment text."`
}

func (s *Server) toolComment(ctx context.Context, args CommentArgs) (string, bool) {
	if args.ID <= 0 || strings.TrimSpace(args.Text) == "" {
		return "parameters 'id' and non-empty 'text' are required", true
	}

	globalID, err := s.resolveGlobalID(ctx, args.ID)
	if err != nil {
		return fmt.Sprintf("failed to add comment to task %d: %v", args.ID, err), true
	}
	if err := s.vikunja.CreateComment(ctx, globalID, args.Text); err != nil {
		return fmt.Sprintf("failed to add comment to task %d: %v", args.ID, err), true
	}
	return fmt.Sprintf("comment added to task %d", args.ID), false
}

// LinkArgs are the link tool arguments.
type LinkArgs struct {
	ID       int64  `json:"id"               jsonschema:"Source task ID."`
	TargetID int64  `json:"target_id"        jsonschema:"Target task ID."`
	Type     string `json:"type"             jsonschema:"Relation type: 'parent', 'blocks', or 'related'."`
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
	case config.RelationParent:
		kind = relKindParenttask
	case "blocks":
		kind = relKindBlocking
	case config.RelationRelated:
		kind = config.RelationRelated
	default:
		return fmt.Sprintf("invalid relation type: %q (must be parent, blocks, or related)", args.Type), true
	}

	src, err := s.newNodeSource(ctx)
	if err != nil {
		return err.Error(), true
	}

	var globalID, globalTarget int64
	if args.Remove {
		globalID, err = src.ResolveGlobalID(ctx, args.ID)
		if err != nil {
			return fmt.Sprintf("failed to remove relation: %v", err), true
		}
		globalTarget, err = src.ResolveGlobalID(ctx, args.TargetID)
		if err != nil {
			return fmt.Sprintf("failed to remove relation: %v", err), true
		}
		if err = s.vikunja.DeleteRelation(ctx, globalID, globalTarget, kind); err != nil {
			return fmt.Sprintf("failed to remove relation: %v", err), true
		}
		return fmt.Sprintf("relation %s between task %d and %d removed", args.Type, args.ID, args.TargetID), false
	}

	if args.Type != config.RelationRelated {
		if cycleErr := config.DetectGraphCycle(ctx, src, args.ID, args.TargetID, args.Type); cycleErr != nil {
			return cycleErr.Error(), true
		}
	}

	globalID, err = src.ResolveGlobalID(ctx, args.ID)
	if err != nil {
		return fmt.Sprintf("failed to create relation: %v", err), true
	}
	globalTarget, err = src.ResolveGlobalID(ctx, args.TargetID)
	if err != nil {
		return fmt.Sprintf("failed to create relation: %v", err), true
	}
	if err = s.vikunja.CreateRelation(ctx, globalID, globalTarget, kind); err != nil {
		return fmt.Sprintf("failed to create relation: %v", err), true
	}
	return fmt.Sprintf("relation %s between task %d and %d created", args.Type, args.ID, args.TargetID), false
}

func (s *Server) markTaskDone(ctx context.Context, src *vikunjaNodeSource, id int64, comment string, now time.Time) error {
	task, err := src.GetTask(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	task.Done = true
	task.DoneAt = &now
	if _, err := s.vikunja.UpdateTask(ctx, task); err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}
	if comment != "" {
		if err := s.vikunja.CreateComment(ctx, task.ID, comment); err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] comment error", "task_id", id, "error", err)
		}
	}
	return nil
}

func (s *Server) closeCompleted(ctx context.Context, src *vikunjaNodeSource, id int64, reason string, now time.Time) error {
	var comment string
	if reason != "" {
		comment = "Closed as completed: " + reason
	}
	if err := s.markTaskDone(ctx, src, id, comment, now); err != nil {
		return err
	}
	globalID, err := src.ResolveGlobalID(ctx, id)
	if err != nil {
		slog.ErrorContext(ctx, "[docket-mcp] update resolution error", "task_id", id, "error", err)
	} else if err := s.store.UpdateTaskResolutionBatch(ctx, []int64{globalID}, config.TaskStatusCompleted); err != nil {
		slog.ErrorContext(ctx, "[docket-mcp] update resolution error", "task_id", id, "error", err)
	}
	return nil
}

func (s *Server) closeWontDo(ctx context.Context, src *vikunjaNodeSource, id int64, reason string, openSubIDs []int64, now time.Time) error {
	if err := s.markTaskDone(ctx, src, id, "Closed as wont_do: "+reason, now); err != nil {
		return err
	}

	for _, sid := range openSubIDs {
		comment := fmt.Sprintf("Cascaded close (wont_do) from parent task %d: %s", id, reason)
		if err := s.markTaskDone(ctx, src, sid, comment, now); err != nil {
			slog.ErrorContext(ctx, "[docket-mcp] update subtask error", "task_id", sid, "error", err)
		}
	}
	closedIDs := append([]int64{id}, openSubIDs...)
	globalIDs, err := src.ResolveGlobalIDs(ctx, closedIDs)
	if err != nil {
		slog.ErrorContext(ctx, "[docket-mcp] update resolution batch error", "task_ids", closedIDs, "error", err)
	} else if err := s.store.UpdateTaskResolutionBatch(ctx, globalIDs, config.TaskStatusWontDo); err != nil {
		slog.ErrorContext(ctx, "[docket-mcp] update resolution batch error", "task_ids", closedIDs, "error", err)
	}
	return nil
}

// CloseArgs are the close tool arguments.
type CloseArgs struct {
	ID         int64  `json:"id"         jsonschema:"Task ID to close."`
	Resolution string `json:"resolution" jsonschema:"Resolution: 'completed' or 'wont_do'."`
	Reason     string `json:"reason"     jsonschema:"Required explanation for closing the task."`
}

func (s *Server) toolClose(ctx context.Context, args CloseArgs) (string, bool) {
	if args.ID <= 0 {
		return errMsgInvalidID, true
	}
	if strings.TrimSpace(args.Reason) == "" {
		return "parameter 'reason' cannot be empty", true
	}
	if args.Resolution != config.TaskStatusCompleted && args.Resolution != config.TaskStatusWontDo {
		return fmt.Sprintf("invalid resolution: %q (must be 'completed' or 'wont_do')", args.Resolution), true
	}

	src, err := s.newNodeSource(ctx)
	if err != nil {
		return err.Error(), true
	}
	nodesBefore, err := config.LoadCloseNeighborhood(ctx, src, args.ID)
	if err != nil {
		return err.Error(), true
	}
	if err = config.ValidateCloseGuard(nodesBefore, args.ID, args.Resolution); err != nil {
		return err.Error(), true
	}

	now := time.Now().UTC()
	var closedIDs []int64
	if args.Resolution == config.TaskStatusCompleted {
		closedIDs = []int64{args.ID}
		if err = s.closeCompleted(ctx, src, args.ID, args.Reason, now); err != nil {
			return err.Error(), true
		}
	} else {
		openSubIDs := config.CollectOpenSubtreeIDs(nodesBefore, args.ID)
		closedIDs = append([]int64{args.ID}, openSubIDs...)
		if err = s.closeWontDo(ctx, src, args.ID, args.Reason, openSubIDs, now); err != nil {
			return err.Error(), true
		}
	}

	nodesAfter := maps.Clone(nodesBefore)
	for _, cid := range closedIDs {
		n, ok := nodesAfter[cid]
		if !ok {
			continue
		}
		n.Status = args.Resolution
		n.Done = true
		n.ClaimedByAgent = ""
		n.ClaimedByTurnID = ""
		n.ClaimedAt = nil
		nodesAfter[cid] = n
	}

	unblocked := config.ComputeUnblockedIDs(nodesBefore, nodesAfter, closedIDs)
	title := nodesBefore[args.ID].Title

	res := config.ClosedTaskResult{
		ID:           args.ID,
		Title:        title,
		Status:       args.Resolution,
		UnblockedIDs: unblocked,
	}
	return marshalToolResult(res, "close result")
}

// ClaimArgs are the claim tool arguments.
type ClaimArgs struct {
	ID     int64  `json:"id"                jsonschema:"Task ID to claim."`
	Agent  string `json:"agent,omitempty"   jsonschema:"Agent identifier (optional if passed via X-Aerial-Agent header)."`
	TurnID string `json:"turn_id,omitempty" jsonschema:"Turn ID (optional if passed via X-Aerial-Turn-Id header)."`
}

func findTopParentID(ctx context.Context, src config.NodeSource, nodeID int64, parentID *int64) int64 {
	if parentID == nil {
		return nodeID
	}
	topParentID := *parentID
	visited := map[int64]bool{nodeID: true, topParentID: true}
	curr := parentID
	for curr != nil {
		pNode, pOk, pErr := src.Node(ctx, *curr)
		if pErr != nil || !pOk || pNode.ParentID == nil || visited[*pNode.ParentID] {
			topParentID = *curr
			break
		}
		visited[*pNode.ParentID] = true
		topParentID = *pNode.ParentID
		curr = pNode.ParentID
	}
	return topParentID
}

func (s *Server) toolClaim(ctx context.Context, args ClaimArgs) (string, bool) {
	if args.ID <= 0 {
		return errMsgInvalidID, true
	}

	agent := AgentFromContext(ctx)
	if agent == "" {
		agent = strings.TrimSpace(args.Agent)
	}
	turnID := TurnIDFromContext(ctx)
	if turnID == "" {
		turnID = strings.TrimSpace(args.TurnID)
	}
	if agent == "" && turnID == "" {
		return "either agent or turn_id must be provided via headers or arguments", true
	}

	src, err := s.newNodeSource(ctx)
	if err != nil {
		return err.Error(), true
	}

	node, ok, err := src.Node(ctx, args.ID)
	if err != nil {
		return err.Error(), true
	}
	if !ok {
		return fmt.Sprintf("task %d not found in project graph", args.ID), true
	}
	if node.Status != config.TaskStatusOpen {
		return fmt.Sprintf("cannot claim task %d: task is %s (must be open)", args.ID, node.Status), true
	}

	if node.ParentID != nil {
		topParentID := findTopParentID(ctx, src, node.ID, node.ParentID)
		return fmt.Sprintf("cannot claim task %d: task is a subtask (top-level parent is task %d; only top-level tasks can be claimed)", args.ID, topParentID), true
	}

	if node.ClaimedByAgent != "" || node.ClaimedByTurnID != "" {
		return fmt.Sprintf("cannot claim task %d: already claimed by agent %q (turn %q)", args.ID, node.ClaimedByAgent, node.ClaimedByTurnID), true
	}

	globalID, err := src.ResolveGlobalID(ctx, args.ID)
	if err != nil {
		return fmt.Sprintf("failed to resolve task %d: %v", args.ID, err), true
	}

	now := time.Now().UTC()
	claimed, err := s.store.ClaimTask(ctx, globalID, agent, turnID, now)
	if err != nil {
		return fmt.Sprintf("failed to claim task %d: %v", args.ID, err), true
	}
	if !claimed {
		return fmt.Sprintf("cannot claim task %d: already claimed or closed", args.ID), true
	}

	node.ClaimedByAgent = agent
	node.ClaimedByTurnID = turnID
	node.ClaimedAt = &now
	summary := config.SummarizeNode(node)
	return marshalToolResult(summary, "claim result")
}

// UnclaimArgs are the unclaim tool arguments.
type UnclaimArgs struct {
	ID int64 `json:"id" jsonschema:"Task ID to unclaim."`
}

func (s *Server) toolUnclaim(ctx context.Context, args UnclaimArgs) (string, bool) {
	if args.ID <= 0 {
		return errMsgInvalidID, true
	}

	src, err := s.newNodeSource(ctx)
	if err != nil {
		return err.Error(), true
	}

	node, ok, err := src.Node(ctx, args.ID)
	if err != nil {
		return err.Error(), true
	}
	if !ok {
		return fmt.Sprintf("task %d not found in project graph", args.ID), true
	}

	globalID, err := src.ResolveGlobalID(ctx, args.ID)
	if err != nil {
		return fmt.Sprintf("failed to resolve task %d: %v", args.ID, err), true
	}

	if err := s.store.UnclaimTask(ctx, globalID); err != nil {
		return fmt.Sprintf("failed to unclaim task %d: %v", args.ID, err), true
	}

	node.ClaimedByAgent = ""
	node.ClaimedByTurnID = ""
	node.ClaimedAt = nil
	summary := config.SummarizeNode(node)
	return marshalToolResult(summary, "unclaim result")
}

// HandleTasks serves GET /api/tasks?status=... as a JSON task list.
func (s *Server) HandleTasks(w http.ResponseWriter, r *http.Request) {
	if !requireGET(w, r) {
		return
	}
	q, err := config.ParseTaskListQuery(r.URL.Query())
	if err != nil {
		writeRESTError(w, http.StatusBadRequest, err)
		return
	}
	tasks, err := s.listTasks(r.Context(), q.Status, nil, nil)
	if err != nil {
		writeRESTError(w, restErrorStatus(err), err)
		return
	}
	if tasks == nil {
		tasks = []config.TaskSummary{}
	}
	writeRESTJSON(w, tasks)
}

// HandleTaskDetail serves GET /api/tasks/{id} as a hydrated task.
func (s *Server) HandleTaskDetail(w http.ResponseWriter, r *http.Request) {
	if !requireGET(w, r) {
		return
	}
	id, err := config.ParseTaskID(strings.TrimPrefix(r.URL.Path, "/api/tasks/"))
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
	q, err := config.ParseTaskSearchQuery(r.URL.Query())
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
		results = []config.ScoredTaskSummary{}
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
	mux.HandleFunc("/health", mcpkit.HandleHealth)
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("docket-mcp"))
	mux.Handle("/mcp", tracing.Middleware("docket-mcp", http.HandlerFunc(srv.HandleMCPStreamable)))
	mux.HandleFunc("/api/tasks", srv.HandleTasks)
	mux.HandleFunc("/api/tasks/", srv.HandleTaskDetail)
	mux.HandleFunc("/api/search", srv.HandleSearch)
	mux.Handle("/", tracing.Middleware("docket-mcp", http.HandlerFunc(srv.HandleMCPStreamable)))
	return mux
}

// RunServer starts the HTTP server listening on cfg.Port.
func RunServer(ctx context.Context, cfg *config.Config, sqlOpen func(string, string) (*sql.DB, error)) error {
	if sqlOpen == nil {
		sqlOpen = sql.Open
	}
	if cfg == nil || strings.TrimSpace(cfg.DatabaseURL) == "" {
		return errors.New("database URL is not configured")
	}
	db, err := sqlOpen("pgx", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer httpkit.CloseWarn(db, "database")

	storeInstance := store.NewSQLTaskStore(db)
	vikunjaClient := store.NewVikunjaClient(cfg, nil)
	ollamaClient := ollama.NewDynamicClient(func() ollama.Config { return cfg.Ollama })

	srv := NewServer(cfg, vikunjaClient, storeInstance, ollamaClient)
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
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}
