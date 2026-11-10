package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// VikunjaTask models a task object from the Vikunja REST API.
type VikunjaTask struct {
	ID           int64                    `json:"id"`
	Title        string                   `json:"title"`
	Description  string                   `json:"description"`
	Done         bool                     `json:"done"`
	DoneAt       *time.Time               `json:"done_at,omitempty"`
	Created      time.Time                `json:"created"`
	Updated      time.Time                `json:"updated"`
	ProjectID    int64                    `json:"project_id"`
	RelatedTasks map[string][]VikunjaTask `json:"related_tasks,omitempty"`
}

// VikunjaProject models a project object in Vikunja.
type VikunjaProject struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
}

// VikunjaComment models a comment on a Vikunja task.
type VikunjaComment struct {
	ID     int64 `json:"id"`
	Author struct {
		Username string `json:"username"`
	} `json:"author"`
	Comment string    `json:"comment"`
	Created time.Time `json:"created"`
}

// VikunjaRelationReq models the body for creating a task relation.
type VikunjaRelationReq struct {
	OtherTaskID  int64  `json:"other_task_id"`
	RelationKind string `json:"relation_kind"`
}

// VikunjaClient provides HTTP interaction with Vikunja REST API.
type VikunjaClient struct {
	cfg              *Config
	httpClient       *http.Client
	mu               sync.Mutex
	token            string
	defaultProjectID int64
}

// NewVikunjaClient initializes a new Vikunja client.
func NewVikunjaClient(cfg *Config, hc *http.Client) *VikunjaClient {
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	return &VikunjaClient{
		cfg:              cfg,
		httpClient:       hc,
		defaultProjectID: cfg.DefaultProjectID,
	}
}

func (c *VikunjaClient) getToken() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token
}

func (c *VikunjaClient) authenticateLocked(ctx context.Context) error {
	loginReq := map[string]string{
		"username": c.cfg.VikunjaUsername,
		"password": c.cfg.VikunjaPassword,
	}
	var authResp struct {
		Token string `json:"token"`
	}
	err := c.execRequest(ctx, http.MethodPost, "/api/v1/login", loginReq, &authResp, false)
	if err == nil && authResp.Token != "" {
		c.token = authResp.Token
		return nil
	}

	regReq := map[string]string{
		"username": c.cfg.VikunjaUsername,
		"password": c.cfg.VikunjaPassword,
		"email":    c.cfg.VikunjaUsername + "@aerial.local",
	}
	var regResp struct {
		Token string `json:"token"`
	}
	regErr := c.execRequest(ctx, http.MethodPost, "/api/v1/register", regReq, &regResp, false)
	if regErr == nil && regResp.Token != "" {
		c.token = regResp.Token
		return nil
	}

	if err := c.execRequest(ctx, http.MethodPost, "/api/v1/login", loginReq, &authResp, false); err != nil {
		return fmt.Errorf("vikunja authentication failed: %w", err)
	}
	c.token = authResp.Token
	return nil
}

func (c *VikunjaClient) execRequest(ctx context.Context, method, path string, reqBody, respOut any, withAuth bool) error {
	baseURL := strings.TrimRight(c.cfg.VikunjaURL, "/")
	var bodyReader io.Reader
	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to encode request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if withAuth {
		token := c.getToken()
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request %s %s failed: %w", method, path, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPStatusError{StatusCode: resp.StatusCode, Body: string(bodyBytes)}
	}

	if respOut != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, respOut); err != nil {
			return fmt.Errorf("failed to parse response JSON: %w", err)
		}
	}
	return nil
}

// HTTPStatusError represents a non-2xx HTTP response from Vikunja.
type HTTPStatusError struct {
	StatusCode int
	Body       string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("vikunja API returned status %d: %s", e.StatusCode, e.Body)
}

func (c *VikunjaClient) doJSON(ctx context.Context, method, path string, reqBody, respOut any) error {
	if c.getToken() == "" {
		c.mu.Lock()
		if c.token == "" {
			if err := c.authenticateLocked(ctx); err != nil {
				c.mu.Unlock()
				return err
			}
		}
		c.mu.Unlock()
	}

	err := c.execRequest(ctx, method, path, reqBody, respOut, true)
	if err != nil {
		var statusErr *HTTPStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusUnauthorized {
			c.mu.Lock()
			reauthErr := c.authenticateLocked(ctx)
			c.mu.Unlock()
			if reauthErr != nil {
				return fmt.Errorf("reauth on 401 failed: %w", reauthErr)
			}
			return c.execRequest(ctx, method, path, reqBody, respOut, true)
		}
		return err
	}
	return nil
}

// GetDefaultProject returns the default project ID, auto-creating a project if none exist.
func (c *VikunjaClient) GetDefaultProject(ctx context.Context) (int64, error) {
	c.mu.Lock()
	if c.defaultProjectID > 0 {
		id := c.defaultProjectID
		c.mu.Unlock()
		return id, nil
	}
	c.mu.Unlock()

	projects, err := c.ListProjects(ctx)
	if err != nil {
		return 0, err
	}
	if len(projects) > 0 {
		c.mu.Lock()
		c.defaultProjectID = projects[0].ID
		id := c.defaultProjectID
		c.mu.Unlock()
		return id, nil
	}

	created, err := c.CreateProject(ctx, "Docket")
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	if c.defaultProjectID == 0 {
		c.defaultProjectID = created.ID
	}
	id := c.defaultProjectID
	c.mu.Unlock()
	return id, nil
}

// ListProjects retrieves all projects visible to the authenticated user.
func (c *VikunjaClient) ListProjects(ctx context.Context) ([]VikunjaProject, error) {
	var projects []VikunjaProject
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/projects", nil, &projects); err != nil {
		return nil, err
	}
	return projects, nil
}

// CreateProject creates a new project.
func (c *VikunjaClient) CreateProject(ctx context.Context, title string) (*VikunjaProject, error) {
	req := map[string]string{"title": title}
	var proj VikunjaProject
	if err := c.doJSON(ctx, http.MethodPut, "/api/v1/projects", req, &proj); err != nil {
		return nil, err
	}
	return &proj, nil
}

// vikunjaPageSize is the largest page Vikunja serves; it caps per_page at this value.
const vikunjaPageSize = 50

// ListProjectTasks fetches every task within a project, page by page. Paging stops
// only on an empty page or a page that adds no unseen task (a server ignoring the
// page param), never on a short page, since Vikunja may cap pages below per_page.
func (c *VikunjaClient) ListProjectTasks(ctx context.Context, projectID int64) ([]VikunjaTask, error) {
	var allTasks []VikunjaTask
	seen := make(map[int64]bool)
	for page := 1; ; page++ {
		var pageTasks []VikunjaTask
		path := fmt.Sprintf("/api/v1/projects/%d/tasks?page=%d&per_page=%d", projectID, page, vikunjaPageSize)
		if err := c.doJSON(ctx, http.MethodGet, path, nil, &pageTasks); err != nil {
			return nil, err
		}
		added := 0
		for _, t := range pageTasks {
			if seen[t.ID] {
				continue
			}
			seen[t.ID] = true
			allTasks = append(allTasks, t)
			added++
		}
		if added == 0 {
			return allTasks, nil
		}
	}
}

// GetTask fetches full details of a specific task.
func (c *VikunjaClient) GetTask(ctx context.Context, taskID int64) (*VikunjaTask, error) {
	var task VikunjaTask
	path := fmt.Sprintf("/api/v1/tasks/%d", taskID)
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// CreateTask creates a task within the designated project.
func (c *VikunjaClient) CreateTask(ctx context.Context, projectID int64, title, description string) (*VikunjaTask, error) {
	req := map[string]string{
		"title":       title,
		"description": description,
	}
	var task VikunjaTask
	path := fmt.Sprintf("/api/v1/projects/%d/tasks", projectID)
	if err := c.doJSON(ctx, http.MethodPut, path, req, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// UpdateTask writes mutated task fields back to Vikunja via full-struct overwrite.
func (c *VikunjaClient) UpdateTask(ctx context.Context, task *VikunjaTask) (*VikunjaTask, error) {
	var updated VikunjaTask
	path := fmt.Sprintf("/api/v1/tasks/%d", task.ID)
	if err := c.doJSON(ctx, http.MethodPost, path, task, &updated); err != nil {
		return nil, err
	}
	return &updated, nil
}

// CreateRelation links two tasks via a directed relation kind.
func (c *VikunjaClient) CreateRelation(ctx context.Context, taskID, otherTaskID int64, kind string) error {
	req := VikunjaRelationReq{
		OtherTaskID:  otherTaskID,
		RelationKind: kind,
	}
	path := fmt.Sprintf("/api/v1/tasks/%d/relations", taskID)
	return c.doJSON(ctx, http.MethodPut, path, req, nil)
}

// DeleteRelation removes a directed relation between two tasks.
func (c *VikunjaClient) DeleteRelation(ctx context.Context, taskID, otherTaskID int64, kind string) error {
	path := fmt.Sprintf("/api/v1/tasks/%d/relations/%s/%d", taskID, kind, otherTaskID)
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil)
}

// ListComments retrieves all comments on a task.
func (c *VikunjaClient) ListComments(ctx context.Context, taskID int64) ([]VikunjaComment, error) {
	var comments []VikunjaComment
	path := fmt.Sprintf("/api/v1/tasks/%d/comments", taskID)
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &comments); err != nil {
		return nil, err
	}
	return comments, nil
}

// CreateComment posts a new comment to a task.
func (c *VikunjaClient) CreateComment(ctx context.Context, taskID int64, commentText string) error {
	req := map[string]string{"comment": commentText}
	path := fmt.Sprintf("/api/v1/tasks/%d/comments", taskID)
	return c.doJSON(ctx, http.MethodPut, path, req, nil)
}
