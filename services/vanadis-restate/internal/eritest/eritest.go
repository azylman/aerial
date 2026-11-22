// Package eritest provides a declarative fake ericlient.EriClient shared by
// the vanadis-restate packages that drive eri (build, executor, prflow).
package eritest

import (
	"context"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

// DefaultWidgetID is the message id Widget returns when WidgetID is empty.
const DefaultWidgetID = "msg-123"

// Client is a fake ericlient.EriClient. Every method records its arguments in
// the Last* fields before returning the matching error field.
//
// DispatchAccepted returns DispatchAcceptedErr when set, otherwise behaves
// like Dispatch. When SimulateActive is set, a SkipIfActive dispatch that
// arrives after an accepted dispatch returns eriwire.ErrDispatchActive and
// LastDispatch keeps the active request, mirroring eri's skip_if_active.
type Client struct {
	DispatchErr         error
	DispatchAcceptedErr error
	CancelErr           error
	PromptErr           error
	WidgetErr           error
	DeleteWidgetErr     error
	MessageErr          error
	WidgetID            string
	SimulateActive      bool

	LastDispatch *eriwire.BrainDispatchRequest
	LastCancel   string
	CancelAgent  string
	LastThreadID string
	LastPrompt   string
	LastWidget   *ericlient.StatusWidgetRequest
	// DeletedWidgets records every DeleteWidget messageID in call order.
	DeletedWidgets []string
	LastMsgChan    string
	LastMsgCont    string

	active *eriwire.BrainDispatchRequest
}

var _ ericlient.EriClient = (*Client)(nil)

// Dispatch records req and returns DispatchErr.
func (c *Client) Dispatch(_ context.Context, req eriwire.BrainDispatchRequest) error {
	if c.DispatchErr != nil {
		c.LastDispatch = &req
		return c.DispatchErr
	}
	if c.SimulateActive && req.SkipIfActive && c.active != nil {
		c.LastDispatch = c.active
		return eriwire.ErrDispatchActive
	}
	c.LastDispatch = &req
	if c.SimulateActive {
		c.active = &req
	}
	return nil
}

// DispatchAccepted returns DispatchAcceptedErr when set, else Dispatch's result.
func (c *Client) DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	if c.DispatchAcceptedErr != nil {
		c.LastDispatch = &req
		return c.DispatchAcceptedErr
	}
	return c.Dispatch(ctx, req)
}

// CancelDispatch records chunkID and the context agent, then returns CancelErr.
func (c *Client) CancelDispatch(ctx context.Context, chunkID string) error {
	c.LastCancel = chunkID
	c.CancelAgent = ericlient.AgentFromContext(ctx)
	return c.CancelErr
}

// Prompt records threadID and prompt, then returns PromptErr.
func (c *Client) Prompt(_ context.Context, threadID, prompt string) error {
	c.LastThreadID = threadID
	c.LastPrompt = prompt
	return c.PromptErr
}

// Widget records req and returns ("", WidgetErr) on error, else WidgetID
// (DefaultWidgetID when empty).
func (c *Client) Widget(_ context.Context, req ericlient.StatusWidgetRequest) (string, error) {
	c.LastWidget = &req
	if c.WidgetErr != nil {
		return "", c.WidgetErr
	}
	if c.WidgetID == "" {
		return DefaultWidgetID, nil
	}
	return c.WidgetID, nil
}

// DeleteWidget records messageID and returns DeleteWidgetErr.
func (c *Client) DeleteWidget(_ context.Context, _, _, messageID string) error {
	c.DeletedWidgets = append(c.DeletedWidgets, messageID)
	return c.DeleteWidgetErr
}

// Message records channelID and content, then returns MessageErr.
func (c *Client) Message(_ context.Context, channelID, content string) error {
	c.LastMsgChan = channelID
	c.LastMsgCont = content
	return c.MessageErr
}
