package eritest

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

var errBoom = errors.New("boom")

func TestClientDispatch(t *testing.T) {
	t.Parallel()
	first := eriwire.BrainDispatchRequest{ID: "d1"}
	skip := eriwire.BrainDispatchRequest{ID: "d2", SkipIfActive: true}
	tests := []struct {
		name     string
		client   Client
		reqs     []eriwire.BrainDispatchRequest
		wantErrs []error
		wantLast string
	}{
		{"records and accepts", Client{}, []eriwire.BrainDispatchRequest{first}, []error{nil}, "d1"},
		{"dispatch error still records", Client{DispatchErr: errBoom}, []eriwire.BrainDispatchRequest{first}, []error{errBoom}, "d1"},
		{"skip if active without simulation is accepted", Client{}, []eriwire.BrainDispatchRequest{first, skip}, []error{nil, nil}, "d2"},
		{"simulated active keeps active request", Client{SimulateActive: true}, []eriwire.BrainDispatchRequest{first, skip}, []error{nil, eriwire.ErrDispatchActive}, "d1"},
		{"simulated first skip is accepted", Client{SimulateActive: true}, []eriwire.BrainDispatchRequest{skip}, []error{nil}, "d2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.client
			var errs []error
			for _, req := range tc.reqs {
				errs = append(errs, c.Dispatch(context.Background(), req))
			}
			assert.Equal(t, tc.wantErrs, errs)
			assert.Equal(t, tc.wantLast, c.LastDispatch.ID)
		})
	}
}

func TestClientDispatchAccepted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		client  Client
		wantErr error
	}{
		{"delegates to dispatch", Client{DispatchErr: errBoom}, errBoom},
		{"accepted error overrides dispatch", Client{DispatchErr: errBoom, DispatchAcceptedErr: eriwire.ErrDispatchActive}, eriwire.ErrDispatchActive},
		{"accepted", Client{}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.client
			assert.Equal(t, tc.wantErr, c.DispatchAccepted(context.Background(), eriwire.BrainDispatchRequest{ID: "d1"}))
			assert.Equal(t, "d1", c.LastDispatch.ID)
		})
	}
}

func TestClientWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		client  Client
		wantID  string
		wantErr error
	}{
		{"default id", Client{}, "msg-123", nil},
		{"configured id", Client{WidgetID: "widget-7"}, "widget-7", nil},
		{"error returns empty id", Client{WidgetID: "widget-7", WidgetErr: errBoom}, "", errBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.client
			id, err := c.Widget(context.Background(), ericlient.StatusWidgetRequest{Banner: "note"})
			assert.Equal(t, tc.wantID, id)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, "note", c.LastWidget.Banner)
		})
	}
}

func TestClientNotifications(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		client   Client
		want     Client
		wantErrs []error
	}{
		{"records calls", Client{}, Client{LastCancel: "c1", CancelAgent: "alpha", LastThreadID: "th1", LastPrompt: "wake", LastMsgChan: "ch1", LastMsgCont: "hi", DeletedWidgets: []string{"m1"}}, []error{nil, nil, nil, nil}},
		{"returns configured errors", Client{CancelErr: errBoom, PromptErr: errBoom, MessageErr: errBoom, DeleteWidgetErr: errBoom}, Client{CancelErr: errBoom, PromptErr: errBoom, MessageErr: errBoom, DeleteWidgetErr: errBoom, LastCancel: "c1", CancelAgent: "alpha", LastThreadID: "th1", LastPrompt: "wake", LastMsgChan: "ch1", LastMsgCont: "hi", DeletedWidgets: []string{"m1"}}, []error{errBoom, errBoom, errBoom, errBoom}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.client
			ctx := context.Background()
			errs := []error{
				c.CancelDispatch(ericlient.WithAgent(ctx, "alpha"), "c1"),
				c.Prompt(ctx, "th1", "wake"),
				c.Message(ctx, "ch1", "hi"),
				c.DeleteWidget(ctx, "alpha", "ch1", "m1"),
			}
			assert.Equal(t, tc.wantErrs, errs)
			assert.Equal(t, tc.want, c)
		})
	}
}
