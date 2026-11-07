package queue

import (
	"context"

	"github.com/azylman/aerial/services/eri/pkg/discord"
)

// SetWidgetDeliveryFunc sets the status widget delivery function for testing.
func (p *WorkerPool) SetWidgetDeliveryFunc(fn func(req discord.StatusWidgetRequest) (string, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg.WidgetDeliveryFunc = fn
}

// DeliverStatusWidget delivers or edits a status widget while maintaining cache and active steps.
// It is preserved as a test-only convenience wrapper delegating to DeliverStatusWidgetWithContext.
func (p *WorkerPool) DeliverStatusWidget(req discord.StatusWidgetRequest) (string, error) {
	return p.DeliverStatusWidgetWithContext(context.Background(), req)
}
