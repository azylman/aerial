package ericlient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeterministicPromptMessageID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		threadID string
		prompt   string
		want     string
	}{
		{
			name:     "deterministic hash matches known fixture",
			threadID: "th1",
			prompt:   "wake up",
			want:     "vanadis-prompt-ad174723f0a135a9c1f7db8600f6ecb0",
		},
		{
			name:     "different prompt produces different id",
			threadID: "th1",
			prompt:   "other prompt",
			want:     "vanadis-prompt-c73f17d01b5f53a9bd0baefdaa8ca51f",
		},
		{
			name:     "different thread produces different id",
			threadID: "th2",
			prompt:   "wake up",
			want:     "vanadis-prompt-b1ad91e53a91e48d8a013301d4481558",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := deterministicPromptMessageID(tc.threadID, tc.prompt)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAgentContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	assert.Empty(t, AgentFromContext(ctx))

	ctxBlank := WithAgent(ctx, "   ")
	assert.Empty(t, AgentFromContext(ctxBlank))

	ctxSet := WithAgent(ctx, "aerial")
	assert.Equal(t, "aerial", AgentFromContext(ctxSet))
}
