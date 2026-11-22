package interactions

import (
	"context"
	"net/http"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

// DeployStatusProvider supplies current deployment information for /status.
type DeployStatusProvider func() string

// QuotaProvider supplies current model quota groups for /quota.
type QuotaProvider func() metrics.QuotaSnapshot

// GitOpsChannelUpdater supplies the update function for modifying channel configurations.
type GitOpsChannelUpdater func(ctx context.Context, channelKey string, mutateFn func(p *config.ChannelPolicy)) (commitSHA string, err error)

// ConfigProvider supplies dynamic configuration snapshots JIT.
type ConfigProvider func() *config.Config

// PoolProvider supplies dynamic worker pool snapshots JIT.
type PoolProvider func() *queue.WorkerPool

// TurnCanceller cancels the active turn and queued messages of a thread.
type TurnCanceller interface {
	CancelThreadTurn(ctx context.Context, threadID string, req queue.CancelTurnRequest) (queue.CancelTurnResponse, error)
}

// TurnCancellerProvider supplies the turn canceller JIT; it returns nil when none is available.
type TurnCancellerProvider func() TurnCanceller

// InteractionDeps holds the dependencies required to execute slash commands.
type InteractionDeps struct {
	Store                 db.Store
	Pool                  *queue.WorkerPool
	PoolProvider          PoolProvider
	TurnCancellerProvider TurnCancellerProvider
	Config                *config.Config
	ConfigProvider        ConfigProvider
	AppID                 string
	DeployStatusProvider  DeployStatusProvider
	QuotaProvider         QuotaProvider
	GitOpsChannelUpdater  GitOpsChannelUpdater
	MemoryURL             string
	MemoryClient          *http.Client
	HaroURL               string
}
