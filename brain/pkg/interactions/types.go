package interactions

import (
	"context"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
)

// DeployStatusProvider supplies current deployment information for /status.
type DeployStatusProvider func() string

// GitOpsChannelUpdater supplies the update function for modifying channel configurations.
type GitOpsChannelUpdater func(ctx context.Context, channelKey string, mutateFn func(p *config.ChannelPolicy)) (commitSHA string, err error)

// ConfigProvider supplies dynamic configuration snapshots JIT.
type ConfigProvider func() *config.Config

// PoolProvider supplies dynamic worker pool snapshots JIT.
type PoolProvider func() *queue.WorkerPool

// InteractionDeps holds the dependencies required to execute slash commands.
type InteractionDeps struct {
	Store                db.Store
	Pool                 *queue.WorkerPool
	PoolProvider         PoolProvider
	Config               *config.Config
	ConfigProvider       ConfigProvider
	AppID                string
	DeployStatusProvider DeployStatusProvider
	GitOpsChannelUpdater GitOpsChannelUpdater
}
