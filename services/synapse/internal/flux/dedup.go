package flux

import (
	"fmt"
	"strings"
	"sync"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

// DeployDedupTracker deduplicates deploy_success dispatches.
type DeployDedupTracker struct {
	deploySuccessDispatched sync.Map
}

// RecordDispatched marks a deployment as dispatched by PR and/or by commit SHA and job name.
func (d *DeployDedupTracker) RecordDispatched(repo, jobName, commitSHA string, prNum int) {
	normRepo := model.NormalizeRepo(repo)
	if prNum > 0 {
		d.deploySuccessDispatched.Store(fmt.Sprintf("pr:%s:%d", normRepo, prNum), true)
	}
	cleanJob := strings.TrimSpace(jobName)
	cleanSHA := strings.TrimSpace(commitSHA)
	if cleanSHA != "" && cleanJob != "" {
		d.deploySuccessDispatched.Store(fmt.Sprintf("sha:%s:%s:%s", normRepo, cleanJob, cleanSHA), true)
	}
}

// WasDispatched reports whether a deployment has already been dispatched.
func (d *DeployDedupTracker) WasDispatched(repo, jobName, commitSHA string, prNum int) bool {
	normRepo := model.NormalizeRepo(repo)
	cleanJob := strings.TrimSpace(jobName)
	cleanSHA := strings.TrimSpace(commitSHA)

	if prNum > 0 {
		if _, ok := d.deploySuccessDispatched.Load(fmt.Sprintf("pr:%s:%d", normRepo, prNum)); ok {
			return true
		}
	}
	if cleanSHA != "" && cleanJob != "" {
		if _, ok := d.deploySuccessDispatched.Load(fmt.Sprintf("sha:%s:%s:%s", normRepo, cleanJob, cleanSHA)); ok {
			return true
		}
	}
	return false
}
