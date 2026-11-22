package router

import (
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// legacyFixtureBlob stands in for an inventoried pre-producer workflow blob.
const legacyFixtureBlob = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// legacyFixtureConfig pins legacyFixtureBlob in the immutable allowlist so the
// deployment-plumbing fixtures, which serve historical job names, exercise the
// bounded legacy adapter; structured transport has its own fixtures.
func legacyFixtureConfig(cfg Config) Config {
	for _, repo := range []string{model.NormalizeRepo("aerial"), model.NormalizeRepo("aerial-sidecars"), model.NormalizeRepo("mirrormere")} {
		cfg.LegacyBuildWorkflows = append(cfg.LegacyBuildWorkflows, LegacyBuildWorkflow{Repository: repo, WorkflowPath: buildmeta.MetadataWorkflowPath(repo), BlobSHA: legacyFixtureBlob})
	}
	return cfg
}
