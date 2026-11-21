package router

// legacyFixtureBlob stands in for an inventoried pre-producer workflow blob.
const legacyFixtureBlob = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// legacyFixtureConfig pins legacyFixtureBlob in the immutable allowlist so the
// deployment-plumbing fixtures, which serve historical job names, exercise the
// bounded legacy adapter; structured transport has its own fixtures.
func legacyFixtureConfig(cfg Config) Config {
	for _, repo := range []string{normalizeRepo("aerial"), normalizeRepo("aerial-sidecars"), normalizeRepo("mirrormere")} {
		cfg.LegacyBuildWorkflows = append(cfg.LegacyBuildWorkflows, LegacyBuildWorkflow{Repository: repo, WorkflowPath: metadataWorkflowPath(repo), BlobSHA: legacyFixtureBlob})
	}
	return cfg
}
