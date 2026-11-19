package main

import (
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type (
	BuildMetadata           = model.BuildMetadata
	BuildSelection          = model.BuildSelection
	BuildResult             = model.BuildResult
	GitHubCheckRunsResponse = model.GitHubCheckRunsResponse
	GitHubMetadataCheck     = model.GitHubMetadataCheck
)

const (
	buildMetadataManifest        = model.BuildMetadataManifest
	buildMetadataFinal           = model.BuildMetadataFinal
	buildMetadataResult          = model.BuildMetadataResult
	buildMetadataProgress        = model.BuildMetadataProgress
	buildMetadataCheckName       = model.BuildMetadataCheckName
	buildMetadataResultCheckName = model.BuildMetadataResultCheckName
	buildEventPush               = model.BuildEventPush
	buildEventDispatch           = model.BuildEventDispatch
	buildResultRunning           = model.BuildResultRunning
	buildResultFailure           = model.BuildResultFailure
	buildResultCancelled         = model.BuildResultCancelled
)

func isBuildMetadataCheck(name string) bool {
	return model.IsBuildMetadataCheck(name)
}

func isDependabotAppCheck(appSlug string) bool {
	return model.IsDependabotAppCheck(appSlug)
}

func parseStoredBuildMetadata(raw []byte) (BuildMetadata, error) {
	return model.ParseStoredBuildMetadata(raw)
}
