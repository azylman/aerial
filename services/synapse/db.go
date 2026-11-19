package main

import (
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/registry"
)

// DeployingPR holds metadata for a PR currently undergoing deployment rollout.
type DeployingPR = model.DeployingPR

// ErrInvalidTargetID indicates an invalid Discord snowflake target.
var ErrInvalidTargetID = model.ErrInvalidTargetID

// PRRegisterRequest defines the request payload for POST /internal/pr/register.
type PRRegisterRequest = model.PRRegisterRequest

// PRRegistryUpdater defines the interface for updating PR lifecycle state in PostgreSQL pr_registry.
type PRRegistryUpdater = model.PRRegistryUpdater

type pgxQueryExecer = registry.PGXQueryExecer

// PostgresPRRegistry implements PRRegistryUpdater backed by a pgxpool.Pool.
type PostgresPRRegistry = registry.PostgresPRRegistry

// NewPostgresPRRegistry constructs a new PostgresPRRegistry.
var NewPostgresPRRegistry = registry.NewPostgresPRRegistry

const (
	kustomizationRequestsKey  = registry.KustomizationRequestsKey
	deployedKustomizationsKey = registry.DeployedKustomizationsKey
)

// kustomizationRequest is the reconcile request recorded for one Flux Kustomization of a PR rollout.
type kustomizationRequest = model.KustomizationRequest

// PRBuildLifecycle is the registry lifecycle fence image-build consumers read.
type PRBuildLifecycle = model.PRBuildLifecycle

// extractStringSliceFromMetadata extracts a slice of strings stored under key in metadata JSONB.
func extractStringSliceFromMetadata(meta map[string]any, key string) []string {
	return model.ExtractStringSliceFromMetadata(meta, key)
}

// extractJobsFromMetadata extracts candidate job names stored in the PR registry metadata JSONB.
func extractJobsFromMetadata(meta map[string]any) []string {
	return model.ExtractJobsFromMetadata(meta)
}
