package prstate

import "github.com/azylman/aerial/pkg/prwire"

// The PR lifecycle wire contract lives in pkg/prwire, shared with synapse.
// These aliases are identical types, so reflected handler schemas, journaled
// state and ObjectSend payloads keep their exact bytes.
type (
	PREvent                = prwire.PREvent
	RepoEvent              = prwire.RepoEvent
	SHAEvent               = prwire.SHAEvent
	EriEffect              = prwire.EriEffect
	AutoUpdatePolicy       = prwire.AutoUpdatePolicy
	CollaboratorAlert      = prwire.CollaboratorAlert
	KustomizationRequest   = prwire.KustomizationRequest
	BuildSelection         = prwire.BuildSelection
	BuildResult            = prwire.BuildResult
	BuildMetadata          = prwire.BuildMetadata
	RegisterRequest        = prwire.RegisterRequest
	RegisterResponse       = prwire.RegisterResponse
	BuildLifecycle         = prwire.BuildLifecycle
	DeployView             = prwire.DeployView
	RolloutRef             = prwire.RolloutRef
	RetryReadinessRequest  = prwire.RetryReadinessRequest
	RetryReadinessResponse = prwire.RetryReadinessResponse
	ResolveSHAResponse     = prwire.ResolveSHAResponse
)
