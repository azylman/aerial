# Implementation Plan: Homepage Ongoing Deploy Widget Metric

## Overview
Expose deployment status (`ongoing_deploy`, `is_deploying`) in Aerial status dashboard's `/api/status` endpoint, and add an "Ongoing Deploy" mapping to the Aerial Command HUD widget on the Homepage dashboard.

## Problem Statement
The Homepage dashboard displays the Aerial Command HUD widget with two metrics: `active_tasks_count` ("Active Tasks") and `cluster_status` ("Cluster Status"). It does not display whether a stack deployment is currently in progress (e.g. GitHub Actions CI building, Watchtower pulling, container swapping, or idle).

## Proposed Changes

### 1. Core Logic & Helpers (`dashboard/pure.go`)
Add pure functions:
- `CalculateDeployStatus(deployments []DeploymentStatus) string`:
  Evaluates deployment stages with deterministic precedence:
  `swapping` > `building` > `awaiting_pull` > `queued` > `failed` > `degraded` > `idle`
  Returns the highest-priority stage, or `"idle"` if no active deployment or only `"live"` grace deployments exist.
- `IsDeployOngoing(deployments []DeploymentStatus) bool`:
  Returns `true` if any deployment has an active in-progress stage (`"queued"`, `"building"`, `"awaiting_pull"`, `"swapping"`). Returns `false` for terminal or idle states (`"failed"`, `"degraded"`, `"live"`, or empty slice).

### 2. Status Handler & Response Model (`dashboard/main.go`)
Update `ClusterResponse`:
- Add `OngoingDeploy string json:"ongoing_deploy"`
- Add `IsDeploying bool json:"is_deploying"`
In `statusHandler`:
- Compute `ongoingDeploy := CalculateDeployStatus(deployments)` and `isDeploying := IsDeployOngoing(deployments)`
- Populate `OngoingDeploy: ongoingDeploy` and `IsDeploying: isDeploying` in `ClusterResponse`.

### 3. Homepage Widget Mapping (`docker-compose.yml`)
Add third mapping to `dashboard` service labels:
```yaml
      homepage.widget.mappings[2].field: "ongoing_deploy"
      homepage.widget.mappings[2].label: "Ongoing Deploy"
```
When idle: renders `IDLE`
When deploying: renders `BUILDING`, `SWAPPING`, `QUEUED`, etc.
When failed: renders `FAILED`
When degraded: renders `DEGRADED`

### 4. Verification & Unit Tests (`dashboard/pure_test.go`, `dashboard/main_test.go`)
- Table-driven unit tests for `CalculateDeployStatus` and `IsDeployOngoing` covering all stages (`queued`, `building`, `awaiting_pull`, `swapping`, `failed`, `degraded`, `live`, empty slice, and mixed precedence).
- Unit tests in `main_test.go` verifying that `/api/status` returns `ongoing_deploy` and `is_deploying`.
- Staged pre-commit verification via `./scripts/verify.sh --staged`.

## Review Gates
- Complexity Tier: **Tier 1** (Targeted metric addition across dashboard package and docker-compose label).
- Review Gate: Solo Adversarial Systems Critic review (Completed & remediated).
- Execution: Autonomous continuous execution.
- Verification: `./scripts/verify.sh --staged` and package tests.
