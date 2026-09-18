# Implementation Plan - Configurable Multi-Repo CI Fanout in Aerial Dashboard

## 1. Overview & Context
Alex requested that the Aerial Command HUD status dashboard fan out CI polling across multiple repositories (e.g. `azylman/aerial`, `azylman/aerial-sidecars`, and `azylman/aerial-config`), while strictly keeping the core engine repository generic and decoupled:
> "Let's do this fanout described here: https://discord.com/channels/1464358485817954538/1550586356571312141/1550634684701212934
> But it needs to be configurable in aerial-config so that the core repo doesn't know about my own private repos"

## 2. Architectural Specification & Remediations (The Girl Gang Approved)

### A. Two-Repository Decoupling & Config Hierarchy
1. **Core Engine (`azylman/aerial`)**:
   - Generic open-source defaults: `GITHUB_REPO` defaults to `azylman/aerial` if unspecified.
   - Zero private/hardcoded repo names.
   - Dynamically inspects user configuration at `AERIAL_CONFIG_PATH` (defaults to `/share/aerial-config/config.yaml`), mounted read-only (`:ro`).
   - Discovery Hierarchy:
     1. `dashboard.github_repos: [repo1, repo2, ...]` in `config.yaml`
     2. `dashboard.repos: [repo1, repo2, ...]` in `config.yaml`
     3. `github_repos: [repo1, repo2, ...]` in `config.yaml`
     4. `GITHUB_REPOS="repo1,repo2,..."` environment variable fallback/override
     5. `GITHUB_REPO="repo"` environment variable fallback (default: `"azylman/aerial"`)
   - If `config.yaml` is missing or invalid YAML, log warning and gracefully retain current in-memory repo list (LKGC resilience).

2. **User Configuration (`azylman/aerial-config`)**:
   - Authors the private/user-specific repository list in `config.yaml`:
     ```yaml
     dashboard:
       github_repos:
         - "azylman/aerial"
         - "azylman/aerial-sidecars"
         - "azylman/aerial-config"
     ```

### B. Poller Concurrency, Mutex Discipline & State Lifecycle
1. **Per-Repo ETags & Mutex Invariant**:
   - `runsETagMap map[string]string`: Tracks HTTP ETag independently per repository.
   - `jobsETagMap map[int64]string`: Tracks ETags per job across runs.
   - **ZERO-LOCK-ON-IO INVARIANT**: `p.mu` is strictly held ONLY for memory read/writes (fetching/storing ETags and updating in-memory caches). Never hold `p.mu` during `p.client.Do()` network calls.
2. **Atomic State Publishing & Per-Repo Caching**:
   - State stored internally in `cachedRunsByRepo map[string][]GitHubRun`.
   - Each repo updates its own entries without wiping other repos on 304 Not Modified or network errors.
   - At the end of the full poll cycle, `p.cachedRuns` is reconstructed, sorted (active runs first, then sorted by recency), and published atomically under `p.mu.Lock()`.
3. **Global Job Pruning (Eliminating the Pruning Wipeout Bug)**:
   - Job and job ETag pruning occurs globally at the end of the full poll cycle using `allActiveRunIDs` collected across all repos, preventing Repo B from deleting Repo A's cached matrix jobs.
4. **Context-Aware Job Fetching**:
   - `fetchJobsForRun(ctx context.Context, repo string, runID int64)`: Uses the run's specific repository in the GitHub API endpoint `/repos/{repo}/actions/runs/{runID}/jobs`.
5. **Rate Limiting & Sequential Polling**:
   - Sequentially polls each repo with 4s timeout per request.
   - Detects HTTP 403 or rate limit exhaustion, logging a diagnostic warning without crashing.

### C. Deployment Status Aggregation & Hangar Boundary
1. **Multi-Repo Active Deployment Fanout**:
   - `mergeClusterDeploymentsWithHangar` checks all runs in `runs`. Every active run (`queued` or `in_progress`) produces an active `DeploymentStatus` card with `Repository: run.Repository`.
   - If 1 or more active runs exist, all active runs are returned.
2. **Hangar Container Reconciliation Guard (State 4 Trap Fix)**:
   - State 4 container swap / 120s timeout checks strictly apply ONLY to runs that contain container build jobs or match the core container repository (`azylman/aerial`).
   - Runs for non-container repositories (e.g. `aerial-config`) transition to completed without failing on container swap timeout.

### D. Permet HUD Frontend (`static/app.js` & `style.css`)
1. **Dynamic Commit URLs**:
   - Link commits to `https://github.com/${repo}/commit/${sha}` respecting `dep.repository` (validated with regex).
2. **Repository Badge**:
   - Render `<span class="deploy-repo-badge">📦 ${escapeHtml(repoShortName)}</span>` in `.deploy-target`.
   - Cyberpunk purple theme styling in `style.css` with mobile truncation (`max-width: 130px; text-overflow: ellipsis;`).
   - Invariant: Avoid `.deploy-service-name` to maintain existing regression test assertions.
3. **Dynamic Header Badge Pluralization**:
   - Dynamically compute active building count: `${activeCount > 1 ? `⚡ ${activeCount} CI BUILDS ACTIVE` : `⚡ 1 CI BUILD ACTIVE`}`.

---

## 3. Step-by-Step Implementation Tasks
1. **Task 1 (TDD - Config Parsing & DashboardConfig)**:
   - Unit tests in `main_test.go` for `loadDashboardRepos` and `NewDashboardConfigFromLookup`.
   - Implement `loadDashboardRepos(configPath string) []string`, update `rawUserConfig`, `NewDashboardConfigFromLookup`, and `DashboardConfig`.
2. **Task 2 (TDD - Multi-Repo GitHubPoller)**:
   - Unit tests in `main_test.go` with mock HTTP server testing multi-repo fanout, per-repo ETags, global job pruning across repos, and `fetchJobsForRun` with repository context.
   - Implement `NewMultiGitHubPoller`, `getActiveRepos`, `pollOnce`, `fetchJobsForRun`, and `GetSnapshot`.
3. **Task 3 (TDD - Deployment Status Aggregation & Hangar Boundary)**:
   - Unit tests in `main_test.go` verifying `mergeClusterDeploymentsWithHangar` handles multi-repo active runs and avoids false container swap timeouts on non-container repos.
   - Update `mergeClusterDeploymentsWithHangar`, `GitHubRun`, and `DeploymentStatus`.
4. **Task 4 (Frontend Permet HUD Updates)**:
   - Update `static/app.js` for dynamic commit links, repository badge, and dynamic building badge count.
   - Add styling in `static/style.css`.
   - Update `app.test.js` to verify repository URL generation and rendering invariants. Run `node --test app.test.js`.
5. **Task 5 (Pre-Flight Verification & PR Submission for Aerial)**:
   - Run `go test -v ./...` and `./scripts/verify.sh --staged`.
   - Author `PR_DESCRIPTION.md`.
   - Submit PR via `/share/aerial/scripts/aerial-pr.sh submit`.
6. **Task 6 (User Configuration PR for Aerial-Config)**:
   - Initialize scratch workspace via `/share/aerial/scripts/aerial-config-pr.sh init`.
   - Update `config.yaml` to include `dashboard.github_repos`.
   - Pre-flight verify and submit PR via `/share/aerial/scripts/aerial-config-pr.sh submit`.
