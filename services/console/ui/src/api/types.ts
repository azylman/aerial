// Wire types for the console /api/* endpoints. Each interface mirrors the JSON tags of the Go
// response structs in services/console (main.go, handlers.go, config.go) field for field.
//
// Mapping rules:
//   - `omitempty` / `omitzero` scalars, slices, maps and pointers are optional (`?`).
//   - `time.Time` with `omitempty` is still always emitted by encoding/json (structs are never
//     "empty"), so those fields stay required; only pointers and `omitzero` make times optional.
//   - Slices without `omitempty` that the handler does not normalise may arrive as `null`.
//   - Everything is readonly: poll results are immutable snapshots shared by reference.

/** RFC 3339 timestamp as emitted by Go's time.Time JSON marshaller. */
export type IsoTimestamp = string;

// ---- /api/status (main.go: ClusterResponse) ----

export interface ServiceStatus {
  readonly name: string;
  readonly status: string;
  readonly uptime_seconds: number;
  readonly last_check_time: IsoTimestamp;
}

export type PipelineState = 'completed' | 'active' | 'pending' | 'failed';

export interface MatrixJobChip {
  readonly name: string;
  readonly status: PipelineState | (string & {});
  readonly conclusion?: string;
  readonly duration?: string;
  readonly type?: 'ci' | 'container' | (string & {});
}

export interface DeploymentStep {
  readonly name: string;
  readonly icon: string;
  readonly status: PipelineState | (string & {});
}

export interface DeploymentStatus {
  readonly id: string;
  readonly service: string;
  readonly repository?: string;
  readonly commit: string;
  readonly commit_msg?: string;
  readonly commit_time?: IsoTimestamp;
  readonly stage: string;
  readonly progress: number;
  readonly steps: readonly DeploymentStep[] | null;
  readonly matrix_jobs?: readonly MatrixJobChip[];
  readonly html_url?: string;
  readonly started_at: IsoTimestamp;
}

export interface ActiveTaskStatus {
  readonly id: string;
  readonly thread_id: string;
  readonly session_id?: string;
  readonly author_name: string;
  readonly prompt: string;
  readonly summary: string;
  readonly status: string;
  readonly retry_count: number;
  readonly trigger_type: string;
  readonly created_at: IsoTimestamp;
  readonly started_at: IsoTimestamp;
}

export interface QuickLaunchLink {
  readonly name: string;
  readonly url: string;
  readonly icon?: string;
  readonly description?: string;
  readonly target?: string;
  readonly is_core?: boolean;
  readonly is_custom?: boolean;
}

export interface RepoStatus {
  readonly repo: string;
  readonly github_repo?: string;
  readonly disk_commit: string;
  readonly disk_commit_time?: IsoTimestamp;
  readonly remote_commit: string;
  readonly remote_commit_time?: IsoTimestamp;
  readonly time_lag_seconds: number;
  readonly sync_status: 'synced' | 'lagging' | 'error' | (string & {});
  readonly last_sync_time: IsoTimestamp;
  readonly error?: string;
}

export interface ReconciliationStatus {
  readonly active: boolean;
  readonly state: string;
  readonly stage: string;
  readonly trigger?: string;
  readonly target_services?: readonly string[];
  readonly commit_sha?: string;
  readonly started_at: IsoTimestamp;
  readonly completed_at: IsoTimestamp;
  readonly error?: string;
}

export interface GitSyncStatusResponse {
  readonly status: 'synced' | 'lagging' | 'error' | (string & {});
  readonly max_lag_seconds: number;
  readonly last_sync_time: IsoTimestamp;
  readonly repos?: Readonly<Record<string, RepoStatus>>;
  readonly reconciliation?: ReconciliationStatus;
  readonly error?: string;
}

export interface ClusterResponse {
  readonly system_time: IsoTimestamp;
  readonly cluster_status: string;
  readonly active_tasks_count: number;
  readonly active_tasks: readonly ActiveTaskStatus[];
  readonly services: readonly ServiceStatus[];
  readonly deployments: readonly DeploymentStatus[];
  readonly quick_launch_links: readonly QuickLaunchLink[] | null;
  readonly git_sync: GitSyncStatusResponse;
  readonly ongoing_deploy: string;
  readonly is_deploying: boolean;
}

// ---- /api/facts (handlers.go: FactsAPIResponse) ----

export interface FactItem {
  readonly id: number;
  readonly fact_text: string;
  readonly reinforce_count: number;
  readonly last_reinforced_at?: IsoTimestamp;
  readonly created_at: IsoTimestamp;
}

export interface FactsAPIResponse {
  readonly facts: readonly FactItem[];
  readonly total: number;
  readonly limit: number;
  readonly offset: number;
  readonly status?: string;
  readonly error?: string;
}

// ---- /api/schedules (handlers.go: SchedulesAPIResponse) ----

export interface ScheduleSummaryMetrics {
  readonly total_active: number;
  readonly cron_count: number;
  readonly one_shot_count: number;
  readonly total_runs_24h: number;
  readonly success_rate_24h: number;
}

export interface CronSchedule {
  readonly id: string;
  readonly target_id?: string;
  readonly channel_id: string;
  readonly title_prefix: string;
  readonly cron_expr: string;
  readonly cron_description: string;
  readonly prompt: string;
  readonly timezone: string;
  readonly next_run_at: IsoTimestamp;
  readonly enabled: boolean;
  readonly created_at: IsoTimestamp;
}

export interface OneShotSchedule {
  readonly id: string;
  readonly thread_id: string;
  readonly prompt: string;
  readonly run_at: IsoTimestamp;
  readonly created_at: IsoTimestamp;
}

export interface SchedulesAPIResponse {
  readonly status: string;
  readonly system_time: IsoTimestamp;
  readonly summary: ScheduleSummaryMetrics;
  readonly crons: readonly CronSchedule[];
  readonly one_shots: readonly OneShotSchedule[];
  readonly error?: string;
}

// ---- /api/schedules/runs (handlers.go: ScheduleRunsAPIResponse) ----

export interface ScheduleRun {
  readonly id: string;
  readonly schedule_id: string;
  readonly schedule_type: string;
  readonly message_id?: string;
  readonly target_id?: string;
  readonly thread_id?: string;
  readonly title?: string;
  readonly prompt: string;
  readonly status: string;
  readonly started_at: IsoTimestamp;
  readonly completed_at?: IsoTimestamp;
  readonly duration_ms: number;
  readonly error?: string;
}

export interface ScheduleRunsAPIResponse {
  readonly status: string;
  readonly total: number;
  readonly limit: number;
  readonly offset: number;
  readonly runs: readonly ScheduleRun[];
  readonly error?: string;
}

// ---- /api/builds and /api/builds/{id} (handlers.go) ----

export interface VanadisChunkDTO {
  readonly id: string;
  readonly build_id: string;
  readonly chunk_index: number;
  readonly repo?: string;
  readonly title: string;
  readonly instructions?: string;
  readonly target_files?: readonly string[];
  readonly target_symbols?: readonly string[];
  readonly effort?: string;
  readonly acceptance_criteria?: readonly string[];
  readonly status: string;
  readonly pr_number?: number;
  readonly started_at: IsoTimestamp;
  readonly completed_at: IsoTimestamp;
  readonly created_at: IsoTimestamp;
  readonly updated_at: IsoTimestamp;
}

export interface VanadisBuildDTO {
  readonly id: string;
  readonly thread_id: string;
  readonly tier: number;
  readonly title: string;
  readonly objective: string;
  readonly shared_context?: string;
  readonly status: string;
  readonly status_message_id?: string;
  readonly created_at: IsoTimestamp;
  readonly updated_at: IsoTimestamp;
  readonly queue_state?: string;
  readonly total_chunks?: number;
  readonly completed_chunks?: number;
  readonly running_chunks?: number;
  readonly pending_chunks?: number;
  readonly failed_chunks?: number;
  readonly chunks?: readonly VanadisChunkDTO[];
}

export interface BuildsAPIResponse {
  readonly status: string;
  readonly total: number;
  readonly running_count: number;
  readonly queued_count: number;
  readonly completed_count: number;
  readonly builds: readonly VanadisBuildDTO[];
  readonly error?: string;
}

export interface BuildDetailAPIResponse {
  readonly status: string;
  readonly build?: VanadisBuildDTO;
  readonly chunks: readonly VanadisChunkDTO[];
  readonly entry?: VanadisBuildDTO;
  readonly error?: string;
}

// ---- Docket (read-only task tracker, proxied from docket-mcp) ----

/** docket-mcp's TaskSummary: one graph node with its edge ids. */
export interface DocketTaskSummaryDTO {
  readonly id: number;
  readonly title: string;
  readonly status: string;
  readonly parent_id?: number;
  readonly blocked_by?: readonly number[];
  readonly blocks?: readonly number[];
}

/** docket-mcp's ScoredTaskSummary: one semantic search hit. */
export interface DocketScoredTaskDTO {
  readonly id: number;
  readonly title: string;
  readonly status: string;
  readonly parent_id?: number;
  readonly score: number;
}

/** docket-mcp's TaskComment. */
export interface DocketTaskCommentDTO {
  readonly id: number;
  readonly author?: string;
  readonly comment: string;
  readonly created_at: IsoTimestamp;
}

/** docket-mcp's HydratedTask: one task with its neighbours and comments. */
export interface DocketTaskDTO {
  readonly id: number;
  readonly title: string;
  readonly status: string;
  readonly description?: string;
  readonly created_at: IsoTimestamp;
  readonly updated_at: IsoTimestamp;
  readonly closed_at?: IsoTimestamp;
  readonly parent?: DocketTaskSummaryDTO;
  readonly subtasks?: readonly DocketTaskSummaryDTO[];
  readonly blocks?: readonly DocketTaskSummaryDTO[];
  readonly blocked_by?: readonly DocketTaskSummaryDTO[];
  readonly related?: readonly DocketTaskSummaryDTO[];
  readonly comments?: readonly DocketTaskCommentDTO[];
}

/** The `status` query value both list and search accept. */
export type DocketStatusFilter = 'open' | 'completed' | 'wont_do' | 'all';

export interface DocketTasksAPIResponse {
  readonly status: string;
  readonly tasks: readonly DocketTaskSummaryDTO[];
  readonly error?: string;
}

export interface DocketSearchAPIResponse {
  readonly status: string;
  readonly results: readonly DocketScoredTaskDTO[];
  readonly error?: string;
}

export interface DocketTaskDetailAPIResponse {
  readonly status: string;
  readonly task?: DocketTaskDTO;
  readonly error?: string;
}

/**
 * Client-side result of one task-detail fetch: the requested id and its task, or `task: null`
 * when the console answered 404 for it. Carrying the id lets the drawer tell "not found" for this
 * task apart from a stale lookup of the task it showed before.
 */
export interface DocketTaskLookup {
  readonly id: number;
  readonly task: DocketTaskDTO | null;
}
