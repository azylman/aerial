// One-line DTO factories shared by the console view tests. Defaults are the ones the view tests
// were written against; callers pass overrides for anything a row depends on.
import type {
  ActiveTaskStatus,
  CronSchedule,
  DocketTaskSummaryDTO,
  FactItem,
  OneShotSchedule,
  ScheduleRun,
  ServiceStatus,
  VanadisBuildDTO,
  VanadisChunkDTO,
} from '../api/types';

/** An instant at local wall-clock time on 2026-11-08, so rendered times read the same in any TZ. */
export const at = (h: number, mi: number, sec = 0): string => new Date(2026, 10, 8, h, mi, sec).toISOString();

/** Go's zero time, which the API sends for unset timestamps. */
export const ZERO_TIME = '0001-01-01T00:00:00Z';

export const cron = (id: string, over: Partial<CronSchedule> = {}): CronSchedule => ({
  id,
  target_id: `2000${id}`,
  channel_id: `3000${id}`,
  title_prefix: `Routine ${id}`,
  cron_expr: '0 9 * * *',
  cron_description: 'Every day at 9:00 AM',
  prompt: `Run routine ${id}`,
  timezone: 'UTC',
  next_run_at: at(12, 10),
  enabled: true,
  created_at: '2026-11-01T00:00:00Z',
  ...over,
});

export const once = (id: string, over: Partial<OneShotSchedule> = {}): OneShotSchedule => ({
  id,
  thread_id: `4000000000${id.length}`,
  prompt: `Reminder ${id}`,
  run_at: at(12, 30),
  created_at: at(0, 0),
  ...over,
});

export const run = (id: string, over: Partial<ScheduleRun> = {}): ScheduleRun => ({
  id,
  schedule_id: 'cron-a',
  schedule_type: 'cron',
  title: `Run ${id}`,
  target_id: '200',
  thread_id: '600000000000000000',
  prompt: `Prompt ${id}`,
  status: 'completed',
  started_at: at(11, 0),
  duration_ms: 25564,
  ...over,
});

export const chunk = (id: string, index: number, status: string, over: Partial<VanadisChunkDTO> = {}): VanadisChunkDTO => ({
  id,
  build_id: 'b',
  chunk_index: index,
  title: `Chunk ${id}`,
  status,
  started_at: ZERO_TIME,
  completed_at: ZERO_TIME,
  created_at: at(10, 0),
  updated_at: at(10, 0),
  ...over,
});

/** A queue entry; `chunks: undefined` leaves the field out, as the list API does for chunk-less builds. */
export const build = (
  id: string,
  status: string,
  title: string,
  chunks: readonly VanadisChunkDTO[] | undefined,
  over: Partial<VanadisBuildDTO> = {},
): VanadisBuildDTO => ({
  id,
  thread_id: 't',
  tier: 2,
  title,
  objective: `Ship ${title.toLowerCase()}`,
  status,
  created_at: at(10, 0),
  updated_at: at(10, 30),
  ...(chunks === undefined ? {} : { chunks }),
  ...over,
});

export const fact = (id: number, text: string, count: number, last?: string): FactItem => ({
  id,
  fact_text: text,
  reinforce_count: count,
  created_at: '2026-11-06T12:00:00Z',
  ...(last === undefined ? {} : { last_reinforced_at: last }),
});

export const svc = (name: string, status: string, uptime: number): ServiceStatus => ({
  name,
  status,
  uptime_seconds: uptime,
  last_check_time: '2026-11-08T12:00:00Z',
});

export const node = (id: number, status = 'open', over: Partial<DocketTaskSummaryDTO> = {}): DocketTaskSummaryDTO => ({
  id,
  title: `Task ${id}`,
  status,
  ...over,
});

export const task = (id: string, over: Partial<ActiveTaskStatus> = {}): ActiveTaskStatus => ({
  id,
  thread_id: `10000000000000000${id}`,
  session_id: `session-${id}`,
  author_name: 'pilot',
  prompt: `Task ${id}`,
  summary: 'working',
  status: 'PROCESSING',
  retry_count: 0,
  trigger_type: 'discord',
  created_at: '2026-11-08T11:58:00Z',
  started_at: '2026-11-08T11:59:00Z',
  ...over,
});
