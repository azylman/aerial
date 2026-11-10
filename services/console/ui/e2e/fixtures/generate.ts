// Regenerates the committed e2e-suite fixtures: `node e2e/fixtures/generate.ts`.
//
// Output is fully deterministic (fixed clock, seeded PRNG) and typed against src/api/types.ts, so
// a wire-type change that the fixtures no longer satisfy fails `npm run typecheck`. The console
// is served these exact payloads by e2e/fixtures.ts.

import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type {
  ActiveTaskStatus,
  BuildDetailAPIResponse,
  BuildsAPIResponse,
  ClusterResponse,
  CronSchedule,
  DeploymentStatus,
  DocketSearchAPIResponse,
  DocketStatusFilter,
  DocketTaskCommentDTO,
  DocketTaskDetailAPIResponse,
  DocketTasksAPIResponse,
  DocketTaskSummaryDTO,
  FactItem,
  FactsAPIResponse,
  OneShotSchedule,
  QuickLaunchLink,
  ScheduleRun,
  ScheduleRunsAPIResponse,
  SchedulesAPIResponse,
  ServiceStatus,
  VanadisBuildDTO,
  VanadisChunkDTO,
} from '../../src/api/types.ts';
import { FIXTURE_NOW } from '../constants.ts';

const NOW = Date.parse(FIXTURE_NOW);
const iso = (offsetSeconds: number): string => new Date(NOW + offsetSeconds * 1000).toISOString();

// mulberry32: tiny seeded PRNG so "random" spreads are stable across runs.
let seed = 0x5eed_c0de;
function rand(): number {
  seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const pick = <T>(items: readonly T[]): T => items[Math.floor(rand() * items.length)] as T;
const int = (min: number, max: number): number => min + Math.floor(rand() * (max - min + 1));
const hex = (n: number): string => Array.from({ length: n }, () => int(0, 15).toString(16)).join('');

// ---- /api/status ----

const SERVICE_NAMES = [
  'eri', 'haro', 'coven', 'vanadis', 'synapse', 'console', 'docs', 'homepage', 'postgres',
  'senses', 'pjev', 'vision', 'prospera', 'discord-mcp', 'docket-mcp', 'github-mcp',
  'kubernetes-mcp', 'kernel-mcp',
] as const;
const DEGRADED = new Set(['vision', 'prospera']);
const DOWN = new Set(['kernel-mcp']);

const services: ServiceStatus[] = SERVICE_NAMES.map((name, i) => ({
  name: `aerial-${name}`,
  status: DOWN.has(name) ? 'down' : DEGRADED.has(name) ? 'degraded' : 'healthy',
  uptime_seconds: DOWN.has(name) ? 0 : 3600 * (i + 1) + 61 * i,
  last_check_time: iso(-5),
}));

const step = (name: string, icon: string, status: string) => ({ name, icon, status });

const deployments: DeploymentStatus[] = [
  {
    id: 'dep-console-1', service: 'console', repository: 'azylman/aerial', commit: 'a1b2c3d',
    commit_msg: 'feat(console/ui): app shell and parity suite', commit_time: iso(-240), stage: 'building', progress: 45,
    steps: [step('CI', '🧪', 'completed'), step('BUILD', '🛠️', 'active'), step('PULL', '⬇️', 'pending'), step('SWAP', '🔄', 'pending')],
    matrix_jobs: [
      { name: 'console', status: 'completed', conclusion: 'success', duration: '48s', type: 'ci' },
      { name: 'lint', status: 'completed', conclusion: 'success', duration: '31s', type: 'ci' },
      { name: 'aerial-console', status: 'active', type: 'container' },
      { name: 'aerial-eri', status: 'pending', type: 'container' },
    ],
    html_url: 'https://github.com/azylman/aerial/actions/runs/1', started_at: iso(-180),
  },
  {
    id: 'dep-eri-1', service: 'eri', repository: 'azylman/aerial', commit: 'b2c3d4e', commit_msg: 'fix(eri): retry transient ingress errors',
    commit_time: iso(-3600), stage: 'live', progress: 100,
    steps: [step('CI', '🧪', 'completed'), step('BUILD', '🛠️', 'completed'), step('PULL', '⬇️', 'completed'), step('SWAP', '🔄', 'completed')],
    started_at: iso(-3500),
  },
  {
    id: 'dep-haro-1', service: 'haro', repository: 'azylman/aerial', commit: 'c3d4e5f', commit_msg: 'feat(haro): heartbeat jitter',
    commit_time: iso(-900), stage: 'awaiting_pull', progress: 70,
    steps: [step('CI', '🧪', 'completed'), step('BUILD', '🛠️', 'completed'), step('PULL', '⬇️', 'active'), step('SWAP', '🔄', 'pending')],
    started_at: iso(-850),
  },
  {
    id: 'dep-coven-1', service: 'coven', repository: 'azylman/aerial', commit: 'd4e5f60', commit_msg: 'refactor(coven): decay batches',
    commit_time: iso(-1800), stage: 'failed', progress: 30,
    steps: [step('CI', '🧪', 'failed'), step('BUILD', '🛠️', 'pending'), step('PULL', '⬇️', 'pending'), step('SWAP', '🔄', 'pending')],
    started_at: iso(-1750),
  },
  {
    id: 'dep-synapse-1', service: 'synapse', repository: 'azylman/aerial', commit: 'e5f6071', commit_msg: 'chore(synapse): bump river',
    commit_time: iso(-300), stage: 'swapping', progress: 90,
    steps: [step('CI', '🧪', 'completed'), step('BUILD', '🛠️', 'completed'), step('PULL', '⬇️', 'completed'), step('SWAP', '🔄', 'active')],
    started_at: iso(-280),
  },
  {
    id: 'dep-docs-1', service: 'docs', repository: 'azylman/aerial-config', commit: 'f607182', commit_msg: 'docs: runbook refresh',
    commit_time: iso(-7200), stage: 'live', progress: 100,
    steps: [step('CI', '🧪', 'completed'), step('BUILD', '🛠️', 'completed'), step('PULL', '⬇️', 'completed'), step('SWAP', '🔄', 'completed')],
    started_at: iso(-7100),
  },
];

const TRIGGERS = ['discord', 'cron', 'reminder', 'http', 'discord'] as const;
const activeTasks: ActiveTaskStatus[] = TRIGGERS.map((trigger, i) => ({
  id: `task-${i + 1}`,
  thread_id: `10000000000000000${i + 1}`,
  session_id: `session-${hex(8)}`,
  author_name: i % 2 === 0 ? 'pilot' : 'scheduler',
  prompt: `Task ${i + 1}: ${pick(['summarise the overnight alerts', 'draft the weekly plan', 'check the build queue', 'review open PRs', 'rotate the staging secrets'])}`,
  summary: i < 3 ? 'working' : 'queued',
  status: i < 3 ? 'PROCESSING' : 'PENDING',
  retry_count: i === 2 ? 1 : 0,
  trigger_type: trigger,
  created_at: iso(-120 * (i + 1)),
  started_at: iso(-60 * (i + 1)),
}));

const quickLaunchLinks: QuickLaunchLink[] = [
  { name: 'Console', url: '/console/', icon: '🛰️', target: '_self', is_core: true },
  { name: 'Grafana', url: '/grafana/', icon: '📈', description: 'Telemetry dashboards', is_core: true },
  { name: 'Docs', url: '/docs/', icon: '📚', is_core: true },
  { name: 'Agentsview', url: '/agentsview/', icon: '🔭', is_core: true },
  { name: 'Homepage', url: '/', icon: '🏠', target: '_self', is_core: true },
  { name: 'Home Assistant', url: 'http://homeassistant.example.test:8123/', icon: '🏡', description: 'Smart home', is_custom: true },
];

const status: ClusterResponse = {
  system_time: FIXTURE_NOW,
  cluster_status: 'degraded',
  active_tasks_count: activeTasks.length,
  active_tasks: activeTasks,
  services,
  deployments,
  quick_launch_links: quickLaunchLinks,
  git_sync: {
    status: 'lagging',
    max_lag_seconds: 75,
    last_sync_time: iso(-75),
    repos: {
      aerial: {
        repo: 'aerial', github_repo: 'azylman/aerial', disk_commit: 'a1b2c3d', disk_commit_time: iso(-400),
        remote_commit: 'b2c3d4e', remote_commit_time: iso(-325), time_lag_seconds: 75, sync_status: 'lagging', last_sync_time: iso(-75),
      },
      'aerial-config': {
        repo: 'aerial-config', github_repo: 'azylman/aerial-config', disk_commit: 'f607182', disk_commit_time: iso(-7200),
        remote_commit: 'f607182', remote_commit_time: iso(-7200), time_lag_seconds: 0, sync_status: 'synced', last_sync_time: iso(-10),
      },
    },
    reconciliation: {
      active: true, state: 'pulling', stage: 'pulling', trigger: 'image_poll', target_services: ['console'],
      commit_sha: 'a1b2c3d', started_at: iso(-30), completed_at: '0001-01-01T00:00:00Z',
    },
  },
  ongoing_deploy: 'console',
  is_deploying: true,
};

// ---- /api/schedules and /api/schedules/runs ----

const CRON_EXPRS = ['0 9 * * *', '30 7 * * 1-5', '0 */2 * * *', '15 18 * * 0', '0 8 1 * *', '*/30 * * * *', '0 22 * * 5'];
const CRON_DESCS = ['Every day at 9:00 AM', 'Weekdays at 7:30 AM', 'Every 2 hours', 'Sundays at 6:15 PM', 'Monthly on the 1st at 8:00 AM', 'Every 30 minutes', 'Fridays at 10:00 PM'];
const ROUTINES = ['Morning standup digest', 'Meal prep planner', 'Weekly review', 'Plant watering', 'Budget check-in', 'Inbox triage', 'Backup audit'];

const crons: CronSchedule[] = Array.from({ length: 28 }, (_, i) => {
  const k = i % CRON_EXPRS.length;
  return {
    id: `cron-${String(i + 1).padStart(2, '0')}`,
    target_id: `20000000000000${String(i).padStart(4, '0')}`,
    channel_id: `30000000000000${String(i % 4).padStart(4, '0')}`,
    title_prefix: `${ROUTINES[k] ?? 'Routine'} ${Math.floor(i / CRON_EXPRS.length) + 1}`,
    cron_expr: CRON_EXPRS[k] ?? '0 9 * * *',
    cron_description: CRON_DESCS[k] ?? '',
    prompt: `Run the ${(ROUTINES[k] ?? 'routine').toLowerCase()} and post a short summary with any action items.`,
    timezone: i % 3 === 0 ? 'UTC' : 'America/Los_Angeles',
    next_run_at: iso(600 * (i + 1) + int(0, 59)),
    enabled: true,
    created_at: iso(-86400 * (i + 3)),
  };
});

const oneShots: OneShotSchedule[] = Array.from({ length: 12 }, (_, i) => ({
  id: `once-${String(i + 1).padStart(2, '0')}`,
  thread_id: `40000000000000${String(i).padStart(4, '0')}`,
  prompt: `Reminder ${i + 1}: ${pick(['take the laundry out', 'call the vet', 'renew the permit', 'check the oven', 'water the basil'])}`,
  // Mix of overdue, imminent, and far-future one-shots so every countdown state appears.
  run_at: iso([-120, 30, 90, 300, 900, 1800, 3600, 7200, 14400, 43200, 86400, 172800][i] ?? 3600),
  created_at: iso(-3600 * (i + 1)),
}));

const schedules: SchedulesAPIResponse = {
  status: 'ok',
  system_time: FIXTURE_NOW,
  summary: { total_active: crons.length + oneShots.length, cron_count: crons.length, one_shot_count: oneShots.length, total_runs_24h: 50, success_rate_24h: 94 },
  crons,
  one_shots: oneShots,
};

const RUN_STATUSES = (i: number): string => (i === 0 ? 'running' : i === 1 ? 'enqueued' : i % 16 === 5 ? 'failed' : 'completed');
const runs: ScheduleRun[] = Array.from({ length: 50 }, (_, i) => {
  const cron = crons[i % crons.length] as CronSchedule;
  const isCron = i % 5 !== 4;
  const statusName = RUN_STATUSES(i);
  const started = -900 * (i + 1);
  const durationMs = statusName === 'running' || statusName === 'enqueued' ? 0 : int(800, 95_000);
  return {
    id: `run-${String(i + 1).padStart(3, '0')}`,
    schedule_id: isCron ? cron.id : `once-${String((i % 12) + 1).padStart(2, '0')}`,
    schedule_type: isCron ? 'cron' : 'one_shot',
    message_id: `50000000000000${String(i).padStart(4, '0')}`,
    target_id: cron.target_id ?? '',
    thread_id: `60000000000000${String(i).padStart(4, '0')}`,
    title: isCron ? cron.title_prefix : `Reminder ${(i % 12) + 1}`,
    prompt: isCron ? cron.prompt : `Reminder ${(i % 12) + 1}`,
    status: statusName,
    started_at: iso(started),
    ...(durationMs > 0 ? { completed_at: iso(started + Math.round(durationMs / 1000)) } : {}),
    duration_ms: durationMs,
    ...(statusName === 'failed' ? { error: 'agy exited with status 1: context deadline exceeded' } : {}),
  };
});

const scheduleRuns: ScheduleRunsAPIResponse = { status: 'ok', total: runs.length, limit: 50, offset: 0, runs };

// ---- /api/facts ----

const FACT_TEMPLATES = [
  'Prefers oat milk in lattes',
  'The kiosk display sleeps at 11 PM',
  'Weekly meal prep happens on Sundays',
  'Grafana alerts route to the system channel',
  'The garden irrigation runs at 6 AM',
  'Uses dark mode everywhere',
  'Avoids cumin in recipes',
  'The NAS backup completes around 3 AM',
  'Standup digest should stay under 200 words',
  'Builds ship through the Vanadis queue',
];

const facts: FactItem[] = Array.from({ length: 150 }, (_, i) => {
  const reinforce = i % 3 === 0 ? int(2, 12) : int(0, 1);
  return {
    id: 1000 + i,
    fact_text: `${FACT_TEMPLATES[i % FACT_TEMPLATES.length] ?? 'Fact'} (#${i + 1})`,
    reinforce_count: reinforce,
    ...(reinforce >= 2 ? { last_reinforced_at: iso(-3600 * int(1, 240)) } : {}),
    created_at: iso(-86400 * int(1, 365)),
  };
});

const factsResponse: FactsAPIResponse = { facts, total: facts.length, limit: 500, offset: 0, status: 'ok' };

// ---- /api/builds and /api/builds/{id} ----

type BuildPlan = { readonly status: string; readonly chunks: readonly string[] };
const BUILD_PLANS: readonly BuildPlan[] = [
  ...Array.from({ length: 4 }, (_, i): BuildPlan => ({ status: 'running', chunks: ['merged', 'merged', 'in_progress', ...Array.from({ length: i }, () => 'pending')] })),
  { status: 'running', chunks: ['pending', 'pending', 'pending'] }, // running but no chunk started: reads as queued
  ...Array.from({ length: 5 }, (_, i): BuildPlan => ({ status: 'queued', chunks: Array.from({ length: 2 + i }, () => 'pending') })),
  ...Array.from({ length: 10 }, (_, i): BuildPlan => ({ status: 'completed', chunks: Array.from({ length: 2 + (i % 5) }, () => 'merged') })),
  ...Array.from({ length: 3 }, (): BuildPlan => ({ status: 'failed', chunks: ['merged', 'failed', 'pending'] })),
  ...Array.from({ length: 2 }, (): BuildPlan => ({ status: 'cancelled', chunks: ['merged', 'pending'] })),
];

const REPOS = ['azylman/aerial', 'azylman/aerial-config', 'azylman/mirrormere', 'azylman/aerial-sidecars'];
const OBJECTIVES = ['Console React rewrite', 'Scheduler heartbeat jitter', 'Kiosk widget refresh', 'Memory decay tuning', 'Synapse webhook hardening'];

const buildEntries: VanadisBuildDTO[] = BUILD_PLANS.map((plan, b) => {
  const id = `build-${1794100000 + b * 1111}-${hex(8)}`;
  const created = -3600 * (b + 1);
  const chunks: VanadisChunkDTO[] = plan.chunks.map((chunkStatus, c) => {
    const started = chunkStatus === 'pending' ? '0001-01-01T00:00:00Z' : iso(created + 600 * (c + 1));
    const done = chunkStatus === 'merged' || chunkStatus === 'failed';
    return {
      id: `${id}-chunk-${c + 1}`,
      build_id: id,
      chunk_index: c,
      repo: REPOS[(b + c) % REPOS.length] as string,
      title: `${['Scaffold', 'Port logic', 'Shell', 'Views', 'Drawer', 'Cutover'][c] ?? 'Chunk'} for ${OBJECTIVES[b % OBJECTIVES.length] ?? 'build'}`,
      instructions: `## Objective\n\nImplement chunk ${c + 1}.\n\n- keep the **budget**\n- add \`tests\`\n\n\`\`\`ts\nconst ok = true;\n\`\`\``,
      target_files: [`services/console/ui/src/file${c + 1}.ts`, 'services/console/ui/package.json'],
      target_symbols: [`symbol${c + 1}`],
      effort: pick(['low', 'medium', 'high']),
      acceptance_criteria: ['Tests pass', 'Coverage stays at 95%'],
      status: chunkStatus,
      ...(chunkStatus === 'merged' ? { pr_number: 1500 + b * 10 + c } : {}),
      started_at: started,
      completed_at: done ? iso(created + 600 * (c + 1) + 480) : '0001-01-01T00:00:00Z',
      created_at: iso(created),
      updated_at: done ? iso(created + 600 * (c + 1) + 480) : iso(created + 60),
    };
  });
  const count = (s: string): number => plan.chunks.filter((x) => x === s).length;
  return {
    id,
    thread_id: `70000000000000${String(b).padStart(4, '0')}`,
    tier: (b % 3) + 1,
    title: `${OBJECTIVES[b % OBJECTIVES.length] ?? 'Build'} #${b + 1}`,
    objective: `Ship ${(OBJECTIVES[b % OBJECTIVES.length] ?? 'the build').toLowerCase()} with tests and docs.`,
    shared_context: 'Layout: services/console/ui is a standalone Vite project.',
    status: plan.status,
    created_at: iso(created),
    updated_at: iso(created + 1800),
    queue_state: plan.status === 'running' && count('pending') === plan.chunks.length ? 'queued' : plan.status,
    total_chunks: plan.chunks.length,
    completed_chunks: count('merged'),
    running_chunks: count('in_progress'),
    pending_chunks: count('pending'),
    failed_chunks: count('failed'),
    chunks,
  };
});

const builds: BuildsAPIResponse = {
  status: 'ok',
  total: buildEntries.length,
  running_count: 4,
  queued_count: 6,
  completed_count: 15,
  builds: buildEntries,
};

const buildDetails: Record<string, BuildDetailAPIResponse> = Object.fromEntries(
  buildEntries.map((b) => {
    const { chunks = [], ...build } = b;
    return [b.id, { status: 'ok', build, chunks }];
  }),
);

// ---- /api/docket/tasks, /api/docket/tasks/{id}, /api/docket/search ----
//
// 30 tasks, ids 201-230, built without the PRNG so they never shift the fixtures above. Status:
// every i % 5 === 3 is completed (6), every i % 10 === 9 is won't-do (3), the other 21 are open.
// Tasks 207-230 are subtasks of 201-206 (parent 201 + i % 6); blocks and related edges are listed.

const DOCKET_TOPICS = ['Kiosk widget refresh', 'Scheduler heartbeat jitter', 'Memory decay tuning', 'Console task tab', 'Webhook retry budget', 'Backup audit trail'];
const DOCKET_BLOCKS: readonly (readonly [number, number])[] = [
  [207, 208],
  [207, 209],
  [208, 210],
  [213, 214],
  [219, 214],
  [221, 222],
  [225, 226],
];
const DOCKET_RELATED: readonly (readonly [number, number])[] = [
  [201, 213],
  [207, 219],
  [210, 222],
];

interface DocketNode {
  readonly id: number;
  readonly title: string;
  readonly status: string;
  readonly parentId: number | undefined;
  readonly created: number;
}

const docketNodes: DocketNode[] = Array.from({ length: 30 }, (_, i) => ({
  id: 201 + i,
  title: `${DOCKET_TOPICS[i % 6] as string} step ${Math.floor(i / 6) + 1}`,
  status: i % 5 === 3 ? 'completed' : i % 10 === 9 ? 'wont_do' : 'open',
  parentId: i >= 6 ? 201 + (i % 6) : undefined,
  created: -86400 * (30 - i),
}));
const docketById = new Map(docketNodes.map((n) => [n.id, n]));
const edgeIds = (edges: readonly (readonly [number, number])[], from: 0 | 1, id: number): number[] =>
  edges.filter((e) => e[from] === id).map((e) => e[from === 0 ? 1 : 0]);

function docketSummary(n: DocketNode): DocketTaskSummaryDTO {
  const blockedBy = edgeIds(DOCKET_BLOCKS, 1, n.id);
  const blocks = edgeIds(DOCKET_BLOCKS, 0, n.id);
  return {
    id: n.id,
    title: n.title,
    status: n.status,
    ...(n.parentId === undefined ? {} : { parent_id: n.parentId }),
    ...(blockedBy.length > 0 ? { blocked_by: blockedBy } : {}),
    ...(blocks.length > 0 ? { blocks } : {}),
  };
}
const summaryOf = (id: number): DocketTaskSummaryDTO => docketSummary(docketById.get(id) as DocketNode);

const DOCKET_STATUSES: readonly DocketStatusFilter[] = ['open', 'completed', 'wont_do', 'all'];
const docketTasks: Record<string, DocketTasksAPIResponse> = Object.fromEntries(
  DOCKET_STATUSES.map((status) => [
    status,
    { status: 'ok', tasks: docketNodes.filter((n) => status === 'all' || n.status === status).map(docketSummary) },
  ]),
);

function docketComments(n: DocketNode, i: number): DocketTaskCommentDTO[] {
  if (i % 4 !== 0) return [];
  return [
    { id: n.id * 10 + 1, author: 'aerial', comment: `Started on **${n.title}**.`, created_at: iso(-7200) },
    { id: n.id * 10 + 2, author: 'operator', comment: 'Looks good, ship it with `tests`.', created_at: iso(-1800) },
  ];
}

const docketDetails: Record<string, DocketTaskDetailAPIResponse> = Object.fromEntries(
  docketNodes.map((n, i) => {
    const subtasks = docketNodes.filter((c) => c.parentId === n.id).map(docketSummary);
    const blocks = edgeIds(DOCKET_BLOCKS, 0, n.id).map(summaryOf);
    const blockedBy = edgeIds(DOCKET_BLOCKS, 1, n.id).map(summaryOf);
    const related = [...edgeIds(DOCKET_RELATED, 0, n.id), ...edgeIds(DOCKET_RELATED, 1, n.id)].map(summaryOf);
    const comments = docketComments(n, i);
    return [
      String(n.id),
      {
        status: 'ok',
        task: {
          id: n.id,
          title: n.title,
          status: n.status,
          description: `## Goal\n\nDeliver ${n.title.toLowerCase()}.\n\n- keep it **read-only**\n- add \`tests\``,
          created_at: iso(n.created),
          updated_at: iso(n.created + 3600),
          ...(n.status === 'open' ? {} : { closed_at: iso(n.created + 7200) }),
          ...(n.parentId === undefined ? {} : { parent: summaryOf(n.parentId) }),
          ...(subtasks.length > 0 ? { subtasks } : {}),
          ...(blocks.length > 0 ? { blocks } : {}),
          ...(blockedBy.length > 0 ? { blocked_by: blockedBy } : {}),
          ...(related.length > 0 ? { related } : {}),
          ...(comments.length > 0 ? { comments } : {}),
        },
      },
    ];
  }),
);

// "kiosk" over open tasks: the four open Kiosk widget refresh steps, best match first.
const docketSearch: Record<string, DocketSearchAPIResponse> = {
  'q=kiosk&status=open': {
    status: 'ok',
    results: [213, 201, 225, 207].map((id, rank) => {
      const n = docketById.get(id) as DocketNode;
      const score = Math.round((0.92 - rank * 0.07) * 100) / 100;
      return { id, title: n.title, status: n.status, ...(n.parentId === undefined ? {} : { parent_id: n.parentId }), score };
    }),
  },
};

const OUT = import.meta.dirname;
const write = (name: string, value: unknown): void => writeFileSync(join(OUT, name), `${JSON.stringify(value, null, 2)}\n`);

write('status.json', status);
write('schedules.json', schedules);
write('schedule-runs.json', scheduleRuns);
write('facts.json', factsResponse);
write('builds.json', builds);
write('build-details.json', buildDetails);
write('docket-tasks.json', docketTasks);
write('docket-task-details.json', docketDetails);
write('docket-search.json', docketSearch);
