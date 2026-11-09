import { describe, expect, it } from 'vitest';
import type { DeploymentStatus, MatrixJobChip, ServiceStatus } from '../../api/types';
import {
  compactDeployModel,
  deployBadge,
  deployCardModel,
  deploySteps,
  findServiceByName,
  healthyCountText,
  isCompactDeploy,
} from './model';

// Wall-clock fixtures are built from local-time components so expectations hold in any TZ.
const localIso = (h: number, mi: number, s: number): string => new Date(2026, 10, 8, h, mi, s).toISOString();

const svc = (name: string, status: string): ServiceStatus => ({ name, status, uptime_seconds: 60, last_check_time: '' });
const SERVICES = [svc('aerial-eri', 'healthy'), svc('aerial-vision', 'degraded'), svc('aerial-kernel-mcp', 'down'), svc('aerial-console', 'healthy')];

const dep = (over: Partial<DeploymentStatus>): DeploymentStatus => ({
  id: 'd',
  service: 'console',
  commit: 'a1b2c3d',
  stage: 'building',
  progress: 45,
  steps: null,
  started_at: localIso(11, 57, 0),
  ...over,
});
const stages = (...list: string[]): DeploymentStatus[] => list.map((stage, i) => dep({ id: `d${i}`, stage }));
const chip = (name: string, status: string, over: Partial<MatrixJobChip> = {}): MatrixJobChip => ({ name, status, ...over });

describe('healthyCountText', () => {
  it.each([
    { name: 'before the first poll', services: null, want: '0 / 0 HEALTHY' },
    { name: 'empty snapshot', services: [], want: '0 / 0' },
    { name: 'mixed states', services: SERVICES, want: '2 / 4' },
  ])('$name', ({ services, want }) => {
    expect(healthyCountText(services)).toBe(want);
  });
});

describe('findServiceByName', () => {
  it.each([
    { name: 'exact name', services: SERVICES, query: 'aerial-vision', want: 'aerial-vision' },
    { name: 'prefix dropped and case folded', services: SERVICES, query: 'VISION', want: 'aerial-vision' },
    { name: 'container chip name', services: SERVICES, query: 'aerial-console', want: 'aerial-console' },
    { name: 'substring match', services: SERVICES, query: 'kernel', want: 'aerial-kernel-mcp' },
    { name: 'no match', services: SERVICES, query: 'lint', want: null },
    { name: 'no snapshot', services: null, query: 'eri', want: null },
  ])('$name', ({ services, query, want }) => {
    expect(findServiceByName(services, query)?.name ?? null).toBe(want);
  });
});

describe('deployBadge', () => {
  it.each([
    { name: 'failed wins', deploys: stages('failed', 'swapping'), want: { icon: 'alert', text: 'CI BUILD FAILED', className: 'section-badge failed' } },
    { name: 'degraded', deploys: stages('degraded', 'live'), want: { icon: 'alert', text: 'STACK DEGRADED', className: 'section-badge failed' } },
    { name: 'several active', deploys: stages('building', 'pulling'), want: { icon: null, text: '2 DEPLOYS IN PROGRESS', className: 'section-badge active' } },
    { name: 'swapping', deploys: stages('swapping', 'live'), want: { icon: 'refresh', text: 'FLUX SWAPPING', className: 'section-badge swapping' } },
    { name: 'pulling', deploys: stages('pulling'), want: { icon: 'download', text: 'FLUX PULLING', className: 'section-badge pulling' } },
    { name: 'one build', deploys: stages('queued', 'completed'), want: { icon: 'bolt', text: '1 CI BUILD ACTIVE', className: 'section-badge building' } },
    { name: 'awaiting pull', deploys: stages('awaiting_pull'), want: { icon: 'download', text: 'AWAITING FLUX SYNC', className: 'section-badge active' } },
    { name: 'other active stage', deploys: stages('verifying', 'live'), want: { icon: null, text: '1 IN PROGRESS', className: 'section-badge active' } },
    { name: 'all live', deploys: stages('live', 'completed'), want: { icon: null, text: 'ALL SERVICES LIVE (STACK SYNCED)', className: 'section-badge live' } },
    { name: 'nothing deploying', deploys: [], want: { icon: null, text: 'SYSTEM IN SYNC', className: 'section-badge' } },
  ])('$name', ({ deploys, want }) => {
    expect(deployBadge(deploys)).toEqual(want);
  });

});

describe('isCompactDeploy', () => {
  it.each([
    { stage: 'building', index: 0, total: 6, want: false },
    { stage: 'live', index: 0, total: 1, want: false },
    { stage: 'live', index: 1, total: 6, want: true },
    { stage: 'failed', index: 1, total: 6, want: false },
    { stage: 'failed', index: 2, total: 6, want: true },
  ])('$stage at $index of $total -> $want', ({ stage, index, total, want }) => {
    expect(isCompactDeploy(stage, index, total)).toBe(want);
  });
});

describe('compactDeployModel', () => {
  it.each([
    {
      name: 'live with a message',
      dep: dep({ stage: 'live', service: 'eri', commit: 'b2c3d4e', commit_msg: 'fix(eri): retry' }),
      want: { className: 'deploy-card-compact stage-live', commit: 'b2c3d4e', title: 'eri • fix(eri): retry', badgeClass: 'compact-badge live', badgeIcon: 'check', badgeText: 'LIVE' },
    },
    {
      name: 'degraded',
      dep: dep({ stage: 'degraded' }),
      want: { className: 'deploy-card-compact stage-degraded', commit: 'a1b2c3d', title: 'console', badgeClass: 'compact-badge failed', badgeIcon: 'x', badgeText: 'FAILED' },
    },
    {
      name: 'swapping',
      dep: dep({ stage: 'swapping' }),
      want: { className: 'deploy-card-compact stage-swapping', commit: 'a1b2c3d', title: 'console', badgeClass: 'compact-badge swapping', badgeIcon: null, badgeText: 'SWAPPING' },
    },
    {
      name: 'pulling',
      dep: dep({ stage: 'pulling' }),
      want: { className: 'deploy-card-compact stage-pulling', commit: 'a1b2c3d', title: 'console', badgeClass: 'compact-badge pulling', badgeIcon: null, badgeText: 'PULLING' },
    },
    {
      name: 'missing fields fall back',
      dep: dep({ stage: '', service: '', commit: '' }),
      want: { className: 'deploy-card-compact stage-active', commit: 'latest', title: 'aerial-stack', badgeClass: 'compact-badge active', badgeIcon: null, badgeText: 'ACTIVE' },
    },
  ])('$name', ({ dep: d, want }) => {
    expect(compactDeployModel(d)).toEqual(want);
  });
});

const CHIPS = [
  chip('console', 'completed', { type: 'ci', duration: '48s' }),
  chip('lint', 'failed'),
  chip('aerial-console', 'active', { type: 'container' }),
  chip('aerial-eri', 'pending', { type: 'container' }),
];
const NAMED_STEPS = [
  { name: 'Commit Trigger', icon: '📦', status: 'completed' },
  { name: 'CI Build & GHCR', icon: '⚙️', status: 'completed' },
  { name: 'Container Swap', icon: '🔄', status: 'active' },
];

describe('deploySteps', () => {
  it.each([
    {
      name: 'fixture-style steps get no chips',
      dep: dep({ steps: [{ name: 'CI', icon: '🧪', status: 'completed' }, { name: 'BUILD', icon: '🛠️', status: 'failed' }], matrix_jobs: CHIPS }),
      want: [
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-failed', 'x', 'FAILED', []],
      ],
    },
    {
      name: 'building: CI chips on the CI step, container chips held back',
      dep: dep({ stage: 'building', steps: NAMED_STEPS, matrix_jobs: CHIPS }),
      want: [
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', ['matrix-chip chip-done', 'matrix-chip chip-failed']],
        ['step-panel step-active', 'bolt', 'RUNNING', []],
      ],
    },
    {
      name: 'swapping: container chips appear on Container Swap',
      dep: dep({ stage: 'swapping', steps: NAMED_STEPS, matrix_jobs: CHIPS }),
      want: [
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', ['matrix-chip chip-done', 'matrix-chip chip-failed']],
        ['step-panel step-active', 'bolt', 'RUNNING', ['matrix-chip chip-running', 'matrix-chip chip-queued']],
      ],
    },
    {
      name: 'no steps: the default live pipeline',
      dep: dep({ stage: 'live' }),
      want: [
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', []],
      ],
    },
    {
      name: 'no steps: the default in-flight pipeline',
      dep: dep({ stage: 'swapping' }),
      want: [
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-completed', 'check', 'DONE', []],
        ['step-panel step-active', 'bolt', 'RUNNING', []],
        ['step-panel step-pending', 'circle', 'PENDING', []],
      ],
    },
  ])('$name', ({ dep: d, want }) => {
    expect(deploySteps(d).map((s) => [s.className, s.statusIcon, s.statusText, s.chips.map((c) => c.className)])).toEqual(want);
  });

  it('labels chips like the vanilla chip template', () => {
    const [, ci] = deploySteps(dep({ steps: NAMED_STEPS, matrix_jobs: [chip('console', 'completed', { type: 'ci', duration: '48s' })] }));
    expect(ci?.chips).toEqual([
      { name: 'console', className: 'matrix-chip chip-done', icon: 'check', label: 'console (48s)', title: 'console: completed (48s) • Click for diagnostics' },
    ]);
  });
});

describe('deployCardModel', () => {
  const started = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit', second: '2-digit' }).format(new Date(2026, 10, 8, 11, 57, 0));
  it.each([
    {
      name: 'building with every link',
      dep: dep({
        repository: 'azylman/aerial',
        commit_msg: 'feat: shell',
        commit_time: localIso(11, 56, 0),
        html_url: 'https://github.com/azylman/aerial/actions/runs/1',
      }),
      want: {
        className: 'deploy-card stage-building',
        inFlight: true,
        repo: 'azylman/aerial',
        commit: 'a1b2c3d',
        commitUrl: 'https://github.com/azylman/aerial/commit/a1b2c3d',
        commitTime: { text: 'Nov 8, 11:56 AM', iso: localIso(11, 56, 0) },
        runUrl: 'https://github.com/azylman/aerial/actions/runs/1',
        timerStart: localIso(11, 57, 0),
        badgeClass: 'deploy-stage-badge active',
        stageText: 'BUILDING',
        progress: 45,
        footer: '"feat: shell"',
        started,
      },
    },
    {
      name: 'live, unsafe repository and non-SHA commit',
      dep: dep({ stage: 'live', progress: 100, repository: 'bad repo', commit: 'latest-tag', html_url: 'http://evil.test/' }),
      want: {
        className: 'deploy-card stage-live',
        inFlight: false,
        repo: 'bad repo',
        commit: 'latest-tag',
        commitUrl: null,
        commitTime: { text: 'Nov 8, 11:57 AM', iso: localIso(11, 57, 0) },
        runUrl: null,
        timerStart: null,
        badgeClass: 'deploy-stage-badge live',
        stageText: 'LIVE',
        progress: 100,
        footer: 'STAGE: LIVE (100%)',
        started,
      },
    },
    {
      name: 'default repository for SHA links',
      dep: dep({ stage: 'failed', progress: Number.NaN, commit: 'd4e5f60', started_at: '' }),
      want: {
        className: 'deploy-card stage-failed',
        inFlight: false,
        repo: null,
        commit: 'd4e5f60',
        commitUrl: 'https://github.com/azylman/aerial/commit/d4e5f60',
        commitTime: null,
        runUrl: null,
        timerStart: null,
        badgeClass: 'deploy-stage-badge failed',
        stageText: 'FAILED',
        progress: 0,
        footer: 'STAGE: FAILED (0%)',
        started: 'RECENT',
      },
    },
  ])('$name', ({ dep: d, want }) => {
    expect(deployCardModel(d)).toEqual(want);
  });

  it.each([
    { stage: 'swapping', className: 'deploy-card stage-swapping', badgeClass: 'deploy-stage-badge swapping', inFlight: true },
    { stage: 'pulling', className: 'deploy-card stage-pulling', badgeClass: 'deploy-stage-badge pulling', inFlight: true },
    { stage: 'awaiting_pull', className: 'deploy-card stage-building', badgeClass: 'deploy-stage-badge active', inFlight: true },
    { stage: 'degraded', className: 'deploy-card stage-failed', badgeClass: 'deploy-stage-badge failed', inFlight: false },
    { stage: 'verifying', className: 'deploy-card stage-active', badgeClass: 'deploy-stage-badge active', inFlight: false },
  ])('$stage stage classes', ({ stage, className, badgeClass, inFlight }) => {
    expect(deployCardModel(dep({ stage }))).toMatchObject({ className, badgeClass, inFlight });
  });

  it('reports an unparseable start like toLocaleTimeString', () => {
    expect(deployCardModel(dep({ started_at: 'garbage' })).started).toBe('Invalid Date');
  });
});
