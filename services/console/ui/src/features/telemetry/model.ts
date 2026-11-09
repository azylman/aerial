import type { DeploymentStatus, DeploymentStep, MatrixJobChip, ServiceStatus } from '../../api/types';
import type { IconName } from '../../components/Icon';
import { formatCommitTimestamp, parseValidTimestampMs } from '../../lib/formatters';
import type { BadgeModel } from '../tasks/model';

// Pure view models for the telemetry view, ported from renderDeployments, renderServicesGrid and
// openDiagnosticDrawerByName in static/app.js.

// ---- Services ----

/** `healthy / total` for the ACTIVE STACK COMPONENTS badge (renderServicesGrid's active-count). */
export function healthyCountText(services: readonly ServiceStatus[] | null): string {
  if (services === null) return '0 / 0 HEALTHY';
  const healthy = services.filter((s) => s.status === 'healthy').length;
  return `${healthy} / ${services.length}`;
}

const stripPrefix = (name: string): string => name.toLowerCase().replace(/^aerial-/, '');

/**
 * Port of openDiagnosticDrawerByName: matches case-insensitively with the `aerial-` prefix
 * dropped, exact name first in list order, else the first service whose name contains it.
 */
export function findServiceByName(services: readonly ServiceStatus[] | null, name: string): ServiceStatus | null {
  const clean = stripPrefix(name);
  return services?.find((s) => {
    const candidate = stripPrefix(s.name || '');
    return candidate === clean || candidate.includes(clean);
  }) ?? null;
}

// ---- Deployments ----

const isBuildingStage = (stage: string): boolean => stage === 'building' || stage === 'queued';

/** Header badge for DEPLOYMENT PIPELINE: the first matching rule of renderDeployments wins. */
export function deployBadge(deploys: readonly DeploymentStatus[]): BadgeModel {
  const has = (stage: string): boolean => deploys.some((d) => d.stage === stage);
  const active = deploys.filter((d) => d.stage !== 'live' && d.stage !== 'completed').length;
  if (has('failed')) return { icon: 'alert', text: 'CI BUILD FAILED', className: 'section-badge failed' };
  if (has('degraded')) return { icon: 'alert', text: 'STACK DEGRADED', className: 'section-badge failed' };
  if (active > 1) return { icon: null, text: `${active} DEPLOYS IN PROGRESS`, className: 'section-badge active' };
  if (has('swapping')) return { icon: 'refresh', text: 'FLUX SWAPPING', className: 'section-badge swapping' };
  if (has('pulling')) return { icon: 'download', text: 'FLUX PULLING', className: 'section-badge pulling' };
  // Two or more building deploys already matched "N DEPLOYS IN PROGRESS" above, so a build that
  // reaches this rule is the only one (the vanilla plural branch here is unreachable).
  if (deploys.some((d) => isBuildingStage(d.stage))) {
    return { icon: 'bolt', text: '1 CI BUILD ACTIVE', className: 'section-badge building' };
  }
  if (has('awaiting_pull')) return { icon: 'download', text: 'AWAITING FLUX SYNC', className: 'section-badge active' };
  if (active > 0) return { icon: null, text: `${active} IN PROGRESS`, className: 'section-badge active' };
  if (deploys.length > 0) return { icon: null, text: 'ALL SERVICES LIVE (STACK SYNCED)', className: 'section-badge live' };
  return { icon: null, text: 'SYSTEM IN SYNC', className: 'section-badge' };
}

/** Cards past the first two, and live cards when several deploys exist, render as compact rows. */
export const isCompactDeploy = (stage: string, index: number, total: number): boolean =>
  index >= 2 || (stage === 'live' && total > 1);

export interface CompactDeployModel {
  readonly className: string;
  readonly commit: string;
  readonly title: string;
  readonly badgeClass: string;
  readonly badgeIcon: IconName | null;
  readonly badgeText: string;
}

export function compactDeployModel(dep: DeploymentStatus): CompactDeployModel {
  const live = dep.stage === 'live';
  const failed = dep.stage === 'failed' || dep.stage === 'degraded';
  return {
    className: `deploy-card-compact stage-${dep.stage || 'active'}`,
    commit: dep.commit || 'latest',
    title: `${dep.service || 'aerial-stack'}${dep.commit_msg ? ` • ${dep.commit_msg}` : ''}`,
    badgeClass: `compact-badge ${live ? 'live' : failed ? 'failed' : dep.stage === 'swapping' ? 'swapping' : dep.stage === 'pulling' ? 'pulling' : 'active'}`,
    badgeIcon: live ? 'check' : failed ? 'x' : null,
    badgeText: live ? 'LIVE' : failed ? 'FAILED' : (dep.stage || 'ACTIVE').toUpperCase(),
  };
}

export interface ChipModel {
  readonly name: string;
  readonly className: string;
  readonly icon: IconName;
  readonly label: string;
  readonly title: string;
}

const CHIP_STYLE: Readonly<Record<string, { readonly css: string; readonly icon: IconName }>> = {
  completed: { css: 'chip-done', icon: 'check' },
  active: { css: 'chip-running', icon: 'bolt' },
  failed: { css: 'chip-failed', icon: 'x' },
};
const CHIP_QUEUED = { css: 'chip-queued', icon: 'circle' } as const;

function chipModel(chip: MatrixJobChip): ChipModel {
  const style = CHIP_STYLE[chip.status] ?? CHIP_QUEUED;
  const dur = chip.duration ? ` (${chip.duration})` : '';
  return {
    name: chip.name,
    className: `matrix-chip ${style.css}`,
    icon: style.icon,
    label: `${chip.name}${dur}`,
    title: `${chip.name}: ${chip.status}${dur} • Click for diagnostics`,
  };
}

const isCiName = (n: string): boolean => n.includes('test') || n.includes('lint') || n.includes('build') || n.includes('Build');

export interface StepModel {
  readonly key: string;
  readonly name: string;
  readonly icon: string;
  readonly className: string;
  readonly statusIcon: IconName;
  readonly statusText: string;
  readonly chips: readonly ChipModel[];
}

const STEP_STATUS: Readonly<Record<string, { readonly icon: IconName; readonly text: string }>> = {
  completed: { icon: 'check', text: 'DONE' },
  active: { icon: 'bolt', text: 'RUNNING' },
  failed: { icon: 'x', text: 'FAILED' },
};
const STEP_PENDING = { icon: 'circle', text: 'PENDING' } as const;

const EMPTY_CHIPS: readonly ChipModel[] = Object.freeze([]);

function defaultSteps(live: boolean): readonly DeploymentStep[] {
  return [
    { name: 'Commit Trigger', icon: '📦', status: 'completed' },
    { name: 'CI Build & GHCR', icon: '⚙️', status: 'completed' },
    { name: 'Flux Sync', icon: '⬇️', status: 'completed' },
    { name: 'Container Swap', icon: '🔄', status: live ? 'completed' : 'active' },
    { name: 'Health Check', icon: '🩺', status: live ? 'completed' : 'pending' },
  ];
}

/**
 * Pipeline steps with their matrix chips. CI chips (typed `ci`, or untyped with a test/lint/build
 * name) hang off the `CI Build` step; container chips hang off `Container Swap` once the deploy
 * reaches the host phase (pulling, swapping, live or degraded).
 */
export function deploySteps(dep: DeploymentStatus): readonly StepModel[] {
  const stage = dep.stage;
  const hostPhase = stage === 'pulling' || stage === 'swapping' || stage === 'live' || stage === 'degraded';
  const chips = dep.matrix_jobs ?? [];
  const ci = chips.filter((c) => c.type === 'ci' || (!c.type && c.name && isCiName(c.name))).map(chipModel);
  const container = chips
    .filter((c) => c.type === 'container' || (!c.type && c.name && !c.name.includes('test') && !c.name.includes('lint')))
    .map(chipModel);
  return (dep.steps ?? defaultSteps(stage === 'live')).map((step, i) => {
    const status = STEP_STATUS[step.status] ?? STEP_PENDING;
    const name = step.name || '';
    const stepChips = name.includes('CI Build') ? ci : name.includes('Container Swap') && hostPhase ? container : EMPTY_CHIPS;
    return {
      key: `${i}:${name}`,
      name: step.name,
      icon: step.icon,
      className: `step-panel step-${step.status}`,
      statusIcon: status.icon,
      statusText: status.text,
      chips: stepChips,
    };
  });
}

const REPO_RE = /^[a-zA-Z0-9._-]+\/[a-zA-Z0-9._-]+$/;
const SHA_RE = /^[0-9a-f]{7,40}$/i;
const timeOfDay = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit', second: '2-digit' });

export interface DeployCardModel {
  readonly className: string;
  readonly inFlight: boolean;
  readonly repo: string | null;
  readonly commit: string;
  /** GitHub commit URL when `commit` looks like a SHA; null renders a plain commit label. */
  readonly commitUrl: string | null;
  readonly commitTime: { readonly text: string; readonly iso: string } | null;
  readonly runUrl: string | null;
  readonly timerStart: string | null;
  readonly badgeClass: string;
  readonly stageText: string;
  readonly progress: number;
  readonly footer: string;
  readonly started: string;
}

/** The full pipeline card, field for field with renderDeployments' non-compact template. */
export function deployCardModel(dep: DeploymentStatus): DeployCardModel {
  const stage = dep.stage;
  const live = stage === 'live';
  const failed = stage === 'failed' || stage === 'degraded';
  const inFlight = isBuildingStage(stage) || stage === 'pulling' || stage === 'swapping' || stage === 'awaiting_pull';
  const commit = dep.commit || 'latest';
  const repoTrimmed = typeof dep.repository === 'string' ? dep.repository.trim() : '';
  const [owner = '', name = ''] = (REPO_RE.test(repoTrimmed) ? repoTrimmed : 'azylman/aerial').split('/');
  const repoUrl = `https://github.com/${encodeURIComponent(owner)}/${encodeURIComponent(name)}`;
  const trimmedCommit = (dep.commit || '').trim();
  const timeStr = dep.commit_time || dep.started_at;
  const timeMs = parseValidTimestampMs(timeStr);
  const progress = Number(dep.progress) || 0;
  const startedMs = dep.started_at ? Date.parse(dep.started_at) : Number.NaN;
  return {
    className: `deploy-card ${
      live
        ? 'stage-live'
        : failed
          ? 'stage-failed'
          : stage === 'swapping'
            ? 'stage-swapping'
            : stage === 'pulling'
              ? 'stage-pulling'
              : isBuildingStage(stage) || stage === 'awaiting_pull'
                ? 'stage-building'
                : 'stage-active'
    }`,
    inFlight,
    repo: dep.repository ? dep.repository : null,
    commit,
    commitUrl: SHA_RE.test(trimmedCommit) ? `${repoUrl}/commit/${encodeURIComponent(trimmedCommit)}` : null,
    commitTime: timeMs === null ? null : { text: formatCommitTimestamp(timeStr), iso: new Date(timeMs).toISOString() },
    runUrl: dep.html_url?.startsWith('https://github.com/') ? dep.html_url : null,
    timerStart: inFlight && dep.started_at ? dep.started_at : null,
    badgeClass: `deploy-stage-badge ${
      live ? 'live' : failed ? 'failed' : stage === 'swapping' ? 'swapping' : stage === 'pulling' ? 'pulling' : 'active'
    }`,
    stageText: (stage || 'ACTIVE').toUpperCase(),
    progress,
    footer: dep.commit_msg ? `"${dep.commit_msg}"` : `STAGE: ${(stage || '').toUpperCase()} (${progress}%)`,
    started: Number.isNaN(startedMs) ? (dep.started_at ? 'Invalid Date' : 'RECENT') : timeOfDay.format(startedMs),
  };
}
