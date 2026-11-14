import { type MouseEvent, memo } from 'react';
import type { DeploymentStatus } from '../../api/types';
import { TimerBadge } from '../../components/ElapsedTicker';
import { Glyph, GlyphText } from '../../components/Glyph';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import {
  type ChipModel,
  compactDeployModel,
  deployBadge,
  deployCardModel,
  deploySteps,
  isCompactDeploy,
  type StepModel,
} from './model';
import { openServiceDrawer } from './openService';
import { onActivateKey } from './ServicesGrid';

const EMPTY_DEPLOYS: readonly DeploymentStatus[] = Object.freeze([]);
const selectDeploys = (s: ConsoleState): readonly DeploymentStatus[] => s.status.cluster.data?.deployments ?? EMPTY_DEPLOYS;

function MatrixChip({ chip }: { readonly chip: ChipModel }) {
  const open = (): void => openServiceDrawer(chip.name);
  return (
    <span
      className={chip.className}
      role="button"
      tabIndex={0}
      title={chip.title}
      onClick={(e: MouseEvent) => {
        e.stopPropagation();
        open();
      }}
      onKeyDown={(e) => onActivateKey(e, open)}
    >
      {chip.label} <Glyph name={chip.icon} />
    </span>
  );
}

function StepPanel({ step }: { readonly step: StepModel }) {
  return (
    <div className={step.className}>
      <div className="step-header">
        <span className="step-icon">{step.icon}</span>
        <span className="step-status-badge">
          <Glyph name={step.statusIcon} /> {step.statusText}
        </span>
      </div>
      <div className="step-name">{step.name}</div>
      {step.chips.length === 0 ? null : (
        <div className="matrix-chips-container">
          {step.chips.map((chip) => (
            <MatrixChip key={chip.name} chip={chip} />
          ))}
        </div>
      )}
    </div>
  );
}

/** Full pipeline card; memoized on the reconciled deployment object. */
export const DeployCard = memo(function DeployCard({ dep }: { readonly dep: DeploymentStatus }) {
  const m = deployCardModel(dep);
  return (
    <div className={m.className}>
      {m.inFlight ? <div className="deploy-card-laser" /> : null}
      <div className="deploy-card-header">
        <div className="deploy-target">
          {m.repo === null ? null : (
            <span className="deploy-repo-badge" title={`Repository: ${m.repo}`}>
              <Glyph name="package" /> {m.repo}
            </span>
          )}
          {m.commitUrl === null ? (
            <span className="deploy-commit">{m.commit}</span>
          ) : (
            <a href={m.commitUrl} target="_blank" rel="noopener" className="deploy-commit-link" title="View commit on GitHub">
              {m.commit} ↗
            </a>
          )}
          {m.commitTime === null ? null : (
            <span className="deploy-commit-time" title={`Committed: ${m.commitTime.iso}`}>
              <Glyph name="clock" /> {m.commitTime.text}
            </span>
          )}
          {m.runUrl === null ? null : (
            <a href={m.runUrl} target="_blank" rel="noopener" className="deploy-run-link" title="Open GitHub Actions Run">
              RUN LOGS ↗
            </a>
          )}
        </div>
        <div className="card-header-right">
          {m.timerStart === null ? null : <TimerBadge startedAt={m.timerStart} />}
          <span className={m.badgeClass}>
            <Glyph name="bolt" /> {m.stageText}
          </span>
        </div>
      </div>
      <div className="deploy-steps-grid">
        {deploySteps(dep).map((step) => (
          <StepPanel key={step.key} step={step} />
        ))}
      </div>
      <div className="deploy-progress-bg">
        <div className="deploy-progress-fill" style={{ width: `${m.progress}%` }} />
      </div>
      <div className="deploy-footer">
        <span>{m.footer}</span>
        <span>STARTED: {m.started}</span>
      </div>
    </div>
  );
});

export const CompactDeployCard = memo(function CompactDeployCard({ dep }: { readonly dep: DeploymentStatus }) {
  const m = compactDeployModel(dep);
  return (
    <div className={m.className}>
      <span className="compact-commit">{m.commit}</span>
      <span className="compact-title">{m.title}</span>
      <span className={m.badgeClass}>
        <GlyphText icon={m.badgeIcon} text={m.badgeText} />
      </span>
      <span className="compact-toggle">▾</span>
    </div>
  );
});

/** The deployment pipeline card list. */
export const Deployments = memo(function Deployments() {
  const deploys = useConsoleStore(selectDeploys);
  const loaded = useConsoleStore((s) => s.status.cluster.data !== null);
  if (!loaded) return <div className="deployments-container" />;
  return (
    <div className="deployments-container">
      {deploys.length === 0 ? (
        <div className="deploy-idle-card">
          <div className="idle-indicator">
            <span className="pulse-dot healthy" />
            <span>ALL SERVICES IN SYNC // NO PENDING DEPLOYS</span>
          </div>
          <div>FLUX RECONCILING</div>
        </div>
      ) : (
        deploys.map((dep, i) =>
          isCompactDeploy(dep.stage, i, deploys.length) ? (
            <CompactDeployCard key={dep.id} dep={dep} />
          ) : (
            <DeployCard key={dep.id} dep={dep} />
          ),
        )
      )}
    </div>
  );
});

/** DEPLOYMENT PIPELINE header badge; static `IN SYNC` until the first status poll lands. */
export const DeployBadge = memo(function DeployBadge() {
  const deploys = useConsoleStore((s) => s.status.cluster.data?.deployments ?? null);
  const badge = deploys === null ? { icon: null, text: 'IN SYNC', className: 'section-badge' } : deployBadge(deploys);
  return (
    <span className={badge.className} id="deploy-count-badge">
      <GlyphText icon={badge.icon} text={badge.text} />
    </span>
  );
});
