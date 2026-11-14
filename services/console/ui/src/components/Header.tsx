import { memo, useMemo } from 'react';
import type { QuickLaunchLink } from '../api/types';
import { dockLinks, isDockLinkActive, type PermetTone, permetScore } from '../store/selectors';
import { useConsoleStore } from '../store/store';
import { Icon } from './Icon';
import { TabBar } from './TabBar';

const TONE_COLOR: Readonly<Record<PermetTone, string | undefined>> = {
  idle: undefined,
  optimal: 'var(--neon-cyan)',
  stable: 'var(--neon-amber)',
  warning: 'var(--neon-red)',
};

const PermetGauge = memo(function PermetGauge() {
  const services = useConsoleStore((s) => s.status.cluster.data?.services ?? null);
  const score = useMemo(() => permetScore(services), [services]);
  const color = TONE_COLOR[score.tone];
  return (
    <div className="permet-gauge">
      <div className="permet-gauge-header">
        <span className="permet-label">PERMET SCORE</span>
        <span className="permet-value" style={color === undefined ? undefined : { color }}>
          {score.label}
        </span>
      </div>
      <div className="permet-bar-bg">
        <div className="permet-bar-fill" style={{ width: `${score.percent}%` }} />
      </div>
    </div>
  );
});

function DockChip({ link, pathname }: { readonly link: QuickLaunchLink; readonly pathname: string }) {
  const blank = link.target !== '_self';
  return (
    <a
      href={link.url}
      className={isDockLinkActive(link.url, pathname) ? 'dock-chip active' : 'dock-chip'}
      target={blank ? '_blank' : '_self'}
      rel={blank ? 'noopener noreferrer' : undefined}
      title={link.description || undefined}
    >
      <span className={link.is_custom === true ? 'ping-dot custom' : 'ping-dot'} />
      <span>
        {link.icon ? `${link.icon} ` : ''}
        {link.name}
      </span>
    </a>
  );
}

/** Quick-launch dock: hidden entirely until the status poll returns links. */
const QuickLaunchDock = memo(function QuickLaunchDock() {
  const raw = useConsoleStore((s) => s.status.cluster.data?.quick_launch_links ?? null);
  const links = useMemo(() => dockLinks(raw), [raw]);
  if (links.length === 0) return null;
  const pathname = window.location.pathname;
  return (
    <nav className="quick-launch-dock" aria-label="Tactical Gateway Launcher">
      {links.map((link) => (
        <DockChip key={`${link.name}\u0000${link.url}`} link={link} pathname={pathname} />
      ))}
    </nav>
  );
});

/**
 * The HUD header. It subscribes to nothing itself: the gauge, tab buttons and dock each select
 * their own slice of state, so the header shell renders exactly once.
 */
export const Header = memo(function Header() {
  return (
    <header className="hud-header">
      <div className="header-top">
        <div className="brand-block">
          <div className="brand-badge">
            <Icon name="bolt" size={18} />
          </div>
          <div className="brand-text">
            <h1>
              AERIAL <span className="version">v3.0</span>
            </h1>
            <div className="sub">PERMET COMMAND HUD // TACTICAL GATEWAY</div>
          </div>
        </div>
        <PermetGauge />
      </div>
      <TabBar />
      <QuickLaunchDock />
    </header>
  );
});
