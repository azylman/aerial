import { memo, useMemo } from 'react';
import type { QuickLaunchLink } from '../api/types';
import { dockLinks, isDockLinkActive, type PermetTone, permetScore } from '../store/selectors';
import { useConsoleStore } from '../store/store';
import { Icon } from './Icon';
import { TabBar } from './TabBar';

const TONE_COLOR: Readonly<Record<PermetTone, string | undefined>> = {
  idle: undefined,
  optimal: 'var(--accent-emerald)',
  stable: 'var(--accent-amber)',
  warning: 'var(--accent-crimson)',
};

const PermetGauge = memo(function PermetGauge() {
  const services = useConsoleStore((s) => s.status.cluster.data?.services ?? null);
  const score = useMemo(() => permetScore(services), [services]);
  const color = TONE_COLOR[score.tone];
  return (
    <div className="permet-gauge status-pill">
      <span className="pulse-dot healthy" />
      <span id="permet-score-val" className={`permet-val ${score.tone}`} style={color === undefined ? undefined : { color }}>
        {score.label}
      </span>
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
      <div className="cmd-left">
        <div className="brand-block">
          <div className="brand-badge">
            <span className="brand-monogram">A</span>
            <Icon name="bolt" size={18} />
          </div>
          <div className="brand-text">
            <h1>Aerial Console</h1>
          </div>
        </div>
        <TabBar />
      </div>
      <div className="cmd-right">
        <QuickLaunchDock />
        <PermetGauge />
      </div>
    </header>
  );
});
