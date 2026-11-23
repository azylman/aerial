import { memo, useEffect, useMemo, useState } from 'react';
import { getAgentPaletteIndex } from '../lib/formatters';
import { applyTheme, initTheme, nextTheme, subscribeTheme, type ThemeMode } from '../lib/theme';
import { type PermetTone, permetScore } from '../store/selectors';
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



const ThemeToggle = memo(function ThemeToggle() {
  const [theme, setTheme] = useState<ThemeMode>(() => initTheme());
  // Another tab, or another app on the shared cookie domain, changed the theme: follow it live.
  useEffect(() => subscribeTheme(setTheme), []);

  return (
    <button
      type="button"
      className="theme-toggle-btn"
      aria-label={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
      title={theme === 'dark' ? 'Switch to light theme (Polar Slate)' : 'Switch to dark theme (Catppuccin Mocha)'}
      onClick={() => {
        const next = nextTheme(theme);
        applyTheme(next);
        setTheme(next);
      }}
    >
      <Icon name={theme === 'dark' ? 'sun' : 'moon'} size={14} />
      <span className="theme-toggle-label">{theme === 'dark' ? 'Light' : 'Dark'}</span>
    </button>
  );
});

const DEFAULT_AGENTS = ['all', 'aerial'] as const;

export const AgentSelector = memo(function AgentSelector() {
  const selectedAgent = useConsoleStore((s) => s.selectedAgent);
  const setSelectedAgent = useConsoleStore((s) => s.setSelectedAgent);
  const configured = useConsoleStore((s) => s.status.cluster.data?.configured_agents);

  const agents = useMemo(() => {
    if (configured && configured.length > 0) {
      const list = ['all', ...configured.map((a) => a.toLowerCase())];
      return Array.from(new Set(list));
    }
    return DEFAULT_AGENTS;
  }, [configured]);

  return (
    <nav className="agent-selector-bar" role="toolbar" aria-label="Agent Selector">
      {agents.map((agent) => {
        const isSelected = selectedAgent === agent;
        const label = agent === 'all' ? 'All' : agent.charAt(0).toUpperCase() + agent.slice(1);
        const palette = getAgentPaletteIndex(agent);
        return (
          <button
            key={agent}
            type="button"
            className={isSelected ? 'agent-pill active' : 'agent-pill'}
            data-agent={agent}
            {...(palette >= 0 ? { 'data-agent-palette': String(palette) } : {})}
            aria-pressed={isSelected}
            onClick={() => setSelectedAgent(agent)}
          >
            {label}
          </button>
        );
      })}
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
        <AgentSelector />
        <PermetGauge />
        <ThemeToggle />
      </div>
    </header>
  );
});
