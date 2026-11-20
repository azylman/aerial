import { memo } from 'react';
import { getAgentPaletteIndex } from '../lib/formatters';

export interface AgentBadgeProps {
  readonly agent?: string | null | undefined;
  readonly className?: string | undefined;
}

export const AgentBadge = memo(function AgentBadge({ agent, className }: AgentBadgeProps) {
  const norm = (agent ?? '').trim().toLowerCase() || 'aerial';
  const palette = getAgentPaletteIndex(norm);
  const paletteClass = palette >= 0 ? ` agent-palette-${palette}` : '';
  const extraClass = className ? ` ${className}` : '';
  return (
    <span
      className={`agent-badge agent-${norm}${paletteClass}${extraClass}`}
      data-agent={norm}
      {...(palette >= 0 ? { 'data-agent-palette': String(palette) } : {})}
    >
      {norm}
    </span>
  );
});
