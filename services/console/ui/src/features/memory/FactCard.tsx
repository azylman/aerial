import { memo } from 'react';
import type { FactItem } from '../../api/types';
import { CopyButton } from '../../components/CopyButton';
import { Glyph } from '../../components/Glyph';
import { formatTimeAgo } from '../../lib/formatters';
import { Highlight } from '../../lib/search';
import { factAgeTimestamp } from './model';

export interface FactCardProps {
  readonly fact: FactItem;
  /** Clamped reinforcement count (IndexedFact.count). */
  readonly count: number;
  /** Trimmed search text to highlight (original case; matching is case-insensitive). */
  readonly query: string;
  /** Instant the card ages from: when the archive payload was synced, like renderFactCards. */
  readonly now: number;
}

/**
 * Port of one renderFactCards card. Memoized: poll results are reconciled, so an unchanged fact
 * keeps its reference and its card never re-renders; copy reads the text from props instead of
 * an index into the filtered list.
 */
export const FactCard = memo(function FactCard({ fact, count, query, now }: FactCardProps) {
  return (
    <div className={count >= 2 ? 'fact-card high-importance' : 'fact-card'}>
      <div className="fact-card-header">
        <span className="fact-category-tag cat-system_config">NODE #{String(fact.id || 0)}</span>
        <div className="fact-importance-gauge">
          <span>
            <Glyph name="bolt" /> {count}x
          </span>
        </div>
      </div>
      <div className="fact-body">
        <Highlight text={fact.fact_text} query={query} />
      </div>
      <div className="fact-footer">
        <span>
          <Glyph name="timer" /> {formatTimeAgo(factAgeTimestamp(fact), now)}
        </span>
        <CopyButton text={fact.fact_text} ariaLabel="Copy fact text" className="fact-copy-btn" />
      </div>
    </div>
  );
});
