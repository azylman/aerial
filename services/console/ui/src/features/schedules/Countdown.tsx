import { memo } from 'react';
import { GlyphText } from '../../components/Glyph';
import { formatCountdown } from '../../lib/formatters';
import { useServerNow } from './clock';

/**
 * Live countdown badge on a schedule card (startSchedulesTicker's per-card writer). It is the only
 * part of a card on the 1s clock, so a tick re-renders this span and never the card.
 */
export const CountdownBadge = memo(function CountdownBadge({ target }: { readonly target: string }) {
  const cd = formatCountdown(target, useServerNow());
  return (
    <span className={`schedule-countdown-badge ${cd.cssClass}`}>
      <GlyphText icon={cd.icon} text={cd.text} />
    </span>
  );
});

/** NEXT TRIGGER summary value: countdown to `target`, or NO UPCOMING. */
export const NextTriggerValue = memo(function NextTriggerValue({ target }: { readonly target: string | null }) {
  const now = useServerNow();
  if (target === null) {
    return (
      <span className="value" id="schedules-next-trigger">
        NO UPCOMING
      </span>
    );
  }
  const cd = formatCountdown(target, now);
  return (
    <span className={`value ${cd.cssClass}`} id="schedules-next-trigger">
      <GlyphText icon={cd.icon} text={cd.text} />
    </span>
  );
});
