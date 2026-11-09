// Search-term highlighting, ported from highlightSearch in static/app.js. Instead of building an
// HTML string the text is split into plain/match segments and React renders <mark> elements, so
// escaping is handled by React and no markup is ever parsed.

import { memo } from 'react';

export interface HighlightSegment {
  readonly text: string;
  readonly match: boolean;
}

const REGEX_SPECIALS = /[.*+?^${}()|[\]\\]/g;

/** Splits `text` into alternating segments, flagging case-insensitive literal matches of `query`. */
export function highlightSegments(text: string, query: string): HighlightSegment[] {
  if (!text) return [];
  if (!query) return [{ text, match: false }];
  // With one capture group, split() puts matches at odd indexes.
  const parts = text.split(new RegExp(`(${query.replace(REGEX_SPECIALS, '\\$&')})`, 'i'));
  const segments: HighlightSegment[] = [];
  parts.forEach((part, i) => {
    if (part) segments.push({ text: part, match: i % 2 === 1 });
  });
  return segments;
}

export interface HighlightProps {
  readonly text: string;
  readonly query: string;
}

/** Renders `text` with every match of `query` wrapped in `<mark class="cyber-highlight">`. */
export const Highlight = memo(function Highlight({ text, query }: HighlightProps) {
  if (!query) return text;
  return highlightSegments(text, query).map((seg, i) =>
    seg.match ? (
      <mark key={i} className="cyber-highlight">
        {seg.text}
      </mark>
    ) : (
      seg.text
    ),
  );
});
