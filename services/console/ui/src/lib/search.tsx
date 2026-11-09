// Search-term highlighting, ported from highlightSearch in static/app.js. Instead of building an
// HTML string the text is split into plain/match segments and React renders <mark> elements, so
// escaping is handled by React and no markup is ever parsed.

import { createContext, memo, useContext } from 'react';

export interface HighlightSegment {
  readonly text: string;
  readonly match: boolean;
}

const REGEX_SPECIALS = /[.*+?^${}()|[\]\\]/g;

// A keystroke runs every visible leaf against the same query, so the compiled matcher is reused
// until the query changes instead of being rebuilt per leaf.
let lastQuery = '';
let lastMatcher = /(?:)/i;

/** Case-insensitive literal matcher for `query`, with the match in capture group 1. */
function matcherFor(query: string): RegExp {
  if (query !== lastQuery) {
    lastQuery = query;
    lastMatcher = new RegExp(`(${query.replace(REGEX_SPECIALS, '\\$&')})`, 'i');
  }
  return lastMatcher;
}

/** Splits `text` into alternating segments, flagging case-insensitive literal matches of `query`. */
export function highlightSegments(text: string, query: string): HighlightSegment[] {
  if (!text) return [];
  if (!query) return [{ text, match: false }];
  // With one capture group, split() puts matches at odd indexes.
  const parts = text.split(matcherFor(query));
  const segments: HighlightSegment[] = [];
  parts.forEach((part, i) => {
    if (part) segments.push({ text: part, match: i % 2 === 1 });
  });
  return segments;
}

/**
 * The query `text` is highlighted against: `query` when it occurs in `text`, otherwise ''. Both
 * render the same plain text, so leaves that do not match keep an unchanged prop across keystrokes.
 */
export const effectiveQuery = (text: string, query: string): string =>
  query && matcherFor(query).test(text) ? query : '';

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

/**
 * The normalized search query for every QueryHighlight below it. Lists provide it once so their
 * memoized cards take no query prop and skip re-rendering when only the query changes.
 */
export const HighlightQuery = createContext('');

/**
 * Highlight against the HighlightQuery context. A query change re-runs this wrapper only; the
 * memoized Highlight below re-renders just where the highlighted output actually changes.
 */
export const QueryHighlight = memo(function QueryHighlight({ text }: { readonly text: string }) {
  return <Highlight text={text} query={effectiveQuery(text, useContext(HighlightQuery))} />;
});
