import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import {
  effectiveQuery,
  Highlight,
  HighlightQuery,
  highlightSegments,
  type HighlightSegment,
  QueryHighlight,
} from './search';

const plain = (text: string): HighlightSegment => ({ text, match: false });
const hit = (text: string): HighlightSegment => ({ text, match: true });

describe('highlightSegments', () => {
  it.each<{ name: string; text: string; query: string; want: HighlightSegment[] }>([
    { name: 'empty text', text: '', query: 'x', want: [] },
    { name: 'empty query', text: 'Daily standup', query: '', want: [plain('Daily standup')] },
    { name: 'case-insensitive match keeps original case', text: 'Daily Standup', query: 'stand', want: [plain('Daily '), hit('Stand'), plain('up')] },
    { name: 'every occurrence', text: 'aXbxc', query: 'x', want: [plain('a'), hit('X'), plain('b'), hit('x'), plain('c')] },
    { name: 'match at both ends', text: 'go to go', query: 'go', want: [hit('go'), plain(' to '), hit('go')] },
    { name: 'whole text match', text: 'eri', query: 'ERI', want: [hit('eri')] },
    { name: 'regex specials are literal', text: 'cost (a+b)*2', query: '(a+b)*', want: [plain('cost '), hit('(a+b)*'), plain('2')] },
    { name: 'html-looking text stays raw', text: '<b>&amp;</b>', query: '&', want: [plain('<b>'), hit('&'), plain('amp;</b>')] },
    { name: 'no match', text: 'quiet', query: 'zero', want: [plain('quiet')] },
    { name: 'reused matcher after a new query', text: 'zero quiet', query: 'zero', want: [hit('zero'), plain(' quiet')] },
    { name: 'regex specials in a reused slot', text: 'a.b', query: '.', want: [plain('a'), hit('.'), plain('b')] },
  ])('$name', ({ text, query, want }) => {
    expect(highlightSegments(text, query)).toEqual(want);
  });
});

describe('Highlight', () => {
  it.each([
    { name: 'no query renders plain text', text: 'Run <script>', query: '', wantText: 'Run <script>', wantMarks: [] },
    { name: 'marks each match', text: 'Pick up the PICKAXE', query: 'pick', wantText: 'Pick up the PICKAXE', wantMarks: ['Pick', 'PICK'] },
    { name: 'empty text renders nothing', text: '', query: 'x', wantText: '', wantMarks: [] },
  ])('$name', ({ text, query, wantText, wantMarks }) => {
    const { container } = render(<Highlight text={text} query={query} />);
    expect(container.textContent).toBe(wantText);
    const marks = [...container.querySelectorAll('mark')];
    expect(marks.map((m) => m.textContent)).toEqual(wantMarks);
    expect(marks.every((m) => m.className === 'cyber-highlight')).toBe(true);
    expect(container.querySelector('script')).toBeNull();
  });
});

describe('effectiveQuery', () => {
  it.each([
    { name: 'empty query', text: 'Daily standup', query: '', want: '' },
    { name: 'match keeps the query', text: 'Daily Standup', query: 'stand', want: 'stand' },
    { name: 'no match collapses to empty', text: 'Daily standup', query: 'kiosk', want: '' },
    { name: 'regex specials are literal', text: 'cost (a+b)', query: '(a+b)', want: '(a+b)' },
    { name: 'special that would match anything as a regex', text: 'abc', query: '.', want: '' },
  ])('$name', ({ text, query, want }) => {
    expect(effectiveQuery(text, query)).toBe(want);
  });
});

describe('QueryHighlight', () => {
  // The leaves read the query from HighlightQuery: a context change re-highlights them in place.
  it.each([
    { name: 'query matching one leaf', query: 'garden', wantMarks: ['garden'] },
    { name: 'query matching neither leaf', query: 'kiosk', wantMarks: [] },
    { name: 'query matching both leaves', query: 'e', wantMarks: ['e', 'e', 'e', 'e', 'e'] },
  ])('$name', ({ query, wantMarks }) => {
    const tree = (q: string) => (
      <HighlightQuery value={q}>
        <p>
          <QueryHighlight text="Check the garden" />
        </p>
        <p>
          <QueryHighlight text="Water the basil" />
        </p>
      </HighlightQuery>
    );
    const { container, rerender } = render(tree(''));
    rerender(tree(query));
    expect([...container.querySelectorAll('mark.cyber-highlight')].map((m) => m.textContent)).toEqual(wantMarks);
    expect(container.textContent).toBe('Check the gardenWater the basil');
  });
});
