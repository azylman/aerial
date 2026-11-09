import { act, fireEvent, render } from '@testing-library/react';
import { useCallback, useRef, useState } from 'react';
import { beforeEach, describe, expect, it } from 'vitest';
import { type TabId, useConsoleStore } from '../store/store';
import { resolveShortcut, type ShortcutContext, useKeyboardShortcuts, useSearchShortcutTarget } from './useKeyboardShortcuts';

const base: ShortcutContext = {
  key: '/',
  modifier: false,
  drawerOpen: false,
  hasSearch: true,
  inSearch: false,
  inEditable: false,
};

describe('resolveShortcut', () => {
  it.each([
    { name: "'/' focuses the view search", ctx: base, want: 'focusSearch' },
    { name: "'/' without a search box does nothing", ctx: { ...base, hasSearch: false }, want: 'none' },
    { name: "'/' while typing in a field types", ctx: { ...base, inEditable: true }, want: 'none' },
    { name: "'/' inside the search box types", ctx: { ...base, inSearch: true, inEditable: true }, want: 'none' },
    { name: "ctrl+'/' is left to the browser", ctx: { ...base, modifier: true }, want: 'none' },
    { name: 'Escape in the search clears it', ctx: { ...base, key: 'Escape', inSearch: true, inEditable: true }, want: 'clearSearch' },
    { name: 'Escape elsewhere does nothing', ctx: { ...base, key: 'Escape' }, want: 'none' },
    { name: 'drawer open owns every key', ctx: { ...base, key: 'Escape', inSearch: true, drawerOpen: true }, want: 'none' },
    { name: 'other keys are ignored', ctx: { ...base, key: 'k' }, want: 'none' },
  ] as const)('$name', ({ ctx, want }) => {
    expect(resolveShortcut(ctx)).toBe(want);
  });
});

/** Test harness: one view with a registered search box plus an unrelated text field. */
function Harness({ activeTab, searchTab }: { readonly activeTab: TabId; readonly searchTab: TabId }) {
  const input = useRef<HTMLInputElement>(null);
  const [value, setValue] = useState('');
  const clear = useCallback(() => setValue(''), []);
  useSearchShortcutTarget(searchTab, input, clear);
  useKeyboardShortcuts(activeTab);
  return (
    <>
      <input aria-label="search" ref={input} value={value} onChange={(e) => setValue(e.target.value)} />
      <input aria-label="other" />
    </>
  );
}

interface FlowRow {
  name: string;
  activeTab: TabId;
  searchTab: TabId;
  focus: 'search' | 'other' | 'body';
  drawerOpen: boolean;
  key: string;
  wantFocus: 'search' | 'other' | 'body';
  wantValue: string;
  wantPrevented: boolean;
}

const flowRows: FlowRow[] = [
  { name: "'/' focuses the active view's search", activeTab: 'memory', searchTab: 'memory', focus: 'body', drawerOpen: false, key: '/', wantFocus: 'search', wantValue: 'abc', wantPrevented: true },
  { name: "'/' ignores other views' search boxes", activeTab: 'builds', searchTab: 'memory', focus: 'body', drawerOpen: false, key: '/', wantFocus: 'body', wantValue: 'abc', wantPrevented: false },
  { name: "'/' while typing elsewhere is ignored", activeTab: 'memory', searchTab: 'memory', focus: 'other', drawerOpen: false, key: '/', wantFocus: 'other', wantValue: 'abc', wantPrevented: false },
  { name: 'Escape clears and blurs the search', activeTab: 'schedules', searchTab: 'schedules', focus: 'search', drawerOpen: false, key: 'Escape', wantFocus: 'body', wantValue: '', wantPrevented: false },
  { name: 'Escape with a drawer open is left to the drawer', activeTab: 'builds', searchTab: 'builds', focus: 'search', drawerOpen: true, key: 'Escape', wantFocus: 'search', wantValue: 'abc', wantPrevented: false },
];

describe('useKeyboardShortcuts', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  });

  it.each(flowRows)('$name', ({ activeTab, searchTab, focus, drawerOpen, key, wantFocus, wantValue, wantPrevented }) => {
    const { getByLabelText, unmount } = render(<Harness activeTab={activeTab} searchTab={searchTab} />);
    const search = getByLabelText('search') as HTMLInputElement;
    const other = getByLabelText('other') as HTMLInputElement;
    fireEvent.change(search, { target: { value: 'abc' } });
    if (drawerOpen) useConsoleStore.getState().openDrawer({ kind: 'build', id: 'b1' });
    const targets = { search, other, body: document.body };
    act(() => {
      if (focus === 'body') (document.activeElement as HTMLElement | null)?.blur();
      else targets[focus].focus();
    });

    const prevented = !fireEvent.keyDown(document.activeElement ?? document.body, { key });

    expect(document.activeElement).toBe(targets[wantFocus]);
    expect(search.value).toBe(wantValue);
    expect(prevented).toBe(wantPrevented);

    unmount();
    // Unmounting unregisters both the listener and the search target.
    expect(fireEvent.keyDown(document.body, { key: '/' })).toBe(true);
  });
});
