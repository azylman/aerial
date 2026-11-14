import { type RefObject, useEffect, useRef } from 'react';
import { type TabId, useConsoleStore } from '../store/store';

// Global keyboard shortcuts:
//   '/'      focuses the active view's search box (unless already typing in a field)
//   Escape   clears and blurs that search box when it has focus
// Escape with a drawer open belongs to the drawer (see Drawer.tsx), so it is ignored here.
// Views opt in by registering their search input with useSearchShortcutTarget; no DOM queries.

export type ShortcutAction = 'none' | 'focusSearch' | 'clearSearch';

export interface ShortcutContext {
  readonly key: string;
  readonly modifier: boolean;
  readonly drawerOpen: boolean;
  readonly hasSearch: boolean;
  readonly inSearch: boolean;
  readonly inEditable: boolean;
}

export function resolveShortcut(ctx: ShortcutContext): ShortcutAction {
  if (ctx.drawerOpen || ctx.modifier) return 'none';
  if (ctx.key === '/' && ctx.hasSearch && !ctx.inEditable) return 'focusSearch';
  if (ctx.key === 'Escape' && ctx.inSearch) return 'clearSearch';
  return 'none';
}

interface SearchTarget {
  readonly input: RefObject<HTMLInputElement | null>;
  readonly clear: RefObject<() => void>;
}

const searchTargets = new Map<TabId, SearchTarget>();

/** Registers `input` as `tab`'s search box for the '/' and Escape shortcuts while mounted. */
export function useSearchShortcutTarget(
  tab: TabId,
  input: RefObject<HTMLInputElement | null>,
  clear: () => void,
): void {
  const clearRef = useRef(clear);
  useEffect(() => {
    clearRef.current = clear;
  }, [clear]);
  useEffect(() => {
    const target: SearchTarget = { input, clear: clearRef };
    searchTargets.set(tab, target);
    return () => {
      if (searchTargets.get(tab) === target) searchTargets.delete(tab);
    };
  }, [tab, input]);
}

function isEditable(el: Element | null): boolean {
  if (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement || el instanceof HTMLSelectElement) {
    return true;
  }
  return el instanceof HTMLElement && el.isContentEditable;
}

/** Installs the single global keydown listener for the active tab. */
export function useKeyboardShortcuts(activeTab: TabId): void {
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent): void => {
      const target = searchTargets.get(activeTab);
      const input = target?.input.current ?? null;
      const focused = document.activeElement;
      const action = resolveShortcut({
        key: event.key,
        modifier: event.ctrlKey || event.metaKey || event.altKey,
        drawerOpen: useConsoleStore.getState().drawer.kind !== 'closed',
        hasSearch: input !== null,
        inSearch: input !== null && focused === input,
        inEditable: isEditable(focused),
      });
      if (action === 'focusSearch') {
        event.preventDefault();
        input?.focus();
      } else if (action === 'clearSearch') {
        target?.clear.current();
        input?.blur();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [activeTab]);
}
