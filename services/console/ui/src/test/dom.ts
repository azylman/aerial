import { useKeyboardShortcuts } from '../hooks/useKeyboardShortcuts';
import type { TabId } from '../store/store';

/** The whitespace-collapsed text of the first element matching selector ('' when absent). */
export const text = (selector: string): string => (document.querySelector(selector)?.textContent ?? '').replace(/\s+/g, ' ').trim();

/** Mounts the app-level keyboard shortcuts as if `tab` were the active tab. */
export function Shortcuts({ tab }: { tab: TabId }): null {
  useKeyboardShortcuts(tab);
  return null;
}
