import { createContext, useContext } from 'react';

/**
 * Whether the enclosing tab panel is the shown one. Visited tabs stay mounted (their layout is
 * cached), so views gate their own polling and live clocks on this instead of being unmounted.
 * Defaults to true: anything outside a tab panel (the drawer, header badges) is always active.
 */
export const TabActiveContext = createContext<boolean>(true);

export function useTabActive(): boolean {
  return useContext(TabActiveContext);
}
