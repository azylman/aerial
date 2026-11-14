import { useConsoleStore } from '../../store/store';
import { findServiceByName } from './model';

/**
 * Resolves a loose service name (matrix chip, `aerial-` prefixed or
 * not) against the current services snapshot and opens that service's drawer; no match is a no-op.
 */
export function openServiceDrawer(name: string): void {
  const state = useConsoleStore.getState();
  const service = findServiceByName(state.status.cluster.data?.services ?? null, name);
  if (service !== null) state.openDrawer({ kind: 'service', name: service.name });
}
