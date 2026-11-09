import type { ServiceStatus } from '../../api/types';
import type { ConsoleState } from '../../store/store';

const EMPTY_SERVICES: readonly ServiceStatus[] = Object.freeze([]);

export const selectServices = (s: ConsoleState): readonly ServiceStatus[] => s.status.cluster.data?.services ?? EMPTY_SERVICES;

/** The service with exactly this name in the current snapshot (reconciled, so stable by reference). */
export const selectService =
  (name: string) =>
  (s: ConsoleState): ServiceStatus | undefined =>
    selectServices(s).find((svc) => svc.name === name);
