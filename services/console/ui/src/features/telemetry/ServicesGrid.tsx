import { type KeyboardEvent, memo } from 'react';
import { useShallow } from 'zustand/react/shallow';
import { formatUptime } from '../../lib/formatters';
import { useConsoleStore } from '../../store/store';
import { healthyCountText } from './model';
import { selectService, selectServices } from './selectors';


const openDrawerFor = (name: string): void => useConsoleStore.getState().openDrawer({ kind: 'service', name });

/** Enter/Space activate a `role="button"` element, matching native button semantics. */
export function onActivateKey(event: KeyboardEvent, activate: () => void): void {
  if (event.key !== 'Enter' && event.key !== ' ') return;
  event.preventDefault();
  activate();
}

/**
 * One service card, subscribed to its own service by name: a poll that changes one service
 * re-renders exactly one card, and an unchanged poll re-renders none.
 */
export const ServiceCard = memo(function ServiceCard({ name }: { readonly name: string }) {
  const svc = useConsoleStore(selectService(name));
  if (svc === undefined) return null;
  const open = (): void => openDrawerFor(svc.name);
  return (
    <div
      className="service-card"
      role="button"
      tabIndex={0}
      aria-label={`${svc.name} diagnostics`}
      onClick={open}
      onKeyDown={(e) => onActivateKey(e, open)}
    >
      <div className="header">
        <span className="title">{svc.name}</span>
        <div className="status-badge-container">
          <span className={`pulse-dot ${svc.status}`} />
          <span className={`badge ${svc.status}`}>{svc.status.toUpperCase()}</span>
        </div>
      </div>
      <div className="service-metrics">
        <span>UPTIME: {formatUptime(svc.uptime_seconds)}</span>
        <span>PORT: READY</span>
      </div>
      <div className="card-action-hint">CLICK FOR DIAGNOSTICS &rarr;</div>
    </div>
  );
});

/** The grid only tracks the list of names, so it re-renders when services appear or vanish. */
export const ServicesGrid = memo(function ServicesGrid() {
  const names = useConsoleStore(useShallow((s) => selectServices(s).map((svc) => svc.name)));
  return (
    <div className="services-grid">
      {names.map((name) => (
        <ServiceCard key={name} name={name} />
      ))}
    </div>
  );
});

export const HealthyCount = memo(function HealthyCount() {
  const text = useConsoleStore((s) => healthyCountText(s.status.cluster.data?.services ?? null));
  return (
    <span className="section-badge" id="active-count">
      {text}
    </span>
  );
});
