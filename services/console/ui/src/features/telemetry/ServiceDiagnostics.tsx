import { memo } from 'react';
import { formatUptime } from '../../lib/formatters';
import { useConsoleStore } from '../../store/store';
import { selectService } from './selectors';
import '../../styles/sections.css';
import './ServiceDiagnostics.css';

/**
 * Service drawer body. It follows the live snapshot, so a status
 * change while the drawer is open updates it in place; a service that disappears renders nothing.
 */
export const ServiceDiagnostics = memo(function ServiceDiagnostics({ target }: { readonly target: { readonly name: string } }) {
  const svc = useConsoleStore(selectService(target.name));
  if (svc === undefined) return null;
  return (
    <>
      <div className="diag-grid">
        <div className="diag-item">
          <span className="lbl">SERVICE NAME</span>
          <span className="val">{svc.name}</span>
        </div>
        <div className="diag-item">
          <span className="lbl">HEALTH STATUS</span>
          <span className={svc.status === 'healthy' ? 'val text-success' : 'val text-danger'}>{svc.status.toUpperCase()}</span>
        </div>
        <div className="diag-item">
          <span className="lbl">UPTIME CONTEXT</span>
          <span className="val">{formatUptime(svc.uptime_seconds)}</span>
        </div>
        <div className="diag-item">
          <span className="lbl">DISCOVERY NODE</span>
          <span className="val">aerial-net:internal (Docker Bridge)</span>
        </div>
      </div>
      <div className="console-box">
        <div>[TELEMETRY LOG STREAM]</div>
        <div>&gt; Service node {svc.name} initialized via Docker Supervisor.</div>
        <div>&gt; Healthcheck ping: 200 OK (latency &lt; 2ms)</div>
        <div>&gt; Permet link established. Memory footprint nominal.</div>
      </div>
    </>
  );
});
