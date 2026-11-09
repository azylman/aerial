import { GitSyncBadge } from '../../components/GitSyncBadge';
import { Icon } from '../../components/Icon';
import { useConsoleStore } from '../../store/store';
import '../../styles/sections.css';
import { QueueBadge, TaskQueue } from '../tasks/TaskQueue';
import { DeployBadge, Deployments } from './Deployments';
import { HealthyCount, ServicesGrid } from './ServicesGrid';
import './telemetry.css';

const expandTasks = (): void => useConsoleStore.getState().setActiveTab('tasks');

/**
 * Port of #telemetry-view. The view itself subscribes to nothing: every badge, list and card
 * selects its own slice of the status snapshot, so the shell of the view renders once.
 */
export default function TelemetryView() {
  return (
    <>
      <section className="active-tasks-section" aria-live="polite">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="bolt" />
            </span>
            <h2>LIVE AGENT EXECUTION QUEUE</h2>
          </div>
          <div className="section-actions">
            <QueueBadge id="tasks-count-badge" />
            <button type="button" className="task-inspect-btn expand-tasks-btn" onClick={expandTasks}>
              <span>EXPAND VIEW ↗</span>
            </button>
          </div>
        </div>
        <div className="active-tasks-container">
          <TaskQueue />
        </div>
      </section>

      <section className="deployment-pipeline-section">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="rocket" />
            </span>
            <h2>DEPLOYMENT PIPELINE</h2>
          </div>
          <div className="section-actions">
            <DeployBadge />
            <GitSyncBadge />
          </div>
        </div>
        <Deployments />
      </section>

      <main className="services-grid-section">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="bolt" />
            </span>
            <h2>ACTIVE STACK COMPONENTS</h2>
          </div>
          <HealthyCount />
        </div>
        <ServicesGrid />
      </main>
    </>
  );
}
