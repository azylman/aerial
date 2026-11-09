import { memo } from 'react';
import { Icon } from '../../components/Icon';
import '../../styles/sections.css';
import { queueKpis } from './model';
import { QueueBadge, TaskQueue, useQueueCounts } from './TaskQueue';
import './tasks.css';

// Summary cards keep the whitespace between their spans that the vanilla markup has, so label,
// value and sub-label wrap and space identically.

/** Queue concurrency and backlog cards; subscribes to the two counts only. */
const QueueKpis = memo(function QueueKpis() {
  const kpis = queueKpis(useQueueCounts());
  return (
    <>
      <div className="summary-card">
        <span className="summary-label">Queue Concurrency</span>
        <span className={kpis.workersClass}>{kpis.workers}</span>
        <span className="summary-sub">{kpis.workersSub}</span>
      </div>
      <div className="summary-card">
        <span className="summary-label">Queued Backlog</span>
        {' '}
        <span className="summary-value kpi-queued">{kpis.queued}</span>
        {' '}
        <span className="summary-sub">FIFO DISPATCH READY</span>
      </div>
    </>
  );
});

/** Port of the ACTIVE TASKS cockpit (#tasks-view in static/index.html). */
export default function TasksView() {
  return (
    <>
      <section className="summary-bar">
        <QueueKpis />
        <div className="summary-card">
          <span className="summary-label">Active Channel Funnel</span>
          {' '}
          <span className="summary-value text-success">ONLINE</span>
          {' '}
          <span className="summary-sub">DISCORD GATEWAY OK</span>
        </div>
        <div className="summary-card">
          <span className="summary-label">Engine Architecture</span>
          {' '}
          <span className="summary-value kpi-engine">AGY DUAL-ENGINE</span>
          {' '}
          <span className="summary-sub">OLLAMA + GEMINI 3.8</span>
        </div>
      </section>

      <section className="active-tasks-section">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="bolt" />
            </span>
            <h2>LIVE DISPATCH STREAM</h2>
          </div>
          <QueueBadge id="tasks-view-badge" />
        </div>
        <div className="active-tasks-container">
          <TaskQueue />
        </div>
      </section>

      <section className="active-tasks-section">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="clipboard" />
            </span>
            <h2>RECENT TASK EXECUTION HISTORY</h2>
          </div>
          <span className="section-badge history-badge">PASSED</span>
        </div>
        <div className="active-tasks-container">
          <div className="task-idle-card">
            <div className="idle-indicator">
              <span className="pulse-dot healthy" />
              <span>RECENT RUN TURNS AUDITED // NO FAILURES RECORDED</span>
            </div>
            <div>HISTORY BUFFER (POSTGRESQL)</div>
          </div>
        </div>
      </section>
    </>
  );
}
