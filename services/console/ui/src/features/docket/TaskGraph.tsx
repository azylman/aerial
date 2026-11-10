import type { DocketTaskDTO, DocketTaskSummaryDTO } from '../../api/types';
import { graphGroups } from './model';
import { openTask, StatusChip } from './TaskCard';

function GraphNode({ node }: { readonly node: DocketTaskSummaryDTO }) {
  return (
    <button type="button" className="docket-node" data-task-id={node.id} onClick={() => openTask(node.id)}>
      <span className="docket-node-id">#{node.id}</span>
      <span className="docket-node-title">{node.title || 'Untitled task'}</span>
      <StatusChip status={node.status} />
    </button>
  );
}

/**
 * The task's dependency neighbourhood on a CSS grid: parent above, blocked-by on the left, the
 * task in the centre, blocks on the right, subtasks below and related along the bottom. Empty
 * groups are omitted; every neighbour retargets the drawer to itself.
 */
export function TaskGraph({ task }: { readonly task: DocketTaskDTO }) {
  return (
    <div className="docket-graph" aria-label="Task dependency graph">
      <div className="docket-graph-center">
        <span className="docket-node docket-node-current" aria-current="true">
          <span className="docket-node-id">#{task.id}</span>
          <span className="docket-node-title">{task.title || 'Untitled task'}</span>
          <StatusChip status={task.status} />
        </span>
      </div>
      {graphGroups(task).map((group) => (
        <section key={group.key} className={`docket-graph-group docket-graph-${group.key}`} aria-label={group.label}>
          <span className="docket-graph-label">{group.label}</span>
          {group.nodes.map((node) => (
            <GraphNode key={node.id} node={node} />
          ))}
        </section>
      ))}
    </div>
  );
}
