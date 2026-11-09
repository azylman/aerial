import { memo } from 'react';
import type { GitSyncStatusResponse } from '../api/types';
import { useLiveNow } from '../hooks/useLiveNow';
import { type GitSyncBadgeModel, gitSyncBadge } from '../store/selectors';
import { useConsoleStore } from '../store/store';
import { Icon } from './Icon';
import './GitSyncBadge.css';

function Pill({ model }: { readonly model: GitSyncBadgeModel }) {
  return (
    <div className={model.tone === 'synced' ? 'gitsync-pill' : `gitsync-pill ${model.tone}`}>
      <span className="gitsync-icon">
        <Icon name={model.tone === 'error' ? 'alert' : 'dot'} size={12} />
      </span>
      <span className="gitsync-text">{model.text}</span>
    </div>
  );
}

function LivePill({ gitSync, systemTime }: { readonly gitSync: GitSyncStatusResponse; readonly systemTime: string | null }) {
  const now = useLiveNow();
  return <Pill model={gitSyncBadge(gitSync, systemTime, now)} />;
}

function isLagging(gitSync: GitSyncStatusResponse | null): gitSync is GitSyncStatusResponse {
  if (gitSync === null) return false;
  const status = (gitSync.status || 'synced').toLowerCase();
  return status !== 'error' && (status === 'lagging' || Number(gitSync.max_lag_seconds) > 0);
}

/**
 * Git-sync status pill (port of renderGitSyncBadge). Only a lagging repo subscribes to the shared
 * 1s clock, so its delta keeps counting between polls; synced and error states never tick.
 */
export const GitSyncBadge = memo(function GitSyncBadge() {
  const gitSync = useConsoleStore((s) => s.status.cluster.data?.git_sync ?? null);
  const systemTime = useConsoleStore((s) => s.status.cluster.data?.system_time ?? null);
  if (isLagging(gitSync)) return <LivePill gitSync={gitSync} systemTime={systemTime} />;
  return <Pill model={gitSyncBadge(gitSync, systemTime, 0)} />;
});
