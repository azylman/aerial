import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { BuildDetailAPIResponse, VanadisBuildDTO } from '../api/types';
import { useConsoleStore } from '../store/store';
import { Drawer, type DrawerPanels, drawerTitle, type OpenDrawer } from './Drawer';

const PANELS: DrawerPanels = {
  service: ({ target }) => <p>service body {target.name}</p>,
  build: ({ target }) => <p>build body {target.id}</p>,
};

const build = (id: string, title: string): VanadisBuildDTO => ({
  id,
  thread_id: 't',
  tier: 1,
  title,
  objective: '',
  status: 'running',
  created_at: '2026-11-08T11:00:00Z',
  updated_at: '2026-11-08T11:00:00Z',
});
const detail = (id: string, title: string, key: 'build' | 'entry' = 'build'): BuildDetailAPIResponse => ({
  status: 'ok',
  chunks: [],
  [key]: build(id, title),
});

describe('drawerTitle', () => {
  it.each<{ name: string; drawer: OpenDrawer; detail: BuildDetailAPIResponse | null; want: string }>([
    { name: 'service', drawer: { kind: 'service', name: 'aerial-eri' }, detail: null, want: 'DIAGNOSTICS // AERIAL-ERI' },
    { name: 'build before its detail', drawer: { kind: 'build', id: 'build-17-ab' }, detail: null, want: 'BUILD // BUILD-17-AB' },
    { name: 'build with its detail', drawer: { kind: 'build', id: 'b-1' }, detail: detail('b-1', 'Ship it #1'), want: 'BUILD // SHIP IT #1' },
    { name: 'legacy entry key', drawer: { kind: 'build', id: 'b-1' }, detail: detail('b-1', 'Old', 'entry'), want: 'BUILD // OLD' },
    { name: 'untitled build', drawer: { kind: 'build', id: 'b-1' }, detail: detail('b-1', ''), want: 'BUILD // B-1' },
    { name: 'another build\'s detail', drawer: { kind: 'build', id: 'b-2' }, detail: detail('b-1', 'Ship it #1'), want: 'BUILD // B-2' },
  ])('$name', ({ drawer, detail: d, want }) => {
    expect(drawerTitle(drawer, d)).toBe(want);
  });
});

function Trigger() {
  return (
    <button type="button" onClick={() => useConsoleStore.getState().openDrawer({ kind: 'service', name: 'haro' })}>
      open haro
    </button>
  );
}

interface CloseRow {
  name: string;
  close: 'escape' | 'button' | 'overlay' | 'store';
}

describe('Drawer', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  });
  afterEach(cleanup);

  it('renders nothing while closed', () => {
    const { container } = render(<Drawer panels={PANELS} />);
    expect(container.innerHTML).toBe('');
  });

  it.each([
    { drawer: { kind: 'service', name: 'eri' }, wantTitle: 'DIAGNOSTICS // ERI', wantBody: 'service body eri' },
    { drawer: { kind: 'build', id: 'b-42' }, wantTitle: 'BUILD // B-42', wantBody: 'build body b-42' },
  ] as const)('opens the $drawer.kind panel as a labelled dialog', ({ drawer, wantTitle, wantBody }) => {
    render(<Drawer panels={PANELS} />);
    act(() => useConsoleStore.getState().openDrawer(drawer));
    const dialog = screen.getByRole('dialog', { name: wantTitle });
    expect(dialog.getAttribute('aria-modal')).toBe('true');
    expect(screen.getByText(wantBody)).toBeTruthy();
    expect(document.activeElement).toBe(dialog);
  });

  it('retitles an open build drawer when its detail lands', () => {
    render(<Drawer panels={PANELS} />);
    act(() => useConsoleStore.getState().openDrawer({ kind: 'build', id: 'b-1' }));
    expect(screen.getByRole('dialog', { name: 'BUILD // B-1' })).toBeTruthy();
    act(() => useConsoleStore.getState().setBuildDetail(detail('b-1', 'Ship it #1')));
    expect(screen.getByRole('dialog', { name: 'BUILD // SHIP IT #1' })).toBeTruthy();
  });

  it('shows a drawer that was already open when it mounted', () => {
    useConsoleStore.getState().openDrawer({ kind: 'build', id: 'b-1' });
    render(<Drawer panels={PANELS} />);
    expect(screen.getByRole('dialog', { name: 'BUILD // B-1' })).toBeTruthy();
  });

  it.each<CloseRow>([
    { name: 'Escape closes', close: 'escape' },
    { name: 'close button closes', close: 'button' },
    { name: 'overlay click closes', close: 'overlay' },
    { name: 'store close (from anywhere) closes', close: 'store' },
  ])('$name and returns focus to the trigger', ({ close }) => {
    const { container } = render(
      <>
        <Trigger />
        <Drawer panels={PANELS} />
      </>,
    );
    const trigger = screen.getByRole('button', { name: 'open haro' });
    act(() => trigger.focus());
    act(() => trigger.click());
    expect(screen.getByRole('dialog', { name: 'DIAGNOSTICS // HARO' })).toBeTruthy();

    act(() => {
      if (close === 'escape') fireEvent.keyDown(window, { key: 'Escape' });
      else if (close === 'button') screen.getByRole('button', { name: 'Close drawer' }).click();
      else if (close === 'overlay') fireEvent.click(container.querySelector('.drawer-overlay') as Element);
      else useConsoleStore.getState().closeDrawer();
    });

    expect(screen.queryByRole('dialog')).toBeNull();
    expect(useConsoleStore.getState().drawer).toEqual({ kind: 'closed' });
    expect(document.activeElement).toBe(trigger);
  });

  it('ignores keys other than Escape', () => {
    render(<Drawer panels={PANELS} />);
    act(() => useConsoleStore.getState().openDrawer({ kind: 'service', name: 'eri' }));
    act(() => {
      fireEvent.keyDown(window, { key: 'Enter' });
    });
    expect(screen.getByRole('dialog')).toBeTruthy();
  });
});
