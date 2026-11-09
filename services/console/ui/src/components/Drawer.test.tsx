import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { useConsoleStore } from '../store/store';
import { Drawer, type DrawerPanels, drawerTitle, type OpenDrawer } from './Drawer';

const PANELS: DrawerPanels = {
  service: ({ target }) => <p>service body {target.name}</p>,
  build: ({ target }) => <p>build body {target.id}</p>,
};

describe('drawerTitle', () => {
  it.each<{ drawer: OpenDrawer; want: string }>([
    { drawer: { kind: 'service', name: 'aerial-eri' }, want: 'DIAGNOSTICS // AERIAL-ERI' },
    { drawer: { kind: 'build', id: 'build-17-ab' }, want: 'BUILD // BUILD-17-AB' },
  ])('$want', ({ drawer, want }) => {
    expect(drawerTitle(drawer)).toBe(want);
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
