import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { StrictMode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { BuildDetailAPIResponse, DocketTaskLookup, VanadisBuildDTO } from '../api/types';
import { useConsoleStore } from '../store/store';
import { Drawer, type DrawerPanels, drawerTitle, type OpenDrawer } from './Drawer';

const PANELS: DrawerPanels = {
  service: ({ target }) => <p>service body {target.name}</p>,
  build: ({ target }) => <p>build body {target.id}</p>,
  task: ({ target }) => <p>task body {target.id}</p>,
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

const lookup = (id: number, title: string | null): DocketTaskLookup => ({
  id,
  task: title === null ? null : { id, title, status: 'open', created_at: '2026-11-08T11:00:00Z', updated_at: '2026-11-08T11:00:00Z' },
});

describe('drawerTitle', () => {
  it.each<{ name: string; drawer: OpenDrawer; detail: BuildDetailAPIResponse | null; task?: DocketTaskLookup; want: string }>([
    { name: 'service', drawer: { kind: 'service', name: 'aerial-eri' }, detail: null, want: 'DIAGNOSTICS // AERIAL-ERI' },
    { name: 'build before its detail', drawer: { kind: 'build', id: 'build-17-ab' }, detail: null, want: 'BUILD // BUILD-17-AB' },
    { name: 'build with its detail', drawer: { kind: 'build', id: 'b-1' }, detail: detail('b-1', 'Ship it #1'), want: 'BUILD // SHIP IT #1' },
    { name: 'legacy entry key', drawer: { kind: 'build', id: 'b-1' }, detail: detail('b-1', 'Old', 'entry'), want: 'BUILD // OLD' },
    { name: 'untitled build', drawer: { kind: 'build', id: 'b-1' }, detail: detail('b-1', ''), want: 'BUILD // B-1' },
    { name: 'another build\'s detail', drawer: { kind: 'build', id: 'b-2' }, detail: detail('b-1', 'Ship it #1'), want: 'BUILD // B-2' },
    { name: 'task before its detail', drawer: { kind: 'task', id: 7 }, detail: null, want: 'TASK // #7' },
    { name: 'task with its detail', drawer: { kind: 'task', id: 7 }, detail: null, task: lookup(7, 'Fix the widget'), want: 'TASK // #7 FIX THE WIDGET' },
    { name: 'untitled task', drawer: { kind: 'task', id: 7 }, detail: null, task: lookup(7, ''), want: 'TASK // #7' },
    { name: 'another task\'s detail', drawer: { kind: 'task', id: 8 }, detail: null, task: lookup(7, 'Fix the widget'), want: 'TASK // #8' },
    { name: 'missing task', drawer: { kind: 'task', id: 7 }, detail: null, task: lookup(7, null), want: 'TASK // #7' },
  ])('$name', ({ drawer, detail: d, task, want }) => {
    expect(drawerTitle(drawer, d, task ?? null)).toBe(want);
  });
});

function Trigger() {
  return <button type="button">trigger</button>;
}

type Step =
  | { readonly open: OpenDrawer }
  | { readonly close: 'escape' | 'button' | 'overlay' | 'store' }
  | { readonly key: string }
  | { readonly detail: BuildDetailAPIResponse }
  | { readonly task: DocketTaskLookup }
  | { readonly transition: 'transitionend' | 'transitioncancel'; readonly property: string; readonly on: 'panel' | 'child' };

interface Row {
  name: string;
  preopen?: OpenDrawer;
  reducedMotion?: boolean;
  steps: readonly Step[];
  wantActive: boolean;
  wantTitle: string | null;
  wantBody: string | null;
  wantFocus: 'trigger' | 'dialog' | 'page';
}

const SERVICE: OpenDrawer = { kind: 'service', name: 'haro' };
const BUILD: OpenDrawer = { kind: 'build', id: 'b-42' };
const TASK: OpenDrawer = { kind: 'task', id: 42 };
const PANEL_FADE: Step = { transition: 'transitionend', property: 'opacity', on: 'panel' };

/** jsdom has no TransitionEvent, so build a bubbling Event carrying the property name. */
function dispatchTransition(target: Element, type: string, propertyName: string): void {
  const event = new Event(type, { bubbles: true });
  Object.defineProperty(event, 'propertyName', { value: propertyName });
  target.dispatchEvent(event);
}

function runStep(step: Step, container: HTMLElement): void {
  const panel = container.querySelector('.diagnostic-drawer') as Element;
  if ('open' in step) {
    if (panel.getAttribute('aria-hidden') === 'true') screen.getByRole('button', { name: 'trigger' }).focus();
    useConsoleStore.getState().openDrawer(step.open);
  } else if ('close' in step) {
    if (step.close === 'escape') fireEvent.keyDown(window, { key: 'Escape' });
    else if (step.close === 'button') fireEvent.click(container.querySelector('.drawer-close') as Element);
    else if (step.close === 'overlay') fireEvent.click(container.querySelector('.drawer-overlay') as Element);
    else useConsoleStore.getState().closeDrawer();
  } else if ('key' in step) {
    fireEvent.keyDown(window, { key: step.key });
  } else if ('detail' in step) {
    useConsoleStore.getState().setBuildDetail(step.detail);
  } else if ('task' in step) {
    useConsoleStore.getState().setDocketTask(step.task);
  } else {
    const target = step.on === 'panel' ? panel : (panel.querySelector('.drawer-close') as Element);
    dispatchTransition(target, step.transition, step.property);
  }
}

describe('Drawer', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  });
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it.each<Row>([
    { name: 'closed renders no body and nothing active', steps: [], wantActive: false, wantTitle: null, wantBody: null, wantFocus: 'page' },
    { name: 'opens the service panel', steps: [{ open: SERVICE }], wantActive: true, wantTitle: 'DIAGNOSTICS // HARO', wantBody: 'service body haro', wantFocus: 'dialog' },
    { name: 'opens the build panel', steps: [{ open: BUILD }], wantActive: true, wantTitle: 'BUILD // B-42', wantBody: 'build body b-42', wantFocus: 'dialog' },
    {
      name: 'retitles an open build drawer when its detail lands',
      steps: [{ open: BUILD }, { detail: detail('b-42', 'Ship it #1') }],
      wantActive: true,
      wantTitle: 'BUILD // SHIP IT #1',
      wantBody: 'build body b-42',
      wantFocus: 'dialog',
    },
    { name: 'opens the task panel', steps: [{ open: TASK }], wantActive: true, wantTitle: 'TASK // #42', wantBody: 'task body 42', wantFocus: 'dialog' },
    {
      name: 'retitles an open task drawer when its detail lands',
      steps: [{ open: TASK }, { task: lookup(42, 'Wire the tab') }],
      wantActive: true,
      wantTitle: 'TASK // #42 WIRE THE TAB',
      wantBody: 'task body 42',
      wantFocus: 'dialog',
    },
    {
      name: 'retargets an open task drawer',
      steps: [{ open: TASK }, { open: { kind: 'task', id: 9 } }],
      wantActive: true,
      wantTitle: 'TASK // #9',
      wantBody: 'task body 9',
      wantFocus: 'dialog',
    },
    { name: 'shows a drawer already open at mount', preopen: BUILD, steps: [], wantActive: true, wantTitle: 'BUILD // B-42', wantBody: 'build body b-42', wantFocus: 'dialog' },
    {
      name: 'closing a drawer open at mount returns focus to the page',
      preopen: SERVICE,
      steps: [{ close: 'store' }, PANEL_FADE],
      wantActive: false,
      wantTitle: null,
      wantBody: null,
      wantFocus: 'page',
    },
    { name: 'switches targets while open', steps: [{ open: SERVICE }, { open: BUILD }], wantActive: true, wantTitle: 'BUILD // B-42', wantBody: 'build body b-42', wantFocus: 'dialog' },
    { name: 'ignores keys other than Escape', steps: [{ open: SERVICE }, { key: 'Enter' }], wantActive: true, wantTitle: 'DIAGNOSTICS // HARO', wantBody: 'service body haro', wantFocus: 'dialog' },
    { name: 'close drops .active at once and keeps the body while fading', steps: [{ open: SERVICE }, { close: 'store' }], wantActive: false, wantTitle: null, wantBody: 'service body haro', wantFocus: 'trigger' },
    { name: 'Escape closes and the body unmounts after the fade', steps: [{ open: SERVICE }, { close: 'escape' }, PANEL_FADE], wantActive: false, wantTitle: null, wantBody: null, wantFocus: 'trigger' },
    { name: 'close button closes', steps: [{ open: SERVICE }, { close: 'button' }, PANEL_FADE], wantActive: false, wantTitle: null, wantBody: null, wantFocus: 'trigger' },
    { name: 'overlay click closes', steps: [{ open: BUILD }, { close: 'overlay' }, PANEL_FADE], wantActive: false, wantTitle: null, wantBody: null, wantFocus: 'trigger' },
    {
      name: 'a cancelled fade also unmounts the body',
      steps: [{ open: SERVICE }, { close: 'store' }, { transition: 'transitioncancel', property: 'opacity', on: 'panel' }],
      wantActive: false,
      wantTitle: null,
      wantBody: null,
      wantFocus: 'trigger',
    },
    {
      name: 'transform and child transitions do not end the fade',
      steps: [
        { open: SERVICE },
        { close: 'store' },
        { transition: 'transitionend', property: 'transform', on: 'panel' },
        { transition: 'transitionend', property: 'opacity', on: 'child' },
      ],
      wantActive: false,
      wantTitle: null,
      wantBody: 'service body haro',
      wantFocus: 'trigger',
    },
    {
      name: 'reduced motion unmounts the body without waiting',
      reducedMotion: true,
      steps: [{ open: SERVICE }, { close: 'store' }],
      wantActive: false,
      wantTitle: null,
      wantBody: null,
      wantFocus: 'trigger',
    },
    {
      name: 'reopening during the close fade keeps the new body',
      steps: [{ open: SERVICE }, { close: 'store' }, { open: BUILD }, PANEL_FADE],
      wantActive: true,
      wantTitle: 'BUILD // B-42',
      wantBody: 'build body b-42',
      wantFocus: 'dialog',
    },
  ])('$name', ({ preopen, reducedMotion, steps, wantActive, wantTitle, wantBody, wantFocus }) => {
    vi.stubGlobal('matchMedia', (query: string) => ({ matches: reducedMotion === true && query === '(prefers-reduced-motion: reduce)' }));
    if (preopen) useConsoleStore.getState().openDrawer(preopen);
    // StrictMode, like main.tsx: mount effects run twice.
    const { container } = render(
      <StrictMode>
        <Trigger />
        <Drawer panels={PANELS} />
      </StrictMode>,
    );
    for (const step of steps) act(() => runStep(step, container));

    const panel = container.querySelector('.diagnostic-drawer') as HTMLElement;
    expect(container.querySelectorAll('.active').length).toBe(wantActive ? 2 : 0);
    expect(screen.queryByRole('dialog')?.getAttribute('aria-modal') ?? null).toBe(wantTitle === null ? null : 'true');
    expect(wantTitle === null ? screen.queryByRole('dialog') : screen.getByRole('dialog', { name: wantTitle })).toBe(wantTitle === null ? null : panel);
    expect(container.querySelector('.drawer-body')?.textContent).toBe(wantBody ?? '');
    const focus = { trigger: screen.getByRole('button', { name: 'trigger' }), dialog: panel, page: document.body }[wantFocus];
    expect(document.activeElement).toBe(focus);
  });
});
