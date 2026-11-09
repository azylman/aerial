import {
  type ComponentType,
  startTransition,
  Suspense,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  ViewTransition,
} from 'react';
import { type DrawerState, useConsoleStore } from '../store/store';
import { Icon } from './Icon';
import './Drawer.css';

export type OpenDrawer = Exclude<DrawerState, { readonly kind: 'closed' }>;

/** Body component per drawer kind; each receives its own narrowed target. */
export type DrawerPanels = {
  readonly [K in OpenDrawer['kind']]: ComponentType<{ readonly target: Extract<OpenDrawer, { readonly kind: K }> }>;
};

/** Header text, ported from openDiagnosticDrawer and openBuildDetailDrawer. */
export function drawerTitle(drawer: OpenDrawer): string {
  return drawer.kind === 'service' ? `DIAGNOSTICS // ${drawer.name.toUpperCase()}` : `BUILD // ${drawer.id.toUpperCase()}`;
}

const closeDrawer = (): void => useConsoleStore.getState().closeDrawer();

function DrawerBody({ drawer, panels }: { readonly drawer: OpenDrawer; readonly panels: DrawerPanels }) {
  switch (drawer.kind) {
    case 'service': {
      const Panel = panels.service;
      return <Panel target={drawer} />;
    }
    case 'build': {
      const Panel = panels.build;
      return <Panel target={drawer} />;
    }
  }
}

function DrawerPanel({ drawer, panels }: { readonly drawer: OpenDrawer; readonly panels: DrawerPanels }) {
  const titleId = useId();
  const panelRef = useRef<HTMLElement>(null);

  // Focus moves into the dialog on open and returns to whatever opened it on close.
  useLayoutEffect(() => {
    const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    panelRef.current?.focus();
    return () => trigger?.focus();
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key !== 'Escape') return;
      event.preventDefault();
      closeDrawer();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  return (
    <aside
      ref={panelRef}
      className="diagnostic-drawer active"
      role="dialog"
      aria-modal="true"
      aria-labelledby={titleId}
      tabIndex={-1}
    >
      <div className="drawer-header">
        <div className="drawer-title">
          <span className="drawer-icon">
            <Icon name="bolt" size={16} />
          </span>
          <h3 id={titleId}>{drawerTitle(drawer)}</h3>
        </div>
        <button type="button" className="drawer-close" aria-label="Close drawer" onClick={closeDrawer}>
          <Icon name="x" size={22} />
        </button>
      </div>
      <div className="drawer-body">
        <Suspense fallback={null}>
          <DrawerBody drawer={drawer} panels={panels} />
        </Suspense>
      </div>
    </aside>
  );
}

/**
 * The single drawer host. Any code opens or closes a drawer through the store's drawer slice;
 * the host mirrors that slice into local state inside startTransition, which is what lets
 * <ViewTransition> run the enter/exit animation on the compositor instead of animating layout.
 * Closed drawers are unmounted, so they cost nothing while hidden.
 */
export function Drawer({ panels }: { readonly panels: DrawerPanels }) {
  const [shown, setShown] = useState<DrawerState>(() => useConsoleStore.getState().drawer);

  useEffect(() => {
    const show = (next: DrawerState): void => startTransition(() => setShown(next));
    show(useConsoleStore.getState().drawer);
    return useConsoleStore.subscribe((state, prev) => {
      if (state.drawer !== prev.drawer) show(state.drawer);
    });
  }, []);

  if (shown.kind === 'closed') return null;
  return (
    <>
      <ViewTransition enter="drawer-fade" exit="drawer-fade" default="none">
        <div className="drawer-overlay active" aria-hidden="true" onClick={closeDrawer} />
      </ViewTransition>
      <ViewTransition enter="drawer-slide" exit="drawer-slide" default="none">
        <DrawerPanel drawer={shown} panels={panels} />
      </ViewTransition>
    </>
  );
}
