import {
  type ComponentType,
  Suspense,
  type TransitionEvent,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from 'react';
import type { BuildDetailAPIResponse } from '../api/types';
import { buildDrawerLabel } from '../store/selectors';
import { type ConsoleState, type DrawerState, useConsoleStore } from '../store/store';
import { Icon } from './Icon';
import './Drawer.css';

export type OpenDrawer = Exclude<DrawerState, { readonly kind: 'closed' }>;

/** Body component per drawer kind; each receives its own narrowed target. */
export type DrawerPanels = {
  readonly [K in OpenDrawer['kind']]: ComponentType<{ readonly target: Extract<OpenDrawer, { readonly kind: K }> }>;
};

/**
 * Header text, ported from openDiagnosticDrawer and openBuildDetailDrawer. A build drawer shows
 * its id until the build's detail payload arrives, then the build's title.
 */
export function drawerTitle(drawer: OpenDrawer, buildDetail: BuildDetailAPIResponse | null): string {
  return drawer.kind === 'service'
    ? `DIAGNOSTICS // ${drawer.name.toUpperCase()}`
    : `BUILD // ${buildDrawerLabel(buildDetail, drawer.id).toUpperCase()}`;
}

const selectBuildDetail = (s: ConsoleState): BuildDetailAPIResponse | null => s.builds.detail.data;

/** The header reads the build detail slice, so only it re-renders when a detail poll lands. */
function DrawerHeading({ drawer, id }: { readonly drawer: OpenDrawer; readonly id: string }) {
  const title = useConsoleStore((s) => drawerTitle(drawer, drawer.kind === 'build' ? selectBuildDetail(s) : null));
  return <h3 id={id}>{title}</h3>;
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

const REDUCED_MOTION = '(prefers-reduced-motion: reduce)';
const prefersReducedMotion = (): boolean => window.matchMedia?.(REDUCED_MOTION).matches === true;

function DrawerPanel({
  drawer,
  open,
  panels,
  onClosed,
}: {
  readonly drawer: OpenDrawer | null;
  readonly open: boolean;
  readonly panels: DrawerPanels;
  readonly onClosed: () => void;
}) {
  const titleId = useId();
  const panelRef = useRef<HTMLElement>(null);
  const triggerRef = useRef<HTMLElement | null>(null);

  // Focus moves into the dialog on open and returns to whatever opened it on close, without
  // scrolling the page: closing leaves the page where the user left it. The restore runs as a
  // layout effect, not a cleanup: cleanups run before React's own post-commit focus restore,
  // which would hand focus straight back to the still-mounted panel.
  useLayoutEffect(() => {
    if (open) {
      // StrictMode re-runs this on mount with the panel already focused; keep the real trigger.
      const active = document.activeElement;
      if (active !== panelRef.current) triggerRef.current = active instanceof HTMLElement ? active : null;
      panelRef.current?.focus();
      return;
    }
    // The shell stays mounted, so let go of focus even when there is no trigger to return to.
    panelRef.current?.blur();
    triggerRef.current?.focus({ preventScroll: true });
    triggerRef.current = null;
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key !== 'Escape') return;
      event.preventDefault();
      closeDrawer();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [open]);

  // The close transition has finished (or was cut short) once the panel's own opacity settles.
  // React only gives transitionend a TransitionEvent interface, so read the native property name.
  const onTransitionDone = (event: TransitionEvent<HTMLElement>): void => {
    if (event.target === event.currentTarget && event.nativeEvent.propertyName === 'opacity') onClosed();
  };

  return (
    <aside
      ref={panelRef}
      className={open ? 'diagnostic-drawer active' : 'diagnostic-drawer'}
      role="dialog"
      aria-modal="true"
      aria-labelledby={titleId}
      aria-hidden={open ? undefined : true}
      tabIndex={-1}
      onTransitionEnd={onTransitionDone}
      onTransitionCancel={onTransitionDone}
    >
      <div className="drawer-header">
        <div className="drawer-title">
          <span className="drawer-icon">
            <Icon name="bolt" size={16} />
          </span>
          {drawer ? <DrawerHeading drawer={drawer} id={titleId} /> : <h3 id={titleId} />}
        </div>
        <button type="button" className="drawer-close" aria-label="Close drawer" onClick={closeDrawer}>
          <Icon name="x" size={22} />
        </button>
      </div>
      <div className="drawer-body">
        {drawer && (
          <Suspense fallback={null}>
            <DrawerBody drawer={drawer} panels={panels} />
          </Suspense>
        )}
      </div>
    </aside>
  );
}

const selectDrawer = (s: ConsoleState): DrawerState => s.drawer;

/**
 * The single drawer host, built like the vanilla drawer: the overlay and panel shell stay mounted
 * and the store's drawer slice toggles their `.active` class in the same commit, so open and close
 * are a class flip plus a compositor-only CSS transition. Opening renders the new target's body at
 * once; closing keeps the last body painted until the panel's fade ends, then drops it, so a
 * closed drawer renders no body and costs nothing while hidden.
 */
export function Drawer({ panels }: { readonly panels: DrawerPanels }) {
  const drawer = useConsoleStore(selectDrawer);
  const [prev, setPrev] = useState(drawer);
  const [closing, setClosing] = useState<OpenDrawer | null>(null);
  if (drawer !== prev) {
    setPrev(drawer);
    // Reopening while the close transition runs drops the outgoing body for the new one.
    setClosing(drawer.kind === 'closed' && prev.kind !== 'closed' ? prev : null);
  }

  // Reduced motion turns the transition off, so no transitionend would ever arrive.
  useLayoutEffect(() => {
    if (closing && prefersReducedMotion()) setClosing(null);
  }, [closing]);

  const open = drawer.kind !== 'closed';
  return (
    <>
      <div className={open ? 'drawer-overlay active' : 'drawer-overlay'} aria-hidden="true" onClick={closeDrawer} />
      <DrawerPanel drawer={open ? drawer : closing} open={open} panels={panels} onClosed={() => setClosing(null)} />
    </>
  );
}
