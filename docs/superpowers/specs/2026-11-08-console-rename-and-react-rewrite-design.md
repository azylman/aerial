# Console: Rename `dashboard` → `console` and React Rewrite — Design

Status: approved by pilot (2026-11-08). Tracks docket Task #4 (migrated from azylman/aerial#1399).

## 1. Intent

The `dashboard` service is the operator workspace: live telemetry, active agent tasks, schedules,
the memory archive, and the Vanadis build queue. Its name collides conceptually with `homepage`
(landing portal), `gatus` (uptime), and Grafana dashboards. Renaming it to **console** makes its role
obvious.

Its frontend is a ~2,900-line vanilla JS monolith (`static/app.js`) plus ~3,200 lines of CSS and a
`node:test` suite that loads `app.js` into a `vm` sandbox to reach pure functions. Every render is an
`innerHTML` rebuild driven by 1s/5s timers. It is hard to extend and churns the DOM on every poll.
The rewrite moves it to React 19 + strict TypeScript + Zustand + Vitest so future console features
are cheap to add, and makes it **much faster**: speed is a primary goal, proven with before/after
measurements.

The pilot wants this run ambitiously: full scope, driven end to end by the bot, with every code
change shipped through Vanadis.

## 2. Goals

- Rename the service everywhere it is live (code, image, k8s objects, routes, DNS, CI, scripts,
  config, homepage, gatus, rules/skills that describe it) with old URLs redirecting.
- Port the frontend feature-for-feature: 5 views (Telemetry, Active Tasks, Schedules, Memory
  Archive, Build Queue), the service diagnostic drawer, the build detail drawer, quick-launch dock,
  git-sync badge, deployment pipeline, keyboard shortcuts, hash-based tab routing, copy buttons,
  search highlighting, filter pills, live tickers and countdowns.
- Apply the polish listed in Task #4: inline SVG icons replacing emoji chrome, `<button type="button">`
  + `aria-label` on icon buttons, `tabular-nums` on live numbers, `min-width: 0` on flex/grid text
  children, `overscroll-behavior: contain` on drawers, `color-scheme: dark`, timers paused while
  `document.hidden`.
- Render isolation: an update to one store slice re-renders zero sibling feature components
  (asserted in tests).
- **Much faster**, proven by before/after benchmarks against the targets in §8. Speed is a primary
  goal, not a side effect.

## 3. Non-goals

- New console features or new views. (Layout and visual identity stay as they are.)
- Changing any `/api/*` response shape or the Go backend logic beyond routing/prefix changes and
  serving the new bundle.
- Showing Marida's stack in the console. The console keeps reading the Aerial-stack services it reads
  today.
- Renaming Grafana "dashboards" or anything else that merely contains the word.
- Rewriting historical docs under `docs/specs/` and `docs/superpowers/` that mention the dashboard.

## 4. Anti-goals (failure even if it "works")

- A visible regression in any existing interaction the pilot uses daily.
- Broken bookmarks: `/dashboard/...` and `dashboard.lan.zylman.com` must keep landing on the console.
- Any `dangerouslySetInnerHTML`, `innerHTML`, or `document.getElementById` in the new UI.
- Committed build artifacts drifting from source.
- A window where the cluster tries to pull an image tag that was never published.

## 5. Constraints

- **Execution:** every code change ships through Vanadis. Separate builds per stage are fine.
  Benchmarks are **not** Vanadis work; the bot runs them in the Discord thread from its own scratch
  workspace.
- **Deploy model (current reality, not what Task #4 says):** Nomad, hangar, and mesh-proxy are gone.
  The service runs on k3s via Flux:
  - `k8s/core-services.yaml`: ClusterRole `aerial-dashboard-reader`, ServiceAccount `dashboard-sa`,
    ClusterRoleBinding `aerial-dashboard-reader-binding`, ConfigMap `dashboard-config` (inline
    `dashboard.yaml`), Deployment `dashboard`, Service `dashboard` (ports 8080/80 → 8084).
  - `k8s/gateways-and-networkpolicies.yaml`: `/dashboard` → `/dashboard/` redirects and
    `/dashboard/` PathPrefix backendRefs on both the LAN and edge homepage routes,
    `dashboard-lan-route` (host `dashboard.lan.zylman.com`), and a NetworkPolicy allow-list entry.
  - Image `ghcr.io/azylman/aerial-dashboard:${TAG_AERIAL_DASHBOARD:=latest}`. Synapse derives tag keys
    generically from the image name (`TAG_` + upper-snake), so `aerial-console` becomes
    `TAG_AERIAL_CONSOLE` with no synapse code change.
  - Static assets are baked into the image (`COPY services/dashboard/static`), served by
    `AssetRegistry` (`assets.go`) with content-hash cache busting.
  - `*.aerial` DNS resolves to the k8s Service name, so `dashboard.aerial` becomes `console.aerial`.
- **Go service** registers every route twice (`/x` and `/dashboard/x`) and strips `/dashboard` for
  static assets (`main.go`).
- **Repo rules:** generic/no personal data in `azylman/aerial`; table-driven Go tests; 95% coverage
  floor; `scripts/verify.sh` gates; no `time.Sleep`; config in YAML, not env vars.
- **Current library versions (npm, 2026-11-08):** react/react-dom 19.3.1, zustand 5.1.0, vite 8.3.6,
  vitest 5.1.0, typescript 7.0.2, @vitejs/plugin-react 6.2.1, @testing-library/react 16.3.3.

## 6. Approaches considered

1. **One Vanadis build for everything.** Simple to track, but the rename and rewrite have different
   risk profiles and the benchmark gate would sit inside a running build. Rejected by the pilot.
2. **Bot implements inline in the thread, outside Vanadis.** Maximum control, but breaks the
   Vanadis-for-everything rule and is fragile across daemon restarts. Rejected by the pilot.
3. **Chosen: separate Vanadis builds per stage, benchmarks in the thread.** Pilot's direction.
   The bot authors each build plan, reviews chunk output, and runs the before/after benchmarks
   itself between builds.

Within approach 3, the rewrite ships **side by side** (bot's call): the React app is served at
`/console/next/` while the vanilla UI stays at `/console/`, then a final small build flips it. This
gives a live comparison, a fair benchmark against the same backend, and a one-line rollback.
The alternative (single big-bang cutover PR) was rejected as a ~6k-line review with no live
comparison.

## 7. Rollout

### Step 0 — Baseline benchmark (thread, now)
Bot runs the benchmark harness against the current vanilla UI and records `before-vanilla.json`.

### Build A — Rename (Vanadis)
Ordered so no manifest references an unpublished image and no health check goes red mid-rollout:
1. `azylman/aerial` — code/CI rename: `git mv services/dashboard services/console`, Go module path,
   binary `aerial-console`, Go symbols and log prefixes, config key `dashboard:` → `console:` and
   default path `/config/console.yaml`, Dockerfile, `docker-publish.yml` filter + matrix entry
   (`image_name: aerial-console`), `dependabot.yml`, `Makefile`, `scripts/verify.sh`,
   `scripts/verify.ps1`, `scripts/test-linux.ps1`, `scripts/check-coverage.sh`, `config.example.yaml`,
   Go routes `/console/*` (keep bare `/x` routes; drop `/dashboard/*` handlers since the gateway
   redirects), frontend `getApiBase()` prefix, page `<title>`. k8s manifests untouched, so the live
   Deployment keeps running the last `aerial-dashboard` image until chunk 2.
2. `azylman/aerial` — k8s rename: all objects in §5 renamed (`console`, `console-sa`,
   `aerial-console-reader[-binding]`, `console-config` with key `console.yaml`, `CONFIG_PATH`),
   image `aerial-console:${TAG_AERIAL_CONSOLE:=latest}`, `kubernetes-mcp` (which also runs as
   `dashboard-sa`) moved to `console-sa`, routes `/console/` + `console.lan.zylman.com`, 301
   redirects `/dashboard` and `/dashboard/*` → `/console/*` (path preserved), old LAN host 301 →
   new host, NetworkPolicy allow-list entry. A temporary alias Service `dashboard` selecting the
   console pods keeps `dashboard.aerial` resolving for homepage and gatus until chunk 3 lands.
   Plus live docs: `README.md`, `rules/discord/06-architecture.md`,
   `services/homepage/config/custom.css` and its test, `services/docs-service/app/fallback/README.md`.
3. `azylman/aerial-config` — `homepage/services.yaml` (href `/console/`, `console.aerial` monitor and
   widget URLs), `services/gatus/gatus.yaml`, `README.md`, `docs/README.md`, `docs/architecture.md`.
   Delete the unreferenced `services/dashboard/dashboard.yaml` (the live config is the inline
   ConfigMap). Generic uses of the word "dashboard" (Grafana, Home Assistant, skills) stay.
4. `azylman/aerial` — remove the temporary `dashboard` alias Service.
5. `action` chunk — verify live: `/console/` serves, `/dashboard/*` and the old LAN host redirect,
   homepage widget and gatus check are green, no `dashboard`-named k8s objects remain.

### Build B — React rewrite, side by side (Vanadis)
All in `azylman/aerial`. Each chunk is independently green.
1. **Scaffold + pipeline.** `services/console/ui/` (Vite + React + strict TS + Vitest, exact-pinned
   versions, `package-lock.json`). Dockerfile gains a `node` build stage that runs `npm ci && npm run
   build` and copies output to `/app/static/next/` in the final image (final image stays Node-free;
   no built files committed). Go serves `/console/next/` (SPA fallback to its `index.html`) via
   the existing `AssetRegistry`. `verify.sh`, `check-coverage.sh`, Makefile, and CI run `tsc
   --noEmit`, Vitest with ≥95% coverage, and `vite build` when `services/console/ui/**` changes.
   `types.ts` (API response types derived from the Go handlers), `api.ts` (typed fetchers with
   AbortController), Zustand store skeleton.
2. **Pure logic modules + tests.** Port every pure function from `app.js` into `formatters.ts`,
   `cron.ts`, `markdown.ts` (renders to React elements, not HTML strings), `builds.ts` (queue state
   classification, chunk stats, timing, badges), `facts.ts` (sorting, reinforce parsing), `search.ts`
   (highlight → React segments). Port the relevant `app.test.js` cases to Vitest tables.
3. **App shell.** Header, tab bar with SVG icons and badges, hash routing, keyboard shortcuts,
   `useDocumentVisible`, `useLiveTicker` (one shared 1s clock), `usePollingSync` (5s, paused while
   hidden or tab inactive, matching today's per-tab polling), drawer host with `startTransition`
   + `<ViewTransition>` enter/exit. CSS: today's `style.css` split by feature into plain CSS files
   imported by their components; class names preserved so visuals match.
4. **Telemetry + Active Tasks + service diagnostic drawer.**
5. **Schedules** (cron cards, one-shots, run feed, pills, search, copy prompt, countdowns).
6. **Memory Archive** (fact cards, category pills, search highlight, metrics, copy).
7. **Build Queue + build detail drawer** (pills, metrics, cards, markdown chunk detail).

Store contract: `create<ConsoleStore>()()` with slices `status`, `schedules`, `memory`, `builds`,
`drawer`, `ui`. Poll ingestion structurally diffs incoming data against current state and keeps
unchanged object references. Components subscribe through narrow selectors (`useShallow` where
returning objects) with frozen empty fallbacks. Render-count tests assert slice isolation.

### Step B' — After benchmark (thread)
Bot runs the same harness against `/console/next/` and posts the comparison. If budgets fail, the
bot proposes fixes as a follow-up build before cutover.

### Build C — Cutover (Vanadis, single chunk)
Serve the React bundle at `/console/` (redirect `/console/next/*` → `/console/*`), delete vanilla
`static/app.js`, `static/style.css`, `static/index.html`, `app.test.js`, and the `node --check` /
`node --test` gates that targeted them. Keep favicons.

## 8. Benchmarks (thread-run, not committed)

Harness lives in the bot's scratch workspace. Raw CDP against headless Chromium under 4x CPU
throttle. The console Go binary runs locally with its upstream URLs pointed at a deterministic
fixture server (18 services, 6 deployments, 5 active tasks, 40 schedules, 50 runs, 150 facts,
25 multi-chunk builds), so both UIs see identical data. Metrics, p50/p95/max over repeated runs:

- `screen_ready` (cold load to first full paint) and gzipped JS/CSS bytes
- `poll_update` duration and `dom_nodes_mutated` over 20 polls with 0–1 changed items
- `tab_switch` click-to-paint across all 5 views
- `filter_input` input-to-paint for memory/schedule/build search and pill toggles
- `drawer_toggle` frame deltas and >50ms long tasks

### Baseline (vanilla UI, 2026-11-08, 4x CPU throttle, p50 unless noted)

- `screen_ready`: telemetry 390ms, schedules 405ms, builds 342ms, memory 1,163ms (TBT 893ms).
  ~20k DOM nodes on every view. JS 25 KB / CSS 11 KB gzipped; ~700 KB of API JSON per load.
- `tab_switch`: 139ms overall, p95 932ms; memory 873ms.
- `filter_input`: memory search 429ms, memory search clear 842ms, memory "all" pill 673ms,
  schedules search 218ms, other pills/searches 77–100ms.
- `drawer_toggle`: open ~100ms, close 170–190ms, 3–7 jank frames.
- `poll_update` with no data change: telemetry 179ms / 215 nodes mutated, schedules 183ms /
  317 nodes, tasks 85ms, builds 103ms. Hidden tabs still cost 70–138ms per poll.
- Interaction event duration: p50 160ms, p95 912ms.

### Budget (React at `/console/next/`, same harness and fixtures)

The pilot wants the rewrite **much faster**, so the cutover gate is:
- `screen_ready` p50 ≤ 300ms on every view, including memory.
- `tab_switch` and every `filter_input` p95 ≤ 100ms.
- `poll_update` with no data change: ~0 DOM nodes mutated and task ≤ 20ms; hidden tabs ~0 cost.
- `drawer_toggle`: ≤ 1 jank frame.
- Interaction event duration p95 ≤ 150ms.
- Gzipped JS ≤ 80 KB.

## 9. Left to the builder

File layout inside each feature folder, component granularity, exact CSS file split, test helper
design, Vite config details, how the fixture server is written, and harness implementation.
