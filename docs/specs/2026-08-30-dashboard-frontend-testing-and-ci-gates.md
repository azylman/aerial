# Architectural Specification: Permet HUD Frontend Testing & CI Verification Gates

**Author**: Aerial Monorepo Architecture Squad (Frontend, CI/CD, SRE, Devil's Advocate)  
**Date**: 2026-08-30  
**Target Services**: `dashboard/static/app.js`, `dashboard/static/index.html`, `dashboard/main_test.go`, `.github/workflows/docker-publish.yml`, `.githooks/pre-push`  
**Status**: APPROVED & IMPLEMENTED  

---

## 1. Context & Motivation

Aerial's Permet HUD (`dashboard/static/app.js`) is an embedded single-page dashboard serving real-time queue states, cluster telemetry, CI build pipelines, memory archives, and recurring schedules.

Previously, frontend changes lacked automated pre-flight testing and static analysis gates. A missing function definition (`formatAgentsviewSessionUrl`) in `app.js` caused an unhandled runtime `ReferenceError` during task execution, cascading into a false `OFFLINE` alarm.

This specification defines a dual-tier testing and verification architecture that enforces zero-dependency runtime execution, sub-second test execution, and airtight prevention of UI crashes.

---

## 2. Architecture & Design Principles

### 2.1 The Zero-Dependency Standard
- No heavy bundlers (Webpack, Vite, Rollup, Babel).
- No massive `node_modules` footprint in the repository.
- Standard Node.js native test runner (`node:test` + `node:assert/strict`) for JavaScript unit testing.

### 2.2 Dual-Tier Verification Gates
1. **Tier 1 (Local & Go Test Suite - `dashboard/main_test.go`)**:
   - Embed-aware static AST & token audit in Go.
   - Validates DOM ID bindings between `index.html` and `app.js`.
   - Asserts mandatory JS helper symbol definitions.
   - Audits against hardcoded private IPs and sensitive token leaks.
   - Runs locally in sub-millisecond time on any environment with Go.
2. **Tier 2 (CI & Node.js Native Suite - `dashboard/static/app.test.js`)**:
   - Executes pure JS unit tests and state fixtures via `node --test`.
   - Validates `formatAgentsviewSessionUrl`, `parseValidTimestampMs`, `escapeHtml`, `formatUptime`, `getTriggerBadge`.
   - Tests state permutations (empty queue, 1 active task, queued tasks, retries, malformed payloads).
   - Validates deterministic tickers via simulated timestamps.

---

## 3. Component Specification

### 3.1 JavaScript Unit Test Suite (`dashboard/static/app.test.js`)
- Uses `node:test` (`describe`, `it`) and `node:assert/strict`.
- Extracts or imports pure JS functions from `app.js`.
- Test cases:
  - **`formatAgentsviewSessionUrl`**: Valid UUIDs, prefixed IDs (`antigravity-cli/<uuid>`), slash trimming, URL encoding, fallback on null/empty/invalid types.
  - **`parseValidTimestampMs`**: ISO strings, rejection of Go zero-time (`0001-01-01T00:00:00Z`), rejection of pre-2020 epochs, null handling.
  - **`escapeHtml`**: Sanitization of `&`, `<`, `>`, `"`, `'` to block XSS.
  - **`formatUptime`**: Conversion of seconds to `Xd Xh Xm`, `Xh Xm`, `Xm Xs`, `Xs`.
  - **`formatElapsedTicker`**: Formatting elapsed duration into `⏱ HH:MM:SS` and `⏱ MM:SSs`.
  - **`getTriggerBadge`**: Mapping `cron`, `reminder`, `http`/`api`, `discord` to icons and CSS classes.

### 3.2 Go Embed Test Suite (`dashboard/main_test.go`)
- `TestEmbeddedStaticAssetsIntegrity`: Verifies all static files exist in embed FS.
- `TestAppJSDeclaredFunctions`: Asserts that all required function symbols are present in `app.js`.
- `TestIndexHTMLRequiredDOMBindings`: Asserts that `index.html` contains all IDs queried by `app.js` (`summary-tasks-val`, `summary-tasks-sub`, `tasks-count-badge`, `active-tasks-container`, `overall-status`, etc.).
- `TestZeroPersonalDataAndHardcodedIPs`: Scans static assets for regex matching private subnets (`192.168.`) or unredacted token patterns.

### 3.3 CI/CD Integration (`.github/workflows/docker-publish.yml`)
- Executes `node --test dashboard/static/*.test.js` under the `test` job when `dashboard/**` changes.
- Validates syntax via `node --check dashboard/static/app.js` under the `lint` job.

### 3.4 Pre-Push Hook & Makefile
- `.githooks/pre-push`: Runs Go tests and JavaScript tests before allowing git push.
- `Makefile`: Includes `test` and `lint` targets running both Go and Frontend suites.

---

## 4. Verification & Sign-Off Criteria
1. 100% pass rate on `dashboard/main_test.go` and `dashboard/static/app.test.js`.
2. Total test execution time under 1 second.
3. Zero npm dependencies required in production Docker image.
