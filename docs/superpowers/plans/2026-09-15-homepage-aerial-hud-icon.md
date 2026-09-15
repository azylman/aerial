# Implementation Plan: Homepage Panel Icon for Aerial Command HUD

## Overview
Update the Homepage dashboard card icon for the Aerial Command HUD service from the generic fallback `dashboard-icons.png` to `/dashboard/apple-touch-icon.png` (the high-resolution Discord bot profile avatar merged in PR #241).

## Background & Context
In PR #241, Aerial added the official Discord bot avatar as high-resolution favicon and touch assets in `dashboard/static/` (`apple-touch-icon.png`, 180x180; `favicon.png`, 32x32).
Homepage (`aerial-homepage`) serves as the root landing hub (`/`) through `aerial-proxy` and auto-discovers containers via Docker labels.
Its frontend supports root-relative icon URLs (`if (e.startsWith("http") || e.startsWith("/")) return <img src={e} ...>`).
Currently, `docker-compose.yml` configures:
`homepage.icon: "dashboard-icons.png"`
which pulls a generic third-party dashboard icon from jsdelivr CDN.

## Proposed Changes

### 1. Update Docker Service Label (`docker-compose.yml`)
In `docker-compose.yml` under service `dashboard:`, update line 363:
```yaml
-      homepage.icon: "dashboard-icons.png"
+      homepage.icon: "/dashboard/apple-touch-icon.png"
```
Using `/dashboard/apple-touch-icon.png` provides:
- High-DPI / Retina sharpness (180x180 scaled to 32px/48px).
- Zero third-party CDN external dependencies (jsdelivr).
- Direct internal routing via `aerial-proxy` to `aerial-dashboard`.
- 100% domain-agnostic and privacy-preserving (zero personal metadata).

### 2. Pre-Flight Verification & Checks
- Run `./scripts/verify.sh --staged`.
- Verify YAML parsing of `docker-compose.yml`.

## Review Gates
- Complexity Tier: **Tier 1** (Docker Compose label update).
- Review Gate: Solo Adversarial Systems Critic review (~30s).
- Execution: Autonomous continuous execution.
- Verification: `./scripts/verify.sh --staged` and YAML lint.
