# Implementation Plan: Status Dashboard Favicon from Discord Bot Profile Picture

## Overview
Add standard and responsive favicon icons (multi-resolution `.ico`, 32x32 PNG, 16x16 PNG, and 180x180 Apple Touch Icon) to Aerial's Permet Command HUD status dashboard (`/dashboard`), sourced directly from the official Discord bot profile avatar.

## Problem Statement
The Aerial status dashboard (`dashboard/static/index.html`) currently does not define any `<link rel="icon">` tags or supply a `favicon.ico` / `favicon.png`. When browsers load `/dashboard/`, they show the generic blank sheet or attempt to request `/favicon.ico`, which returns 404.

## Proposed Changes

### 1. Static Favicon Assets (`dashboard/static/`)
Extract the high-resolution (1024x1024) Discord bot avatar and downsample using area-averaging downsampling into:
- `dashboard/static/favicon.ico`: Multi-resolution ICO container (16x16, 32x32, 48x48) with 32-bit RGBA.
- `dashboard/static/favicon-32x32.png`: 32x32 PNG (metadata stripped).
- `dashboard/static/favicon-16x16.png`: 16x16 PNG (metadata stripped).
- `dashboard/static/favicon.png`: Standard 32x32 PNG favicon fallback (metadata stripped).
- `dashboard/static/apple-touch-icon.png`: 180x180 PNG touch icon for iOS / mobile bookmarks (metadata stripped).

### 2. Dashboard HTML Header (`dashboard/static/index.html`)
Add standard modern favicon and Apple touch icon link tags inside `<head>`, along with PWA cyberpunk theme metadata:
```html
    <!-- Favicon & Touch Icons -->
    <link rel="icon" type="image/png" sizes="32x32" href="favicon-32x32.png">
    <link rel="icon" type="image/png" sizes="16x16" href="favicon-16x16.png">
    <link rel="shortcut icon" href="favicon.ico">
    <link rel="apple-touch-icon" sizes="180x180" href="apple-touch-icon.png">
    <meta name="theme-color" content="#06050a">
    <meta name="apple-mobile-web-app-title" content="Aerial HUD">
```

### 3. Git Attributes Hygiene (`.gitattributes`)
Ensure `.gitattributes` marks image assets as binary:
```gitattributes
*.ico binary
*.png binary
```

### 4. Verification & Unit Tests (`dashboard/main_test.go`)
- Update `TestEmbeddedStaticAssetsIntegrity` in `dashboard/main_test.go` to include `favicon.ico`, `favicon.png`, `favicon-32x32.png`, `favicon-16x16.png`, and `apple-touch-icon.png` in `requiredFiles`.
- Add integration tests verifying:
  - GET `/dashboard/favicon.ico` returns 200 with Content-Type `image/x-icon` or `image/vnd.microsoft.icon` and valid ETag.
  - GET `/dashboard/favicon.ico` with `If-None-Match` returns 304 Not Modified.
  - GET `/dashboard/favicon.png` returns 200 with Content-Type `image/png`.
  - GET `/dashboard/apple-touch-icon.png` returns 200 with Content-Type `image/png`.
  - GET `/favicon.ico` returns 200 with Content-Type `image/x-icon` or `image/vnd.microsoft.icon`.
  - `index.html` contains the favicon and apple-touch-icon link tags.

## Review Gates
- Stage 2: Adversarial Systems Critic reviewed and approved with 5 targeted amendments.
- Stage 3: Human Review Checkpoint (Bypassed for Tier 1 autonomous execution).
- Stage 4: Continuous TDD implementation in scratch repository.
- Stage 5: Pre-Flight Verification (`./scripts/verify.sh --staged` + package tests).
- Stage 6: Commit and PR submission via `scripts/aerial-pr.sh submit`.
