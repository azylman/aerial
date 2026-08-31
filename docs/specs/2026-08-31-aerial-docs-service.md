# Technical Specification: Aerial Documentation Service (`aerial-docs`)

## 1. Overview
The `aerial-docs` service provides living Markdown documentation rendering with embedded Mermaid.js diagrams directly from the user's private configuration repository (`${AERIAL_CONFIG_DIR}/docs`). It is namespaced under the `/docs/` subpath on the `aerial-proxy` reverse gateway.

## 2. Architecture & Topography
- **Engine**: Nginx Alpine (`nginx:1.27-alpine`) serving a Docsify SPA shell and vendored client-side libraries.
- **Reverse Proxy Routing**:
  - `aerial-proxy` routes `location /docs/` -> `http://aerial-docs:80/`
  - Exact match `location = /docs` issues an HTTP 301 redirect to `/docs/`
- **Volume Mount**:
  - `${AERIAL_CONFIG_DIR}:/share/aerial-config:ro`
  - Nginx document root points to `/share/aerial-config/docs`
  - Built-in fallback templates at `/usr/share/nginx/html/fallback/` ensure graceful degradation if `docs/` is uninitialized.

## 3. Resilience & Security Invariants
- **100% Vendored Assets**: Zero external CDN calls (`jsdelivr`, `unpkg`, `cdnjs`). All JavaScript, CSS, and Prism highlighters are baked into the Docker image.
- **XSS Mitigation**: Disabled raw script execution (`executeScript: false`) and enforced `X-Content-Type-Options: nosniff`.
- **Zero Host Lockup**: Bind mounts the parent configuration directory read-only (`:ro`), avoiding the Docker daemon host directory creation trap.
- **Two-Repository Invariant**: Core engine remains 100% domain-agnostic and free of user-specific data; documentation files reside in `aerial-config/docs/`.
