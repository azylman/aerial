# Architectural Specification: Permet Memory & Facts HUD Viewer

**Date**: 2026-08-30  
**Status**: APPROVED & IMPLEMENTING  
**Authors**: Aerial Engineering Squad, Arcane, Distributed Systems Architect, Cyberpunk UX Lead, Devil's Advocate  

---

## 1. Overview & Objective

Aerial continuously extracts semantic facts, user preferences, system configurations, and operational routines from Discord interactions, storing them in SQLite (`facts` table in `/data/aerial.db`).

This specification establishes a **stateless, isolated HTTP architecture** to visualize and search these memory records in the **Aerial Command HUD (`aerial-dashboard`)** without multi-container SQLite sharing or `-shm` locking issues.

---

## 2. System Architecture & Boundaries

```
┌────────────────────────────────────────────────────────┐
│                   BROWSER (Permet HUD)                 │
│  - Top Navigation with #telemetry & #memory tabs       │
│  - Instant search (150ms debounce) & category pills    │
│  - Shimmer skeleton loaders & glitch recovery alert    │
└───────────────────────────┬────────────────────────────┘
                            │ GET /api/facts
                            ▼
┌────────────────────────────────────────────────────────┐
│              EDGE PROXY (aerial-proxy:80)              │
│  - Routes /api/ -> aerial-dashboard:8080/api/          │
│  - Routes / -> aerial-dashboard:8080/                  │
│  - Routes /conversations/ -> aerial-agentsview:8080/   │
└───────────────────────────┬────────────────────────────┘
                            │ GET /api/facts
                            ▼
┌────────────────────────────────────────────────────────┐
│             DASHBOARD SERVICE (aerial-dashboard)       │
│  - 100% Stateless: NO SQLite mounts, NO CGO dependencies│
│  - Singleton http.Client with connection pooling       │
│  - Strict parameter validation & SSRF protection       │
│  - Graceful fallback with structured degraded envelope │
└───────────────────────────┬────────────────────────────┘
                            │ GET http://brain:8080/facts (aerial-net)
                            ▼
┌────────────────────────────────────────────────────────┐
│             BRAIN SERVICE (aerial-brain)               │
│  - Sole owner of /data/aerial.db (Single writer/reader)│
│  - Context-bound queries (800ms) on read connection    │
│  - Parameterized queries with LIKE wildcard escaping   │
│  - Omission of heavy embedding BLOB vectors            │
└───────────────────────────┬────────────────────────────┘
```

---

## 3. Database Layer (`brain/pkg/db/`)

### Schema & Indexing
```sql
CREATE INDEX IF NOT EXISTS idx_facts_category_created_at ON facts(category, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_created_at ON facts(created_at DESC);
```

### Query Signature
`GetFactsPaginated(database *sql.DB, filter FactsFilter) (*FactsResult, error)`
- Parameterized dynamic `WHERE` builder.
- Escapes SQLite `LIKE` wildcards (`%`, `_`).
- Bounded limits (`1 <= limit <= 100`, default `50`).
- Excludes `embedding BLOB` to prevent memory bloat.

---

## 4. API Endpoints

### `brain` Endpoint: `GET /facts`
- Query Params: `limit`, `offset`, `category`, `q`.
- Response:
```json
{
  "facts": [
    {
      "id": 1,
      "category": "user_preference",
      "fact_text": "Arcane prefers concise responses and dark mode aesthetics",
      "importance": 0.95,
      "thread_id": "1543513328691707944",
      "created_at": "2026-08-30T06:50:54Z"
    }
  ],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```

### `dashboard` Endpoint: `GET /api/facts`
- Proxy target: `http://brain:8080/facts`.
- Parameter sanitization & SSRF immunity.
- Graceful degradation on upstream failure:
```json
{
  "facts": [],
  "total": 0,
  "status": "degraded",
  "error": "Brain service unreachable. Retrying..."
}
```

---

## 5. Reverse Proxy Configuration (`proxy/default.conf`)

Add a generic route forwarding all `/api/` traffic to `aerial-dashboard:8080/api/` before the fallback `location /` handler:
```nginx
location /api/ {
    proxy_pass http://aerial-dashboard:8080/api/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

---

## 6. Frontend HUD Architecture (`dashboard/static/`)

1. **SPA Hash Navigation**: `#telemetry` vs `#memory` with browser history support and polling pause on inactive views.
2. **Skeleton Shimmer**: 6-card holographic wireframes during API fetch.
3. **Cyberpunk Permet Aesthetic**: Space Grotesk headings, JetBrains Mono body, neon category pills (Purple, Cyan, Green, Amber), and mini importance score bars.
4. **Safety & XSS Defense**: Safe DOM construction and sanitized search keyword match highlighting.
