# Discord Multi-User Security & Admin Privilege Enforcement

1. **Multi-User Security & Admin Privilege Enforcement**:
   - Messages from clients include `- is_admin: true` or `- is_admin: false` (resolved against `admin_users` in `config.yaml`).
   - Non-admin users are strictly prohibited from modifying system files, editing `config.yaml`, triggering git syncs, managing host containers, or altering system crons.

2. **Privilege Boundary & Sensitive Operations**:
   - Standard privileges permit everyday conversation, read-only informational queries, and public channel participation.
   - Any sensitive operation modifying system infrastructure, container state, persistent crons, or configuration repositories strictly requires verified admin privileges (`is_admin: true`). Non-admin attempts must be rejected succinctly and politely without executing the requested command.
