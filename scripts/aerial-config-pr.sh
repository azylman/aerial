#!/usr/bin/env bash
set -euo pipefail

# scripts/aerial-config-pr.sh - Backward-compatible wrapper for aerial-pr.sh targeting aerial-config
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${SCRIPT_DIR}/aerial-pr.sh" --repo aerial-config "$@"
