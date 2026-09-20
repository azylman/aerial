#!/usr/bin/env bash
set -euo pipefail

# scripts/aerial-sidecars-pr.sh - Backward-compatible wrapper for aerial-pr.sh targeting aerial-sidecars
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${SCRIPT_DIR}/aerial-pr.sh" --repo aerial-sidecars "$@"
