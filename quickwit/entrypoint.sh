#!/bin/sh
set -e

# Forward termination signals to Quickwit
trap 'kill -TERM "$QW_PID" 2>/dev/null; wait "$QW_PID"' TERM INT

# Start Quickwit
quickwit run --config /quickwit/config/quickwit.yaml &
QW_PID=$!

# Initialize docker-logs index asynchronously in background
(
  echo "[Quickwit Init] Waiting for Quickwit REST API on http://127.0.0.1:7280..."
  for i in $(seq 1 30); do
    if quickwit index list --endpoint http://127.0.0.1:7280 >/dev/null 2>&1; then
      if ! quickwit index list --endpoint http://127.0.0.1:7280 2>/dev/null | grep -q "docker-logs"; then
        echo "[Quickwit Init] Creating docker-logs index from /quickwit/config/docker-logs.yaml..."
        quickwit index create --index-config /quickwit/config/docker-logs.yaml --endpoint http://127.0.0.1:7280 || true
      else
        echo "[Quickwit Init] docker-logs index already exists."
      fi
      break
    fi
    sleep 1
  done
) &

wait "$QW_PID"
