#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == -* ]]; then
  inject_model=1
  skip_next=0
  for arg in "$@"; do
    if [[ "${skip_next}" -eq 1 ]]; then
      skip_next=0
      continue
    fi
    case "${arg}" in
      --prompt|--print|-p|--add-dir|--agent|--conversation|--effort|--input-format|--json-schema|--log-file|--mode|--output-format|--print-timeout|--project|--settings)
        skip_next=1
        ;;
      --model|--model=*|--version|-v|--help|-h)
        inject_model=0
        break
        ;;
    esac
  done
  if [[ "${inject_model}" -eq 1 ]]; then
    exec /usr/local/bin/agy.real --model "${AGY_DEFAULT_MODEL:-gemini-4-hydrogen-low}" "$@"
  fi
fi

exec /usr/local/bin/agy.real "$@"
