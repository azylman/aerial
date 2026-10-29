job "coverage-ingest" {
  datacenters = ["dc1"]
  type        = "batch"

  node_pool   = "default"

  periodic {
    cron             = "0 4 * * *" # Daily at 4:00 AM UTC
    prohibit_overlap = true
    time_zone        = "UTC"
  }

  reschedule {
    attempts = 0
  }

  group "coverage" {
    count = 1

    network {
      mode = "host"
    }

    restart {
      attempts = 1
      interval = "5m"
      delay    = "15s"
      mode     = "fail"
    }

    task "ingest" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "golang:1.27.3-alpine"
        network_mode = "host"
        command      = "/bin/sh"
        args         = ["/local/ingest.sh"]
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .POSTGRES_PASSWORD }}POSTGRES_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ end }}
{{ else }}
POSTGRES_URL="postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable"
{{ end }}
EOH
        destination = "secrets/postgres.env"
        env         = true
      }

      template {
        data = <<EOH
#!/bin/sh
set -eu

echo "📦 [coverage-ingest] Installing postgresql client and curl..."
apk add --no-cache postgresql-client curl >/dev/null 2>&1

psql "$POSTGRES_URL" -c "DELETE FROM production_code_coverage WHERE service IN ('brain', 'memory-mcp', 'scheduler-mcp');" >/dev/null 2>&1 || true

echo "🔄 [coverage-ingest] Streaming and ingesting runtime coverage from registered services..."
for entry in \
  "webhooks-router|http://webhooks-router.aerial/debug/coverage/flush" \
  "vanadis|http://vanadis.aerial/debug/coverage/flush" \
  "hangar|http://hangar.aerial/debug/coverage/flush" \
  "haro|http://haro.aerial/debug/coverage/flush" \
  "coven|http://coven.aerial/debug/coverage/flush" \
  "discord-mcp|http://discord-mcp.aerial/debug/coverage/flush" \
  "kernel-mcp|http://kernel-mcp.aerial/debug/coverage/flush" \
  "dashboard|http://dashboard.aerial/debug/coverage/flush" \
  "eri|http://eri.aerial/debug/coverage/flush" \
  "prospera|http://prospera.aerial/debug/coverage/flush" \
  "mirrormere|http://mirrormere-core.aerial/debug/coverage/flush" \
  "hass-proxy|http://mirrormere-hass-proxy.aerial/debug/coverage/flush" \
  "aura-farming|http://aura-farming.aerial/debug/coverage/flush" \
  "photos-api|http://photos-api.aerial/debug/coverage/flush" \
  "photos-sync|http://photos-sync.aerial/debug/coverage/flush" \
  "scratch-service|http://scratch-service.aerial/debug/coverage/flush"; do
  svc_name="$${entry%%|*}"
  flush_url="$${entry#*|}"
  stage_dir="/tmp/stage_$${svc_name}"
  rm -rf "$stage_dir" && mkdir -p "$stage_dir"

  if ! curl -fsS -X POST -m 5 "$flush_url" 2>/dev/null | tar -xf - -C "$stage_dir" 2>/dev/null; then
    rm -rf "$stage_dir"
    continue
  fi

  count=$(find "$stage_dir" -maxdepth 1 -type f -name "covcounters.*" 2>/dev/null | wc -l)
  if [ "$count" -eq 0 ]; then
    rm -rf "$stage_dir"
    continue
  fi

  echo "⚡ [coverage-ingest] Ingesting $count counter file(s) for service: $svc_name"
  sql_tmp="/tmp/$${svc_name}.sql"
  printf "BEGIN;\nCREATE TEMP TABLE active_coverage_symbols (file TEXT NOT NULL, function_name TEXT NOT NULL, PRIMARY KEY (file, function_name)) ON COMMIT DROP;\n" > "$sql_tmp"

  go tool covdata func -i="$stage_dir" 2>/dev/null | awk -v svc="$svc_name" '
    /^total/ || NF < 3 { next }
    {
      file_name = $1; fn_name = $2; pct = $3
      sub(/:[0-9]+:$/, "", file_name)
      sub(/^.*\./, "", fn_name)
      sub(/%$/, "", pct)
      gsub(/\x27/, "\x27\x27", file_name)
      gsub(/\x27/, "\x27\x27", fn_name)
      if (file_name != "" && fn_name != "" && pct != "") {
        if (!truncated) { printf "TRUNCATE active_coverage_symbols;\n"; truncated = 1 }
        printf "INSERT INTO active_coverage_symbols (file, function_name) VALUES (\x27%s\x27, \x27%s\x27) ON CONFLICT DO NOTHING;\n", file_name, fn_name
        printf "INSERT INTO production_code_coverage (service, file, function_name, coverage_percent, last_executed_at, updated_at) VALUES (\x27%s\x27, \x27%s\x27, \x27%s\x27, %s, CASE WHEN %s > 0 THEN CURRENT_TIMESTAMP ELSE NULL END, CURRENT_TIMESTAMP) ON CONFLICT (service, file, function_name) DO UPDATE SET coverage_percent = CASE WHEN EXCLUDED.coverage_percent > 0 THEN EXCLUDED.coverage_percent ELSE production_code_coverage.coverage_percent END, last_executed_at = CASE WHEN EXCLUDED.coverage_percent > 0 THEN CURRENT_TIMESTAMP ELSE production_code_coverage.last_executed_at END, updated_at = CURRENT_TIMESTAMP;\n", svc, file_name, fn_name, pct, pct
      }
    }
  ' >> "$sql_tmp" || true

  rm -rf "$stage_dir"

  if ! grep -q "^INSERT INTO production_code_coverage" "$sql_tmp"; then
    echo "🚨 [coverage-ingest] Zero coverage rows extracted for $svc_name"
    rm -f "$sql_tmp"
    continue
  fi

  printf "DELETE FROM production_code_coverage p WHERE p.service = '%s' AND EXISTS (SELECT 1 FROM active_coverage_symbols LIMIT 1) AND NOT EXISTS (SELECT 1 FROM active_coverage_symbols a WHERE a.file = p.file AND a.function_name = p.function_name);\nCOMMIT;\n" "$svc_name" >> "$sql_tmp"

  if psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -f "$sql_tmp" >/dev/null 2>&1; then
    echo "✅ [coverage-ingest] Upserted coverage for $svc_name into production_code_coverage."
  else
    echo "🚨 [coverage-ingest] SQL upsert failed for $svc_name"
  fi

  rm -f "$sql_tmp"
done

echo "🎉 [coverage-ingest] Ingestion sweep completed."
EOH
        destination = "local/ingest.sh"
        perms       = "0755"
      }

      resources {
        cpu        = 200
        memory     = 256
        memory_max = 512
      }
    }
  }
}
