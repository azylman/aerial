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
        image        = "golang:1.27.1-alpine"
        network_mode = "host"
        command      = "/bin/sh"
        args         = ["/local/ingest.sh"]
        volumes = [
          "/mnt/data/supervisor/share/coverage:/share/coverage:rw",
          "/mnt/data/supervisor/share/aerial:/share/aerial:ro",
          "/mnt/data/supervisor/share/mirrormere:/share/mirrormere:ro"
        ]
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

COVERAGE_DIR="/share/coverage"

mkdir -p "$COVERAGE_DIR/mirrormere" "$COVERAGE_DIR/hass-proxy" "$COVERAGE_DIR/aura-farming" "$COVERAGE_DIR/photos-api" "$COVERAGE_DIR/photos-sync" "$COVERAGE_DIR/scratch-service" && chmod -R 0777 "$COVERAGE_DIR" 2>/dev/null || true
find "$COVERAGE_DIR" -type f -name "covcounters.*" -mtime +2 -delete 2>/dev/null || true
find "$COVERAGE_DIR" -type f -name "covmeta.*" -mtime +7 -delete 2>/dev/null || true

if [ ! -d "$COVERAGE_DIR" ]; then
  echo "ℹ️ [coverage-ingest] No coverage directory found at $COVERAGE_DIR."
  exit 0
fi

# 1. Trigger live in-memory flush on active microservices
echo "🔄 [coverage-ingest] Requesting live runtime flush on registered services..."
for flush_url in \
  "http://webhooks-router.aerial/debug/coverage/flush" \
  "http://vanadis.aerial/debug/coverage/flush" \
  "http://hangar.aerial/debug/coverage/flush" \
  "http://scheduler-mcp.aerial/debug/coverage/flush" \
  "http://discord-mcp.aerial/debug/coverage/flush" \
  "http://dashboard.aerial/debug/coverage/flush" \
  "http://brain.aerial/debug/coverage/flush" \
  "http://mirrormere-core.aerial/debug/coverage/flush" \
  "http://mirrormere-hass-proxy.aerial/debug/coverage/flush" \
  "http://aura-farming.aerial/debug/coverage/flush" \
  "http://photos-api.aerial/debug/coverage/flush" \
  "http://photos-sync.aerial/debug/coverage/flush" \
  "http://scratch-service.aerial/debug/coverage/flush" \
  "http://127.0.0.1:4012/debug/coverage/flush" \
  "http://127.0.0.1:4013/debug/coverage/flush" \
  "http://127.0.0.1:4014/debug/coverage/flush" \
  "http://127.0.0.1:4017/debug/coverage/flush" \
  "http://127.0.0.1:4020/debug/coverage/flush" \
  "http://127.0.0.1:4025/debug/coverage/flush" \
  "http://127.0.0.1:8087/debug/coverage/flush" \
  "http://127.0.0.1:4005/debug/coverage/flush" \
  "http://127.0.0.1:4001/debug/coverage/flush" \
  "http://127.0.0.1:8084/debug/coverage/flush" \
  "http://127.0.0.1:8088/debug/coverage/flush"; do
  curl -fsS -m 2 "$flush_url" >/dev/null 2>&1 || true
done

# 2. Sweep service directories
for svc_path in "$COVERAGE_DIR"/*; do
  [ -d "$svc_path" ] || continue
  svc_name="$(basename "$svc_path")"

  # Prune orphan counter files lacking a matching covmeta.<hash>
  for cf in "$svc_path"/covcounters.*; do
    [ -f "$cf" ] || continue
    base="$(basename "$cf")"
    rest="$${base#covcounters.}"
    hash="$${rest%%.*}"
    if [ ! -f "$svc_path/covmeta.$hash" ]; then
      echo "🧹 [coverage-ingest] Removing orphan counter file $base for $svc_name (missing covmeta.$hash)"
      rm -f "$cf"
    fi
  done

  # Check if any counter files exist
  count=$(find "$svc_path" -maxdepth 1 -name "covcounters.*" 2>/dev/null | wc -l)
  if [ "$count" -eq 0 ]; then
    continue
  fi

  echo "⚡ [coverage-ingest] Ingesting $count counter file(s) for service: $svc_name"
  sql_tmp="/tmp/$${svc_name}.sql"
  echo "BEGIN;" > "$sql_tmp"

  for meta_file in $(ls -1tr "$svc_path"/covmeta.* 2>/dev/null); do
    [ -f "$meta_file" ] || continue
    meta_base="$(basename "$meta_file")"
    hash="$${meta_base#covmeta.}"
    set -- "$svc_path"/covcounters."$hash".*
    [ -f "$1" ] || continue

    stage_dir="/tmp/stage_$${svc_name}_$${hash}"
    rm -rf "$stage_dir" && mkdir -p "$stage_dir"
    cp "$meta_file" "$svc_path"/covcounters."$hash".* "$stage_dir/"

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
          printf "INSERT INTO production_code_coverage (service, file, function_name, coverage_percent, last_executed_at, updated_at) VALUES (\x27%s\x27, \x27%s\x27, \x27%s\x27, %s, CASE WHEN %s > 0 THEN CURRENT_TIMESTAMP ELSE NULL END, CURRENT_TIMESTAMP) ON CONFLICT (service, file, function_name) DO UPDATE SET coverage_percent = CASE WHEN EXCLUDED.coverage_percent > 0 THEN EXCLUDED.coverage_percent ELSE production_code_coverage.coverage_percent END, last_executed_at = CASE WHEN EXCLUDED.coverage_percent > 0 THEN CURRENT_TIMESTAMP ELSE production_code_coverage.last_executed_at END, updated_at = CURRENT_TIMESTAMP;\n", svc, file_name, fn_name, pct, pct
        }
      }
    ' >> "$sql_tmp" || true

    rm -rf "$stage_dir"
  done

  if ! grep -q "^INSERT INTO" "$sql_tmp"; then
    echo "🚨 [coverage-ingest] Zero coverage rows extracted for $svc_name; preserving counter files"
    rm -f "$sql_tmp"
    continue
  fi

  echo "COMMIT;" >> "$sql_tmp"

  if psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -f "$sql_tmp" >/dev/null 2>&1; then
    echo "✅ [coverage-ingest] Upserted coverage for $svc_name into production_code_coverage."
    rm -f "$svc_path"/covcounters.*
    ls -1t "$svc_path"/covmeta.* 2>/dev/null | tail -n +5 | xargs -r rm -f
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
