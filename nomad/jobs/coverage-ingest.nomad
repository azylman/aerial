job "coverage-ingest" {
  datacenters = ["dc1"]
  type        = "batch"

  # Target quiet-zero core server node
  constraint {
    attribute = "${node.class}"
    operator  = "regexp"
    value     = "quiet-zero|haos"
  }

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
        image        = "golang:1.24-alpine"
        network_mode = "host"
        command      = "/bin/sh"
        args         = ["/local/ingest.sh"]
        volumes = [
          "/mnt/data/supervisor/share/coverage:/share/coverage:rw",
          "/mnt/data/supervisor/share/aerial:/share/aerial:ro"
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
        data = <<'EOH'
#!/bin/sh
set -eu

echo "📦 [coverage-ingest] Installing postgresql client..."
apk add --no-cache postgresql-client >/dev/null 2>&1

COVERAGE_DIR="/share/coverage"
SOURCE_DIR="/share/aerial"

if [ ! -d "$COVERAGE_DIR" ]; then
  echo "ℹ️ [coverage-ingest] No coverage directory found at $COVERAGE_DIR."
  exit 0
fi

# 1. Trigger live in-memory flush on active microservices
echo "🔄 [coverage-ingest] Requesting live runtime flush on registered services..."
curl -fsS -m 2 http://127.0.0.1:4020/debug/coverage/flush >/dev/null 2>&1 || true

# 2. Sweep service directories
for svc_path in "$COVERAGE_DIR"/*; do
  [ -d "$svc_path" ] || continue
  svc_name="$(basename "$svc_path")"

  # Check if any counter files exist
  count=$(find "$svc_path" -maxdepth 1 -name "covcounters.*" 2>/dev/null | wc -l)
  if [ "$count" -eq 0 ]; then
    continue
  fi

  echo "⚡ [coverage-ingest] Ingesting $count counter file(s) for service: $svc_name"
  tmp_out="/tmp/${svc_name}.cov"

  if ! go tool covdata textfmt -i="$svc_path" -o="$tmp_out" 2>/dev/null; then
    echo "⚠️ [coverage-ingest] Failed to convert binary covdata for $svc_name"
    rm -f "$tmp_out"
    continue
  fi

  src_path="$SOURCE_DIR/$svc_name"
  if [ ! -d "$src_path" ]; then
    src_path="$SOURCE_DIR"
  fi

  sql_tmp="/tmp/${svc_name}.sql"
  echo "BEGIN;" > "$sql_tmp"

  (cd "$src_path" && go tool cover -func="$tmp_out" 2>/dev/null || true) | while IFS= read -r line; do
    case "$line" in
      total:*|"") continue ;;
    esac
    
    file_loc=$(echo "$line" | awk '{print $1}')
    fn_name=$(echo "$line" | awk '{print $2}')
    pct=$(echo "$line" | awk '{print $3}' | tr -d '%')
    file_name="${file_loc%%:*}"

    # Sanitize quotes for safe SQL formatting
    fn_name=$(echo "$fn_name" | sed "s/'/''/g")
    file_name=$(echo "$file_name" | sed "s/'/''/g")

    if [ -n "$file_name" ] && [ -n "$fn_name" ] && [ -n "$pct" ]; then
      cat <<EOS >> "$sql_tmp"
INSERT INTO production_code_coverage (service, file, function_name, coverage_percent, last_executed_at, updated_at)
VALUES ('$svc_name', '$file_name', '$fn_name', $pct, CASE WHEN $pct > 0 THEN CURRENT_TIMESTAMP ELSE NULL END, CURRENT_TIMESTAMP)
ON CONFLICT (service, file, function_name) DO UPDATE SET
  coverage_percent = EXCLUDED.coverage_percent,
  last_executed_at = CASE WHEN EXCLUDED.coverage_percent > 0 THEN CURRENT_TIMESTAMP ELSE production_code_coverage.last_executed_at END,
  updated_at = CURRENT_TIMESTAMP;
EOS
    fi
  done

  echo "COMMIT;" >> "$sql_tmp"

  if psql "$POSTGRES_URL" -f "$sql_tmp" >/dev/null 2>&1; then
    echo "✅ [coverage-ingest] Upserted coverage for $svc_name into production_code_coverage."
    # Purge processed counters and meta to prevent inode leak
    rm -f "$svc_path"/covcounters.* "$svc_path"/covmeta.*
  else
    echo "🚨 [coverage-ingest] SQL upsert failed for $svc_name"
  fi

  rm -f "$tmp_out" "$sql_tmp"
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
