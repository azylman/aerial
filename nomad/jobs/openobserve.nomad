job "openobserve" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "openobserve" {
    count = 1

    restart {
      attempts = 5
      interval = "15m"
      delay    = "15s"
      mode     = "fail"
    }

    network {
      port "http" {
        to = 5080
      }
    }

    task "openobserve" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "public.ecr.aws/zinclabs/openobserve:v1.0.4"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        mounts = [
          {
            type     = "volume"
            target   = "/data"
            source   = "aerial-openobserve-data"
            readonly = false
          }
        ]
      }

      env {
        ZO_ROOT_USER_EMAIL           = "admin@aerial.local"
        ZO_DATA_DIR                  = "/data"
        ZO_BASE_URI                  = "/openobserve"
        ZO_TELEMETRY                 = "false"
        ZO_AUTO_QUERY_ENABLED        = "true"
        ZO_COLS_PER_RECORD_LIMIT     = "5000"
        ZO_INGEST_FLATTEN_LEVEL      = "2"
        ZO_WIDENING_SCHEMA_EVOLUTION = "true"
        ZO_MEMORY_CACHE_MAX_SIZE     = "512"
        ZO_QUICK_MODE_ENABLED        = "false"
        ZO_QUICK_MODE_FORCE_ENABLED  = "false"
        ZO_QUICK_MODE_NUM_FIELDS     = "2000"
        ZO_QUICK_MODE_STRATEGY       = "both"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .OPENOBSERVE_ROOT_USER_PASSWORD }}ZO_ROOT_USER_PASSWORD="{{ .OPENOBSERVE_ROOT_USER_PASSWORD }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/openobserve.env"
        env         = true
      }

      service {
        name     = "openobserve"
        port     = "http"
        provider = "nomad"

        check {
          name     = "openobserve-health"
          type     = "http"
          path     = "/openobserve/healthz"
          interval = "15s"
          timeout  = "5s"
          check_restart {
            limit           = 3
            grace           = "120s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu        = 500
        memory     = 2048
        memory_max = 4096
      }
    }
  }
}
