job "postgres-exporter" {
  datacenters = ["dc1"]
  type        = "service"

  # Target quiet-zero core server node
  constraint {
    attribute = "${node.class}"
    operator  = "regexp"
    value     = "quiet-zero|haos"
  }

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "postgres-exporter" {
    count = 1

    network {
      mode = "host"
      port "metrics" {
        static = 9187
      }
    }

    task "postgres-exporter" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "prometheuscommunity/postgres-exporter:v0.20.1"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        security_opt = ["no-new-privileges:true"]
        args = [
          "--log.format=json"
        ]
      }

      env {
        PG_EXPORTER_DISABLE_SETTINGS_METRICS = "true"
        PG_EXPORTER_AUTO_DISCOVER_DATABASES  = "false"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .POSTGRES_PASSWORD }}DATA_SOURCE_NAME="postgresql://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/postgres.env"
        env         = true
      }

      service {
        name     = "postgres-exporter"
        port     = "metrics"
        provider = "nomad"

        check {
          name     = "postgres-exporter-http"
          type     = "http"
          path     = "/"
          interval = "15s"
          timeout  = "3s"
          check_restart {
            limit           = 3
            grace           = "60s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu        = 50
        memory     = 64
        memory_max = 128
      }
    }
  }
}
