job "scheduler-mcp" {
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

  group "scheduler-mcp" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 4005
      }
    }

    task "scheduler-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-scheduler-mcp:latest"
        force_pull   = true
        network_mode = "host"
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/coverage/scheduler-mcp:/coverage:rw",
        ]
      }

      env {
        GOCOVERDIR = "/coverage"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/scheduler-mcp" -}}
{{- with nomadVar "nomad/jobs/scheduler-mcp" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/config.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .POSTGRES_PASSWORD }}DATABASE_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/postgres.env"
        env         = true
      }

      service {
        name     = "scheduler-mcp"
        port     = "http"
        provider = "nomad"

        check {
          name     = "scheduler-mcp-health"
          type     = "http"
          path     = "/health"
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
        cpu        = 100
        memory     = 128
        memory_max = 256
      }
    }
  }
}
