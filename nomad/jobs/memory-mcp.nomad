variable "image_tag" {
  type    = string
  default = "latest"
}

job "memory-mcp" {
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

  group "memory-mcp" {
    count = 1

    network {
      port "http" {
        to = 4008
      }
    }

    task "memory-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-memory-mcp:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        volumes = [
          "/share/aerial-config:/config:ro",
          "/mnt/data/supervisor/share/coverage/memory-mcp:/coverage:rw",
        ]
      }

      env {
        GOCOVERDIR = "/coverage"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/memory-mcp" -}}
{{- with nomadVar "nomad/jobs/memory-mcp" -}}
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
        name     = "memory-mcp"
        port     = "http"
        provider = "nomad"

        check {
          name     = "memory-mcp-health"
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
        memory     = 64
        memory_max = 128
      }
    }
  }
}
