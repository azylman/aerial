variable "image_tag" {
  type    = string
  default = "latest"
}

job "webhooks-router" {
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

  group "webhooks-router" {
    count = 1

    network {
      port "http" {
        to = 4020
      }
    }

    task "webhooks-router" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-webhooks-router:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/coverage/webhooks-router:/coverage:rw",
        ]
      }

      env {
        CONFIG_PATH = "/local/webhooks-router.yaml"
        NOMAD_ADDR  = "http://${attr.unique.network.ip-address}:4646"
        HANGAR_URL    = "http://hangar.aerial"
        BRAIN_URL     = "http://brain.aerial"
        INFISICAL_URL = "http://infisical.aerial"
        PORT          = "4020"
        GOCOVERDIR  = "/coverage"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/webhooks-router" -}}
{{- with nomadVar "nomad/jobs/webhooks-router" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/webhooks-router.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/infisical-mcp" }}
{{ with nomadVar "nomad/jobs/infisical-mcp" }}
{{ if .client_id }}INFISICAL_CLIENT_ID="{{ .client_id }}"{{ end }}
{{ if .client_secret }}INFISICAL_CLIENT_SECRET="{{ .client_secret }}"{{ end }}
{{ if .project_id }}INFISICAL_PROJECT_ID="{{ .project_id }}"{{ end }}
{{ end }}
{{ end }}
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ if .POSTGRES_PASSWORD }}POSTGRES_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ if .SYSTEM_CHANNEL_ID }}SYSTEM_CHANNEL_ID="{{ .SYSTEM_CHANNEL_ID }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/webhooks-router.env"
        env         = true
      }

      service {
        name     = "webhooks-router"
        port     = "http"
        provider = "nomad"

        check {
          name     = "webhooks-router-health"
          type     = "http"
          path     = "/healthz"
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
