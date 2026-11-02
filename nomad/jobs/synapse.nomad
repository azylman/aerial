variable "image_tag" {
  type    = string
  default = "latest"
}

job "synapse" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 1
    auto_promote      = true
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "synapse" {
    count = 1

    network {
      port "http" {
        to = 4020
      }
    }

    task "synapse" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-synapse:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
      }

      env {
        CONFIG_PATH = "/local/synapse.yaml"
        NOMAD_ADDR  = "http://${attr.unique.network.ip-address}:4646"
        HANGAR_URL    = "http://hangar.aerial"
        ERI_URL       = "http://eri.aerial"
        INFISICAL_URL = "http://infisical.aerial"
        VANADIS_URL   = "http://vanadis.aerial"
        HARO_URL      = "http://haro.aerial"
        PORT          = "4020"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/synapse" -}}
{{- with nomadVar "nomad/jobs/synapse" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/synapse.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/infisical-mcp" }}
{{ with nomadVar "nomad/jobs/infisical-mcp" }}
{{ if .client_id.Value }}INFISICAL_CLIENT_ID="{{ .client_id.Value }}"{{ end }}
{{ if .client_secret.Value }}INFISICAL_CLIENT_SECRET="{{ .client_secret.Value }}"{{ end }}
{{ if .project_id.Value }}INFISICAL_PROJECT_ID="{{ .project_id.Value }}"{{ end }}
{{ end }}
{{ end }}
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .INFISICAL_CLIENT_ID.Value }}INFISICAL_CLIENT_ID="{{ .INFISICAL_CLIENT_ID.Value }}"{{ end }}
{{ if .INFISICAL_CLIENT_SECRET.Value }}INFISICAL_CLIENT_SECRET="{{ .INFISICAL_CLIENT_SECRET.Value }}"{{ end }}
{{ if .INFISICAL_PROJECT_ID.Value }}INFISICAL_PROJECT_ID="{{ .INFISICAL_PROJECT_ID.Value }}"{{ end }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ if .POSTGRES_PASSWORD }}DATABASE_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ if .DISCORD_BOT_TOKEN }}DISCORD_TOKEN="{{ .DISCORD_BOT_TOKEN }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/synapse.env"
        env         = true
      }

      service {
        name     = "synapse"
        port     = "http"
        provider = "nomad"

        check {
          name     = "synapse-health"
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
