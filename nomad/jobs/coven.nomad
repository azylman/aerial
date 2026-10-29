variable "image_tag" {
  type    = string
  default = "latest"
}

variable "node_pool" {
  type    = string
  default = "calibarn"
}

job "coven" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = var.node_pool

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "5s"
    healthy_deadline  = "1m"
    progress_deadline = "2m"
    auto_revert       = true
  }

  group "coven" {
    count = 1

    network {
      port "http" {
        to = 4008
      }
    }

    task "coven" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-coven:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        mounts = [
          {
            type     = "volume"
            target   = "/share"
            source   = "aerial-share"
            readonly = true
          }
        ]
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/coven" -}}
{{- with nomadVar "nomad/jobs/coven" -}}
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
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ if .SYSTEM_CHANNEL_ID }}SYSTEM_CHANNEL_ID="{{ .SYSTEM_CHANNEL_ID }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/postgres.env"
        env         = true
      }

      service {
        name     = "coven"
        port     = "http"
        provider = "nomad"

        check {
          name     = "coven-health"
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
        cpu        = 500
        memory     = 256
        memory_max = 512
      }
    }
  }
}
