variable "image_tag" {
  type    = string
  default = "latest"
}

job "haro" {
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

  group "haro" {
    count = 1

    network {
      port "http" {
        to = 4005
      }
    }

    task "haro" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-haro:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
      }

      env {
        BRAIN_INTERNAL_URL = "http://eri.aerial"
        VANADIS_URL = "http://vanadis.aerial"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/haro" -}}
{{- with nomadVar "nomad/jobs/haro" -}}
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
        name     = "haro"
        port     = "http"
        provider = "nomad"

        check {
          name     = "haro-health"
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
        cpu        = 50
        memory     = 128
        memory_max = 256
      }
    }
  }
}
