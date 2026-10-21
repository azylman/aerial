variable "image_tag" {
  type    = string
  default = "latest"
}

job "vanadis" {
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

  group "vanadis" {
    count = 1

    network {
      port "http" {
        to = 4025
      }
    }

    task "vanadis" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/coverage/vanadis:/coverage:rw",
        ]
      }

      env {
        CONFIG_PATH   = "/local/vanadis.yaml"
        BRAIN_URL     = "http://eri.aerial"
        PORT          = "4025"
        GOCOVERDIR    = "/coverage"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/vanadis" -}}
{{- with nomadVar "nomad/jobs/vanadis" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/vanadis.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .POSTGRES_PASSWORD }}POSTGRES_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/vanadis.env"
        env         = true
      }

      service {
        name     = "vanadis"
        port     = "http"
        provider = "nomad"

        check {
          name     = "vanadis-health"
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
