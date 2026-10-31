variable "image_tag" {
  type    = string
  default = "latest"
}

variable "node_pool" {
  type    = string
  default = "calibarn"
}

job "vanadis" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = var.node_pool

  update {
    max_parallel      = 1
    canary            = 1
    auto_promote      = true
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
        mounts = [
          {
            type     = "volume"
            target   = "/share"
            source   = "aerial-share"
            readonly = true
          }
        ]
      }

      env {
        CONFIG_PATH   = "/local/vanadis.yaml"
        ERI_URL       = "http://eri.aerial"
        PORT          = "4025"
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
{{ if .POSTGRES_PASSWORD }}DATABASE_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
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
        cpu        = 500
        memory     = 256
        memory_max = 512
      }
    }
  }
}
