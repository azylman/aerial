variable "image_tag" {
  type    = string
  default = "latest"
}

job "prospera" {
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

  group "prospera" {
    count = 1

    network {
      port "http" {
        static = 4019
        to     = 4019
      }
    }

    task "prospera" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image              = "ghcr.io/azylman/aerial-prospera:${var.image_tag}"
        ports              = ["http"]
        healthchecks {
          disable = true
        }
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/prospera" -}}
{{- with nomadVar "nomad/jobs/prospera" -}}
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
{{ if .GEMINI_HARNESS_API_KEY }}GEMINI_HARNESS_API_KEY="{{ .GEMINI_HARNESS_API_KEY }}"
GEMINI_API_KEY="{{ .GEMINI_HARNESS_API_KEY }}"{{ end }}
{{ end }}
{{ end }}
{{ if nomadVarExists "nomad/jobs/voice-call" }}
{{ with nomadVar "nomad/jobs/voice-call" }}
{{ if .TWILIO_ACCOUNT_SID }}TWILIO_ACCOUNT_SID="{{ .TWILIO_ACCOUNT_SID }}"{{ end }}
{{ if .TWILIO_AUTH_TOKEN }}TWILIO_AUTH_TOKEN="{{ .TWILIO_AUTH_TOKEN }}"{{ end }}
{{ if .TWILIO_FROM_NUMBER }}TWILIO_FROM_NUMBER="{{ .TWILIO_FROM_NUMBER }}"{{ end }}
{{ if .ALLOW_FROM }}ALLOW_FROM="{{ .ALLOW_FROM }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/prospera.env"
        env         = true
      }

      service {
        name     = "prospera"
        port     = "http"
        provider = "nomad"

        check {
          name     = "prospera-health"
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
