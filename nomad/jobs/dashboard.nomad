variable "image_tag" {
  type    = string
  default = "latest"
}

job "dashboard" {
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

  group "dashboard" {
    count = 1

    network {
      port "http" {
        to = 8084
      }
    }

    task "dashboard" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-dashboard:${var.image_tag}"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
      }

      artifact {
        source      = "http://hangar.aerial/internal/artifacts/dashboard.tar.gz"
        destination = "local/static"
      }

      env {
        CONFIG_PATH        = "/local/dashboard.yaml"
        AERIAL_CONFIG_PATH = "/local/config.yaml"
        NOMAD_ADDR         = "http://${attr.unique.network.ip-address}:4646"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/dashboard" -}}
{{- with nomadVar "nomad/jobs/dashboard" -}}
{{- if .ASSET_HASH -}}{{- .ASSET_HASH -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/assets.sha256"
        change_mode = "script"
        change_script {
          command       = "/bin/sh"
          args          = ["-c", "rm -rf /tmp/dashboard-stage && mkdir -p /tmp/dashboard-stage && wget -qO /tmp/dashboard.tar.gz http://hangar.aerial/internal/artifacts/dashboard.tar.gz && tar -xzf /tmp/dashboard.tar.gz -C /tmp/dashboard-stage && cp -a /tmp/dashboard-stage/. /local/static/ && rm -rf /tmp/dashboard-stage /tmp/dashboard.tar.gz && kill -HUP 1"]
          timeout       = "30s"
          fail_on_error = false
        }
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/dashboard" -}}
{{- with nomadVar "nomad/jobs/dashboard" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/dashboard.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/dashboard" -}}
{{- with nomadVar "nomad/jobs/dashboard" -}}
{{- if .BRAIN_CONFIG_YAML -}}{{- .BRAIN_CONFIG_YAML -}}{{- end -}}
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
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/dashboard.env"
        env         = true
      }

      service {
        name     = "dashboard"
        port     = "http"
        provider = "nomad"

        check {
          name     = "dashboard-health"
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
