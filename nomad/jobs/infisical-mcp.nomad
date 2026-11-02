variable "image_tag" {
  type    = string
  default = "latest"
}

job "infisical-mcp" {
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

  group "infisical-mcp" {
    count = 1

    network {
      port "mcp" {
        to = 4007
      }
    }

    task "infisical-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-infisical-mcp:${var.image_tag}"
        ports        = ["mcp"]
        healthchecks {
          disable = true
        }
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/infisical-mcp" -}}
{{- with nomadVar "nomad/jobs/infisical-mcp" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/config.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/infisical-mcp" }}
{{ with nomadVar "nomad/jobs/infisical-mcp" }}
{{ if .client_id.Value }}INFISICAL_UNIVERSAL_AUTH_CLIENT_ID="{{ .client_id.Value }}"{{ end }}
{{ if .client_secret.Value }}INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET="{{ .client_secret.Value }}"{{ end }}
{{ if .token.Value }}INFISICAL_TOKEN="{{ .token.Value }}"{{ end }}
{{ if .project_id.Value }}INFISICAL_PROJECT_ID="{{ .project_id.Value }}"{{ end }}
{{ end }}
{{ end }}
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .INFISICAL_CLIENT_ID.Value }}INFISICAL_UNIVERSAL_AUTH_CLIENT_ID="{{ .INFISICAL_CLIENT_ID.Value }}"{{ end }}
{{ if .INFISICAL_CLIENT_SECRET.Value }}INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET="{{ .INFISICAL_CLIENT_SECRET.Value }}"{{ end }}
{{ if .INFISICAL_TOKEN.Value }}INFISICAL_TOKEN="{{ .INFISICAL_TOKEN.Value }}"{{ end }}
{{ if .INFISICAL_PROJECT_ID.Value }}INFISICAL_PROJECT_ID="{{ .INFISICAL_PROJECT_ID.Value }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/infisical.env"
        env         = true
      }

      service {
        name     = "infisical-mcp"
        port     = "mcp"
        provider = "nomad"

        check {
          name     = "infisical-mcp-tcp"
          type     = "tcp"
          port     = "mcp"
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
