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
    canary            = 0
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
{{ if .client_id }}INFISICAL_UNIVERSAL_AUTH_CLIENT_ID="{{ .client_id }}"{{ end }}
{{ if .client_secret }}INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET="{{ .client_secret }}"{{ end }}
{{ if .token }}INFISICAL_TOKEN="{{ .token }}"{{ end }}
{{ if .project_id }}INFISICAL_PROJECT_ID="{{ .project_id }}"{{ end }}
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
        cpu        = 100
        memory     = 256
        memory_max = 512
      }
    }
  }
}
