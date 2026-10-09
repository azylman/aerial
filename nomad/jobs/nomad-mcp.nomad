variable "image_tag" {
  type    = string
  default = "latest"
}

job "nomad-mcp" {
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

  group "nomad-mcp" {
    count = 1

    network {
      mode = "host"
      port "mcp" {
        static = 4006
      }
    }

    task "nomad-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-nomad-mcp:${var.image_tag}"
        network_mode = "host"
        healthchecks {
          disable = true
        }
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/nomad-mcp" -}}
{{- with nomadVar "nomad/jobs/nomad-mcp" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/config.yaml"
        change_mode = "restart"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/nomad-mcp" }}
{{ with nomadVar "nomad/jobs/nomad-mcp" }}
{{ if .token }}NOMAD_TOKEN="{{ .token }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/nomad.env"
        env         = true
      }

      service {
        name     = "nomad-mcp"
        port     = "mcp"
        provider = "nomad"

        check {
          name     = "nomad-mcp-tcp"
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
