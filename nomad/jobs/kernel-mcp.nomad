variable "image_tag" {
  type    = string
  default = "latest"
}

job "kernel-mcp" {
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

  group "kernel-mcp" {
    count = 1

    network {
      port "http" {
        to = 4018
      }
    }

    task "kernel-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-kernel-mcp:${var.image_tag}"
        init         = true
        ports        = ["http"]
        healthchecks {
          disable = true
        }
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/kernel-mcp" -}}
{{- with nomadVar "nomad/jobs/kernel-mcp" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/config.yaml"
        change_mode = "restart"
      }

      service {
        name     = "kernel-mcp"
        port     = "http"
        provider = "nomad"

        check {
          name     = "kernel-mcp-health"
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
