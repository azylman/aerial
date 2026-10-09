variable "image_tag" {
  type    = string
  default = "latest"
}

job "docker-mcp" {
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

  group "docker-mcp" {
    count = 1

    network {
      port "mcp" {
        to = 4002
      }
    }

    task "docker-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-docker-mcp:${var.image_tag}"
        ports        = ["mcp"]
        healthchecks {
          disable = true
        }
        volumes = [
          "/var/run/docker.sock:/var/run/docker.sock"
        ]
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/docker-mcp" -}}
{{- with nomadVar "nomad/jobs/docker-mcp" -}}
{{- if .CONFIG_YAML -}}{{- .CONFIG_YAML -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/config.yaml"
        change_mode = "restart"
      }


      service {
        name     = "docker-mcp"
        port     = "mcp"
        provider = "nomad"

        check {
          name     = "docker-mcp-tcp"
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
        memory     = 384
        memory_max = 768
      }
    }
  }
}
