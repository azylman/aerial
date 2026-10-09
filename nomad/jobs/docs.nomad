variable "image_tag" {
  type    = string
  default = "latest"
}

job "docs" {
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

  group "docs" {
    count = 1

    network {
      port "http" {
        to = 80
      }
    }

    task "docs" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image              = "ghcr.io/azylman/aerial-docs:${var.image_tag}"
        ports              = ["http"]
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/aerial-config:/share/aerial-config:ro",
          "/mnt/data/supervisor/share/aerial/docs-service/app:/usr/share/nginx/html:ro"
        ]
      }

      service {
        name     = "docs"
        port     = "http"
        provider = "nomad"

        check {
          name            = "docs-http"
          type            = "http"
          path            = "/health"
          interval        = "15s"
          timeout         = "3s"
          check_restart {
            limit           = 3
            grace           = "60s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu        = 50
        memory     = 64
        memory_max = 128
      }
    }
  }
}
