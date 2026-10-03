job "docker-mcp" {
  datacenters = ["dc1"]
  type        = "service"

  # Target quiet-zero core server node
  constraint {
    attribute = "${node.class}"
    operator  = "regexp"
    value     = "quiet-zero|haos"
  }

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
      mode = "host"
      port "mcp" {
        static = 4002
      }
    }

    task "docker-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-docker-mcp:latest"
        network_mode = "host"
        volumes = [
          "/var/run/docker.sock:/var/run/docker.sock"
        ]
      }

      env {
        PORT = "4002"
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
        cpu    = 100
        memory = 128
      }
    }
  }
}
