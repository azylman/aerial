job "victoriametrics-mcp" {
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

  group "victoriametrics-mcp" {
    count = 1

    network {
      mode = "host"
      port "mcp" {
        static = 4044
      }
    }

    task "victoriametrics-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/victoriametrics/mcp-victoriametrics:latest"
        network_mode = "host"
      }

      env {
        MCP_SERVER_MODE        = "http"
        MCP_LISTEN_ADDR        = ":4044"
        VM_INSTANCE_ENTRYPOINT = "http://victoriametrics:8428"
        VM_INSTANCE_TYPE       = "single"
        MCP_DISABLED_TOOLS     = "documentation,export,metric_relabel_debug"
      }

      service {
        name     = "victoriametrics-mcp"
        port     = "mcp"
        provider = "nomad"

        check {
          name     = "victoriametrics-mcp-tcp"
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
        memory     = 64
        memory_max = 128
      }
    }
  }
}
