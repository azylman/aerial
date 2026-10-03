job "infisical-mcp" {
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

  group "infisical-mcp" {
    count = 1

    network {
      mode = "host"
      port "mcp" {
        static = 4007
      }
    }

    task "infisical-mcp" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-infisical-mcp:latest"
        network_mode = "host"
      }

      env {
        PORT                         = "4007"
        INFISICAL_HOST_URL           = "http://infisical:8085"
        INFISICAL_AUTH_METHOD        = "universal-auth"
        INFISICAL_MASK_SECRET_VALUES = "false"
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
        cpu    = 200
        memory = 256
      }
    }
  }
}
