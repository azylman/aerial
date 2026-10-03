job "webhooks-router" {
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

  group "webhooks-router" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 4020
      }
    }

    task "webhooks-router" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "ghcr.io/azylman/aerial-webhooks-router:latest"
        network_mode = "host"
        force_pull   = true
      }

      env {
        PORT                  = "4020"
        INFISICAL_URL         = "http://infisical:8085"
        INFISICAL_ENVIRONMENT = "default"
        NOMAD_ADDR            = "http://127.0.0.1:4646"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/infisical-mcp" }}
{{ with nomadVar "nomad/jobs/infisical-mcp" }}
{{ if .client_id }}INFISICAL_CLIENT_ID="{{ .client_id }}"{{ end }}
{{ if .client_secret }}INFISICAL_CLIENT_SECRET="{{ .client_secret }}"{{ end }}
{{ if .project_id }}INFISICAL_PROJECT_ID="{{ .project_id }}"{{ end }}
{{ end }}
{{ end }}
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .GITHUB_PAT }}GITHUB_PAT="{{ .GITHUB_PAT }}"{{ end }}
{{ if .POSTGRES_PASSWORD }}POSTGRES_URL="postgres://aerial:{{ .POSTGRES_PASSWORD }}@postgres:5432/aerial?sslmode=disable"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/webhooks-router.env"
        env         = true
      }

      service {
        name     = "webhooks-router"
        port     = "http"
        provider = "nomad"

        check {
          name     = "webhooks-router-health"
          type     = "http"
          path     = "/healthz"
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
        memory = 128
      }
    }
  }
}
