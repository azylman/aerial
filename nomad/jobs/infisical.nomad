job "infisical" {
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
    min_healthy_time  = "10s"
    healthy_deadline  = "3m"
    progress_deadline = "5m"
    auto_revert       = true
  }

  group "infisical" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 8085
      }
      port "redis" {
        static = 6380
      }
    }

    restart {
      attempts = 5
      delay    = "15s"
      interval = "10m"
      mode     = "delay"
    }

    # Task 1: Redis cache & session backend
    task "redis" {
      driver = "docker"

      lifecycle {
        hook    = "prestart"
        sidecar = true
      }

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "valkey/valkey:9-alpine"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        args         = ["--port", "6380", "--bind", "0.0.0.0", "--protected-mode", "no", "--save", ""]
      }

      service {
        name     = "infisical-redis"
        port     = "redis"
        provider = "nomad"

        check {
          name     = "redis-tcp-ping"
          type     = "tcp"
          interval = "15s"
          timeout  = "3s"
          check_restart {
            limit           = 3
            grace           = "30s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu        = 100
        memory     = 64
        memory_max = 128
      }
    }

    # Task 2: Infisical secret management core server
    task "server" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "infisical/infisical:v0.166.2"
        network_mode = "host"
        healthchecks {
          disable = true
        }
      }

      env {
        PORT                          = "8085"
        HOST                          = "0.0.0.0"
        REDIS_URL                     = "redis://infisical-redis:6380"
        SITE_URL                      = "http://infisical:8085"
        ALLOW_INTERNAL_IP_CONNECTIONS = "true"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/infisical" }}
{{ with nomadVar "nomad/jobs/infisical" }}
ENCRYPTION_KEY="{{ .ENCRYPTION_KEY }}"
AUTH_SECRET="{{ .AUTH_SECRET }}"
DB_CONNECTION_URI="postgresql://aerial:{{ .DB_PASSWORD }}@postgres:5432/infisical?sslmode=disable"
{{ end }}
{{ end }}
EOH
        destination = "secrets/infisical.env"
        env         = true
      }

      service {
        name     = "infisical"
        port     = "http"
        provider = "nomad"

        check {
          name     = "infisical-health"
          type     = "http"
          path     = "/api/status"
          interval = "15s"
          timeout  = "5s"
          check_restart {
            limit           = 3
            grace           = "60s"
            ignore_warnings = false
          }
        }
      }

      resources {
        cpu        = 500
        memory     = 1024
        memory_max = 2048
      }
    }
  }
}
