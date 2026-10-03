job "postgres-backup" {
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

  group "postgres-backup" {
    count = 1

    network {
      mode = "host"
      port "health" {
        static = 8086
      }
    }

    task "postgres-backup" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "prodrigestivill/postgres-backup-local:16"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        volumes = [
          "/mnt/data/supervisor/share/Aerial/backups/postgres:/backups"
        ]
      }

      env {
        POSTGRES_HOST       = "postgres"
        POSTGRES_PORT       = "5432"
        POSTGRES_DB         = "aerial"
        POSTGRES_USER       = "aerial"
        SCHEDULE            = "@daily"
        BACKUP_KEEP_DAYS    = "14"
        BACKUP_KEEP_WEEKS   = "26"
        BACKUP_KEEP_MONTHS  = "1200"
        BACKUP_LATEST_TYPE  = "none"
        POSTGRES_EXTRA_OPTS = "-Fc"
        HEALTHCHECK_PORT    = "8086"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .POSTGRES_PASSWORD }}POSTGRES_PASSWORD="{{ .POSTGRES_PASSWORD }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/postgres.env"
        env         = true
      }

      service {
        name     = "postgres-backup"
        port     = "health"
        provider = "nomad"

        check {
          name     = "postgres-backup-health"
          type     = "http"
          path     = "/"
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
