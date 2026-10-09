job "postgres" {
  datacenters = ["dc1"]
  type        = "service"

  node_pool   = "default"

  update {
    max_parallel      = 1
    canary            = 0
    min_healthy_time  = "10s"
    healthy_deadline  = "2m"
    progress_deadline = "3m"
    auto_revert       = true
  }

  group "postgres" {
    count = 1

    network {
      mode = "host"
      port "db" {
        static = 5432
      }
    }

    task "postgres" {
      driver = "docker"

      kill_timeout = "30s"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "pgvector/pgvector:pg18"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        mounts = [
          {
            type     = "volume"
            target   = "/var/lib/postgresql/data"
            source   = "aerial-postgres-18-data"
            readonly = false
          }
        ]
      }

      env {
        POSTGRES_DB   = "aerial"
        POSTGRES_USER = "aerial"
        PGDATA        = "/var/lib/postgresql/data/pgdata"
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
        name     = "postgres"
        port     = "db"
        provider = "nomad"

        check {
          name     = "postgres-tcp"
          type     = "tcp"
          port     = "db"
          interval = "10s"
          timeout  = "3s"

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
