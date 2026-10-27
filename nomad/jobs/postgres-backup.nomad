job "postgres-backup" {
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

  group "postgres-backup" {
    count = 1

    network {
      port "health" {
        to = 8086
      }
    }

    task "postgres-backup" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "prodrigestivill/postgres-backup-local:18"
        init         = true
        ports        = ["health"]
        healthchecks {
          disable = true
        }
        mounts = [
          {
            type     = "volume"
            target   = "/backups"
            source   = "nas-aerial-backups"
            readonly = false
            volume_options = [{
              driver_config = [{
                name = "local"
                options = [{
                  type   = "cifs"
                  device = "//${NAS_HOST}/Aerial/backups/postgres"
                  o      = "addr=${NAS_HOST},vers=3.1.1,username=aerial,password=${QNAP_PASS},uid=0,gid=0,file_mode=0755,dir_mode=0755,soft,nounix,mapposix"
                }]
              }]
            }]
          }
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
{{ if .NAS_HOST }}NAS_HOST="{{ .NAS_HOST }}"{{ end }}
{{ if .QNAP_PASS }}QNAP_PASS="{{ .QNAP_PASS }}"{{ end }}
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
        cpu        = 50
        memory     = 128
        memory_max = 256
      }
    }
  }
}
