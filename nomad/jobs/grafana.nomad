job "grafana" {
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

  group "grafana" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 3000
      }
    }

    task "grafana" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "grafana/grafana:13.2.3"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        security_opt = ["no-new-privileges:true"]
        volumes = [
          "/mnt/data/supervisor/share/aerial:/share/aerial:ro",
          "/mnt/data/supervisor/share/aerial-config:/share/aerial-config:ro"
        ]
      }

      env {
        GF_PATHS_PROVISIONING          = "/local/provisioning"
        GF_DATABASE_TYPE               = "postgres"
        GF_DATABASE_HOST               = "postgres:5432"
        GF_DATABASE_NAME               = "aerial"
        GF_DATABASE_USER               = "aerial"
        GF_DATABASE_SSL_MODE           = "disable"
        GF_SERVER_ROOT_URL             = "%(protocol)s://%(domain)s:%(http_port)s/grafana/"
        GF_SERVER_SERVE_FROM_SUB_PATH  = "true"
        GF_SECURITY_ADMIN_USER         = "admin"
        GF_USERS_ALLOW_SIGN_UP         = "false"
        GF_AUTH_ANONYMOUS_ENABLED      = "true"
        GF_AUTH_ANONYMOUS_ORG_ROLE     = "Admin"
        GF_AUTH_ANONYMOUS_ORG_NAME     = "Main Org."
        GF_AUTH_ANONYMOUS_HIDE_VERSION = "true"
        GF_AUTH_DISABLE_LOGIN_FORM     = "true"
        GF_AUTH_DISABLE_SIGNOUT_MENU   = "true"
        GF_PANELS_DISABLE_SANITIZE_HTML = "true"
        GF_LOG_MODE                    = "console"
        GF_LOG_CONSOLE_FORMAT          = "json"
      }

      template {
        data = <<EOH
apiVersion: 1

datasources:
  - name: VictoriaMetrics
    type: prometheus
    access: proxy
    url: http://victoriametrics:8428
    isDefault: true
    jsonData:
      httpMethod: POST
      timeInterval: "15s"
    editable: false
EOH
        destination = "local/provisioning/datasources/victoriametrics.yml"
      }

      template {
        data = <<EOH
apiVersion: 1

providers:
  - name: 'Aerial System'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    editable: true
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: /share/aerial/grafana/dashboards
      foldersFromFilesStructure: true
EOH
        destination = "local/provisioning/dashboards/dashboards.yml"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .POSTGRES_PASSWORD }}GF_DATABASE_PASSWORD="{{ .POSTGRES_PASSWORD }}"{{ end }}
{{ if .GRAFANA_ADMIN_PASSWORD }}GF_SECURITY_ADMIN_PASSWORD="{{ .GRAFANA_ADMIN_PASSWORD }}"{{ else if .POSTGRES_PASSWORD }}GF_SECURITY_ADMIN_PASSWORD="{{ .POSTGRES_PASSWORD }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/grafana.env"
        env         = true
      }

      service {
        name     = "grafana"
        port     = "http"
        provider = "nomad"

        check {
          name     = "grafana-health"
          type     = "http"
          path     = "/api/health"
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
        memory     = 512
        memory_max = 1024
      }
    }
  }
}
