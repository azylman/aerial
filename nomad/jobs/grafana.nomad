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
      port "http" {
        to = 3000
      }
    }

    task "grafana" {
      driver = "docker"

      config {
        dns_servers        = ["${attr.unique.network.ip-address}"]
        dns_search_domains = ["aerial"]
        image        = "grafana/grafana:13.2.4"
        ports        = ["http"]
        healthchecks {
          disable = true
        }
        security_opt = ["no-new-privileges:true"]
        entrypoint   = ["/bin/sh", "-c", "mkdir -p /local/dashboards/system /local/dashboards/custom && cp -a /local/dashboards-seed/. /local/dashboards/ && exec /run.sh"]
      }

      artifact {
        source      = "http://hangar.aerial/internal/artifacts/grafana.tar.gz"
        destination = "local/dashboards-seed"
      }

      env {
        GF_PATHS_PROVISIONING          = "/local/provisioning"
        GF_DATABASE_TYPE               = "sqlite3"
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
{{- if nomadVarExists "nomad/jobs/grafana" -}}
{{- with nomadVar "nomad/jobs/grafana" -}}
{{- if .ASSET_HASH -}}{{- .ASSET_HASH -}}{{- end -}}
{{- end -}}
{{- end -}}
EOH
        destination = "local/assets.sha256"
        change_mode = "script"
        change_script {
          command       = "/bin/sh"
          args          = ["-c", "rm -rf /tmp/grafana-stage && mkdir -p /tmp/grafana-stage && wget -qO /tmp/grafana.tar.gz http://hangar.aerial/internal/artifacts/grafana.tar.gz && tar -xzf /tmp/grafana.tar.gz -C /tmp/grafana-stage && cp -a /tmp/grafana-stage/system/. /local/dashboards/system/ && cp -a /tmp/grafana-stage/custom/. /local/dashboards/custom/ && rm -rf /tmp/grafana-stage /tmp/grafana.tar.gz"]
          timeout       = "30s"
          fail_on_error = false
        }
      }

      template {
        data = <<EOH
apiVersion: 1

datasources:
  - name: VictoriaMetrics
    type: prometheus
    access: proxy
    url: http://victoriametrics.aerial
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
      path: /local/dashboards/system
      foldersFromFilesStructure: true
  - name: 'Aerial Custom'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    editable: true
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: /local/dashboards/custom
      foldersFromFilesStructure: true
EOH
        destination = "local/provisioning/dashboards/dashboards.yml"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
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
