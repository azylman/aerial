job "victoriametrics" {
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

  group "victoriametrics" {
    count = 1

    network {
      mode = "host"
      port "http" {
        static = 8428
      }
    }

    task "victoriametrics" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "victoriametrics/victoria-metrics:v1.153.0"
        network_mode = "host"
        healthchecks {
          disable = true
        }
        security_opt = ["no-new-privileges:true"]
        mounts = [
          {
            type     = "volume"
            target   = "/victoria-metrics-data"
            source   = "aerial-victoriametrics-data"
            readonly = false
          }
        ]
        args = [
          "-storageDataPath=/victoria-metrics-data",
          "-promscrape.config=/local/scrape.yml",
          "-promscrape.configCheckInterval=15s",
          "-promscrape.config.strictParse=false",
          "-retentionPeriod=5y",
          "-loggerFormat=json"
        ]
      }

      template {
        data = <<EOH
global:
  scrape_interval: 15s
  scrape_timeout: 10s

scrape_config_files:
  - "/local/scrapes/*.yml"

scrape_configs:
  - job_name: "cadvisor"
    static_configs:
      - targets: ["127.0.0.1:8083"]

  - job_name: "node-exporter"
    static_configs:
      - targets: ["127.0.0.1:9100"]

  - job_name: "victoriametrics"
    static_configs:
      - targets: ["127.0.0.1:8428"]

  - job_name: "aerial-brain"
    static_configs:
      - targets: ["127.0.0.1:8088"]

  - job_name: "aerial-hangar"
    static_configs:
      - targets: ["127.0.0.1:8087"]

  - job_name: "postgres"
    static_configs:
      - targets: ["127.0.0.1:9187"]

  - job_name: "webhooks-router"
    static_configs:
      - targets: ["127.0.0.1:4020"]
EOH
        destination   = "local/scrape.yml"
        change_mode   = "signal"
        change_signal = "SIGHUP"
      }

      template {
        data = <<EOH
{{- if nomadVarExists "nomad/jobs/victoriametrics" -}}
{{- with nomadVar "nomad/jobs/victoriametrics" -}}
{{- if .CONFIG_YAML -}}
{{- if not (contains "global:" .CONFIG_YAML) -}}
{{ .CONFIG_YAML }}
{{- else -}}
[]
{{- end -}}
{{- else -}}
[]
{{- end -}}
{{- end -}}
{{- else -}}
[]
{{- end -}}
EOH
        destination   = "local/scrapes/config.yml"
        change_mode   = "signal"
        change_signal = "SIGHUP"
      }

      template {
        data = <<EOH
{{ if nomadVarExists "nomad/jobs/shared" }}
{{ with nomadVar "nomad/jobs/shared" }}
{{ if .HA_METRICS_TOKEN }}HA_METRICS_TOKEN="{{ .HA_METRICS_TOKEN }}"{{ end }}
{{ end }}
{{ end }}
EOH
        destination = "secrets/ha.env"
        env         = true
      }

      service {
        name     = "victoriametrics"
        port     = "http"
        provider = "nomad"

        check {
          name     = "victoriametrics-health"
          type     = "http"
          path     = "/health"
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
        cpu        = 200
        memory     = 512
        memory_max = 1024
      }
    }
  }
}
