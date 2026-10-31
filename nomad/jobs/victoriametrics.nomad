job "victoriametrics" {
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

  group "victoriametrics" {
    count = 1

    network {
      mode = "host"
      port "http" {}
    }

    task "victoriametrics" {
      driver = "docker"

      config {
        dns_servers        = ["127.0.0.1"]
        dns_search_domains = ["aerial"]
        image        = "victoriametrics/victoria-metrics:v1.155.0"
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
          "-httpListenAddr=:${NOMAD_PORT_http}",
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
{{- range nomadService "cadvisor" }}
      - targets: ["{{ .Address }}:{{ .Port }}"]
{{- range $i, $t := .Tags }}{{- if eq $i 0 }}
        labels:
          instance: "{{ $t }}"
          node: "{{ $t }}"
{{- end }}{{- end }}
{{- else }}
      - targets: ["127.0.0.1:8083"]
{{- end }}
    metric_relabel_configs:
      - source_labels: [name]
        regex: "^(.+)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        target_label: name
        replacement: "$1"

  - job_name: "node-exporter"
    static_configs:
{{- range nomadService "node-exporter" }}
      - targets: ["{{ .Address }}:{{ .Port }}"]
{{- range $i, $t := .Tags }}{{- if eq $i 0 }}
        labels:
          instance: "{{ $t }}"
          node: "{{ $t }}"
{{- end }}{{- end }}
{{- else }}
      - targets: ["127.0.0.1:9100"]
{{- end }}

  - job_name: "victoriametrics"
    static_configs:
{{- range nomadService "victoriametrics" }}
      - targets: ["{{ .Address }}:{{ .Port }}"]
        labels:
          instance: "victoriametrics"
{{- else }}
      - targets: ["127.0.0.1:8428"]
        labels:
          instance: "victoriametrics"
{{- end }}

  - job_name: "aerial-brain"
    static_configs:
{{- range nomadService "eri" }}
      - targets: ["{{ .Address }}:{{ .Port }}"]
        labels:
          instance: "aerial-brain"
{{- else }}
      - targets: ["127.0.0.1:8088"]
        labels:
          instance: "aerial-brain"
{{- end }}

  - job_name: "aerial-hangar"
    static_configs:
{{- range nomadService "hangar" }}
      - targets: ["{{ .Address }}:{{ .Port }}"]
        labels:
          instance: "aerial-hangar"
{{- else }}
      - targets: ["127.0.0.1:8087"]
        labels:
          instance: "aerial-hangar"
{{- end }}

  - job_name: "postgres"
    static_configs:
{{- range nomadService "postgres-exporter" }}
      - targets: ["{{ .Address }}:{{ .Port }}"]
        labels:
          instance: "postgres"
{{- else }}
      - targets: ["127.0.0.1:9187"]
        labels:
          instance: "postgres"
{{- end }}

  - job_name: "synapse"
    static_configs:
{{- range nomadService "synapse" }}
      - targets: ["{{ .Address }}:{{ .Port }}"]
        labels:
          instance: "synapse"
{{- else }}
      - targets: ["127.0.0.1:4020"]
        labels:
          instance: "synapse"
{{- end }}
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
